#!/usr/bin/env bash
# fast-copydb.sh (v3.3 — Python-only; Linux/macOS portable)
# - EXCLUDE_TABLES_DATA: keep table structure but skip data for selected tables
#   Implementation: DDL-only dump -> load, then Data-only dump for included tables -> load
set -Eeuo pipefail

export LC_ALL=C LANG=C LANGUAGE=C

log(){ printf "\033[1;34m[fast-copydb]\033[0m %s\n" "$*" >&2; }
warn(){ printf "\033[1;33m[fast-copydb WARNING]\033[0m %s\n" "$*" >&2; }
die(){ printf "\033[1;31m[fast-copydb ERROR]\033[0m %s\n" "$*" >&2; exit 1; }
require_cmd(){ command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"; }
tolower(){ printf '%s' "$1" | tr '[:upper:]' '[:lower:]'; }

# Find a free port (portable)
find_free_port(){
  local tries=300 port
  for _ in $(seq 1 $tries); do
    port=$(( 24000 + (RANDOM % 6000) ))
    lsof -i TCP:${port} -sTCP:LISTEN -nP >/dev/null 2>&1 || { echo "$port"; return 0; }
  done
  return 1
}

# Backtick-quote identifier safely (works with set -u, mac bash 3.2)
qi(){
  local name="${1-}"
  local bt; bt="$(printf '%b' '\x60')"   # `
  local escaped
  escaped="$(printf '%s' "$name" | awk 'BEGIN{RS="";ORS=""}{gsub(/`/, "``"); print}')"
  printf '%s%s%s' "$bt" "$escaped" "$bt"
}

# Build Python list of tables to INCLUDE (all base tables minus EXCLUDE_TABLES_DATA)
# Produces:
#   PY_INCLUDED_LIST       -> "['t1','t2',...]"
#   PY_INCLUDED_LIST_EMPTY -> "1" if empty else "0"
build_included_tables_list(){
  local excluded_csv
  excluded_csv="$(printf '%s' "${EXCLUDE_TABLES_DATA-}" | tr -d '[:space:]')"

  local exclude_set_file; exclude_set_file="$(mktemp)"; : > "$exclude_set_file"
  if [ -n "$excluded_csv" ]; then
    IFS=',' read -r -a _arr <<< "$excluded_csv"
    for item in "${_arr[@]}"; do
      [ -n "$item" ] || continue
      item="${item//\`/}"
      case "$item" in
        *.*)  # only honor same-schema exclusions
          local schema="${item%%.*}" tbl="${item#*.}"
          [ "$schema" = "$SOURCE_DB_NAME" ] || continue
          printf '%s\n' "$tbl" >> "$exclude_set_file"
          ;;
        *)    printf '%s\n' "$item" >> "$exclude_set_file" ;;
      esac
    done
    sort -u "$exclude_set_file" -o "$exclude_set_file"
  fi

  local sqlq="SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
              WHERE TABLE_SCHEMA='${SOURCE_DB_NAME}' AND TABLE_TYPE='BASE TABLE' \
              ORDER BY TABLE_NAME;"
  local all_tbls
  all_tbls="$(
    printf '%s\n' "$sqlq" \
    | mysqlsh --sql --quiet-start=2 --uri "${REMOTE_DB_USER}:${REMOTE_DB_PASSWORD}@127.0.0.1:${PORT}" 2>/dev/null \
    | awk '
        BEGIN{IGNORECASE=1}
        /^[[:space:]]*$/ {next}
        /^[-+|]/ {next}
        $1=="TABLE_NAME" {next}
        {print $1}
      '
  )"

  local includes=()
  if [ -n "$all_tbls" ]; then
    while IFS= read -r t; do
      printf '%s' "$t" | grep -Eq '^[A-Za-z0-9_\$]+$' || continue
      if [ -s "$exclude_set_file" ] && grep -Fxq "$t" "$exclude_set_file"; then
        continue
      fi
      includes+=("'$t'")
    done <<EOF
$all_tbls
EOF
  fi
  rm -f "$exclude_set_file" || true

  if [ ${#includes[@]} -eq 0 ]; then
    PY_INCLUDED_LIST="[]"
    PY_INCLUDED_LIST_EMPTY="1"
  else
    PY_INCLUDED_LIST="[$(IFS=','; echo "${includes[*]}")]"
    PY_INCLUDED_LIST_EMPTY="0"
  fi
}

# --- cfg ---
CFG="${1:-}"; [ -n "$CFG" ] && [ -f "$CFG" ] || die "Usage: ./fast-copydb.sh /path/to/server.cfg"
set -a
# shellcheck source=/dev/null
. "$CFG"
set +a

# --- required & defaults ---
require_cmd ssh; require_cmd docker; require_cmd mysqlsh
: "${REMOTE_HOST:?}"; : "${REMOTE_SSH_USER:?}"
: "${REMOTE_DB_USER:?}"; : "${REMOTE_DB_PASSWORD:?}"
: "${SOURCE_DB_NAME:?}"
: "${TARGET_DOCKER_CONTAINER:?}"; : "${TARGET_DB_USER:?}"; : "${TARGET_DB_PASSWORD:?}"; : "${TARGET_DB_NAME:?}"

REMOTE_DB_HOST="${REMOTE_DB_HOST:-127.0.0.1}"
REMOTE_DB_PORT="${REMOTE_DB_PORT:-3306}"
SSH_PORT="${SSH_PORT:-22}"
SSH_IDENTITY_FILE="${SSH_IDENTITY_FILE:-}"
SSH_STRICT_HOST_KEY_CHECKING="${SSH_STRICT_HOST_KEY_CHECKING:-yes}"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOCAL_DUMP_BASE="${LOCAL_DUMP_BASE:-/tmp}"
# Base names
DUMP_BASE="${LOCAL_DUMP_BASE%/}/${SOURCE_DB_NAME}_${TIMESTAMP}"
DUMP_DIR="${DUMP_BASE}"                   # used when no exclusions
DUMP_DIR_DDL="${DUMP_BASE}_ddl"           # used when exclusions
DUMP_DIR_DATA="${DUMP_BASE}_data"         # used when exclusions

# Threads/knobs
if command -v nproc >/dev/null 2>&1; then CPU_THREADS="$(nproc)"; elif command -v sysctl >/dev/null 2>&1; then CPU_THREADS="$(sysctl -n hw.ncpu 2>/dev/null || echo 4)"; else CPU_THREADS=4; fi
DUMP_THREADS="${DUMP_THREADS:-$CPU_THREADS}"
LOAD_THREADS="${LOAD_THREADS:-$CPU_THREADS}"
DUMP_COMPRESSION="${DUMP_COMPRESSION:-zstd}"     # zstd|gzip|none
DEFER_INDEXES="${DEFER_INDEXES:-all}"            # none|fulltext|secondary|all
IGNORE_EXISTING="${IGNORE_EXISTING:-true}"       # true|false
KEEP_DUMP="${KEEP_DUMP:-false}"
DROP_TARGET_DATABASE_BEFORE_LOAD="${DROP_TARGET_DATABASE_BEFORE_LOAD:-true}"
TARGET_DB_CHARSET="${TARGET_DB_CHARSET:-}"
TARGET_DB_COLLATION="${TARGET_DB_COLLATION:-}"
EXCLUDE_TABLES_DATA="${EXCLUDE_TABLES_DATA:-}"

[ -n "$EXCLUDE_TABLES_DATA" ] && log "Will skip data for tables: ${EXCLUDE_TABLES_DATA}"

# --- SSH tunnel to SOURCE ---
PORT="$(find_free_port)" || die "No free local port for SSH tunnel"
SSH_OPTS=( -p "$SSH_PORT" -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=60 -o StrictHostKeyChecking="${SSH_STRICT_HOST_KEY_CHECKING}" )
[ -n "$SSH_IDENTITY_FILE" ] && SSH_OPTS+=( -i "$SSH_IDENTITY_FILE" )

log "Opening SSH tunnel: localhost:${PORT} -> ${REMOTE_DB_HOST}:${REMOTE_DB_PORT} via ${REMOTE_SSH_USER}@${REMOTE_HOST}"
ssh -fN -L "${PORT}:${REMOTE_DB_HOST}:${REMOTE_DB_PORT}" "${SSH_OPTS[@]}" "${REMOTE_SSH_USER}@${REMOTE_HOST}" || die "SSH tunnel failed"
TUNNEL_PID="$(pgrep -f "ssh .*${PORT}:${REMOTE_DB_HOST}:${REMOTE_DB_PORT}.*${REMOTE_SSH_USER}@${REMOTE_HOST}" | head -n1 || true)"
cleanup(){ if [ -n "${TUNNEL_PID:-}" ] && ps -p "$TUNNEL_PID" >/dev/null 2>&1; then log "Closing SSH tunnel (pid $TUNNEL_PID)"; kill "$TUNNEL_PID" || true; fi; }
trap cleanup EXIT

# --- Build include list (needs tunnel) ---
build_included_tables_list

# --- DUMP with mysqlsh --py ---
rm -rf -- "$DUMP_DIR" "$DUMP_DIR_DDL" "$DUMP_DIR_DATA" 2>/dev/null || true

if [ -z "$EXCLUDE_TABLES_DATA" ]; then
  # No exclusions: one directory, full dump
  if [ "$SOURCE_DB_NAME" = "$TARGET_DB_NAME" ]; then
    log "Dumping schema '${SOURCE_DB_NAME}' to ${DUMP_DIR}"
    mysqlsh --py --quiet-start=2 \
      --uri "${REMOTE_DB_USER}:${REMOTE_DB_PASSWORD}@127.0.0.1:${PORT}" \
      -e "opts={'threads':${DUMP_THREADS},'consistent':True,'compression':'${DUMP_COMPRESSION}','showProgress':True}; util.dump_schemas(['${SOURCE_DB_NAME}'],'${DUMP_DIR}',opts)" \
      || die "dump_schemas failed"
  else
    log "Renaming schema (${SOURCE_DB_NAME} -> ${TARGET_DB_NAME}); dumping all tables to ${DUMP_DIR}"
    mysqlsh --py --quiet-start=2 \
      --uri "${REMOTE_DB_USER}:${REMOTE_DB_PASSWORD}@127.0.0.1:${PORT}" \
      -e "opts={'all':True,'threads':${DUMP_THREADS},'consistent':True,'compression':'${DUMP_COMPRESSION}','showProgress':True}; util.dump_tables('${SOURCE_DB_NAME}',[],'${DUMP_DIR}',opts)" \
      || die "dump_tables failed"
  fi
  [ -d "$DUMP_DIR" ] || die "Dump finished but dump dir not found: $DUMP_DIR"
  log "Dump complete at: ${DUMP_DIR}"
else
  # Exclusions present: two separate directories (DDL-only, then Data-only for included)
  if [ "$SOURCE_DB_NAME" = "$TARGET_DB_NAME" ]; then
    log "Dumping DDL-only for schema '${SOURCE_DB_NAME}' to ${DUMP_DIR_DDL}"
    mysqlsh --py --quiet-start=2 \
      --uri "${REMOTE_DB_USER}:${REMOTE_DB_PASSWORD}@127.0.0.1:${PORT}" \
      -e "opts={'threads':${DUMP_THREADS},'compression':'${DUMP_COMPRESSION}','showProgress':True,'ddlOnly':True}; util.dump_schemas(['${SOURCE_DB_NAME}'],'${DUMP_DIR_DDL}',opts)" \
      || die "dump_schemas (ddlOnly) failed"
  else
    log "Renaming schema (${SOURCE_DB_NAME} -> ${TARGET_DB_NAME}); dumping DDL-only for all objects to ${DUMP_DIR_DDL}"
    mysqlsh --py --quiet-start=2 \
      --uri "${REMOTE_DB_USER}:${REMOTE_DB_PASSWORD}@127.0.0.1:${PORT}" \
      -e "opts={'all':True,'threads':${DUMP_THREADS},'compression':'${DUMP_COMPRESSION}','showProgress':True,'ddlOnly':True}; util.dump_tables('${SOURCE_DB_NAME}', [], '${DUMP_DIR_DDL}', opts)" \
      || die "dump_tables (all, ddlOnly) failed"
  fi
  [ -d "$DUMP_DIR_DDL" ] || die "DDL-only dump dir missing: $DUMP_DIR_DDL"

  if [ "$PY_INCLUDED_LIST_EMPTY" = "0" ]; then
    log "Dumping data-only for included tables to ${DUMP_DIR_DATA}"
    mysqlsh --py --quiet-start=2 \
      --uri "${REMOTE_DB_USER}:${REMOTE_DB_PASSWORD}@127.0.0.1:${PORT}" \
      -e "opts={'threads':${DUMP_THREADS},'compression':'${DUMP_COMPRESSION}','showProgress':True,'dataOnly':True}; util.dump_tables('${SOURCE_DB_NAME}', ${PY_INCLUDED_LIST}, '${DUMP_DIR_DATA}', opts)" \
      || die "dump_tables (dataOnly) failed"
    [ -d "$DUMP_DIR_DATA" ] || die "Data-only dump dir missing: $DUMP_DIR_DATA"
  else
    log "All tables excluded from data; structure-only clone."
  fi
fi

# --- TARGET docker port ---
HOST_BIND="$(docker port "${TARGET_DOCKER_CONTAINER}" 3306/tcp 2>/dev/null || true)"
[ -n "$HOST_BIND" ] || die "Target port 3306 not published. Start container with -p HOSTPORT:3306."
TARGET_DB_HOST="${TARGET_DB_HOST:-127.0.0.1}"
TARGET_DB_PORT="${TARGET_DB_PORT:-${HOST_BIND##*:}}"
log "Target MySQL via ${TARGET_DB_HOST}:${TARGET_DB_PORT}"

# --- SQL helper ---
sql(){
  printf '%s\n' "$1" | env LC_ALL=C LANG=C \
    mysqlsh --sql --quiet-start=2 \
      --uri "${TARGET_DB_USER}:${TARGET_DB_PASSWORD}@${TARGET_DB_HOST}:${TARGET_DB_PORT}"
}

# --- local_infile ON during load ---
ORIG_LOCAL_INFILE="$(sql "SELECT @@GLOBAL.local_infile;" 2>/dev/null | tail -n1 | tr -dc '0-9' || true)"; [ -z "$ORIG_LOCAL_INFILE" ] && ORIG_LOCAL_INFILE="0"
if [ "$ORIG_LOCAL_INFILE" != "1" ]; then
  log "Enabling local_infile=ON for load (was ${ORIG_LOCAL_INFILE})"
  sql "SET GLOBAL local_infile=ON;" || warn "Could not SET GLOBAL local_infile=ON (privileges?)"
fi

# --- Drop/Create target schema ---
if [ "$(tolower "${DROP_TARGET_DATABASE_BEFORE_LOAD}")" = "true" ]; then
  if [ "$SOURCE_DB_NAME" = "$TARGET_DB_NAME" ]; then
    log "Dropping existing schema $(qi "$SOURCE_DB_NAME") on target"
    sql "DROP DATABASE IF EXISTS $(qi "$SOURCE_DB_NAME");"
  else
    log "Dropping and re-creating schema $(qi "$TARGET_DB_NAME") on target"
    ddl="DROP DATABASE IF EXISTS $(qi "$TARGET_DB_NAME"); CREATE DATABASE $(qi "$TARGET_DB_NAME")"
    [ -n "$TARGET_DB_CHARSET" ]   && ddl="${ddl} CHARACTER SET ${TARGET_DB_CHARSET}"
    [ -n "$TARGET_DB_COLLATION" ] && ddl="${ddl} COLLATE ${TARGET_DB_COLLATION}"
    ddl="${ddl};"
    sql "$ddl"
  fi
fi

# --- LOAD ---
PY_IGNORE=$( [ "$(tolower "$IGNORE_EXISTING")" = "true" ] && echo True || echo False )

if [ -z "$EXCLUDE_TABLES_DATA" ]; then
  # One directory load
  if [ "$SOURCE_DB_NAME" = "$TARGET_DB_NAME" ]; then
    log "Loading from ${DUMP_DIR}"
    mysqlsh --py --quiet-start=2 \
      --uri "${TARGET_DB_USER}:${TARGET_DB_PASSWORD}@${TARGET_DB_HOST}:${TARGET_DB_PORT}" \
      -e "opts={'threads':${LOAD_THREADS},'deferTableIndexes':'${DEFER_INDEXES}','ignoreExistingObjects':${PY_IGNORE},'showProgress':True}; util.load_dump('${DUMP_DIR}',opts)" \
      || die "load_dump failed"
  else
    log "Loading (rename to '${TARGET_DB_NAME}') from ${DUMP_DIR}"
    mysqlsh --py --quiet-start=2 \
      --uri "${TARGET_DB_USER}:${TARGET_DB_PASSWORD}@${TARGET_DB_HOST}:${TARGET_DB_PORT}" \
      -e "opts={'threads':${LOAD_THREADS},'deferTableIndexes':'${DEFER_INDEXES}','ignoreExistingObjects':${PY_IGNORE},'showProgress':True,'schema':'${TARGET_DB_NAME}'}; util.load_dump('${DUMP_DIR}',opts)" \
      || die "load_dump (renamed) failed"
  fi
else
  # Two directory loads: DDL first, then data
  if [ "$SOURCE_DB_NAME" = "$TARGET_DB_NAME" ]; then
    log "Loading DDL from ${DUMP_DIR_DDL}"
    mysqlsh --py --quiet-start=2 \
      --uri "${TARGET_DB_USER}:${TARGET_DB_PASSWORD}@${TARGET_DB_HOST}:${TARGET_DB_PORT}" \
      -e "opts={'threads':${LOAD_THREADS},'ignoreExistingObjects':False,'showProgress':True}; util.load_dump('${DUMP_DIR_DDL}',opts)" \
      || die "load_dump DDL failed"

    if [ "$PY_INCLUDED_LIST_EMPTY" = "0" ]; then
      log "Loading data from ${DUMP_DIR_DATA}"
      mysqlsh --py --quiet-start=2 \
        --uri "${TARGET_DB_USER}:${TARGET_DB_PASSWORD}@${TARGET_DB_HOST}:${TARGET_DB_PORT}" \
        -e "opts={'threads':${LOAD_THREADS},'deferTableIndexes':'${DEFER_INDEXES}','ignoreExistingObjects':${PY_IGNORE},'showProgress':True}; util.load_dump('${DUMP_DIR_DATA}',opts)" \
        || die "load_dump data failed"
    fi
  else
    log "Loading DDL (rename) from ${DUMP_DIR_DDL}"
    mysqlsh --py --quiet-start=2 \
      --uri "${TARGET_DB_USER}:${TARGET_DB_PASSWORD}@${TARGET_DB_HOST}:${TARGET_DB_PORT}" \
      -e "opts={'threads':${LOAD_THREADS},'ignoreExistingObjects':False,'showProgress':True,'schema':'${TARGET_DB_NAME}'}; util.load_dump('${DUMP_DIR_DDL}',opts)" \
      || die "load_dump DDL (rename) failed"

    if [ "$PY_INCLUDED_LIST_EMPTY" = "0" ]; then
      log "Loading data (rename) from ${DUMP_DIR_DATA}"
      mysqlsh --py --quiet-start=2 \
        --uri "${TARGET_DB_USER}:${TARGET_DB_PASSWORD}@${TARGET_DB_HOST}:${TARGET_DB_PORT}" \
        -e "opts={'threads':${LOAD_THREADS},'deferTableIndexes':'${DEFER_INDEXES}','ignoreExistingObjects':${PY_IGNORE},'showProgress':True,'schema':'${TARGET_DB_NAME}'}; util.load_dump('${DUMP_DIR_DATA}',opts)" \
        || die "load_dump data (rename) failed"
    fi
  fi
fi

# --- Restore local_infile if we enabled it ---
if [ "$ORIG_LOCAL_INFILE" != "1" ]; then
  log "Restoring local_infile=OFF after load"
  sql "SET GLOBAL local_infile=OFF;" || warn "Could not SET GLOBAL local_infile=OFF"
fi

# --- Remove dump dirs on success (guarded) ---
if [ "$(tolower "${KEEP_DUMP}")" != "true" ]; then
  for d in "$DUMP_DIR" "$DUMP_DIR_DDL" "$DUMP_DIR_DATA"; do
    [ -n "$d" ] || continue
    [ -d "$d" ] || continue
    case "$d" in
      ${LOCAL_DUMP_BASE%/}/${SOURCE_DB_NAME}_*)
        log "Removing dump directory: $d"
        rm -rf -- "$d" || warn "Failed to remove $d"
        ;;
      *) warn "Refusing to delete unexpected dump path: $d" ;;
    esac
  done
fi

log "Done. Source '${SOURCE_DB_NAME}' -> '${TARGET_DB_NAME}'."
