#!/usr/bin/env bash
# fast-copydb.sh (v3.0 — Python-only mysqlsh; portable Linux/macOS)
# Usage: ./fast-copydb.sh /path/to/server.cfg
set -Eeuo pipefail

# Quiet mysqlsh locale warnings
export LC_ALL=C LANG=C LANGUAGE=C

log(){ printf "\033[1;34m[fast-copydb]\033[0m %s\n" "$*" >&2; }
warn(){ printf "\033[1;33m[fast-copydb WARNING]\033[0m %s\n" "$*" >&2; }
die(){ printf "\033[1;31m[fast-copydb ERROR]\033[0m %s\n" "$*" >&2; exit 1; }
require_cmd(){ command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"; }
tolower(){ printf '%s' "$1" | tr '[:upper:]' '[:lower:]'; }

# Find a free port (portable; uses $RANDOM + lsof)
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
DUMP_DIR="${LOCAL_DUMP_BASE%/}/${SOURCE_DB_NAME}_${TIMESTAMP}"

# Threads/knobs (portable CPU count)
if command -v nproc >/dev/null 2>&1; then CPU_THREADS="$(nproc)"; elif command -v sysctl >/dev/null 2>&1; then CPU_THREADS="$(sysctl -n hw.ncpu 2>/dev/null || echo 4)"; else CPU_THREADS=4; fi
DUMP_THREADS="${DUMP_THREADS:-$CPU_THREADS}"
LOAD_THREADS="${LOAD_THREADS:-$CPU_THREADS}"
DUMP_COMPRESSION="${DUMP_COMPRESSION:-zstd}"     # zstd|gzip|none
DEFER_INDEXES="${DEFER_INDEXES:-all}"            # none|fulltext|secondary|all
IGNORE_EXISTING="${IGNORE_EXISTING:-true}"       # true|false
KEEP_DUMP="${KEEP_DUMP:-false}"                  # delete dump after success?
DROP_TARGET_DATABASE_BEFORE_LOAD="${DROP_TARGET_DATABASE_BEFORE_LOAD:-true}"
TARGET_DB_CHARSET="${TARGET_DB_CHARSET:-}"
TARGET_DB_COLLATION="${TARGET_DB_COLLATION:-}"

# --- SSH tunnel to SOURCE ---
PORT="$(find_free_port)" || die "No free local port for SSH tunnel"
SSH_OPTS=( -p "$SSH_PORT" -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=60 -o StrictHostKeyChecking="${SSH_STRICT_HOST_KEY_CHECKING}" )
[ -n "$SSH_IDENTITY_FILE" ] && SSH_OPTS+=( -i "$SSH_IDENTITY_FILE" )

log "Opening SSH tunnel: localhost:${PORT} -> ${REMOTE_DB_HOST}:${REMOTE_DB_PORT} via ${REMOTE_SSH_USER}@${REMOTE_HOST}"
ssh -fN -L "${PORT}:${REMOTE_DB_HOST}:${REMOTE_DB_PORT}" "${SSH_OPTS[@]}" "${REMOTE_SSH_USER}@${REMOTE_HOST}" || die "SSH tunnel failed"
TUNNEL_PID="$(pgrep -f "ssh .*${PORT}:${REMOTE_DB_HOST}:${REMOTE_DB_PORT}.*${REMOTE_SSH_USER}@${REMOTE_HOST}" | head -n1 || true)"
cleanup(){ if [ -n "${TUNNEL_PID:-}" ] && ps -p "$TUNNEL_PID" >/dev/null 2>&1; then log "Closing SSH tunnel (pid $TUNNEL_PID)"; kill "$TUNNEL_PID" || true; fi; }
trap cleanup EXIT

# --- DUMP with mysqlsh --py ---
rm -rf -- "$DUMP_DIR" 2>/dev/null || true
if [ "$SOURCE_DB_NAME" = "$TARGET_DB_NAME" ]; then
  log "Dumping schema '${SOURCE_DB_NAME}' to ${DUMP_DIR}"
  mysqlsh --py --quiet-start=2 \
    --uri "${REMOTE_DB_USER}:${REMOTE_DB_PASSWORD}@127.0.0.1:${PORT}" \
    -e "opts={'threads':${DUMP_THREADS},'consistent':True,'compression':'${DUMP_COMPRESSION}','showProgress':True}; util.dump_schemas(['${SOURCE_DB_NAME}'],'${DUMP_DIR}',opts)" \
    || die "dump_schemas failed"
else
  log "Renaming schema (${SOURCE_DB_NAME} -> ${TARGET_DB_NAME}); dumping all tables"
  mysqlsh --py --quiet-start=2 \
    --uri "${REMOTE_DB_USER}:${REMOTE_DB_PASSWORD}@127.0.0.1:${PORT}" \
    -e "opts={'all':True,'threads':${DUMP_THREADS},'consistent':True,'compression':'${DUMP_COMPRESSION}','showProgress':True}; util.dump_tables('${SOURCE_DB_NAME}',[],'${DUMP_DIR}',opts)" \
    || die "dump_tables failed"
fi
[ -d "$DUMP_DIR" ] || die "Dump finished but dump dir not found: $DUMP_DIR"
log "Dump complete at: $DUMP_DIR"

# --- TARGET: Docker port published? ---
HOST_BIND="$(docker port "${TARGET_DOCKER_CONTAINER}" 3306/tcp 2>/dev/null || true)"
[ -n "$HOST_BIND" ] || die "Target port 3306 not published. Start container with -p HOSTPORT:3306."
TARGET_DB_HOST="${TARGET_DB_HOST:-127.0.0.1}"
TARGET_DB_PORT="${TARGET_DB_PORT:-${HOST_BIND##*:}}"
log "Target MySQL via ${TARGET_DB_HOST}:${TARGET_DB_PORT}"

# --- SQL helper (safe piping; no shell quoting issues) ---
sql(){
  printf '%s\n' "$1" | env LC_ALL=C LANG=C \
    mysqlsh --sql --quiet-start=2 \
      --uri "${TARGET_DB_USER}:${TARGET_DB_PASSWORD}@${TARGET_DB_HOST}:${TARGET_DB_PORT}"
}

# --- Ensure local_infile ON during load ---
ORIG_LOCAL_INFILE="$(sql "SELECT @@GLOBAL.local_infile;" 2>/dev/null | tail -n1 | tr -dc '0-9' || true)"; [ -z "$ORIG_LOCAL_INFILE" ] && ORIG_LOCAL_INFILE="0"
if [ "$ORIG_LOCAL_INFILE" != "1" ]; then
  log "Enabling local_infile=ON for load (was ${ORIG_LOCAL_INFILE})"
  sql "SET GLOBAL local_infile=ON;" || warn "Could not SET GLOBAL local_infile=ON (privileges?)"
fi

# --- Drop/Create target schema before load ---
if [ "$(tolower "${DROP_TARGET_DATABASE_BEFORE_LOAD}")" = "true" ]; then
  if [ "$SOURCE_DB_NAME" = "$TARGET_DB_NAME" ]; then
    log "Dropping existing schema $(qi "$SOURCE_DB_NAME") on target"
    sql "DROP DATABASE IF EXISTS $(qi "$SOURCE_DB_NAME");"
    # schema dump contains its own CREATE DATABASE
  else
    log "Dropping and re-creating schema $(qi "$TARGET_DB_NAME") on target"
    ddl="DROP DATABASE IF EXISTS $(qi "$TARGET_DB_NAME"); CREATE DATABASE $(qi "$TARGET_DB_NAME")"
    [ -n "$TARGET_DB_CHARSET" ]   && ddl="${ddl} CHARACTER SET ${TARGET_DB_CHARSET}"
    [ -n "$TARGET_DB_COLLATION" ] && ddl="${ddl} COLLATE ${TARGET_DB_COLLATION}"
    ddl="${ddl};"
    sql "$ddl"
  fi
fi

# --- LOAD with mysqlsh --py ---
PY_IGNORE=$( [ "$(tolower "$IGNORE_EXISTING")" = "true" ] && echo True || echo False )
if [ "$SOURCE_DB_NAME" = "$TARGET_DB_NAME" ]; then
  log "Starting parallel load into '${TARGET_DB_NAME}' from ${DUMP_DIR}"
  mysqlsh --py --quiet-start=2 \
    --uri "${TARGET_DB_USER}:${TARGET_DB_PASSWORD}@${TARGET_DB_HOST}:${TARGET_DB_PORT}" \
    -e "opts={'threads':${LOAD_THREADS},'deferTableIndexes':'${DEFER_INDEXES}','ignoreExistingObjects':${PY_IGNORE},'showProgress':True}; util.load_dump('${DUMP_DIR}',opts)" \
    || die "load_dump failed"
else
  log "Starting parallel load (rename to '${TARGET_DB_NAME}') from ${DUMP_DIR}"
  mysqlsh --py --quiet-start=2 \
    --uri "${TARGET_DB_USER}:${TARGET_DB_PASSWORD}@${TARGET_DB_HOST}:${TARGET_DB_PORT}" \
    -e "opts={'threads':${LOAD_THREADS},'deferTableIndexes':'${DEFER_INDEXES}','ignoreExistingObjects':${PY_IGNORE},'showProgress':True,'schema':'${TARGET_DB_NAME}'}; util.load_dump('${DUMP_DIR}',opts)" \
    || die "load_dump (renamed schema) failed"
fi

# --- Restore local_infile if we enabled it ---
if [ "$ORIG_LOCAL_INFILE" != "1" ]; then
  log "Restoring local_infile=OFF after load"
  sql "SET GLOBAL local_infile=OFF;" || warn "Could not SET GLOBAL local_infile=OFF"
fi

# --- Remove dump on success (guarded) ---
if [ "$(tolower "${KEEP_DUMP}")" != "true" ]; then
  if [ -n "$DUMP_DIR" ] && [ -d "$DUMP_DIR" ]; then
    case "$DUMP_DIR" in
      ${LOCAL_DUMP_BASE%/}/${SOURCE_DB_NAME}_*)
        log "Removing dump directory: $DUMP_DIR"
        rm -rf -- "$DUMP_DIR" || warn "Failed to remove $DUMP_DIR"
        ;;
      *) warn "Refusing to delete unexpected dump path: $DUMP_DIR" ;;
    esac
  fi
fi

log "Done. Source '${SOURCE_DB_NAME}' -> '${TARGET_DB_NAME}'."
