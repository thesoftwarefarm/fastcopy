#!/usr/bin/env bash
# fastcopy-multi.sh — orchestrator: clone a landlord DB + all tenant DBs
# Discovers tenants via SQL on the landlord, then invokes ./fastcopy.sh per DB.
# Sequential, continue-on-failure; prints a summary table; exits 1 if any failed.
set -Eeuo pipefail
export LC_ALL=C LANG=C LANGUAGE=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FASTCOPY="${SCRIPT_DIR}/fastcopy.sh"

log(){  printf "\033[1;34m[fast-copydb-multi]\033[0m %s\n" "$*" >&2; }
warn(){ printf "\033[1;33m[fast-copydb-multi WARNING]\033[0m %s\n" "$*" >&2; }
die(){  printf "\033[1;31m[fast-copydb-multi ERROR]\033[0m %s\n" "$*" >&2; exit 1; }
tolower(){ printf '%s' "$1" | tr '[:upper:]' '[:lower:]'; }
require_cmd(){ command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"; }

require_cmd ssh; require_cmd mysqlsh; require_cmd docker; require_cmd mktemp
[ -x "$FASTCOPY" ] || die "fastcopy.sh not found or not executable at $FASTCOPY"

# --- cfg ---
CFG="${1:-}"; [ -n "$CFG" ] && [ -f "$CFG" ] || die "Usage: ./fastcopy-multi.sh /path/to/multi.cfg"
set -a
# shellcheck source=/dev/null
. "$CFG"
set +a

: "${REMOTE_HOST:?}"; : "${REMOTE_SSH_USER:?}"
: "${REMOTE_DB_USER:?}"; : "${REMOTE_DB_PASSWORD:?}"
: "${LANDLORD_DB_NAME:?}"; : "${TENANT_DISCOVERY_QUERY:?}"
: "${TARGET_DOCKER_CONTAINER:?}"; : "${TARGET_DB_USER:?}"; : "${TARGET_DB_PASSWORD:?}"

REMOTE_DB_HOST="${REMOTE_DB_HOST:-127.0.0.1}"
REMOTE_DB_PORT="${REMOTE_DB_PORT:-3306}"
SSH_PORT="${SSH_PORT:-22}"
SSH_IDENTITY_FILE="${SSH_IDENTITY_FILE:-}"
SSH_STRICT_HOST_KEY_CHECKING="${SSH_STRICT_HOST_KEY_CHECKING:-yes}"
TARGET_DB_PREFIX="${TARGET_DB_PREFIX:-}"
CLONE_WITH_TIMESTAMP="${CLONE_WITH_TIMESTAMP:-false}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

TMP_CFGS=()
TUNNEL_PID=""

# --- helpers ---
find_free_port(){
  local tries=300 port
  for _ in $(seq 1 $tries); do
    port=$(( 24000 + (RANDOM % 6000) ))
    lsof -i TCP:${port} -sTCP:LISTEN -nP >/dev/null 2>&1 || { echo "$port"; return 0; }
  done
  return 1
}

cleanup(){
  if [ -n "${TUNNEL_PID:-}" ] && ps -p "$TUNNEL_PID" >/dev/null 2>&1; then
    log "Closing discovery tunnel (pid $TUNNEL_PID)"
    kill "$TUNNEL_PID" || true
  fi
  if [ ${#TMP_CFGS[@]} -gt 0 ]; then
    rm -f "${TMP_CFGS[@]}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# --- discovery tunnel ---
PORT="$(find_free_port)" || die "No free local port for SSH tunnel"
SSH_OPTS=( -p "$SSH_PORT" -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=60 -o StrictHostKeyChecking="${SSH_STRICT_HOST_KEY_CHECKING}" )
[ -n "$SSH_IDENTITY_FILE" ] && SSH_OPTS+=( -i "$SSH_IDENTITY_FILE" )

log "Opening discovery SSH tunnel: localhost:${PORT} -> ${REMOTE_DB_HOST}:${REMOTE_DB_PORT} via ${REMOTE_SSH_USER}@${REMOTE_HOST}"
ssh -fN -L "${PORT}:${REMOTE_DB_HOST}:${REMOTE_DB_PORT}" "${SSH_OPTS[@]}" "${REMOTE_SSH_USER}@${REMOTE_HOST}" || die "SSH tunnel failed"
TUNNEL_PID="$(pgrep -f "ssh .*${PORT}:${REMOTE_DB_HOST}:${REMOTE_DB_PORT}.*${REMOTE_SSH_USER}@${REMOTE_HOST}" | head -n1 || true)"

# --- tenant discovery ---
discover_tenant_dbs(){
  local raw rc=0
  raw="$(printf '%s\n' "$TENANT_DISCOVERY_QUERY" \
    | mysqlsh --sql --quiet-start=2 \
        --uri "${REMOTE_DB_USER}:${REMOTE_DB_PASSWORD}@127.0.0.1:${PORT}/${LANDLORD_DB_NAME}")" \
    || rc=$?
  if [ $rc -ne 0 ]; then
    die "Tenant discovery query failed (mysqlsh exit $rc). Check TENANT_DISCOVERY_QUERY — reserved words like 'database', 'order', 'group' must be backticked: SELECT \`database\` FROM tenants ..."
  fi
  local out
  out="$(printf '%s\n' "$raw" | awk '
        BEGIN{IGNORECASE=1}
        /^[[:space:]]*$/ {next}
        /^[-+|]/ {next}
        NR==1 && NF==1 {next}
        {print $1}
      ')"
  TENANT_DBS=()
  while IFS= read -r name; do
    [ -n "$name" ] || continue
    printf '%s' "$name" | grep -Eq '^[A-Za-z0-9_$]+$' \
      || { warn "Skipping invalid tenant DB name: $name"; continue; }
    TENANT_DBS+=("$name")
  done <<EOF
$out
EOF
}

discover_tenant_dbs
log "Discovered ${#TENANT_DBS[@]} tenant DB(s)"

# --- option resolution (bash 3.2 safe; avoids ${!var} under set -u) ---
resolve(){   # SCOPE NAME DEFAULT -> stdout value
  local scope="$1" name="$2" default="$3" v
  eval "v=\${${scope}_${name}-__UNSET__}"
  if [ "$v" = "__UNSET__" ]; then eval "v=\${${name}-$default}"; fi
  printf '%s' "$v"
}

derive_target_name(){   # SOURCE_NAME -> stdout TARGET
  local out="${TARGET_DB_PREFIX}${1}"
  [ "$(tolower "$CLONE_WITH_TIMESTAMP")" = "true" ] && out="${out}_${TIMESTAMP}"
  printf '%s' "$out"
}

# --- per-clone driver ---
RESULTS=()

run_one(){   # role source
  local role="$1" src="$2"
  local tgt ex di ie kd dropdb dthreads lthreads dc charset coll dbase
  local tmpcfg t0 dt rc=0

  tgt="$(derive_target_name "$src")"
  ex=$(resolve "$role" EXCLUDE_TABLES_DATA "")
  di=$(resolve "$role" DEFER_INDEXES "all")
  ie=$(resolve "$role" IGNORE_EXISTING "true")
  kd="${KEEP_DUMP:-false}"
  dropdb="${DROP_TARGET_DATABASE_BEFORE_LOAD:-true}"
  dthreads="${DUMP_THREADS:-}"
  lthreads="${LOAD_THREADS:-}"
  dc="${DUMP_COMPRESSION:-zstd}"
  charset="${TARGET_DB_CHARSET:-}"
  coll="${TARGET_DB_COLLATION:-}"
  dbase="${LOCAL_DUMP_BASE:-/tmp}"

  tmpcfg="$(mktemp -t fastcopy-multi.XXXXXX)"
  TMP_CFGS+=("$tmpcfg")

  {
    printf 'REMOTE_HOST=%q\n'                      "$REMOTE_HOST"
    printf 'REMOTE_SSH_USER=%q\n'                  "$REMOTE_SSH_USER"
    printf 'SSH_PORT=%q\n'                         "$SSH_PORT"
    [ -n "$SSH_IDENTITY_FILE" ] && printf 'SSH_IDENTITY_FILE=%q\n' "$SSH_IDENTITY_FILE"
    printf 'SSH_STRICT_HOST_KEY_CHECKING=%q\n'     "$SSH_STRICT_HOST_KEY_CHECKING"
    printf 'REMOTE_DB_HOST=%q\n'                   "$REMOTE_DB_HOST"
    printf 'REMOTE_DB_PORT=%q\n'                   "$REMOTE_DB_PORT"
    printf 'REMOTE_DB_USER=%q\n'                   "$REMOTE_DB_USER"
    printf 'REMOTE_DB_PASSWORD=%q\n'               "$REMOTE_DB_PASSWORD"
    printf 'SOURCE_DB_NAME=%q\n'                   "$src"
    printf 'TARGET_DOCKER_CONTAINER=%q\n'          "$TARGET_DOCKER_CONTAINER"
    printf 'TARGET_DB_USER=%q\n'                   "$TARGET_DB_USER"
    printf 'TARGET_DB_PASSWORD=%q\n'               "$TARGET_DB_PASSWORD"
    printf 'TARGET_DB_NAME=%q\n'                   "$tgt"
    printf 'CLONE_WITH_TIMESTAMP=false\n'
    printf 'EXCLUDE_TABLES_DATA=%q\n'              "$ex"
    printf 'DEFER_INDEXES=%q\n'                    "$di"
    printf 'IGNORE_EXISTING=%q\n'                  "$ie"
    printf 'KEEP_DUMP=%q\n'                        "$kd"
    printf 'DROP_TARGET_DATABASE_BEFORE_LOAD=%q\n' "$dropdb"
    [ -n "$dthreads" ] && printf 'DUMP_THREADS=%q\n' "$dthreads"
    [ -n "$lthreads" ] && printf 'LOAD_THREADS=%q\n' "$lthreads"
    printf 'DUMP_COMPRESSION=%q\n'                 "$dc"
    [ -n "$charset" ] && printf 'TARGET_DB_CHARSET=%q\n' "$charset"
    [ -n "$coll" ]    && printf 'TARGET_DB_COLLATION=%q\n' "$coll"
    printf 'LOCAL_DUMP_BASE=%q\n'                  "$dbase"
  } > "$tmpcfg"

  log "[$role] cloning $src -> $tgt"
  t0=$(date +%s)
  "$FASTCOPY" "$tmpcfg" || rc=$?
  dt=$(( $(date +%s) - t0 ))
  if [ $rc -eq 0 ]; then
    RESULTS+=("$src|OK|$dt")
  else
    RESULTS+=("$src|FAIL|$dt")
    warn "[$role] clone failed: $src (exit $rc)"
  fi
}

# --- run landlord, then each tenant ---
run_one LANDLORD "$LANDLORD_DB_NAME"
for t in "${TENANT_DBS[@]}"; do
  run_one TENANT "$t"
done

# --- summary ---
fails=0
printf '\n%-40s %-6s %8s\n' "DATABASE" "STATUS" "SECONDS"
printf '%-40s %-6s %8s\n' "----------------------------------------" "------" "--------"
for r in "${RESULTS[@]}"; do
  IFS='|' read -r db st sec <<<"$r"
  printf '%-40s %-6s %8s\n' "$db" "$st" "$sec"
  [ "$st" = "FAIL" ] && fails=$((fails+1))
done

echo
if [ $fails -gt 0 ]; then
  log "$fails clone(s) failed"
  exit 1
fi
log "All clones completed successfully"
