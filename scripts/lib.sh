#!/usr/bin/env bash
# Shared helpers. Source me; don't run me. Compatible with macOS bash 3.2.
#
# Safety principles baked in here:
#   * every network/wait call has a timeout - nothing blocks forever
#   * a job is only stopped via stop-with-savepoint; the savepoint path is persisted BEFORE anything
#     else happens and is only forgotten after the job is confirmed RUNNING again
#   * never delete volumes/state implicitly
#   * one lifecycle script at a time (lock)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ ! -f .env ]; then
  cp .env.example .env
  echo "[lib] created .env from .env.example (dev defaults)" >&2
fi
set -a; . ./.env; set +a

JOB_NAME="flink-template-job"
JM_URL="http://127.0.0.1:${FLINK_UI_PORT:-8081}"
STATE_DIR="$ROOT/.state"
PENDING_SP="$STATE_DIR/pending-savepoint"      # savepoint we must restore from next submit
SP_HISTORY="$STATE_DIR/savepoint-history.log"
JAR_LOCAL="$ROOT/artifacts/job.jar"
JAR_IN_CONTAINER="/opt/flink/jobs/job.jar"
SP_DIR_IN_CONTAINER="file:///opt/flink/savepoints"
PARALLELISM="${FLINK_PARALLELISM:-2}"
mkdir -p "$STATE_DIR"

dc()  { docker compose "$@"; }
jm()  { docker compose exec -T jobmanager "$@"; }
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

require_docker() {
  docker info >/dev/null 2>&1 || die "Docker daemon is not running. Start Docker Desktop and retry."
}

# ---- locking ---------------------------------------------------------------------------------
LOCK_DIR="$STATE_DIR/lock"
acquire_lock() {
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    echo $$ > "$LOCK_DIR/pid"
    trap 'rm -rf "$LOCK_DIR"' EXIT
  else
    local pid; pid="$(cat "$LOCK_DIR/pid" 2>/dev/null || echo '?')"
    if [ "$pid" != "?" ] && ! kill -0 "$pid" 2>/dev/null; then
      log "stale lock from dead pid $pid - removing"
      rm -rf "$LOCK_DIR"; acquire_lock; return
    fi
    die "another lifecycle script is running (pid $pid). Wait for it, or remove $LOCK_DIR if you are sure."
  fi
}

# ---- REST helpers (always bounded) ------------------------------------------------------------
jm_get() { curl -fsS --max-time 10 "$JM_URL$1"; }

jm_up() { jm_get /overview >/dev/null 2>&1; }

wait_for_jm() {
  local deadline=$(( $(date +%s) + ${1:-120} ))
  until jm_up; do
    [ "$(date +%s)" -lt "$deadline" ] || die "JobManager REST not reachable at $JM_URL"
    sleep 2
  done
}

wait_for_slots() {
  local deadline=$(( $(date +%s) + ${1:-120} )) n
  while :; do
    n="$(jm_get /overview 2>/dev/null | jq -r '.["slots-available"] // 0' 2>/dev/null || echo 0)"
    [ "${n:-0}" -gt 0 ] && return 0
    [ "$(date +%s)" -lt "$deadline" ] || die "no free TaskManager slots after waiting"
    sleep 2
  done
}

# prints job id of the RUNNING (or RESTARTING) job with our name, or nothing
running_job_id() {
  jm_get /jobs/overview 2>/dev/null \
    | jq -r --arg n "$JOB_NAME" '[.jobs[] | select(.name==$n and (.state=="RUNNING" or .state=="RESTARTING"))][0].jid // empty' \
    2>/dev/null || true
}

job_state() { jm_get "/jobs/$1" 2>/dev/null | jq -r '.state // "UNKNOWN"' 2>/dev/null || echo UNKNOWN; }

wait_job_running() {
  local deadline=$(( $(date +%s) + ${1:-120} )) jid
  while :; do
    jid="$(running_job_id)"
    if [ -n "$jid" ] && [ "$(job_state "$jid")" = "RUNNING" ]; then echo "$jid"; return 0; fi
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 2
  done
}

# ---- build -----------------------------------------------------------------------------------
build_jar() {  # $1 = extra maven args (e.g. -DskipTests)
  log "building fat jar in Docker (maven:3.9 / JDK 17)..."
  docker run --rm \
    -v "$ROOT":/app -v flink-template-m2:/root/.m2 -w /app \
    maven:3.9-eclipse-temurin-17 mvn -B -q ${1:-} package
  [ -f target/job.jar ] || die "build produced no target/job.jar"
  # atomic replace so a running submit never sees a half-written file
  cp target/job.jar artifacts/.job.jar.tmp && mv -f artifacts/.job.jar.tmp "$JAR_LOCAL"
  log "jar ready: artifacts/job.jar ($(du -h "$JAR_LOCAL" | cut -f1))"
}

# ---- savepoints / checkpoints -----------------------------------------------------------------
# Stop-with-savepoint: the only way we ever stop the job. Prints the savepoint path on stdout.
stop_with_savepoint() {  # $1 = job id
  local jid="$1" out sp
  log "stop-with-savepoint for job $jid (timeout 300s)..."
  if ! out="$(jm timeout 300 flink stop --savepointPath "$SP_DIR_IN_CONTAINER" "$jid" 2>&1)"; then
    printf '%s\n' "$out" >&2
    return 1
  fi
  sp="$(printf '%s\n' "$out" | sed -n 's/.*Path: *//p' | tail -1 | tr -d '\r')"
  [ -n "$sp" ] || { printf '%s\n' "$out" >&2; return 1; }
  # Persist IMMEDIATELY - if anything later fails, this is how we find our state.
  echo "$sp" > "$PENDING_SP"
  echo "$(date -u +%FT%TZ) $sp" >> "$SP_HISTORY"
  log "savepoint saved: $sp"
  echo "$sp"
}

trigger_savepoint() {  # job keeps running
  local jid="$1" out sp
  out="$(jm timeout 300 flink savepoint "$jid" "$SP_DIR_IN_CONTAINER" 2>&1)" || { printf '%s\n' "$out" >&2; return 1; }
  sp="$(printf '%s\n' "$out" | sed -n 's/.*Path: *//p' | tail -1 | tr -d '\r')"
  [ -n "$sp" ] || { printf '%s\n' "$out" >&2; return 1; }
  echo "$(date -u +%FT%TZ) $sp" >> "$SP_HISTORY"
  echo "$sp"
}

# newest retained checkpoint dir (survives cancel/crash because of RETAIN_ON_CANCELLATION)
latest_checkpoint() {
  jm sh -c 'ls -dt /opt/flink/checkpoints/*/chk-* 2>/dev/null | head -1' | tr -d '\r'
}

# ---- submit ----------------------------------------------------------------------------------
# submit_job [restore_path]
submit_job() {
  local restore="${1:-}"
  [ -f "$JAR_LOCAL" ] || die "no artifacts/job.jar - run scripts/build.sh"
  local args="-d -p $PARALLELISM"
  [ -n "$restore" ] && args="$args -s $restore"
  [ "${ALLOW_NON_RESTORED_STATE:-0}" = "1" ] && args="$args --allowNonRestoredState"
  log "submitting job (restore from: ${restore:-<fresh state>})"
  # shellcheck disable=SC2086
  jm flink run $args "$JAR_IN_CONTAINER"
}
