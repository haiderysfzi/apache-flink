#!/usr/bin/env bash
# Start (or converge) the whole stack and make sure the job is running. Idempotent.
#
#   scripts/up.sh                    start; restore from pending savepoint if one exists, else fresh state
#   scripts/up.sh --from-checkpoint  after a crash/unclean stop: restore from newest retained checkpoint
#   scripts/up.sh --fresh            ignore any savepoint/checkpoint (WILL reprocess from scratch)
source "$(dirname "$0")/lib.sh"
require_docker
acquire_lock

mode="auto"
for a in "$@"; do
  case "$a" in
    --from-checkpoint) mode="checkpoint" ;;
    --fresh) mode="fresh" ;;
    -h|--help) sed -n '2,7p' "$0"; exit 0 ;;
    *) die "unknown option: $a" ;;
  esac
done

[ -f "$JAR_LOCAL" ] || { log "no jar yet - building"; build_jar ""; }

log "starting containers (waits for healthchecks, 240s max)..."
dc up -d --build --wait --wait-timeout 240

wait_for_jm 120
wait_for_slots 120

if [ -n "$(running_job_id)" ]; then
  log "job already running ($(running_job_id)) - nothing to submit."
  exit 0
fi

restore=""
case "$mode" in
  fresh) log "--fresh: not restoring any state" ;;
  checkpoint)
    restore="$(latest_checkpoint)"
    [ -n "$restore" ] || die "no retained checkpoint found"
    restore="file://$restore" ;;
  auto)
    if [ -s "$PENDING_SP" ]; then restore="$(cat "$PENDING_SP")"; fi ;;
esac

submit_job "$restore"
jid="$(wait_job_running 120)" || die "job did not reach RUNNING. See: make logs / $JM_URL"
rm -f "$PENDING_SP"
log "job RUNNING: $jid   UI: $JM_URL"
