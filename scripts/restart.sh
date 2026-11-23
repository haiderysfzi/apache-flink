#!/usr/bin/env bash
# SAFE restart: savepoint -> stop -> (optionally restart containers) -> resubmit from that savepoint.
# No data is lost or skipped: the new job continues exactly where the old one stopped.
#
#   scripts/restart.sh            restart the job only (cluster keeps running)
#   scripts/restart.sh --cluster  also restart jobmanager + taskmanager containers (DBs untouched)
#   scripts/restart.sh --force    job is wedged/failing so a savepoint is impossible: cancel it and
#                                 restore from the newest retained checkpoint instead
#
# If resubmit fails, the savepoint path stays in .state/pending-savepoint; fix the cause and run
# scripts/up.sh to resume from it.
source "$(dirname "$0")/lib.sh"
require_docker
acquire_lock

cluster=0; force=0
for a in "$@"; do
  case "$a" in
    --cluster) cluster=1 ;;
    --force) force=1 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) die "unknown option: $a" ;;
  esac
done

jid=""
jm_up && jid="$(running_job_id)"
restore=""

if [ -n "$jid" ]; then
  if restore="$(stop_with_savepoint "$jid")"; then
    :
  elif [ "$force" = 1 ]; then
    log "savepoint failed; --force: cancelling and falling back to latest checkpoint"
    jm timeout 60 flink cancel "$jid" || true
    cp_dir="$(latest_checkpoint)"
    [ -n "$cp_dir" ] || die "no retained checkpoint to restore from"
    restore="file://$cp_dir"
    echo "$restore" > "$PENDING_SP"
  else
    die "could not take a savepoint; job left RUNNING and untouched. Inspect $JM_URL, or use --force."
  fi
elif [ -s "$PENDING_SP" ]; then
  restore="$(cat "$PENDING_SP")"
  log "no running job; resuming from pending savepoint $restore"
else
  log "no running job and no pending savepoint - starting fresh"
fi

if [ "$cluster" = 1 ]; then
  log "restarting flink containers (graceful, 60s)..."
  dc restart -t 60 jobmanager taskmanager
  dc up -d --wait --wait-timeout 240 jobmanager taskmanager
fi

dc up -d --wait --wait-timeout 240 jobmanager taskmanager   # no-op if already up
wait_for_jm 120
wait_for_slots 120
submit_job "$restore"
new="$(wait_job_running 120)" || die "job did not reach RUNNING; savepoint kept in $PENDING_SP"
rm -f "$PENDING_SP"
log "restarted OK. job: $new"
