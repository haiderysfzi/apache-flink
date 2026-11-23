#!/usr/bin/env bash
# Graceful shutdown. Savepoints the job first, then stops containers. KEEPS all data volumes.
# Bring it back with scripts/up.sh (restores from the savepoint taken here).
#
#   scripts/down.sh          graceful stop, data kept
#   scripts/down.sh --purge  ALSO delete volumes (Postgres, ClickHouse, checkpoints, savepoints). Asks first.
source "$(dirname "$0")/lib.sh"
require_docker
acquire_lock

purge=0
for a in "$@"; do
  case "$a" in
    --purge) purge=1 ;;
    -h|--help) sed -n '2,7p' "$0"; exit 0 ;;
    *) die "unknown option: $a" ;;
  esac
done

if [ "$purge" = 1 ]; then
  echo "This will PERMANENTLY DELETE these docker volumes of this project:"
  dc config --volumes | sed 's/^/   - /'
  printf "Type 'delete-all-data' to continue: "
  read -r reply
  [ "$reply" = "delete-all-data" ] || die "aborted, nothing deleted"
  dc down -v -t 60
  rm -f "$PENDING_SP" "$SP_HISTORY"
  log "stack and data removed."
  exit 0
fi

if jm_up; then
  jid="$(running_job_id)"
  if [ -n "$jid" ]; then
    stop_with_savepoint "$jid" >/dev/null \
      || die "could not savepoint the job; refusing to stop containers. Use scripts/restart.sh --force or stop manually."
  fi
fi

dc stop -t 60
log "stopped. Data kept. Resume with: scripts/up.sh"
