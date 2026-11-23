#!/usr/bin/env bash
# Rebuild + redeploy new code with state preserved.
#   1. build the new jar while the OLD job keeps running (build failure => nothing changes)
#   2. savepoint + stop old job
#   3. start new jar from that savepoint
#
#   scripts/rebuild.sh               code change
#   scripts/rebuild.sh --skip-tests
#   scripts/rebuild.sh --image       also rebuild the Flink docker image and recreate flink containers
#
# Changed the job graph (removed/renamed an operator uid)? Savepoint restore will refuse; either
# keep uids stable, or consciously run with ALLOW_NON_RESTORED_STATE=1 scripts/rebuild.sh
source "$(dirname "$0")/lib.sh"
require_docker

skip=""; image=0
for a in "$@"; do
  case "$a" in
    --skip-tests) skip="-DskipTests" ;;
    --image) image=1 ;;
    -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
    *) die "unknown option: $a" ;;
  esac
done

build_jar "$skip"            # fails -> script exits (set -e), running job untouched

if [ "$image" = 1 ]; then
  log "rebuilding flink image"
  dc build --pull jobmanager
  exec "$ROOT/scripts/restart.sh" --cluster
fi
exec "$ROOT/scripts/restart.sh"
