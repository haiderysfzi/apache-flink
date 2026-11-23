#!/usr/bin/env bash
# One-screen health summary. Read-only.
source "$(dirname "$0")/lib.sh"
require_docker

echo "== containers =="; dc ps --format 'table {{.Service}}\t{{.State}}\t{{.Health}}\t{{.Ports}}'
echo
echo "== flink =="
if jm_up; then
  jm_get /overview | jq -r '"taskmanagers=\(.taskmanagers) slots=\(.["slots-total"]) free=\(.["slots-available"]) running_jobs=\(.["jobs-running"]) failed=\(.["jobs-failed"])"'
  jm_get /jobs/overview | jq -r '.jobs[] | "\(.jid)  \(.state)  \(.name)"'
else
  echo "JobManager REST not reachable at $JM_URL"
fi
echo
echo "== state =="
[ -s "$PENDING_SP" ] && echo "PENDING savepoint (next up.sh restores it): $(cat "$PENDING_SP")" || echo "no pending savepoint"
[ -f "$SP_HISTORY" ] && { echo "last savepoints:"; tail -3 "$SP_HISTORY"; }
echo
"$ROOT/scripts/verify.sh" || true
