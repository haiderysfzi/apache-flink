#!/usr/bin/env bash
# Take a savepoint WITHOUT stopping the job (manual backup / pre-upgrade safety net).
source "$(dirname "$0")/lib.sh"
require_docker
jid="$(running_job_id)"; [ -n "$jid" ] || die "no running job"
sp="$(trigger_savepoint "$jid")" || die "savepoint failed"
log "savepoint: $sp"
echo "$sp"
