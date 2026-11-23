#!/usr/bin/env bash
# Build artifacts/job.jar inside Docker (no local JDK/Maven needed). Does NOT touch the running job.
# Usage: scripts/build.sh [--skip-tests]
source "$(dirname "$0")/lib.sh"
require_docker
extra=""
[ "${1:-}" = "--skip-tests" ] && extra="-DskipTests"
build_jar "$extra"
