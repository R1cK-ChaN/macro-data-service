#!/usr/bin/env bash
# Wrapper for scripts/release_aware_refresh.py invoked from the
# systemd --user timer (issue #130).

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LOCK_FILE="${REPO_ROOT}/.macro-data/calendar_recurring.lock"
RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%S+00:00")"

mkdir -p "$(dirname "$LOCK_FILE")"

cd "$REPO_ROOT"
exec flock --wait 120 "$LOCK_FILE" \
    python3 scripts/release_aware_refresh.py --now "$RUN_STARTED_AT" "$@"
