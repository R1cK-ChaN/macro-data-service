#!/usr/bin/env bash
# Wrapper for scripts/backfill_fundamentals.py invoked from the
# systemd --user timer (issue #68 slice 3).
#
# Responsibilities:
#   * flock guard — overlapping daily fires cannot race the engine DB.
#   * cd into the repo so .macro-data/, .env, and scripts/ resolve.
#   * exec the python entry-point so the systemd unit's MainPID
#     tracks Python directly (clean exit-code reporting).

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LOCK_FILE="${REPO_ROOT}/.macro-data/fundamentals_recurring.lock"

mkdir -p "$(dirname "$LOCK_FILE")"

cd "$REPO_ROOT"
exec flock --nonblock "$LOCK_FILE" \
    python3 scripts/backfill_fundamentals.py --execute "$@"
