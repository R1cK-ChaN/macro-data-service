#!/usr/bin/env bash
# Wrapper for scripts/parity_daily.py invoked from systemd --user timer.
#
# Responsibilities:
#   * flock guard — two instances cannot race the engine DB.
#   * cd into the repo so .macro-data/, .env, and scripts/ resolve.
#   * exec the python entry-point so the systemd unit's MainPID
#     tracks Python directly (clean exit-code reporting).
#
# Override REPO_ROOT to point at a different checkout (smoke-testing
# the timer in a worktree). The default is the directory containing
# this script's parent.

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LOCK_FILE="${REPO_ROOT}/.macro-data/parity_daily.lock"

mkdir -p "$(dirname "$LOCK_FILE")"

cd "$REPO_ROOT"
exec flock --nonblock "$LOCK_FILE" \
    python3 scripts/parity_daily.py "$@"
