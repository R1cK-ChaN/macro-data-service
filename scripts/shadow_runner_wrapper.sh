#!/usr/bin/env bash
# Wrapper for shadow_runner.py invoked from systemd --user timer.
#
# Always passes --once: the script's internal sleep loop is for daemon
# use; under systemd, the timer drives cadence and each firing is a
# single cycle that exits cleanly.
#
# Responsibilities:
#   * flock guard — overlapping fires cannot race the engine DB.
#   * cd into the repo so .macro-data/, .env, and scripts/ resolve.
#   * exec python so the systemd unit's MainPID tracks the entry-point
#     directly (clean exit-code reporting).
#
# Override REPO_ROOT to point at a different checkout (smoke-testing
# the timer in a worktree). The default is the directory containing
# this script's parent.

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LOCK_FILE="${REPO_ROOT}/.macro-data/shadow_digest.lock"

mkdir -p "$(dirname "$LOCK_FILE")"

cd "$REPO_ROOT"
PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
[ -x "$PYTHON_BIN" ] || PYTHON_BIN="python3"
exec flock --nonblock "$LOCK_FILE" \
    "$PYTHON_BIN" shadow_runner.py --once "$@"
