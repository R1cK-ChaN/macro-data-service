#!/usr/bin/env bash
# Wrapper for scripts/data_quality_daily.py invoked from systemd --user timer.
#
# Responsibilities:
#   * flock guard — overlapping fires cannot race the engine DB.
#   * cd into the repo so .macro-data/, .env, and scripts/ resolve.
#   * exec the python entry-point so the systemd unit's MainPID
#     tracks Python directly (clean exit-code reporting).
#
# Override REPO_ROOT to point at a different checkout (smoke-testing
# the timer in a worktree). The default is the directory containing
# this script's parent.

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LOCK_FILE="${REPO_ROOT}/.macro-data/data_quality_daily.lock"

mkdir -p "$(dirname "$LOCK_FILE")"

cd "$REPO_ROOT"
# Prefer the repo-local venv if present (VPS install pattern); fall back
# to system python3 for dev workstations that have deps available
# system-wide.
PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
[ -x "$PYTHON_BIN" ] || PYTHON_BIN="python3"
exec flock --nonblock "$LOCK_FILE" \
    "$PYTHON_BIN" scripts/data_quality_daily.py "$@"
