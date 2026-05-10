#!/usr/bin/env bash
# Wrapper for scripts/calendar_corp_forward.py invoked from the
# systemd --user timer (issue #63).
#
# Responsibilities:
#   * flock guard — overlapping daily fires cannot race the engine DB.
#   * cd into the repo so .macro-data/, .env, and scripts/ resolve.
#   * exec the python entry-point so the systemd unit's MainPID
#     tracks Python directly (clean exit-code reporting).

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
# Shared with calendar_sweep_values_wrapper.sh and
# calendar_refresh_schedules_wrapper.sh — all three jobs write the
# engine DB; one lock serialises them so SQLite write contention does
# not surface as transient connector failures.
LOCK_FILE="${REPO_ROOT}/.macro-data/calendar_recurring.lock"

mkdir -p "$(dirname "$LOCK_FILE")"

cd "$REPO_ROOT"
PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
[ -x "$PYTHON_BIN" ] || PYTHON_BIN="python3"
exec flock --nonblock "$LOCK_FILE" \
    "$PYTHON_BIN" scripts/calendar_corp_forward.py "$@"
