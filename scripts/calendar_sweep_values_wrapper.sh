#!/usr/bin/env bash
# Wrapper for scripts/calendar_sweep_values.py invoked from the
# systemd --user timer (issue #31 P1).
#
# Responsibilities:
#   * flock guard — overlapping hourly fires cannot race the engine DB.
#   * cd into the repo so .macro-data/, .env, and scripts/ resolve.
#   * exec the python entry-point so the systemd unit's MainPID
#     tracks Python directly (clean exit-code reporting).
#
# Override REPO_ROOT to point at a different checkout (smoke-testing
# the timer in a worktree). The default is the directory containing
# this script's parent.

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
# Shared with calendar_refresh_schedules_wrapper.sh — both jobs write
# the engine DB and a 04:15 sweep can otherwise overlap a still-running
# 04:00 refresh, surfacing `database is locked` ticks against the
# per-connector breakers. One lock serialises them.
LOCK_FILE="${REPO_ROOT}/.macro-data/calendar_recurring.lock"

mkdir -p "$(dirname "$LOCK_FILE")"

cd "$REPO_ROOT"
exec flock --nonblock "$LOCK_FILE" \
    python3 scripts/calendar_sweep_values.py "$@"
