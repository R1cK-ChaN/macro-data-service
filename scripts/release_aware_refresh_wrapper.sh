#!/usr/bin/env bash
# Wrapper for scripts/release_aware_refresh.py invoked from the
# systemd --user timer (issue #130).

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LOCK_FILE="${REPO_ROOT}/.macro-data/calendar_recurring.lock"
RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%S+00:00")"

mkdir -p "$(dirname "$LOCK_FILE")"

cd "$REPO_ROOT"
PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
[ -x "$PYTHON_BIN" ] || PYTHON_BIN="python3"
exec flock --wait 120 "$LOCK_FILE" \
    "$PYTHON_BIN" scripts/release_aware_refresh.py --now "$RUN_STARTED_AT" "$@"
