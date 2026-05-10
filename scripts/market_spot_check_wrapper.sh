#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LOCK_FILE="${REPO_ROOT}/.macro-data/market_spot_check.lock"

mkdir -p "$(dirname "$LOCK_FILE")"
cd "$REPO_ROOT"
PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
[ -x "$PYTHON_BIN" ] || PYTHON_BIN="python3"
exec flock --nonblock "$LOCK_FILE" \
    "$PYTHON_BIN" scripts/spot_check_close.py "$@"
