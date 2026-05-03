#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LOCK_FILE="${REPO_ROOT}/.macro-data/market_recurring.lock"

mkdir -p "$(dirname "$LOCK_FILE")"
cd "$REPO_ROOT"
exec flock --nonblock "$LOCK_FILE" \
    python3 scripts/market_daily_refresh.py "$@"
