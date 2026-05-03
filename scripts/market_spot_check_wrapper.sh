#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LOCK_FILE="${REPO_ROOT}/.macro-data/market_spot_check.lock"

mkdir -p "$(dirname "$LOCK_FILE")"
cd "$REPO_ROOT"
exec flock --nonblock "$LOCK_FILE" \
    python3 scripts/spot_check_close.py "$@"
