#!/usr/bin/env bash
# snapshot_sqlite.sh — online SQLite snapshot of engine.db (issue #136).
#
# Uses Python's sqlite3 .backup API so the snapshot is consistent even
# while a writer holds the WAL — a plain shutil.copy / cp could capture
# pages mid-flight. Same pattern as parity_daily.py:_refresh_backup_snapshot.
#
# Usage:
#   snapshot_sqlite.sh [--date YYYY-MM-DD] [--src PATH] [--dest-root PATH]
#
# Env (lower priority than flags):
#   ANALYST_MACRO_DATA_DB_PATH    source engine.db   (default /var/lib/macro-data/engine.db)
#   BACKUP_LOCAL_ROOT             backup dir root    (default /var/lib/macro-data/backups)
#
# Stdout: absolute path of the written snapshot.

set -euo pipefail

DATE="$(date -u +%F)"
SRC="${ANALYST_MACRO_DATA_DB_PATH:-/var/lib/macro-data/engine.db}"
DEST_ROOT="${BACKUP_LOCAL_ROOT:-/var/lib/macro-data/backups}"

while (( $# )); do
    case "$1" in
        --date) DATE="$2"; shift 2;;
        --src)  SRC="$2";  shift 2;;
        --dest-root) DEST_ROOT="$2"; shift 2;;
        -h|--help) sed -n '2,15p' "$0"; exit 0;;
        *) echo "unknown arg: $1" >&2; exit 2;;
    esac
done

[[ -f "$SRC" ]] || { echo "source DB not found: $SRC" >&2; exit 1; }

DEST_DIR="$DEST_ROOT/sqlite/$DATE"
DEST="$DEST_DIR/engine.db"
mkdir -p "$DEST_DIR"

PYTHON_BIN=""
for c in "/home/data/macro-data-service/.venv/bin/python" "$(command -v python3)"; do
    if [[ -x "$c" ]]; then PYTHON_BIN="$c"; break; fi
done
[[ -n "$PYTHON_BIN" ]] || { echo "python3 not found" >&2; exit 1; }

"$PYTHON_BIN" - "$SRC" "$DEST" <<'PY'
import sqlite3, sys
src, dest = sys.argv[1], sys.argv[2]
s = sqlite3.connect(src)
try:
    d = sqlite3.connect(dest)
    try:
        s.backup(d)
    finally:
        d.close()
finally:
    s.close()
PY

echo "$DEST"
