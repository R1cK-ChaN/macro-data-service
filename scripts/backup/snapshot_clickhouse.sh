#!/usr/bin/env bash
# snapshot_clickhouse.sh — ClickHouse BACKUP DATABASE → tar.gz (issue #136).
#
# Drives clickhouse-client BACKUP into the pre-configured 'backup' disk,
# then tars + gzips the result for off-site upload. Assumes the disk is
# defined in /etc/clickhouse-server/config.d/backups.xml — see runbook
# docs/runbooks/backup_b2.md for the one-time CH config.
#
# Usage:
#   snapshot_clickhouse.sh [--date YYYY-MM-DD] [--db NAME] [--dest-root PATH]
#
# Env:
#   CLICKHOUSE_DATABASE   source DB name             (default market)
#   BACKUP_LOCAL_ROOT     backup dir root            (default /var/lib/macro-data/backups)
#   CH_BACKUP_DISK        ClickHouse disk name       (default backup)
#   CH_DISK_PATH          filesystem path of disk    (default /var/lib/clickhouse/backups)
#
# Stdout: absolute path of the tar.gz archive.

set -euo pipefail

DATE="$(date -u +%F)"
DB="${CLICKHOUSE_DATABASE:-market}"
DEST_ROOT="${BACKUP_LOCAL_ROOT:-/var/lib/macro-data/backups}"
CH_BACKUP_DISK="${CH_BACKUP_DISK:-backup}"
CH_DISK_PATH="${CH_DISK_PATH:-/var/lib/clickhouse/backups}"

while (( $# )); do
    case "$1" in
        --date) DATE="$2"; shift 2;;
        --db)   DB="$2";   shift 2;;
        --dest-root) DEST_ROOT="$2"; shift 2;;
        -h|--help) sed -n '2,18p' "$0"; exit 0;;
        *) echo "unknown arg: $1" >&2; exit 2;;
    esac
done

DEST_DIR="$DEST_ROOT/clickhouse/$DATE"
mkdir -p "$DEST_DIR"

# Inside ClickHouse the BACKUP target is keyed by Disk('<name>', '<rel-path>').
# Remove any prior partial run on the same date so BACKUP doesn't refuse.
SRC_PATH="$CH_DISK_PATH/$DATE/$DB"
[[ -d "$SRC_PATH" ]] && rm -rf "$SRC_PATH"

clickhouse-client --query \
    "BACKUP DATABASE \`$DB\` TO Disk('$CH_BACKUP_DISK', '$DATE/$DB')"

[[ -d "$SRC_PATH" ]] || { echo "expected ClickHouse backup at $SRC_PATH" >&2; exit 1; }

DEST="$DEST_DIR/$DB.tar.gz"
tar -C "$CH_DISK_PATH/$DATE" -czf "$DEST" "$DB"
rm -rf "$CH_DISK_PATH/$DATE"

echo "$DEST"
