#!/usr/bin/env bash
# sync_to_b2.sh — encrypted off-site sync to Backblaze B2 (issue #136).
#
# Streams /var/lib/macro-data/backups/{sqlite,clickhouse}/<date>/ to a
# B2 bucket via rclone's crypt remote. The crypt remote sits on top of
# a raw B2 remote so files are encrypted client-side before they leave
# the host. Bucket lifecycle on the B2 side enforces 30-day retention
# for daily/. On the 1st of each month the day's snapshots are also
# copied to monthly/<YYYY-MM>/, which is kept indefinitely.
#
# Required env (sourced from /etc/macro-data/.env in production):
#   B2_ACCOUNT_ID, B2_APPLICATION_KEY,
#   RCLONE_CRYPT_PASSWORD          (already 'rclone obscure'd — see runbook)
# Optional:
#   B2_BUCKET                      bucket name      (default macro-data-backups)
#   BACKUP_LOCAL_ROOT              local source dir (default /var/lib/macro-data/backups)
#
# Usage:
#   sync_to_b2.sh [--date YYYY-MM-DD]

set -euo pipefail

DATE="$(date -u +%F)"
SRC="${BACKUP_LOCAL_ROOT:-/var/lib/macro-data/backups}"
BUCKET="${B2_BUCKET:-macro-data-backups}"

while (( $# )); do
    case "$1" in
        --date) DATE="$2"; shift 2;;
        -h|--help) sed -n '2,21p' "$0"; exit 0;;
        *) echo "unknown arg: $1" >&2; exit 2;;
    esac
done

: "${B2_ACCOUNT_ID:?B2_ACCOUNT_ID not set}"
: "${B2_APPLICATION_KEY:?B2_APPLICATION_KEY not set}"
: "${RCLONE_CRYPT_PASSWORD:?RCLONE_CRYPT_PASSWORD not set (run 'rclone obscure' first)}"

# rclone reads RCLONE_CONFIG_<remote>_<key> for env-var-driven remotes,
# so we don't need a config file on disk. Two remotes: 'b2raw' (the
# bucket) and 'backups' (crypt over b2raw).
export RCLONE_CONFIG_B2RAW_TYPE=b2
export RCLONE_CONFIG_B2RAW_ACCOUNT="$B2_ACCOUNT_ID"
export RCLONE_CONFIG_B2RAW_KEY="$B2_APPLICATION_KEY"
export RCLONE_CONFIG_B2RAW_HARD_DELETE=false

export RCLONE_CONFIG_BACKUPS_TYPE=crypt
export RCLONE_CONFIG_BACKUPS_REMOTE="b2raw:$BUCKET"
export RCLONE_CONFIG_BACKUPS_PASSWORD="$RCLONE_CRYPT_PASSWORD"
# Encrypt file CONTENTS only, leave the path/filename in plaintext on
# B2. With default crypt settings the daily/ + monthly/ keys would be
# scrambled too, defeating bucket lifecycle rules that match prefix.
# Backup file names ('engine.db', 'market.tar.gz', date dirs) are not
# sensitive on their own; the data inside still ships encrypted.
export RCLONE_CONFIG_BACKUPS_FILENAME_ENCRYPTION=off
export RCLONE_CONFIG_BACKUPS_DIRECTORY_NAME_ENCRYPTION=false

# Daily push: copy today's per-date dirs into daily/. We use copy not
# sync — local /var/lib retention is shorter (7d) than remote (30d), so
# remote files older than 7d must NOT be deleted by the local side.
RCLONE_FLAGS=(--transfers=4 --checkers=8 --fast-list)

if [[ -d "$SRC/sqlite/$DATE" ]]; then
    rclone copy "$SRC/sqlite/$DATE" "backups:daily/sqlite/$DATE" "${RCLONE_FLAGS[@]}"
else
    echo "warn: no sqlite snapshot at $SRC/sqlite/$DATE" >&2
fi

if [[ -d "$SRC/clickhouse/$DATE" ]]; then
    rclone copy "$SRC/clickhouse/$DATE" "backups:daily/clickhouse/$DATE" "${RCLONE_FLAGS[@]}"
else
    echo "warn: no clickhouse snapshot at $SRC/clickhouse/$DATE" >&2
fi

# Monthly archive — never expired by lifecycle.
DAY=$(date -u -d "$DATE" +%d)
if [[ "$DAY" == "01" ]]; then
    YYYYMM=$(date -u -d "$DATE" +%Y-%m)
    [[ -d "$SRC/sqlite/$DATE" ]] && \
        rclone copy "$SRC/sqlite/$DATE" \
            "backups:monthly/$YYYYMM/sqlite/$DATE" "${RCLONE_FLAGS[@]}"
    [[ -d "$SRC/clickhouse/$DATE" ]] && \
        rclone copy "$SRC/clickhouse/$DATE" \
            "backups:monthly/$YYYYMM/clickhouse/$DATE" "${RCLONE_FLAGS[@]}"
fi
