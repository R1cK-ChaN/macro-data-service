#!/usr/bin/env bash
# daily_backup.sh — chain the backup pipeline (issue #136).
#
# Order: snapshot_sqlite → snapshot_clickhouse → sync_to_b2 → prune local
# beyond BACKUP_LOCAL_RETENTION_DAYS. Any non-zero exit aborts the chain
# and the wrapper above us files a data-quality issue.

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}"
DATE="${BACKUP_DATE:-$(date -u +%F)}"
LOCAL_ROOT="${BACKUP_LOCAL_ROOT:-/var/lib/macro-data/backups}"
RETENTION_DAYS="${BACKUP_LOCAL_RETENTION_DAYS:-7}"

cd "$REPO_ROOT"

# Pull secrets so rclone env vars resolve. systemd already injects this
# via EnvironmentFile= but a manual run from a terminal needs it too.
ENV_FILE="${MACRO_DATA_ENV_FILE:-/etc/macro-data/.env}"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

scripts/backup/snapshot_sqlite.sh --date "$DATE"
scripts/backup/snapshot_clickhouse.sh --date "$DATE"
scripts/backup/sync_to_b2.sh --date "$DATE"

# Local retention: drop per-date subdirs older than the window. Each
# date dir is mtime-stamped on creation, so -mtime is the simplest gate.
for kind in sqlite clickhouse; do
    base="$LOCAL_ROOT/$kind"
    [[ -d "$base" ]] || continue
    find "$base" -mindepth 1 -maxdepth 1 -type d -mtime "+$RETENTION_DAYS" \
        -exec rm -rf {} +
done

echo "daily_backup ok: $DATE"
