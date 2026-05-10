#!/usr/bin/env bash
# Wrapper for scripts/backup/daily_backup.sh from systemd
# macro-data-backup.service.
#
# Responsibilities:
#   * flock guard so a slow run + next-day fire don't overlap.
#   * On failure, file a `data-quality` GitHub issue so #102's filer
#     surfaces the breakage in the same place as ingestion DQ misses.
#   * Pass through the underlying exit code so systemd reports failed.

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}"
LOCK_FILE="${REPO_ROOT}/.macro-data/backup_daily.lock"
mkdir -p "$(dirname "$LOCK_FILE")"

cd "$REPO_ROOT"

set +e
flock --nonblock --conflict-exit-code 0 "$LOCK_FILE" \
    "$REPO_ROOT/scripts/backup/daily_backup.sh" "$@"
RC=$?
set -e

if (( RC != 0 )); then
    TITLE="backup pipeline failed $(date -u +%F)"
    LOG_TAIL="$(journalctl --user -u macro-data-backup.service -n 80 --no-pager 2>/dev/null || true)"
    BODY=$(cat <<EOF
\`macro-data-backup.service\` returned $RC at $(date -u +%FT%TZ).

Likely culprits: B2 unreachable, application key revoked, disk full
on /var/lib/macro-data/backups, or ClickHouse BACKUP refused. Check
/etc/macro-data/.env and re-run manually:

\`\`\`
$REPO_ROOT/scripts/backup/daily_backup_wrapper.sh
\`\`\`

\`\`\`
$LOG_TAIL
\`\`\`

Auto-filed by daily_backup_wrapper.sh — issue #136.
EOF
)
    # Use a dedicated label so the DQ filer's `data-quality`-keyed
    # selector (data_quality_filer.py:_list_open_data_quality_issue)
    # doesn't pick this issue up and start commenting on it.
    gh label create backup-failure --color FBCA04 \
        --description "Backup pipeline failure" 2>/dev/null || true
    gh issue create \
        --title "$TITLE" \
        --label backup-failure \
        --body "$BODY" >/dev/null 2>&1 \
        || echo "gh issue create failed; check creds + manual run" >&2
fi

exit "$RC"
