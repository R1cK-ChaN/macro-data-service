#!/usr/bin/env bash
# Wrapper for scripts/backup/restore_drill.sh from systemd
# macro-data-restore-drill.service.
#
# On failure, file a `data-quality` GitHub issue so a broken backup
# pipeline surfaces in the same place as ingestion DQ misses.

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}"
LOCK_FILE="${REPO_ROOT}/.macro-data/restore_drill.lock"
mkdir -p "$(dirname "$LOCK_FILE")"

cd "$REPO_ROOT"

set +e
flock --nonblock --conflict-exit-code 0 "$LOCK_FILE" \
    "$REPO_ROOT/scripts/backup/restore_drill.sh" "$@"
RC=$?
set -e

if (( RC != 0 )); then
    TITLE="restore drill failed $(date -u +%F)"
    LOG_TAIL="$(journalctl --user -u macro-data-restore-drill.service -n 80 --no-pager 2>/dev/null || true)"
    BODY=$(cat <<EOF
\`macro-data-restore-drill.service\` returned $RC at $(date -u +%FT%TZ).

Likely culprits: B2 fetch failed, archive corrupt, SQLite integrity
check failed, ClickHouse archive missing metadata/. The drill is
read-only — a failure means the most recent backup is not recoverable
without manual intervention. Re-run after fixing:

\`\`\`
$REPO_ROOT/scripts/backup/restore_drill_wrapper.sh
\`\`\`

\`\`\`
$LOG_TAIL
\`\`\`

Auto-filed by restore_drill_wrapper.sh — issue #136.
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
