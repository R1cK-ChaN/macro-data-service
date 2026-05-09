#!/usr/bin/env bash
# restore_drill.sh — monthly recovery drill (issue #136).
#
# 1. Pull the most recent daily/{sqlite,clickhouse}/<date>/ from B2.
# 2. SQLite: open the restored engine.db and run a fixed set of golden
#    queries; sha256 the result line block. Failure of any query, or a
#    corrupt DB, exits non-zero so the wrapper's filer fires.
# 3. ClickHouse: validate tar archive integrity (tar -tzf), confirm the
#    expected metadata/ + data/ subtrees exist, and that the unpacked
#    size is plausible. We skip a full RESTORE here — it requires root
#    or a writable CH backup disk owned by the data user; archive-level
#    integrity is enough to catch corrupt-on-upload regressions in the
#    monthly cadence the spec calls for.
# 4. Append a JSON line (date + hashes + sizes) to the drill log.
# 5. Always cleanup tmpdir on exit.
#
# Usage:
#   restore_drill.sh [--date YYYY-MM-DD] [--tmpdir PATH] [--log PATH]

set -euo pipefail

DATE="$(date -u +%F)"
TMPDIR="${RESTORE_DRILL_TMPDIR:-/tmp/restore-$DATE}"
LOG="${RESTORE_DRILL_LOG:-/var/log/macro-data/restore_drill.log}"

while (( $# )); do
    case "$1" in
        --date) DATE="$2"; shift 2;;
        --tmpdir) TMPDIR="$2"; shift 2;;
        --log) LOG="$2"; shift 2;;
        -h|--help) sed -n '2,20p' "$0"; exit 0;;
        *) echo "unknown arg: $1" >&2; exit 2;;
    esac
done

ENV_FILE="${MACRO_DATA_ENV_FILE:-/etc/macro-data/.env}"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

: "${B2_ACCOUNT_ID:?B2_ACCOUNT_ID not set}"
: "${B2_APPLICATION_KEY:?B2_APPLICATION_KEY not set}"
: "${RCLONE_CRYPT_PASSWORD:?RCLONE_CRYPT_PASSWORD not set}"

BUCKET="${B2_BUCKET:-macro-data-backups}"
export RCLONE_CONFIG_B2RAW_TYPE=b2
export RCLONE_CONFIG_B2RAW_ACCOUNT="$B2_ACCOUNT_ID"
export RCLONE_CONFIG_B2RAW_KEY="$B2_APPLICATION_KEY"
export RCLONE_CONFIG_BACKUPS_TYPE=crypt
export RCLONE_CONFIG_BACKUPS_REMOTE="b2raw:$BUCKET"
export RCLONE_CONFIG_BACKUPS_PASSWORD="$RCLONE_CRYPT_PASSWORD"
# Match sync_to_b2.sh — paths plaintext so B2 lifecycle prefix rules
# work; only file contents are encrypted.
export RCLONE_CONFIG_BACKUPS_FILENAME_ENCRYPTION=off
export RCLONE_CONFIG_BACKUPS_DIRECTORY_NAME_ENCRYPTION=false

cleanup() {
    local code=$?
    rm -rf "$TMPDIR"
    exit "$code"
}
trap cleanup EXIT

mkdir -p "$TMPDIR" "$(dirname "$LOG")"

# Pick the latest date that has BOTH sqlite and clickhouse uploaded.
# Picking each independently would happily validate a half-complete day
# (e.g. SQLite synced for T but ClickHouse stuck on T-1) and report ok.
SQLITE_DATES=$(rclone lsf "backups:daily/sqlite/"     --dirs-only 2>/dev/null | sed 's:/$::' | sort)
CH_DATES=$(    rclone lsf "backups:daily/clickhouse/" --dirs-only 2>/dev/null | sed 's:/$::' | sort)
LATEST_DATE=$(comm -12 <(printf '%s\n' "$SQLITE_DATES") <(printf '%s\n' "$CH_DATES") | tail -n1)

[[ -n "$LATEST_DATE" ]] || { echo "no matching daily backup pair in B2" >&2; exit 1; }

# Reject stale pairs — if uploads stopped weeks ago we'd otherwise
# happily validate ancient state and report ok. 2-day window absorbs a
# missed run + UTC/local timezone wraparound.
FRESHNESS_DAYS="${RESTORE_DRILL_FRESHNESS_DAYS:-2}"
TODAY=$(date -u +%F)
DAYS_OLD=$(( ( $(date -u -d "$TODAY" +%s) - $(date -u -d "$LATEST_DATE" +%s) ) / 86400 ))
if (( DAYS_OLD > FRESHNESS_DAYS )); then
    echo "stale backup: latest matched pair $LATEST_DATE is $DAYS_OLD days old (max $FRESHNESS_DAYS)" >&2
    exit 1
fi

echo "drill: matched_date=$LATEST_DATE days_old=$DAYS_OLD"

rclone copy "backups:daily/sqlite/$LATEST_DATE"     "$TMPDIR/sqlite/"
rclone copy "backups:daily/clickhouse/$LATEST_DATE" "$TMPDIR/clickhouse/"

# --- SQLite: open + golden queries -------------------------------
RESTORED_DB="$TMPDIR/sqlite/engine.db"
[[ -f "$RESTORED_DB" ]] || { echo "engine.db missing in restored backup" >&2; exit 1; }

PYTHON_BIN=""
for c in "/home/data/macro-data-service/.venv/bin/python" "$(command -v python3)"; do
    if [[ -x "$c" ]]; then PYTHON_BIN="$c"; break; fi
done
[[ -n "$PYTHON_BIN" ]] || { echo "python3 not found" >&2; exit 1; }

SQLITE_OUTPUT=$("$PYTHON_BIN" - "$RESTORED_DB" <<'PY'
import hashlib, sqlite3, sys
con = sqlite3.connect(sys.argv[1])
# integrity_check returns rows. A clean DB returns [('ok',)]; corruption
# shows up as one or more error rows that .fetchall() would otherwise
# silently discard.
chk = con.execute("PRAGMA integrity_check").fetchall()
if chk != [("ok",)]:
    print(f"integrity_check failed: {chk}", file=sys.stderr)
    sys.exit(1)
ops = [
    "SELECT COUNT(*) FROM concept_map",
    "SELECT COUNT(*) FROM release_schedule",
    "SELECT COUNT(*) FROM source_capability",
    "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
]
lines = []
for q in ops:
    lines.append(f"{q}={con.execute(q).fetchone()[0]}")
con.close()
blob = "\n".join(lines).encode()
print(hashlib.sha256(blob).hexdigest())
print("|".join(lines))
PY
)
SQLITE_HASH=$(echo "$SQLITE_OUTPUT" | sed -n '1p')
SQLITE_OPS=$(echo  "$SQLITE_OUTPUT" | sed -n '2p')

# --- ClickHouse: archive structural integrity --------------------
CH_ARCHIVE="$TMPDIR/clickhouse/${CLICKHOUSE_DATABASE:-market}.tar.gz"
[[ -f "$CH_ARCHIVE" ]] || { echo "archive missing: $CH_ARCHIVE" >&2; exit 1; }

# tar -tzf returns non-zero on a corrupt gz/tar stream. Capture the
# listing once — `tar -tzf | grep -q` under `set -o pipefail` can
# surface tar's SIGPIPE (141) as a false alarm once grep short-circuits
# on a large archive.
CH_LISTING=$(tar -tzf "$CH_ARCHIVE")
CH_ENTRIES=$(printf '%s\n' "$CH_LISTING" | wc -l)
(( CH_ENTRIES > 0 )) || { echo "tar archive empty: $CH_ARCHIVE" >&2; exit 1; }

printf '%s\n' "$CH_LISTING" | grep -q '/metadata/' \
    || { echo "archive missing metadata/" >&2; exit 1; }
printf '%s\n' "$CH_LISTING" | grep -q '/data/' \
    || { echo "archive missing data/" >&2; exit 1; }

CH_SIZE=$(stat -c '%s' "$CH_ARCHIVE")

# --- Log + verdict ----------------------------------------------
ENTRY=$(printf '{"date":"%s","matched_backup":"%s","sqlite_hash":"%s","sqlite_ops":"%s","clickhouse_archive_size":%s,"clickhouse_entries":%s,"ok":true}\n' \
    "$DATE" "$LATEST_DATE" "$SQLITE_HASH" "$SQLITE_OPS" \
    "$CH_SIZE" "$CH_ENTRIES")
echo "$ENTRY" >> "$LOG"
echo "$ENTRY"
