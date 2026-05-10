#!/usr/bin/env bash
# cutover.sh — one-shot migration of local engine.db + ClickHouse 'market'
# database to the data VPS (issue #140).
#
# Phases (single-pass; either all succeed or operator follows rollback):
#   1. preflight       — ssh / docker / disk-space checks; interactive confirm
#   2. stop_writers    — disable local systemd --user writer timers; verify
#                        lock files released
#   3. snapshot_sqlite — sqlite3 .backup → /tmp/cutover-<date>/sqlite/
#   4. snapshot_ch     — docker stop CH, tar data dir, restart CH
#   5. local_baseline  — cutover_baseline.py against the snapshot
#   6. upload          — rsync snapshot bundle to VPS staging
#   7. vps_restore     — SSH: stop API, swap engine.db, swap CH data dir
#   8. vps_baseline    — cutover_baseline.py on VPS (live DBs)
#   9. compare         — diff baselines; succeed only on byte-equal
#
# Pre-existing files on the VPS are preserved as
# `<path>.pre-migrate-<date>.bak` so rollback is a single mv away.
#
# Usage:
#   scripts/migrate/cutover.sh [--vps data@vl] [--vps-staging /path] [--yes] [--dry-run]
#
# Required prereqs (see docs/ops/cutover.md):
#   - VPS bootstrap (#133), prod API service (#137), backup pipeline (#136) all in place.
#   - VPS writer timers NOT yet enabled — they should only fire after this cutover passes.
#   - Local + VPS ClickHouse versions compatible (same major, ideally same minor).

set -euo pipefail

VPS_HOST="${VPS_HOST:-data@vl}"
VPS_STAGING="${VPS_STAGING:-/var/lib/macro-data/migrate}"
DRY_RUN=0
ASSUME_YES=0
SKIP_STOP=0

while (( $# )); do
    case "$1" in
        --vps) VPS_HOST="$2"; shift 2;;
        --vps-staging) VPS_STAGING="$2"; shift 2;;
        --dry-run) DRY_RUN=1; shift;;
        --yes|-y) ASSUME_YES=1; shift;;
        --skip-stop-writers) SKIP_STOP=1; shift;;
        -h|--help) sed -n '2,30p' "$0"; exit 0;;
        *) echo "unknown arg: $1" >&2; exit 2;;
    esac
done

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DATE="$(date -u +%F)"
SNAPSHOT_DIR="/tmp/cutover-$DATE"
LOCAL_BASELINE="$SNAPSHOT_DIR/local-baseline.json"
VPS_BASELINE="$SNAPSHOT_DIR/vps-baseline.json"

cd "$REPO_ROOT"

# Local writer timers from scripts/systemd/. We disable rather than stop
# so a missed catch-up doesn't fire mid-cutover. The list mirrors
# scripts/systemd/README.md.
LOCAL_WRITER_TIMERS=(
    macro-data-refresh.timer
    parity-daily.timer
    calendar-schedule-refresh.timer
    calendar-value-sweep.timer
    calendar-corp-forward.timer
    macro-data-release-watch.timer
    fundamentals-forward.timer
    macro-data-market-refresh.timer
    macro-data-market-self-heal.timer
    macro-data-market-spot-check.timer
    shadow-digest.timer
    data-quality-daily.timer
    macro-data-backup.timer
)

log() { printf '[%s] cutover: %s\n' "$(date -u +%FT%TZ)" "$*"; }

run() {
    if (( DRY_RUN )); then
        printf '[dry-run] %s\n' "$*"
    else
        "$@"
    fi
}

confirm() {
    (( ASSUME_YES )) && return 0
    local prompt="$1"
    read -r -p "$prompt [y/N] " ans
    [[ "$ans" == "y" || "$ans" == "Y" ]]
}

# --------------------------------------------------------------------
# 1. preflight
# --------------------------------------------------------------------
phase_preflight() {
    log "preflight: ssh + docker + disk space"

    ssh -o ConnectTimeout=10 "$VPS_HOST" "echo ssh-ok" >/dev/null

    command -v rsync >/dev/null || { echo "rsync not installed locally" >&2; exit 1; }
    command -v docker >/dev/null || { echo "docker not installed locally" >&2; exit 1; }

    [[ -f .macro-data/engine.db ]] || { echo "local engine.db missing" >&2; exit 1; }
    docker ps --format '{{.Names}}' | grep -q '^macro-data-clickhouse$' \
        || { echo "macro-data-clickhouse container not running" >&2; exit 1; }

    local local_size_kb
    local_size_kb=$(du -sk .macro-data/engine.db .macro-data/clickhouse/data 2>/dev/null | awk '{s += $1} END {print s}')
    log "local snapshot footprint estimate: $((local_size_kb / 1024)) MB"

    local vps_avail_kb
    vps_avail_kb=$(ssh "$VPS_HOST" "df --output=avail -k /var/lib | tail -1")
    if (( vps_avail_kb < local_size_kb * 3 )); then
        echo "VPS disk too small: $((vps_avail_kb/1024)) MB free, need ~$((local_size_kb*3/1024)) MB" >&2
        exit 1
    fi
    log "vps free space ok: $((vps_avail_kb/1024)) MB"

    # ClickHouse version gate — file-copy across CH major.minor is unsafe.
    local local_v vps_v local_mm vps_mm
    local_v=$(docker exec macro-data-clickhouse clickhouse-client --query "SELECT version()" 2>/dev/null || echo "?")
    vps_v=$(ssh "$VPS_HOST" "clickhouse-client --query 'SELECT version()'" 2>/dev/null || echo "?")
    local_mm=$(printf '%s' "$local_v" | cut -d. -f1-2)
    vps_mm=$(printf  '%s' "$vps_v"   | cut -d. -f1-2)
    log "clickhouse versions: local=$local_v vps=$vps_v"
    if [[ "$local_mm" != "$vps_mm" ]]; then
        echo "clickhouse major.minor mismatch ($local_mm vs $vps_mm) — file-copy is unsafe; see cutover.md troubleshooting" >&2
        exit 1
    fi

    confirm "Cutover from $(hostname) → $VPS_HOST will stop local writers and overwrite VPS engine.db + ClickHouse market. Proceed?" \
        || { echo "aborted by operator"; exit 1; }
}

# --------------------------------------------------------------------
# 2. stop_writers
# --------------------------------------------------------------------
phase_stop_writers() {
    if (( SKIP_STOP )); then log "stop_writers: skipped (--skip-stop-writers)"; return; fi
    log "stopping local writer timers"
    for t in "${LOCAL_WRITER_TIMERS[@]}"; do
        run systemctl --user disable --now "$t" 2>/dev/null || true
    done
    # Lock files are released only when the holder exits. Give catch-up
    # fires a moment to drain.
    sleep 5
    for lock in .macro-data/*.lock; do
        [[ -f "$lock" ]] || continue
        if fuser "$lock" >/dev/null 2>&1; then
            echo "lock still held: $lock — investigate before retrying" >&2
            exit 1
        fi
    done
    log "writers stopped, locks released"
}

# --------------------------------------------------------------------
# 3. snapshot_sqlite — online .backup so the live DB stays consistent
# even if a writer reconnects between disable and snapshot.
# --------------------------------------------------------------------
phase_snapshot_sqlite() {
    log "snapshot SQLite engine.db"
    run mkdir -p "$SNAPSHOT_DIR/sqlite"
    if (( DRY_RUN )); then return; fi
    python3 - "$REPO_ROOT/.macro-data/engine.db" "$SNAPSHOT_DIR/sqlite/engine.db" <<'PY'
import sqlite3, sys
src, dst = sys.argv[1], sys.argv[2]
s = sqlite3.connect(src)
try:
    d = sqlite3.connect(dst)
    try:
        s.backup(d)
    finally:
        d.close()
finally:
    s.close()
PY
    log "wrote $SNAPSHOT_DIR/sqlite/engine.db ($(du -sh "$SNAPSHOT_DIR/sqlite/engine.db" | cut -f1))"
}

# --------------------------------------------------------------------
# 4. snapshot_ch — clean shutdown + tar the bind-mounted data dir, then
# restart. File-copy is what we ship to the VPS; cross-server portability
# requires same major.minor CH version (the runbook gates on this).
# --------------------------------------------------------------------
phase_snapshot_ch() {
    log "snapshot ClickHouse data dir (clean shutdown + tar)"
    run mkdir -p "$SNAPSHOT_DIR/clickhouse"

    run docker stop macro-data-clickhouse >/dev/null
    if ! (( DRY_RUN )); then
        # Cover Ctrl-C / tar-failure between stop and start so local
        # market reads recover even on aborted cutovers. Cleared after
        # the explicit start succeeds.
        trap 'docker start macro-data-clickhouse >/dev/null 2>&1 || true' EXIT
        tar -C .macro-data/clickhouse/data -czf "$SNAPSHOT_DIR/clickhouse/clickhouse-data.tar.gz" .
        trap - EXIT
    fi
    run docker start macro-data-clickhouse >/dev/null

    if (( DRY_RUN )); then return; fi
    # Wait for CH to come back up — local baseline below queries it.
    for _ in $(seq 1 30); do
        if docker exec macro-data-clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    log "wrote $SNAPSHOT_DIR/clickhouse/clickhouse-data.tar.gz ($(du -sh "$SNAPSHOT_DIR/clickhouse/clickhouse-data.tar.gz" | cut -f1))"
}

# --------------------------------------------------------------------
# 5. local_baseline — counts + content hashes against the snapshot
# (SQLite) and live CH (which is identical to the tarball, since CH was
# stopped during tar).
# --------------------------------------------------------------------
phase_local_baseline() {
    log "compute local baseline"
    if (( DRY_RUN )); then return; fi
    python3 scripts/migrate/cutover_baseline.py \
        --sqlite-db "$SNAPSHOT_DIR/sqlite/engine.db" \
        --clickhouse-via docker \
        > "$LOCAL_BASELINE"
    log "wrote $LOCAL_BASELINE ($(wc -c < "$LOCAL_BASELINE") bytes)"
}

# --------------------------------------------------------------------
# 6. upload — rsync the bundle + the baseline tool itself so we can
# rerun it on the VPS with identical logic.
# --------------------------------------------------------------------
phase_upload() {
    log "rsync snapshot bundle to $VPS_HOST:$VPS_STAGING/$DATE/"
    run ssh "$VPS_HOST" "sudo install -d -m 0755 -o data -g data $VPS_STAGING && sudo install -d -m 0755 -o data -g data $VPS_STAGING/$DATE"
    run rsync -av --info=progress2 "$SNAPSHOT_DIR/sqlite" "$SNAPSHOT_DIR/clickhouse" \
        "$VPS_HOST:$VPS_STAGING/$DATE/"
    run rsync -av scripts/migrate/cutover_baseline.py "$VPS_HOST:$VPS_STAGING/$DATE/"
}

# --------------------------------------------------------------------
# 7. vps_restore — stop API, swap engine.db, stop CH, untar data, start.
# Existing VPS files moved to .pre-migrate-<date>.bak for rollback.
# --------------------------------------------------------------------
phase_vps_restore() {
    log "VPS restore: stop API → swap engine.db → swap CH data → restart"
    if (( DRY_RUN )); then return; fi
    ssh "$VPS_HOST" "VPS_STAGING='$VPS_STAGING' DATE='$DATE' bash -se" <<'EOSSH'
set -euo pipefail

cd "$VPS_STAGING/$DATE"

# API is a `systemctl --user` unit owned by the data user we're SSHed
# in as; sudo would target the (non-existent) system unit.
systemctl --user stop macro-data-api.service 2>/dev/null || true

# --- SQLite ---
sudo install -d -m 0750 -o data -g data /var/lib/macro-data
# Move BOTH the main DB and any `-wal` / `-shm` sidecars into the
# rollback backup. Leaving stale sidecars next to the new engine.db
# corrupts SQLite's WAL view on first read.
for ext in db db-wal db-shm; do
    if [[ -f "/var/lib/macro-data/engine.$ext" ]]; then
        sudo mv "/var/lib/macro-data/engine.$ext" \
                "/var/lib/macro-data/engine.$ext.pre-migrate-$DATE.bak"
    fi
done
sudo cp sqlite/engine.db /var/lib/macro-data/engine.db
sudo chown data:data /var/lib/macro-data/engine.db
sudo chmod 600 /var/lib/macro-data/engine.db

# --- ClickHouse ---
sudo systemctl stop clickhouse-server.service
if [[ -d /var/lib/clickhouse ]]; then
    sudo mv /var/lib/clickhouse "/var/lib/clickhouse.pre-migrate-$DATE.bak"
fi
sudo install -d -m 0750 -o clickhouse -g clickhouse /var/lib/clickhouse
sudo tar -xzf clickhouse/clickhouse-data.tar.gz -C /var/lib/clickhouse
sudo chown -R clickhouse:clickhouse /var/lib/clickhouse
sudo systemctl start clickhouse-server.service

# Wait for CH ready.
for _ in $(seq 1 60); do
    if clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
clickhouse-client --query "SELECT 1" >/dev/null \
    || { echo "clickhouse-server failed to start" >&2; exit 1; }

# Restart API last; reads both DBs.
systemctl --user start macro-data-api.service
EOSSH
    log "VPS restore complete"
}

# --------------------------------------------------------------------
# 8. vps_baseline + 9. compare
# --------------------------------------------------------------------
phase_vps_baseline() {
    log "compute VPS baseline"
    if (( DRY_RUN )); then return; fi
    ssh "$VPS_HOST" \
        "python3 $VPS_STAGING/$DATE/cutover_baseline.py \
            --sqlite-db /var/lib/macro-data/engine.db \
            --clickhouse-via local" \
        > "$VPS_BASELINE"
}

phase_compare() {
    log "compare local vs VPS baselines"
    if (( DRY_RUN )); then return; fi
    if diff -u "$LOCAL_BASELINE" "$VPS_BASELINE" > "$SNAPSHOT_DIR/baseline.diff"; then
        log "BASELINE MATCH — cutover successful"
        rm "$SNAPSHOT_DIR/baseline.diff"
    else
        echo "BASELINE MISMATCH — see $SNAPSHOT_DIR/baseline.diff" >&2
        cat "$SNAPSHOT_DIR/baseline.diff" >&2
        cat >&2 <<EOR

Rollback on VPS (mirrors the swap path — API is a --user unit;
restore all sqlite sidecars not just engine.db):

  ssh $VPS_HOST '
      systemctl --user stop macro-data-api 2>/dev/null || true
      sudo systemctl stop clickhouse-server
      sudo rm -f /var/lib/macro-data/engine.db /var/lib/macro-data/engine.db-wal /var/lib/macro-data/engine.db-shm
      for ext in db db-wal db-shm; do
          if [[ -f "/var/lib/macro-data/engine.\$ext.pre-migrate-$DATE.bak" ]]; then
              sudo mv "/var/lib/macro-data/engine.\$ext.pre-migrate-$DATE.bak" \
                      "/var/lib/macro-data/engine.\$ext"
          fi
      done
      sudo rm -rf /var/lib/clickhouse
      sudo mv /var/lib/clickhouse.pre-migrate-$DATE.bak /var/lib/clickhouse
      sudo systemctl start clickhouse-server
      systemctl --user start macro-data-api
  '
EOR
        exit 1
    fi
}

# --------------------------------------------------------------------
# main
# --------------------------------------------------------------------
phase_preflight
phase_stop_writers
phase_snapshot_sqlite
phase_snapshot_ch
phase_local_baseline
phase_upload
phase_vps_restore
phase_vps_baseline
phase_compare

cat <<EONEXT
Cutover complete on $DATE.

Next steps (operator):
  1. Smoke-test API: curl -fsS https://<vps-host>/healthz
  2. Authenticated read from a real route (any of /v1/manifest, /v1/calendar, /v1/fundamentals/{ticker}, /v1/ops/{op}):
       curl -fsS -H "X-API-Key: \$TOKEN" https://<vps-host>/v1/manifest
  3. Trigger first VPS backup (from #136):
       ssh $VPS_HOST 'systemctl --user start macro-data-backup.service'
       ssh $VPS_HOST 'journalctl --user -u macro-data-backup.service -n 50'
  4. Enable VPS writer timers per scripts/systemd/README.md.
  5. After 24h of green ops, drop rollback artifacts:
       ssh $VPS_HOST 'sudo rm -rf /var/lib/macro-data/engine.db.pre-migrate-$DATE.bak /var/lib/clickhouse.pre-migrate-$DATE.bak $VPS_STAGING/$DATE'

Rollback (if any of 1-3 fails — VPS-side data is unchanged from cutover snapshot):
  ssh $VPS_HOST '
    systemctl --user stop macro-data-api 2>/dev/null || true
    sudo systemctl stop clickhouse-server
    sudo rm -f /var/lib/macro-data/engine.db /var/lib/macro-data/engine.db-wal /var/lib/macro-data/engine.db-shm
    for ext in db db-wal db-shm; do
        if [[ -f "/var/lib/macro-data/engine.\$ext.pre-migrate-$DATE.bak" ]]; then
            sudo mv "/var/lib/macro-data/engine.\$ext.pre-migrate-$DATE.bak" \
                    "/var/lib/macro-data/engine.\$ext"
        fi
    done
    sudo rm -rf /var/lib/clickhouse
    sudo mv /var/lib/clickhouse.pre-migrate-$DATE.bak /var/lib/clickhouse
    sudo systemctl start clickhouse-server
    systemctl --user start macro-data-api
  '
  Locally, re-enable writer timers:
    for t in ${LOCAL_WRITER_TIMERS[*]}; do systemctl --user enable --now \$t; done
EONEXT
