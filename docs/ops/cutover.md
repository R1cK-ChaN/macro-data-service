# Cold migration: laptop → data VPS (issue #140)

One-shot procedure to move 823 MB local `engine.db` (272k+ `cal_econ_raw`
rows, 551k+ `indicator_vintages`, 27k+ `news_articles`, …) and the local
ClickHouse `market` database to the data VPS. Bronze raw responses cannot
be re-fetched (upstreams don't keep snapshots), so the cutover preserves
data identity rather than re-deriving it from upstream.

`scripts/migrate/cutover.sh` orchestrates the move; this doc is the
operator checklist around it.

---

## Prereqs

Verify each before running:

| Check | Command | Expected |
| --- | --- | --- |
| VPS bootstrap done (#133) | `ssh data@vl 'systemctl is-active clickhouse-server'` | `active` |
| VPS API service installed (#137) | `ssh data@vl 'systemctl --user is-enabled macro-data-api.service'` | `enabled` |
| VPS backup pipeline installed (#136) | `ssh data@vl 'systemctl --user list-timers macro-data-backup.timer'` | timer listed |
| VPS writer timers NOT yet enabled | `ssh data@vl 'systemctl --user list-unit-files \*.timer --state=enabled'` | macro-data-backup only |
| ClickHouse versions compatible | `docker exec macro-data-clickhouse clickhouse-client -q 'SELECT version()'` vs `ssh data@vl clickhouse-client -q 'SELECT version()'` | same major.minor |
| SSH alias works | `ssh data@vl 'echo ok'` | `ok` |
| VPS disk free | `ssh data@vl 'df -h /var/lib'` | ≥ 5 GB free |

If VPS clickhouse-server is on a different major than local, **stop here**
— file-copy migration assumes binary on-disk format compatibility within
the same major. Either pin both to the same image or use `BACKUP/RESTORE`
manually (see Troubleshooting below).

---

## Execution

```bash
# Stage the run (no mutations, prints planned phases):
scripts/migrate/cutover.sh --dry-run

# Real run. Will prompt for confirmation before stopping local writers.
scripts/migrate/cutover.sh
```

Phases (each gated on the previous; failure exits non-zero):

1. **preflight** — SSH reachable, docker running, disk space ok, version compat, operator confirm.
2. **stop_writers** — `systemctl --user disable --now` each writer timer; verify lock files released.
3. **snapshot_sqlite** — `sqlite3.backup` API → `/tmp/cutover-<date>/sqlite/engine.db`.
4. **snapshot_ch** — `docker stop`, `tar czf` the data dir, `docker start`. Brief CH downtime (~30 s) on the local laptop.
5. **local_baseline** — `cutover_baseline.py` against the snapshots → `local-baseline.json` (per-table COUNT + SQLite file sha256 + CH per-table sum-of-cityHash64).
6. **upload** — `rsync` the bundle + the baseline tool itself to `data@vl:/var/lib/macro-data/migrate/<date>/`.
7. **vps_restore** — SSH session: stop API, mv existing engine.db to `.pre-migrate-<date>.bak`, copy the snapshot in; stop clickhouse-server, mv `/var/lib/clickhouse` to `.pre-migrate-<date>.bak`, untar, chown, start.
8. **vps_baseline** — same `cutover_baseline.py` run on the VPS against the live restored DBs.
9. **compare** — `diff -u local-baseline.json vps-baseline.json`. Identical → success; any diff → exit 1 with a written `baseline.diff` and the rollback command sequence.

The script prints the exact rollback shell on mismatch.

---

## Verification

After the script reports success, smoke-test from the operator laptop:

```bash
# 1. API liveness.
curl -fsS https://<vps-host>/healthz

# 2. Authenticated read against a real route.
curl -fsS -H "X-API-Key: $TOKEN" https://<vps-host>/v1/manifest | jq

# 3. Bronze counts match the issue's acceptance bar.
ssh data@vl "sqlite3 /var/lib/macro-data/engine.db 'SELECT COUNT(*) FROM cal_econ_raw'"  # ≥ 272000
ssh data@vl "clickhouse-client -q 'SELECT count() FROM market.bars_1d'"                    # = local
```

Then trigger the first VPS backup (#136 acceptance):

```bash
ssh data@vl 'systemctl --user start macro-data-backup.service'
ssh data@vl 'journalctl --user -u macro-data-backup.service -n 80 --no-pager'
ssh data@vl 'rclone lsf backups:daily/sqlite/'   # encrypted SQLite snapshot for today
```

Once green for 24 h, enable VPS writer timers per
`scripts/systemd/README.md`.

---

## Rollback

The cutover never touches local DBs after the initial snapshot, and on
the VPS it preserves the prior state at:

- `/var/lib/macro-data/engine.db.pre-migrate-<date>.bak` (plus any
  `engine.db-wal.pre-migrate-<date>.bak` / `engine.db-shm.pre-migrate-<date>.bak` sidecars)
- `/var/lib/clickhouse.pre-migrate-<date>.bak`

Roll back any time before VPS writers fire:

```bash
ssh data@vl '
  systemctl --user stop macro-data-api
  sudo systemctl stop clickhouse-server
  sudo rm -f /var/lib/macro-data/engine.db /var/lib/macro-data/engine.db-wal /var/lib/macro-data/engine.db-shm
  for ext in db db-wal db-shm; do
      if [[ -f "/var/lib/macro-data/engine.$ext.pre-migrate-<date>.bak" ]]; then
          sudo mv "/var/lib/macro-data/engine.$ext.pre-migrate-<date>.bak" \
                  "/var/lib/macro-data/engine.$ext"
      fi
  done
  sudo rm -rf /var/lib/clickhouse && sudo mv /var/lib/clickhouse.pre-migrate-<date>.bak /var/lib/clickhouse
  sudo systemctl start clickhouse-server
  systemctl --user start macro-data-api
'

# Re-enable local writers (matches cutover.sh's LOCAL_WRITER_TIMERS list).
for t in macro-data-refresh.timer parity-daily.timer calendar-schedule-refresh.timer \
         calendar-value-sweep.timer calendar-corp-forward.timer \
         macro-data-release-watch.timer fundamentals-forward.timer \
         macro-data-market-refresh.timer macro-data-market-self-heal.timer \
         macro-data-market-spot-check.timer shadow-digest.timer data-quality-daily.timer \
         macro-data-backup.timer; do
    systemctl --user enable --now "$t" 2>/dev/null || true
done
```

Once VPS writers HAVE fired and produced new rows, rollback is no longer
clean — those rows would be lost. The cutover sequence is designed so
verification (steps 8–9) gates writer re-enablement; do not enable VPS
writers until baselines match.

---

## Cleanup (after 24 h green)

```bash
ssh data@vl '
  sudo rm -rf /var/lib/macro-data/engine.db.pre-migrate-<date>.bak \
              /var/lib/clickhouse.pre-migrate-<date>.bak \
              /var/lib/macro-data/migrate/<date>
'
rm -rf /tmp/cutover-<date>
```

---

## Out of scope

- Periodic / streaming replication — that lives in #136.
- Live cutover with zero downtime — not worth the build cost for a 1-person operation.
- Migrating `.macro-data/logs/`, `.macro-data/backfill_cursor.json`, lock files, or other runtime state. The VPS regenerates these.
- Migrating `.macro-data/backups/te_calendar_*` snapshots — superseded by the `te_calendar_*` tables that already live in `engine.db`.

---

## Troubleshooting

**`stop_writers` reports lock still held.** A long-running ingestion
job is mid-flight. Wait for it to finish (`ps aux | grep python3 | grep
macro`), then re-run.

**`docker stop macro-data-clickhouse` hangs.** A long query is in
flight. `docker exec macro-data-clickhouse clickhouse-client --query
'KILL QUERY WHERE 1=1 ASYNC'`, then retry.

**Baselines diverge in `clickhouse.hashes` only.** ReplicatedMergeTree
or ReplacingMergeTree parts can have different physical row sets
representing the same logical state. If counts match and `sum(cityHash64(*))`
diverges, query a few specific rows by primary key to compare. If
those match, it's a part-merge artifact; rerun on the VPS after `OPTIMIZE
TABLE … FINAL` and re-baseline.

**Cross-major ClickHouse versions.** The file-copy path in `cutover.sh`
assumes the binary on-disk format is compatible. If versions diverge,
do `BACKUP/RESTORE` manually:

```bash
# Local (one-time prep): bind-mount a config.d entry for the backup disk,
# recreate container, then:
docker exec macro-data-clickhouse clickhouse-client --query \
    "BACKUP DATABASE market TO Disk('backup', 'cutover/')"
# Tarball the host-side path (the bind-mount surfaces it),
# rsync to VPS, then:
ssh data@vl 'clickhouse-client --query \
    "RESTORE DATABASE market FROM Disk(\"backup\", \"cutover/\")"'
```

That detour skips `cutover.sh phase 4 + 7-CH`; everything else (SQLite
side, baselines, rollback) is reusable.
