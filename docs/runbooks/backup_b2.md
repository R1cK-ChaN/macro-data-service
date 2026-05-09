# Backup + off-site replication runbook (issue #136)

End-to-end procedure for the full-DB backup pipeline:
SQLite `engine.db` + ClickHouse `market` → local `/var/lib/macro-data/backups/`
→ Backblaze B2 with client-side encryption via rclone.

Cadence:

| Job | Cadence | systemd unit |
| --- | --- | --- |
| Daily snapshot + B2 sync | 19:00 UTC (= 03:00 SGT) | `macro-data-backup.timer` |
| Monthly recovery drill   | 08:00 UTC, 1st of month | `macro-data-restore-drill.timer` |

Retention:

| Location | Retention | Mechanism |
| --- | --- | --- |
| `/var/lib/macro-data/backups/` | 7 days | local prune in `daily_backup.sh` |
| `b2://<bucket>/daily/`         | 30 days | B2 bucket lifecycle rule |
| `b2://<bucket>/monthly/`       | indefinite | written on day 1; lifecycle exempts `monthly/` |

---

## One-time setup

### 1. Backblaze B2

1. Sign up / sign in to Backblaze, B2 Cloud Storage.
2. Create a private bucket — default name `macro-data-backups`. Region
   choice does not matter much; pick the one nearest the VPS.
3. Lifecycle settings: keep the default "Keep all versions". B2 will
   not auto-delete anything. Manual lifecycle (below) handles the
   30-day daily/ rule.
4. Create an Application Key scoped to the bucket. Allowed operations
   need at minimum `listFiles`, `readFiles`, `writeFiles`, `deleteFiles`.
   Save the **keyID** (B2_ACCOUNT_ID) and **applicationKey** (B2_APPLICATION_KEY)
   — the application key is shown only once.
5. (Optional, recommended.) Add a bucket-level lifecycle rule so daily
   uploads expire after 30 days:
   - Match prefix: `daily/`
   - Hide files after 1 day, delete after 30 days.
   - Leave `monthly/` rule absent → never expires.

### 2. Crypt password

`rclone` cannot read a plaintext password from env; it requires the
output of `rclone obscure`. Generate it once on the VPS:

```bash
# Pick a strong password (≥32 random chars), then obscure it.
PLAINTEXT='<a very long random string — keep a secure copy>'
rclone obscure "$PLAINTEXT"
# output: <obscured-blob>  ← paste this into RCLONE_CRYPT_PASSWORD
```

Crypt config in this repo runs with `filename_encryption=off` so the
B2 object keys stay plaintext (e.g. `daily/sqlite/2026-05-09/engine.db`).
The bucket lifecycle prefix rule in step 1 only matches plaintext
prefixes, so encrypting the filenames would silently break retention.
File contents still ship encrypted; the filenames themselves are not
considered sensitive.

### 3. Populate `/etc/macro-data/.env`

```
B2_ACCOUNT_ID=<keyID from B2>
B2_APPLICATION_KEY=<applicationKey from B2>
B2_BUCKET=macro-data-backups
RCLONE_CRYPT_PASSWORD=<output of `rclone obscure '<plaintext>'`>
```

The wrapper sources `/etc/macro-data/.env` at run-time. Production
systemd units load it via `EnvironmentFile=`.

### 4. ClickHouse BACKUP disk

`snapshot_clickhouse.sh` calls `BACKUP DATABASE market TO Disk('backup', ...)`.
That disk has to be declared in ClickHouse server config:

```bash
sudo tee /etc/clickhouse-server/config.d/backups.xml >/dev/null <<'XML'
<clickhouse>
    <storage_configuration>
        <disks>
            <backup>
                <type>local</type>
                <path>/var/lib/clickhouse/backups/</path>
            </backup>
        </disks>
    </storage_configuration>
    <backups>
        <allowed_disk>backup</allowed_disk>
        <allowed_path>/var/lib/clickhouse/backups/</allowed_path>
    </backups>
</clickhouse>
XML

# The 'data' user runs the backup timer and needs to remove
# /var/lib/clickhouse/backups/<date>/ after tarring it up. Make the
# disk group-writable and add `data` to the clickhouse group so both
# users can stage + clean up there.
sudo install -d -m 0775 -o clickhouse -g clickhouse /var/lib/clickhouse/backups
sudo usermod -aG clickhouse data
# `data` must re-login (or `sudo -u data -i`) for the group membership
# to take effect for that user's processes.

sudo systemctl restart clickhouse-server
clickhouse-client --query "SELECT name FROM system.disks WHERE name='backup'"
```

### 5. Local backup root

```bash
sudo install -d -m 0750 -o data -g data /var/lib/macro-data/backups
sudo install -d -m 0750 -o data -g data /var/log/macro-data
```

### 6. Install systemd units

```bash
mkdir -p ~/.config/systemd/user
cp scripts/systemd/macro-data-backup.service        ~/.config/systemd/user/
cp scripts/systemd/macro-data-backup.timer          ~/.config/systemd/user/
cp scripts/systemd/macro-data-restore-drill.service ~/.config/systemd/user/
cp scripts/systemd/macro-data-restore-drill.timer   ~/.config/systemd/user/

# Edit Environment=REPO_ROOT= and ExecStart= paths to point at the
# checkout (e.g. /home/data/macro-data-service on the data VPS).

systemctl --user daemon-reload
systemctl --user enable --now macro-data-backup.timer
systemctl --user enable --now macro-data-restore-drill.timer

# user-level timers fire only while the user has a session — enable
# linger so they keep firing on a headless VPS.
loginctl enable-linger "$USER"
```

Verify:

```bash
systemctl --user list-timers macro-data-backup.timer macro-data-restore-drill.timer
journalctl --user -u macro-data-backup.service -n 50
journalctl --user -u macro-data-restore-drill.service -n 50
```

### 7. First-run smoke

```bash
# Snapshot only — does not touch B2.
scripts/backup/snapshot_sqlite.sh
scripts/backup/snapshot_clickhouse.sh

# Full pipeline including B2 push (will create encrypted objects).
scripts/backup/daily_backup.sh

# Recovery drill against today's push.
scripts/backup/restore_drill.sh
```

After the first backup run, `rclone lsf backups:daily/` shows
encrypted filenames; `rclone tree backups:` decrypts on-the-fly via
the local crypt remote so file listings are readable.

---

## Operations

### Manual run

```bash
scripts/backup/daily_backup_wrapper.sh
scripts/backup/restore_drill_wrapper.sh
```

The wrappers file a `data-quality` GitHub issue on non-zero exit.

### Inspect remote

```bash
# Decrypt + list
rclone lsf backups:daily/sqlite/
rclone lsf backups:monthly/

# Disk usage on B2
rclone size backups:
```

### Restore (manual recovery)

SQLite:

```bash
LATEST=$(rclone lsf backups:daily/sqlite/ --dirs-only | sort | tail -n1 | sed 's:/$::')
rclone copy "backups:daily/sqlite/$LATEST" /tmp/restore-sqlite/
sudo systemctl stop macro-data-api.service
sudo cp /tmp/restore-sqlite/engine.db /var/lib/macro-data/engine.db
sudo chown data:data /var/lib/macro-data/engine.db
sudo systemctl start macro-data-api.service
```

ClickHouse — single database from the latest archive:

```bash
LATEST=$(rclone lsf backups:daily/clickhouse/ --dirs-only | sort | tail -n1 | sed 's:/$::')
rclone copy "backups:daily/clickhouse/$LATEST" /tmp/restore-ch/
sudo install -d -m 0755 -o clickhouse -g clickhouse /var/lib/clickhouse/backups/manual-restore
sudo tar -xzf /tmp/restore-ch/market.tar.gz -C /var/lib/clickhouse/backups/manual-restore
sudo chown -R clickhouse:clickhouse /var/lib/clickhouse/backups/manual-restore
clickhouse-client --query \
    "RESTORE DATABASE \`market\` AS \`market_restore\` FROM Disk('backup', 'manual-restore/market')"
# Validate, then DROP + RENAME if you want to swap in:
clickhouse-client --query "DROP DATABASE market"
clickhouse-client --query "RENAME DATABASE market_restore TO market"
```

### Failure escalation

Backup-pipeline and restore-drill failures are filed under a dedicated
`backup-failure` label (auto-created by the wrapper if absent). The
ingestion DQ filer uses the `data-quality` label, so the two streams
stay separate — a backup outage does not interfere with the daily DQ
issue's open-issue selector. Triage:

1. `journalctl --user -u <unit>.service` — see the actual error.
2. If the failure is B2-side (403, network), confirm credentials in
   `/etc/macro-data/.env`. Application keys can be revoked in the B2
   console — generate a new one + restart.
3. If the failure is local (disk full, CH BACKUP refused), free space
   under `/var/lib/clickhouse/backups/` + `/var/lib/macro-data/backups/`
   and re-run `daily_backup_wrapper.sh`.

---

## Known limitations

- **Single-region B2.** Spec is single bucket; cross-region replication
  is out of scope.
- **No PITR.** Daily granularity only — last 24 h of writes are at risk.
- **Restore drill skips full CH RESTORE.** Validates archive structural
  integrity (tar ok, metadata/ + data/ present, size sane) but doesn't
  exercise `RESTORE DATABASE`. The manual section above is the full
  exercise; perform it once a quarter as belt-and-braces.
- **rclone obscure is reversible.** The obscured value protects against
  shoulder-surfing, not exfiltration. Treat `/etc/macro-data/.env` as a
  secret file (chmod 600, owned by `data`).
