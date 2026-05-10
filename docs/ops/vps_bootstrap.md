# VPS bootstrap checklist

End-to-end procedure to take a fresh Ubuntu 24.04 LTS host (target spec:
Contabo Storage VPS 20 — 3 vCPU / 8 GB RAM / 400 GB SSD / Singapore
region) to a state ready for the macro-data writer / reader lanes.
Issue #133.

The bootstrap is **idempotent** — re-running `vps_bootstrap.sh` against
a partially-configured host is safe.

---

## Prerequisites (on local workstation)

- ssh access to the VPS as a sudoer (initial Contabo `root` or your
  uploaded SSH-key user).
- Local clone of `macro-data-service` checked out at the commit you want
  to deploy.

## Phase 1 — manual: prep the sudoer account

If the VPS does not yet have a non-root SSH-key sudoer:

1. ssh in as the provider's initial user (Contabo provisions root by
   default).
2. Create your bootstrap account:
   ```
   adduser --disabled-password --gecos "" rick   # or your alias
   usermod -aG sudo rick
   install -d -m 700 -o rick -g rick /home/rick/.ssh
   nano /home/rick/.ssh/authorized_keys           # paste your SSH pubkey
   chown rick:rick /home/rick/.ssh/authorized_keys
   chmod 600 /home/rick/.ssh/authorized_keys
   ```
3. From your laptop, `ssh rick@<vps>` and confirm sudo works.

## Phase 2 — manual: ensure the `data` account

The bootstrap script will create the `data` user if absent. To create it
manually first (e.g. so GitHub Actions can deploy before bootstrap runs):

```
ssh rick@<vps>
sudo adduser --disabled-password --gecos "" data
sudo usermod -aG sudo data
sudo install -d -m 700 -o data -g data /home/data/.ssh
sudo cp /home/rick/.ssh/authorized_keys /home/data/.ssh/authorized_keys
sudo chown data:data /home/data/.ssh/authorized_keys
sudo chmod 600 /home/data/.ssh/authorized_keys
```

## Phase 3 — get the source on the VPS

Either:

- **Via GitHub Actions** (preferred steady-state): merge the `dev` branch
  into `master`, approve the `prod` deployment in the GitHub Actions UI,
  and `.github/workflows/deploy-prod.yml` rsyncs the working tree into
  `/home/data/macro-data-service/` for you.
- **Via manual rsync** (initial bring-up before the workflow is live):
  ```
  rsync -av --exclude='.git/' --exclude='.macro-data/' \
    --exclude='.env' --exclude='*.db*' --exclude='.venv/' \
    /path/to/macro-data-service/ data@<vps>:/home/data/macro-data-service/
  ```

## Phase 4 — run the bootstrap script

```
ssh rick@<vps>
cd /home/data/macro-data-service
sudo -v
./scripts/bootstrap/vps_bootstrap.sh
```

Eight phases run in sequence: apt baseline, chrony, user, SSH hardening,
UFW, fail2ban, ClickHouse, directory skeleton. Re-runs are no-ops.

### Override variables

| Variable | Default | Effect |
|---|---|---|
| `DATA_USER` | `data` | Non-root account systemd jobs run as |
| `SSH_ALLOWED_FROM` | empty | If set (e.g. `1.2.3.4` or `1.2.3.0/24`), restricts port 22 to that source |
| `LOCK_DATA_PASSWORD` | `0` | Set `1` to also `passwd -l` data's local password. SSH password login is already blocked by the sshd drop-in; locking on top closes local sudo + Contabo KVM console fallbacks. Default keeps `sudo` usable from data's own session. |

After the script completes, verify:

```
timedatectl | grep 'Time zone'                 # Etc/UTC
systemctl is-active chrony fail2ban ssh ufw clickhouse-server unattended-upgrades
sudo ufw status numbered                        # only 22 + 443 rules
clickhouse-client --query 'SELECT version()'    # returns a version string
ssh -o PreferredAuthentications=password data@<vps>  # must refuse
```

## Phase 5 — fill secrets

```
sudo cp /home/data/macro-data-service/scripts/bootstrap/secrets.env.template \
        /etc/macro-data/.env
sudo chown data:data /etc/macro-data/.env
sudo chmod 600 /etc/macro-data/.env
sudo nano /etc/macro-data/.env
```

Empty values are acceptable for sources you do not yet ingest.

The application reads env vars via two paths, and **both must point at
`/etc/macro-data/.env`** for production secrets to resolve:

- **`os.environ` direct reads** — `CLICKHOUSE_*`, `WAYBACK_*`,
  `ANALYST_MACRO_DATA_API_TOKENS_PATH`, etc. are read straight from
  process environment. Production systemd units must set
  `EnvironmentFile=/etc/macro-data/.env` (covered in #106) so these
  values reach the process.
- **`get_env_value()` fallback** — source-API keys (`FRED_API_KEY`,
  `BLS_API_KEY`, `EODHD_API_KEY`, …) flow through `src/env.py`, which
  by default falls back to repo `.env` and `~/.macro-data/dev.env`. To
  also include `/etc/macro-data/.env` in the fallback chain, set
  `MACRO_DATA_ENV_FILES=/etc/macro-data/.env` in the same systemd unit.

`EnvironmentFile=` covers both paths; setting only `MACRO_DATA_ENV_FILES`
leaves `os.environ`-direct vars (notably `CLICKHOUSE_PASSWORD`) unloaded.

## Phase 6 — what's next

Bootstrap stops at "ready to install application". Subsequent issues
take it forward:

- **#106** — writer systemd timer pack (data-quality, shadow digest,
  main ingestion refresh).
- **#134** *(closed)* — production HTTP stack; the reader unit lands
  in #106.
- **#135** — Cloudflare front-door + UFW-vs-CF IP whitelist.
- **#136** — backup + Backblaze B2 off-site replication. Setup runbook:
  `docs/runbooks/backup_b2.md` (B2 bucket + key, `rclone obscure`,
  ClickHouse backup-disk config, systemd install).
- **#137** — systemd unit hardening + Cloudflare Health Checks.
- **#140** — cold migration of local `engine.db` + ClickHouse to VPS.
  Runbook: `docs/ops/cutover.md` (preflight, execution phases, verify,
  rollback). Driver script: `scripts/migrate/cutover.sh`.

## Rollback / re-bootstrap

The script is intentionally idempotent. To reset a single concern:

- `sudo rm /etc/ssh/sshd_config.d/10-bootstrap-hardening.conf && sudo systemctl reload ssh.service`
  reverts SSH hardening (re-enables password login).
- `sudo ufw reset` clears the firewall rules.
- `sudo apt purge clickhouse-server clickhouse-client` removes ClickHouse
  binaries; data dirs in `/var/lib/clickhouse/` survive purge.
  `sudo rm -rf /var/lib/clickhouse` for a clean slate.

A full host re-image is faster than chasing partial undo paths.
