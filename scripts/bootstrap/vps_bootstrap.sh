#!/usr/bin/env bash
# vps_bootstrap.sh — idempotent VPS bootstrap for the data lane (issue #133).
#
# Usage: sudo -v && ./scripts/bootstrap/vps_bootstrap.sh
#
# Environment overrides:
#   DATA_USER=data              non-root account systemd jobs run as
#   SSH_ALLOWED_FROM=""         CIDR/IP to lock :22 down to (empty = open)
#   LOCK_DATA_PASSWORD=0        set 1 to also lock data's local password.
#                               SSH password login is already blocked by the
#                               sshd drop-in; locking on top closes the local
#                               sudo + console fallbacks for fully-key-only
#                               hosts. Default 0 keeps `sudo` usable from
#                               data's own session.
#
# Run as a sudoer (not root). Re-running on a partially-configured host
# is a no-op. Targets Ubuntu 24.04 LTS.

set -euo pipefail
IFS=$'\n\t'

DATA_USER="${DATA_USER:-data}"
SSH_ALLOWED_FROM="${SSH_ALLOWED_FROM:-}"
LOCK_DATA_PASSWORD="${LOCK_DATA_PASSWORD:-0}"

DATA_DIR=/var/lib/macro-data
LOG_DIR=/var/log/macro-data
SECRETS_DIR=/etc/macro-data
SSHD_DROPIN=/etc/ssh/sshd_config.d/10-bootstrap-hardening.conf
F2B_JAIL=/etc/fail2ban/jail.d/sshd.local
CH_KEYRING=/etc/apt/keyrings/clickhouse-keyring.gpg
CH_LIST=/etc/apt/sources.list.d/clickhouse.list

# Works as either: root (via `sudo ./vps_bootstrap.sh`) or sudoer with
# cached/NOPASSWD sudo (via `sudo -v && ./vps_bootstrap.sh`). Inner sudo
# calls are no-op passthroughs when EUID is 0.
if ! sudo -n true 2>/dev/null; then
  echo "Need cached or passwordless sudo. Run 'sudo -v' first." >&2
  exit 1
fi

log() { printf '[%s] bootstrap: %s\n' "$(date -u +%FT%TZ)" "$*"; }

# ---------- 1/8 apt baseline + unattended security updates ----------------
log "phase 1/8: apt baseline + unattended-upgrades"
sudo DEBIAN_FRONTEND=noninteractive apt-get -qq update
sudo DEBIAN_FRONTEND=noninteractive apt-get -qq -y install \
  ca-certificates curl gnupg lsb-release apt-transport-https \
  unattended-upgrades chrony fail2ban ufw git \
  python3.12 python3.12-venv python3-pip
sudo systemctl enable --now unattended-upgrades.service >/dev/null

# ---------- 2/8 timezone UTC + chrony -------------------------------------
log "phase 2/8: timezone UTC + chrony"
if [[ "$(timedatectl show -p Timezone --value)" != "Etc/UTC" ]]; then
  sudo timedatectl set-timezone Etc/UTC
fi
sudo systemctl enable --now chrony.service >/dev/null

# ---------- 3/8 data user + sudo + ssh dir --------------------------------
log "phase 3/8: ensure $DATA_USER user with sudo"
if ! id -u "$DATA_USER" >/dev/null 2>&1; then
  sudo adduser --disabled-password --gecos "" "$DATA_USER"
fi
sudo usermod -aG sudo "$DATA_USER"
sudo install -d -m 700 -o "$DATA_USER" -g "$DATA_USER" "/home/$DATA_USER/.ssh"

# ---------- 4/8 SSH hardening (key-only, no root, no password) ------------
log "phase 4/8: SSH hardening"
sudo install -m 644 /dev/stdin "$SSHD_DROPIN" <<'SSHD_EOF'
# Managed by scripts/bootstrap/vps_bootstrap.sh — issue #133.
PasswordAuthentication no
ChallengeResponseAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin no
PermitEmptyPasswords no
SSHD_EOF
# Validate before reload — bad sshd_config kills the only path back in.
sudo sshd -t
sudo systemctl reload ssh.service

if [[ "$LOCK_DATA_PASSWORD" == "1" ]]; then
  sudo passwd -l "$DATA_USER" >/dev/null
  log "  $DATA_USER password locked (key-only login). Override with LOCK_DATA_PASSWORD=0."
fi

# ---------- 5/8 UFW firewall ----------------------------------------------
log "phase 5/8: UFW firewall (default deny, allow 22 + 443)"
sudo ufw --force default deny incoming >/dev/null
sudo ufw --force default allow outgoing >/dev/null
# Strip any pre-existing :22 rule (broad or scoped) so SSH_ALLOWED_FROM
# toggles between runs cleanly — without this, a later allowlisted run
# leaves the prior broad rule active and SSH stays open globally.
while :; do
  rule=$(sudo ufw status numbered 2>/dev/null \
    | awk '/22\/tcp/ {gsub(/[][]/,"",$1); print $1; exit}')
  [ -z "$rule" ] && break
  sudo ufw --force delete "$rule" >/dev/null
done
# Allow 22 BEFORE enable so the active SSH session does not drop.
if [[ -n "$SSH_ALLOWED_FROM" ]]; then
  sudo ufw allow from "$SSH_ALLOWED_FROM" to any port 22 proto tcp \
    comment 'ssh (allowlisted)' >/dev/null
else
  sudo ufw allow 22/tcp comment 'ssh' >/dev/null
fi
sudo ufw allow 443/tcp comment 'https (will be CF-restricted in #135)' >/dev/null
sudo ufw --force enable >/dev/null

# ---------- 6/8 fail2ban (sshd jail) --------------------------------------
log "phase 6/8: fail2ban sshd jail"
sudo install -m 644 /dev/stdin "$F2B_JAIL" <<'F2B_EOF'
# Managed by scripts/bootstrap/vps_bootstrap.sh — issue #133.
[sshd]
enabled = true
maxretry = 5
findtime = 10m
bantime = 1h
F2B_EOF
sudo systemctl enable --now fail2ban.service >/dev/null
sudo systemctl reload fail2ban.service >/dev/null 2>&1 || true

# ---------- 7/8 ClickHouse server (DEB repo) ------------------------------
log "phase 7/8: ClickHouse server"
if ! command -v clickhouse-server >/dev/null 2>&1; then
  sudo install -d -m 755 /etc/apt/keyrings
  # --batch --yes lets a partial-rerun (keyring written, apt-install failed)
  # overwrite the existing keyring instead of aborting on prompt.
  curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key \
    | sudo gpg --batch --yes --dearmor -o "$CH_KEYRING"
  sudo chmod 644 "$CH_KEYRING"
  arch=$(dpkg --print-architecture)
  echo "deb [signed-by=$CH_KEYRING arch=${arch}] https://packages.clickhouse.com/deb stable main" \
    | sudo tee "$CH_LIST" >/dev/null
  sudo DEBIAN_FRONTEND=noninteractive apt-get -qq update
  # Skip the interactive default-user password prompt.
  echo "clickhouse-server clickhouse-server/default-password password " \
    | sudo debconf-set-selections
  sudo DEBIAN_FRONTEND=noninteractive apt-get -qq -y install clickhouse-server clickhouse-client
fi
sudo systemctl enable --now clickhouse-server.service >/dev/null

# ---------- 8/8 directory skeleton (owned by data) ------------------------
log "phase 8/8: directory skeleton"
for d in "$DATA_DIR" "$LOG_DIR"; do
  sudo install -d -o "$DATA_USER" -g "$DATA_USER" -m 755 "$d"
done
sudo install -d -o "$DATA_USER" -g "$DATA_USER" -m 700 "$SECRETS_DIR"

if [[ ! -f "$SECRETS_DIR/.env" ]]; then
  log "  $SECRETS_DIR/.env missing — copy scripts/bootstrap/secrets.env.template and fill"
fi

log "DONE. Verify:"
log "  timedatectl | grep 'Time zone'"
log "  systemctl is-active chrony fail2ban ssh ufw clickhouse-server unattended-upgrades"
log "  sudo ufw status numbered"
log "  clickhouse-client --query 'SELECT version()'"
log "  ssh -o PreferredAuthentications=password $DATA_USER@<host>  # should refuse"
