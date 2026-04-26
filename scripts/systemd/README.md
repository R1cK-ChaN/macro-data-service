# Parity tripwire systemd unit

Files in this directory wire the daily TE-vs-official parity job
(issue #22) into a `systemd --user` timer.

## Install

```bash
# 1. One-time GitHub label bootstrap. The filer uses
#    `parity-drift` + `agency:<id>` labels; `gh issue create` errors
#    out if any are missing. Idempotent — re-running skips existing.
bash scripts/parity_setup_labels.sh

# 2. Install the systemd user unit.
mkdir -p ~/.config/systemd/user
cp scripts/systemd/parity-daily.service ~/.config/systemd/user/
cp scripts/systemd/parity-daily.timer   ~/.config/systemd/user/

# If your checkout is somewhere other than
# ~/Desktop/analyst/macro-data-service, edit the Environment= and
# ExecStart= paths in parity-daily.service before reloading.

systemctl --user daemon-reload
systemctl --user enable --now parity-daily.timer
```

Verify:

```bash
systemctl --user list-timers parity-daily.timer
journalctl --user -u parity-daily.service -n 100
```

## Operations

* Daily structured log: `.macro-data/logs/parity_daily.log` (one JSON
  per run).
* Filer state: `.macro-data/parity_state.json` (per-agency clean
  streak + open issue id).
* Infra strike counter: `.macro-data/logs/parity_infra_streak.json`.
  Two consecutive failures auto-file an `agency:infra` issue.
* Backup snapshots: `.macro-data/backups/te_calendar_<date>/engine.db`
  refreshed each successful run.

## Manual run

```bash
# Default: yesterday UTC.
scripts/parity_daily_wrapper.sh

# Specific date, comparator only (skip TE pull):
scripts/parity_daily_wrapper.sh --date 2026-04-22 --skip-fetch

# Dry-run: no gh side effects.
scripts/parity_daily_wrapper.sh --dry-run
```

## Design notes

* `flock --nonblock` in the wrapper guards against overlapping runs
  if the timer fires while a previous invocation is still working
  through TE rate-limit backoff.
* The python entry-point is the source of truth for exit codes; the
  wrapper just unlocks and propagates.
* `Persistent=true` on the timer means a missed firing (laptop
  suspended at 06:00 UTC) catches up on resume. The job is idempotent
  end-to-end so this cannot duplicate state.
