# Calendar systemd units

Files in this directory wire the recurring calendar jobs into
`systemd --user` timers:

| Unit | Cadence | Purpose | Issue |
| --- | --- | --- | --- |
| `calendar-schedule-refresh.timer` | daily 04:00 UTC | every connector pulls forward-looking schedule rows | #31 |
| `calendar-value-sweep.timer` | hourly at :15 | every value-side connector fills `actual` on recent rows | #31 |
| `parity-daily.timer` | daily 06:00 UTC | TE-vs-official parity tripwire (depends on the 04:00 refresh having run) | #22 |
| `fundamentals-forward.timer` | daily 23:00 ET | EODHD `/api/fundamentals/` snapshot for the seeded universe (~17 tickers) | #68 |

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

## Calendar refresh + value sweep (issue #31)

```bash
mkdir -p ~/.config/systemd/user
cp scripts/systemd/calendar-schedule-refresh.service ~/.config/systemd/user/
cp scripts/systemd/calendar-schedule-refresh.timer   ~/.config/systemd/user/
cp scripts/systemd/calendar-value-sweep.service      ~/.config/systemd/user/
cp scripts/systemd/calendar-value-sweep.timer        ~/.config/systemd/user/

# Same caveat as the parity unit: edit Environment= / ExecStart= in
# each .service if your checkout is not at
# ~/Desktop/analyst/macro-data-service.

systemctl --user daemon-reload
systemctl --user enable --now calendar-schedule-refresh.timer
systemctl --user enable --now calendar-value-sweep.timer
```

Verify:

```bash
systemctl --user list-timers \
    calendar-schedule-refresh.timer calendar-value-sweep.timer
journalctl --user -u calendar-schedule-refresh.service -n 100
journalctl --user -u calendar-value-sweep.service -n 100
```

Operations:

* Schedule refresh log: `.macro-data/logs/calendar_refresh_schedules.log`
  (one JSON per run — `ok_count`, `failed_count`,
  `failed_connectors[]`, `wall_seconds`).
* Value sweep log: `.macro-data/logs/calendar_sweep_values.log` (same
  shape).
* Per-connector breaker state: `cal_provider` table column
  `cooling_until_ms` and `calendar_connector_state.consecutive_failures`
  in the engine DB.

Manual run:

```bash
# Default: hits live upstreams and writes to engine.db.
scripts/calendar_refresh_schedules_wrapper.sh
scripts/calendar_sweep_values_wrapper.sh

# Plan only — no HTTP, no DB writes.
scripts/calendar_refresh_schedules_wrapper.sh --dry-run
scripts/calendar_sweep_values_wrapper.sh --dry-run

# Subset by connector.
scripts/calendar_refresh_schedules_wrapper.sh --connectors bls bea
```

Design notes (calendar units):

* Cadence ordering — schedule refresh at 04:00 UTC ships forward-looking
  rows before the parity tripwire fires at 06:00 UTC. Value sweep at
  every `:15` is staggered off the 04:00 / 06:00 hourly slots so the
  three timers never collide on the engine DB.
* `flock --nonblock` is held by the wrapper so an over-running run
  silently skips the next slot rather than racing on the engine DB.
  The refresh and sweep wrappers share one lock
  (`.macro-data/calendar_recurring.lock`) — without sharing, the 04:15
  sweep can step on a still-running 04:00 refresh and trip
  `database is locked` ticks against the per-connector breakers.
* On resume after a long suspend, systemd queues the missed firings
  for refresh + sweep + parity together. `parity-daily.service`
  carries `After=calendar-schedule-refresh.service` and
  `After=calendar-value-sweep.service` so the catch-up parity run
  sees today's freshly-written `actual` values instead of running
  ahead of the catch-up sweep and filing false TE-only drift issues.
* Per-connector failures are isolated by the
  `_run_connector_with_breaker` driver in
  `src/ingestion/calendar/scheduler.py` — one upstream outage rolls
  back only that connector. Connector-level signal lives in
  `calendar_connector_state` and is enforced by `cooling_until_ms`,
  not by systemd `Restart=`.

## EODHD fundamentals daily sweep (issue #68)

```bash
mkdir -p ~/.config/systemd/user
cp scripts/systemd/fundamentals-forward.service ~/.config/systemd/user/
cp scripts/systemd/fundamentals-forward.timer   ~/.config/systemd/user/

# Same Environment= / ExecStart= caveat — edit if the checkout path
# differs from ~/Desktop/analyst/macro-data-service.

systemctl --user daemon-reload
systemctl --user enable --now fundamentals-forward.timer
```

Operations:

* Daily structured log: `.macro-data/logs/backfill_fundamentals.log`
  (one JSON per run — `tickers_planned`, `tickers_fetched`,
  `requests_spent`, `raw_inserted`, `company_upserted`,
  `financials_upserted`, `highlights_upserted`, `stopped_reason`,
  `errors[]`).
* Cadence ordering — 23:00 ET sits one hour after the corp calendar
  forward sweep (22:00 ET); both share `.macro-data/` but use
  separate locks (`fundamentals_recurring.lock` vs
  `calendar_recurring.lock`) so they don't queue against each other.
* Idempotency: `fundamentals_raw` ignores duplicate
  `(provider, ticker, content_hash)`; projections only update when
  the incoming `observed_at_epoch_ms` is at least as recent. A
  late-resumed missed firing (`Persistent=true`) cannot create stale
  state.

Manual run:

```bash
# Default: walk the seeded universe (~17 tickers), live execute.
scripts/backfill_fundamentals_wrapper.sh

# Subset by ticker.
scripts/backfill_fundamentals_wrapper.sh --tickers AAPL.US MSFT.US

# Plan-only via the python entry-point (no --execute):
PYTHONPATH=src python3 scripts/backfill_fundamentals.py
```
