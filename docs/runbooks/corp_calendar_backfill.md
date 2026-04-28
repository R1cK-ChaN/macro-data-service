# Corporate Calendar Historical Backfill Runbook

Issue #62. Walk the four backfillable EODHD corp-calendar subtypes
(`earnings`, `ipo`, `split`, `dividend`) from 2015-01-01 to today,
landing rows in `cal_corp_raw` and `cal_corp_event`.
`earnings_trend` is forward-only and excluded.

## Driver

`scripts/backfill_corp_calendar.py` — single-shot, bounded, resumable.
Each invocation is capped at `--max-requests` calls; progress lives in
`cal_corp_backfill_cursor` keyed by `(provider, subtype, phase)`. A
budget breach mid-run advances the cursor only past windows that
completed, so the next invocation picks up where the last left off.

## Phases

| Phase  | Span                       | Density (rough)             |
|--------|----------------------------|-----------------------------|
| recent | 2024-01-01 → today         | High — densest, prioritised |
| mid    | 2018-01-01 → 2023-12-31    | Medium                      |
| early  | 2015-01-01 → 2017-12-31    | Sparse, EODHD floor era     |

Phase upper bounds are independent — exhausting `recent`'s budget does
**not** reset `mid` or `early`.

## Typical operator flow

Plan first (zero HTTP):

```sh
PYTHONPATH=src python3 scripts/backfill_corp_calendar.py \
    --subtype split --phase recent --dry-run
```

Run a bounded chunk:

```sh
PYTHONPATH=src python3 scripts/backfill_corp_calendar.py \
    --subtype split --phase recent --max-requests 100
```

Re-run the same command on subsequent days; the cursor advances
automatically and idempotent `INSERT OR IGNORE` in `cal_corp_raw`
absorbs any boundary overlap.

Discovery + per-ticker enrichment (dividend two-stage):

```sh
PYTHONPATH=src python3 scripts/backfill_corp_calendar.py \
    --subtype dividend --phase recent --max-requests 200
# discovery sweep first; leftover budget pays for /api/div/{TICKER}
# detail fetches against the unique tickers surfaced this invocation.
```

Skip enrichment (discovery only):

```sh
… --subtype dividend --phase recent --no-dividend-details
```

## Reading cursor state

```sql
SELECT subtype, phase, cursor_date, window_end_date,
       rows_ingested, requests_spent, last_run_at, is_complete
FROM cal_corp_backfill_cursor
ORDER BY subtype, phase;
```

`is_complete = 1` means the cursor advanced past the phase upper bound
— no further runs needed for that `(subtype, phase)` pair.

## Bounding budget

`--max-requests` caps **all** EODHD calls in one invocation, including
the dividend two-stage enrichment pass. Choose a value an order of
magnitude below the daily plan limit so a stuck loop can't drain the
quota. The script's stdout one-liner reports `requests_spent` and
`stopped_reason`; persistent `max_requests_reached` lines mean you're
correctly bounded.

## Recovery

* `stopped_reason: throttled:…` — EODHD 429 storm. Wait, re-run the
  same command. The cursor still points at the in-flight window.
* `stopped_reason: max_requests_reached` — expected; raise
  `--max-requests` only after confirming today's quota.
* Cursor row stuck on the same date across runs — verify the window
  is actually returning rows (see `cal_corp_raw` for the snapshot)
  before raising the budget; `early` phase windows on dense subtypes
  may produce zero rows yet still consume one request each.

## systemd

`scripts/systemd/calendar-corp-backfill.service` is a one-shot unit
(no timer) with `--dry-run` defaults so `systemctl start` without
overrides is safe. Override args via the `CORP_BACKFILL_ARGS`
environment variable:

```sh
CORP_BACKFILL_ARGS='--subtype split --phase early --max-requests 200' \
    systemctl --user start calendar-corp-backfill.service
journalctl --user -u calendar-corp-backfill.service -e
```

The unit is **not** wired to a timer on purpose — historical backfills
should remain operator-driven so quota use stays under direct control.
Forward maintenance is a separate concern.

## Out of scope (separate issues)

- Forward / daily corp-calendar maintenance: future issue.
- PIT exposure of corp events to downstream consumers: #63.
- Restatement detection / surfacing: #64.
- `earnings_trend` historical backfill: not applicable (forward-only).
