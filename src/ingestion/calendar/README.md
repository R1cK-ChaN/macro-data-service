# Calendar Ingestion

This package owns two physically separate calendar lanes:

- **Economic lane** (`cal_econ_*`) — macro releases. TradingEconomics is the
  historical bootstrap source; source-owned publishers (BLS, BEA, Census, ISM,
  U Michigan, Conference Board, NAR, ECB, Eurostat, Destatis, INE, ISTAT,
  Fed, NBS, BoJ, Statistics Bureau JP, CAO, MoF, METI) are the forward collection path.
- **Corporate lane** (`cal_corp_*`) — earnings, IPOs, splits, dividends, and
  earnings trends. EODHD is the current corporate-actions source.

Downstream consumers read `v_calendar_item` for a unified `CalendarItem`
shape across both lanes.

## Direction

Use Trading Economics for historical economic-calendar backfill. Use official
government and institution sources for forward-looking release data. Use
EODHD for corporate actions.

## Lanes

### Economic — Historical Backfill

`te_api/` contains the Trading Economics Calendar API path:

- `client.py` handles auth, rate limiting, retry, URL-length guard, and JSON
  transport.
- `parser.py` maps the 22-field TE payload into `cal_econ_raw` plus a projected
  event record.
- `projector.py` stores revision history in `cal_econ_raw` and upserts the
  latest view into `cal_econ_event`.
- `backfill.py` plans adaptive date windows from 2013 forward and persists
  progress in `cal_backfill_cursor`.
- `updates.py` reconciles TE update pointers through `/calendar/calendarid`
  hydration and records upstream drops.

Service operations:

- `calendar_econ_backfill` plans or runs bounded TE backfill batches.
- `calendar_econ_sync_updates` runs TE update-pointer reconciliation.

Both operations default to dry-run behavior at the service boundary.

### Corporate — EODHD

`eodhd_api/` routes the five EODHD calendar endpoints into `cal_corp_*`:

- `client.py` handles auth (`EODHD_API_KEY`), 429 retry, and `fmt=json` injection.
- `parser.py` maps rows per subtype (`earnings`, `earnings_trend`, `ipo`,
  `split`, `dividend`). `provider_event_id` is synthesized as
  `sha256(provider|subtype|code|primary_date|subtype_key)`.
- `projector.py` writes revisions to `cal_corp_raw` and upserts the latest
  projection into `cal_corp_event` under an `observed_at` gate.
- `fetcher.py` dispatches a single subtype per call and slices the
  requested date range into bounded windows.

Service operation:

- `calendar_corp_fetch` plans or runs one subtype (`subtype=earnings|ipo|
  split|dividend|earnings_trend`).

Defaults to dry-run. `earnings_trend` requires `symbols` because EODHD's
`/calendar/trends` endpoint is symbol-scoped only.

The dividend parser targets EODHD's calendar feed, which is **discovery-only**:
each row is `(symbol, ex-dividend-date)` with no value, period, currency, or
declaration/record/payment dates. Those richer fields are pulled via the
per-ticker `/api/div/{TICKER}.{EXCHANGE}` endpoint — see
`parse_dividend_detail_row` and the `fetch_dividend_details` free function.
The detail parser reuses the same `provider_event_id` the discovery parser
synthesised, so the rich snapshot upserts the existing `cal_corp_event` row
in place (discovery and detail land as two `cal_corp_raw` revisions of one
event). Exposed as the `calendar_corp_fetch_dividend_details(symbols, from,
to, dry_run, max_requests)` service op. Validated against live EODHD on
2026-04-21 (see `docs/validation/calendar_acquisition_eodhd_2026-04-21.md`);
enrichment probe `dividend_details_aapl` added to the live validator.

### Forward Calendar Data

Forward data should come from official publishers. This lane should resolve
future event dates, event titles, release times, source URLs, and revision
metadata directly from institution-owned sources.

The official-source lane should write into the same canonical calendar tables:

- `cal_econ_raw` for source payload snapshots.
- `cal_econ_event` for the latest normalized event view.
- `cal_econ_drops` for observed removals or source-side retirements.

Provider IDs should preserve source ownership. For example, a BLS release row
should carry a BLS-derived provider identity, while a TE historical row keeps
its TE `CalendarId`.

## Operating Rules

- TE is the historical bootstrap and revision-reconciliation source.
- Official institution sources are the forward collection path.
- Raw payloads stay immutable and provider-specific.
- `cal_econ_event` stays provider-agnostic enough for API consumers.
- New forward connectors should share parser and projector concepts with
  `te_api/`, while keeping source-specific transport code separate.

## Implementation Notes

The TE backfill starts from the earliest confirmed TE calendar data
(`2013-01-02`) and uses adaptive window sizing to stay inside TE basic-plan row
and request limits. Backfill runs should use small request budgets and rely on
`cal_backfill_cursor` for resumability.

Forward official-source connectors should prioritize stable machine-readable
interfaces. Scraped institution pages should store selector/version metadata
when the source publishes HTML pages.
