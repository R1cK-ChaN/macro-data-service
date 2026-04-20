# Calendar Ingestion

This package owns economic event calendar ingestion.

## Direction

Use Trading Economics for historical economic-calendar backfill. Use official
government and institution sources for forward-looking release data.

The goal is a durable calendar pipeline with TE as a bootstrap data source and
official publishers as the primary source for future data. Examples include
BLS, BEA, Census, Treasury, EIA, central banks, Eurostat, OECD, IMF, and other
institution-owned release pages or APIs.

## Lanes

### Historical Backfill

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
