# Architecture — current state

**Version:** 0.1.0 — *mini-Bloomberg foundations* (pre-1.0)
**Target:** 1.0 = mini-Bloomberg for macro + markets. One terminal-shaped
surface covering real-time quotes, macro time series, economic + corporate
calendar, research documents, news flow, release-aware scheduling, cross-source
resolution, and a RAG/agent layer — all off a single canonical store, pulled
from ~25 free/licensed upstreams instead of BBG's paid feed.
**Role in the monorepo:** the data-aggregation layer. Ingests, normalizes, and
resolves macro + market + calendar + document data from ~25 upstreams into a
single SQLite store that downstream services (analyst UI, RAG, agents) read
through one service interface.
**Last updated:** 2026-04-21 (branch `feat/issue-9-official-calendar-connectors`, P2 BEA scaffold shipped)

### Bloomberg-capability coverage (current → 1.0)

| BBG capability | Our equivalent | Status |
|---|---|---|
| Real-time quotes, EOD bars | `market_price_bars` + Tiingo/EODHD/macro projection | shipped (issue #1) |
| `<GO>` command surface (HDB, ECO, CALT, …) | `macro-data-service` CLI + thin HTTP | shipped; expanding |
| ECO — economic calendar | `cal_econ_*` + TE backfill + official sources | in flight (issue #8) |
| EVTS — corporate calendar | `cal_corp_*` + EODHD | in flight (issue #8) |
| Terminal News (N) | `news_articles` + 140-feed RSS pipeline + classifier | shipped core; dedup + ranking ongoing |
| Research / NIM | `document` + `document_blob` (+ RAG index) | shipped core; Fed + gov-report wired |
| PORT / PRTU analytics | `portfolio_*`, `trade_signals`, `performance_records` | scaffolded; awaiting downstream |
| FLDS / SRCH (metadata search) | `subjects` + `concept_map` + `list_items` family filter | shipped (issues #2, #5) |
| Release alerts, SRCY | `release_schedule` + `release_status` + 3 alert types | shipped |
| Terminal chat (IB / MSG) | `conversation_threads` / `_messages`, `group_*`, `delivery_queue` | scaffolded |

"Shipped" = tables + code in place. "In flight" = current work. "Scaffolded" =
schema exists; population pending downstream services.

This doc is the shared context surface for other agents (Claude subagents,
Codex, fresh sessions) starting cold. README.md is the user-facing intro; this
file is "what's wired up right now, where it lives, and what's in flight."

When the doc disagrees with the code, trust the code and update the doc.

---

## One-paragraph mental model

Sources → per-source ingestion clients → storage (SQLite, `engine.db`) → a
unified resolution layer (`resolve_indicator`, `get_market_history`,
`list_items`, soon `v_calendar_item`) → thin HTTP/CLI service. Every table is
keyed for point-in-time queries; revisions and vintages are preserved rather
than overwritten. Downstream never talks to upstream providers — it talks to
the resolution layer.

---

## Layers

| Layer | Lives in | Role |
|---|---|---|
| **Sources** | ~25 upstream APIs / RSS feeds / HTML endpoints | Raw data |
| **Ingestion** | `src/ingestion/` | Fetch, normalize, validate, write |
| **Storage** | `src/storage/sqlite.py` (8.8k lines, ~60 tables) | Canonical persistence |
| **Resolution** | `src/macro_data/service.py` + storage helpers | Cross-source ranking, PIT queries, unified views |
| **Service** | `src/macro_data/cli.py`, `server.py` | CLI + HTTP boundary |
| **RAG sidecar** | `src/rag/` | Local semantic index + retrieval (Milvus optional) |

Everything else (agents, UIs, notebooks) is a consumer.

---

## Domains and current status

### 1. Macro time-series (the backbone)

- **Sources wired** (11): BLS, FRED, EIA, NYFed, Treasury, IMF, Eurostat, BIS, ECB, OECD, World Bank. SDMX providers unified under `ingestion/sdmx/` (6 of the 11 go through it).
- **Schema:** `obs_source`, `obs_family`, `indicators`, `indicator_vintages`. `concept_map` bridges source-native IDs to 86 canonical concepts; `subject_aliases` lets text queries resolve back to concepts.
- **Resolution:** `resolve_indicator(concept_id)` ranks sources by `concept_map.priority`, returns primary + alternates. Vintages preserved for PIT reconstruction.
- **Status:** architecture complete; first-pass ingestion bootstrap underway. DB starts empty; Phase 1 (top US macro, 10 indicators) is the immediate next milestone, not more code.

### 2. Documents (government reports + central-bank comms)

- **5-table normalized schema:** `doc_source` → `doc_release_family` → `document` → `document_blob` + `document_extra`. Covers ~40 publishers across US/CN/JP/EU.
- **Clients:** `GovReportIngestionClient`, `FedIngestionClient` (Fed speeches/minutes), news pipeline for wire stories.
- **Linkage:** `obs_family_document` connects an obs family to the release family that produced it (produced_by / derived_from / related_to). Subject tagging happens at ingest via `storage/subjects.py`.

### 3. News

- **Clients:** `NewsIngestionClient` over 140 RSS feeds (`ingestion/news/_config.py`). Fingerprinting in `article_fingerprint` handles de-dup.
- **Classifier pipeline:** `news_classify.py` + `news_extract.py` for content extraction, domain detection, trend bucketing.

### 4. Market data (issue #1 — shipped)

- **Identity-first design.** Instruments resolved through OpenFIGI / ISIN / EODHD symbol history — `market_instruments` + `market_symbol_history` + `market_price_bars`.
- **Providers:** Tiingo (US equities/ETFs, P0), EODHD (global + delistings, P1), FRED/EIA/ECB projections (`MacroMarketProvider`, P2), `IdentityRepairService` (lazy repair on `break_detected`).
- **Universe:** 11 US macro ETFs + 6 global instruments seeded; extendable via `_tiingo_universe.py` / `_eodhd_universe.py`.

### 5. Calendar (issue #8 complete — replacing TE with official sources on `feat/issue-9-official-calendar-connectors`)

Two physical lanes behind one downstream contract (see `src/storage/STORAGE.md` → "Unified calendar").

- **Economic lane** — `cal_econ_raw` / `cal_econ_event` / `cal_econ_drops`. TradingEconomics now (paid key enabled 2026-04-20, API survey in `analyst/te_endpoint/`); BLS / BEA / Federal-Reserve / ECB / NBS connectors replacing it over issue #9 — five official-source providers are already registered in `cal_provider` at `precedence=100` (above TE's `10`). `v_calendar_item` itself is `UNION ALL` today; the parity harness (#9 P6) is the first caller to read the `precedence` column and resolve duplicates.
- **Corporate lane** — `cal_corp_raw` / `cal_corp_event`. EODHD earnings / IPOs / splits / dividends / earnings_trends.
- **Unified view:** `v_calendar_item` `UNION ALL`s both lanes into the `CalendarItem` DTO shape. Downstream sees one target.
- **Revision model:** content-hash over mutable fields; append-only raw; PIT projection as the queryable layer. Mirrors `obs_family` / `indicator_vintages`.
- **Legacy `calendar_events`** (HTML-scraped) untouched until slice-2 API fetcher proves parity.

Slice progress:

- **P0 (shipped):** six new tables + `v_calendar_item` VIEW + extended `CalendarItem` DTO + 9 smoke tests.
- **P1 (shipped — scaffold):** TE API scaffold under `src/ingestion/calendar/te_api/` — `client.py` (auth / 1 r/s / 409 backoff / URL guard), `parser.py` (22-field → raw + event records, content-hash over 6 mutable fields), `projector.py` (idempotent raw inserts + snapshot-aware event upsert + drop audit), `backfill.py` (era-bracketed adaptive window planner + cursor-persisting runner), `updates.py` (`/calendar/updates` → `/calendarid` reconciler). Service ops `calendar_econ_backfill` + `calendar_econ_sync_updates` exposed over HTTP with `dry_run=True` default. Nothing auto-runs. 22 mocked `respx` tests — no real TE calls in CI.
- **P2 (shipped — scaffold):** EODHD corporate scaffold under `src/ingestion/calendar/eodhd_api/` — `client.py` (auth / 429 backoff / `fmt=json` injection), `parser.py` (five subtype parsers: `earnings` / `earnings_trend` / `ipo` / `split` / `dividend`; `provider_event_id = sha256(provider|subtype|code|primary_date|subtype_key)`; IPO id anchors on lifecycle-stable `filing_date`; per-subtype `content_hash` over mutable fields), `projector.py` (idempotent `cal_corp_raw` inserts + `observed_at`-gated `cal_corp_event` upsert), `fetcher.py` (subtype-dispatched window fetcher, trend-payload flattening for `[[…]]` shape). Service op `calendar_corp_fetch(subtype, from, to, symbols, dry_run)` — `dry_run=True` default.
- **P3 (shipped):** TE live-validation harness `scripts/validate_calendar_acquisition.py` (`--provider te`, dry-run default, budget-capped) + acquisition fixes from first live run — `_country_code` no longer synthesises false ISO codes (e.g. "IMF" → "IM"), and `/calendar/updates` 1000-row truncation is surfaced via `updates_truncated` on the reconciler summary.
- **P4 (shipped):** EODHD live-validation harness extension (`--provider eodhd`, 8 probes) + acquisition fixes — `/calendar/dividends` redefined as discovery-only `(symbol, ex_date)` (the extended dividend format does not arrive on the calendar feed); `_SubtypeSpec` grew `symbols_param` + `one_symbol_per_request` so dividends route through `filter[symbol]=X` (one request per symbol) rather than the generic `symbols=A,B` that the server silently drops.
- **P5 (shipped — scaffold):** per-ticker dividend enrichment via EODHD's `/api/div/{TICKER}.{EXCHANGE}`. New `parse_dividend_detail_row` shares `provider_event_id` with the discovery parser so the rich snapshot (amount / currency / declaration / record / payment dates) upserts the same `cal_corp_event` row. New `fetch_dividend_details` free function — one request per symbol, top-level-array payload (no envelope). Service op `calendar_corp_fetch_dividend_details(symbols, from, to, dry_run, max_requests)` — `dry_run=True` default. Validator gained a 9th probe (`dividend_details_aapl`) covering the enrichment feed.
- **P5a (shipped):** `/calendar/dividends` JSON:API pagination — `_SubtypeSpec` gained `paginated: bool`; `CorpCalendarFetcher._execute_request` loops `page[offset]` += `page[limit]` (1000) while `links.next` is truthy, trusting the link as the authoritative terminator rather than row-count heuristics. `max_requests` budget halts mid-cursor and surfaces `stopped_reason=max_requests_reached` so callers can retry. Closes the silent 1000-row truncation class of bug (same shape as the TE `/calendar/updates` issue surfaced in P3).
- **P6 (shipped — TE P1 backfill + country_code coverage):** First real TE backfill executed end-to-end (121 windows, 77,771 events, 2023-01-01 → 2026-04-21, zero truncation, zero drops, ~2:39 wall-clock, 121/1000 monthly quota). Live fidelity spot-check rehydrated 5 sampled `CalendarId`s via `/calendar/calendarid/` and matched 30/30 mutable-field comparisons. Surfaced a downstream-query gap: `TE_COUNTRY_MAP` covered 49 of 172 upstream `Country` values, leaving 16,837 rows (21%) with empty `country_code`. Map extended to 115 sovereign entries.
- **P6a (shipped — supra-national aggregate codes):** Five non-sovereign tags (`IMF`/`OPEC`/`World`/`G20`/`G7`) now map into ISO-3166-1's user-assigned `QM-QZ` range (`QM`/`QP`/`QW`/`QT`/`QS`) so the typed column stays strictly alpha-2 while still being filterable. Downstream filters via the new `SUPRA_NATIONAL_CODES` constant — `LIKE 'Q%'` is *wrong* because Qatar (`QA`) is also Q-prefixed. Full coverage: 77,771/77,771 rows (100%) mapped, zero empty `country_code` in the P1 backfill.
- **P7 (shipped — in-session truncation re-fetch + bracket tightening + TE P2 backfill):** The first P2 run (2016-2022, 164 windows, 20d/12d brackets) surfaced a silent-data-loss bug: when a window hit the 1000-row cap, the runner saved the truncation cursor but then advanced to the next *planned* window, dropping the tail `[last_date, window.end]`. Because the final window always advanced past the phase end, the cursor never rewound on a subsequent invocation either — 255 days / 51 consecutive-day runs were permanently missing. Fix: runner now uses a `deque` and enqueues a continuation `Window(last_date, window.end)` at the head of the queue when truncation is detected; raw-table content-hash dedup absorbs the overlap at `last_date`. Progress guard prevents infinite loops on pathological 1000+ rows on a single date. Also tightened `ERA_BRACKETS` for 2016-2022 from 20d/12d to 10d (P1's proven-safe size), which eliminated truncation entirely on the clean re-run (256/256 windows, zero truncated). P2 events post-fix: 152,884 (was 136,531). Missing-day count: **40** (was 255) — all weekend or sparse-holiday upstream, verified. New `continuation_fetches` field on `RunSummary` makes the dedup pass visible to callers.
- **P7a (shipped — discontinued-coverage country map extension):** P2's 2016-2022 run surfaced 1,773 events across 40 small-state countries (Afghanistan, Andorra, Syria, Puerto Rico, Greenland, Isle of Man, San Marino, Monaco, Liechtenstein, various Pacific/Caribbean nations, etc.) that TE published through 2019-2022 but has since dropped from the feed — they never appeared in P1's 2023-era enumeration so the original map missed them. Map extended to 208 entries total. Post-projection coverage on the combined P1+P2 dataset: **230,655 / 230,655 rows (100%)** mapped, zero empty `country_code`. Historical events from TE's wider coverage era stay queryable even after upstream narrowed.
- **P8 (shipped — TE P3 backfill, full 2013→today coverage complete):** 2013-01-02 → 2015-12-31, 22 planned windows on 180d/30d brackets, 46 total fetches (22 planned + 24 in-session continuations — the P7 fix earning its keep on the sparse-era's oversized windows), 34,137 events, ~3 min wall-clock. 146 upstream-sparse days in the P3 range (TE's calendar coverage ramped up through Feb–Mar 2013: Jan-2013=36 events, Feb=264, Mar=996, plateau ≥900/mo from Apr onward; 2015-12=1,166). Verified upstream-empty via direct single-day probes. Fidelity spot-check on 3 random CalendarIds across Mar-2013/Apr-2014/Oct-2015 matched 18/18 mutable-field comparisons against `/calendar/calendarid/` rehydration. **Combined store post-P8: 264,792 events, 2013-01-02 → 2026-04-21, 100% country-code coverage across P1+P2+P3.** Monthly TE budget used this session ≈ 600 / 1000.

Issue #9 — official-source replacement (in progress on `feat/issue-9-official-calendar-connectors`):

- **P0 (shipped — provider registration + shared scaffolding):** Five official-source providers (`bls` / `bea` / `federal-reserve` / `ecb` / `nbs`) seeded into `cal_provider` at `precedence=100` (above TE's `10`); BLS / BEA / NBS typed as `government_agency`, Fed / ECB typed as `central_bank`. New `src/ingestion/calendar/_official_shared/` package exposes three utilities every P1–P5 connector will reuse: `canonicalize_indicator` (alias table: `"Consumer Price Index" → "CPI"`, `"FOMC Rate Decision" → "FOMC_RATE"`, …), `parse_scheduled_release_time` (DST-aware — BLS `"8:30 AM ET"` resolves correctly across winter/summer), and `synthesize_event_id` (`sha256(provider|country|indicator_canonical|event_time_utc)` — upstream sources without stable ids get a deterministic key the projector can upsert on). `scripts/validate_calendar_acquisition.py` gained `--provider {bls,bea,fed,ecb,nbs}` flags (scaffold-only; probe bodies land per-phase). 10 smoke tests cover the shared utilities + a synthetic BLS row round-tripping through `v_calendar_item`.
- **P1 (shipped — BLS connector scaffold):** `src/ingestion/calendar/bls_api/` — calendar-side projection layer on top of the existing `ingestion.timeseries.scrapers.bls.BLSClient` (reused verbatim for auth / batching / 500-req-daily budget tracking). Parser turns a `BLSObservation` into `(raw, event)` records using the `_official_shared` utilities (`canonicalize_indicator` → `"CPI"`, `synthesize_event_id` for deterministic ids anchored on the reference-period date — stable across the value → value+schedule lifecycle). Projector mirrors the TE SQL shape (store_raw INSERT-OR-IGNORE + project_events upsert with observed-at ordering). Whitelist ships with two anchors — CPI (`CUUR0000SA0`) and NFP (`CES0000000001`) — the highest-trader-impact US releases; additional indicators (Core CPI / PPI / JOLTS / ECI / Productivity / Jobless Claims) land as later slices. Service op `calendar_econ_fetch_bls(start_year, end_year, series_ids, dry_run=True)` exposed on `LocalMacroDataService`; `dry_run=True` default. `event_time_utc` on P1-only rows is the **reference-month end** with `event_time_precision='approximate'` — a deliberate placeholder that P1a's schedule scraper overwrites with the true scheduled datetime. 18 mocked tests — no real BLS calls in CI. No live probe this slice (user elected 1a scope).
- **P1a (shipped — release-schedule scraper):** `src/ingestion/calendar/bls_api/schedule.py` scrapes `bls.gov/schedule/news_release/<series>.htm` (CPI + NFP), parses the `<table class="release-list">` DOM with BeautifulSoup, and projects each upcoming-release row into `cal_econ_event` with `actual=NULL` / `event_time_precision='datetime'`. Release times (`"08:30 AM"`) are DST-aware converted to UTC via `_official_shared.parse_scheduled_release_time` with `default_tz="America/New_York"` (BLS publishes all major indicators at 8:30 AM ET, unadorned on the page). Live fetch uses a browser-style header bundle because `bls.gov` returns HTTP 403 on the default `python-requests` UA. New `project_schedule_events` upsert splits the write paths: schedule side owns `event_time_utc` / `precision` / `title` / `source_url`; API side owns `actual` / `previous` / `content_hash`. `project_events` cross-source merge rule preserves an already-datetime-precise `event_time_utc` when a later API-side write would otherwise clobber it with the approximate placeholder. Result: schedule + API rows converge on one `cal_econ_event` row regardless of write ordering, via the shared `provider_event_id` (anchored on reference-period date in both sides). Service op `calendar_econ_schedule_bls(series_ids, dry_run=True)`. Fixture HTML captured live 2026-04-21 in `tests/fixtures/bls_schedule/`; 12 new tests cover parse / merge (both orderings) / DST / service wiring. No live probe in CI.
- **P2 (shipped — BEA connector scaffold):** `src/ingestion/calendar/bea_api/` — calendar-side projection on top of the existing `ingestion.timeseries.scrapers.bea.BEAClient` (reused verbatim for auth / ~100-req-per-min throttle / error handling). Parser turns a `BEAObservation` into `(raw, event)` records using the same `_official_shared` utilities (`canonicalize_indicator` collapses the BEA release title to a canonical token; `synthesize_event_id` anchors the id on the BEA-canonical reference-period date — stable across the value → value+schedule lifecycle that P2a will introduce). Whitelist ships with two anchors — **Real GDP** (NIPA `T10101` line 1, quarterly SAAR — the advance / second / third estimate headline) and **Personal Income** (NIPA `T20600` line 1, monthly — the headline aggregate on BEA's Personal Income and Outlays release). PCE lives on the same monthly release but at a different `(table, line)` coordinate; rather than ship a guess at the coordinate with no live probe to verify, PCE is deferred to P2a. Trade Balance (ITA dataset) and Corporate Profits are deferred to P2b / P2c because the ITA parameter surface differs from NIPA and warrants its own live probe. Fetcher groups whitelist entries by `(dataset, table, frequency)` so one BEA HTTP call satisfies every line on a shared table; off-whitelist lines in the response are discarded rather than projected as unknown-indicator rows. `BEAObservation` gained a `raw: dict` field (default empty) so `NoteRef`-only revisions register new audit rows — same pattern applied to `BLSObservation` in P1. Projector mirrors the BLS SQL shape including the `datetime`-precision preservation clause; BLS + BEA are now the two concrete callers of the merge-rule variant, so promotion into `_official_shared` waits for a third caller (likely ECB / Fed / NBS). Service op `calendar_econ_fetch_bea(start_year, end_year, series_ids, dry_run=True)`. `event_time_utc` on P2-only rows is the **reference-period end** with `event_time_precision='approximate'` — the P2a schedule scraper (bea.gov/news/schedule) will overwrite with the true scheduled datetime. 22 mocked tests cover registry / parser / projector / fetcher / service op; no real BEA calls in CI.
- **Remaining:** indicator-whitelist expansion (BLS / BEA); P2a BEA release-schedule scraper (bea.gov/news/schedule); P3 ECB / P4 Fed / P5 NBS connectors; P6 cross-provider parity harness; P7 downstream `GET /calendar` HTTP surface over `v_calendar_item`; P8 TE subscription retirement once P6 passes.

### 6. Release scheduling + availability

- `release_schedule` (rules) + `release_status` (per-release tracking). Date-math resolvers in `ingestion/release_schedule.py` compute `next_expected` per concept.
- State machine: `PENDING → WAITING → FETCHED → CONFIRMED / STALE / FAILED`. Retry ladder 1m / 5m / 15m / 1h / 4h.
- Three alert types: `DELAY` (missed expected release by 30m), `FAILED` (retries exhausted), `MISMATCH` (cross-source divergence over threshold).

### 7. Source capabilities + catalog sync

- `source_capability` registers each source's discovery / latest-sync / status contract. `catalog_entity` + `catalog_sync_*` persist crawled catalogs and run logs — used for OECD / World Bank / ILO / SDMX catalogs.

### 8. RAG sidecar

- `src/rag/`: chunker, BM25 lexical, embeddings, Milvus vector store, reranker, retriever, orchestrator, policy layer. Reads from `document_blob`; writes to a sibling index. Optional at runtime.

### 9. Trading / research artifacts (scaffolded)

- `trade_signals`, `decision_log`, `position_state`, `performance_records`, `analytical_observations`, `research_artifacts`, `generated_notes`, `regime_snapshots`, `portfolio_*`, `group_*`, `client_profiles`, `conversation_threads` / `_messages`, `delivery_queue`, `subagent_runs`. Tables exist; most are scaffolding pending the downstream services that will populate them.

---

## Service boundary

**HTTP API is the only downstream contract.** Every feature this repo ships
must be reachable through `macro-data-api` routes. Downstream services (analyst
UI, RAG consumers, agents, notebooks, scheduled jobs, external teams) are not
permitted to:

- import from `src/` as a Python library,
- read `engine.db` directly,
- shell out to the `macro-data-service` CLI,
- tail files, parse logs, or scrape any internal artifact.

If a consumer needs a capability, the path is always: add a service op →
expose an HTTP route → call it. No exceptions, no "for now we'll just
import it." Breaking this rule couples downstream to our storage shape and
defeats the whole aggregation-layer premise.

- **HTTP API** (`macro-data-api`) — the contract. Routes are a thin mapping
  over `LocalMacroDataService` ops; schema is the `contracts.py` DTOs.
- **CLI** (`macro-data-service`) — operational only. Subcommands like
  `refresh`, `refresh-source`, `schedule --run`, `health`, plus the new
  `list_items` op (issue #5). Used for bootstrap, backfills, and debugging
  by repo maintainers. Not part of the downstream contract.
- **Source family discriminator** (issue #5, shipped): every
  `IngestionSourceDefinition` carries a `family` tag; `list_sources` returns
  `[{name, family}, ...]`. `list_items` unions documents + indicators +
  market bars under a single subject, with a `family` filter — exposed over
  HTTP, not only via CLI.

---

## Where things live — quick map

```
src/
  contracts.py              Core DTOs (Event, CalendarItem, MarketSnapshot, RegimeState, ...)
  storage/
    sqlite.py               All CREATE TABLE statements + all read/write helpers
    STORAGE.md              Table-by-table narrative (kept in sync with sqlite.py)
    subjects.py             Subject vocabulary loader + tagger
    subjects.yaml           Canonical subject list (edit here, sync via subjects.py)
  ingestion/
    sources.py              IngestionOrchestrator — scheduling, retry, health, source family
    source_capabilities.py  Capability registry + discovery/latest-sync adapters
    release_schedule.py     Date-math resolvers + availability state machine
    validation/             Data quality + cross-source checks
    sdmx/                   Unified SDMX engine (base client, parsing, providers/)
    timeseries/, news/, documents/, trends/, market/, calendar/  One dir per domain
    _shared/                http_transport, url_canon, selector versioning
  macro_data/
    service.py              LocalMacroDataService — the one interface downstream reads
    cli.py, server.py       Thin boundaries
    factory.py, client.py   Wiring + programmatic access
  rag/                      Local RAG (see src/rag/README.md)
```

---

## In flight right now

| Issue | Branch | Slice |
|---|---|---|
| #8 | `feat/issue-8-calendar-two-lane-schema` | P0 shipped (two-lane schema + DTO + view). P1 shipped (TE API scaffold + dry-run HTTP ops). P2 shipped (EODHD corporate scaffold + `calendar_corp_fetch` op, dry-run default). Slices P3–P5 pending (TE backfill execution, official sources, legacy retirement). |

Closed recently (context only — the code is the source of truth):

- #5 — source family tag + cross-type `list_items`.
- #1 — market-data layer with OpenFIGI/ISIN identity + lazy repair.
- #2 — unified subject vocabulary.

---

## Invariants worth knowing

1. **Downstream consumes only the HTTP API.** No library imports, no direct
   `engine.db` reads, no CLI shelling, no file/log tailing. Every feature ships
   with a route or it doesn't ship. (See "Service boundary" above for the full
   rule.)
2. **Revisions are never lost.** `indicator_vintages`, `cal_econ_raw` /
   `cal_corp_raw` append; PIT projections are derived views over them.
3. **The service layer exposes one contract.** Storage lanes can split (macro
   vs calendar vs corporate) but resolution presents a single shape —
   `resolve_indicator`, `get_market_history`, `list_items`, and (coming)
   `v_calendar_item` reads, all via HTTP.
4. **Upstream IDs are kept.** `provider_event_id`, `provider_series_id`,
   `canonical_url` — we never discard the upstream handle, even when we
   synthesize our own.
5. **Concept map is the cross-source bridge.** Source-native IDs → canonical
   concept IDs (CPI_US, UNEMP_US, …) → `resolve_indicator` ranks and returns
   with provenance.
6. **`subjects.yaml` is authoritative for the subject vocabulary.**
   `subjects` / `subject_aliases` / `item_subjects` tables are derived.
