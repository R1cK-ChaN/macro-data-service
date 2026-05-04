# Macro Data Service

Institutional-grade macro-data ingestion, resolution, and observability platform. Ingests 86 economic concepts from 11 core sources, supports a 25-source capability registry, and includes release-calendar-aware scheduling, availability verification, cross-source fallback, and a ClickHouse-backed EODHD market-data layer for US active plus delisted instruments.

**Last updated:** 2026-05-03 (issue #123 calendar scraper retirement: source-owned calendar APIs, `te_api`, and EODHD corporate calendar remain.)

## Architecture

```text
Sources (macro + EODHD)                           Ingestion              Storage              Resolution
────────────────────────────────────────────     ──────────             ─────────            ───────────
BLS, FRED, EIA                                   Fetchers + SDMX        SQLite               resolve_indicator()
NYFed, Treasury                                  Normalization           concept_map (86)     source-priority ranking
IMF, Eurostat                                    Date alignment          obs_family           cross-source alternates
BIS, ECB, OECD                                   Deduplication           release_schedule     get_market_history()
World Bank                                                               release_status       get_market_history()
EODHD         (US symbols, EOD, corp actions)    EODHDClient             ClickHouse market.*

                     ┌─────────────────────────────────────────┐
                     │  Release Schedule → Availability Check  │
                     │  → Retry (1m/5m/15m/1h/4h) → Fallback  │
                     │  → Status: CONFIRMED / STALE / FAILED   │
                     └─────────────────────────────────────────┘
                     ┌─────────────────────────────────────────┐
                     │  Health Dashboard    │  3 Alert Types    │
                     │  per-source status   │  DELAY            │
                     │  freshness + retries │  FAILED           │
                     │  provenance          │  MISMATCH         │
                     └─────────────────────────────────────────┘
                     ┌─────────────────────────────────────────┐
                     │ Source Capability Layer                 │
                     │  catalog-crawlable / discovery-rich     │
                     │  fixed-scope-complete                   │
                     │  discovery / latest-sync / status       │
                     └─────────────────────────────────────────┘
                     ┌─────────────────────────────────────────┐
                     │ Vintage + PIT Layer                     │
                     │  indicator_vintages canonical store     │
                     │  indicators SQL view over latest rows    │
                     │  as_of reads in resolve/history          │
                     └─────────────────────────────────────────┘
                     ┌─────────────────────────────────────────┐
                     │ Bronze Replay Layer                     │
                     │  obs_raw + cal_*_raw + fundamentals_raw  │
                     │  raw payload → canonical silver rows     │
                     │  per-source replay round trips           │
                     └─────────────────────────────────────────┘
                     ┌─────────────────────────────────────────┐
                     │ Market-Data Layer (#118/#119)           │
                     │  EODHD US active + delisted universe    │
                     │  ClickHouse bars_1d/dividends/splits    │
                     │  Daily bulk refresh + DELETE/refetch    │
                     │  Self-heal + realtime spot checks       │
                     └─────────────────────────────────────────┘
```

## Layout

```text
src/
  ingestion/
    timeseries/       FRED, BLS, EIA, Treasury, NYFed, WorldBank + SDMX providers
      scrapers/       Source-specific API clients (fred.py, bls.py, eia.py, ...)
      fetchers/       RawSeries adapters (Fetcher protocol)
      clients/        Ingestion clients (FREDIngestionClient, ...)
      sdmx/           Unified SDMX engine + providers/ (IMF, ECB, BIS, OECD, ...)
      _config.py      Series configs (MACRO_SERIES, BLS_SERIES, OECD_SERIES, ...)
    news/             Reuters, FT, WSJ, Bloomberg
      scrapers/       Article listing scrapers
      client.py       NewsIngestionClient
      _config.py      RSS feed registry (140 feeds)
    documents/        Government reports, Fed communications
      scrapers/       gov_report.py
      clients/        GovReportIngestionClient, FedIngestionClient
      _config.py      FED_FEEDS, FED_SPEAKERS
    trends/           Reddit + Weibo social trend tracking
      scrapers/       reddit.py, weibo.py upstream readers
      clients/        _trends.py ingestion clients
    sentiment/        X (Twitter) sentiment lane
      x_api/          X API client, scrapers, discovery
    market/           EODHD US universe, EOD bars, corporate actions
      scrapers/       Provider HTTP clients
        _eodhd.py        EODHDClient — symbol lists, EOD, bulk, realtime
        _eodhd_identity.py  EODHD search / delisted / symbol-change / exchanges
        _openfigi.py     OpenFIGIClient — /v3/mapping
      clients/        High-level providers
        _eodhd.py        EODHDMarketDataProvider (ClickHouse)
        _macro_market.py MacroMarketProvider (FRED/EIA/ECB projection)
        _identity_repair.py Historical identity-repair prototype
      _quality.py          Provider-neutral bar checks
      _macro_map.py        13-entry rates/FX/commodity mapping
      _config.py           MACRO_WATCHLIST (real-time watchlist)
    calendar/         Economic + corporate calendars
      *_api/          Source-owned packages (abs_api, banxico_api, bcb_api,
                      bls_api, boc_api, boe_api, boj_api, ecb_api, ...)
      te_api/         TradingEconomics historical bootstrap + updates
      eodhd_api/      EODHD earnings, IPOs, splits, dividends
      scheduler.py    Recurring schedule/value refresh driver
      agency_registry.py  Agency attribution map for parity filings
      parity.py       TE-vs-official parity harness
      parity_daily.py Daily parity comparator
      parity_filer.py Parity issue filer
      evidence_archive.py  Wayback evidence archive helper
    _shared/          Cross-cutting: http_transport, url_canon, selector versioning
    validation/       Data quality checks, cross-source consistency
    sources.py        IngestionOrchestrator — scheduling, retry, health
    source_capabilities.py  Unified capability registry + discovery/sync adapters
    release_schedule.py     Date-math resolvers, availability checks, alert logic
    scrapers/         Facade — backward-compatible re-exports
    clients/          Facade — backward-compatible re-exports
    fetchers/         Facade — backward-compatible re-exports
    sdmx/             Facade — backward-compatible re-exports
  storage/            SQLite macro schema/query mixins + ClickHouse market backend
    clickhouse/       market.bars_1d/dividends/splits/instruments store
  macro_data/         CLI, HTTP server, service layer, factory
  contracts.py        Date normalization, epoch utilities
```

## Concepts covered (86)

| Category | Examples |
|---|---|
| US Inflation | CPI_US, CORE_CPI_US, PPI_US, CORE_PCE_US, breakevens |
| US Employment | NFP_US, UNEMP_US, JOLTS, initial/continuing claims, ECI |
| US Growth | GDP_REAL_US, GDP_NOMINAL_US, retail sales, industrial production |
| US Rates | POLICY_RATE_US, SOFR, OBFR, 2Y/10Y/30Y treasuries, yield curve |
| US Liquidity | Fed balance sheet, M2, reverse repo, TGA |
| US Credit/Fiscal | HY OAS, credit gap, debt outstanding, avg interest rate |
| US Energy | WTI, Brent, natural gas, crude stocks, petroleum supply |
| Euro Area | CPI_EU, GDP_EU, UNEMP_EU, ECB policy rate, M1/M2/M3, EURUSD |
| China | CPI_CN, GDP_REAL_CN, FX reserves, PBOC rate, credit gap |
| Japan | CPI_JP, GDP_REAL_JP, BOJ rate |
| Cross-country | OECD CLI (US/CN/JP/EU), BIS EER/property, World Bank development |

## CLI

### Core operations

```bash
macro-data-service serve                              # start HTTP server
macro-data-service refresh                            # refresh all sources
macro-data-service refresh-source --source fred       # refresh single source
macro-data-service refresh-indicator CPI_US           # refresh single concept

# Market-data operations:
PYTHONPATH=src python3 scripts/seed_market_universe.py
PYTHONPATH=src python3 scripts/backfill_market_history.py --status active
PYTHONPATH=src python3 scripts/market_daily_refresh.py --date 2026-05-01
```

### Resolution

```bash
macro-data-service resolve CPI_US                     # latest resolved value
macro-data-service resolve CPI_US --date 2024-01-01   # specific date
macro-data-service resolve CPI_US --history --limit 12  # resolved time series
macro-data-service resolve CPI_US --as-of 2024-03-01  # point-in-time vintage view
macro-data-service resolve CPI_US --json              # JSON with provenance
```

### Release schedule

```bash
macro-data-service schedule --show                    # all 86 concepts + next_expected
macro-data-service schedule --due                     # concepts due within 2h window
macro-data-service schedule --status                  # availability status per concept
macro-data-service schedule --run                     # start release-aware scheduler
macro-data-service schedule --show --json             # JSON output
```

### Health dashboard and alerts

```bash
macro-data-service health                             # per-source status table
macro-data-service health --indicator CPI_US          # single concept
macro-data-service health --alerts                    # active alerts only
macro-data-service health --json                      # JSON output
macro-data-service source-health                      # customer-facing source matrix
macro-data-service source-health --all               # include internal/coming-soon sources
```

Output:

```text
  indicator                 source       status       freshness  retries  note
  -------------------------------------------------------------------------------------
  CPI_US                    bls          CONFIRMED    fresh      0        primary
  CPI_US                    fred         CONFIRMED    fresh      0        secondary
  GDP_REAL_US               fred         WAITING      stale      2        primary
  SOFR_US                   nyfed        CONFIRMED    fresh      0        primary
```

Three alert types:

- **DELAY** — data not confirmed 30min after expected release
- **FAILED** — fetch retries exhausted (5 attempts)
- **MISMATCH** — cross-source value divergence exceeds threshold

### Validation

```bash
macro-data-service validate                           # full validation suite
macro-data-service validate-concept CPI_US            # single concept
macro-data-service validate-concept --all             # all concepts
macro-data-service launch-gate --json                 # production data-quality gate
```

### Catalog exploration (OECD / World Bank)

```bash
macro-data-service oecd-dataflows --query inflation
macro-data-service oecd-structure --dataflow QNA
macro-data-service wb-sources
macro-data-service wb-indicators --query gdp
```

### Source capabilities and catalog sync

```bash
macro-data-service list-sources                                # every registered source as {name, family}
macro-data-service list-sources --family market_price          # filter to one family
macro-data-service sources-capabilities
macro-data-service catalog-list --source oecd --refresh --limit 10
macro-data-service catalog-structure --source ecb --entity ECB_EA_DEPOSIT_RATE
macro-data-service catalog-sync-discovery --source worldbank --limit 10
macro-data-service catalog-sync-latest --source worldbank --entity SP.POP.TOTL --limit 1
macro-data-service catalog-status --source worldbank
```

Source modes:

- `catalog-crawlable` — source supports discovery plus catalog-based latest sync
- `discovery-rich` — source exposes discovery/structure metadata, but latest sync still maps to curated or configured paths
- `fixed-scope-complete` — source has a complete supported-entity surface, but no meaningful upstream full-catalog crawl

Operational notes:

- Capability `structure` output may be a live provider structure summary or a config-backed summary, depending on the source mode.
- Capability latest-sync is best-effort and now surfaces upstream HTTP/timeouts instead of silently returning false-green zero results.
- Some scaffolded sources can validly return zero discovered entities when the upstream provider or local configured series set is empty.
- Customer-facing health surfaces hide `ILO` and `UNSD` until they return non-empty entity catalogs.
- `news` disables known bad feeds and drops low-content articles.
- `gov_reports` falls back to RSS `description` when full content extraction fails; metadata-only records are stored for PDF/asset sources.
- `eia` prefers live EIA data, then recent local cache, then selected FRED fallback series. Timeout is configurable via `EIA_TIMEOUT` env var (default 60s). Failed series degrade gracefully instead of crashing the batch.
- `fred_nondaily` source refreshes weekly/monthly/quarterly FRED series (ICSA, CCSA, WALCL, PCEPILFE, INDPRO, RSAFS, M2SL, GDP, GDPC1) every 6h, complementing `fred_daily` which handles daily series only.

## Revision and replay layers

`indicator_vintages` is the canonical macro time-series write target. `indicators` is a SQL view over the latest vintage per `(source, series_id, observation_date)`, and `resolve_indicator` / `resolve_indicator_history` accept `as_of=YYYY-MM-DD` to reconstruct point-in-time projections from vintages.

Routine time-series fetchers follow a bronze-to-silver dual-write contract: raw HTTP payloads land in `obs_raw` before normalization writes parsed observations to `indicator_vintages`. Canonicalizers in `ingestion/timeseries/canonicalize.py` hash stable observation content, while per-source `_parse_*` replay helpers re-project stored payloads through the same parsers used by live fetches.

Calendar and fundamentals use the same replay shape. `cal_econ_raw`, `cal_corp_raw`, and `fundamentals_raw` preserve provider payloads; `cal_econ_event`, `cal_corp_event`, and `fundamentals_*` tables expose parsed projections.

## HTTP API

The API is served by FastAPI under uvicorn. `GET /healthz` is the liveness
probe and is open for uptime checks. Other routes require `X-API-Key`;
tokens are loaded at startup from `/etc/macro-data/api_tokens.json` unless
`ANALYST_MACRO_DATA_API_TOKENS_PATH` is set.

- `GET /health`      customer-facing source health matrix
- `GET /healthz`     lightweight liveness probe
- `GET /v1/manifest` data availability manifest for consumer routing
- `GET /v1/calendar` unified calendar feed
- `GET /v1/calendar/revisions` corporate calendar revision feed
- `GET /v1/fundamentals/{ticker}` fundamentals projection
- `POST /v1/ops/<operation>`

Key operations:

| Operation | Description |
|---|---|
| `get_data_manifest` | Dataset availability manifest with status, row counts, freshness, and quality status |
| `resolve_indicator` | Highest-priority observation for a concept; accepts `as_of` for PIT vintage reads |
| `resolve_indicator_history` | Resolved time series with best source per date; accepts `as_of` |
| `get_release_schedule` | Release calendar for all/single concept |
| `get_release_status` | Availability tracking status |
| `get_health` | Per-source health dashboard |
| `get_alerts` | Active alerts (DELAY, FAILED, MISMATCH) |
| `list_sources` | Registered ingestion sources as `{name, family}` rows, optional `family` filter |
| `list_source_capabilities` | Capability registry for all known sources |
| `get_source_health_dashboard` | Customer-facing per-source health summary |
| `sync_catalog_discovery` | Persist discovered provider entities for a source |
| `list_catalog_entities` | List stored catalog entities for a source |
| `get_catalog_structure` | Source-specific structure/config summary for an entity |
| `sync_catalog_latest` | Run source-level latest sync through the capability layer |
| `get_catalog_status` | Checkpoints and recent runs for capability jobs |
| `refresh_indicator` | Trigger ingestion for a single concept |
| `validate_concept` | Run validation checks on a concept |
| `get_indicator_ontology` | Structural metadata for an indicator |
| `get_recent_releases` | Recent economic data releases |
| `get_upcoming_calendar` | Upcoming calendar events |
| `get_market_snapshot` | Latest market prices |
| `list_items` | Cross-type feed for a subject: documents (news / gov reports / notes) unioned with indicator observations (via `concept_map` + direct `subject_aliases`) and market rows from the service layer. Every row carries a `family` + `kind` tag; optional `family` filter narrows to one bucket. Still accepts `q` (FTS5, documents only), `document_type`, `country_code`, `min_confidence` |
| `get_document` | Single document by `document_id` or `hash_sha256` (17-field summary + markdown body + subject tags) |
| `list_subjects` | Subject vocabulary (auto-synced from `src/storage/subjects.yaml`) |
| `backfill_document_indexes` | One-shot FTS + subject-tag backfill for DBs whose documents predate the new sidecars (idempotent) |

## Information layer

Non-numeric sources (news, gov reports, research notes) land in a
shared `document` surface keyed by a canonical subject vocabulary, so
downstream callers can pull them alongside the macro timeseries through
one API. The schema additions live in `src/storage/sqlite.py`:

- `subjects` / `subject_aliases` / `item_subjects` — canonical subject
  IDs (`econ.cpi`, `rate.us.sofr`, …) and the aliases that map
  source-native identifiers (FRED series, calendar indicators, title
  regex) back to them. Seeded from `src/storage/subjects.yaml`.
- `document` carries the 17-field information surface (`institution`,
  `authors`, `asset_class`, `impact_level`, `contains_commentary`,
  `confidence`, …), with full-text search in `documents_fts`.
- `obs_enrichment` sidecar — derived labels keyed by `(obs_family_id,
  date, key)`. Currently used for VIX regime classification.

Ingestion paths:

- `news` — `NewsIngestionClient` mirrors each article into `document`
  (document_type='report', source_id='news') + documents_fts +
  item_subjects, alongside the legacy `news_articles` row. News rows
  carry scraper-supplied metadata.
- `gov_reports` — `GovReportIngestionClient` merges scraper metadata
  with optional LLM extraction into the 17-field document surface.
- `notes` — `python -m ingestion.notes.ingest --input <dir>` parses
  YAML-frontmatter markdown into `document` (source_id='notes',
  document_type='report', confidence=1.0).
- `FEDWATCH_US` — CME-equivalent midpoint persisted daily from
  `rateprobability.com`; bridges to the `rate.us.fedwatch` subject.
- `VIX_US` — FRED `VIXCLS` close; `refresh_vix_regime` writes a
  low/elevated/stressed label into `obs_enrichment` per date.

Optional gov-report LLM enrichment of `institution`, `asset_class`,
`impact_level`, and the rest of the 17-field surface is controlled by
`DOCUMENT_EXTRACT_API_KEY` (or `OPENAI_API_KEY`) plus
`DOCUMENT_EXTRACT_MODEL` / `DOCUMENT_EXTRACT_BASE_URL`. Unset → ingestion
uses scraper-supplied metadata.

### Source families (issue #5)

Every `IngestionSourceDefinition` carries a `family` tag applied at
registration via `SOURCE_FAMILIES` in `src/ingestion/sources.py`. The
tag flows through `IngestionRunReport.to_dict()`, the `list_sources`
op and the `list_items` envelope so downstream callers can group
sources and rows by type without reflecting on names:

| Family | Sources |
|---|---|
| `economic_data` | `fred_daily`, `fred_nondaily`, `fred_full`, `fred_vintages`, `bls`, `eia`, `treasury_fiscal`, `nyfed_rates`, `imf`, `imf_vintages`, `eurostat`, `bis`, `ecb`, `oecd`, `worldbank`, `worldbank_catalog` |
| `market_price` | ClickHouse market read ops and EODHD maintenance scripts |
| `release_report` | `gov_reports`, `fed` |
| `news` | `news` |
| `calendar` | `calendar` |
| `trend` | `reddit_trends`, `weibo_trends` |
| `signal` | `rate_probability` |

`list_items(subject="econ.cpi")` returns the merged envelope:
documents (via `item_subjects`), indicator observations (via
`subject_aliases → concept_map → indicators`, pivoted through
`concept_id` so cross-source alternates surface), and market rows from
the service layer. Each row carries a `family` + `kind`
discriminator; an optional `family` filter narrows the result.

## Market-data layer

The market lane uses `src/storage/clickhouse/` and ClickHouse tables
under the `market` database: `bars_1d`, `dividends`, `splits`, and
`instruments`. EODHD is the provider. Universe seeding discovers US
active symbols and active plus delisted symbols from
`/api/exchange-symbol-list/US`; `is_active` is derived from set
membership. SQLite stays the macro store for calendar, indicators,
documents, news, and fundamentals.

Historical backfill uses per-ticker `/api/eod/{T}.US`,
`/api/div/{T}.US`, and `/api/splits/{T}.US` with the resumable cursor
`.macro-data/market_backfill_cursor.json`. Daily refresh uses
`/api/eod-bulk-last-day/US` for bars, dividends, and splits. New corp
actions append to `market.dividends` / `market.splits`, then the
ticker's bars are deleted and re-fetched from EODHD so
`adjusted_close` stays provider-trusted.

Operational entrypoints:

- `scripts/seed_market_universe.py`
- `scripts/backfill_market_history.py`
- `scripts/market_daily_refresh.py`
- `scripts/detect_missing_bars.py`
- `scripts/spot_check_close.py`

### Quality checks

The market maintenance scripts check stale active tickers, delisting
set membership, and sampled realtime close drift.

Local ClickHouse storage is booted by `docker-compose.yml` and persists
under `.macro-data/clickhouse/{data,tmp,user_files,logs}`.
The `get_market_history` service op reads daily bars from this ClickHouse lane.

## Data model

```text
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  concept_map    │────▶│  indicators  │     │ release_schedule │
│  86 concepts    │     │  observations│     │  86 rules        │
│  11 sources     │     │  per source  │     │  8 rule types    │
│  priority ranks │     └──────┬───────┘     └────────┬────────┘
└─────────────────┘            │                      │
                               │ (macro projection)   │
                               ▼                      ▼
                   ┌───────────────────────┐ ┌───────────────────┐
                   │  ClickHouse market    │ │ release_status    │
                   │  instruments          │ │  PENDING / WAITING│
                   │  bars_1d/div/splits   │ │  FETCHED / CONFIRM│
                   │  US active+delisted   │ │  STALE / FAILED   │
                   └───────────────────────┘ └───────────────────┘
┌─────────────────┐
│  obs_family     │
│  obs_source     │
│  calendar_*     │
│  documents      │
│  fundamentals_* │
└─────────────────┘

┌─────────────────────┐    ┌──────────────────────┐
│ source_capability   │───▶│ catalog_entity       │
│ source mode + flags │    │ discovered entities  │
└─────────────────────┘    └──────────┬───────────┘
                                      │
                         ┌────────────▼────────────┐
                         │ catalog_sync_checkpoint │
                         │ catalog_sync_run        │
                         └─────────────────────────┘
```

## Shadow mode

Continuous ingestion runner with structured logging and daily digest.

```bash
python shadow_runner.py --interval 6    # run full cycle every 6 hours
python shadow_status.py                 # show latest digest + recent errors
```

Logs: `.macro-data/logs/shadow.log`, `.macro-data/logs/daily_digest.jsonl`

Production status (2026-03-22): 86/86 concepts covered (100%), 12 sources active, cycle time ~16 min.

## Local run

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
macro-data-service serve --host 127.0.0.1 --port 8765
```

SQLite path: `.macro-data/engine.db`

API token file shape:

```json
{
  "tokens": {
    "secret-token": {
      "consumer_id": "analyst-project",
      "rate_limit_per_minute": 60
    }
  }
}
```

### Environment variables

`src/env.py` reads from process env first, then `.env` at the repo root.

| Variable | Used by | Required? |
|---|---|---|
| `FRED_API_KEY` | FRED series, FX, and macro-market rate/FX projection | yes for any FRED data |
| `BLS_API_KEY` | BLS series | yes for BLS |
| `BEA_API_KEY` | BEA series | yes for BEA |
| `CENSUS_API_KEY` | Census series | yes for Census |
| `EIA_API_KEY` | EIA + macro-market commodity projection | yes for EIA |
| `IMF_API_KEY` | IMF SDMX | yes for IMF |
| `EODHD_API_KEY` | EODHD market universe, EOD bars, corp actions, fundamentals | yes for market/fundamentals |
| `OPENFIGI_API_KEY` | historical identity-repair prototype | optional (anonymous tier works) |
| `DOCUMENT_EXTRACT_API_KEY` / `OPENAI_API_KEY` | Gov-report 17-field LLM extraction (`make_extractor_from_env`, **reads `os.environ` only — not `.env`**) | optional; unset → scraper metadata |
| `DOCUMENT_EXTRACT_MODEL`, `DOCUMENT_EXTRACT_BASE_URL`, `DOCUMENT_EXTRACT_CONTEXT_CHARS` | Model / endpoint / chunk-size overrides for the gov-report extractor (same process-env-only loading) | optional |
| `ANALYST_MACRO_DATA_API_TOKENS_PATH` | FastAPI public API token mapping path | optional; default `/etc/macro-data/api_tokens.json` |
| `ANALYST_MACRO_DATA_RATE_LIMIT_DB` | shared SQLite fixed-window rate-limit state | optional; default `/tmp/macro-data-api-rate-limits.sqlite3` |
| `ANALYST_MACRO_DATA_WORKERS` | uvicorn worker count for `macro-data-service serve` | optional; default `2` |
| `ANALYST_MACRO_DATA_API_TOKEN` | client token sent as `X-API-Key`; server also accepts it as a local inline token for CLI launches | optional |

## Agent wiring

Point `analyst-project` at this service:

```text
ANALYST_MACRO_DATA_BASE_URL=http://127.0.0.1:8765
ANALYST_MACRO_DATA_API_TOKEN=
```

## Tests

```bash
python -m pytest tests/ -v                            # all tests
python -m pytest tests/test_release_schedule.py -v    # schedule + alerts (58 tests)
python -m pytest tests/test_macro_data_cli.py -v      # CLI smoke tests
python -m pytest tests/test_fetcher_adapters.py -v    # fetcher adapter tests
python -m pytest tests/test_source_capabilities.py -v # capability registry + catalog ops
```
