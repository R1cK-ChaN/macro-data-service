# Macro Data Service

Institutional-grade macro-data ingestion, resolution, and observability platform. Ingests 86 economic concepts from 11 core sources, supports a 25-source capability registry, and includes release-calendar-aware scheduling, availability verification, and cross-source fallback. A unified market-data layer covers Tiingo US equities/ETFs, EODHD global equities/ETFs/indices, and rates/FX/commodities projected from FRED, EIA, and ECB, with ISIN/FIGI identity and lazy history repair.

## Architecture

```text
Sources (11 macro + Tiingo + EODHD + OpenFIGI)    Ingestion              Storage              Resolution
────────────────────────────────────────────     ──────────             ─────────            ───────────
BLS, FRED, EIA                                   Fetchers + SDMX        SQLite               resolve_indicator()
NYFed, Treasury                                  Normalization           concept_map (86)     source-priority ranking
IMF, Eurostat                                    Date alignment          obs_family           cross-source alternates
BIS, ECB, OECD                                   Deduplication           release_schedule     get_market_history()
World Bank                                                               release_status       (equity / ETF / index /
Tiingo        (US stocks + ETFs, OHLCV)          TiingoClient            market_instruments    rate / fx / commodity)
EODHD         (global + identity + delisted)     EODHDClient             market_symbol_history
OpenFIGI      (identity enrichment)              OpenFIGIClient          market_price_bars

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
                     │ Market-Data Layer (issue #1)            │
                     │  Tiingo US ETFs  → market_price_bars    │
                     │  EODHD global    → market_price_bars    │
                     │  FRED/EIA/ECB    → synthetic bars       │
                     │  IdentityRepair  → break_detected       │
                     │       ↳ EODHD ID + OpenFIGI + delisted  │
                     │       ↳ stitch → refetch → stitched     │
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
    trends/           Reddit, Weibo social trend tracking
      scrapers/       reddit.py, weibo.py
      clients/        RedditTrendIngestionClient, WeiboTrendIngestionClient
    market/           Real-time prices, EOD bars, identity, lazy repair
      scrapers/       Provider HTTP clients
        _tiingo.py       TiingoClient — US stocks + ETFs OHLCV
        _eodhd.py        EODHDClient — global EOD bars
        _eodhd_identity.py  EODHD search / delisted / symbol-change / exchanges
        _openfigi.py     OpenFIGIClient — /v3/mapping
      clients/        High-level providers
        _market.py       MarketPriceClient (yfinance, real-time watchlist)
        _tiingo.py       TiingoMarketDataProvider (P0)
        _eodhd.py        EODHDMarketDataProvider (P1 global)
        _macro_market.py MacroMarketProvider (FRED/EIA/ECB projection)
        _identity_repair.py IdentityRepairService (P2 lazy repair)
      _tiingo_universe.py  11-ETF US macro universe + ISIN/FIGI seeds
      _eodhd_universe.py   6-instrument global universe
      _macro_map.py        13-entry rates/FX/commodity mapping
      _config.py           MACRO_WATCHLIST (real-time watchlist)
    calendar/         Economic event calendars
      scrapers/       ForexFactory, Investing, TradingEconomics
    _shared/          Cross-cutting: http_transport, url_canon, selector versioning
    validation/       Data quality checks, cross-source consistency
    sources.py        IngestionOrchestrator — scheduling, retry, health
    source_capabilities.py  Unified capability registry + discovery/sync adapters
    release_schedule.py     Date-math resolvers, availability checks, alert logic
    scrapers/         Facade — backward-compatible re-exports
    clients/          Facade — backward-compatible re-exports
    fetchers/         Facade — backward-compatible re-exports
    sdmx/             Facade — backward-compatible re-exports
  storage/            SQLite schema, CRUD, concept map, release tracking
  macro_data/         CLI, HTTP server, service layer, factory
  rag/                Local RAG index and retrieval
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

# Market-data layer sources (issue #1):
macro-data-service refresh-source --source tiingo_market     # US ETFs (SPY, QQQ, ...)
macro-data-service refresh-source --source eodhd_market      # global (N225, GDAXI, ...)
macro-data-service refresh-source --source macro_market      # project FRED/EIA/ECB rows
macro-data-service refresh-source --source identity_repair   # lazy repair break_detected
```

### Resolution

```bash
macro-data-service resolve CPI_US                     # latest resolved value
macro-data-service resolve CPI_US --date 2024-01-01   # specific date
macro-data-service resolve CPI_US --history --limit 12  # resolved time series
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

## HTTP API

- `GET /health`      customer-facing source health matrix
- `GET /healthz`     lightweight liveness probe
- `POST /v1/ops/<operation>`

Key operations:

| Operation | Description |
|---|---|
| `resolve_indicator` | Highest-priority observation for a concept |
| `resolve_indicator_history` | Resolved time series with best source per date |
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
| `list_items` | Cross-type feed for a subject: documents (news / gov reports / notes) unioned with indicator observations (via `concept_map` + direct `subject_aliases`) and market-price bars (via `market_instruments`). Every row carries a `family` + `kind` tag; optional `family` filter narrows to one bucket. Still accepts `q` (FTS5, documents only), `document_type`, `country_code`, `min_confidence` |
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
- `document` extended with 11 LLM-extraction columns (`institution`,
  `authors`, `asset_class`, `impact_level`, `contains_commentary`,
  `confidence`, …) plus a `documents_fts` FTS5 virtual table.
- `obs_enrichment` sidecar — derived labels keyed by `(obs_family_id,
  date, key)`. Currently used for VIX regime classification.

Ingestion paths:

- `news` — `NewsIngestionClient` mirrors each article into `document`
  (document_type='report', source_id='news') + documents_fts +
  item_subjects, alongside the legacy `news_articles` row.
- `gov_reports` — `GovReportIngestionClient` merges scraper metadata
  with optional LLM extraction into the 17-field document surface.
- `notes` — `python -m ingestion.notes.ingest --input <dir>` parses
  YAML-frontmatter markdown into `document` (source_id='notes',
  document_type='report', confidence=1.0).
- `FEDWATCH_US` — CME-equivalent midpoint persisted daily from
  `rateprobability.com`; bridges to the `rate.us.fedwatch` subject.
- `VIX_US` — FRED `VIXCLS` close; `refresh_vix_regime` writes a
  low/elevated/stressed label into `obs_enrichment` per date.

Optional LLM enrichment of `institution`, `asset_class`, `impact_level`
and the rest of the 17-field surface is controlled by
`DOCUMENT_EXTRACT_API_KEY` (or `OPENAI_API_KEY`) plus
`DOCUMENT_EXTRACT_MODEL` / `DOCUMENT_EXTRACT_BASE_URL`. Unset → ingestion
falls back to scraper-supplied metadata without LLM calls.

### Source families (issue #5)

Every `IngestionSourceDefinition` carries a `family` tag applied at
registration via `SOURCE_FAMILIES` in `src/ingestion/sources.py`. The
tag flows through `IngestionRunReport.to_dict()`, the `list_sources`
op and the `list_items` envelope so downstream callers can group
sources and rows by type without reflecting on names:

| Family | Sources |
|---|---|
| `economic_data` | `fred_daily`, `fred_nondaily`, `fred_full`, `fred_vintages`, `bls`, `eia`, `treasury_fiscal`, `nyfed_rates`, `imf`, `imf_vintages`, `eurostat`, `bis`, `ecb`, `oecd`, `worldbank`, `worldbank_catalog` |
| `market_price` | `market`, `tiingo_market`, `eodhd_market`, `macro_market`, `identity_repair` |
| `release_report` | `gov_reports`, `fed` |
| `news` | `news` |
| `calendar` | `calendar` |
| `trend` | `reddit_trends`, `weibo_trends` |
| `signal` | `rate_probability` |

`list_items(subject="econ.cpi")` returns the merged envelope:
documents (via `item_subjects`), indicator observations (via
`subject_aliases → concept_map → indicators`, pivoted through
`concept_id` so cross-source alternates surface), and market-price
bars (via `subject_aliases → market_instruments`, matching
`primary_ticker` / `instrument_id` / values in
`provider_symbols_json`). Each row carries a `family` + `kind`
discriminator; an optional `family` filter narrows the result.

## Market-data layer

Unified surface for equities, ETFs, indices, rates, FX, and commodities.
All instruments share a stable `instrument_id`, carry ISIN/FIGI/CUSIP
columns from day one, and expose a `history_status` that lazy repair can
advance as ticker history is discovered.

### Schema (`src/storage/sqlite.py`)

- `market_instruments` — one row per instrument. Fields include
  `instrument_id`, `primary_ticker`, `asset_class`, `market`,
  `exchange_code`, `currency`, `isin`, `openfigi`, `composite_figi`,
  `share_class_figi`, `cusip`, `lei`, `primary_provider`,
  `provider_symbols_json`, `history_status`, `description_for_agent`.
- `market_symbol_history` — ticker segments (listing_start,
  ticker_rename, exchange_change, delisting, manual_link) with
  mapping_confidence in `{provider_native, auto_isin, auto_figi,
  name_match, manual}` and `valid_from`/`valid_to` boundaries.
- `market_price_bars` — daily OHLCV + adjusted_* + dividend_cash,
  split_factor, plus four boolean quality flags
  (`has_break_detected`, `has_pre2018_delisted`,
  `has_missing_corp_acts`, `has_mapping_review_needed`) and
  `quality_flags_json` for detailed diagnostics. Unique key
  `(instrument_id, date, bar_interval, source_name, source_symbol)`.

### Providers

| Source | Universe | Orchestrator source |
|---|---|---|
| Tiingo (`/tiingo/daily/<ticker>/prices`) | 11 US macro ETFs (SPY, QQQ, IWM, DIA, TLT, IEF, HYG, LQD, GLD, SLV, USO) | `tiingo_market` |
| EODHD (`/api/eod/<ticker>`) | 6 global instruments (N225, GDAXI, HSI, VWRL.LSE, SAP.XETRA, 0700.HK) | `eodhd_market` |
| FRED / EIA / ECB (projection) | 13 rates/FX/commodities (DFF, DGS2/10/30, DFII10, T10Y2Y, ECB deposit, DTWEXBGS, DEXCHUS, ECB EUR/USD daily, WTI, Brent, Henry Hub natgas) | `macro_market` |

Each provider exposes the same minimal interface:

- `seed_universe(store)` — idempotent upsert of instruments + initial segments
- `refresh_market_history(store, symbol, start, end)` — fetch + normalize + quality flags
- `get_market_history(store, symbol, start, end, adjusted)` — agent-native rows

### Quality checks

After every `refresh_market_history` the provider runs, in order: OHLC
sanity, adjusted-close-vs-close adjustment check, corporate-action skip
(split_factor ≠ 1 or dividend_cash > 0), and adjusted-close break
detection at a 50% threshold. Detected breaks flip `history_status` to
`break_detected`; status is only ever upgraded, never downgraded on
partial-window refreshes.

### Lazy repair (`identity_repair` source)

When `history_status = break_detected`, `IdentityRepairService`:

1. Reads local segments from `market_symbol_history`.
2. Queries EODHD by ISIN (`/api/search`); falls back to primary ticker
   for ISIN-less instruments (indices, macro).
3. Queries OpenFIGI `/v3/mapping` by ISIN, falls back to ticker.
4. Scans EODHD `/api/exchange-symbol-list?delisted=1` for fuzzy name
   matches (provider-local codes like `NYSEARCA`/`NASDAQ`/`NYSE`/`AMEX`
   are translated to EODHD's `US` first).
5. Parses `/api/symbol-change-history` events filtered by both ticker
   AND exchange so cross-venue bleed cannot occur.
6. Persists new segments, triggers the caller-supplied refetch
   callback, and only then promotes `history_status` from
   `break_detected` → `stitched`. No callback → status stays
   `break_detected` with a `segments_discovered_pending_refetch` note.
   No candidates at all → `manual_review`.

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
                   │  market_instruments   │ │ release_status    │
                   │  market_symbol_history│ │  PENDING / WAITING│
                   │  market_price_bars    │ │  FETCHED / CONFIRM│
                   │  (equity / etf / idx /│ │  STALE / FAILED   │
                   │   rate / fx / commod) │ └───────────────────┘
                   └───────────────────────┘
┌─────────────────┐
│  obs_family     │
│  obs_source     │
│  calendar_*     │
│  documents      │
│  market_prices  │ (legacy real-time watchlist)
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
| `TIINGO_API_KEY` | `tiingo_market` — US stocks/ETFs OHLCV | yes for P0 |
| `EODHD_API_KEY` | `eodhd_market` + `identity_repair` (search, delisted, symbol-change, exchanges-list) | yes for P1 global + P2 repair |
| `OPENFIGI_API_KEY` | `identity_repair` — higher rate limit on `/v3/mapping` | optional (anonymous tier works) |
| `LLM_API_KEY` / `OPENROUTER_API_KEY` | News LLM extraction (`ingestion.news._extract`, loaded via `get_env_value` → process env **or** `.env`) | optional; unset → keyword-based metadata |
| `DOCUMENT_EXTRACT_API_KEY` / `OPENAI_API_KEY` | Gov-report + notes 17-field LLM extraction (`make_extractor_from_env`, **reads `os.environ` only — not `.env`**) | optional; unset → scraper metadata only |
| `DOCUMENT_EXTRACT_MODEL`, `DOCUMENT_EXTRACT_BASE_URL`, `DOCUMENT_EXTRACT_CONTEXT_CHARS` | Model / endpoint / chunk-size overrides for the gov-report extractor (same process-env-only loading) | optional |

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
