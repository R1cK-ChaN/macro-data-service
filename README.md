# Macro Data Service

Institutional-grade macro-data ingestion, resolution, and observability platform. Ingests 86 economic concepts from 11 core sources, supports a 25-source capability registry, and includes release-calendar-aware scheduling, availability verification, and cross-source fallback.

## Architecture

```text
Sources (11)          Ingestion              Storage              Resolution
─────────────        ──────────             ─────────            ───────────
BLS, FRED, EIA       Fetchers + SDMX        SQLite               resolve_indicator()
NYFed, Treasury      Normalization           concept_map (86)     source-priority ranking
IMF, Eurostat        Date alignment          obs_family           cross-source alternates
BIS, ECB, OECD       Deduplication           release_schedule
World Bank                                   release_status
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
```

## Layout

```text
src/
  ingestion/          Scrapers, SDMX clients, fetcher adapters, orchestrator
    scrapers/         BLS, FRED, EIA, NYFed, Treasury, World Bank, news sources
    sdmx/providers/   IMF, Eurostat, BIS, ECB, OECD (SDMX protocol)
    source_capabilities.py  Unified capability registry + discovery/sync adapters
    release_schedule.py  Date-math resolvers, availability checks, alert logic
    sources.py        IngestionOrchestrator — scheduling, retry, health
    validation/       Data quality checks, cross-source consistency
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

## HTTP API

- `GET /health`
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
| `list_source_capabilities` | Capability registry for all known sources |
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

## Data model

```text
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  concept_map    │────▶│  indicators  │     │ release_schedule │
│  86 concepts    │     │  observations│     │  86 rules        │
│  11 sources     │     │  per source  │     │  8 rule types    │
│  priority ranks │     └──────────────┘     └────────┬────────┘
└─────────────────┘                                   │
                                              ┌───────▼────────┐
┌─────────────────┐                           │ release_status  │
│  obs_family     │                           │  PENDING        │
│  obs_source     │                           │  WAITING        │
│  calendar_*     │                           │  FETCHED        │
│  documents      │                           │  CONFIRMED      │
│  market_prices  │                           │  STALE / FAILED │
└─────────────────┘                           └────────────────┘

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

## Local run

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
macro-data-service serve --host 127.0.0.1 --port 8765
```

SQLite path: `.macro-data/engine.db`

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
