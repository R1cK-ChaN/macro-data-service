# Storage Layer

SQLite-based persistence with WAL mode and foreign keys enabled.
All tables live in a single `engine.db` file (`~/.analyst/engine.db` by default).

The structural ontology lives in the normalized `doc_*`, `obs_*`, and
`calendar_indicator*` tables. `market_prices` remains a separate market-data
store and is not linked through deterministic ontology edges.

---

## Document Storage (5-table normalized schema)

Stores government reports and official statistical releases from ~40 sources
across US, CN, JP, and EU. Designed for clean separation of source metadata,
recurring release streams, document records, content blobs, and overflow JSON.

### Tables

```
doc_source              Publisher-level info (BLS, ECB, NBS...)
  |
  +-- doc_release_family   Recurring release streams (us.bls.cpi, cn.nbs.gdp...)
        |
        +-- document          Canonical stored report record
              |
              +-- document_blob   Content by role (markdown, raw_html, raw_pdf...)
              +-- document_extra  Overflow JSON metadata
```

### doc_source

| Column | Type | Notes |
|--------|------|-------|
| `source_id` | TEXT PK | e.g. `us.bls`, `cn.nbs`, `eu.ecb` |
| `source_code` | TEXT | Short code: `bls`, `nbs` |
| `source_name` | TEXT | Full name: `BLS`, `国家统计局` |
| `source_type` | TEXT | CHECK: `government_agency`, `central_bank`, `intl_org`, `statistics_bureau`, `news_agency` |
| `country_code` | TEXT | 2-letter ISO: `US`, `CN`, `JP`, `EU` |
| `default_language_code` | TEXT | `en`, `zh` |
| `is_active` | INTEGER | 1/0 |

### doc_release_family

| Column | Type | Notes |
|--------|------|-------|
| `release_family_id` | TEXT PK | e.g. `us.bls.cpi`, `cn.pboc.lpr` |
| `source_id` | TEXT FK | -> `doc_source` |
| `release_code` | TEXT | `cpi`, `gdp`, `lpr` |
| `topic_code` | TEXT | `inflation`, `employment`, `monetary_policy` |
| `frequency` | TEXT | `monthly`, `quarterly`, `irregular` |

### document

| Column | Type | Notes |
|--------|------|-------|
| `document_id` | TEXT PK | SHA-256 prefix of URL |
| `release_family_id` | TEXT FK | -> `doc_release_family` (nullable) |
| `source_id` | TEXT FK | -> `doc_source` |
| `canonical_url` | TEXT UNIQUE | Full URL |
| `title` | TEXT | Report title |
| `document_type` | TEXT | CHECK: `release`, `bulletin`, `speech`, `methodology`, `revision_notice`, `minutes`, `statement`, `press_release`, `report`, `outlook` |
| `language_code` | TEXT | 2-letter ISO |
| `country_code` | TEXT | 2-letter ISO |
| `topic_code` | TEXT | Same codes as release_family |
| `published_date` | TEXT | `YYYY-MM-DD` |
| `published_at` | TEXT | Exact UTC ISO timestamp when known, otherwise the original `YYYY-MM-DD` date |
| `published_precision` | TEXT | `exact`, `date_only`, or `estimated` |
| `published_epoch_ms` | INTEGER | Canonical UTC publish timestamp in milliseconds |
| `status` | TEXT | CHECK: `published`, `revised`, `superseded`, `withdrawn` |
| `version_no` | INTEGER | Default 1 |
| `hash_sha256` | TEXT | Full SHA-256 of URL |
| `created_epoch_ms` | INTEGER | Canonical UTC ingest timestamp in milliseconds |
| `updated_epoch_ms` | INTEGER | Canonical UTC update timestamp in milliseconds |

### document_blob

| Column | Type | Notes |
|--------|------|-------|
| `document_blob_id` | TEXT PK | `{doc_id}_{role}` |
| `document_id` | TEXT FK | -> `document` |
| `blob_role` | TEXT | CHECK: `raw_pdf`, `raw_html`, `clean_html`, `plain_text`, `markdown` |
| `content_text` | TEXT | Text content (for markdown, plain_text, html) |
| `content_bytes` | BLOB | Binary content (for PDFs) |
| `byte_size` | INTEGER | Content size |
| `parser_name` | TEXT | e.g. `markdownify` |

### document_extra

| Column | Type | Notes |
|--------|------|-------|
| `document_id` | TEXT PK FK | -> `document` |
| `extra_json` | TEXT | JSON overflow: importance, institution, description, source-specific fields |

### Indexes

```sql
idx_document_url                  UNIQUE ON document(canonical_url)
idx_document_source_date          ON document(source_id, published_date)
idx_document_release_date         ON document(release_family_id, published_date)
idx_document_country_topic_date   ON document(country_code, topic_code, published_date)
idx_document_status               ON document(status)
idx_blob_document_role            ON document_blob(document_id, blob_role)
```

### Seeding

Sources and release families are auto-seeded from the gov_report scraper
configs on first ingestion refresh:

```python
store.seed_doc_sources_and_families({
    "us": _US_SOURCES, "cn": _CN_SOURCES,
    "jp": _JP_SOURCES, "eu": _EU_SOURCES,
})
```

This populates 16 sources and 41 release families.

### CRUD Methods

| Method | Description |
|--------|-------------|
| `upsert_doc_source()` | Insert/update a source |
| `get_doc_source()` / `list_doc_sources()` | Query sources |
| `upsert_doc_release_family()` | Insert/update a release family |
| `get_doc_release_family()` / `list_doc_release_families()` | Query families (filter by source, country, topic) |
| `upsert_document()` | Insert/update a document |
| `get_document()` / `get_document_by_url()` / `document_exists()` | Lookup documents |
| `list_documents()` | Filter by source, family, country, topic, status, type, days |
| `upsert_document_blob()` | Insert/update a blob |
| `get_document_blob()` / `list_document_blobs()` | Query blobs by doc + role |
| `upsert_document_extra()` / `get_document_extra()` | JSON overflow metadata |
| `list_items_for_subject()` / `list_items_combined()` | Subject-filtered document feed; accepts a `family` kwarg that pushes a family predicate (`news`, `note`, `calendar`, `release_report`) into SQL so the LIMIT bounds matching rows |
| `list_subject_indicators()` | Indicator observations reached from a `subject_id` — unions the `subject_aliases → concept_map → indicators` bridge (pivoted through `concept_id`, constrained by alias source) with a direct `subject_aliases → indicators` path; applies per-series fair-share before the cap |
| `list_subject_market_bars()` | Market-price bars reached from a `subject_id` via `primary_ticker` / `instrument_id` / `provider_symbols_json` matches against subject aliases |

---

## Observation Family Storage (3-table hierarchy)

Formalizes the observation/indicator side with a parallel hierarchy to the
document schema. Connects numeric data streams (CPI = 3.2%, Fed Funds = 4.33%)
to their document publication streams (BLS CPI report, FOMC statement).

### Tables

```
obs_source               Data provider info (FRED, EIA, NY Fed...)
  |
  +-- obs_family           Series definitions (us.inflation.cpi_all, us.rates.sofr...)
        |
        +-- indicators       Existing time series (linked via obs_family_id)
        +-- indicator_vintages  Existing revision data (linked via obs_family_id)

obs_family_document      Links obs_family <-> doc_release_family
```

### obs_source

| Column | Type | Notes |
|--------|------|-------|
| `source_id` | TEXT PK | `fred`, `eia`, `treasury_fiscal`, `nyfed`, `rateprobability` |
| `source_code` | TEXT | Short code |
| `source_name` | TEXT | Full name |
| `source_type` | TEXT | CHECK: `data_aggregator`, `government_agency`, `central_bank`, `exchange`, `market_data` |
| `country_code` | TEXT | 2-letter ISO |
| `homepage_url` | TEXT | Provider homepage |
| `api_base_url` | TEXT | API endpoint base |
| `is_active` | INTEGER | 1/0 |

### obs_family

| Column | Type | Notes |
|--------|------|-------|
| `family_id` | TEXT PK | e.g. `us.inflation.cpi_all`, `us.rates.sofr` |
| `source_id` | TEXT FK | -> `obs_source` |
| `provider_series_id` | TEXT | Maps to `indicators.series_id` (e.g. `CPIAUCSL`) |
| `canonical_name` | TEXT | Human-readable name |
| `unit` | TEXT | `index`, `percent`, `billions_usd`, etc. |
| `frequency` | TEXT | CHECK: `daily`, `weekly`, `monthly`, `quarterly`, `annual`, `irregular` |
| `seasonal_adjustment` | TEXT | CHECK: `sa`, `nsa`, `saar`, `none` |
| `country_code` | TEXT | 2-letter ISO |
| `topic_code` | TEXT | `inflation`, `employment`, `rates`, `energy`, `fiscal` |
| `category` | TEXT | `cpi_all`, `treasury_yields`, etc. |
| `has_vintages` | INTEGER | 1 if series has revision history |

### obs_family_document

| Column | Type | Notes |
|--------|------|-------|
| `family_id` | TEXT FK | -> `obs_family` |
| `release_family_id` | TEXT FK | -> `doc_release_family` |
| `relationship` | TEXT | CHECK: `produced_by`, `derived_from`, `related_to` |
| PRIMARY KEY | | `(family_id, release_family_id)` |

### Indexes

```sql
idx_obs_family_source            ON obs_family(source_id)
idx_obs_family_country_topic     ON obs_family(country_code, topic_code)
idx_obs_family_provider_series   UNIQUE ON obs_family(source_id, provider_series_id)
idx_indicators_family_date       ON indicators(obs_family_id, date)
idx_vintages_family_date         ON indicator_vintages(obs_family_id, observation_date)
idx_obs_family_doc_release       ON obs_family_document(release_family_id)
```

### ALTER TABLE migrations

Both `indicators` and `indicator_vintages` gain a nullable `obs_family_id TEXT`
column, populated via backfill after seeding.

### Seeding

Auto-seeded on first `IngestionOrchestrator.refresh_all()`:

```python
store.seed_obs_sources_and_families()   # 5 sources, 37 families
store.backfill_obs_family_ids()         # populate existing rows
```

Seed data: 26 FRED series + 5 EIA + 3 Treasury Fiscal + 3 NY Fed = 37 families.
10 obs_family_document links connect observation families to document release families.

### CRUD Methods

| Method | Description |
|--------|-------------|
| `upsert_obs_source()` | Insert/update a source |
| `get_obs_source()` / `list_obs_sources()` | Query sources |
| `upsert_obs_family()` | Insert/update a family |
| `get_obs_family()` / `get_obs_family_by_series()` | Lookup by family_id or (source, series) |
| `list_obs_families()` | Filter by source, country, topic, frequency |
| `upsert_obs_family_document()` | Insert/update a link |
| `list_obs_families_for_release()` | Obs families linked to a doc release family |
| `list_releases_for_obs_family()` | Doc releases linked to an obs family |
| `list_release_families_for_indicator()` | Doc releases linked to a normalized calendar indicator |
| `seed_obs_sources_and_families()` | Populate all seed data |
| `backfill_obs_family_ids()` | Set obs_family_id on existing indicator rows |
| `build_obs_family_lookup()` | Build (source, series) -> family_id dict |
| `seed_structural_ontology()` | Seed doc sources, release families, obs families, and calendar indicators for ontology queries |

---

## Indicator Vintage Storage

Tracks **revision history** for macro series (GDP, CPI, payrolls, etc.) where
official agencies publish initial estimates then revise them over subsequent
releases. The `indicators` table always holds the **latest** value; the
`indicator_vintages` table stores the **full revision timeline**.

### indicator_vintages

| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER PK | Auto-increment |
| `series_id` | TEXT | e.g. `GDP`, `CPIAUCSL`, `PAYEMS` |
| `source` | TEXT | e.g. `fred` |
| `observation_date` | TEXT | The date being measured (`YYYY-MM-DD`) |
| `vintage_date` | TEXT | When this value was published/revised (`YYYY-MM-DD`) |
| `value` | REAL | The observed value at this vintage |
| `metadata_json` | TEXT | JSON: `{"name": "GDP"}` |
| `scraped_at` | TEXT | ISO timestamp of ingestion |

**Unique constraint:** `(series_id, source, observation_date, vintage_date)`

Same observation_date can have multiple vintage_dates showing how a value
changed over time (e.g. GDP advance → second → third estimate).

### CRUD Methods

| Method | Description |
|--------|-------------|
| `upsert_indicator_vintage(vintage)` | Insert/update a vintage record |
| `get_vintage_history(series_id, observation_date)` | All vintages for one observation, ordered by vintage_date ASC |
| `get_vintages_for_series(series_id, *, limit=50)` | Most recent vintage records for a series |

### Data Record

```python
@dataclass(frozen=True)
class IndicatorVintageRecord:
    series_id: str
    source: str
    observation_date: str   # the date being measured
    vintage_date: str       # when this measurement was published
    value: float
    metadata: dict[str, Any] = field(default_factory=dict)
```

### Key Vintage Series (ALFRED)

`GDP`, `GDPC1`, `CPIAUCSL`, `PAYEMS`, `UNRATE`, `INDPRO`, `RSAFS` —
monthly/quarterly macro that gets revised across releases.

---

## Cross-Source Concept Map

Groups equivalent series under a single concept (e.g. `CPI_US` maps to both
FRED `CPIAUCSL` and BLS `CUUR0000SA0`). The `priority` column encodes
domain-knowledge precedence so the system can answer "which value is
authoritative?" without ambiguity.

### concept_map

| Column | Type | Notes |
|--------|------|-------|
| `concept_id` | TEXT | e.g. `CPI_US`, `POLICY_RATE_US` |
| `source_id` | TEXT | e.g. `fred`, `bls`, `nyfed` |
| `provider_series_id` | TEXT | Maps to `indicators.series_id` |
| `obs_family_id` | TEXT | FK to `obs_family.family_id` (may be empty) |
| `priority` | INTEGER | 1 = authoritative, 2 = secondary, 3 = tertiary |
| `role` | TEXT | CHECK: `primary`, `secondary`, `cross_check` |
| `notes` | TEXT | Free-form description |
| PRIMARY KEY | | `(concept_id, source_id, provider_series_id)` |

### Priority assignments

| Concept family | p=1 | p=2 | p=3 |
|----------------|-----|-----|-----|
| CPI/PPI/Core CPI (US) | bls | fred | — |
| UNEMP_US | bls | fred | oecd |
| NFP, JOLTS, ECI, productivity | bls | — | — |
| POLICY_RATE_US | nyfed | fred | bis |
| SOFR, OBFR | nyfed | — | — |
| TGA_US | fred | treasury_fiscal | — |
| DOLLAR_INDEX_US | fred | bis | — |
| CPI_EU | eurostat | imf | — |
| POLICY_RATE_EU | ecb | bis | — |
| All single-source concepts | 1 | — | — |

### Data Records

```python
@dataclass(frozen=True)
class ConceptMapRecord:
    concept_id: str
    source_id: str
    provider_series_id: str
    obs_family_id: str
    priority: int = 0
    role: str = "primary"
    notes: str = ""

@dataclass(frozen=True)
class ResolvedObservation:
    concept_id: str
    date: str
    value: float
    source_id: str
    provider_series_id: str
    priority: int
    role: str
    alternates: int = 0   # how many other sources also had this date
```

### Resolution Methods

| Method | Description |
|--------|-------------|
| `seed_concept_map()` | Populate/update concept_map from built-in definitions |
| `get_concept_series(concept_id)` | All mappings for a concept, ordered by priority |
| `list_concepts(*, country_code=None)` | Distinct concept IDs |
| `get_concept_observations(concept_id)` | Raw (source, series, date, value) across all sources |
| `get_concept_stats(concept_id)` | Per-source row counts and date ranges |
| `resolve_indicator(concept_id, *, date=None)` | Highest-priority observation (latest if no date) |
| `resolve_indicator_history(concept_id, *, limit=12)` | Resolved time series with per-date fallback |

Resolution is done in Python — at most 3-4 sources per concept and limited dates, trivially fast.

---

## Source Capability Catalog

Tracks the provider/source capability layer introduced for catalog discovery,
entity persistence, and latest-sync observability.

### Tables

| Table | Purpose |
|-------|---------|
| `source_capability` | One row per source capability adapter: mode, entity type, and support flags |
| `catalog_entity` | Discovered provider entities or fixed-scope supported entities |
| `catalog_sync_checkpoint` | Per-source checkpoint/status for `discovery` and `latest_sync` jobs |
| `catalog_sync_run` | Historical sync run log with counts, duration, and error metadata |

### source_capability

Key columns:

- `source_id`: stable source key such as `oecd`, `worldbank`, `news`, `eia`
- `source_type`: `catalog-crawlable`, `discovery-rich`, or `fixed-scope-complete`
- `entity_type`: `dataflow`, `dataset`, `feed`, `route`, `symbol`, etc.
- `supports_discovery`, `supports_structure`, `supports_latest_sync`, `supports_backfill`
- `is_default_scheduled`: whether the source is in the default orchestrator refresh order

### catalog_entity

Stores the discovered or enumerated entity surface for a source:

- `entity_id`: provider key such as a dataflow id, dataset id, route, or feed key
- `display_name`: human-readable label
- `description`: short source/provider description
- `metadata_json`: provider-specific overflow metadata (agency/version/params/url/etc.)

### catalog sync methods

| Method | Description |
|--------|-------------|
| `upsert_source_capability(payload)` | Insert/update a source capability row |
| `list_source_capabilities()` / `get_source_capability()` | Query capability registry |
| `upsert_catalog_entity(payload)` | Insert/update a discovered entity |
| `list_catalog_entities(source_id, ...)` / `count_catalog_entities(source_id)` | Query stored entities |
| `upsert_catalog_sync_checkpoint(payload)` / `get_catalog_sync_checkpoint()` | Persist checkpoint/status |
| `insert_catalog_sync_run(payload)` / `update_catalog_sync_run()` / `list_catalog_sync_runs()` | Track sync run history |
| `get_source_storage_stats(source_id)` | Aggregate customer-facing source record count + latest ingest timestamp |

These tables are intentionally separate from `concept_map` and `indicators`:
capability/catalog state describes what a source *can* expose, while
`concept_map` and `indicators` remain the curated resolution layer.

---

## Other Tables

| Table | Purpose |
|-------|---------|
| `calendar_events` | Legacy economic calendar compatibility table; HTML refresh retired |
| `market_prices` | Asset price snapshots (yfinance) |
| `central_bank_comms` | Fed speeches, statements, testimony (RSS feeds) |
| `obs_source` | Observation data providers (FRED, EIA, Treasury Fiscal, NY Fed, rateprobability) |
| `obs_family` | Series definitions — canonical metadata for each observation stream |
| `obs_family_document` | Links observation families to document release families |
| `indicators` | Time series macro data (FRED, EIA, Treasury Fiscal, NY Fed, rate probabilities) |
| `indicator_vintages` | Revision history for macro series (ALFRED vintage data) |
| `news_articles` | News + gov reports (FTS5 full-text search) |
| `regime_snapshots` | Market regime JSON snapshots |
| `generated_notes` | AI-generated analysis notes |
| `analytical_observations` | Observations & insights |
| `research_artifacts` | Research documents with tags |
| `trade_signals` | Trading signals with rationale |
| `decision_log` | Decision tracking |
| `position_state` | Current portfolio positions |
| `source_capability` | Capability registry for catalog/discovery/source-mode metadata |
| `catalog_entity` | Persisted catalog entities or fixed-scope source entities |
| `catalog_sync_checkpoint` | Latest discovery/latest-sync checkpoint state |
| `catalog_sync_run` | Historical discovery/latest-sync run log |
| `performance_records` | Trading performance metrics |
| `trading_artifacts` | Trading strategy documents |
| `client_profiles` | User profiles (20+ dimensions) |
| `conversation_threads` / `conversation_messages` | Chat history |
| `delivery_queue` | Content delivery to users |
| `group_profiles` / `group_members` / `group_messages` | Group chat |
| `portfolio_holdings` / `portfolio_vol_snapshots` / `portfolio_alerts` | Portfolio management |
| `subagent_runs` | Sub-agent task tracking |

---

## Unified calendar (issue #8)

Two physical lanes, one downstream contract. Storage is split because
mandatory fields diverge (country+indicator vs ticker+subtype) and revision
semantics differ (TE exposes `LastUpdate`; EODHD does not). Consumers read
`v_calendar_item`, which `UNION ALL`s both lanes into the `CalendarItem`
contract shape.

### Shared dim

| Column | Type | Notes |
|--------|------|-------|
| `provider_id` | TEXT PK | e.g. `tradingeconomics`, `eodhd`, later `bls`, `ecb` |
| `domain` | TEXT PK | CHECK: `economic`, `corporate` |
| `provider_type` | TEXT | CHECK: `data_aggregator`, `government_agency`, `central_bank`, `exchange`, `market_data` |
| `precedence` | INTEGER | Higher wins when multiple providers cover the same event; official sources land above aggregators |
| `is_active` | INTEGER | 1/0 |

PK is `(provider_id, domain)` so one provider can serve both lanes (e.g.
EODHD could later expose economic-events alongside corporate).

Seeded on `init_schema` (issue #8 + issue #9 P0):

| provider_id       | provider_type     | domain    | precedence |
|-------------------|-------------------|-----------|------------|
| `tradingeconomics`| `data_aggregator` | economic  | 10         |
| `eodhd`           | `data_aggregator` | corporate | 10         |
| `bls`             | `government_agency` | economic | 100       |
| `bea`             | `government_agency` | economic | 100       |
| `census`          | `government_agency` | economic | 100       |
| `ism`             | `market_data`     | economic  | 100        |
| `federal-reserve` | `central_bank`    | economic  | 100        |
| `ecb`             | `central_bank`    | economic  | 100        |
| `nbs`             | `government_agency` | economic | 100       |

Precedence is a ranking signal, not a view-level filter. The current
`v_calendar_item` VIEW is a plain `UNION ALL` across both lanes and
surfaces every provider row. Conflict resolution on
`(country_code, event_time_utc, canonical_indicator)` is issue #9 P6's
parity-harness responsibility — that's the first caller that actually
reads the `precedence` column.

### Economic lane

```
cal_econ_raw     Append-only JSON snapshots (one row per revision)
  |
  └── content_hash = SHA256 over mutable fields
        (Actual, Previous, Revised, Forecast, TEForecast, LastUpdate)

cal_econ_event   PIT projection — one row per upstream event, typed columns

cal_econ_drops   Audit stream for upstream-retired IDs
```

`cal_econ_event` key columns: `provider`, `provider_event_id`,
`event_time_utc`, `event_time_precision`, `country_code NOT NULL`,
`indicator_id FK→calendar_indicator`, `importance`, `currency`, `unit`,
`actual`, `previous`, `revised`, `forecast`, `consensus_forecast`,
`source_url`, `last_update_epoch_ms`, `content_hash`. Numeric value fields
stay TEXT (TE returns them as strings, possibly empty); coerce at query
layer.

### Corporate lane

```
cal_corp_raw     Append-only JSON snapshots (content_hash = only revision signal)

cal_corp_event   PIT projection — typed columns + payload_json overflow
```

`cal_corp_event` key columns: `provider`, `provider_event_id`,
`event_subtype CHECK IN ('earnings','ipo','split','dividend','earnings_trend')`,
`event_time_utc`, `event_time_precision`, `ticker NOT NULL`, `exchange`,
`currency`, `currency_reporting`, `title`, `source_url`, `content_hash`,
`payload_json`. Subtype-specific fields (price_from/to/offer, old/new shares,
actual/estimate EPS, `deal_type` lifecycle) live in `payload_json` — the raw
layer already holds the full upstream row. Promote a field to a typed
column only when three concrete queries need SQL-level access.

### Backfill cursor (issue #8 slice 2)

`cal_backfill_cursor` — per-`(provider, phase)` resumability for the
economic-lane API fetcher. One row per phase keeps the recent / mid /
early sweeps independent so a mid-sweep budget breach only rewinds its
own phase.

| Column | Type | Notes |
|--------|------|-------|
| `provider` | TEXT PK | e.g. `tradingeconomics` |
| `phase` | TEXT PK | `p1_recent`, `p2_mid`, `p3_early` |
| `cursor_date` | TEXT | Next `window_start` to execute (ISO date) |
| `window_end_date` | TEXT | End date of the last completed window |
| `rows_ingested` | INTEGER | Cumulative across resumes |
| `requests_spent` | INTEGER | Cumulative across resumes |
| `last_run_at` | TEXT | ISO timestamp of the last executed window |
| `is_complete` | INTEGER | 1 when the phase's `window_end_date` ≥ phase end |

Budget accounting also reads from this table (sum `requests_spent` across
phases for the current month) — no separate `cal_budget` table.

### Unified read view

`v_calendar_item` projects both tables into the `CalendarItem` column set:
`event_id` (as `provider:provider_event_id`), `domain`, `subtype`,
`provider`, `event_time_utc`, `title`, `country`, `ticker`, `exchange`,
`currency`, `importance`, `indicator_id`, `reference_date`, `actual`,
`previous`, `forecast`, `consensus_forecast`, `source_url`,
`last_update_epoch_ms`, `observed_at_epoch_ms`.

HTTP surface: `GET /v1/calendar?domain=economic&country=US` or
`?domain=corporate&ticker=AAPL&subtype=earnings` reads this view.

### Relationship to legacy `calendar_events`

`calendar_events` is retained as a compatibility table for old local DBs.
The HTML-scraped live refresh path is retired; legacy read helpers use
`cal_econ_event`, and new consumers read `v_calendar_item` through
`GET /v1/calendar`.

## Record Dataclasses (`storage/models/`)

Issue #58 Tier 2.1A extracted 40 frozen record dataclasses out of
`sqlite.py` into per-domain submodules under `src/storage/models/`. Each
file groups records by the bounded context they belong to:

```
storage/models/
  __init__.py     — re-exports every record name; backwards-compatible.
  analytical.py   — RegimeSnapshotRecord, GeneratedNoteRecord,
                    AnalyticalObservationRecord, ResearchArtifactRecord.
  calendar.py     — StoredEventRecord, CalendarIndicatorRecord,
                    CalendarIndicatorAliasRecord,
                    CalendarEventVintageRecord.
  documents.py    — DocSourceRecord, DocReleaseFamilyRecord,
                    DocumentRecord, DocumentBlobRecord,
                    DocumentExtraRecord.
  indicator.py    — IndicatorObservationRecord, IndicatorVintageRecord,
                    ObsSourceRecord, ObsFamilyRecord,
                    ObsFamilyDocumentRecord, ConceptMapRecord,
                    ResolvedObservation, ReleaseScheduleRecord,
                    ReleaseStatusRecord, CentralBankCommunicationRecord.
  market.py       — MarketPriceRecord, MarketInstrumentRecord,
                    MarketSymbolHistoryRecord, MarketPriceBarRecord.
  messaging.py    — ClientProfileRecord, ConversationMessageRecord,
                    DeliveryQueueRecord, GroupProfileRecord,
                    GroupMemberRecord, GroupMessageRecord.
  news.py         — NewsArticleRecord, TrendTopicRecord.
  trading.py      — TradeSignalRecord, DecisionLogRecord,
                    PositionStateRecord, PerformanceRecord,
                    TradingArtifactRecord.
```

`storage.sqlite` re-exports every record name, so existing imports
(`from storage.sqlite import StoredEventRecord`) keep working
unchanged. New consumers should prefer the per-domain import path
(`from storage.models.calendar import StoredEventRecord`) for tighter
dependency surfaces and to avoid pulling the full `EngineStore`
graph for record-only use cases.

When adding a new record, drop it into the matching per-domain file
and append the name to both the file's `__all__` (if present) and the
re-export block in `storage/models/__init__.py`. Keep records frozen
(`@dataclass(frozen=True)`) and leave all DDL in `storage/schema.py`,
read/write helpers in `storage/queries/{domain}.py`.

## Schema DDL (`storage/schema.py`)

Issue #71 Tier 2.1B-1 extracted every `CREATE TABLE` / `CREATE INDEX` /
additive `ALTER` from `SQLiteEngineStore.init_schema` into a free
function `apply_schema(connection)` in `storage/schema.py`. The
`SQLiteEngineStore.init_schema` wrapper opens a commit-bracketed
connection and delegates to it. `_ensure_table_columns` moved alongside
as a module-private helper.

When adding a new table or index, edit `storage/schema.py`. When adding
an additive column to an existing table, use `_ensure_table_columns`
inside `apply_schema` so existing engine.db files migrate forward
without losing data.

## Per-Domain Queries (`storage/queries/`)

Issue #71 Tier 2.1B-2 extracted the ~165 `SQLiteEngineStore` methods
into 8 per-domain mixin modules under `src/storage/queries/`. Each
domain owns its tables' read/write paths plus any module-level seed
data and helpers that pair with them:

```
storage/queries/
  __init__.py     — package marker; mixins are imported by storage.sqlite directly.
  analytical.py   — regime_snapshots, generated_notes, analytical_observations,
                    subagent_runs, research_artifacts.
  calendar.py     — calendar_events + calendar_event_vintages +
                    calendar_indicator + calendar_indicator_alias +
                    release_schedule + release_status. Owns the module-level
                    free helpers (_calendar_country_code,
                    _add_event_time_lower_bound, _calendar_surprise, …) and
                    the timestamp-safety helpers (_safe_epoch_ms,
                    _safe_utc_iso, _infer_timestamp_precision,
                    _matches_scope_tags) that other domain mixins import.
                    Also owns _CALENDAR_INDICATOR_DEFS, _CALENDAR_ALIAS_DEFS,
                    _RELEASE_SCHEDULE_DEFS seed lists.
  documents.py    — doc_source + doc_release_family + document +
                    document_blob + document_extra + documents_fts +
                    item_subjects + cross-cutting subject queries
                    (list_items_combined, list_subject_indicators,
                    list_subject_market_bars).
  indicator.py    — indicators + indicator_vintages + central_bank_comms +
                    obs_source / obs_family / obs_family_document +
                    concept_map + obs_enrichment + subjects (taxonomy table
                    reads) + source_capability / catalog_entity /
                    catalog_sync_*. Owns the per-source family-map seed
                    dicts (_FRED_FAMILY_MAP, _EIA_FAMILY_MAP, _OBS_SOURCE_DEFS,
                    _OBS_DOC_LINKS, _VINTAGE_FAMILY_IDS).
  market.py       — market_prices, market_instruments, market_symbol_history,
                    market_price_bars, plus portfolio_holdings /
                    portfolio_vol_snapshots / portfolio_alerts.
  messaging.py    — client_profiles, conversation_threads, delivery_queue,
                    group_profiles / group_members / group_messages plus
                    the search-scoring helpers (_search_terms,
                    _score_text_match, _recency_decay).
  news.py         — news_articles + trend_topics + article_fingerprint +
                    news_context scoring (with the impact-decay constants).
  trading.py      — trade_signals, decision_log, position_state,
                    performance_records, trading_artifacts.
```

Each domain module exposes a private `_XQueriesMixin` class.
`SQLiteEngineStore` composes them via multiple inheritance, mirroring
the `macro_data.service` mixin layout shipped in issue #58 Tier 1.1.
Cross-mixin method calls resolve at runtime via Python's MRO — e.g.,
`seed_structural_ontology` (indicator) calling `self.seed_doc_sources_and_families`
(documents) and `self.seed_calendar_indicators` (calendar) Just Works.

`storage.sqlite` re-exports `append_calendar_event_vintage_if_changed_with_conn`
from `queries.calendar` for backwards compatibility — ingestion code
outside the EngineStore imports the helper from `storage.sqlite`.

When adding a new method, drop it into the matching per-domain mixin
file. When adding a new domain, follow the existing layout
(`_XQueriesMixin` private class, free helpers / seed data above the
class, mixin added to the inheritance list in `storage/sqlite.py`).

## Running Tests

```bash
python3 -m pytest tests/test_document_storage.py tests/test_obs_family.py -v
python3 -m pytest tests/test_normalization_and_concept_map.py -v   # includes resolution tests
```
