# Storage Layer

SQLite-based persistence with WAL mode and foreign keys enabled.
All tables live in a single `engine.db` file (`~/.analyst/engine.db` by default).

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
| `seed_obs_sources_and_families()` | Populate all seed data |
| `backfill_obs_family_ids()` | Set obs_family_id on existing indicator rows |
| `build_obs_family_lookup()` | Build (source, series) -> family_id dict |

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

## Other Tables

| Table | Purpose |
|-------|---------|
| `calendar_events` | Economic calendar (Investing, ForexFactory, TradingEconomics) |
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
| `performance_records` | Trading performance metrics |
| `trading_artifacts` | Trading strategy documents |
| `client_profiles` | User profiles (20+ dimensions) |
| `conversation_threads` / `conversation_messages` | Chat history |
| `delivery_queue` | Content delivery to users |
| `group_profiles` / `group_members` / `group_messages` | Group chat |
| `portfolio_holdings` / `portfolio_vol_snapshots` / `portfolio_alerts` | Portfolio management |
| `subagent_runs` | Sub-agent task tracking |

## Running Tests

```bash
python3 -m pytest tests/test_document_storage.py tests/test_obs_family.py -v
```
