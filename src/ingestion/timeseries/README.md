# Time-series Ingestion

Macro indicators, rates, vintages, and statistical series — anything
that lives in `obs_*` (or `obs_vintage*`) downstream. Calendar /
corporate-actions live next door in `src/ingestion/calendar/` and have
their own README.

The package is layered: each upstream provider gets one file per layer.
Pick the layer that matches what you're doing, not the provider.

## Layers

```
scrapers/   — direct upstream-API clients. Pure HTTP + payload parsing.
              No DB. Outputs typed records (e.g. FredObservation).
              Example: scrapers/fred.py defines FredClient + the typed
              observation/vintage dataclasses.

clients/    — ingestion orchestration. Refreshes daily / full / vintage
              series end-to-end (scraper → record dedup → store).
              Owns "RefreshStats" reporting.
              Example: clients/_fred.py defines FREDIngestionClient.

fetchers/   — v2 pipeline adapters. Wrap a scraper and return
              list[RawSeries] for the Fetcher → Normalizer → Store path
              that IngestionOrchestrator drives.
              Example: fetchers/_fred.py defines FredFetcher.

sdmx/       — shared SDMX infrastructure + per-organization providers.
              `_base_client.py`, `_base_ingestion_client.py`,
              `_json_parser.py`, `_parsing.py`, `_types.py` are reused
              across all SDMX sources. `providers/` has one file per
              org: bis, ecb, eurostat, ilo, imf, oecd, unsd.
```

Top-level files:

- `_config.py` — module-level config defaults shared by clients/fetchers.
- `regimes.py` — regime-detection utilities used by indicator analysis.
- `__init__.py` — empty surface; importers reach into the layered
  subpackages directly (see the canonical-imports note in the file).

## Where to add X

| Adding | Touch | Notes |
|---|---|---|
| New upstream HTTP client | `scrapers/<name>.py` | Define the API client + typed record dataclasses. No DB writes. |
| New end-to-end refresh job | `clients/_<name>.py` | Define `<NAME>IngestionClient` with `refresh_*` methods. Pulls from the scraper, dedups, calls `store.bulk_insert_*`. Register the job in `src/ingestion/sources.py::_build_<name>_source`. |
| New v2 fetcher (pipeline) | `fetchers/_<name>.py` | Define `<Name>Fetcher` returning `list[RawSeries]`. Wire into the same `_build_<name>_source` if it goes through the v2 path. |
| New SDMX provider | `sdmx/providers/<org>.py` | Subclass `_base_client.SDMXBaseClient` + `_base_ingestion_client.SDMXBaseIngestionClient`. Reuse the shared `_json_parser` / `_parsing` / `_types`. |
| New series for an existing source | `src/ingestion/series_config.py` | Add the series id / dataset entry. Don't touch the scraper unless the API surface itself changed. |

## Operating rules

- Scrapers must not write to the DB. Clients are the only layer that
  touches `self._store`.
- Per-provider files are named with a leading underscore in `clients/`
  and `fetchers/` (`_fred.py`, `_oecd.py`) but not in `scrapers/`
  (`fred.py`). The underscore matches the historical "private module"
  convention in those two subpackages.
- All SDMX providers go through the shared `_base_*` classes — never
  ship a one-off SDMX HTTP client outside `sdmx/`.
- Series-level configuration (which datasets / series ids / dimensions
  to ingest) belongs in `src/ingestion/series_config.py`, not in the
  scrapers or clients.
