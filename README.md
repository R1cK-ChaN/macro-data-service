# Macro Data Service

Standalone macro-data service extracted from `analyst-project`.

## What lives here

- ingestion and scraper code
- SQLite storage layer
- RAG indexing and retrieval
- unified `macro_data` HTTP/CLI surface
- local compatibility factory so the service can boot without importing the agent app layer

## Layout

- `src/analyst/macro_data`: API, client, local service factory, CLI
- `src/analyst/ingestion`: scrapers and ingestion orchestration
- `src/analyst/storage`: SQLite schema and query layer
- `src/analyst/rag`: local RAG index and retrieval

## Data model layers

The service keeps structural macro knowledge separate from observations and market data:

- Structural ontology: normalized indicator, source, release-family, and observation-family tables
- Event and observation data: calendar events, indicator histories, vintages, and stored documents
- Market data: latest market prices and live market fetches

Structural ontology responses are deterministic and do not encode market-reaction edges such as `indicator -> affects -> asset`.
Market behavior stays in separate analytics and market endpoints.

## Local run

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
macro-data-service serve --host 127.0.0.1 --port 8765
```

Direct server entrypoint:

```bash
macro-data-api --host 127.0.0.1 --port 8765
```

The default SQLite path for this repo is:

```text
.macro-data/engine.db
```

## Agent wiring

Point `analyst-project` at this service with:

```text
ANALYST_MACRO_DATA_BASE_URL=http://127.0.0.1:8765
ANALYST_MACRO_DATA_API_TOKEN=
```

Because `analyst-project` now prefers the remote macro-data endpoint when that env var is set, the agent can run as a consumer without importing local ingestion/storage/RAG internals.

## HTTP API

- `GET /health`
- `POST /v1/ops/<operation>`

Ontology-oriented operations:

- `get_indicator_ontology`
- `list_indicators_by_topic`
- `list_release_families_for_indicator`
- `get_trends`

Example:

```bash
curl -s http://127.0.0.1:8765/health
curl -s -X POST http://127.0.0.1:8765/v1/ops/get_recent_releases \
  -H 'Content-Type: application/json' \
  -d '{"arguments":{"limit":5,"days":7}}'
curl -s -X POST http://127.0.0.1:8765/v1/ops/get_trends \
  -H 'Content-Type: application/json' \
  -d '{"arguments":{"limit":5,"hours":48}}'
```

## Verification

Smoke tests currently cover:

```bash
python -m pytest tests/test_macro_data_cli.py
```

The agent-side repo also contains an end-to-end HTTP integration test that starts this service as a separate process and verifies that `analyst-project` consumes it through `HttpMacroDataClient`.
