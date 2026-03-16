# analyst/rag — Macro-Economic RAG Retrieval Engine

Hybrid dense + sparse (BM25) retrieval with RRF fusion, reranking, and
coverage-aware selection.  Backed by **SQLite + numpy** for lightweight
single-VPS deployment.

Vendored from `rag-service` and adapted for macro-economic content.

## What goes into RAG (and what doesn't)

RAG only ingests **unstructured text** that benefits from semantic search:

| Source | SQLite table | Why RAG |
|--------|-------------|---------|
| News articles | `news_articles` | Semantic search finds relevant narratives even without exact keywords |
| Fed / central bank comms | `central_bank_comms` | Long speeches and minutes need semantic understanding |
| Research artifacts | `research_artifacts` | Analyst's own generated notes and flash commentaries |

**NOT in RAG** — structured time-series data already served by dedicated SQL-backed tools:

| Source | Tool | Why not RAG |
|--------|------|-------------|
| Indicators (EFFR, SOFR, CPI, etc.) | `build_indicator_history_tool` | Exact SQL queries with date/series filtering; semantic search on numbers is meaningless |
| Calendar events (actual vs forecast) | `build_live_calendar_tool` | Structured data with exact values; better served by SQL |

## Architecture

```
SQLite engine.db
  └─ rag_chunks table (text + dense BLOB + sparse JSON + metadata)

Retrieval flow:
  query → embed (OpenAI) ──┐
                            ├─ RRF fusion → dedup → time-decay
  query → BM25 sparse ─────┘     → coverage selection → rerank (optional)
                                       → evidence bundle

Time boundary (hard cutoff per mode):
  BRIEFING=7d, REGIME=14d, QA=30d, RESEARCH=60d
  Old articles are excluded regardless of semantic similarity.
  User can override via explicit `days` parameter.
```

### Retrieval strategy

1. **Dual-path search**: query is embedded (OpenAI 3072-dim) for dense search AND tokenized for BM25 sparse search, both run against the same `rag_chunks` table
2. **RRF fusion** (k=60): merges dense and sparse ranked lists into a unified score
3. **Source-type weighting**: policy can boost/demote source types (e.g. Fed comms 1.3x in REGIME mode)
4. **Time-decay**: exponential decay — news half-life 7 days, Fed comms 14 days. Recent content scores higher.
5. **Hard time boundary**: `default_days` per policy ensures stale content never appears (not just scored lower, but filtered out entirely)
6. **Dedup**: by content_hash to remove duplicate chunks
7. **Coverage selection**: ensures diversity across source types (e.g. max 4 news, 3 Fed comms, 3 research)
8. **Neighbor expansion**: optionally pulls adjacent chunks (±1) for context continuity
9. **Reranker** (optional): Jina API cross-encoder for final re-scoring
10. **Fallback**: if coverage fails, stage 1 increases budget ×1.5, stage 2 disables filters

### Retrieval modes

| Mode | Dense top_k | BM25 top_k | Final K | Time window | Use case |
|------|------------|------------|---------|-------------|----------|
| **BRIEFING** | 15 | 20 | 12 | 7 days | Morning/after-market briefings |
| **REGIME** | 15 | 10 | 10 | 14 days | Regime state assessment |
| **QA** | 15 | 15 | 8 | 30 days | Free-form user questions |
| **RESEARCH** | 20 | 15 | 10 | 60 days | Flash commentary, deep-dive |

### Key modules

| Module | Description |
|--------|-------------|
| `vector_store.py` | SQLite table `rag_chunks` + numpy brute-force IP search |
| `embeddings.py` | OpenAI `text-embedding-3-large` (3072-dim) |
| `bm25.py` | BM25 sparse vectors via mmh3 hashing (10M buckets) |
| `pipeline.py` | Core retrieval: dense+sparse → RRF → time-decay → coverage |
| `reranker.py` | Optional Jina API reranker |
| `retriever.py` | `MacroRetriever` — high-level wrapper owning all components |
| `chunker.py` | Content-type-aware chunking (news, Fed comms, research) |
| `bridge.py` | `MacroIngestionBridge` — SQLite → chunk → embed → vector store |
| `policies/*.yaml` | Policy definitions for 4 retrieval modes |

## Embedding frequency

Embeddings are **not automatic**. They run only on explicit CLI commands:

| Command | What happens | When to use |
|---------|-------------|-------------|
| `analyst-cli rag calibrate` | Drop all chunks, re-embed everything | First time, or after chunker/schema changes |
| `analyst-cli rag sync` | Embed only new rows (rowid > watermark) | After each data refresh cycle |
| `analyst-cli rag status` | Show chunk count + watermarks | Monitoring |

After running `refresh --once` or any news/comms scraper, call `rag sync` to
embed new content. There is no auto-trigger yet.

## Configuration

All via environment variables (read from `.env` by `analyst.env.get_env_value`):

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | — | Required for embeddings |
| `ANALYST_RAG_DB_PATH` | `engine.db` | SQLite DB path (shared with analyst engine) |
| `ANALYST_EMBEDDING_MODEL` | `text-embedding-3-large` | OpenAI model |
| `ANALYST_EMBEDDING_DIM` | `3072` | Embedding dimension |
| `ANALYST_ENABLE_RERANKER` | `false` | Enable Jina reranker |
| `ANALYST_RERANKER_API_KEY` | — | Jina API key |
| `ANALYST_BM25_STATS_DIR` | — | Directory for BM25 stats JSON |
| `ANALYST_RAG_POLICY_DIR` | `policies/` | Policy YAML directory |

## Agent Tool

The `search_knowledge_base` tool is automatically registered when `MacroRetriever`
initializes successfully.  Parameters: `query`, `mode`, `country`,
`indicator_group`, `impact_level`, `content_type`, `days`, `limit`.

## Dependencies

- `openai>=1.10.0` — embeddings
- `numpy>=1.24` — brute-force vector search
- `mmh3>=4.1.0` — BM25 hash buckets
- `PyYAML` — policy loading
