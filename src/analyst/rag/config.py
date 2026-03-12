"""RAG configuration — reads ANALYST_* env vars via ``get_env_value``."""

from __future__ import annotations

from dataclasses import dataclass

from analyst.env import get_env_value


@dataclass(frozen=True)
class RAGConfig:
    # SQLite DB path (shares the analyst engine.db by default)
    db_path: str = ""

    # Embeddings
    openai_api_key: str = ""
    embedding_model: str = "text-embedding-3-large"
    embedding_dim: int = 3072

    # Reranker
    enable_reranker: bool = False
    reranker_provider: str = "jina"
    reranker_model: str = "jina-reranker-v3"
    reranker_api_key: str = ""
    reranker_api_base: str = "https://api.jina.ai/v1"
    reranker_timeout_sec: float = 10.0
    reranker_max_retries: int = 2
    reranker_truncation: bool = True
    reranker_return_documents: bool = False
    reranker_max_doc_length: int | None = None
    reranker_top_n_cap: int = 20

    # BM25
    bm25_stats_dir: str = ""

    # Retrieval tuning
    search_workers: int = 4
    text_fetch_batch_size: int = 64
    neighbor_batch_docs: int = 24

    # Time-decay
    time_decay_half_life_news: int = 7
    time_decay_half_life_fed: int = 14
    time_decay_max_boost: float = 1.5
    time_decay_min_boost: float = 0.7

    # Policy
    policy_dir: str = ""

    @classmethod
    def from_env(cls) -> RAGConfig:
        def _int(val: str, default: int) -> int:
            try:
                return int(val)
            except (ValueError, TypeError):
                return default

        def _float(val: str, default: float) -> float:
            try:
                return float(val)
            except (ValueError, TypeError):
                return default

        def _bool(val: str) -> bool:
            return val.lower() in ("1", "true", "yes")

        # Default db_path: same directory as the analyst engine.db
        db_path = get_env_value("ANALYST_RAG_DB_PATH")
        if not db_path:
            from analyst.storage.sqlite import default_engine_db_path

            db_path = str(default_engine_db_path())

        return cls(
            db_path=db_path,
            openai_api_key=get_env_value("ANALYST_OPENAI_API_KEY", "OPENAI_API_KEY"),
            embedding_model=get_env_value(
                "ANALYST_EMBEDDING_MODEL", default="text-embedding-3-large"
            ),
            embedding_dim=_int(get_env_value("ANALYST_EMBEDDING_DIM"), 3072),
            enable_reranker=_bool(
                get_env_value("ANALYST_ENABLE_RERANKER", default="false")
            ),
            reranker_provider=get_env_value(
                "ANALYST_RERANKER_PROVIDER", default="jina"
            ),
            reranker_model=get_env_value(
                "ANALYST_RERANKER_MODEL", default="jina-reranker-v3"
            ),
            reranker_api_key=get_env_value("ANALYST_RERANKER_API_KEY"),
            reranker_api_base=get_env_value(
                "ANALYST_RERANKER_API_BASE", default="https://api.jina.ai/v1"
            ),
            reranker_timeout_sec=_float(
                get_env_value("ANALYST_RERANKER_TIMEOUT_SEC"), 10.0
            ),
            reranker_max_retries=_int(
                get_env_value("ANALYST_RERANKER_MAX_RETRIES"), 2
            ),
            reranker_truncation=_bool(
                get_env_value("ANALYST_RERANKER_TRUNCATION", default="true")
            ),
            reranker_return_documents=_bool(
                get_env_value("ANALYST_RERANKER_RETURN_DOCUMENTS", default="false")
            ),
            reranker_max_doc_length=(
                _int(get_env_value("ANALYST_RERANKER_MAX_DOC_LENGTH"), 0) or None
            ),
            reranker_top_n_cap=_int(
                get_env_value("ANALYST_RERANKER_TOP_N_CAP"), 20
            ),
            bm25_stats_dir=get_env_value("ANALYST_BM25_STATS_DIR"),
            search_workers=_int(get_env_value("ANALYST_SEARCH_WORKERS"), 4),
            text_fetch_batch_size=_int(
                get_env_value("ANALYST_TEXT_FETCH_BATCH_SIZE"), 64
            ),
            neighbor_batch_docs=_int(
                get_env_value("ANALYST_NEIGHBOR_BATCH_DOCS"), 24
            ),
            time_decay_half_life_news=_int(
                get_env_value("ANALYST_TIME_DECAY_HALF_LIFE_NEWS"), 7
            ),
            time_decay_half_life_fed=_int(
                get_env_value("ANALYST_TIME_DECAY_HALF_LIFE_FED"), 14
            ),
            time_decay_max_boost=_float(
                get_env_value("ANALYST_TIME_DECAY_MAX_BOOST"), 1.5
            ),
            time_decay_min_boost=_float(
                get_env_value("ANALYST_TIME_DECAY_MIN_BOOST"), 0.7
            ),
            policy_dir=get_env_value("ANALYST_RAG_POLICY_DIR"),
        )
