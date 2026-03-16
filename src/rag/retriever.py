"""High-level retriever wrapper — owns VectorStore + Embedder + Reranker + PolicyStore."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from .config import RAGConfig
from .embeddings import Embedder
from .models import MacroMode
from .orchestrator import apply_fallback
from .pipeline import retrieve_with_policy
from .policy_loader import PolicyStore, load_policies
from .policy_selector import select_policy
from .reranker import JinaReranker
from .vector_store import VectorStore

log = logging.getLogger(__name__)


class MacroRetriever:
    def __init__(
        self,
        store: VectorStore,
        embedder: Embedder,
        reranker: JinaReranker | None,
        policy_store: PolicyStore,
        cfg: RAGConfig,
    ) -> None:
        self.store = store
        self.embedder = embedder
        self.reranker = reranker
        self.policy_store = policy_store
        self.cfg = cfg

    @classmethod
    def from_env(cls) -> MacroRetriever | None:
        """Create from environment.  Returns ``None`` if OpenAI API key is missing."""
        cfg = RAGConfig.from_env()
        if not cfg.openai_api_key:
            log.info("rag_disabled: no ANALYST_OPENAI_API_KEY configured")
            return None

        store = VectorStore(cfg)
        try:
            store.init_collection()
        except Exception as e:
            log.warning("rag_store_init_failed err=%s", repr(e))
            return None

        embedder = Embedder(cfg)
        reranker = None
        if cfg.enable_reranker and cfg.reranker_api_key:
            try:
                reranker = JinaReranker(cfg)
            except Exception as e:
                log.warning("rag_reranker_init_failed err=%s", repr(e))

        # Resolve policy dir — default to bundled policies/
        policy_dir = cfg.policy_dir
        if not policy_dir:
            policy_dir = os.path.join(os.path.dirname(__file__), "policies")
        policy_store = load_policies(policy_dir)

        return cls(store, embedder, reranker, policy_store, cfg)

    def retrieve(
        self,
        query: str,
        mode: MacroMode = MacroMode.QA,
        *,
        filters: Optional[Dict[str, Any]] = None,
        policy_id: Optional[str] = None,
        limit: int | None = None,
    ) -> Dict[str, Any]:
        """Run full retrieval pipeline.

        Returns dict with ``evidences``, ``bundle``, ``coverage_counts``,
        ``coverage_ok``, ``candidates_total``, ``final_k``, ``timing_ms``.
        """
        selection = select_policy(self.policy_store, mode.value, policy_id=policy_id)
        policy = selection.policy

        if limit:
            policy = dict(policy)
            route = dict(policy.get("route", {}))
            budget = dict(route.get("budget", {}))
            budget["final_context_k"] = limit
            route["budget"] = budget
            policy["route"] = route

        result = retrieve_with_policy(
            query,
            policy,
            self.store,
            self.embedder,
            self.reranker,
            self.cfg,
            request_filters=filters,
        )

        # Fallback if coverage insufficient
        if not result["coverage_ok"]:
            fallback_stages = policy.get("fallback", {}).get("stages", [])
            for stage in fallback_stages:
                relaxed = apply_fallback(policy, stage)
                result = retrieve_with_policy(
                    query,
                    relaxed,
                    self.store,
                    self.embedder,
                    self.reranker,
                    self.cfg,
                    request_filters=filters,
                )
                if result["coverage_ok"]:
                    break

        result["policy_id"] = selection.policy.get("id")
        result["selection_reason"] = selection.selection_reason
        return result
