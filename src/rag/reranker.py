"""Jina API reranker.

Vendored from rag-service ``app/retrieval/reranker.py``.
Only the Jina backend is kept (no local CrossEncoder to avoid heavy deps).
"""

from __future__ import annotations

import logging
from typing import Any, List

import httpx

from .config import RAGConfig

log = logging.getLogger(__name__)
_RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class JinaReranker:
    def __init__(self, cfg: RAGConfig) -> None:
        if not cfg.reranker_api_key:
            raise ValueError("RERANKER_API_KEY_REQUIRED")

        self.default_model = cfg.reranker_model
        self.endpoint = cfg.reranker_api_base.rstrip("/") + "/rerank"
        self.timeout_sec = float(cfg.reranker_timeout_sec)
        self.max_retries = max(0, int(cfg.reranker_max_retries))
        self.truncation = bool(cfg.reranker_truncation)
        self.return_documents = bool(cfg.reranker_return_documents)
        self.max_doc_length = cfg.reranker_max_doc_length
        self.client = httpx.Client(
            timeout=self.timeout_sec,
            headers={
                "Authorization": f"Bearer {cfg.reranker_api_key}",
                "Content-Type": "application/json",
            },
        )

    def _parse_scores(self, payload: Any, passages_count: int) -> List[float]:
        if not isinstance(payload, dict):
            raise ValueError("JINA_RERANK_INVALID_RESPONSE")
        results = payload.get("results")
        if not isinstance(results, list):
            raise ValueError("JINA_RERANK_INVALID_RESPONSE")

        scores = [0.0 for _ in range(passages_count)]
        parsed = 0
        for item in results:
            if not isinstance(item, dict):
                continue
            idx = item.get("index")
            score = item.get("relevance_score")
            if (
                isinstance(idx, int)
                and 0 <= idx < passages_count
                and isinstance(score, (int, float))
            ):
                scores[idx] = float(score)
                parsed += 1

        if passages_count > 0 and parsed == 0:
            raise ValueError("JINA_RERANK_EMPTY_RESULTS")
        return scores

    def rerank(
        self, query: str, passages: List[str], *, model: str | None = None
    ) -> List[float]:
        if not passages:
            return []

        model_name = str(model or self.default_model or "").strip()
        if not model_name:
            raise ValueError("RERANKER_MODEL_REQUIRED")

        payload: dict[str, Any] = {
            "model": model_name,
            "query": query,
            "documents": passages,
            "top_n": len(passages),
            "return_documents": self.return_documents,
            "truncation": self.truncation,
        }
        if self.max_doc_length is not None:
            payload["max_doc_length"] = int(self.max_doc_length)

        attempts = self.max_retries + 1
        for attempt in range(1, attempts + 1):
            try:
                response = self.client.post(self.endpoint, json=payload)
                response.raise_for_status()
                return self._parse_scores(response.json(), len(passages))
            except httpx.HTTPStatusError as e:
                status = e.response.status_code if e.response is not None else None
                retriable = status in _RETRIABLE_STATUS_CODES
                if attempt >= attempts or not retriable:
                    raise RuntimeError(f"JINA_RERANK_HTTP_{status or 'UNKNOWN'}") from e
            except (httpx.TimeoutException, httpx.NetworkError) as e:
                if attempt >= attempts:
                    raise RuntimeError("JINA_RERANK_NETWORK_ERROR") from e
            except Exception as e:
                raise RuntimeError("JINA_RERANK_REQUEST_FAILED") from e

        raise RuntimeError("JINA_RERANK_REQUEST_FAILED")
