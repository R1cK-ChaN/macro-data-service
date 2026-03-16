"""OpenAI embedding client.

Vendored from rag-service ``app/retrieval/embeddings.py``.
Telemetry decorators removed; uses ``RAGConfig`` for API key + model.
"""

from __future__ import annotations

import logging
from typing import List

from openai import OpenAI

from .config import RAGConfig

log = logging.getLogger(__name__)


class Embedder:
    def __init__(self, cfg: RAGConfig) -> None:
        if not cfg.openai_api_key:
            raise ValueError("OPENAI_API_KEY_REQUIRED")
        self.client = OpenAI(api_key=cfg.openai_api_key)
        self.model = cfg.embedding_model

    def embed(self, text: str) -> List[float]:
        resp = self.client.embeddings.create(model=self.model, input=text)
        return resp.data[0].embedding

    def embed_many(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        resp = self.client.embeddings.create(model=self.model, input=texts)
        rows: List[List[float]] = [[] for _ in texts]
        for item in resp.data:
            idx = getattr(item, "index", None)
            embedding = getattr(item, "embedding", None)
            if not isinstance(idx, int) or idx < 0 or idx >= len(texts) or embedding is None:
                continue
            rows[idx] = embedding
        if any(not row for row in rows):
            rows = [item.embedding for item in resp.data]
        return rows
