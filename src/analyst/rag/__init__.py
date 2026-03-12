"""RAG retrieval engine for macro-economic content.

Hybrid dense+sparse retrieval with RRF fusion, reranking, and coverage-aware
selection.  Uses SQLite + numpy for lightweight single-VPS deployment.

Heavy imports (openai, numpy, mmh3) are deferred so the package can be
imported even when those deps are not installed.
"""

from __future__ import annotations

from .config import RAGConfig
from .models import MacroCandidate, MacroEvidence, MacroEvidenceBundle, MacroMode


def __getattr__(name: str):
    if name == "MacroRetriever":
        from .retriever import MacroRetriever

        return MacroRetriever
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "MacroCandidate",
    "MacroEvidence",
    "MacroEvidenceBundle",
    "MacroMode",
    "MacroRetriever",
    "RAGConfig",
]
