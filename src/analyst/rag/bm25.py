"""BM25 sparse vector computation.

Vendored from rag-service ``app/utils/bm25.py``.  Simplified: stats persistence
uses plain JSON files without the versioned-pointer mechanism (the analyst
project builds stats at calibrate time and reads them at retrieval time).
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from typing import Dict, List

import mmh3

from .text_utils import tokenize

BM25_HASH_BUCKETS = 10_000_000


@dataclass
class BM25Stats:
    doc_count: int = 0
    avgdl: float = 0.0
    df: Dict[str, int] = field(default_factory=dict)

    def update(self, tokens: List[str]) -> None:
        self.doc_count += 1
        dl = len(tokens)
        self.avgdl = ((self.avgdl * (self.doc_count - 1)) + dl) / self.doc_count
        seen = set(tokens)
        for t in seen:
            self.df[t] = self.df.get(t, 0) + 1

    def idf(self, term: str) -> float:
        n = self.doc_count
        df = self.df.get(term, 0)
        if n == 0:
            return 0.0
        return math.log(1 + (n - df + 0.5) / (df + 0.5))


def load_stats(path: str) -> BM25Stats:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return BM25Stats(doc_count=data["doc_count"], avgdl=data["avgdl"], df=data["df"])
    except FileNotFoundError:
        return BM25Stats()


def save_stats(path: str, stats: BM25Stats) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    data = {"doc_count": stats.doc_count, "avgdl": stats.avgdl, "df": stats.df}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def bm25_sparse_vector(
    text: str, stats: BM25Stats, k1: float = 1.2, b: float = 0.75
) -> Dict[int, float]:
    tokens = tokenize(text)
    tf: Dict[str, int] = {}
    for t in tokens:
        tf[t] = tf.get(t, 0) + 1
    dl = len(tokens) or 1
    vec: Dict[int, float] = {}
    for term, freq in tf.items():
        idf = stats.idf(term)
        denom = freq + k1 * (1 - b + b * dl / max(stats.avgdl, 1.0))
        score = idf * ((freq * (k1 + 1)) / denom)
        idx = mmh3.hash(term, signed=False) % BM25_HASH_BUCKETS
        vec[idx] = float(score)
    # Keep a sentinel dimension to avoid empty sparse vectors.
    if not vec:
        vec[0] = 1e-9
    return vec
