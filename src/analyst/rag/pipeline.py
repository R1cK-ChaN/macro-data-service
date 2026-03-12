"""Retrieval pipeline — dense + sparse + RRF fusion, dedup, coverage, rerank.

Vendored from rag-service ``app/retrieval/pipeline.py`` with macro-specific
field names, time-decay tuned for macro content, and telemetry removed.
Uses SQLite + numpy VectorStore instead of Milvus.
"""

from __future__ import annotations

import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .bm25 import BM25Stats, bm25_sparse_vector, load_stats
from .config import RAGConfig
from .embeddings import Embedder
from .models import MacroCandidate, MacroEvidence, MacroEvidenceBundle
from .reranker import JinaReranker
from .vector_store import VectorStore

log = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _final_score(c: MacroCandidate) -> float:
    v = c.scores.get("final")
    if isinstance(v, (int, float)) and v is not None:
        return float(v)
    return 0.0


def _rrf_fusion(
    ranked_lists: List[List[Tuple[MacroCandidate, float]]], rrf_k: int = 60
) -> Dict[str, float]:
    scores: Dict[str, float] = {}
    for ranked in ranked_lists:
        for rank, (cand, weight) in enumerate(ranked, start=1):
            score = weight / (rrf_k + rank)
            scores[cand.chunk_id] = scores.get(cand.chunk_id, 0.0) + score
    return scores


def _dedup(candidates: List[MacroCandidate], by: str) -> List[MacroCandidate]:
    seen: set[str] = set()
    result: list[MacroCandidate] = []
    for c in candidates:
        key = (
            c.doc_id
            if by == "doc_id"
            else c.section_path
            if by == "section_path"
            else c.content_hash
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(c)
    return result


def _select_with_coverage(
    candidates: List[MacroCandidate],
    coverage_rules: Dict[str, Any],
    final_k: int,
) -> Tuple[List[MacroCandidate], Dict[str, int], bool]:
    by_type: Dict[str, List[MacroCandidate]] = {}
    for c in candidates:
        by_type.setdefault(c.source_type, []).append(c)
    for k in by_type:
        by_type[k].sort(key=_final_score, reverse=True)

    rules = []
    for source_type, rule in coverage_rules.items():
        rules.append((rule.get("priority", 999), source_type, rule))
    rules.sort(key=lambda x: x[0])

    selected: List[MacroCandidate] = []
    counts: Dict[str, int] = {k: 0 for k in coverage_rules}

    coverage_ok = True
    for _, source_type, rule in rules:
        min_req = int(rule.get("min", 0))
        max_req = rule.get("max")
        pool = by_type.get(source_type, [])
        take = pool[:min_req]
        selected.extend(take)
        counts[source_type] = len(take)
        if len(take) < min_req:
            coverage_ok = False
        if max_req is not None and counts[source_type] > max_req:
            counts[source_type] = max_req

    remaining = [c for c in candidates if c not in selected]
    remaining.sort(key=_final_score, reverse=True)
    for c in remaining:
        if len(selected) >= final_k:
            break
        max_req = coverage_rules.get(c.source_type, {}).get("max")
        if max_req is not None and counts.get(c.source_type, 0) >= max_req:
            continue
        selected.append(c)
        counts[c.source_type] = counts.get(c.source_type, 0) + 1

    selected = selected[:final_k]
    counts["total"] = len(selected)
    return selected, counts, coverage_ok


def _apply_time_decay(
    candidates: List[MacroCandidate],
    cfg: RAGConfig,
) -> None:
    """Multiply ``scores["final"]`` by a time-decay boost."""
    now = datetime.now(timezone.utc)
    for c in candidates:
        if not c.updated_at:
            continue
        try:
            dt = datetime.fromisoformat(c.updated_at.replace("Z", "+00:00"))
            age_days = max((now - dt).total_seconds() / 86400, 0.0)
        except (ValueError, TypeError):
            continue
        # Shorter half-life for news vs Fed comms
        if c.source_type in ("central_bank_comm",):
            half_life = max(cfg.time_decay_half_life_fed, 1)
        else:
            half_life = max(cfg.time_decay_half_life_news, 1)
        boost = cfg.time_decay_min_boost + (
            cfg.time_decay_max_boost - cfg.time_decay_min_boost
        ) * math.pow(2, -age_days / half_life)
        c.scores["time_decay"] = boost
        c.scores["final"] = (c.scores.get("final") or 0.0) * boost


def _group_bundle(evidences: List[MacroEvidence]) -> MacroEvidenceBundle:
    bundle = MacroEvidenceBundle()
    for e in evidences:
        if e.source_type == "news_article":
            bundle.news.append(e)
        elif e.source_type == "central_bank_comm":
            bundle.fed_comms.append(e)
        elif e.source_type == "indicator":
            bundle.indicators.append(e)
        elif e.source_type == "calendar_event":
            bundle.events.append(e)
        else:
            bundle.research.append(e)
    return bundle


# ------------------------------------------------------------------
# Hit/row → candidate
# ------------------------------------------------------------------


def _hit_to_candidate(
    hit: Any,
    dense_score: float | None = None,
    bm25_score: float | None = None,
) -> MacroCandidate:
    entity = hit.entity
    return MacroCandidate(
        chunk_id=entity.get("chunk_id"),
        text=str(entity.get("text") or ""),
        source_type=str(entity.get("source_type") or ""),
        source_id=str(entity.get("source_id") or ""),
        section_path=str(entity.get("section_path") or ""),
        content_type=str(entity.get("content_type") or ""),
        country=str(entity.get("country") or ""),
        indicator_group=str(entity.get("indicator_group") or ""),
        impact_level=str(entity.get("impact_level") or ""),
        data_source=str(entity.get("data_source") or ""),
        updated_at=str(entity.get("updated_at") or ""),
        content_hash=str(entity.get("content_hash") or ""),
        doc_id=str(entity.get("doc_id") or ""),
        chunk_index=int(entity.get("chunk_index") or 0),
        chunk_total=int(entity.get("chunk_total") or 0),
        scores={
            "dense": dense_score,
            "bm25": bm25_score,
            "fused": None,
            "rerank": None,
            "final": None,
        },
    )


def _row_to_candidate(row: Dict[str, Any]) -> MacroCandidate:
    return MacroCandidate(
        chunk_id=row.get("chunk_id"),
        text=str(row.get("text") or ""),
        source_type=str(row.get("source_type") or ""),
        source_id=str(row.get("source_id") or ""),
        section_path=str(row.get("section_path") or ""),
        content_type=str(row.get("content_type") or ""),
        country=str(row.get("country") or ""),
        indicator_group=str(row.get("indicator_group") or ""),
        impact_level=str(row.get("impact_level") or ""),
        data_source=str(row.get("data_source") or ""),
        updated_at=str(row.get("updated_at") or ""),
        content_hash=str(row.get("content_hash") or ""),
        doc_id=str(row.get("doc_id") or ""),
        chunk_index=int(row.get("chunk_index") or 0),
        chunk_total=int(row.get("chunk_total") or 0),
        scores={"dense": None, "bm25": None, "fused": None, "rerank": None, "final": None},
    )


# ------------------------------------------------------------------
# Text hydration (fetch text for reranking / final output)
# ------------------------------------------------------------------


def _hydrate_candidate_texts(
    store: VectorStore,
    candidates: List[MacroCandidate],
    chunk_ids: List[str],
    *,
    batch_size: int,
) -> int:
    if not chunk_ids:
        return 0
    candidate_by_id = {c.chunk_id: c for c in candidates if c.chunk_id}
    pending_ids: List[str] = []
    seen: set[str] = set()
    for cid in chunk_ids:
        if not cid or cid in seen:
            continue
        seen.add(cid)
        candidate = candidate_by_id.get(cid)
        if candidate is None or candidate.text:
            continue
        pending_ids.append(cid)
    if not pending_ids:
        return 0
    rows = store.query_by_chunk_ids(pending_ids, include_text=True, batch_size=batch_size)
    row_by_id: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        chunk_id = row.get("chunk_id")
        if isinstance(chunk_id, str) and chunk_id and chunk_id not in row_by_id:
            row_by_id[chunk_id] = row
    hydrated = 0
    for cid in pending_ids:
        candidate = candidate_by_id.get(cid)
        if candidate is None:
            continue
        row = row_by_id.get(cid)
        if row is None:
            candidate.text = ""
            continue
        candidate.text = str(row.get("text") or "")
        hydrated += 1
    return hydrated


# ------------------------------------------------------------------
# Main entry point
# ------------------------------------------------------------------


def retrieve_with_policy(
    query: str,
    policy: Dict[str, Any],
    store: VectorStore,
    embedder: Embedder,
    reranker: Optional[JinaReranker],
    cfg: RAGConfig,
    *,
    request_filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    t0 = time.perf_counter()

    # Merge policy + request filters
    filters = dict(policy.get("route", {}).get("filters") or {})
    if request_filters:
        _merge_request_filters(filters, request_filters)

    # Apply default_days as a hard time boundary (if no explicit updated_after)
    if "updated_after" not in filters:
        default_days = filters.pop("default_days", None)
        if default_days:
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(default_days))
            filters["updated_after"] = cutoff.isoformat()
    else:
        filters.pop("default_days", None)

    rrf_k = policy.get("route", {}).get("fusion", {}).get("rrf_k", 60)

    # Embed query
    query_vec = embedder.embed(query)

    # Load BM25 stats
    bm25_stats = BM25Stats()
    if cfg.bm25_stats_dir:
        stats_path = f"{cfg.bm25_stats_dir}/bm25_stats.json"
        bm25_stats = load_stats(stats_path)

    # Build search jobs from policy route config
    collections = policy.get("route", {}).get("collections", [])
    search_jobs: List[Dict[str, Any]] = []
    for col in collections:
        if not col.get("enabled", True):
            continue
        weight = float(col.get("weight", 1.0))
        dense_cfg = col.get("dense", {})
        bm25_cfg = col.get("bm25", {})

        if dense_cfg.get("enabled", True):
            search_jobs.append({
                "kind": "dense",
                "weight": weight,
                "query_vec": query_vec,
                "top_k": int(dense_cfg.get("top_k", 10)),
                "filters": filters or None,
            })
        if bm25_cfg.get("enabled", True):
            q_sparse = bm25_sparse_vector(
                query, bm25_stats, bm25_cfg.get("k1", 1.2), bm25_cfg.get("b", 0.75)
            )
            search_jobs.append({
                "kind": "sparse",
                "weight": weight,
                "query_vec": q_sparse,
                "top_k": int(bm25_cfg.get("top_k", 10)),
                "filters": filters or None,
            })

    # Execute search jobs
    dense_results: List[Tuple[MacroCandidate, float]] = []
    sparse_results: List[Tuple[MacroCandidate, float]] = []

    def _run_job(job: Dict[str, Any]) -> Tuple[str, float, List[Any]]:
        if job["kind"] == "dense":
            hits = store.search_dense(
                job["query_vec"], int(job["top_k"]), job["filters"], include_text=False
            )
        else:
            hits = store.search_sparse(
                job["query_vec"], int(job["top_k"]), job["filters"], include_text=False
            )
        return str(job["kind"]), float(job["weight"]), hits

    workers = max(1, cfg.search_workers)
    if search_jobs:
        if workers == 1 or len(search_jobs) == 1:
            for job in search_jobs:
                kind, weight, hits = _run_job(job)
                for hit in hits:
                    cand = _hit_to_candidate(
                        hit,
                        dense_score=float(hit.score) if kind == "dense" else None,
                        bm25_score=float(hit.score) if kind == "sparse" else None,
                    )
                    (dense_results if kind == "dense" else sparse_results).append(
                        (cand, weight)
                    )
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(_run_job, job): idx
                    for idx, job in enumerate(search_jobs)
                }
                ordered: list[Tuple[str, float, List[Any]] | None] = [None] * len(
                    search_jobs
                )
                for future in as_completed(futures):
                    ordered[futures[future]] = future.result()
                for item in ordered:
                    if item is None:
                        continue
                    kind, weight, hits = item
                    for hit in hits:
                        cand = _hit_to_candidate(
                            hit,
                            dense_score=float(hit.score) if kind == "dense" else None,
                            bm25_score=float(hit.score) if kind == "sparse" else None,
                        )
                        (dense_results if kind == "dense" else sparse_results).append(
                            (cand, weight)
                        )

    # RRF fusion
    fusion_scores = _rrf_fusion([dense_results, sparse_results], rrf_k=rrf_k)

    # Merge candidates
    candidates: Dict[str, MacroCandidate] = {}
    for cand, _ in dense_results + sparse_results:
        existing = candidates.get(cand.chunk_id)
        if existing is None:
            candidates[cand.chunk_id] = cand
            continue
        for score_key in ("dense", "bm25"):
            new_val = cand.scores.get(score_key)
            if new_val is not None:
                cur_val = existing.scores.get(score_key)
                if cur_val is None or new_val > cur_val:
                    existing.scores[score_key] = new_val

    for cid, cand in candidates.items():
        cand.scores["fused"] = fusion_scores.get(cid, 0.0)
        cand.scores["final"] = cand.scores.get("fused", 0.0)

    all_candidates = list(candidates.values())
    all_candidates.sort(key=_final_score, reverse=True)

    # Source-type weights from policy filters
    source_type_filter = filters.get("source_type") if isinstance(filters, dict) else None
    if source_type_filter and isinstance(source_type_filter, dict):
        weights = source_type_filter.get("weights") or {}
        if weights:
            for c in all_candidates:
                w = float(weights.get(c.source_type, 1.0))
                c.scores["final"] = (c.scores.get("final") or 0.0) * w
            all_candidates.sort(key=_final_score, reverse=True)

    # Time decay
    _apply_time_decay(all_candidates, cfg)
    all_candidates.sort(key=_final_score, reverse=True)

    # Dedup
    dedup_cfg = policy.get("selection", {}).get("dedup", {})
    all_candidates = _dedup(all_candidates, dedup_cfg.get("by", "content_hash"))

    # Candidate budget cap
    candidate_budget = policy.get("route", {}).get("budget", {}).get(
        "candidate_budget", 40
    )
    all_candidates = all_candidates[:candidate_budget]

    # Rerank
    rerank_cfg = policy.get("route", {}).get("rerank", {})
    if rerank_cfg.get("enabled", False) and reranker:
        top_n = min(
            int(rerank_cfg.get("top_n", 10)),
            len(all_candidates),
            cfg.reranker_top_n_cap,
        )
        if top_n > 0:
            top_ids = [c.chunk_id for c in all_candidates[:top_n] if c.chunk_id]
            _hydrate_candidate_texts(
                store,
                all_candidates,
                top_ids,
                batch_size=max(1, cfg.text_fetch_batch_size),
            )
            passages = [c.text for c in all_candidates[:top_n]]
            rerank_model = str(rerank_cfg.get("model") or "").strip() or None
            try:
                scores = reranker.rerank(query, passages, model=rerank_model)
                for c, s in zip(all_candidates[:top_n], scores):
                    c.scores["rerank"] = s
                    c.scores["final"] = s
                _apply_time_decay(all_candidates[:top_n], cfg)
                all_candidates.sort(key=_final_score, reverse=True)
            except Exception as e:
                log.warning("rerank_failed err=%s", repr(e))

    # Neighbor expansion
    expansion = policy.get("selection", {}).get("neighbor_expansion", {})
    if expansion.get("enabled", False):
        before = int(expansion.get("before", 0))
        after = int(expansion.get("after", 0))
        doc_indices: Dict[str, List[int]] = {}
        for c in list(all_candidates):
            if not c.doc_id:
                continue
            indices = [
                i
                for i in range(c.chunk_index - before, c.chunk_index + after + 1)
                if i >= 0
            ]
            if indices:
                doc_indices.setdefault(c.doc_id, []).extend(indices)
        if doc_indices:
            try:
                rows = store.query_neighbors_batch(
                    doc_indices,
                    include_text=False,
                    batch_docs=max(1, cfg.neighbor_batch_docs),
                )
            except Exception as e:
                log.warning("neighbor_batch_failed err=%s", repr(e), exc_info=True)
                rows = []
                for did, idxs in doc_indices.items():
                    rows.extend(store.query_by_doc_and_index(did, idxs, include_text=False))
            for row in rows:
                n = _row_to_candidate(row)
                if n.chunk_id not in candidates:
                    candidates[n.chunk_id] = n
            all_candidates = list(candidates.values())
            all_candidates.sort(key=_final_score, reverse=True)

    # Coverage selection
    final_k = policy.get("route", {}).get("budget", {}).get("final_context_k", 8)
    coverage_rules = policy.get("selection", {}).get("coverage_rules", {})
    selected, coverage_counts, coverage_ok = _select_with_coverage(
        all_candidates, coverage_rules, final_k
    )

    # Hydrate text for selected
    selected_ids = [c.chunk_id for c in selected if c.chunk_id]
    _hydrate_candidate_texts(
        store,
        all_candidates,
        selected_ids,
        batch_size=max(1, cfg.text_fetch_batch_size),
    )

    # Build evidence list
    evidences: list[MacroEvidence] = []
    for c in selected:
        text = c.text
        if len(text) > 8000:
            text = text[:8000] + "\n...[truncated]"
        evidences.append(
            MacroEvidence(
                chunk_id=c.chunk_id,
                text=text,
                source_type=c.source_type,
                source_id=c.source_id,
                section_path=c.section_path,
                content_type=c.content_type,
                country=c.country,
                indicator_group=c.indicator_group,
                impact_level=c.impact_level,
                data_source=c.data_source,
                updated_at=c.updated_at,
                scores={
                    "dense_score": c.scores.get("dense"),
                    "bm25_score": c.scores.get("bm25"),
                    "fused_score": c.scores.get("fused"),
                    "rerank_score": c.scores.get("rerank"),
                },
            )
        )

    bundle = _group_bundle(evidences)
    total_sec = time.perf_counter() - t0

    return {
        "evidences": evidences,
        "bundle": bundle,
        "coverage_counts": coverage_counts,
        "coverage_ok": coverage_ok,
        "candidates_total": len(candidates),
        "deduped_total": len(all_candidates),
        "final_k": len(evidences),
        "timing_ms": int(total_sec * 1000),
    }


# ------------------------------------------------------------------
# Filter merging (simplified from orchestrator)
# ------------------------------------------------------------------


def _merge_request_filters(
    base: Dict[str, Any], request: Dict[str, Any]
) -> None:
    """Merge request-level filters into policy filters (intersection)."""
    for key in ("source_type", "content_type", "country", "indicator_group", "impact_level"):
        val = request.get(key)
        if val is None:
            continue
        if isinstance(val, list):
            val = {"include": val}
        if isinstance(val, str):
            val = {"include": [val]}
        if not isinstance(val, dict):
            continue
        if key not in base:
            base[key] = val
        elif isinstance(base[key], dict) and isinstance(val.get("include"), list):
            if "include" in base[key]:
                base[key]["include"] = [
                    x for x in base[key]["include"] if x in val["include"]
                ]
            else:
                base[key]["include"] = val["include"]
    updated_after = request.get("updated_after")
    if isinstance(updated_after, str):
        base["updated_after"] = updated_after
