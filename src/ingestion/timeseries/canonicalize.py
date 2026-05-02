"""Per-source canonicalization for ``obs_raw`` content hashing (issue #69 slice 1).

Each function takes the raw HTTP response dict and returns a canonical
JSON string suitable for sha256 hashing. The goal is "wrong
canonicalization is a worse failure than no obs_raw" — every fetch must
produce an identical hash when the underlying observations are
unchanged, otherwise INSERT OR IGNORE never dedupes and the audit table
grows linearly with daily refresh cadence.

Drop fields that are query-time echoes (``realtime_start`` / response
timestamps / request blocks) and sort observation arrays into a
deterministic order so map insertion order, server-side reordering, or
identical-content + new-fetch-time can't masquerade as a revision.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def _hash_canonical(canonical_json: str) -> str:
    """sha256 of UTF-8 bytes of the canonical JSON."""
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


# ── FRED ──────────────────────────────────────────────────────────────────

# Per-observation keys that echo the request rather than the value.
_FRED_DROP_OBS = ("realtime_start", "realtime_end")


def canonicalize_fred_payload(payload: dict[str, Any]) -> str:
    """Canonicalize a FRED ``/series/observations`` response.

    Hashes ONLY the sorted ``observations`` array. FRED's envelope
    contains a dozen query-time echoes (``observation_start``,
    ``observation_end``, ``limit``, ``offset``, ``order_by``,
    ``sort_order``, ``realtime_start``, ``realtime_end``, ``count``,
    ``units``, ``output_type``, ``file_type``) that change across
    routine refreshes — a sliding ``start_date`` window every day would
    flip the hash even when no observation actually changed, and
    INSERT OR IGNORE would never dedupe. Keeping only the observations
    keeps dedup tight and idempotent. The full envelope still lives in
    ``payload_json`` for audit replay.
    """
    obs = payload.get("observations") or []
    cleaned = [
        {k: v for k, v in row.items() if k not in _FRED_DROP_OBS}
        for row in obs
        if isinstance(row, dict)
    ]
    cleaned.sort(key=lambda r: (r.get("date") or "", str(r.get("value") or "")))
    return json.dumps({"observations": cleaned}, sort_keys=True, ensure_ascii=False)


def fred_content_hash(payload: dict[str, Any]) -> str:
    return _hash_canonical(canonicalize_fred_payload(payload))


# ── BLS ───────────────────────────────────────────────────────────────────

# BLS wraps everything in ``status`` / ``responseTime`` / ``message`` plus
# ``Results.series[*].calculations`` (only present when ``calculations=true``;
# contains query-time-relative deltas). All are query-time, drop them.
_BLS_DROP_TOP = ("status", "responseTime", "message")
_BLS_DROP_SERIES = ("calculations",)


def canonicalize_bls_payload(payload: dict[str, Any]) -> str:
    """Canonicalize a BLS ``/timeseries/data`` response.

    BLS returns ``Results.series[]`` ordered by request-array order; each
    series has a ``data[]`` array ordered newest-first by the upstream.
    Both orderings can flip on retries / partial responses, so we sort:
    series alphabetically by ``seriesID``, observations within each
    series by ``(year, period)`` ascending. Volatile envelope fields
    (``status`` / ``responseTime`` / ``message``) and per-series
    ``calculations`` are dropped.
    """
    body = {k: v for k, v in payload.items() if k not in _BLS_DROP_TOP}
    results = body.get("Results")
    if not isinstance(results, dict):
        return json.dumps(body, sort_keys=True, ensure_ascii=False)
    series_list = results.get("series") or []
    cleaned_series = []
    for s in series_list:
        if not isinstance(s, dict):
            continue
        cleaned = {k: v for k, v in s.items() if k not in _BLS_DROP_SERIES}
        data = cleaned.get("data") or []
        sorted_data = sorted(
            (row for row in data if isinstance(row, dict)),
            key=lambda r: (r.get("year", ""), r.get("period", "")),
        )
        cleaned["data"] = sorted_data
        cleaned_series.append(cleaned)
    cleaned_series.sort(key=lambda s: s.get("seriesID", ""))
    new_results = {**results, "series": cleaned_series}
    body["Results"] = new_results
    return json.dumps(body, sort_keys=True, ensure_ascii=False)


def bls_content_hash(payload: dict[str, Any]) -> str:
    return _hash_canonical(canonicalize_bls_payload(payload))


# ── SDMX (IMF / BIS / ECB / Eurostat / OECD) ──────────────────────────────

# SDMX responses include a ``header`` envelope with timestamps (``prepared``
# / ``id`` / ``sender`` / ``receiver``) that change every fetch. Dropping
# them is required — without it every snapshot looks new.
_SDMX_DROP_TOP = ("header", "errors", "meta")


def canonicalize_sdmx_payload(payload: dict[str, Any]) -> str:
    """Canonicalize an SDMX-JSON ``/data/...`` response.

    SDMX-JSON 2.1 wraps observations in
    ``data.dataSets[*].series[<key>].observations`` (object keyed by
    integer obs index → ``[value, ...attrs]``). The integer keys are
    positional within the time dimension and stable within one
    response, but we still sort to be robust to upstream re-orderings.
    Volatile top-level envelope (``header`` with response timestamps
    plus ``meta`` / ``errors`` blocks) is dropped.
    """
    body = {k: v for k, v in payload.items() if k not in _SDMX_DROP_TOP}
    data = body.get("data")
    if not isinstance(data, dict):
        return json.dumps(body, sort_keys=True, ensure_ascii=False)
    datasets = data.get("dataSets") or []
    cleaned_datasets: list[dict[str, Any]] = []
    for ds in datasets:
        if not isinstance(ds, dict):
            continue
        cleaned = dict(ds)
        series = cleaned.get("series")
        if isinstance(series, dict):
            cleaned_series: dict[str, Any] = {}
            for series_key in sorted(series.keys()):
                node = series[series_key]
                if isinstance(node, dict) and "observations" in node:
                    obs = node["observations"]
                    if isinstance(obs, dict):
                        sorted_obs = {
                            k: obs[k]
                            for k in sorted(obs.keys(), key=lambda x: int(x) if str(x).isdigit() else x)
                        }
                        node = {**node, "observations": sorted_obs}
                cleaned_series[series_key] = node
            cleaned["series"] = cleaned_series
        cleaned_datasets.append(cleaned)
    body["data"] = {**data, "dataSets": cleaned_datasets}
    return json.dumps(body, sort_keys=True, ensure_ascii=False)


def sdmx_content_hash(payload: dict[str, Any]) -> str:
    return _hash_canonical(canonicalize_sdmx_payload(payload))


# ── MOF Japan JGB CSV ──────────────────────────────────────────────────────

def canonicalize_mof_jp_payload(payload: dict[str, Any]) -> str:
    """Canonicalize per-series MOF JGB CSV observations."""
    observations = payload.get("observations") or []
    cleaned = [
        {"date": row.get("date"), "value": row.get("value")}
        for row in observations
        if isinstance(row, dict)
    ]
    cleaned.sort(key=lambda row: (row.get("date") or "", str(row.get("value") or "")))
    return json.dumps(
        {"maturity": payload.get("maturity", ""), "observations": cleaned},
        sort_keys=True,
        ensure_ascii=False,
    )


def mof_jp_content_hash(payload: dict[str, Any]) -> str:
    return _hash_canonical(canonicalize_mof_jp_payload(payload))


# ── Dispatch ──────────────────────────────────────────────────────────────

_HASH_BY_SOURCE = {
    "fred": fred_content_hash,
    "bls": bls_content_hash,
    # SDMX-JSON family — BIS is excluded because its endpoint returns CSV.
    "imf": sdmx_content_hash,
    "ecb": sdmx_content_hash,
    "bundesbank": sdmx_content_hash,
    "eurostat": sdmx_content_hash,
    "oecd": sdmx_content_hash,
    "unsd": sdmx_content_hash,
    "ilo": sdmx_content_hash,
    "mof_jp": mof_jp_content_hash,
}


def content_hash_for_source(source: str, payload: dict[str, Any]) -> str | None:
    """Hash a payload using the source's canonicalization function.

    Returns ``None`` for sources without canonicalization yet — callers
    can branch on ``None`` to skip the obs_raw write rather than land a
    bad hash. Adding a new source = add the function above and register
    it in ``_HASH_BY_SOURCE``.
    """
    fn = _HASH_BY_SOURCE.get(source)
    if fn is None:
        return None
    return fn(payload)
