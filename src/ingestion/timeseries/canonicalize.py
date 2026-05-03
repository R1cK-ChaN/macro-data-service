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


# ── AISI weekly raw steel HTML ─────────────────────────────────────────────

def canonicalize_aisi_payload(payload: dict[str, Any]) -> str:
    """Canonicalize per-metric AISI weekly raw steel observations."""
    observations = payload.get("observations") or []
    cleaned = [
        {"date": row.get("date"), "value": row.get("value")}
        for row in observations
        if isinstance(row, dict)
    ]
    cleaned.sort(key=lambda row: (row.get("date") or "", str(row.get("value") or "")))
    return json.dumps(
        {"metric": payload.get("metric", ""), "observations": cleaned},
        sort_keys=True,
        ensure_ascii=False,
    )


def aisi_content_hash(payload: dict[str, Any]) -> str:
    return _hash_canonical(canonicalize_aisi_payload(payload))


# ── ISM PMI report HTML ───────────────────────────────────────────────────

def canonicalize_ism_payload(payload: dict[str, Any]) -> str:
    """Canonicalize per-series ISM report observations."""
    observations = payload.get("observations") or []
    cleaned = [
        {"date": row.get("date"), "value": row.get("value")}
        for row in observations
        if isinstance(row, dict)
    ]
    cleaned.sort(key=lambda row: (row.get("date") or "", str(row.get("value") or "")))
    return json.dumps(
        {
            "survey": payload.get("survey", ""),
            "metric": payload.get("metric", ""),
            "measure": payload.get("measure", ""),
            "observations": cleaned,
        },
        sort_keys=True,
        ensure_ascii=False,
    )


def ism_content_hash(payload: dict[str, Any]) -> str:
    return _hash_canonical(canonicalize_ism_payload(payload))


# ── Redbook Research weekly retail sales ──────────────────────────────────

def canonicalize_redbook_payload(payload: dict[str, Any]) -> str:
    """Canonicalize Redbook weekly retail-sales observations."""
    observations = payload.get("observations") or []
    cleaned = [
        {"date": row.get("date"), "value": row.get("value")}
        for row in observations
        if isinstance(row, dict)
    ]
    cleaned.sort(key=lambda row: (row.get("date") or "", str(row.get("value") or "")))
    return json.dumps(
        {
            "source_symbol": payload.get("source_symbol", ""),
            "indicator": payload.get("indicator", ""),
            "observations": cleaned,
        },
        sort_keys=True,
        ensure_ascii=False,
    )


def redbook_content_hash(payload: dict[str, Any]) -> str:
    return _hash_canonical(canonicalize_redbook_payload(payload))


# ── EIA Open Data v2 ──────────────────────────────────────────────────────

# EIA wraps observations under ``response.data[]`` with a ``request`` /
# ``apiVersion`` envelope at the top level. Both echo the request, drop them.
_EIA_DROP_RESPONSE = ("warnings", "warning", "links", "command", "params")


def canonicalize_eia_payload(payload: dict[str, Any]) -> str:
    """Canonicalize an EIA v2 ``/data`` response.

    Sorts ``response.data[]`` by ``(period, sorted-row-json)`` and strips
    request-echo top-level fields so a daily refresh with a sliding
    ``start`` window dedupes through INSERT OR IGNORE. The EIA shape is
    ``{request: {…}, apiVersion: …, response: {data: [...], total: N, …}}``;
    we keep only ``response.data`` (sorted) plus ``response.total`` for
    audit. ``request`` and ``apiVersion`` change every fetch.

    The full-row tie-breaker is required because some EIA datasets store
    the observation in a named column (``generation``, ``revenue``,
    ``sales`` per the ``data[]`` request parameter) rather than ``value``;
    without it, two rows with the same period but different facet values
    would collapse to the same sort key, and a tie-order flip on
    identical content would mint a fresh hash and defeat dedupe.
    """
    response = payload.get("response", {})
    if not isinstance(response, dict):
        return json.dumps({"response": {}}, sort_keys=True, ensure_ascii=False)
    data = response.get("data", [])
    cleaned_rows = [row for row in data if isinstance(row, dict)]
    cleaned_rows.sort(key=lambda r: (
        r.get("period") or "",
        json.dumps(r, sort_keys=True, ensure_ascii=False),
    ))
    cleaned_response = {
        k: v for k, v in response.items()
        if k not in _EIA_DROP_RESPONSE and k != "data"
    }
    cleaned_response["data"] = cleaned_rows
    return json.dumps({"response": cleaned_response}, sort_keys=True, ensure_ascii=False)


def eia_content_hash(payload: dict[str, Any]) -> str:
    return _hash_canonical(canonicalize_eia_payload(payload))


# ── Treasury Fiscal Data ──────────────────────────────────────────────────

# Treasury Fiscal Data wraps rows in ``data[]`` with ``meta`` (pagination,
# data-types, format-detector) and ``links`` (pagination URLs that change
# every fetch). Both are query-time, drop them.
_TREASURY_DROP_TOP = ("meta", "links")


def canonicalize_treasury_fiscal_payload(payload: dict[str, Any]) -> str:
    """Canonicalize a Treasury Fiscal Data ``/services/api/...`` response.

    Sorts ``data[]`` by ``record_date`` (Treasury's universal observation
    key) and drops ``meta`` + ``links`` (pagination + count metadata that
    change every fetch). Falls back to repr-sort when ``record_date`` is
    absent so the hash stays deterministic.
    """
    data = payload.get("data", [])
    cleaned = [row for row in data if isinstance(row, dict)]
    cleaned.sort(key=lambda r: (r.get("record_date") or "", json.dumps(r, sort_keys=True)))
    return json.dumps({"data": cleaned}, sort_keys=True, ensure_ascii=False)


def treasury_fiscal_content_hash(payload: dict[str, Any]) -> str:
    return _hash_canonical(canonicalize_treasury_fiscal_payload(payload))


# ── NY Fed reference rates + GSCPI ────────────────────────────────────────

def canonicalize_nyfed_payload(payload: dict[str, Any]) -> str:
    """Canonicalize an NY Fed reference-rate or GSCPI snapshot.

    Reference-rate endpoints return ``{refRates: [...]}``; GSCPI is
    projected through the fetcher into ``{observations: [...]}`` (the
    upstream is a binary workbook). Both shapes get sorted by date and
    bound to a ``series_id`` injected by the fetcher so per-rate dispatch
    over the same envelope hashes distinctly. ``mostRecentlyObserved`` /
    cache-control echoes are dropped if present.
    """
    series_id = payload.get("series_id", "")
    ref_rates = payload.get("refRates", [])
    if isinstance(ref_rates, list) and ref_rates:
        cleaned = [row for row in ref_rates if isinstance(row, dict)]
        cleaned.sort(key=lambda r: (r.get("effectiveDate") or "", str(r.get("percentRate") or "")))
        return json.dumps(
            {"series_id": series_id, "refRates": cleaned},
            sort_keys=True, ensure_ascii=False,
        )
    observations = payload.get("observations", [])
    cleaned_obs = [
        {"date": row.get("date"), "value": row.get("value")}
        for row in observations
        if isinstance(row, dict)
    ]
    cleaned_obs.sort(key=lambda r: (r.get("date") or "", str(r.get("value") or "")))
    return json.dumps(
        {"series_id": series_id, "observations": cleaned_obs},
        sort_keys=True, ensure_ascii=False,
    )


def nyfed_content_hash(payload: dict[str, Any]) -> str:
    return _hash_canonical(canonicalize_nyfed_payload(payload))


# ── Eurostat JSON-stat ────────────────────────────────────────────────────

# JSON-stat envelope wraps observations under ``value`` (position → value
# map) plus ``dimension.time.category.index`` (position → period). The
# updated/extension fields echo the request timestamp, drop them.
_EUROSTAT_DROP_TOP = ("updated", "extension", "label", "source", "href")


def canonicalize_eurostat_jsonstat_payload(
    payload: dict[str, Any], *, series_id: str = "",
) -> str:
    """Canonicalize a Eurostat JSON-stat response.

    Reduces to ``{series_id, observations: [...]}`` so the per-series
    dispatch over the same dataset hashes distinctly. Eurostat's JSON-stat
    shape (``dimension.time.category.index`` + ``value`` map) needs joining
    by position before hashing — otherwise two fetches that re-order the
    sparse ``value`` dict by Python insertion-order would mint a fresh
    hash. ``updated`` / ``extension.lang`` / ``label`` echo the request
    rather than the data, drop them.

    Includes the position-keyed ``status`` flag in each observation when
    Eurostat publishes one — JSON-stat carries ``status`` (e.g. ``"p"``
    provisional, ``"e"`` estimated, ``":"`` confidential, ``"u"`` low
    reliability) per-position; a provisional→final flip with the same
    numeric value would otherwise hash identically and be dropped by
    INSERT OR IGNORE, suppressing the audit row that the revision
    actually happened.
    """
    time_dim = payload.get("dimension", {}).get("time", {}).get("category", {}).get("index", {})
    pos_to_period = {v: k for k, v in time_dim.items()} if isinstance(time_dim, dict) else {}
    values = payload.get("value", {})
    status_map = payload.get("status", {})
    if not isinstance(status_map, dict):
        status_map = {}
    rows: list[dict[str, Any]] = []
    if isinstance(values, dict):
        for pos_str, val in values.items():
            try:
                pos = int(pos_str)
            except (TypeError, ValueError):
                continue
            if val is None:
                continue
            period = pos_to_period.get(pos)
            if period is None:
                continue
            row: dict[str, Any] = {"period": period, "value": val}
            status = status_map.get(pos_str)
            if status is not None:
                row["status"] = status
            rows.append(row)
    rows.sort(key=lambda r: (
        r.get("period") or "",
        str(r.get("value") or ""),
        str(r.get("status") or ""),
    ))
    return json.dumps(
        {"series_id": series_id, "observations": rows},
        sort_keys=True, ensure_ascii=False,
    )


def eurostat_jsonstat_content_hash(payload: dict[str, Any], *, series_id: str = "") -> str:
    return _hash_canonical(canonicalize_eurostat_jsonstat_payload(payload, series_id=series_id))


# ── World Bank Indicators v2 ──────────────────────────────────────────────

def canonicalize_worldbank_payload(payload: dict[str, Any]) -> str:
    """Canonicalize a World Bank ``/v2/country/{c}/indicator/{i}`` response.

    The World Bank shape is ``[{page_info}, [records]]``; the fetcher
    wraps it under ``payload['response']``. ``page_info`` echoes the
    request (page index + per_page + total when nothing changed); we
    keep only ``page_info.total`` for audit and sort the records by
    ``(country.id, date)`` to immunize against upstream reordering.
    """
    response = payload.get("response", [])
    if not isinstance(response, list) or len(response) < 2:
        return json.dumps({"response": []}, sort_keys=True, ensure_ascii=False)
    page_info_raw = response[0] if isinstance(response[0], dict) else {}
    records = response[1] if isinstance(response[1], list) else []
    cleaned = [row for row in records if isinstance(row, dict)]
    cleaned.sort(key=lambda r: (
        ((r.get("country") or {}).get("id") or ""),
        (r.get("date") or ""),
    ))
    return json.dumps(
        {"page_info": {"total": page_info_raw.get("total", 0)}, "records": cleaned},
        sort_keys=True, ensure_ascii=False,
    )


def worldbank_content_hash(payload: dict[str, Any]) -> str:
    return _hash_canonical(canonicalize_worldbank_payload(payload))


# ── sentix Economic Index ────────────────────────────────────────────────

def canonicalize_sentix_payload(payload: dict[str, Any]) -> str:
    """Canonicalize sentix Economic Index observations."""
    observations = payload.get("observations") or []
    cleaned = [
        {"date": row.get("date"), "value": row.get("value")}
        for row in observations
        if isinstance(row, dict)
    ]
    cleaned.sort(key=lambda row: (row.get("date") or "", str(row.get("value") or "")))
    return json.dumps(
        {
            "series_id": payload.get("series_id", ""),
            "source_ticker": payload.get("source_ticker", ""),
            "component_tickers": payload.get("component_tickers", []),
            "formula": payload.get("formula", ""),
            "observations": cleaned,
        },
        sort_keys=True,
        ensure_ascii=False,
    )


def sentix_content_hash(payload: dict[str, Any]) -> str:
    return _hash_canonical(canonicalize_sentix_payload(payload))


# ── Dispatch ──────────────────────────────────────────────────────────────

def _eurostat_dispatch(payload: dict[str, Any]) -> str:
    """Dispatch Eurostat payloads by shape.

    Production fetcher uses JSON-stat (``dimension.time.category.index`` +
    ``value`` map); SDMX-JSON catalog discovery uses ``data.dataSets[]``.
    Same source name, two upstream shapes — pick the right canonicalizer
    by inspecting the envelope.
    """
    if isinstance(payload.get("dimension"), dict):
        return canonicalize_eurostat_jsonstat_payload(
            payload, series_id=str(payload.get("series_id", "")),
        )
    return canonicalize_sdmx_payload(payload)


_HASH_BY_SOURCE = {
    "fred": fred_content_hash,
    "bls": bls_content_hash,
    # SDMX-JSON family — BIS is excluded because its endpoint returns CSV.
    # Eurostat dispatches by payload shape: production fetcher uses
    # JSON-stat, catalog discovery uses SDMX-JSON.
    "imf": sdmx_content_hash,
    "ecb": sdmx_content_hash,
    "bundesbank": sdmx_content_hash,
    "oecd": sdmx_content_hash,
    "unsd": sdmx_content_hash,
    "ilo": sdmx_content_hash,
    "eurostat": lambda p: _hash_canonical(_eurostat_dispatch(p)),
    "eia": eia_content_hash,
    "treasury_fiscal": treasury_fiscal_content_hash,
    "nyfed": nyfed_content_hash,
    "worldbank": worldbank_content_hash,
    "mof_jp": mof_jp_content_hash,
    "aisi": aisi_content_hash,
    "ism": ism_content_hash,
    "redbook": redbook_content_hash,
    "sentix": sentix_content_hash,
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
