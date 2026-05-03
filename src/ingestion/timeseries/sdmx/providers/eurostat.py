"""Eurostat SDMX provider — JSON-stat endpoint + geo dimension chunking."""

from __future__ import annotations

import logging
import re
from typing import Callable, Sequence

import requests

from .._base_client import SDMXClient
from .._config import EUROSTAT_CONFIG
from .._errors import EurostatAPIError, EurostatRateLimitError
from .._parsing import build_decade_chunks
from .._types import SDMXObservation

logger = logging.getLogger(__name__)

_EUROSTAT_QUARTER_MAP = {"1": "01", "2": "04", "3": "07", "4": "10"}


def _normalize_eurostat_period(raw: str) -> str:
    """Normalize Eurostat-format periods to YYYY-MM-DD.

    Handles ``2024M01`` and ``2024-M01`` (monthly), ``2024Q1`` and
    ``2024-Q1`` (quarterly), plus the standard ``YYYY-MM`` /
    ``YYYY-MM-DD`` / ``YYYY`` shapes the SDMX base normalizer covers.
    Both the dashed and non-dashed forms appear across Eurostat
    datasets (``namq_10_gdp`` publishes ``2024-Q1``, ``ei_bsin_m_r2``
    publishes ``2024M01``); covering both keeps the replay path
    aligned with the live HTTP path's projected observation_date.
    Standalone so the issue #116 P3 replay path can call it without
    instantiating the client.
    """
    m = re.match(r"^(\d{4})-?M(\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    m = re.match(r"^(\d{4})-?Q(\d)$", raw)
    if m:
        return f"{m.group(1)}-{_EUROSTAT_QUARTER_MAP.get(m.group(2), '01')}-01"
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw
    if re.match(r"^\d{4}-\d{2}$", raw):
        return f"{raw}-01"
    if re.match(r"^\d{4}$", raw):
        return f"{raw}-01-01"
    return raw


def _parse_eurostat_jsonstat(
    payload: dict,
    *,
    dataset_code: str,
    series_id: str,
    limit: int = 100,
) -> list[SDMXObservation]:
    """Parse a Eurostat JSON-stat payload into typed observations.

    Module-level so the issue #116 P3 replay path can re-project a
    stored ``obs_raw`` row through the same parser the live HTTP path
    uses — no behavior drift between live ingest and audit replay.
    Uses ``_normalize_eurostat_period`` for the M/Q/year shapes the
    JSON-stat envelope publishes.
    """
    time_dim = payload.get("dimension", {}).get("time", {}).get("category", {}).get("index", {})
    if not time_dim:
        return []
    pos_to_period: dict[int, str] = {v: k for k, v in time_dim.items()}
    values = payload.get("value", {})

    observations: list[SDMXObservation] = []
    for pos_str, val in values.items():
        try:
            pos = int(pos_str)
            if val is None:
                continue
            period = pos_to_period.get(pos)
            if period is None:
                continue
            observations.append(SDMXObservation(
                series_id=series_id,
                date=_normalize_eurostat_period(period),
                value=float(val),
                dataset=dataset_code,
            ))
        except (ValueError, TypeError, KeyError):
            continue

    observations.sort(key=lambda o: o.date, reverse=True)
    return observations[:limit]


def _filter_nuts_codes(codes: tuple[str, ...], level: int = 0) -> tuple[str, ...]:
    """Filter NUTS codes by level (0=country 2-char, 1=NUTS1 3-char, etc.)."""
    target_len = level + 2
    return tuple(c for c in codes if len(c) == target_len)


def _build_geo_chunks(geo_codes: Sequence[str], batch_size: int = 40) -> list[str]:
    """Split geo codes into ``+``-delimited SDMX key fragments."""
    codes = list(geo_codes)
    if not codes:
        return [""]
    chunks: list[str] = []
    for i in range(0, len(codes), batch_size):
        chunks.append("+".join(codes[i : i + batch_size]))
    return chunks


def _inject_geo_into_key(base_key: str, geo_fragment: str, geo_position: int, total_dims: int) -> str:
    """Inject a geo fragment into the right position of an SDMX key."""
    parts = base_key.split(".")
    while len(parts) < total_dims:
        parts.append("")
    parts[geo_position] = geo_fragment
    return ".".join(parts)


class EurostatClient(SDMXClient):
    """Client for Eurostat JSON-stat and SDMX 2.1 APIs.

    Preserves the original ``get_dataset()`` method (JSON-stat) alongside
    the unified SDMX catalog discovery methods.
    """

    JSON_STAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

    def __init__(self, *, timeout: int = 30) -> None:
        super().__init__(
            EUROSTAT_CONFIG, timeout=timeout,
            api_error_cls=EurostatAPIError,
            rate_limit_error_cls=EurostatRateLimitError,
        )

    def _normalize_date(self, raw: str) -> str:
        # Eurostat-specific: 2024M01, 2024Q1 (no dash)
        m = re.match(r"^(\d{4})M(\d{2})$", raw)
        if m:
            return f"{m.group(1)}-{m.group(2)}-01"
        m = re.match(r"^(\d{4})Q(\d)$", raw)
        if m:
            return f"{m.group(1)}-{_EUROSTAT_QUARTER_MAP.get(m.group(2), '01')}-01"
        # Fall through to base for standard SDMX formats
        return super()._normalize_date(raw)

    # ── JSON-stat endpoint (original, preserved) ──────────────────────

    def get_dataset(
        self,
        dataset_code: str,
        *,
        params: dict[str, str] | None = None,
        series_id: str,
        limit: int = 100,
    ) -> list[SDMXObservation]:
        """Fetch observations from the Eurostat JSON-stat endpoint."""
        observations, _payload, _params = self.get_dataset_with_raw(
            dataset_code, params=params, series_id=series_id, limit=limit,
        )
        return observations

    def get_dataset_with_raw(
        self,
        dataset_code: str,
        *,
        params: dict[str, str] | None = None,
        series_id: str,
        limit: int = 100,
    ) -> tuple[list[SDMXObservation], dict, dict[str, str]]:
        """Fetch JSON-stat observations and return the raw envelope.

        Used by the issue #116 ``obs_raw`` write path so the projector
        boundary can land both the typed observations and the upstream
        payload. Eurostat's JSON-stat shape differs from SDMX-JSON
        (``dimension.time.category.index`` + ``value`` map) so it gets
        its own canonicalize function (``canonicalize_eurostat_jsonstat_payload``).
        """
        url = f"{self.JSON_STAT_BASE}/{dataset_code}"
        query: dict[str, str] = {"format": "JSON", "lang": "en"}
        if params:
            query.update(params)

        response = self.session.get(url, params=query, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        observations = self._parse_jsonstat_payload(
            payload, dataset_code=dataset_code, series_id=series_id, limit=limit,
        )
        audit_params = {"dataset": dataset_code, **{k: str(v) for k, v in query.items()}}
        return observations, payload, audit_params

    def _parse_jsonstat_payload(
        self,
        payload: dict,
        *,
        dataset_code: str,
        series_id: str,
        limit: int,
    ) -> list[SDMXObservation]:
        """Instance shim around the module-level ``_parse_eurostat_jsonstat``."""
        return _parse_eurostat_jsonstat(
            payload, dataset_code=dataset_code, series_id=series_id, limit=limit,
        )

    # ── Geo-aware chunked fetch ───────────────────────────────────────

    def fetch_dataset_chunked(
        self,
        dataflow_id: str,
        key: str = ".",
        *,
        version: str = "1.0",
        series_id: str = "",
        start_year: int = 1960,
        end_year: int = 2026,
        chunk_ranges: Sequence[tuple[str, str]] | None = None,
        geo_codes: Sequence[str] | None = None,
        geo_batch_size: int = 40,
        nuts_level: int | None = None,
        limit: int = 0,
        on_chunk: Callable[[list[SDMXObservation], str, str], None] | None = None,
        **kwargs,
    ) -> list[SDMXObservation]:
        """Fetch with combined time + geo chunking for large Eurostat datasets."""
        if chunk_ranges is None:
            chunk_ranges = build_decade_chunks(start_year, end_year)

        # Resolve geo chunks
        geo_chunks: list[str] | None = None
        geo_position = 0
        total_dims = 0

        if geo_codes is not None:
            geo_chunks = _build_geo_chunks(geo_codes, geo_batch_size)
        elif nuts_level is not None:
            try:
                structure = self.get_datastructure(dataflow_id, version)
                for d in structure.dimensions:
                    if d.id.lower() == "geo":
                        filtered = _filter_nuts_codes(d.codes, level=nuts_level)
                        if filtered:
                            geo_chunks = _build_geo_chunks(filtered, geo_batch_size)
                        geo_position = d.position
                        break
                total_dims = len(structure.dimensions)
            except (EurostatAPIError, EurostatRateLimitError):
                pass
        else:
            try:
                structure = self.get_datastructure(dataflow_id, version)
                for d in structure.dimensions:
                    if d.id.lower() == "geo" and d.code_count > geo_batch_size:
                        geo_chunks = _build_geo_chunks(d.codes, geo_batch_size)
                        geo_position = d.position
                        logger.info(
                            "Eurostat %s: auto-chunking %d geo codes into %d batches",
                            dataflow_id, d.code_count, len(geo_chunks),
                        )
                        break
                total_dims = len(structure.dimensions)
            except (EurostatAPIError, EurostatRateLimitError):
                pass

        all_obs: list[SDMXObservation] = []
        for start_period, end_period in chunk_ranges:
            chunk_obs: list[SDMXObservation] = []

            if geo_chunks and total_dims > 0:
                for geo_fragment in geo_chunks:
                    effective_key = _inject_geo_into_key(key, geo_fragment, geo_position, total_dims)
                    obs = self.get_data(
                        dataflow_id, effective_key,
                        series_id=series_id or dataflow_id,
                        start_period=start_period, end_period=end_period,
                        limit=limit,
                    )
                    chunk_obs.extend(obs)
            else:
                obs = self.get_data(
                    dataflow_id, key,
                    series_id=series_id or dataflow_id,
                    start_period=start_period, end_period=end_period,
                    limit=limit,
                )
                chunk_obs.extend(obs)

            all_obs.extend(chunk_obs)
            if on_chunk is not None:
                on_chunk(chunk_obs, start_period, end_period)

        return all_obs
