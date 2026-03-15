"""Eurostat JSON-stat + SDMX 2.1 API client — Euro Area structured indicators.

Provides both the original JSON-stat dissemination endpoint (``get_dataset``)
and full SDMX catalog discovery (``list_dataflows``, ``get_datastructure``,
``summarize_structure``, ``estimate_size``, ``fetch_dataset_chunked``).
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Callable, Sequence

import requests

logger = logging.getLogger(__name__)


def _normalize_period(raw: str) -> str:
    """Normalize Eurostat period strings to YYYY-MM-DD.

    Handles: ``"2024M01"`` → ``"2024-01-01"``,
             ``"2024Q1"``  → ``"2024-01-01"``,
             ``"2024"``    → ``"2024-01-01"``.
    """
    m = re.match(r"^(\d{4})M(\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    m = re.match(r"^(\d{4})Q(\d)$", raw)
    if m:
        month_map = {"1": "01", "2": "04", "3": "07", "4": "10"}
        return f"{m.group(1)}-{month_map.get(m.group(2), '01')}-01"
    if re.match(r"^\d{4}$", raw):
        return f"{raw}-01-01"
    # Handle SDMX-style "2024-01", "2024-Q1" from the SDMX endpoint
    m = re.match(r"^(\d{4})-Q(\d)$", raw)
    if m:
        month_map = {"1": "01", "2": "04", "3": "07", "4": "10"}
        return f"{m.group(1)}-{month_map.get(m.group(2), '01')}-01"
    if re.match(r"^\d{4}-\d{2}$", raw):
        return f"{raw}-01"
    return raw


@dataclass(frozen=True)
class EurostatObservation:
    """A single observation from the Eurostat API."""

    series_id: str
    date: str
    value: float
    dataset: str = ""


@dataclass(frozen=True)
class EurostatDataflow:
    """Represents a dataset in the Eurostat catalog."""

    id: str
    agency_id: str
    version: str
    name: str = ""
    description: str = ""
    structure_id: str = ""
    structure_version: str = ""


@dataclass(frozen=True)
class EurostatDimension:
    """A single dimension in a Eurostat data structure definition."""

    id: str
    position: int
    name: str = ""
    code_count: int = 0
    is_time: bool = False
    codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class EurostatDataStructure:
    """Full DSD with dimensions for a Eurostat dataflow."""

    id: str
    version: str
    name: str = ""
    dimensions: tuple[EurostatDimension, ...] = ()
    dataflow_id: str = ""
    dataflow_version: str = ""


@dataclass(frozen=True)
class EurostatStructureSummary:
    """Compact summary for catalog inspection."""

    dataflow_id: str
    version: str
    name: str = ""
    structure_id: str = ""
    time_dimension_id: str = ""
    series_dimensions: tuple[str, ...] = ()
    code_counts: dict[str, int] = field(default_factory=dict)
    estimated_series: int = 0


@dataclass(frozen=True)
class EurostatSizeEstimate:
    """Observation count estimate for a Eurostat dataflow."""

    dataflow_id: str
    version: str
    total_series: int = 0
    time_periods: int = 0
    estimated_observations: int = 0


class EurostatAPIError(RuntimeError):
    """Base error for Eurostat API failures."""


class EurostatRateLimitError(EurostatAPIError):
    """Raised when Eurostat throttles a request (HTTP 429)."""


def _build_decade_chunks(start_year: int, end_year: int) -> list[tuple[str, str]]:
    """Split a year range into decade-sized chunks for time-range queries."""
    chunks: list[tuple[str, str]] = []
    year = start_year
    while year <= end_year:
        chunk_end = min(year + 9, end_year)
        chunks.append((str(year), str(chunk_end)))
        year = chunk_end + 1
    return chunks


def _extract_id_from_urn(urn: str) -> str:
    """Extract the artefact ID from an SDMX URN.

    Example: ``"urn:sdmx:...Codelist=ESTAT:CL_FREQ(1.0)"`` → ``"CL_FREQ"``
    """
    if "=" not in urn:
        return ""
    after_eq = urn.rsplit("=", 1)[-1]
    if ":" in after_eq:
        after_eq = after_eq.split(":", 1)[-1]
    if "(" in after_eq:
        return after_eq.split("(")[0]
    return after_eq


def _filter_nuts_codes(codes: tuple[str, ...], level: int = 0) -> tuple[str, ...]:
    """Filter NUTS codes by level.

    Level 0 = countries (2 chars), 1 = major regions (3), 2 = regions (4), 3 = small regions (5).
    """
    target_len = level + 2
    return tuple(c for c in codes if len(c) == target_len)


def _build_geo_chunks(
    geo_codes: Sequence[str],
    batch_size: int = 40,
) -> list[str]:
    """Split a list of geo codes into ``+``-delimited SDMX key fragments.

    Eurostat SDMX keys use ``+`` to combine multiple codes for one dimension,
    e.g. ``"AT+DE+FR"``.  When the geo dimension has hundreds of codes,
    requesting them all at once can produce multi-minute responses or timeouts.
    This helper splits them into manageable batches.

    Returns a list of key fragments like ``["AT+BE+BG+...", "LT+LU+LV+...", ...]``.
    """
    codes = list(geo_codes)
    if not codes:
        return [""]
    chunks: list[str] = []
    for i in range(0, len(codes), batch_size):
        chunks.append("+".join(codes[i : i + batch_size]))
    return chunks


def _inject_geo_into_key(
    base_key: str,
    geo_fragment: str,
    geo_position: int,
    total_dims: int,
) -> str:
    """Replace the geo dimension slot in an SDMX key with a specific fragment.

    ``base_key`` uses dots to separate dimensions.  An empty slot (``".."`` or
    trailing ``.``) means "all values".  This helper injects ``geo_fragment``
    into the slot at ``geo_position``.

    Example::

        _inject_geo_into_key(".", "AT+DE", geo_position=2, total_dims=4)
        # → "..AT+DE."
    """
    parts = base_key.split(".")
    # Extend to cover all dimension positions if the key is short
    while len(parts) < total_dims:
        parts.append("")
    parts[geo_position] = geo_fragment
    return ".".join(parts)


class EurostatClient:
    """Client for the Eurostat JSON-stat and SDMX 2.1 APIs (no key required).

    The ``get_dataset`` method uses the JSON-stat dissemination endpoint.
    Catalog discovery methods use the SDMX 2.1 REST endpoint.
    """

    BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
    SDMX_BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"

    def __init__(self, *, timeout: int = 30) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "AnalystEngine/1.0",
        })
        self.timeout = timeout
        self._last_request: float = 0.0
        self._request_delay: float = 0.5
        self._dataflow_cache: list[EurostatDataflow] | None = None
        self._structure_cache: dict[str, EurostatDataStructure] = {}

    # ── Internal helpers ──────────────────────────────────────────────

    def _get(
        self,
        url: str,
        params: dict[str, str] | None = None,
        *,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        """Internal GET with rate-limit delay and 429 detection."""
        elapsed = time.monotonic() - self._last_request
        if elapsed < self._request_delay:
            time.sleep(self._request_delay - elapsed)
        self._last_request = time.monotonic()

        response = self.session.get(
            url, params=params, headers=headers, timeout=self.timeout,
        )
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            suffix = f" retry_after={retry_after}" if retry_after else ""
            raise EurostatRateLimitError(f"Eurostat rate limit exceeded for {url}.{suffix}")
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = response.text.strip()
            if detail:
                raise EurostatAPIError(
                    f"Eurostat request failed for {url}: HTTP {response.status_code}: {detail[:200]}"
                ) from exc
            raise EurostatAPIError(
                f"Eurostat request failed for {url}: HTTP {response.status_code}"
            ) from exc
        return response

    # ── JSON-stat data endpoint (original) ────────────────────────────

    def get_dataset(
        self,
        dataset_code: str,
        *,
        params: dict[str, str] | None = None,
        series_id: str,
        limit: int = 100,
    ) -> list[EurostatObservation]:
        """Fetch observations from a Eurostat dataset.

        Args:
            dataset_code: Dataset identifier, e.g. ``"prc_hicp_manr"``.
            params: Query parameters for dimension filtering.
            series_id: Logical series id for the returned records.
            limit: Maximum observations to return.
        """
        url = f"{self.BASE_URL}/{dataset_code}"
        query: dict[str, str] = {"format": "JSON", "lang": "en"}
        if params:
            query.update(params)

        response = self.session.get(url, params=query, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()

        # Extract time dimension → position→period mapping
        time_dim = data.get("dimension", {}).get("time", {}).get("category", {}).get("index", {})
        if not time_dim:
            return []
        # Reverse: position (int) → period string
        pos_to_period: dict[int, str] = {v: k for k, v in time_dim.items()}

        values = data.get("value", {})

        observations: list[EurostatObservation] = []
        for pos_str, val in values.items():
            try:
                pos = int(pos_str)
                if val is None:
                    continue
                period = pos_to_period.get(pos)
                if period is None:
                    continue
                observations.append(EurostatObservation(
                    series_id=series_id,
                    date=_normalize_period(period),
                    value=float(val),
                    dataset=dataset_code,
                ))
            except (ValueError, TypeError, KeyError):
                continue

        observations.sort(key=lambda o: o.date, reverse=True)
        return observations[:limit]

    # ── SDMX data endpoint ────────────────────────────────────────────

    def get_data(
        self,
        dataflow_id: str,
        key: str = ".",
        *,
        series_id: str = "",
        start_period: str | None = None,
        end_period: str | None = None,
        limit: int = 100,
    ) -> list[EurostatObservation]:
        """Fetch observations from the Eurostat SDMX 2.1 data endpoint.

        Args:
            dataflow_id: Eurostat dataflow, e.g. ``"prc_hicp_manr"``.
            key: Dimension key, e.g. ``"M.CP00.EA20"``. Use ``"."`` for all.
            series_id: Logical series id for the returned records.
            start_period: Optional start filter, e.g. ``"2020"``.
            end_period: Optional end filter, e.g. ``"2026"``.
            limit: Maximum observations to return (0 = unlimited).
        """
        url = f"{self.SDMX_BASE_URL}/data/{dataflow_id}/{key}"
        params: dict[str, str] = {"format": "jsondata"}
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if limit:
            params["lastNObservations"] = str(limit)

        response = self._get(url, params)
        return self._parse_sdmx_json(
            response.json(), series_id=series_id, dataflow=dataflow_id, limit=limit,
        )

    @staticmethod
    def _parse_sdmx_json(
        data: dict,
        *,
        series_id: str,
        dataflow: str,
        limit: int,
    ) -> list[EurostatObservation]:
        """Parse SDMX-JSON response into observations."""
        observations: list[EurostatObservation] = []

        try:
            datasets = data["dataSets"]
            structure = data.get("structure") or (data.get("structures") or [None])[0]
        except (KeyError, TypeError, IndexError):
            return observations

        if not datasets or not structure:
            return observations

        # Find TIME_PERIOD dimension in observation dimensions
        obs_dims = structure.get("dimensions", {}).get("observation", [])
        time_dim = None
        for dim in obs_dims:
            if dim.get("id") == "TIME_PERIOD":
                time_dim = dim
                break

        if time_dim is None:
            return observations

        # Build index → time period mapping
        time_map: dict[str, str] = {}
        for i, val in enumerate(time_dim.get("values", [])):
            time_map[str(i)] = val.get("id", val.get("value", ""))

        # Iterate all series keys in the first dataset
        all_series = datasets[0].get("series", {})
        for _series_key, series_data in all_series.items():
            for obs_idx, obs_array in series_data.get("observations", {}).items():
                period = time_map.get(obs_idx)
                if not period:
                    continue
                value = obs_array[0] if obs_array else None
                if value is None:
                    continue
                try:
                    observations.append(EurostatObservation(
                        series_id=series_id,
                        date=_normalize_period(period),
                        value=float(value),
                        dataset=dataflow,
                    ))
                except (ValueError, TypeError):
                    continue

        observations.sort(key=lambda o: o.date, reverse=True)
        return observations[:limit] if limit else observations

    # ── Catalog discovery methods ─────────────────────────────────────

    def list_dataflows(self) -> list[EurostatDataflow]:
        """Fetch the full Eurostat dataflow catalog. Results are cached."""
        if self._dataflow_cache is not None:
            return list(self._dataflow_cache)

        url = f"{self.SDMX_BASE_URL}/dataflow/ESTAT/all/latest"
        response = self._get(url, headers={"Accept": "application/json"})
        data = response.json()

        dataflows: list[EurostatDataflow] = []
        for df_node in data.get("data", {}).get("dataflows", []):
            structure_id = ""
            structure_version = ""
            struct_val = df_node.get("structure", "")
            if isinstance(struct_val, str) and "DataStructure=" in struct_val:
                cl_id = _extract_id_from_urn(struct_val)
                structure_id = cl_id
                after_eq = struct_val.rsplit("=", 1)[-1]
                if "(" in after_eq:
                    structure_version = after_eq.split("(")[1].rstrip(")")
            elif isinstance(struct_val, dict):
                structure_id = struct_val.get("id", "")
                structure_version = struct_val.get("version", "")

            name_val = df_node.get("name", "")
            if isinstance(name_val, dict):
                name_val = name_val.get("en", "") or next(iter(name_val.values()), "")
            desc_val = df_node.get("description", "")
            if isinstance(desc_val, dict):
                desc_val = desc_val.get("en", "") or next(iter(desc_val.values()), "")

            dataflows.append(EurostatDataflow(
                id=df_node.get("id", ""),
                agency_id=df_node.get("agencyID", ""),
                version=df_node.get("version", ""),
                name=str(name_val),
                description=str(desc_val),
                structure_id=structure_id,
                structure_version=structure_version,
            ))

        self._dataflow_cache = dataflows
        return list(dataflows)

    def get_datastructure(
        self,
        dataflow_id: str,
        version: str | None = None,
    ) -> EurostatDataStructure:
        """Fetch the data structure definition for a dataflow.

        Uses the dataflow endpoint with ``references=all`` to retrieve
        the DSD, codelists, and concept schemes in a single call.
        """
        flows = self.list_dataflows()
        match = next((f for f in flows if f.id == dataflow_id), None)
        df_version = (match.version if match else version) or "1.0"

        cache_key = f"{dataflow_id}/{df_version}"
        if cache_key in self._structure_cache:
            return self._structure_cache[cache_key]

        url = f"{self.SDMX_BASE_URL}/dataflow/ESTAT/{dataflow_id}/{df_version}"
        params: dict[str, str] = {"references": "all"}
        response = self._get(url, params, headers={"Accept": "application/json"})
        data = response.json()

        structures = data.get("data", {}).get("dataStructures", [])
        if not structures:
            raise EurostatAPIError(f"No data structure found for dataflow {dataflow_id}/{df_version}")

        struct_node = structures[0]
        components = struct_node.get("dataStructureComponents", {})
        dim_list = components.get("dimensionList", {})

        # Parse codelists into a lookup
        codelist_map: dict[str, list[str]] = {}
        for cl in data.get("data", {}).get("codelists", []):
            cl_id = cl.get("id", "")
            codes = [c.get("id", "") for c in cl.get("codes", [])]
            if cl_id:
                codelist_map[cl_id] = codes

        # Build concept → codelist mapping from concept schemes
        concept_codelist: dict[str, str] = {}
        for cs in data.get("data", {}).get("conceptSchemes", []):
            for concept in cs.get("concepts", []):
                cr = concept.get("coreRepresentation", {})
                enum_urn = cr.get("enumeration", "")
                if isinstance(enum_urn, str) and "Codelist=" in enum_urn:
                    cl_id = _extract_id_from_urn(enum_urn)
                    if cl_id:
                        concept_codelist[concept.get("id", "")] = cl_id

        dimensions: list[EurostatDimension] = []
        for dim_node in dim_list.get("dimensions", []):
            dim_id = dim_node.get("id", "")
            position = dim_node.get("position", 0)

            name_val = dim_node.get("name", "")
            if isinstance(name_val, dict):
                name_val = name_val.get("en", "") or next(iter(name_val.values()), "")
            if not name_val:
                name_val = dim_id

            # Resolve codelist: localRepresentation URN, concept mapping, or naming convention
            codes: tuple[str, ...] = ()
            code_count = 0
            local_repr = dim_node.get("localRepresentation", {})
            enum_val = local_repr.get("enumeration")

            cl_id = ""
            if isinstance(enum_val, str) and "Codelist=" in enum_val:
                cl_id = _extract_id_from_urn(enum_val)
            elif isinstance(enum_val, dict):
                cl_id = enum_val.get("id", "")

            if not cl_id:
                cl_id = concept_codelist.get(dim_id, "")

            if not cl_id:
                candidate = f"CL_{dim_id}"
                if candidate in codelist_map:
                    cl_id = candidate

            if cl_id and cl_id in codelist_map:
                code_list = codelist_map[cl_id]
                codes = tuple(code_list)
                code_count = len(codes)

            dimensions.append(EurostatDimension(
                id=dim_id,
                position=position,
                name=str(name_val),
                code_count=code_count,
                is_time=False,
                codes=codes,
            ))

        for td_node in dim_list.get("timeDimensions", []):
            td_id = td_node.get("id", "TIME_PERIOD")
            name_val = td_node.get("name", "")
            if isinstance(name_val, dict):
                name_val = name_val.get("en", "") or next(iter(name_val.values()), "")
            dimensions.append(EurostatDimension(
                id=td_id,
                position=td_node.get("position", len(dimensions)),
                name=str(name_val) or td_id,
                code_count=0,
                is_time=True,
                codes=(),
            ))

        dimensions.sort(key=lambda d: d.position)

        name_val = struct_node.get("name", "")
        if isinstance(name_val, dict):
            name_val = name_val.get("en", "") or next(iter(name_val.values()), "")

        result = EurostatDataStructure(
            id=struct_node.get("id", ""),
            version=struct_node.get("version", df_version),
            name=str(name_val),
            dimensions=tuple(dimensions),
            dataflow_id=dataflow_id,
            dataflow_version=df_version,
        )
        self._structure_cache[cache_key] = result
        return result

    def summarize_structure(
        self,
        dataflow_id: str,
        version: str | None = None,
    ) -> EurostatStructureSummary:
        """Return a compact summary combining dataflow metadata and DSD."""
        flows = self.list_dataflows()
        flow = next((f for f in flows if f.id == dataflow_id), None)
        structure = self.get_datastructure(dataflow_id, version)

        time_dim_id = next((d.id for d in structure.dimensions if d.is_time), "")
        series_dims = tuple(
            d.id for d in sorted(structure.dimensions, key=lambda d: d.position)
            if not d.is_time
        )
        code_counts = {
            d.id: d.code_count
            for d in structure.dimensions
            if not d.is_time
        }
        estimated_series = 1
        for count in code_counts.values():
            if count > 0:
                estimated_series *= count

        # Warn if geo dimension has many codes (NUTS explosion)
        for d in structure.dimensions:
            if d.id.lower() == "geo" and d.code_count > 500:
                logger.warning(
                    "Eurostat dataflow %s has %d geo codes — DSD-based size estimate "
                    "will be inflated (NUTS regions). Use data probe instead.",
                    dataflow_id, d.code_count,
                )

        return EurostatStructureSummary(
            dataflow_id=dataflow_id,
            version=flow.version if flow else (version or ""),
            name=flow.name if flow else structure.name,
            structure_id=structure.id,
            time_dimension_id=time_dim_id,
            series_dimensions=series_dims,
            code_counts=code_counts,
            estimated_series=estimated_series,
        )

    def estimate_size(
        self,
        dataflow_id: str,
        version: str = "1.0",
    ) -> EurostatSizeEstimate:
        """Probe a dataflow with limit=1 to estimate its total size.

        Uses the SDMX data endpoint with ``lastNObservations=1``.
        Falls back to DSD-based estimation if the probe returns nothing.
        """
        total_series = 0
        time_periods = 1

        try:
            url = f"{self.SDMX_BASE_URL}/data/{dataflow_id}/."
            params: dict[str, str] = {
                "format": "jsondata",
                "lastNObservations": "1",
            }
            response = self._get(url, params)
            data = response.json()
            datasets = data.get("dataSets", [])
            if datasets:
                all_series = datasets[0].get("series", {})
                total_series = len(all_series)
        except (EurostatAPIError, EurostatRateLimitError):
            pass

        # Fall back to DSD-based estimate
        if total_series == 0:
            try:
                structure = self.get_datastructure(dataflow_id, version)
                total_series = 1
                has_large_geo = False
                for d in structure.dimensions:
                    if not d.is_time and d.code_count > 0:
                        total_series *= d.code_count
                    if d.id.lower() == "geo" and d.code_count > 500:
                        has_large_geo = True
                if has_large_geo:
                    logger.warning(
                        "Eurostat %s: DSD-based estimate (%d) inflated by large geo dimension",
                        dataflow_id, total_series,
                    )
            except (EurostatAPIError, EurostatRateLimitError):
                pass

        return EurostatSizeEstimate(
            dataflow_id=dataflow_id,
            version=version,
            total_series=total_series,
            time_periods=time_periods,
            estimated_observations=total_series * time_periods,
        )

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
        on_chunk: Callable[[list[EurostatObservation], str, str], None] | None = None,
    ) -> list[EurostatObservation]:
        """Fetch a dataset with time-range and optional geo-dimension chunking.

        Eurostat datasets with NUTS regions can have 1500+ geo codes, making
        full queries extremely slow or prone to timeouts.  This method
        supports two geo-reduction strategies:

        1. **Explicit geo codes**: Pass ``geo_codes=["AT", "DE", "FR", ...]``
           to request only those codes, split into batches of ``geo_batch_size``.
        2. **NUTS level filter**: Pass ``nuts_level=0`` to auto-filter the
           DSD's geo codelist to country-level (2-char) codes only.  Use
           ``nuts_level=1`` for NUTS-1 regions, etc.

        If neither is provided, geo chunking is **automatically enabled** when
        the DSD's geo dimension has more than ``geo_batch_size`` codes — the
        codes are split into batches and each batch is queried separately.

        Time chunking uses ``chunk_ranges`` or decade-sized windows as before.
        When both time and geo chunking are active, the method iterates
        ``time_chunks × geo_chunks`` (outer=time, inner=geo).

        Args:
            dataflow_id: Eurostat dataflow, e.g. ``"prc_hicp_manr"``.
            key: SDMX dimension key.  Use ``"."`` for all series.
            version: Dataflow version (used for DSD lookup).
            series_id: Logical series id for the returned records.
            start_year: Start of time range (default 1960).
            end_year: End of time range (default 2026).
            chunk_ranges: Explicit time-range pairs; overrides year range.
            geo_codes: Explicit list of geo codes to request.
            geo_batch_size: Max geo codes per request (default 40).
            nuts_level: If set, auto-filter DSD geo codes to this NUTS level.
            limit: Max observations per request (0 = unlimited).
            on_chunk: Callback invoked after each time chunk completes.
        """
        if chunk_ranges is None:
            chunk_ranges = _build_decade_chunks(start_year, end_year)

        # ── Resolve geo chunks ────────────────────────────────────────
        geo_chunks: list[str] | None = None
        geo_position: int = 0
        total_dims: int = 0

        if geo_codes is not None:
            # Caller provided explicit geo codes
            geo_chunks = _build_geo_chunks(geo_codes, geo_batch_size)
        elif nuts_level is not None:
            # Auto-filter from DSD
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
            # Auto-detect: chunk if geo dimension is large
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

        # ── Fetch: time × geo ─────────────────────────────────────────
        all_obs: list[EurostatObservation] = []
        for start_period, end_period in chunk_ranges:
            chunk_obs: list[EurostatObservation] = []

            if geo_chunks and total_dims > 0:
                for geo_fragment in geo_chunks:
                    effective_key = _inject_geo_into_key(
                        key, geo_fragment, geo_position, total_dims,
                    )
                    logger.info(
                        "Eurostat chunked fetch %s [%s – %s] geo=%s",
                        dataflow_id, start_period, end_period,
                        geo_fragment[:40] + ("..." if len(geo_fragment) > 40 else ""),
                    )
                    obs = self.get_data(
                        dataflow_id,
                        effective_key,
                        series_id=series_id or dataflow_id,
                        start_period=start_period,
                        end_period=end_period,
                        limit=limit,
                    )
                    chunk_obs.extend(obs)
            else:
                logger.info(
                    "Eurostat chunked fetch %s [%s – %s]",
                    dataflow_id, start_period, end_period,
                )
                obs = self.get_data(
                    dataflow_id,
                    key,
                    series_id=series_id or dataflow_id,
                    start_period=start_period,
                    end_period=end_period,
                    limit=limit,
                )
                chunk_obs.extend(obs)

            all_obs.extend(chunk_obs)
            if on_chunk is not None:
                on_chunk(chunk_obs, start_period, end_period)

        return all_obs
