"""IMF SDMX 3.0 API client — CPI, FX reserves, trade, and GDP dataflows.

Uses the Azure-hosted API at ``api.imf.org`` which requires an
``Ocp-Apim-Subscription-Key`` header.  SDMX 3.0 supports JSON responses
and point-in-time vintage queries via the ``asOf`` parameter.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Callable, Sequence

import requests

from analyst.env import get_env_value

logger = logging.getLogger(__name__)

_QUARTER_MAP = {"Q1": "01", "Q2": "04", "Q3": "07", "Q4": "10"}


def _normalize_date(raw: str) -> str:
    """Normalize IMF date strings to YYYY-MM-DD.

    Handles: ``"2025-M09"`` → ``"2025-09-01"`` (new API monthly),
             ``"2024-Q1"``  → ``"2024-01-01"``,
             ``"2024-01"``  → ``"2024-01-01"`` (legacy monthly),
             ``"2024"``     → ``"2024-01-01"``.
    """
    # New API monthly format: 2025-M09
    m = re.match(r"^(\d{4})-M(\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    # Quarterly: 2024-Q1
    m = re.match(r"^(\d{4})-Q(\d)$", raw)
    if m:
        return f"{m.group(1)}-{_QUARTER_MAP.get('Q' + m.group(2), '01')}-01"
    # Legacy monthly: 2024-01
    if re.match(r"^\d{4}-\d{2}$", raw):
        return f"{raw}-01"
    # Annual: 2024
    if re.match(r"^\d{4}$", raw):
        return f"{raw}-01-01"
    return raw


@dataclass(frozen=True)
class IMFObservation:
    """A single observation from the IMF SDMX API."""

    series_id: str
    date: str
    value: float
    dataflow: str = ""


@dataclass(frozen=True)
class IMFVintageObservation:
    """A single vintage (point-in-time) observation from the IMF SDMX 3.0 API."""

    series_id: str
    date: str           # observation date
    vintage_date: str   # asOf date
    value: float
    dataflow: str = ""


@dataclass(frozen=True)
class IMFDataflow:
    """Represents a dataset in the IMF catalog."""

    id: str
    agency_id: str
    version: str
    name: str = ""
    description: str = ""
    structure_id: str = ""
    structure_version: str = ""


@dataclass(frozen=True)
class IMFDimension:
    """A single dimension in an IMF data structure definition."""

    id: str
    position: int
    name: str = ""
    code_count: int = 0
    is_time: bool = False
    codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class IMFDataStructure:
    """Full DSD with dimensions for an IMF dataflow."""

    id: str
    version: str
    name: str = ""
    dimensions: tuple[IMFDimension, ...] = ()
    dataflow_id: str = ""
    dataflow_version: str = ""


@dataclass(frozen=True)
class IMFStructureSummary:
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
class IMFSizeEstimate:
    """Observation count estimate for an IMF dataflow."""

    dataflow_id: str
    version: str
    total_series: int = 0
    time_periods: int = 0
    estimated_observations: int = 0


class IMFAPIError(RuntimeError):
    """Base error for IMF API failures."""


class IMFRateLimitError(IMFAPIError):
    """Raised when IMF throttles a request (HTTP 429)."""


def _build_decade_chunks(start_year: int, end_year: int) -> list[tuple[str, str]]:
    """Split a year range into decade-sized chunks for time-range queries."""
    chunks: list[tuple[str, str]] = []
    year = start_year
    while year <= end_year:
        chunk_end = min(year + 9, end_year)
        chunks.append((str(year), str(chunk_end)))
        year = chunk_end + 1
    return chunks


class IMFClient:
    """Client for the IMF SDMX 3.0 REST API (requires API key)."""

    BASE_URL = "https://api.imf.org/external/sdmx/3.0"

    def __init__(self, api_key: str | None = None, *, timeout: int = 30) -> None:
        self.session = requests.Session()
        key = api_key or get_env_value("IMF_API_KEY")
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "AnalystEngine/1.0",
            "Ocp-Apim-Subscription-Key": key,
        })
        self.timeout = timeout
        self._last_request: float = 0.0
        self._request_delay: float = 1.0
        self._dataflow_cache: list[IMFDataflow] | None = None
        self._structure_cache: dict[str, IMFDataStructure] = {}

    def _get(
        self,
        url: str,
        params: dict[str, str] | None = None,
    ) -> requests.Response:
        """Internal GET with rate-limit delay and 429 detection."""
        elapsed = time.monotonic() - self._last_request
        if elapsed < self._request_delay:
            time.sleep(self._request_delay - elapsed)
        self._last_request = time.monotonic()

        response = self.session.get(url, params=params, timeout=self.timeout)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            suffix = f" retry_after={retry_after}" if retry_after else ""
            raise IMFRateLimitError(f"IMF rate limit exceeded for {url}.{suffix}")
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = response.text.strip()
            if detail:
                raise IMFAPIError(
                    f"IMF request failed for {url}: HTTP {response.status_code}: {detail[:200]}"
                ) from exc
            raise IMFAPIError(
                f"IMF request failed for {url}: HTTP {response.status_code}"
            ) from exc
        return response

    def get_data(
        self,
        dataflow_id: str,
        key: str,
        *,
        series_id: str,
        version: str,
        start_period: str | None = None,
        end_period: str | None = None,
        limit: int = 100,
    ) -> list[IMFObservation]:
        """Fetch observations from an SDMX 3.0 dataflow as JSON.

        Args:
            dataflow_id: IMF dataflow, e.g. ``"CPI"``, ``"IRFCL"``.
            key: Dimension key, e.g. ``"CHN.CPI._T.IX.M"``.
            series_id: Logical series id for the returned records.
            version: Exact dataflow version, e.g. ``"5.0.0"``.
            start_period: Optional start filter, e.g. ``"2024"``.
            end_period: Optional end filter, e.g. ``"2026"``.
            limit: Maximum observations to return.
        """
        url = f"{self.BASE_URL}/data/dataflow/IMF.STA/{dataflow_id}/{version}/{key}"
        params: dict[str, str] = {
            "attributes": "none",
            "measures": "all",
        }
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if limit:
            params["lastNObservations"] = str(limit)

        response = self._get(url, params)
        return self._parse_json(
            response.json(), series_id=series_id, dataflow=dataflow_id, limit=limit,
        )

    @staticmethod
    def _parse_json(
        data: dict,
        *,
        series_id: str,
        dataflow: str,
        limit: int,
    ) -> list[IMFObservation]:
        """Parse SDMX 3.0 JSON response into observations."""
        observations: list[IMFObservation] = []

        try:
            datasets = data["data"]["dataSets"]
            structures = data["data"]["structures"]
        except (KeyError, TypeError):
            return observations

        if not datasets or not structures:
            return observations

        # Find TIME_PERIOD dimension in observation dimensions
        obs_dims = structures[0].get("dimensions", {}).get("observation", [])
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
            time_map[str(i)] = val["value"]

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
                    observations.append(IMFObservation(
                        series_id=series_id,
                        date=_normalize_date(period),
                        value=float(value),
                        dataflow=dataflow,
                    ))
                except (ValueError, TypeError):
                    continue

        observations.sort(key=lambda o: o.date, reverse=True)
        return observations[:limit] if limit else observations

    def get_vintages(
        self,
        dataflow_id: str,
        key: str,
        *,
        series_id: str,
        version: str,
        as_of_dates: list[str],
        start_period: str | None = None,
        limit: int = 100,
    ) -> list[IMFVintageObservation]:
        """Fetch vintage (point-in-time) observations for multiple asOf dates.

        Args:
            dataflow_id: IMF dataflow, e.g. ``"QNEA"``.
            key: Dimension key, e.g. ``"CHN.B1GQ.V.NSA.XDC.Q"``.
            series_id: Logical series id for the returned records.
            version: Exact dataflow version, e.g. ``"7.0.0"``.
            as_of_dates: List of vintage dates (``"YYYY-MM-DD"``).
            start_period: Optional start filter, e.g. ``"2024"``.
            limit: Maximum observations per vintage call.
        """
        results: list[IMFVintageObservation] = []
        for _i, as_of in enumerate(as_of_dates):
            try:
                url = f"{self.BASE_URL}/data/dataflow/IMF.STA/{dataflow_id}/{version}/{key}"
                params: dict[str, str] = {
                    "attributes": "none",
                    "measures": "all",
                    "asOf": as_of,
                }
                if start_period:
                    params["startPeriod"] = start_period
                if limit:
                    params["lastNObservations"] = str(limit)

                response = self._get(url, params)
                obs_list = self._parse_json(
                    response.json(), series_id=series_id, dataflow=dataflow_id, limit=limit,
                )
                for obs in obs_list:
                    results.append(IMFVintageObservation(
                        series_id=obs.series_id,
                        date=obs.date,
                        vintage_date=as_of,
                        value=obs.value,
                        dataflow=obs.dataflow,
                    ))
            except Exception:
                logger.warning("IMF vintage fetch failed for asOf=%s", as_of, exc_info=True)
        return results

    # ── Catalog discovery methods ─────────────────────────────────────

    def list_dataflows(self) -> list[IMFDataflow]:
        """Fetch the full IMF dataflow catalog. Results are cached."""
        if self._dataflow_cache is not None:
            return list(self._dataflow_cache)

        url = f"{self.BASE_URL}/structure/dataflow/IMF.STA"
        response = self._get(url)
        data = response.json()

        dataflows: list[IMFDataflow] = []
        for df_node in data.get("data", {}).get("dataflows", []):
            # Extract structure reference from the URN
            structure_id = ""
            structure_version = ""
            struct_val = df_node.get("structure")
            if isinstance(struct_val, str) and "DataStructure=" in struct_val:
                # URN format: urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure=IMF.STA:DS_CPI(5.0.0)
                after_eq = struct_val.split("DataStructure=")[-1]
                # agency:id(version)
                if ":" in after_eq:
                    remainder = after_eq.split(":", 1)[-1]
                    if "(" in remainder:
                        structure_id = remainder.split("(")[0]
                        structure_version = remainder.split("(")[1].rstrip(")")
                    else:
                        structure_id = remainder
            elif isinstance(struct_val, dict):
                structure_id = struct_val.get("id", "")
                structure_version = struct_val.get("version", "")

            # Name can be a dict of localized strings or a plain string
            name_val = df_node.get("name", "")
            if isinstance(name_val, dict):
                name_val = name_val.get("en", "") or next(iter(name_val.values()), "")
            desc_val = df_node.get("description", "")
            if isinstance(desc_val, dict):
                desc_val = desc_val.get("en", "") or next(iter(desc_val.values()), "")

            dataflows.append(IMFDataflow(
                id=df_node.get("id", ""),
                agency_id=df_node.get("agencyID", ""),
                version=df_node.get("version", ""),
                name=name_val,
                description=desc_val,
                structure_id=structure_id,
                structure_version=structure_version,
            ))

        self._dataflow_cache = dataflows
        return list(dataflows)

    def get_datastructure(
        self,
        dataflow_id: str,
        version: str | None = None,
    ) -> IMFDataStructure:
        """Fetch the data structure definition for a dataflow.

        Uses the dataflow endpoint with ``references=all`` to retrieve
        the DSD, codelists, and concept schemes in a single call (the
        direct datastructure endpoint returns 204 on the IMF API).
        """
        # Find the dataflow to get its version
        flows = self.list_dataflows()
        match = None
        for flow in flows:
            if flow.id == dataflow_id:
                if version is None or flow.version == version:
                    match = flow
                    break

        df_version = (match.version if match else version) or "latest"
        cache_key = f"{dataflow_id}/{df_version}"
        if cache_key in self._structure_cache:
            return self._structure_cache[cache_key]

        # Fetch dataflow with all referenced artefacts
        url = f"{self.BASE_URL}/structure/dataflow/IMF.STA/{dataflow_id}/{df_version}"
        params: dict[str, str] = {"references": "all"}
        response = self._get(url, params)
        data = response.json()

        structures = data.get("data", {}).get("dataStructures", [])
        if not structures:
            raise IMFAPIError(f"No data structure found for dataflow {dataflow_id}/{df_version}")

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
                    cl_id = self._extract_id_from_urn(enum_urn)
                    if cl_id:
                        concept_codelist[concept.get("id", "")] = cl_id

        dimensions: list[IMFDimension] = []
        for dim_node in dim_list.get("dimensions", []):
            dim_id = dim_node.get("id", "")
            position = dim_node.get("position", 0)

            name_val = dim_node.get("name", "")
            if isinstance(name_val, dict):
                name_val = name_val.get("en", "") or next(iter(name_val.values()), "")
            if not name_val:
                name_val = dim_id

            # Resolve codelist: try localRepresentation, then concept mapping,
            # then CL_{dim_id} naming convention
            codes: tuple[str, ...] = ()
            code_count = 0
            local_repr = dim_node.get("localRepresentation", {})
            enum_val = local_repr.get("enumeration")

            cl_id = ""
            if isinstance(enum_val, str) and "Codelist=" in enum_val:
                cl_id = self._extract_id_from_urn(enum_val)
            elif isinstance(enum_val, dict):
                cl_id = enum_val.get("id", "")

            # Fallback: concept scheme mapping
            if not cl_id:
                cl_id = concept_codelist.get(dim_id, "")

            # Fallback: CL_{dim_id} naming convention
            if not cl_id:
                candidate = f"CL_{dim_id}"
                if candidate in codelist_map:
                    cl_id = candidate

            if cl_id and cl_id in codelist_map:
                code_list = codelist_map[cl_id]
                codes = tuple(code_list)
                code_count = len(codes)

            dimensions.append(IMFDimension(
                id=dim_id,
                position=position,
                name=name_val,
                code_count=code_count,
                is_time=False,
                codes=codes,
            ))

        # Time dimensions
        for td_node in dim_list.get("timeDimensions", []):
            td_id = td_node.get("id", "TIME_PERIOD")
            name_val = td_node.get("name", "")
            if isinstance(name_val, dict):
                name_val = name_val.get("en", "") or next(iter(name_val.values()), "")
            dimensions.append(IMFDimension(
                id=td_id,
                position=td_node.get("position", len(dimensions)),
                name=name_val or td_id,
                code_count=0,
                is_time=True,
                codes=(),
            ))

        dimensions.sort(key=lambda d: d.position)

        name_val = struct_node.get("name", "")
        if isinstance(name_val, dict):
            name_val = name_val.get("en", "") or next(iter(name_val.values()), "")

        dsd_id = struct_node.get("id", f"DS_{dataflow_id}")
        dsd_version = struct_node.get("version", df_version)
        result = IMFDataStructure(
            id=dsd_id,
            version=dsd_version,
            name=name_val,
            dimensions=tuple(dimensions),
            dataflow_id=dataflow_id,
            dataflow_version=match.version if match else (version or ""),
        )
        self._structure_cache[cache_key] = result
        return result

    @staticmethod
    def _extract_id_from_urn(urn: str) -> str:
        """Extract the artefact ID from an SDMX URN.

        Example: ``"urn:sdmx:...Codelist=IMF:CL_COUNTRY(1.0+.0)"`` → ``"CL_COUNTRY"``
        """
        if "=" not in urn:
            return ""
        after_eq = urn.rsplit("=", 1)[-1]
        if ":" in after_eq:
            after_eq = after_eq.split(":", 1)[-1]
        if "(" in after_eq:
            return after_eq.split("(")[0]
        return after_eq

    def summarize_structure(
        self,
        dataflow_id: str,
        version: str | None = None,
    ) -> IMFStructureSummary:
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

        return IMFStructureSummary(
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
        version: str,
        key: str = "all",
    ) -> IMFSizeEstimate:
        """Probe a dataflow with limit=1 to estimate its total size.

        First tries a data endpoint probe.  If that returns 0 series
        (the IMF API often returns an empty response for ``key=all``),
        falls back to estimating from the DSD code counts.
        """
        total_series = 0
        time_periods = 1

        try:
            url = f"{self.BASE_URL}/data/dataflow/IMF.STA/{dataflow_id}/{version}/{key}"
            params: dict[str, str] = {
                "attributes": "none",
                "measures": "all",
                "lastNObservations": "1",
            }
            response = self._get(url, params)
            data = response.json()

            inner = data.get("data", data)
            datasets = inner.get("dataSets", [])
            total_series = sum(len(ds.get("series", {})) for ds in datasets)
            if total_series == 0:
                total_series = sum(len(ds.get("observations", {})) for ds in datasets)

            obs_dims = inner.get("structures", [{}])[0].get("dimensions", {}).get("observation", [])
            for dim in obs_dims:
                if dim.get("id") == "TIME_PERIOD":
                    time_periods = max(len(dim.get("values", [])), 1)
                    break
        except (IMFAPIError, IMFRateLimitError):
            pass

        # Fall back to DSD-based estimate if probe returned nothing
        if total_series == 0:
            try:
                structure = self.get_datastructure(dataflow_id, version)
                total_series = 1
                for d in structure.dimensions:
                    if not d.is_time and d.code_count > 0:
                        total_series *= d.code_count
            except (IMFAPIError, IMFRateLimitError):
                pass

        return IMFSizeEstimate(
            dataflow_id=dataflow_id,
            version=version,
            total_series=total_series,
            time_periods=time_periods,
            estimated_observations=total_series * time_periods,
        )

    def fetch_dataset_chunked(
        self,
        dataflow_id: str,
        version: str,
        key: str = "all",
        *,
        series_id: str = "",
        start_year: int = 1960,
        end_year: int = 2026,
        chunk_ranges: Sequence[tuple[str, str]] | None = None,
        limit: int = 0,
        on_chunk: Callable[[list[IMFObservation], str, str], None] | None = None,
    ) -> list[IMFObservation]:
        """Fetch a dataset with time-range chunking.

        If ``chunk_ranges`` is given those pairs are used; otherwise the
        year range is split into decade-sized windows automatically.
        """
        if chunk_ranges is None:
            chunk_ranges = _build_decade_chunks(start_year, end_year)

        all_obs: list[IMFObservation] = []
        for start_period, end_period in chunk_ranges:
            logger.info(
                "IMF chunked fetch %s/%s [%s – %s]",
                dataflow_id, version, start_period, end_period,
            )
            obs = self.get_data(
                dataflow_id,
                key,
                series_id=series_id or dataflow_id,
                version=version,
                start_period=start_period,
                end_period=end_period,
                limit=limit,
            )
            all_obs.extend(obs)
            if on_chunk is not None:
                on_chunk(obs, start_period, end_period)

        return all_obs
