"""Base SDMX client with Template Method hooks for provider differences."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Sequence

import requests

from ._config import SDMXProviderConfig
from ._errors import SDMXAPIError, SDMXRateLimitError
from ._json_parser import parse_sdmx_json_observations
from ._parsing import build_decade_chunks, extract_id_from_urn, normalize_date
from ._types import (
    SDMXDataStructure,
    SDMXDataflow,
    SDMXDimension,
    SDMXObservation,
    SDMXSizeEstimate,
    SDMXStructureSummary,
)

logger = logging.getLogger(__name__)


class SDMXClient:
    """Config-driven SDMX client with overridable hooks.

    Implements the full catalog discovery pattern:
    ``list_dataflows`` → ``get_datastructure`` → ``summarize_structure``
    → ``estimate_size`` → ``fetch_dataset_chunked``.

    Provider-specific behaviour is handled via:
    - Config: URL patterns, agency, auth, rate-limit delay
    - Hooks: ``_build_*_url()``, ``_parse_data_response()``, ``_normalize_date()``
    """

    def __init__(
        self,
        config: SDMXProviderConfig,
        *,
        timeout: int = 30,
        api_key: str | None = None,
        api_error_cls: type[SDMXAPIError] = SDMXAPIError,
        rate_limit_error_cls: type[SDMXRateLimitError] = SDMXRateLimitError,
    ) -> None:
        self.config = config
        self.timeout = timeout
        self._api_error_cls = api_error_cls
        self._rate_limit_error_cls = rate_limit_error_cls

        self.session = requests.Session()
        self._setup_session(api_key)

        self._last_request: float = 0.0
        self._dataflow_cache: list[SDMXDataflow] | None = None
        self._structure_cache: dict[str, SDMXDataStructure] = {}

    def _setup_session(self, api_key: str | None) -> None:
        headers: dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": "AnalystEngine/1.0",
        }
        if self.config.requires_auth:
            key = api_key
            if not key:
                from env import get_env_value
                key = get_env_value(self.config.auth_env_var)
            if key:
                headers[self.config.auth_header] = key
        self.session.headers.update(headers)

    # ── Rate-limited GET ──────────────────────────────────────────────

    def _get(
        self,
        url: str,
        params: dict[str, str] | None = None,
        *,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        """Internal GET with rate-limit delay and 429 detection."""
        elapsed = time.monotonic() - self._last_request
        if elapsed < self.config.request_delay:
            time.sleep(self.config.request_delay - elapsed)
        self._last_request = time.monotonic()

        response = self.session.get(
            url, params=params, headers=headers, timeout=self.timeout,
        )
        name = self.config.provider_name
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            suffix = f" retry_after={retry_after}" if retry_after else ""
            raise self._rate_limit_error_cls(
                f"{name} rate limit exceeded for {url}.{suffix}"
            )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = response.text.strip()
            if detail:
                raise self._api_error_cls(
                    f"{name} request failed for {url}: HTTP {response.status_code}: {detail[:200]}"
                ) from exc
            raise self._api_error_cls(
                f"{name} request failed for {url}: HTTP {response.status_code}"
            ) from exc
        return response

    def _response_json(
        self, response: requests.Response, *, context: str = "",
    ) -> dict[str, Any]:
        """Decode ``response.json()``, tolerating empty bodies / 204.

        ECB (and likely other SDMX 2.1 providers) returns HTTP 200 with a
        zero-byte body when the requested dataflow/series matches no
        observations. The default ``response.json()`` raises on byte 0,
        which trips the connector's failure breaker for what is really a
        "no data" signal. Returning ``{}`` lets downstream parsers emit
        zero observations — the same shape they produce for a populated
        dataset with no matching rows.
        """
        if response.status_code == 204 or not response.content:
            suffix = f" ({context})" if context else ""
            logger.info(
                "%s returned empty body for %s%s — treating as zero observations",
                self.config.provider_name, response.url, suffix,
            )
            return {}
        return response.json()

    # ── Hook: date normalization ──────────────────────────────────────

    def _normalize_date(self, raw: str) -> str:
        """Override for provider-specific date formats (IMF, Eurostat)."""
        return normalize_date(raw)

    # ── Hook: URL builders ────────────────────────────────────────────

    def _build_dataflow_list_url(self) -> str:
        return f"{self.config.base_url}/dataflow/{self.config.default_agency}/all/latest"

    def _build_structure_url(self, dataflow_id: str, version: str) -> str:
        return f"{self.config.base_url}/dataflow/{self.config.default_agency}/{dataflow_id}/{version}"

    def _build_data_url(self, dataflow_id: str, key: str, **kwargs: Any) -> str:
        return f"{self.config.base_url}/data/{dataflow_id}/{key}"

    def _build_estimate_url(self, dataflow_id: str, **kwargs: Any) -> str:
        all_key = self.config.default_all_key
        return f"{self.config.base_url}/data/{dataflow_id}/{all_key}"

    # ── Hook: data response parsing ───────────────────────────────────

    def _parse_data_response(
        self,
        response: requests.Response,
        *,
        series_id: str,
        dataflow: str,
        limit: int,
    ) -> list[SDMXObservation]:
        """Default: SDMX-JSON. Override for CSV (BIS) or other formats."""
        return parse_sdmx_json_observations(
            self._response_json(
                response, context=f"{dataflow}/{series_id or '*'}",
            ),
            series_id=series_id,
            dataflow=dataflow,
            limit=limit,
            normalize_date_fn=self._normalize_date,
        )

    # ── Data endpoint ─────────────────────────────────────────────────

    def get_data(
        self,
        dataflow_id: str,
        key: str = ".",
        *,
        series_id: str = "",
        start_period: str | None = None,
        end_period: str | None = None,
        limit: int = 100,
        **kwargs: Any,
    ) -> list[SDMXObservation]:
        """Fetch observations from an SDMX dataflow."""
        observations, _payload, _params = self.get_data_with_raw(
            dataflow_id, key,
            series_id=series_id,
            start_period=start_period,
            end_period=end_period,
            limit=limit,
            **kwargs,
        )
        return observations

    def get_data_with_raw(
        self,
        dataflow_id: str,
        key: str = ".",
        *,
        series_id: str = "",
        start_period: str | None = None,
        end_period: str | None = None,
        limit: int = 100,
        **kwargs: Any,
    ) -> tuple[list[SDMXObservation], dict, dict[str, str]]:
        """Fetch observations and return parsed obs + raw HTTP body + request params.

        Used by the issue #69 ``obs_raw`` write path so SDMX-JSON
        consumers (IMF / BIS / ECB / Eurostat / OECD / UNSD / ILO)
        can land both the parsed series and the upstream bytes
        through a single HTTP call.
        """
        url = self._build_data_url(dataflow_id, key, **kwargs)
        params: dict[str, str] = {"format": "jsondata"}
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if limit:
            params["lastNObservations"] = str(limit)

        response = self._get(url, params)
        observations = self._parse_data_response(
            response, series_id=series_id, dataflow=dataflow_id, limit=limit,
        )
        try:
            payload = self._response_json(
                response, context=f"{dataflow_id}/{series_id or '*'}",
            )
        except ValueError:
            payload = {}
        record_params = dict(params)
        record_params["dataflow_id"] = dataflow_id
        record_params["key"] = key
        return observations, payload, record_params

    # ── Catalog discovery ─────────────────────────────────────────────

    def list_dataflows(self, **kwargs: Any) -> list[SDMXDataflow]:
        """Fetch the full dataflow catalog. Results are cached."""
        if self._dataflow_cache is not None:
            return list(self._dataflow_cache)

        url = self._build_dataflow_list_url()
        response = self._get(url, headers={"Accept": "application/json"})
        # A catalog endpoint returning an empty body is an outage, not a
        # "no data" signal — raising here keeps `refresh_catalog()` and
        # config generators from silently reporting a zero-flow success.
        if response.status_code == 204 or not response.content:
            raise self._api_error_cls(
                f"{self.config.provider_name} returned empty body for "
                f"dataflow catalog at {url}"
            )
        data = self._response_json(response, context="dataflow catalog")

        dataflows: list[SDMXDataflow] = []
        for df_node in data.get("data", {}).get("dataflows", []):
            structure_id = ""
            structure_version = ""
            struct_val = df_node.get("structure", "")
            if isinstance(struct_val, str) and "DataStructure=" in struct_val:
                cl_id = extract_id_from_urn(struct_val)
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

            dataflows.append(SDMXDataflow(
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

    # ── Structure discovery ───────────────────────────────────────────

    def get_datastructure(
        self,
        dataflow_id: str,
        version: str | None = None,
        **kwargs: Any,
    ) -> SDMXDataStructure:
        """Fetch the DSD for a dataflow with ``references=all``."""
        if version:
            df_version = version
        else:
            flows = self.list_dataflows()
            match = next((f for f in flows if f.id == dataflow_id), None)
            df_version = (match.version if match else version) or self.config.default_version

        cache_key = f"{dataflow_id}/{df_version}"
        if cache_key in self._structure_cache:
            return self._structure_cache[cache_key]

        url = self._build_structure_url(dataflow_id, df_version)
        params: dict[str, str] = {"references": "all"}
        response = self._get(url, params, headers={"Accept": "application/json"})
        data = self._response_json(
            response, context=f"DSD {dataflow_id}/{df_version}",
        )

        structures = data.get("data", {}).get("dataStructures", [])
        if not structures:
            raise self._api_error_cls(
                f"No data structure found for dataflow {dataflow_id}/{df_version}"
            )

        struct_node = structures[0]
        components = struct_node.get("dataStructureComponents", {})
        dim_list = components.get("dimensionList", {})

        # Parse codelists
        codelist_map: dict[str, list[str]] = {}
        for cl in data.get("data", {}).get("codelists", []):
            cl_id = cl.get("id", "")
            codes = [c.get("id", "") for c in cl.get("codes", [])]
            if cl_id:
                codelist_map[cl_id] = codes

        # Concept → codelist mapping
        concept_codelist: dict[str, str] = {}
        for cs in data.get("data", {}).get("conceptSchemes", []):
            for concept in cs.get("concepts", []):
                cr = concept.get("coreRepresentation", {})
                enum_urn = cr.get("enumeration", "")
                if isinstance(enum_urn, str) and "Codelist=" in enum_urn:
                    cl_id = extract_id_from_urn(enum_urn)
                    if cl_id:
                        concept_codelist[concept.get("id", "")] = cl_id

        dimensions: list[SDMXDimension] = []
        for dim_node in dim_list.get("dimensions", []):
            dim_id = dim_node.get("id", "")
            position = dim_node.get("position", 0)

            name_val = dim_node.get("name", "")
            if isinstance(name_val, dict):
                name_val = name_val.get("en", "") or next(iter(name_val.values()), "")
            if not name_val:
                name_val = dim_id

            codes: tuple[str, ...] = ()
            code_count = 0
            local_repr = dim_node.get("localRepresentation", {})
            enum_val = local_repr.get("enumeration")

            cl_id = ""
            if isinstance(enum_val, str) and "Codelist=" in enum_val:
                cl_id = extract_id_from_urn(enum_val)
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

            dimensions.append(SDMXDimension(
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
            dimensions.append(SDMXDimension(
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

        result = SDMXDataStructure(
            id=struct_node.get("id", ""),
            version=struct_node.get("version", df_version),
            name=str(name_val),
            dimensions=tuple(dimensions),
            dataflow_id=dataflow_id,
            dataflow_version=df_version,
        )
        self._structure_cache[cache_key] = result
        return result

    # ── Structure summary ─────────────────────────────────────────────

    def summarize_structure(
        self,
        dataflow_id: str,
        version: str | None = None,
        **kwargs: Any,
    ) -> SDMXStructureSummary:
        """Return a compact summary combining dataflow metadata and DSD."""
        flows = self.list_dataflows()
        flow = next((f for f in flows if f.id == dataflow_id), None)
        structure = self.get_datastructure(dataflow_id, version, **kwargs)

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

        return SDMXStructureSummary(
            dataflow_id=dataflow_id,
            version=flow.version if flow else (version or ""),
            name=flow.name if flow else structure.name,
            structure_id=structure.id,
            time_dimension_id=time_dim_id,
            series_dimensions=series_dims,
            code_counts=code_counts,
            estimated_series=estimated_series,
        )

    # ── Size estimation ───────────────────────────────────────────────

    def estimate_size(
        self,
        dataflow_id: str,
        version: str = "1.0",
        **kwargs: Any,
    ) -> SDMXSizeEstimate:
        """Probe a dataflow with limit=1 to estimate total size."""
        total_series = 0
        time_periods = 1
        # An explicit zero-observation response from upstream means the
        # dataflow truly has no data — return 0 instead of falling back to
        # the codelist-product, which would advertise a fake nonzero size.
        probe_was_empty = False

        try:
            url = self._build_estimate_url(dataflow_id, **kwargs)
            params: dict[str, str] = {
                "format": "jsondata",
                "lastNObservations": "1",
            }
            response = self._get(url, params)
            if response.status_code == 204 or not response.content:
                probe_was_empty = True
            data = self._response_json(
                response, context=f"estimate {dataflow_id}",
            )
            datasets = data.get("dataSets", [])
            if datasets:
                all_series = datasets[0].get("series", {})
                total_series = len(all_series)
        except (SDMXAPIError, SDMXRateLimitError):
            pass

        if total_series == 0 and not probe_was_empty:
            try:
                structure = self.get_datastructure(dataflow_id, version, **kwargs)
                total_series = 1
                for d in structure.dimensions:
                    if not d.is_time and d.code_count > 0:
                        total_series *= d.code_count
            except (SDMXAPIError, SDMXRateLimitError):
                pass

        return SDMXSizeEstimate(
            dataflow_id=dataflow_id,
            version=version,
            total_series=total_series,
            time_periods=time_periods,
            estimated_observations=total_series * time_periods,
        )

    # ── Chunked fetch ─────────────────────────────────────────────────

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
        limit: int = 0,
        on_chunk: Callable[[list[SDMXObservation], str, str], None] | None = None,
        **kwargs: Any,
    ) -> list[SDMXObservation]:
        """Fetch a dataset with time-range chunking."""
        if chunk_ranges is None:
            chunk_ranges = build_decade_chunks(start_year, end_year)

        name = self.config.provider_name
        all_obs: list[SDMXObservation] = []
        for start_period, end_period in chunk_ranges:
            logger.info(
                "%s chunked fetch %s [%s – %s]",
                name, dataflow_id, start_period, end_period,
            )
            obs = self.get_data(
                dataflow_id,
                key,
                series_id=series_id or dataflow_id,
                start_period=start_period,
                end_period=end_period,
                limit=limit,
                **kwargs,
            )
            all_obs.extend(obs)
            if on_chunk is not None:
                on_chunk(obs, start_period, end_period)

        return all_obs
