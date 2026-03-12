"""OECD Data Explorer / SDMX REST client."""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date
from typing import Mapping, Sequence

import requests

logger = logging.getLogger(__name__)

_QUARTER_MAP = {"Q1": "01", "Q2": "04", "Q3": "07", "Q4": "10"}
_SEMESTER_MAP = {"S1": "01", "S2": "07"}
_XML_NS = {
    "common": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
    "message": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "structure": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
}
_XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"
_EMPTY_DATA_PAYLOAD = {"data": {"dataSets": [], "structures": []}}


def _normalize_date(raw: str) -> str:
    """Normalize OECD period strings to YYYY-MM-DD where possible."""

    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw

    m = re.match(r"^(\d{4})-Q(\d)$", raw)
    if m:
        return f"{m.group(1)}-{_QUARTER_MAP.get('Q' + m.group(2), '01')}-01"

    m = re.match(r"^(\d{4})-S([12])$", raw)
    if m:
        return f"{m.group(1)}-{_SEMESTER_MAP.get('S' + m.group(2), '01')}-01"

    m = re.match(r"^(\d{4})-W(\d{2})$", raw)
    if m:
        try:
            return date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1).isoformat()
        except ValueError:
            return raw

    if re.match(r"^\d{4}-\d{2}$", raw):
        return f"{raw}-01"

    if re.match(r"^\d{4}$", raw):
        return f"{raw}-01-01"

    return raw


def _pick_localized_xml_text(parent: ET.Element, tag: str) -> str:
    values = parent.findall(tag, _XML_NS)
    if not values:
        return ""
    for lang in ("en", "en-US"):
        for value in values:
            if value.attrib.get(_XML_LANG) == lang and value.text:
                return value.text.strip()
    for value in values:
        if value.text and value.text.strip():
            return value.text.strip()
    return ""


def _parse_assignments(raw: str) -> dict[str, str]:
    assignments: dict[str, str] = {}
    for part in raw.split(","):
        left, sep, right = part.partition("=")
        if not sep:
            continue
        key = left.strip()
        value = right.strip()
        if key and value:
            assignments[key] = value
    return assignments


@dataclass(frozen=True)
class OECDCode:
    """A single code from an OECD codelist."""

    id: str
    name: str = ""
    description: str = ""
    parent_id: str = ""


@dataclass(frozen=True)
class OECDDimension:
    """A dimension declared in an OECD datastructure."""

    id: str
    position: int
    name: str = ""
    codelist_id: str = ""
    codelist_agency_id: str = ""
    codelist_version: str = ""
    is_time: bool = False
    codes: tuple[OECDCode, ...] = ()


@dataclass(frozen=True)
class OECDDataflow:
    """Metadata for an OECD dataflow exposed in Data Explorer."""

    id: str
    agency_id: str
    version: str
    name: str = ""
    description: str = ""
    structure_id: str = ""
    structure_agency_id: str = ""
    structure_version: str = ""
    defaults: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class OECDDataStructure:
    """Datastructure metadata and codelists for a dataflow."""

    id: str
    agency_id: str
    version: str
    name: str = ""
    dimensions: tuple[OECDDimension, ...] = ()
    dataflow_id: str = ""
    dataflow_agency_id: str = ""
    dataflow_version: str = ""
    defaults: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class OECDStructureSummary:
    """Compact structure metadata for catalogue discovery and inspection."""

    dataflow_id: str
    agency_id: str
    version: str
    name: str = ""
    description: str = ""
    structure_id: str = ""
    time_dimension_id: str = ""
    series_dimensions: tuple[str, ...] = ()
    code_counts: dict[str, int] = field(default_factory=dict)
    defaults: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class OECDSeries:
    """A single series key and its resolved dimensions."""

    key: str
    raw_key: str = ""
    dimensions: dict[str, str] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class OECDObservation:
    """A single observation from the OECD SDMX API."""

    series_id: str
    date: str
    value: float
    dataflow: str = ""
    dataset: str = ""
    agency_id: str = ""
    series_key: str = ""
    raw_series_key: str = ""
    dimensions: dict[str, str] = field(default_factory=dict)


class OECDAPIError(RuntimeError):
    """Base error for OECD API failures."""


class OECDRateLimitError(OECDAPIError):
    """Raised when OECD throttles a request."""


class OECDResponseFormatError(OECDAPIError):
    """Raised when OECD returns an unexpected response format."""


class OECDClient:
    """Client for the OECD Data Explorer SDMX REST API."""

    BASE_URL = "https://sdmx.oecd.org/public/rest"
    DEFAULT_AGENCY_ID = "OECD.SDD.STES"

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout: int = 30,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "AnalystEngine/1.0",
        })
        self.timeout = timeout
        self._dataflow_cache: dict[tuple[str, str, str], OECDDataflow] = {}
        self._dataflow_list_cache: dict[tuple[str, str], tuple[OECDDataflow, ...]] = {}
        self._structure_cache: dict[tuple[str, str, str], OECDDataStructure] = {}

    def build_data_url(
        self,
        dataflow_id: str,
        *,
        agency_id: str = DEFAULT_AGENCY_ID,
        version: str | None = None,
        key: str = "all",
    ) -> str:
        resource = f"{agency_id},{dataflow_id}"
        if version:
            resource = f"{resource},{version}"
        return f"{self.BASE_URL}/data/{resource}/{key}"

    def build_v2_dataflow_url(
        self,
        dataflow_id: str,
        *,
        agency_id: str = DEFAULT_AGENCY_ID,
        version: str,
        key: str,
    ) -> str:
        return f"{self.BASE_URL}/v2/data/dataflow/{agency_id}/{dataflow_id}/{version}/{key}"

    def build_dataflow_url(
        self,
        *,
        agency_id: str = "all",
        dataflow_id: str = "all",
        version: str = "latest",
    ) -> str:
        return f"{self.BASE_URL}/dataflow/{agency_id}/{dataflow_id}/{version}"

    def build_datastructure_url(
        self,
        structure_id: str,
        *,
        agency_id: str,
        version: str,
    ) -> str:
        return f"{self.BASE_URL}/datastructure/{agency_id}/{structure_id}/{version}"

    def list_dataflows(
        self,
        *,
        agency_id: str = "all",
        version: str = "latest",
    ) -> list[OECDDataflow]:
        cache_key = (agency_id, version)
        cached = self._dataflow_list_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        root = self._get_xml_root(self.build_dataflow_url(agency_id=agency_id, version=version))
        dataflows = tuple(self._parse_dataflows_xml(root))
        self._dataflow_list_cache[cache_key] = dataflows
        for dataflow in dataflows:
            self._dataflow_cache[(dataflow.agency_id, dataflow.id, version)] = dataflow
            self._dataflow_cache[(dataflow.agency_id, dataflow.id, dataflow.version)] = dataflow
        return list(dataflows)

    def search_dataflows(
        self,
        query: str,
        *,
        agency_id: str = "all",
        limit: int = 20,
    ) -> list[OECDDataflow]:
        needle = query.lower().strip()
        if not needle:
            return []
        matches = [
            dataflow for dataflow in self.list_dataflows(agency_id=agency_id)
            if needle in dataflow.id.lower()
            or needle in dataflow.name.lower()
            or needle in dataflow.description.lower()
        ]
        return matches[:limit]

    def search_dataset(
        self,
        query: str,
        *,
        agency_id: str = "all",
        limit: int = 20,
    ) -> list[OECDDataflow]:
        return self.search_dataflows(query, agency_id=agency_id, limit=limit)

    def get_dataflow(
        self,
        dataflow_id: str,
        *,
        agency_id: str = DEFAULT_AGENCY_ID,
        version: str = "latest",
    ) -> OECDDataflow:
        cached = self._dataflow_cache.get((agency_id, dataflow_id, version))
        if cached is not None:
            return cached

        root = self._get_xml_root(
            self.build_dataflow_url(agency_id=agency_id, dataflow_id=dataflow_id, version=version)
        )
        dataflows = self._parse_dataflows_xml(root)
        if not dataflows:
            raise OECDAPIError(f"OECD dataflow not found: {agency_id}/{dataflow_id}/{version}")
        dataflow = dataflows[0]
        self._dataflow_cache[(dataflow.agency_id, dataflow.id, version)] = dataflow
        self._dataflow_cache[(dataflow.agency_id, dataflow.id, dataflow.version)] = dataflow
        return dataflow

    def get_structure(
        self,
        dataflow_id: str,
        *,
        agency_id: str = DEFAULT_AGENCY_ID,
        version: str = "latest",
    ) -> OECDDataStructure:
        dataflow = self.get_dataflow(dataflow_id, agency_id=agency_id, version=version)
        cache_key = (dataflow.structure_agency_id, dataflow.structure_id, dataflow.structure_version)
        cached = self._structure_cache.get(cache_key)
        if cached is not None:
            return cached

        root = self._get_xml_root(
            self.build_datastructure_url(
                dataflow.structure_id,
                agency_id=dataflow.structure_agency_id,
                version=dataflow.structure_version,
            ),
            params={"references": "all"},
        )
        structure = self._parse_datastructure_xml(root, dataflow=dataflow)
        self._structure_cache[cache_key] = structure
        return structure

    def summarize_structure(
        self,
        dataflow_id: str,
        *,
        agency_id: str = DEFAULT_AGENCY_ID,
        version: str = "latest",
    ) -> OECDStructureSummary:
        dataflow = self.get_dataflow(dataflow_id, agency_id=agency_id, version=version)
        structure = self.get_structure(dataflow_id, agency_id=dataflow.agency_id, version=dataflow.version)
        time_dimension_id = next((dimension.id for dimension in structure.dimensions if dimension.is_time), "")
        series_dimensions = tuple(
            dimension.id for dimension in sorted(structure.dimensions, key=lambda item: item.position)
            if not dimension.is_time
        )
        return OECDStructureSummary(
            dataflow_id=dataflow.id,
            agency_id=dataflow.agency_id,
            version=dataflow.version,
            name=dataflow.name or structure.name,
            description=dataflow.description,
            structure_id=structure.id,
            time_dimension_id=time_dimension_id,
            series_dimensions=series_dimensions,
            code_counts={
                dimension.id: len(dimension.codes)
                for dimension in structure.dimensions
                if not dimension.is_time
            },
            defaults=dict(structure.defaults),
        )

    def build_key(
        self,
        dataflow_id: str,
        filters: Mapping[str, str | Sequence[str]],
        *,
        agency_id: str = DEFAULT_AGENCY_ID,
        version: str = "latest",
        use_defaults: bool = False,
    ) -> str:
        structure = self.get_structure(dataflow_id, agency_id=agency_id, version=version)
        known_dimensions = {dimension.id for dimension in structure.dimensions if not dimension.is_time}
        unknown = sorted(set(filters) - known_dimensions)
        if unknown:
            raise ValueError(f"Unknown OECD dimensions for {dataflow_id}: {', '.join(unknown)}")

        key_parts: list[str] = []
        for dimension in sorted(structure.dimensions, key=lambda item: item.position):
            if dimension.is_time:
                continue
            selected = filters.get(dimension.id)
            if selected is None and use_defaults:
                selected = structure.defaults.get(dimension.id)
            key_parts.append(self._serialize_filter_value(selected))
        return ".".join(key_parts) if key_parts else "all"

    def enumerate_series(
        self,
        dataflow_id: str,
        *,
        agency_id: str = DEFAULT_AGENCY_ID,
        version: str | None = None,
        key: str | None = None,
        filters: Mapping[str, str | Sequence[str]] | None = None,
        use_defaults: bool = False,
        start_period: str | None = None,
        end_period: str | None = None,
        observation_limit: int | None = 1,
        max_series: int | None = None,
    ) -> list[OECDSeries]:
        resolved_key = key or self._resolve_key(
            dataflow_id,
            agency_id=agency_id,
            version=version or "latest",
            key=None,
            filters=filters,
            use_defaults=use_defaults,
        )
        payload = self._get_data_json(
            dataflow_id,
            agency_id=agency_id,
            version=version,
            key=resolved_key,
            start_period=start_period,
            end_period=end_period,
            limit=observation_limit,
        )
        return self._parse_series_catalog(payload, max_series=max_series)

    def series_to_filters(
        self,
        dataflow_id: str,
        series: OECDSeries | Mapping[str, str],
        *,
        agency_id: str = DEFAULT_AGENCY_ID,
        version: str = "latest",
    ) -> dict[str, str]:
        structure = self.get_structure(dataflow_id, agency_id=agency_id, version=version)
        values = dict(series.dimensions if isinstance(series, OECDSeries) else series)
        return {
            dimension.id: values[dimension.id]
            for dimension in sorted(structure.dimensions, key=lambda item: item.position)
            if not dimension.is_time and values.get(dimension.id) not in (None, "", "*")
        }

    def fetch_data(
        self,
        dataflow_id: str,
        *,
        agency_id: str = DEFAULT_AGENCY_ID,
        version: str | None = None,
        key: str | None = None,
        filters: Mapping[str, str | Sequence[str]] | None = None,
        use_defaults: bool = False,
        series_id: str | None = None,
        start_period: str | None = None,
        end_period: str | None = None,
        limit: int | None = 100,
        dimension_at_observation: str | None = None,
    ) -> list[OECDObservation]:
        resolved_key = self._resolve_key(
            dataflow_id,
            agency_id=agency_id,
            version=version or "latest",
            key=key,
            filters=filters,
            use_defaults=use_defaults,
        )
        payload = self._get_data_json(
            dataflow_id,
            agency_id=agency_id,
            version=version,
            key=resolved_key,
            start_period=start_period,
            end_period=end_period,
            limit=limit,
            dimension_at_observation=dimension_at_observation,
        )
        return self._parse_json(
            payload,
            series_id=series_id,
            dataflow=dataflow_id,
            dataset=dataflow_id,
            agency_id=agency_id,
            limit=limit,
        )

    def fetch_series(
        self,
        dataflow_id: str,
        key: str,
        *,
        agency_id: str = DEFAULT_AGENCY_ID,
        version: str | None = None,
        series_id: str | None = None,
        start_period: str | None = None,
        end_period: str | None = None,
        limit: int | None = 100,
    ) -> list[OECDObservation]:
        return self.fetch_data(
            dataflow_id,
            agency_id=agency_id,
            version=version,
            key=key,
            series_id=series_id,
            start_period=start_period,
            end_period=end_period,
            limit=limit,
        )

    def fetch_dataset_bulk(
        self,
        dataflow_id: str,
        *,
        agency_id: str = DEFAULT_AGENCY_ID,
        version: str | None = None,
        filters: Mapping[str, str | Sequence[str]] | None = None,
        use_defaults: bool = False,
        series_id: str | None = None,
        start_period: str | None = None,
        end_period: str | None = None,
        limit: int | None = None,
    ) -> list[OECDObservation]:
        return self.fetch_data(
            dataflow_id,
            agency_id=agency_id,
            version=version,
            filters=filters,
            use_defaults=use_defaults,
            series_id=series_id,
            start_period=start_period,
            end_period=end_period,
            limit=limit,
        )

    def get_data(
        self,
        dataflow_id: str,
        version: str,
        key: str,
        *,
        series_id: str,
        start_period: str | None = None,
        end_period: str | None = None,
        limit: int | None = 100,
        agency_id: str = DEFAULT_AGENCY_ID,
    ) -> list[OECDObservation]:
        """Fetch observations from an OECD SDMX dataflow as JSON."""

        return self.fetch_data(
            dataflow_id,
            agency_id=agency_id,
            version=version,
            key=key,
            series_id=series_id,
            start_period=start_period,
            end_period=end_period,
            limit=limit,
        )

    def _resolve_key(
        self,
        dataflow_id: str,
        *,
        agency_id: str,
        version: str,
        key: str | None,
        filters: Mapping[str, str | Sequence[str]] | None,
        use_defaults: bool,
    ) -> str:
        if key:
            return key
        if filters or use_defaults:
            return self.build_key(
                dataflow_id,
                filters or {},
                agency_id=agency_id,
                version=version,
                use_defaults=use_defaults,
            )
        return "all"

    def _get(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> requests.Response:
        response = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            suffix = f" retry_after={retry_after}" if retry_after else ""
            raise OECDRateLimitError(f"OECD rate limit exceeded for {url}.{suffix}")
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = response.text.strip()
            if detail:
                raise OECDAPIError(
                    f"OECD request failed for {url}: HTTP {response.status_code}: {detail[:200]}"
                ) from exc
            raise OECDAPIError(
                f"OECD request failed for {url}: HTTP {response.status_code}"
            ) from exc
        return response

    def _get_xml_root(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
    ) -> ET.Element:
        response = self._get(url, params=params)
        if response.text.lstrip().startswith("{"):
            response = self._get(
                url,
                params=params,
                headers={"Accept": "application/xml, text/xml;q=0.9, */*;q=0.1"},
            )
        text = response.text.strip()
        if not text.startswith("<"):
            raise OECDResponseFormatError(
                f"Expected XML from OECD structure endpoint, got: {text[:200]}"
            )
        try:
            return ET.fromstring(text)
        except ET.ParseError as exc:
            raise OECDResponseFormatError(f"Invalid OECD XML response from {url}") from exc

    def _get_data_json(
        self,
        dataflow_id: str,
        *,
        agency_id: str,
        version: str | None,
        key: str,
        start_period: str | None = None,
        end_period: str | None = None,
        limit: int | None = None,
        dimension_at_observation: str | None = None,
    ) -> dict:
        params: dict[str, str] = {"format": "jsondata"}
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if limit:
            params["lastNObservations"] = str(limit)
        if dimension_at_observation:
            params["dimensionAtObservation"] = dimension_at_observation

        try:
            response = self._get(
                self.build_data_url(dataflow_id, agency_id=agency_id, version=version, key=key),
                params=params,
            )
        except OECDAPIError as exc:
            if version and "*" in key and "HTTP 404" in str(exc):
                response = self._get(
                    self.build_v2_dataflow_url(
                        dataflow_id,
                        agency_id=agency_id,
                        version=version,
                        key=key,
                    ),
                    params=params,
                )
            else:
                raise
        text = response.text.strip()
        if not text or text == "NoResultsFound":
            return _EMPTY_DATA_PAYLOAD
        content_type = response.headers.get("Content-Type", "")
        if "json" not in content_type.lower() and not text.startswith("{"):
            raise OECDResponseFormatError(
                f"Expected JSON from OECD data endpoint, got: {text[:200]}"
            )
        try:
            return response.json()
        except ValueError as exc:
            raise OECDResponseFormatError(
                f"Invalid OECD JSON response for {dataflow_id}: {text[:200]}"
            ) from exc

    @staticmethod
    def _parse_dataflows_xml(root: ET.Element) -> list[OECDDataflow]:
        dataflows: list[OECDDataflow] = []
        for node in root.findall(".//structure:Dataflow", _XML_NS):
            structure_ref = node.find("./structure:Structure/Ref", _XML_NS)
            defaults: dict[str, str] = {}
            for annotation in node.findall("./common:Annotations/common:Annotation", _XML_NS):
                annotation_type = annotation.findtext("common:AnnotationType", "", _XML_NS)
                annotation_title = annotation.findtext("common:AnnotationTitle", "", _XML_NS).strip()
                if annotation_type == "DEFAULT" and annotation_title:
                    defaults.update(_parse_assignments(annotation_title))

            dataflows.append(OECDDataflow(
                id=node.attrib.get("id", ""),
                agency_id=node.attrib.get("agencyID", ""),
                version=node.attrib.get("version", ""),
                name=_pick_localized_xml_text(node, "./common:Name"),
                description=_pick_localized_xml_text(node, "./common:Description"),
                structure_id=structure_ref.attrib.get("id", "") if structure_ref is not None else "",
                structure_agency_id=structure_ref.attrib.get("agencyID", "") if structure_ref is not None else "",
                structure_version=structure_ref.attrib.get("version", "") if structure_ref is not None else "",
                defaults=defaults,
            ))
        return dataflows

    @staticmethod
    def _parse_datastructure_xml(
        root: ET.Element,
        *,
        dataflow: OECDDataflow,
    ) -> OECDDataStructure:
        concept_names: dict[str, str] = {}
        for concept in root.findall(".//structure:Concept", _XML_NS):
            concept_id = concept.attrib.get("id", "")
            if concept_id:
                concept_names[concept_id] = _pick_localized_xml_text(concept, "./common:Name") or concept_id

        codelist_map: dict[tuple[str, str, str], tuple[OECDCode, ...]] = {}
        for codelist in root.findall(".//structure:Codelist", _XML_NS):
            key = (
                codelist.attrib.get("agencyID", ""),
                codelist.attrib.get("id", ""),
                codelist.attrib.get("version", ""),
            )
            codes: list[OECDCode] = []
            for code in codelist.findall("./structure:Code", _XML_NS):
                parent = code.find("./structure:Parent/Ref", _XML_NS)
                codes.append(OECDCode(
                    id=code.attrib.get("id", ""),
                    name=_pick_localized_xml_text(code, "./common:Name"),
                    description=_pick_localized_xml_text(code, "./common:Description"),
                    parent_id=parent.attrib.get("id", "") if parent is not None else "",
                ))
            codelist_map[key] = tuple(codes)

        structure_node = root.find(".//structure:DataStructure", _XML_NS)
        if structure_node is None:
            raise OECDResponseFormatError("OECD datastructure XML did not include a DataStructure node")

        dimensions: list[OECDDimension] = []
        for tag_name, is_time in (("Dimension", False), ("TimeDimension", True)):
            for dimension in structure_node.findall(
                f"./structure:DataStructureComponents/structure:DimensionList/structure:{tag_name}",
                _XML_NS,
            ):
                enum_ref = dimension.find(
                    "./structure:LocalRepresentation/structure:Enumeration/Ref",
                    _XML_NS,
                )
                codelist_key = (
                    enum_ref.attrib.get("agencyID", "") if enum_ref is not None else "",
                    enum_ref.attrib.get("id", "") if enum_ref is not None else "",
                    enum_ref.attrib.get("version", "") if enum_ref is not None else "",
                )
                dimension_id = dimension.attrib.get("id", "")
                dimensions.append(OECDDimension(
                    id=dimension_id,
                    position=int(dimension.attrib.get("position", "0")),
                    name=concept_names.get(dimension_id, dimension_id),
                    codelist_id=codelist_key[1],
                    codelist_agency_id=codelist_key[0],
                    codelist_version=codelist_key[2],
                    is_time=is_time,
                    codes=codelist_map.get(codelist_key, ()),
                ))

        dimensions.sort(key=lambda item: item.position)
        return OECDDataStructure(
            id=structure_node.attrib.get("id", ""),
            agency_id=structure_node.attrib.get("agencyID", ""),
            version=structure_node.attrib.get("version", ""),
            name=_pick_localized_xml_text(structure_node, "./common:Name"),
            dimensions=tuple(dimensions),
            dataflow_id=dataflow.id,
            dataflow_agency_id=dataflow.agency_id,
            dataflow_version=dataflow.version,
            defaults=dict(dataflow.defaults),
        )

    @staticmethod
    def _serialize_filter_value(value: str | Sequence[str] | None) -> str:
        if value is None:
            return "*"
        if isinstance(value, str):
            return value or "*"
        values = [item for item in value if item]
        return "+".join(values) if values else "*"

    @staticmethod
    def _parse_value_label(value_def: dict) -> str:
        names = value_def.get("names")
        if isinstance(names, dict):
            for lang in ("en", "en-US"):
                label = names.get(lang)
                if label:
                    return str(label)
            if names:
                first = next(iter(names.values()))
                if first:
                    return str(first)
        for key in ("name", "value", "id"):
            label = value_def.get(key)
            if label:
                return str(label)
        return ""

    @classmethod
    def _decode_dimension_key(
        cls,
        key: str,
        dimensions: Sequence[dict],
    ) -> tuple[dict[str, str], dict[str, str]]:
        if not dimensions:
            return {}, {}

        parts = [] if key == "" else key.split(":")
        resolved: dict[str, str] = {}
        labels: dict[str, str] = {}
        for idx, dimension in enumerate(dimensions):
            if idx >= len(parts):
                continue
            part = parts[idx]
            if part in {"", "~"}:
                continue
            try:
                value_idx = int(part)
                value_def = dimension.get("values", [])[value_idx]
            except (ValueError, IndexError, TypeError):
                continue
            dim_id = dimension.get("id", "")
            value_id = value_def.get("id") or value_def.get("value") or value_def.get("name")
            if not dim_id or value_id is None:
                continue
            resolved[dim_id] = str(value_id)
            label = cls._parse_value_label(value_def)
            if label:
                labels[dim_id] = label
        return resolved, labels

    @staticmethod
    def _encode_dimension_key(
        values: Mapping[str, str],
        dimensions: Sequence[dict] | Sequence[OECDDimension],
    ) -> str:
        if not dimensions:
            return ""

        parts: list[str] = []
        for dimension in dimensions:
            dim_id = dimension.get("id", "") if isinstance(dimension, dict) else dimension.id
            if not dim_id:
                continue
            parts.append(values.get(dim_id) or "*")
        return ".".join(parts)

    @staticmethod
    def _make_series_id(
        *,
        dataflow: str,
        agency_id: str,
        series_key: str,
    ) -> str:
        prefix = f"{agency_id}:{dataflow}" if agency_id else dataflow
        return f"{prefix}:{series_key}" if series_key else prefix

    @classmethod
    def _parse_series_catalog(
        cls,
        data: dict,
        *,
        max_series: int | None = None,
    ) -> list[OECDSeries]:
        try:
            inner = data.get("data", data)
            datasets = inner["dataSets"]
            structure = inner["structures"][0]
        except (KeyError, IndexError, TypeError):
            return []

        series_dimensions = structure.get("dimensions", {}).get("series", [])
        series_list: list[OECDSeries] = []
        for dataset in datasets:
            for raw_series_key in dataset.get("series", {}):
                dimensions, labels = cls._decode_dimension_key(raw_series_key, series_dimensions)
                series_key = cls._encode_dimension_key(dimensions, series_dimensions) or raw_series_key
                series_list.append(OECDSeries(
                    key=series_key,
                    raw_key=raw_series_key,
                    dimensions=dimensions,
                    labels=labels,
                ))
                if max_series is not None and len(series_list) >= max_series:
                    return series_list
        return series_list

    @classmethod
    def _parse_json(
        cls,
        data: dict,
        *,
        series_id: str | None,
        dataflow: str,
        dataset: str | None = None,
        agency_id: str = "",
        limit: int | None,
    ) -> list[OECDObservation]:
        """Parse SDMX-JSON response into observations."""

        observations: list[OECDObservation] = []

        try:
            inner = data.get("data", data)
            datasets = inner["dataSets"]
            structure = inner["structures"][0]
        except (KeyError, IndexError, TypeError):
            return observations

        series_dims = structure.get("dimensions", {}).get("series", [])
        observation_dims = structure.get("dimensions", {}).get("observation", [])

        def append_observation(
            resolved_series_id: str,
            *,
            series_key: str,
            raw_series_key: str = "",
            dimensions: dict[str, str],
            raw_value: object,
        ) -> None:
            period = dimensions.get("TIME_PERIOD")
            if period is None or raw_value is None:
                return
            try:
                observations.append(OECDObservation(
                    series_id=resolved_series_id,
                    date=_normalize_date(period),
                    value=float(raw_value),
                    dataflow=dataflow,
                    dataset=dataset or dataflow,
                    agency_id=agency_id,
                    series_key=series_key,
                    raw_series_key=raw_series_key,
                    dimensions=dict(dimensions),
                ))
            except (ValueError, TypeError):
                return

        for dataset_node in datasets:
            all_series = dataset_node.get("series", {})
            if all_series:
                for raw_series_key, series_data in all_series.items():
                    series_values, _ = cls._decode_dimension_key(raw_series_key, series_dims)
                    coded_series_key = cls._encode_dimension_key(series_values, series_dims) or raw_series_key
                    resolved_series_id = series_id or cls._make_series_id(
                        dataflow=dataflow,
                        agency_id=agency_id,
                        series_key=coded_series_key or "all",
                    )
                    for obs_key, obs_array in series_data.get("observations", {}).items():
                        observation_values, _ = cls._decode_dimension_key(obs_key, observation_dims)
                        merged = {**series_values, **observation_values}
                        raw_value = obs_array[0] if obs_array else None
                        append_observation(
                            resolved_series_id,
                            series_key=coded_series_key,
                            raw_series_key=raw_series_key,
                            dimensions=merged,
                            raw_value=raw_value,
                        )
            else:
                resolved_series_id = series_id or cls._make_series_id(
                    dataflow=dataflow,
                    agency_id=agency_id,
                    series_key="all",
                )
                for obs_key, obs_array in dataset_node.get("observations", {}).items():
                    observation_values, _ = cls._decode_dimension_key(obs_key, observation_dims)
                    raw_value = obs_array[0] if obs_array else None
                    append_observation(
                        resolved_series_id,
                        series_key="",
                        dimensions=observation_values,
                        raw_value=raw_value,
                    )

        observations.sort(key=lambda item: (item.series_id, item.date), reverse=True)
        if limit is None:
            return observations
        if series_id is None and len({item.series_id for item in observations}) > 1:
            return observations
        return observations[:limit]
