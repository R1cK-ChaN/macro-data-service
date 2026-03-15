"""Eurostat JSON-stat + SDMX 2.1 API client — backward-compatible re-exports.

The implementation lives in ``analyst.ingestion.sdmx.providers.eurostat``.
This module preserves the original public API.
"""

from analyst.ingestion.sdmx._errors import EurostatAPIError, EurostatRateLimitError
from analyst.ingestion.sdmx._parsing import (
    build_decade_chunks as _build_decade_chunks,
    extract_id_from_urn as _extract_id_from_urn,
)
from analyst.ingestion.sdmx._types import (
    SDMXDataStructure as EurostatDataStructure,
    SDMXDataflow as EurostatDataflow,
    SDMXDimension as EurostatDimension,
    SDMXObservation as EurostatObservation,
    SDMXSizeEstimate as EurostatSizeEstimate,
    SDMXStructureSummary as EurostatStructureSummary,
)
from analyst.ingestion.sdmx.providers.eurostat import (
    EurostatClient,
    _build_geo_chunks,
    _filter_nuts_codes,
    _inject_geo_into_key,
)


def _normalize_period(raw: str) -> str:
    """Backward-compat alias — delegates to EurostatClient._normalize_date."""
    return EurostatClient()._normalize_date(raw)


__all__ = [
    "EurostatClient",
    "EurostatObservation",
    "EurostatDataflow",
    "EurostatDimension",
    "EurostatDataStructure",
    "EurostatStructureSummary",
    "EurostatSizeEstimate",
    "EurostatAPIError",
    "EurostatRateLimitError",
    "_normalize_period",
    "_build_decade_chunks",
    "_extract_id_from_urn",
    "_filter_nuts_codes",
    "_build_geo_chunks",
    "_inject_geo_into_key",
]
