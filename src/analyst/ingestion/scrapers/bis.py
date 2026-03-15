"""BIS SDMX REST API client — backward-compatible re-exports.

The implementation lives in ``analyst.ingestion.sdmx.providers.bis``.
This module preserves the original public API.
"""

from analyst.ingestion.sdmx._errors import BISAPIError, BISRateLimitError
from analyst.ingestion.sdmx._parsing import (
    build_decade_chunks as _build_decade_chunks,
    extract_id_from_urn as _extract_id_from_urn,
    normalize_date as _normalize_date,
)
from analyst.ingestion.sdmx._types import (
    SDMXDataStructure as BISDataStructure,
    SDMXDataflow as BISDataflow,
    SDMXDimension as BISDimension,
    SDMXObservation as BISObservation,
    SDMXSizeEstimate as BISSizeEstimate,
    SDMXStructureSummary as BISStructureSummary,
)
from analyst.ingestion.sdmx.providers.bis import BISClient

__all__ = [
    "BISClient",
    "BISObservation",
    "BISDataflow",
    "BISDimension",
    "BISDataStructure",
    "BISStructureSummary",
    "BISSizeEstimate",
    "BISAPIError",
    "BISRateLimitError",
    "_normalize_date",
    "_build_decade_chunks",
    "_extract_id_from_urn",
]
