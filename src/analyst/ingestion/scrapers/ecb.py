"""ECB SDMX 2.1 API client — backward-compatible re-exports.

The implementation lives in ``analyst.ingestion.sdmx.providers.ecb``.
This module preserves the original public API.
"""

from analyst.ingestion.sdmx._errors import ECBAPIError, ECBRateLimitError
from analyst.ingestion.sdmx._parsing import (
    build_decade_chunks as _build_decade_chunks,
    extract_id_from_urn as _extract_id_from_urn,
    normalize_date as _normalize_date,
)
from analyst.ingestion.sdmx._types import (
    SDMXDataStructure as ECBDataStructure,
    SDMXDataflow as ECBDataflow,
    SDMXDimension as ECBDimension,
    SDMXObservation as ECBObservation,
    SDMXSizeEstimate as ECBSizeEstimate,
    SDMXStructureSummary as ECBStructureSummary,
)
from analyst.ingestion.sdmx.providers.ecb import ECBClient

__all__ = [
    "ECBClient",
    "ECBObservation",
    "ECBDataflow",
    "ECBDimension",
    "ECBDataStructure",
    "ECBStructureSummary",
    "ECBSizeEstimate",
    "ECBAPIError",
    "ECBRateLimitError",
    "_normalize_date",
    "_build_decade_chunks",
    "_extract_id_from_urn",
]
