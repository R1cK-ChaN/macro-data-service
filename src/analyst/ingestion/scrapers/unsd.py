"""UNSD (UNData) SDMX REST API client — backward-compatible re-exports.

The implementation lives in ``analyst.ingestion.sdmx.providers.unsd``.
This module preserves the original public API.
"""

from analyst.ingestion.sdmx._errors import UNSDAPIError, UNSDRateLimitError
from analyst.ingestion.sdmx._parsing import (
    build_decade_chunks as _build_decade_chunks,
    extract_id_from_urn as _extract_id_from_urn,
    normalize_date as _normalize_date,
)
from analyst.ingestion.sdmx._types import (
    SDMXDataStructure as UNSDDataStructure,
    SDMXDataflow as UNSDDataflow,
    SDMXDimension as UNSDDimension,
    SDMXObservation as UNSDObservation,
    SDMXSizeEstimate as UNSDSizeEstimate,
    SDMXStructureSummary as UNSDStructureSummary,
)
from analyst.ingestion.sdmx.providers.unsd import UNSDClient

__all__ = [
    "UNSDClient",
    "UNSDObservation",
    "UNSDDataflow",
    "UNSDDimension",
    "UNSDDataStructure",
    "UNSDStructureSummary",
    "UNSDSizeEstimate",
    "UNSDAPIError",
    "UNSDRateLimitError",
    "_normalize_date",
    "_build_decade_chunks",
    "_extract_id_from_urn",
]
