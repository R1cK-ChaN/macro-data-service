"""ILO ILOSTAT SDMX API client — backward-compatible re-exports.

The implementation lives in ``analyst.ingestion.sdmx.providers.ilo``.
This module preserves the original public API.
"""

from analyst.ingestion.sdmx._errors import ILOAPIError, ILORateLimitError
from analyst.ingestion.sdmx._parsing import (
    build_decade_chunks as _build_decade_chunks,
    extract_id_from_urn as _extract_id_from_urn,
    normalize_date as _normalize_date,
)
from analyst.ingestion.sdmx._types import (
    SDMXDataStructure as ILODataStructure,
    SDMXDataflow as ILODataflow,
    SDMXDimension as ILODimension,
    SDMXObservation as ILOObservation,
    SDMXSizeEstimate as ILOSizeEstimate,
    SDMXStructureSummary as ILOStructureSummary,
)
from analyst.ingestion.sdmx.providers.ilo import ILOClient

__all__ = [
    "ILOClient",
    "ILOObservation",
    "ILODataflow",
    "ILODimension",
    "ILODataStructure",
    "ILOStructureSummary",
    "ILOSizeEstimate",
    "ILOAPIError",
    "ILORateLimitError",
    "_normalize_date",
    "_build_decade_chunks",
    "_extract_id_from_urn",
]
