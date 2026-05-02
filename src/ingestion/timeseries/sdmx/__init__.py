"""Unified SDMX ingestion engine.

Provides a base ``SDMXClient`` class with full catalog discovery,
shared types, and provider-specific subclasses in ``sdmx.providers``.
"""

from ._base_client import SDMXClient
from ._base_ingestion_client import SDMXIngestionClient
from ._config import (
    BIS_CONFIG,
    BUNDESBANK_CONFIG,
    ECB_CONFIG,
    EUROSTAT_CONFIG,
    ILO_CONFIG,
    IMF_CONFIG,
    OECD_CONFIG,
    SDMXProviderConfig,
    UNSD_CONFIG,
)
from ._errors import (
    BISAPIError,
    BISRateLimitError,
    BundesbankAPIError,
    BundesbankRateLimitError,
    ECBAPIError,
    ECBRateLimitError,
    EurostatAPIError,
    EurostatRateLimitError,
    ILOAPIError,
    ILORateLimitError,
    IMFAPIError,
    IMFRateLimitError,
    OECDAPIError,
    OECDRateLimitError,
    SDMXAPIError,
    SDMXRateLimitError,
    UNSDAPIError,
    UNSDRateLimitError,
)
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

__all__ = [
    # Base classes
    "SDMXClient",
    "SDMXIngestionClient",
    "SDMXProviderConfig",
    # Types
    "SDMXObservation",
    "SDMXDataflow",
    "SDMXDimension",
    "SDMXDataStructure",
    "SDMXStructureSummary",
    "SDMXSizeEstimate",
    # Errors
    "SDMXAPIError",
    "SDMXRateLimitError",
    "BISAPIError",
    "BISRateLimitError",
    "BundesbankAPIError",
    "BundesbankRateLimitError",
    "ECBAPIError",
    "ECBRateLimitError",
    "EurostatAPIError",
    "EurostatRateLimitError",
    "ILOAPIError",
    "ILORateLimitError",
    "IMFAPIError",
    "IMFRateLimitError",
    "OECDAPIError",
    "OECDRateLimitError",
    "UNSDAPIError",
    "UNSDRateLimitError",
    # Configs
    "BIS_CONFIG",
    "BUNDESBANK_CONFIG",
    "ECB_CONFIG",
    "EUROSTAT_CONFIG",
    "ILO_CONFIG",
    "IMF_CONFIG",
    "OECD_CONFIG",
    "UNSD_CONFIG",
    # Parsing
    "normalize_date",
    "build_decade_chunks",
    "extract_id_from_urn",
    "parse_sdmx_json_observations",
]
