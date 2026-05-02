"""SDMX error hierarchy with per-provider subclasses.

Using actual subclasses (not aliases) so ``except BISAPIError`` works correctly.
``except SDMXAPIError`` catches any provider's errors.
"""


class SDMXAPIError(RuntimeError):
    """Base error for all SDMX API failures."""


class SDMXRateLimitError(SDMXAPIError):
    """Raised when any SDMX provider throttles a request (HTTP 429)."""


# ── Per-provider error classes ────────────────────────────────────────

class BISAPIError(SDMXAPIError): pass          # noqa: E701
class BISRateLimitError(BISAPIError, SDMXRateLimitError): pass  # noqa: E701

class IMFAPIError(SDMXAPIError): pass          # noqa: E701
class IMFRateLimitError(IMFAPIError, SDMXRateLimitError): pass  # noqa: E701

class EurostatAPIError(SDMXAPIError): pass     # noqa: E701
class EurostatRateLimitError(EurostatAPIError, SDMXRateLimitError): pass  # noqa: E701

class ECBAPIError(SDMXAPIError): pass          # noqa: E701
class ECBRateLimitError(ECBAPIError, SDMXRateLimitError): pass  # noqa: E701

class BundesbankAPIError(SDMXAPIError): pass   # noqa: E701
class BundesbankRateLimitError(BundesbankAPIError, SDMXRateLimitError): pass  # noqa: E701

class UNSDAPIError(SDMXAPIError): pass         # noqa: E701
class UNSDRateLimitError(UNSDAPIError, SDMXRateLimitError): pass  # noqa: E701

class ILOAPIError(SDMXAPIError): pass          # noqa: E701
class ILORateLimitError(ILOAPIError, SDMXRateLimitError): pass  # noqa: E701

class OECDAPIError(SDMXAPIError): pass         # noqa: E701
class OECDRateLimitError(OECDAPIError, SDMXRateLimitError): pass  # noqa: E701
class OECDResponseFormatError(OECDAPIError): pass  # noqa: E701
