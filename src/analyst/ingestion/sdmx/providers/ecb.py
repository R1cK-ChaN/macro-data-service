"""ECB SDMX provider — minimal config-only subclass."""

from __future__ import annotations

from .._base_client import SDMXClient
from .._config import ECB_CONFIG
from .._errors import ECBAPIError, ECBRateLimitError


class ECBClient(SDMXClient):
    """Client for the ECB Data Portal SDMX API (no API key required)."""

    def __init__(self, *, timeout: int = 30) -> None:
        super().__init__(
            ECB_CONFIG, timeout=timeout,
            api_error_cls=ECBAPIError,
            rate_limit_error_cls=ECBRateLimitError,
        )

    def _build_data_url(self, dataflow_id, key, **kwargs):
        return f"{self.config.base_url}/data/{dataflow_id}/{key}"

    def _build_estimate_url(self, dataflow_id, **kwargs):
        return f"{self.config.base_url}/data/{dataflow_id}/."
