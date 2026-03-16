"""ILO (ILOSTAT) SDMX provider — minimal config-only subclass."""

from __future__ import annotations

from .._base_client import SDMXClient
from .._config import ILO_CONFIG
from .._errors import ILOAPIError, ILORateLimitError


class ILOClient(SDMXClient):
    """Client for the ILOSTAT SDMX REST API (no API key required)."""

    def __init__(self, *, timeout: int = 30) -> None:
        super().__init__(
            ILO_CONFIG, timeout=timeout,
            api_error_cls=ILOAPIError,
            rate_limit_error_cls=ILORateLimitError,
        )

    def _build_data_url(self, dataflow_id, key, **kwargs):
        return f"{self.config.base_url}/data/ILO,{dataflow_id},latest/{key}"

    def _build_estimate_url(self, dataflow_id, **kwargs):
        return f"{self.config.base_url}/data/ILO,{dataflow_id},latest/."
