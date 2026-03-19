"""UNSD (UNData) SDMX provider — multi-agency resolution."""

from __future__ import annotations

from typing import Any

from .._base_client import SDMXClient
from .._config import UNSD_CONFIG
from .._errors import UNSDAPIError, UNSDRateLimitError
from .._types import SDMXDataStructure, SDMXObservation, SDMXSizeEstimate


class UNSDClient(SDMXClient):
    """Client for the UNSD SDMX REST API (no API key required).

    UNSD hosts dataflows from multiple UN agencies, so ``agency_id``
    is auto-resolved from the catalog when not provided.
    """

    def __init__(self, *, timeout: int = 30) -> None:
        super().__init__(
            UNSD_CONFIG, timeout=timeout,
            api_error_cls=UNSDAPIError,
            rate_limit_error_cls=UNSDRateLimitError,
        )

    def _resolve_agency(self, dataflow_id: str, agency_id: str = "") -> str:
        if agency_id:
            return agency_id
        flows = self.list_dataflows()
        match = next((f for f in flows if f.id == dataflow_id), None)
        return match.agency_id if match else "UNSD"

    def _build_dataflow_list_url(self):
        return f"{self.config.base_url}/dataflow/all/all/latest"

    def _build_data_url(self, dataflow_id, key, **kwargs):
        agency_id = self._resolve_agency(dataflow_id, kwargs.get("agency_id", ""))
        return f"{self.config.base_url}/data/{agency_id},{dataflow_id},latest/{key}"

    def _build_structure_url(self, dataflow_id, version):
        flows = self.list_dataflows()
        match = next((f for f in flows if f.id == dataflow_id), None)
        agency = match.agency_id if match else "UNSD"
        return f"{self.config.base_url}/dataflow/{agency}/{dataflow_id}/{version}"

    def _build_estimate_url(self, dataflow_id, **kwargs):
        agency_id = self._resolve_agency(dataflow_id, kwargs.get("agency_id", ""))
        return f"{self.config.base_url}/data/{agency_id},{dataflow_id},latest/all"

    def get_data(self, dataflow_id, key="all", *, series_id="", agency_id="", **kwargs):
        """Override to pass agency_id through kwargs."""
        return super().get_data(
            dataflow_id, key, series_id=series_id, agency_id=agency_id, **kwargs,
        )

    def estimate_size(self, dataflow_id, version="1.0", agency_id="", **kwargs):
        return super().estimate_size(dataflow_id, version, agency_id=agency_id, **kwargs)

    def get_datastructure(self, dataflow_id, version=None, agency_id=None, **kwargs):
        return super().get_datastructure(dataflow_id, version, **kwargs)
