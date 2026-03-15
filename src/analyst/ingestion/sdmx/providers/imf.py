"""IMF SDMX 3.0 provider — API key auth + vintage queries."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Sequence

from .._base_client import SDMXClient
from .._config import IMF_CONFIG
from .._errors import IMFAPIError, IMFRateLimitError
from .._parsing import normalize_date
from .._types import SDMXObservation


@dataclass(frozen=True)
class IMFVintageObservation:
    """A single vintage (point-in-time) observation from the IMF SDMX 3.0 API."""

    series_id: str
    date: str
    vintage_date: str
    value: float
    dataflow: str = ""


class IMFClient(SDMXClient):
    """Client for the IMF SDMX 3.0 API (requires API key)."""

    def __init__(self, api_key: str | None = None, *, timeout: int = 30) -> None:
        super().__init__(
            IMF_CONFIG, timeout=timeout, api_key=api_key,
            api_error_cls=IMFAPIError,
            rate_limit_error_cls=IMFRateLimitError,
        )

    def _normalize_date(self, raw: str) -> str:
        # IMF adds 2025-M09 monthly format
        m = re.match(r"^(\d{4})-M(\d{2})$", raw)
        if m:
            return f"{m.group(1)}-{m.group(2)}-01"
        return super()._normalize_date(raw)

    def _build_dataflow_list_url(self):
        return f"{self.config.base_url}/structure/dataflow/{self.config.default_agency}"

    def _build_structure_url(self, dataflow_id, version):
        return (
            f"{self.config.base_url}/structure/dataflow/"
            f"{self.config.default_agency}/{dataflow_id}/{version}"
        )

    def _build_data_url(self, dataflow_id, key, **kwargs):
        version = kwargs.get("version")
        if not version:
            flows = self.list_dataflows()
            match = next((f for f in flows if f.id == dataflow_id), None)
            version = match.version if match else "1.0"
        return (
            f"{self.config.base_url}/data/dataflow/"
            f"{self.config.default_agency}/{dataflow_id}/{version}/{key}"
        )

    def _build_estimate_url(self, dataflow_id, **kwargs):
        return self._build_data_url(dataflow_id, "all", **kwargs)

    def get_data(self, dataflow_id, key="all", *, series_id="", version="",
                 start_period=None, end_period=None, limit=100, **kwargs):
        url = self._build_data_url(dataflow_id, key, version=version)
        params: dict[str, str] = {
            "attributes": "none",
            "measures": "all",
            "format": "jsondata",
        }
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if limit:
            params["lastNObservations"] = str(limit)

        response = self._get(url, params)
        return self._parse_data_response(
            response, series_id=series_id, dataflow=dataflow_id, limit=limit,
        )

    def get_vintages(
        self,
        dataflow_id: str,
        key: str,
        *,
        series_id: str,
        version: str = "",
        as_of_dates: Sequence[str] = (),
        limit: int = 1,
    ) -> list[IMFVintageObservation]:
        """Fetch point-in-time vintage data using the SDMX 3.0 ``asOf`` parameter."""
        if not version:
            flows = self.list_dataflows()
            match = next((f for f in flows if f.id == dataflow_id), None)
            version = match.version if match else "1.0"

        vintages: list[IMFVintageObservation] = []
        for as_of in as_of_dates:
            url = (
                f"{self.config.base_url}/data/dataflow/"
                f"{self.config.default_agency}/{dataflow_id}/{version}/{key}"
            )
            params: dict[str, str] = {
                "attributes": "none",
                "measures": "all",
                "format": "jsondata",
                "asOf": as_of,
            }
            if limit:
                params["lastNObservations"] = str(limit)

            try:
                response = self._get(url, params)
                obs = self._parse_data_response(
                    response, series_id=series_id, dataflow=dataflow_id, limit=limit,
                )
                for o in obs:
                    vintages.append(IMFVintageObservation(
                        series_id=o.series_id,
                        date=o.date,
                        vintage_date=as_of,
                        value=o.value,
                        dataflow=o.dataflow,
                    ))
            except (IMFAPIError, IMFRateLimitError):
                continue

        return vintages
