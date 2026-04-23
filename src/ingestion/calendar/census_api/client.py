"""Census Economic Indicators Time Series API client.

The EITS endpoint is year-addressable and returns a header row followed
by data rows. The calendar connector fetches one dataset/year payload
and filters it against :mod:`indicators`.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import requests

from env import get_env_value

logger = logging.getLogger(__name__)


class CensusEITSError(RuntimeError):
    """Base error for Census EITS failures."""


class CensusEITSResponseError(CensusEITSError):
    """Raised when Census returns a non-tabular or explicit error body."""


@dataclass(frozen=True)
class CensusEITSObservation:
    """One filtered row from the Census EITS API."""

    series_id: str
    dataset: str
    time: str
    data_type_code: str
    category_code: str
    seasonally_adj: str
    time_slot_id: str
    time_slot_name: str
    cell_value: str
    error_data: str
    raw: dict[str, Any]


class CensusEITSClient:
    """Thin client for ``api.census.gov/data/timeseries/eits``."""

    BASE_URL = "https://api.census.gov/data/timeseries/eits"

    def __init__(
        self,
        api_key: str | None = None,
        *,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else get_env_value("CENSUS_API_KEY")
        self.session = session or requests.Session()
        self._last_request_time = 0.0
        self.requests_made = 0

    def get_dataset_year(self, dataset: str, year: int) -> list[dict[str, str]]:
        """Fetch all EITS rows for ``dataset`` in ``year``."""
        url = f"{self.BASE_URL}/{dataset}"
        params: dict[str, str] = {
            "get": (
                "data_type_code,seasonally_adj,category_code,cell_value,"
                "error_data,time_slot_id,time_slot_name"
            ),
            "for": "us:*",
            "time": str(year),
        }
        if self.api_key:
            params["key"] = self.api_key

        self._throttle()
        try:
            response = self.session.get(url, params=params, timeout=30)
            self.requests_made += 1
            response.raise_for_status()
            body = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise CensusEITSError(f"Census EITS request failed: {exc}") from exc

        if isinstance(body, dict):
            error = body.get("error")
            if error:
                raise CensusEITSResponseError(f"Census EITS error: {error}")
            return []
        if not isinstance(body, list) or len(body) < 2:
            return []
        headers = body[0]
        if not isinstance(headers, list):
            raise CensusEITSResponseError("Census EITS header row is not a list")
        rows: list[dict[str, str]] = []
        for raw_row in body[1:]:
            if not isinstance(raw_row, list):
                continue
            rows.append({
                str(key): str(value)
                for key, value in zip(headers, raw_row)
            })
        return rows

    def _throttle(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < 0.25:
            time.sleep(0.25 - elapsed)
        self._last_request_time = time.monotonic()
