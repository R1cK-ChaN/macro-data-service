"""EIA (Energy Information Administration) API v2 client — US energy data."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

from analyst.env import get_env_value

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EIAObservation:
    """A single observation from the EIA API."""

    series_id: str
    date: str
    value: float
    unit: str = ""


class EIAClient:
    """Client for the EIA Open Data API v2."""

    BASE_URL = "https://api.eia.gov/v2"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or get_env_value("EIA_API_KEY")
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "AnalystEngine/1.0",
        })

    def get_series(
        self,
        route: str,
        *,
        params: dict,
        series_id: str,
        start: str | None = None,
        limit: int = 100,
    ) -> list[EIAObservation]:
        """Fetch observations from an EIA v2 dataset route.

        Args:
            route: Dataset path, e.g. ``"petroleum/pri/spt/data"``.
            params: Query parameters for facets/data selection.
            series_id: Logical series identifier for the returned records.
            start: Optional start date filter (YYYY-MM-DD).
            limit: Maximum rows to return.
        """
        if not self.api_key:
            return []
        url = f"{self.BASE_URL}/{route}"
        query: dict = {
            "api_key": self.api_key,
            "length": limit,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
        }
        query.update(params)
        if start:
            query["start"] = start
        response = self.session.get(url, params=query, timeout=30)
        response.raise_for_status()
        payload = response.json()
        data = payload.get("response", {}).get("data", [])
        observations: list[EIAObservation] = []
        for row in data:
            try:
                val = row.get("value")
                if val is None or val == "":
                    continue
                observations.append(EIAObservation(
                    series_id=series_id,
                    date=str(row.get("period", "")),
                    value=float(val),
                    unit=str(row.get("units", row.get("unit", ""))),
                ))
            except (ValueError, TypeError):
                continue
        return observations

    def get_metadata(self, route: str) -> dict:
        """Fetch metadata/facets for an EIA dataset route."""
        if not self.api_key:
            return {}
        url = f"{self.BASE_URL}/{route}"
        response = self.session.get(
            url,
            params={"api_key": self.api_key},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
