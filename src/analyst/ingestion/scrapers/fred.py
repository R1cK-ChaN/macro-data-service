"""FRED + ALFRED API client — macro time-series and vintage/revision history."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

from analyst.env import get_env_value

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FredObservation:
    """A single observation from FRED."""

    series_id: str
    date: str       # YYYY-MM-DD
    value: float


@dataclass(frozen=True)
class FredVintageObservation:
    """A single vintage observation from ALFRED (revision history)."""

    series_id: str
    date: str           # observation date
    vintage_date: str   # when this value was published/revised
    value: float


class FredClient:
    """Client for the FRED and ALFRED REST APIs."""

    BASE_URL = "https://api.stlouisfed.org/fred"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or get_env_value("FRED_API_KEY")
        self.session = requests.Session()

    # -- FRED methods --------------------------------------------------------

    def get_series(
        self,
        series_id: str,
        *,
        start_date: str,
        limit: int = 100,
    ) -> list[FredObservation]:
        """Fetch recent observations for a series."""
        if not self.api_key:
            return []
        response = self.session.get(
            f"{self.BASE_URL}/series/observations",
            params={
                "series_id": series_id,
                "observation_start": start_date,
                "sort_order": "desc",
                "limit": limit,
                "api_key": self.api_key,
                "file_type": "json",
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        observations: list[FredObservation] = []
        for obs in payload.get("observations", []):
            if obs.get("value") == ".":
                continue
            try:
                observations.append(FredObservation(
                    series_id=series_id,
                    date=obs["date"],
                    value=float(obs["value"]),
                ))
            except (KeyError, ValueError, TypeError):
                continue
        return observations

    def get_series_info(self, series_id: str) -> dict:
        """Fetch metadata for a series (title, frequency, units, etc.)."""
        if not self.api_key:
            return {}
        response = self.session.get(
            f"{self.BASE_URL}/series",
            params={
                "series_id": series_id,
                "api_key": self.api_key,
                "file_type": "json",
            },
            timeout=30,
        )
        response.raise_for_status()
        seriess = response.json().get("seriess", [])
        return seriess[0] if seriess else {}

    def search_series(self, query: str, *, limit: int = 10) -> list[dict]:
        """Search FRED for series matching a text query."""
        if not self.api_key:
            return []
        response = self.session.get(
            f"{self.BASE_URL}/series/search",
            params={
                "search_text": query,
                "limit": limit,
                "api_key": self.api_key,
                "file_type": "json",
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json().get("seriess", [])

    # -- ALFRED methods (vintage/revision history) ---------------------------

    def get_vintages(
        self,
        series_id: str,
        *,
        start_date: str,
        vintage_dates: str | None = None,
    ) -> list[FredVintageObservation]:
        """Fetch all vintage observations for a series.

        Uses FRED's ``output_type=2`` (vintage dates) to retrieve the full
        revision history: each observation_date may appear multiple times with
        different vintage_dates showing how the value was revised.
        """
        if not self.api_key:
            return []
        params: dict = {
            "series_id": series_id,
            "observation_start": start_date,
            "output_type": 2,
            "api_key": self.api_key,
            "file_type": "json",
        }
        if vintage_dates:
            params["vintage_dates"] = vintage_dates
        response = self.session.get(
            f"{self.BASE_URL}/series/observations",
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        observations: list[FredVintageObservation] = []
        for obs in payload.get("observations", []):
            if obs.get("value") == ".":
                continue
            try:
                observations.append(FredVintageObservation(
                    series_id=series_id,
                    date=obs["date"],
                    vintage_date=obs.get("realtime_start", ""),
                    value=float(obs["value"]),
                ))
            except (KeyError, ValueError, TypeError):
                continue
        return observations

    def get_revision_history(
        self,
        series_id: str,
        observation_date: str,
    ) -> list[FredVintageObservation]:
        """Get all revisions for a specific observation date.

        Returns each vintage (publication) of the given observation_date,
        showing how the value changed across releases.
        """
        if not self.api_key:
            return []
        response = self.session.get(
            f"{self.BASE_URL}/series/observations",
            params={
                "series_id": series_id,
                "observation_start": observation_date,
                "observation_end": observation_date,
                "output_type": 2,
                "api_key": self.api_key,
                "file_type": "json",
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        revisions: list[FredVintageObservation] = []
        for obs in payload.get("observations", []):
            if obs.get("value") == ".":
                continue
            try:
                revisions.append(FredVintageObservation(
                    series_id=series_id,
                    date=obs["date"],
                    vintage_date=obs.get("realtime_start", ""),
                    value=float(obs["value"]),
                ))
            except (KeyError, ValueError, TypeError):
                continue
        return revisions
