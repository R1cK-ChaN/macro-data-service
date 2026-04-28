"""FRED + ALFRED API client — macro time-series and vintage/revision history."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

from env import get_env_value

logger = logging.getLogger(__name__)


class FredAPIError(RuntimeError):
    """Base error for FRED/ALFRED API failures."""


class FredRateLimitError(FredAPIError):
    """Raised when FRED throttles a request (HTTP 429)."""


def _raise_for_status(response: requests.Response) -> None:
    """Convert HTTP errors to FredAPIError/FredRateLimitError."""
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        if response.status_code == 429:
            raise FredRateLimitError(f"FRED rate limit: {exc}") from exc
        raise FredAPIError(
            f"FRED API error {response.status_code}: {exc}"
        ) from exc


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


def _parse_fred_observations(
    payload: dict, *, series_id: str,
) -> list["FredObservation"]:
    """Parse ``payload['observations']`` into typed ``FredObservation`` rows.

    Standalone helper so the issue #69 re-projection path can replay a
    stored ``obs_raw`` row through the same parser the live HTTP path
    uses — no behavior drift between live ingest and audit replay.
    """
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
        observations, _payload, _params = self.get_series_with_raw(
            series_id, start_date=start_date, limit=limit,
        )
        return observations

    def get_series_with_raw(
        self,
        series_id: str,
        *,
        start_date: str,
        limit: int = 100,
    ) -> tuple[list[FredObservation], dict, dict[str, str]]:
        """Fetch observations and also return the raw HTTP body + request params.

        Used by the issue #69 ``obs_raw`` write path so the projector
        boundary can land both the typed observations and the upstream
        bytes. Returns ``([], {}, params)`` when the API key is unset
        (matches ``get_series`` no-op semantics).
        """
        params = {
            "series_id": series_id,
            "observation_start": start_date,
            "sort_order": "desc",
            "limit": str(limit),
            "file_type": "json",
        }
        if not self.api_key:
            return [], {}, params
        response = self.session.get(
            f"{self.BASE_URL}/series/observations",
            params={**params, "api_key": self.api_key},
            timeout=30,
        )
        _raise_for_status(response)
        payload = response.json()
        observations = _parse_fred_observations(payload, series_id=series_id)
        return observations, payload, params

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
        _raise_for_status(response)
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
        _raise_for_status(response)
        return response.json().get("seriess", [])

    # -- ALFRED methods (vintage/revision history) ---------------------------

    def get_vintages(
        self,
        series_id: str,
        *,
        start_date: str,
        realtime_start: str = "1776-07-04",
        realtime_end: str = "9999-12-31",
    ) -> list[FredVintageObservation]:
        """Fetch all vintage observations for a series.

        Uses FRED's real-time period parameters to retrieve the full
        revision history: each observation_date may appear multiple times with
        different ``realtime_start`` values showing how the value was revised.
        """
        if not self.api_key:
            return []
        params: dict = {
            "series_id": series_id,
            "observation_start": start_date,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
            "api_key": self.api_key,
            "file_type": "json",
        }
        response = self.session.get(
            f"{self.BASE_URL}/series/observations",
            params=params,
            timeout=30,
        )
        _raise_for_status(response)
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
                "realtime_start": "1776-07-04",
                "realtime_end": "9999-12-31",
                "api_key": self.api_key,
                "file_type": "json",
            },
            timeout=30,
        )
        _raise_for_status(response)
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

    # -- Catalog browsing methods ---------------------------------------------

    def get_categories(self, category_id: int = 0) -> list[dict]:
        """Fetch child categories for a given category (default: root)."""
        if not self.api_key:
            return []
        response = self.session.get(
            f"{self.BASE_URL}/category/children",
            params={
                "category_id": category_id,
                "api_key": self.api_key,
                "file_type": "json",
            },
            timeout=30,
        )
        _raise_for_status(response)
        return response.json().get("categories", [])

    def get_category_series(
        self, category_id: int, *, limit: int = 1000,
    ) -> list[dict]:
        """Fetch series belonging to a category."""
        if not self.api_key:
            return []
        response = self.session.get(
            f"{self.BASE_URL}/category/series",
            params={
                "category_id": category_id,
                "limit": limit,
                "api_key": self.api_key,
                "file_type": "json",
            },
            timeout=30,
        )
        _raise_for_status(response)
        return response.json().get("seriess", [])

    def count_category_series(self, category_id: int) -> int:
        """Return total series count for a category (lightweight, limit=1)."""
        if not self.api_key:
            return 0
        response = self.session.get(
            f"{self.BASE_URL}/category/series",
            params={
                "category_id": category_id,
                "limit": 1,
                "api_key": self.api_key,
                "file_type": "json",
            },
            timeout=30,
        )
        _raise_for_status(response)
        return response.json().get("count", 0)

    def get_releases(self, *, limit: int = 1000) -> list[dict]:
        """Fetch all FRED releases."""
        if not self.api_key:
            return []
        response = self.session.get(
            f"{self.BASE_URL}/releases",
            params={
                "limit": limit,
                "api_key": self.api_key,
                "file_type": "json",
            },
            timeout=30,
        )
        _raise_for_status(response)
        return response.json().get("releases", [])

    def get_release_series(
        self, release_id: int, *, limit: int = 1000,
    ) -> list[dict]:
        """Fetch series belonging to a release."""
        if not self.api_key:
            return []
        response = self.session.get(
            f"{self.BASE_URL}/release/series",
            params={
                "release_id": release_id,
                "limit": limit,
                "api_key": self.api_key,
                "file_type": "json",
            },
            timeout=30,
        )
        _raise_for_status(response)
        return response.json().get("seriess", [])

    def get_series_release(self, series_id: str) -> dict:
        """Fetch the release that a series belongs to."""
        if not self.api_key:
            return {}
        response = self.session.get(
            f"{self.BASE_URL}/series/release",
            params={
                "series_id": series_id,
                "api_key": self.api_key,
                "file_type": "json",
            },
            timeout=30,
        )
        _raise_for_status(response)
        releases = response.json().get("releases", [])
        return releases[0] if releases else {}

    def count_release_series(self, release_id: int) -> int:
        """Return total series count for a release (lightweight, limit=1)."""
        if not self.api_key:
            return 0
        response = self.session.get(
            f"{self.BASE_URL}/release/series",
            params={
                "release_id": release_id,
                "limit": 1,
                "api_key": self.api_key,
                "file_type": "json",
            },
            timeout=30,
        )
        _raise_for_status(response)
        return response.json().get("count", 0)
