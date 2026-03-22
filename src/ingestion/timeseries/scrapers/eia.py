"""EIA (Energy Information Administration) API v2 client — US energy data."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

from env import get_env_value

logger = logging.getLogger(__name__)


# -- Exceptions ---------------------------------------------------------------

class EIAAPIError(RuntimeError):
    """Base error for EIA API failures."""


class EIARateLimitError(EIAAPIError):
    """Raised when EIA throttles a request (HTTP 429)."""


class EIAResponseError(EIAAPIError):
    """Raised when EIA returns an error inside the response body."""


# -- Data classes --------------------------------------------------------------

@dataclass(frozen=True)
class EIAObservation:
    """A single observation from the EIA API."""

    series_id: str
    date: str
    value: float
    unit: str = ""


@dataclass(frozen=True)
class EIARoute:
    """A route (category) from the EIA v2 route tree."""

    route_id: str
    name: str
    description: str


@dataclass(frozen=True)
class EIAFacet:
    """A facet definition for an EIA dataset."""

    facet_id: str
    description: str


_MIN_REQUEST_INTERVAL = 0.5  # seconds between requests


class EIAClient:
    """Client for the EIA Open Data API v2."""

    BASE_URL = "https://api.eia.gov/v2"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or get_env_value("EIA_API_KEY")
        self._timeout = int(get_env_value("EIA_TIMEOUT", default="60") or "60")
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "AnalystEngine/1.0",
        })
        self._last_request_time = 0.0

    # -- Public methods --------------------------------------------------------

    def list_routes(self, parent: str = "") -> list[EIARoute]:
        """Discover child routes at a given level.

        Args:
            parent: Parent route path (empty string for top-level).
        """
        if not self.api_key:
            return []
        path = f"/{parent}" if parent else ""
        data = self._get(f"{self.BASE_URL}{path}")
        response = data.get("response", {})
        routes_raw = response.get("routes", [])
        return [
            EIARoute(
                route_id=r.get("id", ""),
                name=r.get("name", ""),
                description=r.get("description", ""),
            )
            for r in routes_raw
        ]

    def get_facets(self, route: str) -> list[EIAFacet]:
        """Extract facet definitions from a dataset route's metadata.

        Args:
            route: Dataset path, e.g. ``"petroleum/pri/spt"``.
        """
        if not self.api_key:
            return []
        data = self._get(f"{self.BASE_URL}/{route}")
        response = data.get("response", {})
        facets_raw = response.get("facets", [])
        if isinstance(facets_raw, list):
            return [
                EIAFacet(
                    facet_id=f.get("id", ""),
                    description=f.get("description", ""),
                )
                for f in facets_raw
            ]
        # facets can also be a dict keyed by facet id
        if isinstance(facets_raw, dict):
            return [
                EIAFacet(
                    facet_id=fid,
                    description=fval.get("description", "") if isinstance(fval, dict) else str(fval),
                )
                for fid, fval in facets_raw.items()
            ]
        return []

    def count_routes(self, parent: str = "") -> int:
        """Return the number of child routes at a level."""
        return len(self.list_routes(parent))

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

        # Determine which column holds the value — EIA v2 names the column
        # after the data[] parameter (e.g. "sales", "generation"), falling
        # back to "value" for datasets that use a generic column.
        value_col = params.get("data[]", "value")

        data = self._get(url, _raw_params=query, _skip_api_key=True)
        rows = data.get("response", {}).get("data", [])
        observations: list[EIAObservation] = []
        for row in rows:
            try:
                val = row.get(value_col) if value_col != "value" else row.get("value")
                if val is None and value_col != "value":
                    val = row.get("value")
                if val is None or val == "":
                    continue
                # Derive unit from "<col>-units" or generic "units"/"unit"
                unit_key = f"{value_col}-units" if value_col != "value" else "units"
                unit = str(row.get(unit_key, row.get("units", row.get("unit", ""))))
                observations.append(EIAObservation(
                    series_id=series_id,
                    date=str(row.get("period", "")),
                    value=float(val),
                    unit=unit,
                ))
            except (ValueError, TypeError):
                continue
        return observations

    def get_metadata(self, route: str) -> dict:
        """Fetch metadata/facets for an EIA dataset route."""
        if not self.api_key:
            return {}
        data = self._get(f"{self.BASE_URL}/{route}")
        return data

    # -- Internal helpers ------------------------------------------------------

    def _get(
        self,
        url: str,
        *,
        _raw_params: dict | None = None,
        _skip_api_key: bool = False,
        **params: str,
    ) -> dict:
        """Execute an EIA API GET request with throttling and error handling."""
        self._throttle()

        if _raw_params is not None:
            query = _raw_params
        else:
            query: dict = {"api_key": self.api_key or ""}
            query.update(params)

        if not _skip_api_key and "api_key" not in query:
            query["api_key"] = self.api_key or ""

        try:
            resp = self.session.get(url, params=query, timeout=self._timeout)
        except requests.RequestException as exc:
            raise EIAAPIError(f"EIA request failed: {exc}") from exc

        if resp.status_code == 429:
            raise EIARateLimitError("EIA rate limit (HTTP 429)")

        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            raise EIAAPIError(f"EIA HTTP {resp.status_code}: {exc}") from exc

        try:
            body = resp.json()
        except ValueError as exc:
            raise EIAAPIError("EIA returned non-JSON response") from exc

        # Check for error in response body
        if isinstance(body, dict):
            error = body.get("error")
            if error:
                raise EIAResponseError(f"EIA error: {error}")
            # Some endpoints nest errors under response
            resp_data = body.get("response", {})
            if isinstance(resp_data, dict):
                resp_error = resp_data.get("error")
                if resp_error:
                    raise EIAResponseError(f"EIA response error: {resp_error}")

        return body

    def _throttle(self) -> None:
        """Enforce minimum interval between requests."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()
