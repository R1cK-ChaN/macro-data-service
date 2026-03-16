"""Census Bureau Data API client — ACS, PEP, CBP, and Economic Census data."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

from env import get_env_value

logger = logging.getLogger(__name__)


class CensusAPIError(RuntimeError):
    """Base error for Census API failures."""


class CensusRateLimitError(CensusAPIError):
    """Raised when Census throttles a request (HTTP 429)."""


class CensusResponseError(CensusAPIError):
    """Raised when Census returns an error in the response body."""


@dataclass(frozen=True)
class CensusObservation:
    """A single data point from the Census API."""

    dataset: str        # e.g. "acs/acs5"
    vintage: int        # e.g. 2023
    variable: str       # e.g. "B01001_001E"
    geo_id: str         # e.g. "06" (state FIPS) or "06037" (county FIPS)
    geo_name: str       # e.g. "California"
    value: float | None # parsed numeric; None for annotations/suppressed


@dataclass(frozen=True)
class CensusDataset:
    """A Census dataset from the DCAT discovery catalog."""

    identifier: str     # e.g. "https://api.census.gov/data/id/ACSDT5Y2023"
    title: str          # e.g. "ACS 5-Year Detailed Tables"
    vintage: int | None # e.g. 2023; None if undated
    dataset_path: str   # e.g. "acs/acs5"
    description: str
    api_url: str        # the access URL for data queries


@dataclass(frozen=True)
class CensusVariable:
    """Variable metadata from a Census dataset."""

    name: str               # e.g. "B01001_001E"
    label: str              # e.g. "Estimate!!Total:"
    concept: str            # e.g. "SEX BY AGE"
    predicate_type: str     # e.g. "int", "float", "string"


@dataclass(frozen=True)
class CensusGeography:
    """A supported geography level for a Census dataset."""

    name: str                       # e.g. "state", "county"
    hierarchy: str                  # e.g. "040" (geoLevelDisplay)
    required_geos: tuple[str, ...]  # geo levels required in "in" clause
    wildcard: tuple[str, ...]       # supports wildcard queries


_MIN_REQUEST_INTERVAL = 0.6
_DISCOVERY_URL = "https://api.census.gov/data.json"
_GEO_COLUMNS = ("tract", "block group", "county", "state", "us",
                 "place", "zip code tabulation area",
                 "metropolitan statistical area/micropolitan statistical area")


class CensusClient:
    """Client for the U.S. Census Bureau Data API."""

    BASE_URL = "https://api.census.gov/data"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or get_env_value("CENSUS_API_KEY")
        self.session = requests.Session()
        self._last_request_time = 0.0

    # -- Public methods ------------------------------------------------------

    def list_datasets(
        self,
        *,
        year_min: int | None = None,
        year_max: int | None = None,
    ) -> list[CensusDataset]:
        """Return all available Census datasets from the DCAT catalog.

        Optionally filter by vintage year range. No API key required.
        """
        self._throttle()
        try:
            resp = self.session.get(_DISCOVERY_URL, timeout=60)
            resp.raise_for_status()
            body = resp.json()
        except (requests.RequestException, ValueError) as exc:
            logger.warning("Census discovery failed: %s", exc)
            return []

        raw = body.get("dataset", [])
        datasets: list[CensusDataset] = []
        for entry in raw:
            vintage_raw = entry.get("c_vintage")
            vintage = int(vintage_raw) if vintage_raw is not None else None

            if year_min is not None and (vintage is None or vintage < year_min):
                continue
            if year_max is not None and (vintage is None or vintage > year_max):
                continue

            c_dataset = entry.get("c_dataset", [])
            dataset_path = "/".join(c_dataset) if c_dataset else ""

            # Extract access URL from distribution
            api_url = ""
            for dist in entry.get("distribution", []):
                url = dist.get("accessURL", "")
                if url:
                    api_url = url
                    break

            datasets.append(CensusDataset(
                identifier=entry.get("identifier", ""),
                title=entry.get("title", ""),
                vintage=vintage,
                dataset_path=dataset_path,
                description=entry.get("description", ""),
                api_url=api_url,
            ))
        return datasets

    def get_variables(self, dataset: str, vintage: int) -> list[CensusVariable]:
        """Return variable metadata for a dataset."""
        if not self.api_key:
            return []
        url = f"{self.BASE_URL}/{vintage}/{dataset}/variables.json"
        resp = self._get(url)
        try:
            body = resp.json()
        except ValueError:
            return []

        raw = body.get("variables", {})
        skip = {"for", "in", "ucgid", "NAME", "GEO_ID"}
        variables: list[CensusVariable] = []
        for name, meta in raw.items():
            if name in skip:
                continue
            if not isinstance(meta, dict):
                continue
            variables.append(CensusVariable(
                name=name,
                label=meta.get("label", ""),
                concept=meta.get("concept", ""),
                predicate_type=meta.get("predicateType", ""),
            ))
        return variables

    def get_geographies(self, dataset: str, vintage: int) -> list[CensusGeography]:
        """Return supported geography levels for a dataset."""
        if not self.api_key:
            return []
        url = f"{self.BASE_URL}/{vintage}/{dataset}/geography.json"
        resp = self._get(url)
        try:
            body = resp.json()
        except ValueError:
            return []

        raw = body.get("fips", [])
        geos: list[CensusGeography] = []
        for entry in raw:
            requires = entry.get("requires", []) or []
            if isinstance(requires, str):
                requires = [requires]
            wildcard = entry.get("wildcard", []) or []
            if isinstance(wildcard, str):
                wildcard = [wildcard]
            optional = entry.get("optionalWithWCFor", []) or []
            if isinstance(optional, str):
                optional = [optional]
            geos.append(CensusGeography(
                name=entry.get("name", ""),
                hierarchy=entry.get("geoLevelDisplay", ""),
                required_geos=tuple(requires),
                wildcard=tuple(wildcard + optional),
            ))
        return geos

    def get_data(
        self,
        dataset: str,
        vintage: int,
        variables: list[str],
        *,
        geo_for: str,
        geo_in: str | None = None,
    ) -> list[CensusObservation]:
        """Fetch data for specific variables and geography.

        Args:
            dataset: Dataset path (e.g. "acs/acs5").
            vintage: Year (e.g. 2023).
            variables: Variable names (e.g. ["B01001_001E"]).
            geo_for: Geography spec (e.g. "state:*", "county:*", "us:*").
            geo_in: Parent geography (e.g. "state:06" for CA counties).
        """
        if not self.api_key:
            return []

        url = f"{self.BASE_URL}/{vintage}/{dataset}"
        # Always include NAME for geo_name
        get_vars = ["NAME"] + [v for v in variables if v != "NAME"]
        params: dict[str, str] = {
            "get": ",".join(get_vars),
            "for": geo_for,
        }
        if geo_in:
            params["in"] = geo_in

        resp = self._get(url, **params)

        try:
            body = resp.json()
        except ValueError as exc:
            raise CensusAPIError(f"Census returned non-JSON response") from exc

        # Census error responses are dicts, not arrays
        if isinstance(body, dict):
            error = body.get("error")
            if error:
                raise CensusResponseError(f"Census error: {error}")
            return []

        if not isinstance(body, list) or len(body) < 2:
            return []

        headers = body[0]
        observations: list[CensusObservation] = []
        for row in body[1:]:
            row_dict = dict(zip(headers, row))
            geo_name = row_dict.get("NAME", "")
            geo_id = self._build_geo_id(row_dict)

            for var in variables:
                raw_value = row_dict.get(var)
                observations.append(CensusObservation(
                    dataset=dataset,
                    vintage=vintage,
                    variable=var,
                    geo_id=geo_id,
                    geo_name=geo_name,
                    value=self._parse_value(raw_value),
                ))
        return observations

    def get_acs5(
        self,
        variables: list[str],
        *,
        year: int,
        geo_for: str,
        geo_in: str | None = None,
    ) -> list[CensusObservation]:
        """Convenience method for ACS 5-Year data."""
        return self.get_data(
            "acs/acs5", year, variables,
            geo_for=geo_for, geo_in=geo_in,
        )

    def count_variables(self, dataset: str, vintage: int) -> int:
        """Return the number of variables in a dataset."""
        return len(self.get_variables(dataset, vintage))

    # -- Internal helpers ----------------------------------------------------

    def _get(self, url: str, **params: str) -> requests.Response:
        """Execute a Census API GET request with throttling and error handling."""
        self._throttle()
        if self.api_key:
            params["key"] = self.api_key

        try:
            resp = self.session.get(url, params=params, timeout=30)
        except requests.RequestException as exc:
            raise CensusAPIError(f"Census request failed: {exc}") from exc

        if resp.status_code == 429:
            raise CensusRateLimitError("Census rate limit (HTTP 429)")

        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            # Census sometimes returns error info in the body even with 4xx
            try:
                body = resp.json()
                if isinstance(body, dict) and "error" in body:
                    raise CensusResponseError(
                        f"Census error: {body['error']}"
                    ) from exc
            except ValueError:
                pass
            raise CensusAPIError(
                f"Census HTTP {resp.status_code}: {exc}"
            ) from exc

        self._last_request_time = time.monotonic()
        return resp

    def _throttle(self) -> None:
        """Enforce minimum interval between requests."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    @staticmethod
    def _parse_value(raw: str | None) -> float | None:
        """Parse a Census data value into a float or None.

        Handles Census sentinel values:
        - None, "", "-" → None
        - "-666666666" → None (not available)
        - "-888888888" → None (not applicable)
        - "-999999999" → None (data suppressed)
        - "(X)" → None (not applicable)
        """
        if raw is None:
            return None
        cleaned = str(raw).strip()
        if not cleaned or cleaned in ("-", "(X)", "N", "null",
                                       "-666666666", "-888888888", "-999999999",
                                       "-666666666.0", "-888888888.0", "-999999999.0"):
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None

    @staticmethod
    def _build_geo_id(row_dict: dict[str, str]) -> str:
        """Build a FIPS geo ID from Census response columns.

        Census data includes geography columns (state, county, tract, etc.)
        as separate fields. This concatenates them into a single FIPS code.
        """
        parts: list[str] = []
        # Build in hierarchy order
        for col in ("state", "county", "tract", "block group"):
            val = row_dict.get(col)
            if val is not None:
                parts.append(val)
        if parts:
            return "".join(parts)
        # National level
        if "us" in row_dict:
            return "us"
        return ""
