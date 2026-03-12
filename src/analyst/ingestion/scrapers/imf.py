"""IMF SDMX 3.0 API client — CPI, FX reserves, trade, and GDP dataflows.

Uses the Azure-hosted API at ``api.imf.org`` which requires an
``Ocp-Apim-Subscription-Key`` header.  SDMX 3.0 supports JSON responses
and point-in-time vintage queries via the ``asOf`` parameter.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass

import requests

from analyst.env import get_env_value

logger = logging.getLogger(__name__)

_QUARTER_MAP = {"Q1": "01", "Q2": "04", "Q3": "07", "Q4": "10"}


def _normalize_date(raw: str) -> str:
    """Normalize IMF date strings to YYYY-MM-DD.

    Handles: ``"2025-M09"`` → ``"2025-09-01"`` (new API monthly),
             ``"2024-Q1"``  → ``"2024-01-01"``,
             ``"2024-01"``  → ``"2024-01-01"`` (legacy monthly),
             ``"2024"``     → ``"2024-01-01"``.
    """
    # New API monthly format: 2025-M09
    m = re.match(r"^(\d{4})-M(\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    # Quarterly: 2024-Q1
    m = re.match(r"^(\d{4})-Q(\d)$", raw)
    if m:
        return f"{m.group(1)}-{_QUARTER_MAP.get('Q' + m.group(2), '01')}-01"
    # Legacy monthly: 2024-01
    if re.match(r"^\d{4}-\d{2}$", raw):
        return f"{raw}-01"
    # Annual: 2024
    if re.match(r"^\d{4}$", raw):
        return f"{raw}-01-01"
    return raw


@dataclass(frozen=True)
class IMFObservation:
    """A single observation from the IMF SDMX API."""

    series_id: str
    date: str
    value: float
    dataflow: str = ""


@dataclass(frozen=True)
class IMFVintageObservation:
    """A single vintage (point-in-time) observation from the IMF SDMX 3.0 API."""

    series_id: str
    date: str           # observation date
    vintage_date: str   # asOf date
    value: float
    dataflow: str = ""


class IMFClient:
    """Client for the IMF SDMX 3.0 REST API (requires API key)."""

    BASE_URL = "https://api.imf.org/external/sdmx/3.0"

    def __init__(self, api_key: str | None = None) -> None:
        self.session = requests.Session()
        key = api_key or get_env_value("IMF_API_KEY")
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "AnalystEngine/1.0",
            "Ocp-Apim-Subscription-Key": key,
        })

    def get_data(
        self,
        dataflow_id: str,
        key: str,
        *,
        series_id: str,
        version: str,
        start_period: str | None = None,
        limit: int = 100,
    ) -> list[IMFObservation]:
        """Fetch observations from an SDMX 3.0 dataflow as JSON.

        Args:
            dataflow_id: IMF dataflow, e.g. ``"CPI"``, ``"IRFCL"``.
            key: Dimension key, e.g. ``"CHN.CPI._T.IX.M"``.
            series_id: Logical series id for the returned records.
            version: Exact dataflow version, e.g. ``"5.0.0"``.
            start_period: Optional start filter, e.g. ``"2024"``.
            limit: Maximum observations to return.
        """
        url = f"{self.BASE_URL}/data/dataflow/IMF.STA/{dataflow_id}/{version}/{key}"
        params: dict[str, str] = {
            "attributes": "none",
            "measures": "all",
        }
        if start_period:
            params["startPeriod"] = start_period
        if limit:
            params["lastNObservations"] = str(limit)

        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return self._parse_json(
            response.json(), series_id=series_id, dataflow=dataflow_id, limit=limit,
        )

    @staticmethod
    def _parse_json(
        data: dict,
        *,
        series_id: str,
        dataflow: str,
        limit: int,
    ) -> list[IMFObservation]:
        """Parse SDMX 3.0 JSON response into observations."""
        observations: list[IMFObservation] = []

        try:
            datasets = data["data"]["dataSets"]
            structures = data["data"]["structures"]
        except (KeyError, TypeError):
            return observations

        if not datasets or not structures:
            return observations

        # Find TIME_PERIOD dimension in observation dimensions
        obs_dims = structures[0].get("dimensions", {}).get("observation", [])
        time_dim = None
        for dim in obs_dims:
            if dim.get("id") == "TIME_PERIOD":
                time_dim = dim
                break

        if time_dim is None:
            return observations

        # Build index → time period mapping
        time_map: dict[str, str] = {}
        for i, val in enumerate(time_dim.get("values", [])):
            time_map[str(i)] = val["value"]

        # Iterate all series keys in the first dataset
        all_series = datasets[0].get("series", {})
        for _series_key, series_data in all_series.items():
            for obs_idx, obs_array in series_data.get("observations", {}).items():
                period = time_map.get(obs_idx)
                if not period:
                    continue
                value = obs_array[0] if obs_array else None
                if value is None:
                    continue
                try:
                    observations.append(IMFObservation(
                        series_id=series_id,
                        date=_normalize_date(period),
                        value=float(value),
                        dataflow=dataflow,
                    ))
                except (ValueError, TypeError):
                    continue

        observations.sort(key=lambda o: o.date, reverse=True)
        return observations[:limit]

    def get_vintages(
        self,
        dataflow_id: str,
        key: str,
        *,
        series_id: str,
        version: str,
        as_of_dates: list[str],
        start_period: str | None = None,
        limit: int = 100,
    ) -> list[IMFVintageObservation]:
        """Fetch vintage (point-in-time) observations for multiple asOf dates.

        Args:
            dataflow_id: IMF dataflow, e.g. ``"QNEA"``.
            key: Dimension key, e.g. ``"CHN.B1GQ.V.NSA.XDC.Q"``.
            series_id: Logical series id for the returned records.
            version: Exact dataflow version, e.g. ``"7.0.0"``.
            as_of_dates: List of vintage dates (``"YYYY-MM-DD"``).
            start_period: Optional start filter, e.g. ``"2024"``.
            limit: Maximum observations per vintage call.
        """
        results: list[IMFVintageObservation] = []
        for i, as_of in enumerate(as_of_dates):
            if i > 0:
                time.sleep(1.0)
            try:
                url = f"{self.BASE_URL}/data/dataflow/IMF.STA/{dataflow_id}/{version}/{key}"
                params: dict[str, str] = {
                    "attributes": "none",
                    "measures": "all",
                    "asOf": as_of,
                }
                if start_period:
                    params["startPeriod"] = start_period
                if limit:
                    params["lastNObservations"] = str(limit)

                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                obs_list = self._parse_json(
                    response.json(), series_id=series_id, dataflow=dataflow_id, limit=limit,
                )
                for obs in obs_list:
                    results.append(IMFVintageObservation(
                        series_id=obs.series_id,
                        date=obs.date,
                        vintage_date=as_of,
                        value=obs.value,
                        dataflow=obs.dataflow,
                    ))
            except Exception:
                logger.warning("IMF vintage fetch failed for asOf=%s", as_of, exc_info=True)
        return results
