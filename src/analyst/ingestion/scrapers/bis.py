"""BIS SDMX REST API client — policy rates, exchange rates, credit gaps, property prices."""

from __future__ import annotations

import csv
import io
import logging
import re
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)


def _normalize_date(raw: str) -> str:
    """Normalize BIS date strings to YYYY-MM-DD.

    Handles: ``"2024-01"`` → ``"2024-01-01"``,
             ``"2024-Q1"`` → ``"2024-01-01"``,
             ``"2024"``    → ``"2024-01-01"``.
    """
    m = re.match(r"^(\d{4})-Q(\d)$", raw)
    if m:
        quarter_map = {"1": "01", "2": "04", "3": "07", "4": "10"}
        return f"{m.group(1)}-{quarter_map.get(m.group(2), '01')}-01"
    if re.match(r"^\d{4}-\d{2}$", raw):
        return f"{raw}-01"
    if re.match(r"^\d{4}$", raw):
        return f"{raw}-01-01"
    return raw


@dataclass(frozen=True)
class BISObservation:
    """A single observation from the BIS SDMX API."""

    series_id: str
    date: str
    value: float
    dataflow: str = ""


class BISClient:
    """Client for the BIS SDMX REST API (no API key required).

    Uses CSV format from ``stats.bis.org`` which is the live BIS
    statistical data warehouse.
    """

    BASE_URL = "https://stats.bis.org/api/v2"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "*/*",
            "User-Agent": "AnalystEngine/1.0",
        })

    def get_data(
        self,
        dataflow_id: str,
        key: str,
        *,
        series_id: str,
        start_period: str | None = None,
        limit: int = 100,
    ) -> list[BISObservation]:
        """Fetch observations from a BIS dataflow as CSV.

        Args:
            dataflow_id: BIS dataflow, e.g. ``"WS_CBPOL"``.
            key: Dimension key, e.g. ``"M.US"``.
            series_id: Logical series id for the returned records.
            start_period: Optional start filter.
            limit: Maximum observations to return.
        """
        url = f"{self.BASE_URL}/data/dataflow/BIS/{dataflow_id}/1.0/{key}"
        params: dict[str, str] = {
            "detail": "dataonly",
            "format": "csvdata",
        }
        if start_period:
            params["startPeriod"] = start_period

        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()

        return self._parse_csv(response.text, series_id=series_id, dataflow=dataflow_id, limit=limit)

    @staticmethod
    def _parse_csv(
        text: str,
        *,
        series_id: str,
        dataflow: str,
        limit: int,
    ) -> list[BISObservation]:
        """Parse BIS CSV response into observations.

        CSV columns typically include: FREQ, REF_AREA, TIME_PERIOD, OBS_VALUE
        (plus other dimension columns depending on the dataflow).
        """
        reader = csv.DictReader(io.StringIO(text))
        observations: list[BISObservation] = []
        for row in reader:
            try:
                val = row.get("OBS_VALUE")
                if val is None or val == "":
                    continue
                period = row.get("TIME_PERIOD", "")
                if not period:
                    continue
                observations.append(BISObservation(
                    series_id=series_id,
                    date=_normalize_date(period),
                    value=float(val),
                    dataflow=dataflow,
                ))
            except (ValueError, TypeError):
                continue

        observations.sort(key=lambda o: o.date, reverse=True)
        return observations[:limit]
