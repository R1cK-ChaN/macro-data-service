"""World Bank REST API client — GDP per capita, GDP growth, current account.

Uses the World Bank Indicators API v2 at ``api.worldbank.org/v2`` which
provides free, unauthenticated access to development indicators.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)


def _normalize_date(raw: str) -> str:
    """Normalize World Bank date strings to YYYY-MM-DD.

    Handles: ``"2023"`` → ``"2023-01-01"`` (annual data).
    """
    if re.match(r"^\d{4}$", raw):
        return f"{raw}-01-01"
    return raw


@dataclass(frozen=True)
class WorldBankObservation:
    """A single observation from the World Bank API."""

    series_id: str
    date: str
    value: float
    indicator: str = ""


class WorldBankClient:
    """Client for the World Bank Indicators API v2 (no API key required)."""

    BASE_URL = "https://api.worldbank.org/v2"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "AnalystEngine/1.0",
        })

    def get_indicator(
        self,
        indicator_code: str,
        country: str,
        *,
        series_id: str,
        start_year: int | None = None,
        limit: int = 50,
    ) -> list[WorldBankObservation]:
        """Fetch indicator observations for a country.

        Args:
            indicator_code: World Bank indicator, e.g. ``"NY.GDP.PCAP.PP.CD"``.
            country: Country code, e.g. ``"USA"``, ``"CHN"``.
            series_id: Logical series id for the returned records.
            start_year: Optional start year filter.
            limit: Maximum observations to return.
        """
        url = f"{self.BASE_URL}/country/{country}/indicator/{indicator_code}"
        params: dict[str, str] = {
            "format": "json",
            "per_page": str(limit),
        }
        if start_year:
            params["date"] = f"{start_year}:2099"

        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return self._parse_json(
            response.json(), series_id=series_id, indicator=indicator_code, limit=limit,
        )

    @staticmethod
    def _parse_json(
        data: list | dict,
        *,
        series_id: str,
        indicator: str,
        limit: int,
    ) -> list[WorldBankObservation]:
        """Parse World Bank JSON response into observations.

        World Bank returns ``[{page_info}, [{record}, ...]]``.
        """
        observations: list[WorldBankObservation] = []

        if not isinstance(data, list) or len(data) < 2:
            return observations

        records = data[1]
        if not records:
            return observations

        for record in records:
            try:
                value = record.get("value")
                if value is None:
                    continue
                date_raw = record.get("date", "")
                if not date_raw:
                    continue
                observations.append(WorldBankObservation(
                    series_id=series_id,
                    date=_normalize_date(str(date_raw)),
                    value=float(value),
                    indicator=indicator,
                ))
            except (ValueError, TypeError):
                continue

        observations.sort(key=lambda o: o.date, reverse=True)
        return observations[:limit]
