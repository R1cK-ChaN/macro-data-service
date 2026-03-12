"""Eurostat JSON-stat API client — Euro Area structured indicators."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)


def _normalize_period(raw: str) -> str:
    """Normalize Eurostat period strings to YYYY-MM-DD.

    Handles: ``"2024M01"`` → ``"2024-01-01"``,
             ``"2024Q1"``  → ``"2024-01-01"``,
             ``"2024"``    → ``"2024-01-01"``.
    """
    m = re.match(r"^(\d{4})M(\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    m = re.match(r"^(\d{4})Q(\d)$", raw)
    if m:
        month_map = {"1": "01", "2": "04", "3": "07", "4": "10"}
        return f"{m.group(1)}-{month_map.get(m.group(2), '01')}-01"
    if re.match(r"^\d{4}$", raw):
        return f"{raw}-01-01"
    return raw


@dataclass(frozen=True)
class EurostatObservation:
    """A single observation from the Eurostat API."""

    series_id: str
    date: str
    value: float
    dataset: str = ""


class EurostatClient:
    """Client for the Eurostat JSON-stat dissemination API (no key required)."""

    BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "AnalystEngine/1.0",
        })

    def get_dataset(
        self,
        dataset_code: str,
        *,
        params: dict[str, str] | None = None,
        series_id: str,
        limit: int = 100,
    ) -> list[EurostatObservation]:
        """Fetch observations from a Eurostat dataset.

        Args:
            dataset_code: Dataset identifier, e.g. ``"prc_hicp_manr"``.
            params: Query parameters for dimension filtering.
            series_id: Logical series id for the returned records.
            limit: Maximum observations to return.
        """
        url = f"{self.BASE_URL}/{dataset_code}"
        query: dict[str, str] = {"format": "JSON", "lang": "en"}
        if params:
            query.update(params)

        response = self.session.get(url, params=query, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Extract time dimension → position→period mapping
        time_dim = data.get("dimension", {}).get("time", {}).get("category", {}).get("index", {})
        if not time_dim:
            return []
        # Reverse: position (int) → period string
        pos_to_period: dict[int, str] = {v: k for k, v in time_dim.items()}

        values = data.get("value", {})

        observations: list[EurostatObservation] = []
        for pos_str, val in values.items():
            try:
                pos = int(pos_str)
                if val is None:
                    continue
                period = pos_to_period.get(pos)
                if period is None:
                    continue
                observations.append(EurostatObservation(
                    series_id=series_id,
                    date=_normalize_period(period),
                    value=float(val),
                    dataset=dataset_code,
                ))
            except (ValueError, TypeError, KeyError):
                continue

        observations.sort(key=lambda o: o.date, reverse=True)
        return observations[:limit]
