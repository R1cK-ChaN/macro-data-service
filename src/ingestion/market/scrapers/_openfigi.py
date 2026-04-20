"""OpenFIGI client for identity enrichment and mapping repair.

Wraps the public ``POST /v3/mapping`` and ``POST /v3/search`` endpoints.
An ``OPENFIGI_API_KEY`` unlocks the higher rate limit; callers can run
without one at the anonymous rate.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import requests

from env import get_env_value

logger = logging.getLogger(__name__)


class OpenFIGIAPIError(RuntimeError):
    """Base error for OpenFIGI API failures."""


class OpenFIGIRateLimitError(OpenFIGIAPIError):
    """Raised on HTTP 429 from OpenFIGI."""


def _raise_for_status(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        if response.status_code == 429:
            raise OpenFIGIRateLimitError(f"OpenFIGI rate limit: {exc}") from exc
        raise OpenFIGIAPIError(
            f"OpenFIGI API error {response.status_code}: {exc}"
        ) from exc


@dataclass(frozen=True)
class OpenFIGIHit:
    figi: str
    name: str
    ticker: str
    exch_code: str
    composite_figi: str
    share_class_figi: str
    security_type: str
    market_sector: str
    security_description: str


class OpenFIGIClient:
    """Thin wrapper around https://api.openfigi.com/v3/mapping."""

    BASE_URL = "https://api.openfigi.com/v3"

    def __init__(self, api_key: str | None = None) -> None:
        # OpenFIGI's env var is OPENFIGI_API_KEY (X-OPENFIGI-APIKEY header).
        self.api_key = api_key or get_env_value("OPENFIGI_API_KEY")
        self.session = requests.Session()

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-OPENFIGI-APIKEY"] = self.api_key
        return headers

    def map(self, jobs: list[dict[str, Any]]) -> list[list[OpenFIGIHit]]:
        """Run a batch ``mapping`` call.

        ``jobs`` is a list of dicts like ``{"idType": "ID_ISIN", "idValue": "..."}``.
        The return list is aligned with ``jobs``: each element is the list of
        hits OpenFIGI returned for that job (possibly empty).
        """
        if not jobs:
            return []
        response = self.session.post(
            f"{self.BASE_URL}/mapping",
            json=jobs,
            headers=self._headers(),
            timeout=30,
        )
        _raise_for_status(response)
        payload = response.json() or []
        results: list[list[OpenFIGIHit]] = []
        for group in payload:
            if not isinstance(group, dict):
                results.append([])
                continue
            data = group.get("data") or []
            hits: list[OpenFIGIHit] = []
            for row in data:
                try:
                    hits.append(
                        OpenFIGIHit(
                            figi=str(row.get("figi", "")),
                            name=str(row.get("name", "")),
                            ticker=str(row.get("ticker", "")),
                            exch_code=str(row.get("exchCode", "")),
                            composite_figi=str(row.get("compositeFIGI", "")),
                            share_class_figi=str(row.get("shareClassFIGI", "")),
                            security_type=str(row.get("securityType", "")),
                            market_sector=str(row.get("marketSector", "")),
                            security_description=str(row.get("securityDescription", "")),
                        )
                    )
                except (TypeError, ValueError):
                    continue
            results.append(hits)
        return results

    def map_by_isin(self, isin: str) -> list[OpenFIGIHit]:
        batch = self.map([{"idType": "ID_ISIN", "idValue": isin}])
        return batch[0] if batch else []

    def map_by_ticker(self, ticker: str, *, exch_code: str | None = None) -> list[OpenFIGIHit]:
        job: dict[str, Any] = {"idType": "TICKER", "idValue": ticker}
        if exch_code:
            job["exchCode"] = exch_code
        batch = self.map([job])
        return batch[0] if batch else []


__all__ = [
    "OpenFIGIClient",
    "OpenFIGIHit",
    "OpenFIGIAPIError",
    "OpenFIGIRateLimitError",
]
