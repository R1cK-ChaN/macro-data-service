"""Client for the NY Fed Markets API — SOFR, EFFR, and OBFR reference rates."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NYFedRate:
    """A single rate observation from the NY Fed Markets API."""

    date: str
    type: str  # SOFR, EFFR, or OBFR
    rate: float
    percentile_1: float | None = None
    percentile_25: float | None = None
    percentile_75: float | None = None
    percentile_99: float | None = None
    volume_billions: float | None = None
    target_rate_from: float | None = None
    target_rate_to: float | None = None


class NYFedRatesClient:
    """Fetches daily reference rates from the NY Fed Markets API."""

    BASE_URL = "https://markets.newyorkfed.org/api/rates"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "AnalystEngine/1.0",
        })

    def fetch_sofr(self, last_n: int = 5) -> list[NYFedRate]:
        """Fetch the last N SOFR observations."""
        url = f"{self.BASE_URL}/secured/sofr/last/{last_n}.json"
        return self._fetch_rates(url, "SOFR")

    def fetch_effr(self, last_n: int = 5) -> list[NYFedRate]:
        """Fetch the last N EFFR observations."""
        url = f"{self.BASE_URL}/unsecured/effr/last/{last_n}.json"
        return self._fetch_rates(url, "EFFR")

    def fetch_obfr(self, last_n: int = 5) -> list[NYFedRate]:
        """Fetch the last N OBFR observations."""
        url = f"{self.BASE_URL}/unsecured/obfr/last/{last_n}.json"
        return self._fetch_rates(url, "OBFR")

    def fetch_all_rates(self, last_n: int = 5) -> list[NYFedRate]:
        """Fetch SOFR, EFFR, and OBFR with a short delay between requests."""
        all_rates: list[NYFedRate] = []
        all_rates.extend(self.fetch_sofr(last_n))
        time.sleep(0.5)
        all_rates.extend(self.fetch_effr(last_n))
        time.sleep(0.5)
        all_rates.extend(self.fetch_obfr(last_n))
        return all_rates

    def _fetch_rates(self, url: str, rate_type: str) -> list[NYFedRate]:
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        return self._parse_rates(data, rate_type)

    def _parse_rates(self, data: dict, rate_type: str) -> list[NYFedRate]:
        rates: list[NYFedRate] = []
        for obs in data.get("refRates", []):
            try:
                volume_raw = obs.get("volumeInBillions")
                volume = float(volume_raw) if volume_raw is not None else None

                rates.append(NYFedRate(
                    date=obs.get("effectiveDate", ""),
                    type=rate_type,
                    rate=float(obs.get("percentRate", 0)),
                    percentile_1=_float_or_none(obs.get("percentPercentile1")),
                    percentile_25=_float_or_none(obs.get("percentPercentile25")),
                    percentile_75=_float_or_none(obs.get("percentPercentile75")),
                    percentile_99=_float_or_none(obs.get("percentPercentile99")),
                    volume_billions=volume,
                    target_rate_from=_float_or_none(obs.get("targetRateFrom")),
                    target_rate_to=_float_or_none(obs.get("targetRateTo")),
                ))
            except (ValueError, TypeError):
                continue
        return rates


def _float_or_none(val: str | float | None) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
