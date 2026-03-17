"""NY Fed fetcher adapter — NYFedRatesClient → list[RawSeries]."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.scrapers.nyfed import NYFedRatesClient
from ingestion.types import RawObservation, RawSeries

_RATE_METHODS = {
    "NYFED_SOFR": "fetch_sofr",
    "NYFED_EFFR": "fetch_effr",
    "NYFED_OBFR": "fetch_obfr",
}


class NYFedFetcher:
    source_name = "nyfed"

    def __init__(self, client: NYFedRatesClient | None = None) -> None:
        self.client = client or NYFedRatesClient()

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]:
        results: list[RawSeries] = []
        for series_id, method_name in _RATE_METHODS.items():
            rs = self._fetch_one(series_id, method_name)
            if rs is not None:
                results.append(rs)
        return results

    def fetch_series(
        self, series_id: str, *, lookback_days: int = 365
    ) -> RawSeries | None:
        method_name = _RATE_METHODS.get(series_id)
        if method_name is None:
            return None
        return self._fetch_one(series_id, method_name)

    def _fetch_one(self, series_id: str, method_name: str) -> RawSeries | None:
        try:
            rates = getattr(self.client, method_name)(last_n=30)
        except Exception:
            return None
        raw_obs = tuple(
            RawObservation(
                date=r.date,
                value=r.rate,
                provider_metadata={
                    k: v
                    for k, v in {
                        "percentile_1": r.percentile_1,
                        "percentile_25": r.percentile_25,
                        "percentile_75": r.percentile_75,
                        "percentile_99": r.percentile_99,
                        "volume_billions": r.volume_billions,
                        "target_rate_from": r.target_rate_from,
                        "target_rate_to": r.target_rate_to,
                    }.items()
                    if v is not None
                },
            )
            for r in rates
        )
        return RawSeries(
            source="nyfed",
            series_id=series_id,
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={"category": "rates", "type": series_id.split("_")[-1].lower()},
        )
