"""NY Fed fetcher adapter — NYFedRatesClient → list[RawSeries]."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.scrapers.nyfed import NYFedRatesClient
from ingestion.types import RawObservation, RawSeries

_SERIES_METHODS = {
    "NYFED_SOFR": ("fetch_sofr", "rates", "sofr"),
    "NYFED_EFFR": ("fetch_effr", "rates", "effr"),
    "NYFED_OBFR": ("fetch_obfr", "rates", "obfr"),
    "NYFED_GSCPI": ("fetch_gscpi", "supply_chain", "gscpi"),
}


class NYFedFetcher:
    source_name = "nyfed"

    def __init__(self, client: NYFedRatesClient | None = None) -> None:
        self.client = client or NYFedRatesClient()

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]:
        results: list[RawSeries] = []
        for series_id, (method_name, category, series_type) in _SERIES_METHODS.items():
            rs = self._fetch_one(series_id, method_name, category, series_type)
            if rs is not None:
                results.append(rs)
        return results

    def fetch_series(
        self, series_id: str, *, lookback_days: int = 365
    ) -> RawSeries | None:
        config = _SERIES_METHODS.get(series_id)
        if config is None:
            return None
        method_name, category, series_type = config
        return self._fetch_one(series_id, method_name, category, series_type)

    def _fetch_one(
        self,
        series_id: str,
        method_name: str,
        category: str,
        series_type: str,
    ) -> RawSeries | None:
        try:
            observations = getattr(self.client, method_name)(last_n=30)
        except Exception:
            return None
        raw_obs = tuple(
            RawObservation(
                date=obs.date,
                value=_observation_value(obs),
                provider_metadata=_provider_metadata(obs),
            )
            for obs in observations
        )
        return RawSeries(
            source="nyfed",
            series_id=series_id,
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={"category": category, "type": series_type},
        )


def _observation_value(obs: object) -> float:
    rate = getattr(obs, "rate", None)
    if rate is not None:
        return float(rate)
    return float(getattr(obs, "value"))


def _provider_metadata(obs: object) -> dict[str, float]:
    return {
        k: v
        for k, v in {
            "percentile_1": getattr(obs, "percentile_1", None),
            "percentile_25": getattr(obs, "percentile_25", None),
            "percentile_75": getattr(obs, "percentile_75", None),
            "percentile_99": getattr(obs, "percentile_99", None),
            "volume_billions": getattr(obs, "volume_billions", None),
            "target_rate_from": getattr(obs, "target_rate_from", None),
            "target_rate_to": getattr(obs, "target_rate_to", None),
        }.items()
        if v is not None
    }
