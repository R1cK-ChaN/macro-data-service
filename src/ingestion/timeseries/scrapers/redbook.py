"""Redbook Research weekly retail-sales client.

The Redbook Index is proprietary Redbook Research data. This client uses the
authorized Trading Economics historical API feed for the public weekly series
and keeps the source metadata anchored on Redbook Research.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import requests

from env import get_env_value


REDBOOK_SOURCE_NAME = "Redbook Research Inc."
REDBOOK_RESEARCH_URL = "https://www.redbookresearch.com/"
TE_API_BASE_URL = "https://api.tradingeconomics.com"
TE_REDBOOK_HISTORICAL_PATH = (
    "/historical/country/united%20states/indicator/redbook%20index"
)


@dataclass(frozen=True)
class RedbookObservation:
    date: str
    value: float


@dataclass(frozen=True)
class RedbookHistoricalRow:
    date: str
    value: float
    frequency: str
    category: str
    source_symbol: str
    last_update: str


class RedbookAuthError(RuntimeError):
    """Raised when a Trading Economics API key is required for Redbook."""


class RedbookAPIError(RuntimeError):
    """Raised when the Redbook historical API request fails."""


class RedbookClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def get_all_series_with_raw(
        self,
        series_config: dict[str, dict[str, Any]],
        *,
        lookback_days: int = 365,
    ) -> dict[str, tuple[list[RedbookObservation], dict, dict[str, str]]]:
        start_date = _lookback_start_date(lookback_days)
        rows = self.fetch_historical_rows(
            lookback_days=lookback_days,
            start_date=start_date,
        )
        result: dict[str, tuple[list[RedbookObservation], dict, dict[str, str]]] = {}
        for cfg in series_config.values():
            observations, payload, params = self._series_payload(
                cfg,
                rows,
                lookback_days=lookback_days,
                start_date=start_date,
            )
            result[str(cfg["series_id"])] = (observations, payload, params)
        return result

    def get_series_with_raw(
        self,
        cfg: dict[str, Any],
        *,
        lookback_days: int = 365,
    ) -> tuple[list[RedbookObservation], dict, dict[str, str]]:
        return self.get_all_series_with_raw(
            {"series": cfg},
            lookback_days=lookback_days,
        )[str(cfg["series_id"])]

    def fetch_historical_rows(
        self,
        *,
        lookback_days: int = 365,
        start_date: str | None = None,
    ) -> list[RedbookHistoricalRow]:
        params: dict[str, str] = {"c": self._resolve_api_key(), "f": "json"}
        if start_date is None:
            start_date = _lookback_start_date(lookback_days)
        if start_date:
            params["d1"] = start_date
        try:
            response = self.session.get(
                f"{TE_API_BASE_URL}{TE_REDBOOK_HISTORICAL_PATH}",
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            response_obj = getattr(exc, "response", None)
            status = getattr(response_obj, "status_code", None)
            detail = f" status={status}" if status is not None else ""
            raise RedbookAPIError(
                f"Redbook historical API request failed{detail}"
            ) from None
        payload = response.json()
        return parse_redbook_historical_rows(payload)

    def _resolve_api_key(self) -> str:
        if self.api_key:
            return self.api_key
        value = get_env_value("TE_API_KEY", "TRADINGECONOMICS_API_KEY")
        if value:
            return value
        raise RedbookAuthError(
            "TE_API_KEY not set; Redbook historical API access requires credentials"
        )

    def _series_payload(
        self,
        cfg: dict[str, Any],
        rows: list[RedbookHistoricalRow],
        *,
        lookback_days: int,
        start_date: str | None,
    ) -> tuple[list[RedbookObservation], dict, dict[str, str]]:
        observations = [
            RedbookObservation(date=row.date, value=row.value)
            for row in rows
        ]
        params = {
            "url": f"{TE_API_BASE_URL}{TE_REDBOOK_HISTORICAL_PATH}",
            "country": str(cfg["country"]),
            "indicator": str(cfg["indicator"]),
            "source_symbol": str(cfg["source_symbol"]),
            "lookback_days": str(lookback_days),
        }
        if start_date:
            params["d1"] = start_date
        payload = {
            "source_url": REDBOOK_RESEARCH_URL,
            "source_name": REDBOOK_SOURCE_NAME,
            "provider": "tradingeconomics",
            "api_path": TE_REDBOOK_HISTORICAL_PATH,
            "country": cfg["country"],
            "indicator": cfg["indicator"],
            "source_symbol": cfg["source_symbol"],
            "series_id": cfg["series_id"],
            "observations": [
                {"date": obs.date, "value": obs.value}
                for obs in observations
            ],
            "rows": [
                {
                    "date": row.date,
                    "value": row.value,
                    "frequency": row.frequency,
                    "category": row.category,
                    "source_symbol": row.source_symbol,
                    "last_update": row.last_update,
                }
                for row in rows
            ],
        }
        return observations, payload, params


def parse_redbook_historical_rows(payload: Any) -> list[RedbookHistoricalRow]:
    if not isinstance(payload, list):
        raise ValueError("Redbook historical payload must be a list")
    rows: list[RedbookHistoricalRow] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        raw_date = item.get("DateTime")
        value = item.get("Value")
        if raw_date is None or value is None:
            continue
        rows.append(
            RedbookHistoricalRow(
                date=_parse_date(str(raw_date)),
                value=float(value),
                frequency=str(item.get("Frequency") or ""),
                category=str(item.get("Category") or ""),
                source_symbol=str(item.get("HistoricalDataSymbol") or ""),
                last_update=str(item.get("LastUpdate") or ""),
            )
        )
    rows.sort(key=lambda row: row.date)
    return rows


def _parse_date(raw: str) -> str:
    text = raw.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed.date().isoformat()


def _lookback_start_date(lookback_days: int) -> str | None:
    if lookback_days <= 0:
        return None
    start = datetime.now(UTC).date() - timedelta(days=lookback_days)
    return start.isoformat()
