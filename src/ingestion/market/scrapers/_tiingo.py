"""Tiingo EOD API client — US stocks and ETFs daily OHLCV + adjustments."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import requests

from env import get_env_value

logger = logging.getLogger(__name__)


class TiingoAPIError(RuntimeError):
    """Base error for Tiingo API failures."""


class TiingoRateLimitError(TiingoAPIError):
    """Raised when Tiingo throttles a request (HTTP 429)."""


class TiingoAuthError(TiingoAPIError):
    """Raised when the API key is missing or rejected (HTTP 401/403)."""


def _raise_for_status(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        if response.status_code == 429:
            raise TiingoRateLimitError(f"Tiingo rate limit: {exc}") from exc
        if response.status_code in (401, 403):
            raise TiingoAuthError(f"Tiingo auth error {response.status_code}: {exc}") from exc
        raise TiingoAPIError(f"Tiingo API error {response.status_code}: {exc}") from exc


@dataclass(frozen=True)
class TiingoDailyBar:
    """Normalized Tiingo EOD row — keeps both raw and adjusted values."""

    ticker: str
    date: str                       # YYYY-MM-DD
    open: float
    high: float
    low: float
    close: float
    volume: float
    adj_open: float | None
    adj_high: float | None
    adj_low: float | None
    adj_close: float | None
    adj_volume: float | None
    div_cash: float
    split_factor: float


@dataclass(frozen=True)
class TiingoMetadata:
    """Metadata from /tiingo/daily/<ticker>."""

    ticker: str
    name: str
    exchange_code: str
    start_date: str
    end_date: str
    description: str


class TiingoClient:
    """Low-level HTTP client for https://api.tiingo.com/tiingo/daily."""

    BASE_URL = "https://api.tiingo.com/tiingo"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or get_env_value("TIINGO_API_KEY")
        self.session = requests.Session()

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Token {self.api_key}"
        return headers

    def get_metadata(self, ticker: str) -> TiingoMetadata | None:
        if not self.api_key:
            logger.warning("TIINGO_API_KEY not set; skipping metadata for %s", ticker)
            return None
        response = self.session.get(
            f"{self.BASE_URL}/daily/{ticker.lower()}",
            headers=self._headers(),
            timeout=30,
        )
        _raise_for_status(response)
        payload = response.json() or {}
        return TiingoMetadata(
            ticker=payload.get("ticker", ticker).upper(),
            name=payload.get("name", ""),
            exchange_code=payload.get("exchangeCode", ""),
            start_date=payload.get("startDate", ""),
            end_date=payload.get("endDate", ""),
            description=payload.get("description", ""),
        )

    def get_daily_bars(
        self,
        ticker: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[TiingoDailyBar]:
        """Fetch daily OHLCV bars with raw and adjusted values.

        Uses Tiingo's /tiingo/daily/<ticker>/prices endpoint. Returns an empty
        list when no API key is set so the caller can decide how to handle it.
        """
        if not self.api_key:
            logger.warning("TIINGO_API_KEY not set; skipping bars for %s", ticker)
            return []
        params: dict[str, str] = {"format": "json", "resampleFreq": "daily"}
        if start_date:
            params["startDate"] = start_date
        if end_date:
            params["endDate"] = end_date
        response = self.session.get(
            f"{self.BASE_URL}/daily/{ticker.lower()}/prices",
            params=params,
            headers=self._headers(),
            timeout=60,
        )
        _raise_for_status(response)
        rows = response.json() or []
        bars: list[TiingoDailyBar] = []
        ticker_upper = ticker.upper()
        for row in rows:
            date_str = str(row.get("date", ""))[:10]
            if not date_str:
                continue
            try:
                bars.append(
                    TiingoDailyBar(
                        ticker=ticker_upper,
                        date=date_str,
                        open=_as_float(row.get("open")),
                        high=_as_float(row.get("high")),
                        low=_as_float(row.get("low")),
                        close=_as_float(row.get("close")),
                        volume=_as_float(row.get("volume"), default=0.0),
                        adj_open=_as_optional_float(row.get("adjOpen")),
                        adj_high=_as_optional_float(row.get("adjHigh")),
                        adj_low=_as_optional_float(row.get("adjLow")),
                        adj_close=_as_optional_float(row.get("adjClose")),
                        adj_volume=_as_optional_float(row.get("adjVolume")),
                        div_cash=_as_float(row.get("divCash"), default=0.0),
                        split_factor=_as_float(row.get("splitFactor"), default=1.0),
                    )
                )
            except (TypeError, ValueError):
                logger.debug("Tiingo row skipped for %s: %s", ticker_upper, row)
                continue
        bars.sort(key=lambda b: b.date)
        return bars


def _as_float(value: object, *, default: float | None = None) -> float:
    if value is None or value == "":
        if default is None:
            raise ValueError("missing numeric value")
        return default
    return float(value)  # type: ignore[arg-type]


def _as_optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
