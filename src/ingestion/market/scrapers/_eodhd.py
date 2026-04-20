"""EODHD EOD API client — global equities/ETFs/indices outside the US.

Only the `/api/eod/{TICKER}.{EXCHANGE}` endpoint is wired here. Dividends
and splits live on separate EODHD endpoints and are deferred to the P2
corporate-action + lazy-repair flow described in issue #1.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import requests

from env import get_env_value

logger = logging.getLogger(__name__)


class EODHDAPIError(RuntimeError):
    """Base error for EODHD API failures."""


class EODHDRateLimitError(EODHDAPIError):
    """Raised when EODHD throttles a request (HTTP 429)."""


class EODHDAuthError(EODHDAPIError):
    """Raised when the API token is missing or rejected (HTTP 401/403)."""


class EODHDNotFoundError(EODHDAPIError):
    """Raised when EODHD cannot resolve the requested ticker."""


def _raise_for_status(response: requests.Response, *, ticker: str) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        if response.status_code == 429:
            raise EODHDRateLimitError(f"EODHD rate limit for {ticker}: {exc}") from exc
        if response.status_code in (401, 403):
            raise EODHDAuthError(
                f"EODHD auth error {response.status_code} for {ticker}: {exc}"
            ) from exc
        if response.status_code == 404:
            raise EODHDNotFoundError(f"EODHD ticker not found: {ticker}") from exc
        raise EODHDAPIError(
            f"EODHD API error {response.status_code} for {ticker}: {exc}"
        ) from exc


@dataclass(frozen=True)
class EODHDDailyBar:
    """Normalized EODHD EOD row.

    EODHD's EOD endpoint does not include dividend / split data, so
    ``div_cash`` and ``split_factor`` are fixed at 0.0 and 1.0 and callers
    must treat the series as having missing corporate-action metadata.
    """

    ticker: str                     # full EODHD ticker e.g. "VWRL.LSE"
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


class EODHDClient:
    """Low-level HTTP client for https://eodhd.com/api/eod/<ticker>."""

    BASE_URL = "https://eodhd.com/api"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or get_env_value("EODHD_API_KEY")
        self.session = requests.Session()

    def get_daily_bars(
        self,
        ticker: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[EODHDDailyBar]:
        """Fetch daily bars for ``TICKER.EXCHANGE``.

        Returns an empty list if the API key is not configured or the
        endpoint yields an ``"Ticker Not Found."`` string. HTTP-level errors
        surface as ``EODHDAPIError`` subclasses.
        """
        if not self.api_key:
            logger.warning("EODHD_API_KEY not set; skipping bars for %s", ticker)
            return []
        params: dict[str, str] = {"api_token": self.api_key, "fmt": "json"}
        if start_date:
            params["from"] = start_date
        if end_date:
            params["to"] = end_date
        response = self.session.get(
            f"{self.BASE_URL}/eod/{ticker}",
            params=params,
            timeout=60,
        )
        _raise_for_status(response, ticker=ticker)

        # EODHD serves 200 OK with a plain-text "Ticker Not Found." body
        # (not JSON) for bad/delisted symbols. Inspect the raw text first
        # so response.json() never raises JSONDecodeError on that path.
        text_body = response.text if response.content else ""
        stripped = text_body.strip()
        if not stripped:
            return []
        if not stripped.startswith(("[", "{")):
            logger.info("EODHD reported %r for ticker %s", stripped[:120], ticker)
            return []
        try:
            payload = response.json()
        except ValueError:
            logger.warning(
                "EODHD returned non-JSON body for %s: %r", ticker, stripped[:120]
            )
            return []
        if not isinstance(payload, list):
            return []

        bars: list[EODHDDailyBar] = []
        for row in payload:
            date_str = str(row.get("date", ""))[:10]
            if not date_str:
                continue
            try:
                adj_close = _as_optional_float(row.get("adjusted_close"))
                bars.append(
                    EODHDDailyBar(
                        ticker=ticker,
                        date=date_str,
                        open=_as_float(row.get("open")),
                        high=_as_float(row.get("high")),
                        low=_as_float(row.get("low")),
                        close=_as_float(row.get("close")),
                        volume=_as_float(row.get("volume"), default=0.0),
                        adj_open=None,
                        adj_high=None,
                        adj_low=None,
                        adj_close=adj_close,
                        adj_volume=None,
                        div_cash=0.0,
                        split_factor=1.0,
                    )
                )
            except (TypeError, ValueError):
                logger.debug("EODHD row skipped for %s: %s", ticker, row)
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
