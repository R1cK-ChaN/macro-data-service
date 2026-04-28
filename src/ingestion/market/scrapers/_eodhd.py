"""EODHD EOD API client — global equities/ETFs/indices/FX/crypto/spot metals.

Endpoints wired here:

* ``/api/eod/{TICKER}.{EXCHANGE}`` — daily bars (open/high/low/close/volume +
  adjusted_close). Same endpoint covers equities, ETFs, indices, FX
  (``.FOREX``), crypto (``.CC``), and spot metals (``.FOREX``). Dividend
  / split fields are not on this endpoint — they live on the per-ticker
  endpoints below and are projected into ``market_price_bars`` by the
  market-corp-actions lane (issue #67 slice 2).
* ``/api/div/{TICKER}.{EXCHANGE}`` — full per-ticker dividend history
  (back to listing inception; AAPL goes to 1987). Returns a top-level
  array; rows carry ex-date / declarationDate / recordDate / paymentDate
  / period / value / unadjustedValue / currency.
* ``/api/splits/{TICKER}.{EXCHANGE}`` — full per-ticker split history.
  Top-level array of ``{date, split: "new/old"}``.

The two corp-action endpoints are NOT redundant with EODHD's
``/api/calendar/{dividends,splits}`` discovery feeds (wired separately
under ``ingestion.calendar.eodhd_api`` for the calendar lane). Calendar
endpoints index by date window with ~2015 floor; per-ticker endpoints
index by ticker and reach back to instrument inception. Same EODHD data,
different access shapes, different downstream consumers — see issue #67
slice-2 module docstring on ``EODHDMarketDataProvider.refresh_corp_actions``
and the ``market_corp_actions_raw`` table comment for the full
no-deduplicate-across-tables rationale.
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
    ``div_cash`` and ``split_factor`` are fixed at 0.0 and 1.0 here;
    real corp-action values are projected into ``market_price_bars``
    from the audit lane (``market_corp_actions_raw``) by issue #67
    slice 2's ``EODHDMarketDataProvider.refresh_corp_actions``.
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


@dataclass(frozen=True)
class EODHDDividend:
    """Normalized row from ``/api/div/{TICKER}.{EXCHANGE}``.

    Field names match the EODHD response keys (camelCase preserved on
    the ``unadjusted_value`` etc. only as snake_case for Python
    convention). ``period`` and the three corp-calendar dates are
    optional — EODHD returns ``null`` for older rows where they aren't
    known. ``value`` is the EODHD-adjusted dividend amount (post-split);
    ``unadjusted_value`` is the as-paid amount in nominal share terms.
    Both are kept because each downstream computation needs a different
    one (total-return uses adjusted; cash-flow on legacy share counts
    uses unadjusted).
    """

    ticker: str                          # full EODHD ticker e.g. "AAPL.US"
    date: str                            # ex-dividend date, YYYY-MM-DD
    declaration_date: str | None
    record_date: str | None
    payment_date: str | None
    period: str | None                   # e.g. "Quarterly"
    value: float                         # adjusted dividend (post-split)
    unadjusted_value: float | None       # as-paid amount
    currency: str                        # e.g. "USD"


@dataclass(frozen=True)
class EODHDSplit:
    """Normalized row from ``/api/splits/{TICKER}.{EXCHANGE}``.

    EODHD ships the ratio as a string ``"new/old"`` (e.g.
    ``"4.000000/1.000000"`` for the 2020-08-31 AAPL 4-for-1 split). The
    parser pulls both sides as floats so downstream can derive the
    canonical ``split_factor = new / old`` (so a 4:1 split lands as
    4.0 — the convention ``market_price_bars.split_factor`` expects).
    """

    ticker: str                     # full EODHD ticker e.g. "AAPL.US"
    date: str                       # split date, YYYY-MM-DD
    new_shares: float               # numerator of the ratio
    old_shares: float               # denominator of the ratio
    raw_ratio: str                  # original "new/old" string for audit


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

    def get_historical_dividends(
        self,
        ticker: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[EODHDDividend]:
        """Fetch full per-ticker dividend history from ``/api/div/{ticker}``.

        With no date bounds, returns every dividend EODHD knows back to
        instrument inception (90 rows for AAPL.US, 1987→present at probe
        time). Date bounds match the calendar-lane ``from`` / ``to``
        semantics — inclusive of both endpoints. Empty body or malformed
        response returns an empty list (mirrors :meth:`get_daily_bars`).

        See module docstring for the per-ticker vs calendar-lane
        distinction; the two endpoints are NOT redundant.
        """
        return self._fetch_corp_action_rows(
            path=f"div/{ticker}",
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            row_parser=self._parse_dividend_row,
        )

    def get_historical_splits(
        self,
        ticker: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[EODHDSplit]:
        """Fetch full per-ticker split history from ``/api/splits/{ticker}``.

        Same date-bound semantics as :meth:`get_historical_dividends`.
        """
        return self._fetch_corp_action_rows(
            path=f"splits/{ticker}",
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            row_parser=self._parse_split_row,
        )

    def _fetch_corp_action_rows(
        self,
        *,
        path: str,
        ticker: str,
        start_date: str | None,
        end_date: str | None,
        row_parser,
    ):
        if not self.api_key:
            logger.warning(
                "EODHD_API_KEY not set; skipping corp-action fetch for %s", ticker
            )
            return []
        params: dict[str, str] = {"api_token": self.api_key, "fmt": "json"}
        if start_date:
            params["from"] = start_date
        if end_date:
            params["to"] = end_date
        response = self.session.get(
            f"{self.BASE_URL}/{path}",
            params=params,
            timeout=60,
        )
        _raise_for_status(response, ticker=ticker)

        text_body = response.text if response.content else ""
        stripped = text_body.strip()
        if not stripped:
            return []
        if not stripped.startswith(("[", "{")):
            logger.info(
                "EODHD reported %r for corp-action %s on %s",
                stripped[:120], path, ticker,
            )
            return []
        try:
            payload = response.json()
        except ValueError:
            logger.warning(
                "EODHD returned non-JSON body for corp-action %s: %r",
                path, stripped[:120],
            )
            return []
        if not isinstance(payload, list):
            return []

        out = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            try:
                parsed = row_parser(ticker=ticker, row=row)
            except (TypeError, ValueError):
                logger.debug("EODHD corp-action row skipped for %s: %s", ticker, row)
                continue
            if parsed is not None:
                out.append(parsed)
        out.sort(key=lambda r: r.date)
        return out

    @staticmethod
    def _parse_dividend_row(*, ticker: str, row: dict) -> EODHDDividend | None:
        date_str = str(row.get("date", ""))[:10]
        if not date_str:
            return None
        return EODHDDividend(
            ticker=ticker,
            date=date_str,
            declaration_date=_as_optional_date(row.get("declarationDate")),
            record_date=_as_optional_date(row.get("recordDate")),
            payment_date=_as_optional_date(row.get("paymentDate")),
            period=_as_optional_str(row.get("period")),
            value=_as_float(row.get("value")),
            unadjusted_value=_as_optional_float(row.get("unadjustedValue")),
            currency=str(row.get("currency") or ""),
        )

    @staticmethod
    def _parse_split_row(*, ticker: str, row: dict) -> EODHDSplit | None:
        date_str = str(row.get("date", ""))[:10]
        if not date_str:
            return None
        ratio = str(row.get("split", "")).strip()
        if "/" not in ratio:
            return None
        new_part, _, old_part = ratio.partition("/")
        new_shares = _as_float(new_part)
        old_shares = _as_float(old_part)
        if old_shares == 0:
            return None
        return EODHDSplit(
            ticker=ticker,
            date=date_str,
            new_shares=new_shares,
            old_shares=old_shares,
            raw_ratio=ratio,
        )


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


def _as_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_optional_date(value: object) -> str | None:
    """EODHD ships ``"0000-00-00"`` for unknown dates on some old rows.
    Treat those as absent so downstream logic doesn't store a bogus
    sentinel that doesn't sort correctly with real dates."""
    text = _as_optional_str(value)
    if text is None:
        return None
    if text.startswith("0000"):
        return None
    return text[:10]
