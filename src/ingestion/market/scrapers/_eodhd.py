"""EODHD market-data HTTP client.

Endpoints wired here:

* ``/api/exchanges-list/`` and ``/api/exchange-symbol-list/{EXCHANGE}``
  seed the instrument universe.
* ``/api/eod/{TICKER}.{EXCHANGE}`` returns daily bars with
  ``adjusted_close``.
* ``/api/div/{TICKER}.{EXCHANGE}`` — full per-ticker dividend history
  with ex-date / declarationDate / recordDate / paymentDate / period /
  value / unadjustedValue / currency.
* ``/api/splits/{TICKER}.{EXCHANGE}`` — full per-ticker split history.
  Top-level array of ``{date, split: "new/old"}``.
* ``/api/eod-bulk-last-day/{EXCHANGE}`` powers daily bars, dividends,
  and splits refreshes.
* ``/api/real-time/{TICKER}.{EXCHANGE}`` powers spot checks.
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

    The EOD endpoint carries price/volume fields. Dividends and splits
    come from their dedicated endpoints.
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


def _parse_eodhd_bars(
    payload: list, *, ticker: str,
) -> list["EODHDDailyBar"]:
    """Parse an ``/api/eod`` array into typed ``EODHDDailyBar`` rows.

    Standalone helper shared by per-ticker and maintenance paths.
    """
    bars: list[EODHDDailyBar] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
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


@dataclass(frozen=True)
class EODHDExchange:
    code: str
    name: str
    country: str
    currency: str


@dataclass(frozen=True)
class EODHDSymbol:
    code: str
    name: str
    exchange: str
    currency: str
    type: str
    isin: str
    figi: str
    composite_figi: str
    list_date: str | None
    raw: dict


@dataclass(frozen=True)
class EODHDRealTimeQuote:
    ticker: str
    close: float
    timestamp: int | None
    raw: dict


class EODHDClient:
    """Low-level HTTP client for https://eodhd.com/api/eod/<ticker>."""

    BASE_URL = "https://eodhd.com/api"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or get_env_value("EODHD_API_KEY")
        self.session = requests.Session()

    def list_exchanges(self) -> list[EODHDExchange]:
        """Fetch the EODHD exchange registry."""
        payload = self._get_json_array("exchanges-list/", ticker="EXCHANGES")
        out: list[EODHDExchange] = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            code = _first_text(row, "Code", "code", "Exchange", "exchange")
            if not code:
                continue
            out.append(
                EODHDExchange(
                    code=code,
                    name=_first_text(row, "Name", "name"),
                    country=_first_text(row, "Country", "country"),
                    currency=_first_text(row, "Currency", "currency"),
                )
            )
        return out

    def list_symbols_active(self, exchange: str = "US") -> list[EODHDSymbol]:
        """Fetch active symbols for an EODHD exchange."""
        return self._list_exchange_symbols(exchange, include_delisted=False)

    def list_symbols_with_delisted(self, exchange: str = "US") -> list[EODHDSymbol]:
        """Fetch active plus delisted symbols for an EODHD exchange."""
        return self._list_exchange_symbols(exchange, include_delisted=True)

    def _list_exchange_symbols(
        self, exchange: str, *, include_delisted: bool
    ) -> list[EODHDSymbol]:
        params = {"delisted": "1"} if include_delisted else None
        payload = self._get_json_array(
            f"exchange-symbol-list/{exchange}",
            ticker=f"SYMBOLS:{exchange}",
            extra_params=params,
        )
        out: list[EODHDSymbol] = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            code = _first_text(row, "Code", "code")
            if not code:
                continue
            out.append(
                EODHDSymbol(
                    code=code,
                    name=_first_text(row, "Name", "name"),
                    exchange=(
                        _first_text(
                            row,
                            "Exchange",
                            "exchange",
                            "ExchangeCode",
                            "exchange_code",
                            "exchange_short_name",
                        )
                        or exchange
                    ),
                    currency=_first_text(row, "Currency", "currency"),
                    type=_first_text(row, "Type", "type"),
                    isin=_first_text(row, "Isin", "ISIN", "isin"),
                    figi=_first_text(row, "FIGI", "Figi", "figi"),
                    composite_figi=_first_text(
                        row,
                        "CompositeFIGI",
                        "CompositeFigi",
                        "composite_figi",
                    ),
                    list_date=_as_optional_date(
                        row.get("ListingDate")
                        or row.get("listing_date")
                        or row.get("ListDate")
                    ),
                    raw=row,
                )
            )
        return out

    def get_realtime_quote(self, ticker: str) -> EODHDRealTimeQuote | None:
        """Fetch the latest realtime close for a ticker."""
        payload = self._get_json(
            f"real-time/{ticker}",
            ticker=ticker,
            extra_params={"fmt": "json"},
            timeout=30,
        )
        if not isinstance(payload, dict):
            return None
        close = _as_optional_float(
            payload.get("close")
            or payload.get("close_price")
            or payload.get("previousClose")
        )
        if close is None:
            return None
        timestamp_value = payload.get("timestamp") or payload.get("gmtoffset")
        try:
            timestamp = int(timestamp_value) if timestamp_value is not None else None
        except (TypeError, ValueError):
            timestamp = None
        return EODHDRealTimeQuote(ticker=ticker, close=close, timestamp=timestamp, raw=payload)

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
        bars, _payload, _params = self.get_daily_bars_with_raw(
            ticker, start_date=start_date, end_date=end_date,
        )
        return bars

    def get_daily_bars_with_raw(
        self,
        ticker: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> tuple[list[EODHDDailyBar], list, dict[str, str]]:
        """Fetch daily bars and return parsed bars + raw payload + request params.

        A ticker-not-found response yields ``([], [], params)`` so the
        caller can skip the write path naturally.
        """
        request_params: dict[str, str] = {"fmt": "json"}
        if start_date:
            request_params["from"] = start_date
        if end_date:
            request_params["to"] = end_date
        if not self.api_key:
            logger.warning("EODHD_API_KEY not set; skipping bars for %s", ticker)
            return [], [], request_params
        params = {"api_token": self.api_key, **request_params}
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
            return [], [], request_params
        if not stripped.startswith(("[", "{")):
            logger.info("EODHD reported %r for ticker %s", stripped[:120], ticker)
            return [], [], request_params
        try:
            payload = response.json()
        except ValueError:
            logger.warning(
                "EODHD returned non-JSON body for %s: %r", ticker, stripped[:120]
            )
            return [], [], request_params
        if not isinstance(payload, list):
            return [], [], request_params

        bars = _parse_eodhd_bars(payload, ticker=ticker)
        return bars, payload, request_params

    def get_bulk_last_day(
        self,
        exchange: str,
        *,
        date: str | None = None,
    ) -> list[EODHDDailyBar]:
        """Fetch every actively traded symbol on ``exchange`` for one day.

        Wraps ``/api/eod-bulk-last-day/{exchange}``. Without ``date``,
        returns the most recent trading day. Single request returns ≥4k
        rows for ``US``; the ~75% quota saving over per-ticker EOD is the
        whole point of using this for backfill.

        Each bulk row carries ``code`` + ``exchange_short_name`` instead
        of being keyed on the URL path; the parser builds the canonical
        ``TICKER.EXCHANGE`` form for the returned ``EODHDDailyBar.ticker``
        field so downstream code can route bars through the same universe
        machinery used by :meth:`get_daily_bars`.
        """
        if not self.api_key:
            logger.warning(
                "EODHD_API_KEY not set; skipping bulk-EOD for %s", exchange
            )
            return []
        params: dict[str, str] = {"api_token": self.api_key, "fmt": "json"}
        if date:
            params["date"] = date
        response = self.session.get(
            f"{self.BASE_URL}/eod-bulk-last-day/{exchange}",
            params=params,
            timeout=120,
        )
        _raise_for_status(response, ticker=f"BULK:{exchange}")

        text_body = response.text if response.content else ""
        stripped = text_body.strip()
        if not stripped:
            return []
        if not stripped.startswith(("[", "{")):
            logger.info("EODHD reported %r for bulk-EOD %s", stripped[:120], exchange)
            return []
        try:
            payload = response.json()
        except ValueError:
            logger.warning(
                "EODHD returned non-JSON body for bulk-EOD %s: %r",
                exchange, stripped[:120],
            )
            return []
        if not isinstance(payload, list):
            return []

        bars: list[EODHDDailyBar] = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            date_str = str(row.get("date", ""))[:10]
            code = str(row.get("code", "") or "").strip()
            ex = str(row.get("exchange_short_name", "") or "").strip()
            if not date_str or not code:
                continue
            full_ticker = f"{code}.{ex}" if ex else code
            try:
                adj_close = _as_optional_float(row.get("adjusted_close"))
                bars.append(
                    EODHDDailyBar(
                        ticker=full_ticker,
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
                logger.debug("EODHD bulk row skipped for %s: %s", full_ticker, row)
                continue
        return bars

    def get_bulk_dividends(
        self,
        exchange: str,
        *,
        date: str | None = None,
    ) -> list[EODHDDividend]:
        """Fetch bulk dividend rows for one exchange/day."""
        payload = self._get_bulk_payload(exchange, date=date, kind="dividends")
        out: list[EODHDDividend] = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            code = str(row.get("code", "") or "").strip()
            ex = str(row.get("exchange_short_name", "") or exchange).strip()
            ticker = f"{code}.{ex}" if code and ex else code
            try:
                parsed = self._parse_dividend_row(ticker=ticker, row=row)
            except (TypeError, ValueError):
                logger.debug("EODHD bulk dividend row skipped for %s: %s", ticker, row)
                continue
            if parsed is not None:
                out.append(parsed)
        out.sort(key=lambda r: (r.ticker, r.date))
        return out

    def get_bulk_splits(
        self,
        exchange: str,
        *,
        date: str | None = None,
    ) -> list[EODHDSplit]:
        """Fetch bulk split rows for one exchange/day."""
        payload = self._get_bulk_payload(exchange, date=date, kind="splits")
        out: list[EODHDSplit] = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            code = str(row.get("code", "") or "").strip()
            ex = str(row.get("exchange_short_name", "") or exchange).strip()
            ticker = f"{code}.{ex}" if code and ex else code
            try:
                parsed = self._parse_split_row(ticker=ticker, row=row)
            except (TypeError, ValueError):
                logger.debug("EODHD bulk split row skipped for %s: %s", ticker, row)
                continue
            if parsed is not None:
                out.append(parsed)
        out.sort(key=lambda r: (r.ticker, r.date))
        return out

    def _get_bulk_payload(
        self, exchange: str, *, date: str | None, kind: str
    ) -> list:
        params: dict[str, str] = {"type": kind}
        if date:
            params["date"] = date
        return self._get_json_array(
            f"eod-bulk-last-day/{exchange}",
            ticker=f"BULK:{exchange}:{kind}",
            extra_params=params,
            timeout=120,
        )

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
        value = row.get("value")
        if value in (None, ""):
            value = row.get("dividend")
        return EODHDDividend(
            ticker=ticker,
            date=date_str,
            declaration_date=_as_optional_date(row.get("declarationDate")),
            record_date=_as_optional_date(row.get("recordDate")),
            payment_date=_as_optional_date(row.get("paymentDate")),
            period=_as_optional_str(row.get("period")),
            value=_as_float(value),
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

    def _get_json_array(
        self,
        path: str,
        *,
        ticker: str,
        extra_params: dict[str, str] | None = None,
        timeout: int = 60,
    ) -> list:
        payload = self._get_json(
            path,
            ticker=ticker,
            extra_params=extra_params,
            timeout=timeout,
        )
        return payload if isinstance(payload, list) else []

    def _get_json(
        self,
        path: str,
        *,
        ticker: str,
        extra_params: dict[str, str] | None = None,
        timeout: int = 60,
    ) -> object:
        if not self.api_key:
            logger.warning("EODHD_API_KEY not set; skipping %s", path)
            return []
        params: dict[str, str] = {"api_token": self.api_key, "fmt": "json"}
        if extra_params:
            params.update(extra_params)
        response = self.session.get(
            f"{self.BASE_URL}/{path}",
            params=params,
            timeout=timeout,
        )
        _raise_for_status(response, ticker=ticker)
        text_body = response.text if response.content else ""
        stripped = text_body.strip()
        if not stripped:
            return []
        if not stripped.startswith(("[", "{")):
            logger.info("EODHD reported %r for %s", stripped[:120], path)
            return []
        try:
            return response.json()
        except ValueError:
            logger.warning("EODHD returned non-JSON body for %s: %r", path, stripped[:120])
            return []


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


def _first_text(row: dict, *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""
