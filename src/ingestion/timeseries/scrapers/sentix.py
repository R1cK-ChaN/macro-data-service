"""sentix Economic Index API client.

The sentix Economic Index is proprietary sentix GmbH data. This client uses
the official sentix Data REST API for subscribed historical time series and
keeps credentials out of raw-snapshot audit metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import requests

from env import get_env_value


SENTIX_SOURCE_NAME = "sentix GmbH"
SENTIX_HOMEPAGE_URL = "https://www.sentix.de/"
SENTIX_API_BASE_URL = "https://api.sentix.de"
SENTIX_AUTH_PATH = "/v1/auth/token"
SENTIX_TIMESERIES_PATH = "/v1/data/timeseries"


@dataclass(frozen=True)
class SentixObservation:
    date: str
    value: float
    ticker: str


@dataclass(frozen=True)
class SentixSeriesRow:
    date: str
    value: float
    ticker: str


class SentixAuthError(RuntimeError):
    """Raised when sentix API credentials are required."""


class SentixAPIError(RuntimeError):
    """Raised when the sentix API request fails."""


class SentixClient:
    def __init__(
        self,
        *,
        client_id: str | None = None,
        client_secret: str | None = None,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token = access_token
        self.base_url = (base_url or get_env_value("SENTIX_API_BASE_URL") or SENTIX_API_BASE_URL).rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def get_all_series_with_raw(
        self,
        series_config: dict[str, dict[str, Any]],
        *,
        lookback_days: int = 365 * 3,
    ) -> dict[str, tuple[list[SentixObservation], dict, dict[str, str]]]:
        start_date = _lookback_start_date(lookback_days)
        ticker_rows = self._fetch_required_tickers(
            series_config,
            start_date=start_date,
            lookback_days=lookback_days,
        )
        result: dict[str, tuple[list[SentixObservation], dict, dict[str, str]]] = {}
        for cfg in series_config.values():
            observations, payload, params = self._series_payload(
                cfg,
                ticker_rows,
                start_date=start_date,
                lookback_days=lookback_days,
            )
            result[str(cfg["series_id"])] = (observations, payload, params)
        return result

    def get_series_with_raw(
        self,
        cfg: dict[str, Any],
        *,
        lookback_days: int = 365 * 3,
    ) -> tuple[list[SentixObservation], dict, dict[str, str]]:
        return self.get_all_series_with_raw(
            {"series": cfg},
            lookback_days=lookback_days,
        )[str(cfg["series_id"])]

    def fetch_ticker_rows(
        self,
        ticker: str,
        *,
        lookback_days: int = 365 * 3,
        start_date: str | None = None,
    ) -> list[SentixSeriesRow]:
        rows, _payload = self._request_ticker_rows(
            ticker,
            lookback_days=lookback_days,
            start_date=start_date,
        )
        return rows

    def _request_ticker_rows(
        self,
        ticker: str,
        *,
        lookback_days: int,
        start_date: str | None,
    ) -> tuple[list[SentixSeriesRow], Any]:
        if start_date is None:
            start_date = _lookback_start_date(lookback_days)
        params: dict[str, str] = {"ticker": ticker, "format": "json"}
        if start_date:
            params["start_date"] = start_date
        try:
            response = self.session.get(
                f"{self.base_url}{SENTIX_TIMESERIES_PATH}",
                headers={"Authorization": f"Bearer {self._resolve_access_token()}"},
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            response_obj = getattr(exc, "response", None)
            status = getattr(response_obj, "status_code", None)
            detail = f" status={status}" if status is not None else ""
            raise SentixAPIError(
                f"sentix timeseries API request failed{detail}"
            ) from None
        payload = response.json()
        return parse_sentix_timeseries(payload, ticker=ticker), payload

    def _fetch_required_tickers(
        self,
        series_config: dict[str, dict[str, Any]],
        *,
        start_date: str | None,
        lookback_days: int,
    ) -> dict[str, tuple[list[SentixSeriesRow], Any]]:
        required = sorted(
            {
                ticker
                for cfg in series_config.values()
                for ticker in _config_tickers(cfg)
            }
        )
        result: dict[str, tuple[list[SentixSeriesRow], Any]] = {}
        for ticker in required:
            rows, payload = self._request_ticker_rows(
                ticker,
                lookback_days=lookback_days,
                start_date=start_date,
            )
            result[ticker] = (rows, payload)
        return result

    def _resolve_access_token(self) -> str:
        if self._access_token:
            return self._access_token
        client_id = self.client_id or get_env_value(
            "SENTIX_CLIENT_ID",
            "SENTIX_API_CLIENT_ID",
        )
        client_secret = self.client_secret or get_env_value(
            "SENTIX_CLIENT_SECRET",
            "SENTIX_API_CLIENT_SECRET",
        )
        if not client_id or not client_secret:
            raise SentixAuthError(
                "SENTIX_CLIENT_ID and SENTIX_CLIENT_SECRET are required for sentix API access"
            )
        try:
            response = self.session.post(
                f"{self.base_url}{SENTIX_AUTH_PATH}",
                json={"client_id": client_id, "client_secret": client_secret},
                timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            response_obj = getattr(exc, "response", None)
            status = getattr(response_obj, "status_code", None)
            detail = f" status={status}" if status is not None else ""
            raise SentixAPIError(f"sentix auth API request failed{detail}") from None
        token = _extract_token(payload)
        if not token:
            raise SentixAuthError("sentix auth response did not include an access token")
        self._access_token = token
        return token

    def _series_payload(
        self,
        cfg: dict[str, Any],
        ticker_rows: dict[str, tuple[list[SentixSeriesRow], Any]],
        *,
        start_date: str | None,
        lookback_days: int,
    ) -> tuple[list[SentixObservation], dict, dict[str, str]]:
        tickers = _config_tickers(cfg)
        rows = _rows_for_config(cfg, ticker_rows)
        observations = [
            SentixObservation(date=row.date, value=row.value, ticker=row.ticker)
            for row in rows
        ]
        params = {
            "url": f"{self.base_url}{SENTIX_TIMESERIES_PATH}",
            "series_id": str(cfg["series_id"]),
            "tickers": ",".join(tickers),
            "lookback_days": str(lookback_days),
            "format": "json",
        }
        if start_date:
            params["start_date"] = start_date
        payload = {
            "source_url": SENTIX_HOMEPAGE_URL,
            "source_name": SENTIX_SOURCE_NAME,
            "api_path": SENTIX_TIMESERIES_PATH,
            "series_id": cfg["series_id"],
            "name": cfg["name"],
            "country": cfg.get("country", ""),
            "family": cfg.get("family", ""),
            "source_ticker": cfg.get("source_ticker", ""),
            "component_tickers": list(cfg.get("component_tickers", ())),
            "formula": cfg.get("formula", ""),
            "observations": [
                {"date": obs.date, "value": obs.value}
                for obs in observations
            ],
            "ticker_payloads": {
                ticker: ticker_rows.get(ticker, ([], []))[1]
                for ticker in tickers
            },
        }
        return observations, payload, params


def parse_sentix_timeseries(payload: Any, *, ticker: str) -> list[SentixSeriesRow]:
    rows_payload = _extract_rows(payload)
    rows: list[SentixSeriesRow] = []
    for item in rows_payload:
        parsed = _parse_row(item, ticker=ticker)
        if parsed is not None:
            rows.append(parsed)
    rows.sort(key=lambda row: row.date)
    return rows


def _extract_rows(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("data", "values", "observations", "timeseries", "series"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    nested = payload.get("response")
    if isinstance(nested, dict):
        return _extract_rows(nested)
    return []


def _parse_row(item: Any, *, ticker: str) -> SentixSeriesRow | None:
    if isinstance(item, dict):
        raw_date = (
            item.get("date")
            or item.get("Date")
            or item.get("datetime")
            or item.get("DateTime")
            or item.get("timestamp")
        )
        raw_value = (
            item.get("value")
            if "value" in item
            else item.get("Value")
            if "Value" in item
            else item.get("close")
        )
        raw_ticker = item.get("ticker") or item.get("Ticker") or ticker
    elif isinstance(item, (list, tuple)) and len(item) >= 2:
        raw_date = item[0]
        raw_value = item[1]
        raw_ticker = ticker
    else:
        return None
    if raw_date is None or raw_value is None:
        return None
    return SentixSeriesRow(
        date=_parse_date(str(raw_date)),
        value=float(str(raw_value).replace(",", ".")),
        ticker=str(raw_ticker or ticker),
    )


def _rows_for_config(
    cfg: dict[str, Any],
    ticker_rows: dict[str, tuple[list[SentixSeriesRow], Any]],
) -> list[SentixSeriesRow]:
    source_ticker = cfg.get("source_ticker")
    if source_ticker:
        return list(ticker_rows.get(str(source_ticker), ([], []))[0])
    components = tuple(str(item) for item in cfg.get("component_tickers", ()))
    if cfg.get("formula") == "average" and len(components) == 2:
        left = {row.date: row.value for row in ticker_rows.get(components[0], ([], []))[0]}
        right = {row.date: row.value for row in ticker_rows.get(components[1], ([], []))[0]}
        return [
            SentixSeriesRow(
                date=date_value,
                value=round((left[date_value] + right[date_value]) / 2.0, 10),
                ticker="+".join(components),
            )
            for date_value in sorted(set(left) & set(right))
        ]
    return []


def _config_tickers(cfg: dict[str, Any]) -> tuple[str, ...]:
    source_ticker = cfg.get("source_ticker")
    if source_ticker:
        return (str(source_ticker),)
    return tuple(str(item) for item in cfg.get("component_tickers", ()))


def _extract_token(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in ("access_token", "token", "jwt"):
        value = payload.get(key)
        if value:
            return str(value)
    nested = payload.get("data")
    if isinstance(nested, dict):
        return _extract_token(nested)
    return ""


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
