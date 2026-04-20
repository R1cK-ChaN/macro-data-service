"""EODHD identity + delisted + ticker-history endpoints.

Covers the four endpoints referenced in issue #1 for identity enrichment
and lazy history repair:

* ``/api/search/{query}`` — ticker / ISIN / name search (ID Mapping path)
* ``/api/exchange-symbol-list/{EX}`` — current + ``?delisted=1`` feed
* ``/api/symbol-change-history`` — ticker renames, for segment stitching
* ``/api/exchanges-list`` — available exchange codes + MICs
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import requests

from env import get_env_value
from ingestion.market.scrapers._eodhd import (
    EODHDAPIError,
    _raise_for_status,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EODHDSearchHit:
    code: str                    # e.g. "SPY"
    exchange: str                # e.g. "US"
    name: str
    type: str                    # "ETF" | "Common Stock" | "FUND" | ...
    country: str
    currency: str
    isin: str                    # may be empty


@dataclass(frozen=True)
class EODHDSymbolListEntry:
    code: str
    name: str
    country: str
    exchange: str
    currency: str
    type: str
    isin: str                    # may be empty


@dataclass(frozen=True)
class EODHDSymbolChangeEvent:
    exchange: str
    old_symbol: str
    new_symbol: str
    company_name: str
    effective: str               # YYYY-MM-DD


@dataclass(frozen=True)
class EODHDExchange:
    code: str
    name: str
    country: str
    currency: str
    operating_mic: str


class EODHDIdentityClient:
    """Wrapper around EODHD identity / universe endpoints."""

    BASE_URL = "https://eodhd.com/api"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or get_env_value("EODHD_API_KEY")
        self.session = requests.Session()

    def _get(self, path: str, *, params: dict | None = None, ticker: str = "") -> object:
        if not self.api_key:
            logger.warning("EODHD_API_KEY not set; skipping %s", path)
            return []
        full_params = dict(params or {})
        full_params.setdefault("api_token", self.api_key)
        full_params.setdefault("fmt", "json")
        response = self.session.get(
            f"{self.BASE_URL}/{path.lstrip('/')}",
            params=full_params,
            timeout=60,
        )
        _raise_for_status(response, ticker=ticker or path)
        text = response.text.strip() if response.content else ""
        if not text or not text.startswith(("[", "{")):
            logger.info("EODHD returned non-JSON body for %s: %r", path, text[:120])
            return []
        try:
            return response.json()
        except ValueError:
            logger.warning("EODHD JSON decode failed for %s", path)
            return []

    # -- search (identity / ISIN lookup) ----------------------------------

    def search(self, query: str, *, limit: int = 15) -> list[EODHDSearchHit]:
        payload = self._get(f"search/{query}", params={"limit": str(limit)}, ticker=query)
        hits: list[EODHDSearchHit] = []
        if not isinstance(payload, list):
            return hits
        for row in payload:
            try:
                hits.append(
                    EODHDSearchHit(
                        code=str(row.get("Code", "")),
                        exchange=str(row.get("Exchange", "")),
                        name=str(row.get("Name", "")),
                        type=str(row.get("Type", "")),
                        country=str(row.get("Country", "")),
                        currency=str(row.get("Currency", "")),
                        isin=str(row.get("ISIN") or ""),
                    )
                )
            except (TypeError, ValueError):
                continue
        return hits

    # -- exchange symbol list (delisted=1 for history repair) -------------

    def list_exchange_symbols(
        self, exchange: str, *, delisted: bool = False
    ) -> list[EODHDSymbolListEntry]:
        params: dict[str, str] = {}
        if delisted:
            params["delisted"] = "1"
        payload = self._get(f"exchange-symbol-list/{exchange}", params=params, ticker=exchange)
        out: list[EODHDSymbolListEntry] = []
        if not isinstance(payload, list):
            return out
        for row in payload:
            try:
                out.append(
                    EODHDSymbolListEntry(
                        code=str(row.get("Code", "")),
                        name=str(row.get("Name", "")),
                        country=str(row.get("Country", "")),
                        exchange=str(row.get("Exchange", "")),
                        currency=str(row.get("Currency", "")),
                        type=str(row.get("Type", "")),
                        isin=str(row.get("Isin") or row.get("ISIN") or ""),
                    )
                )
            except (TypeError, ValueError):
                continue
        return out

    # -- symbol change history -------------------------------------------

    def symbol_change_history(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[EODHDSymbolChangeEvent]:
        params: dict[str, str] = {}
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        payload = self._get("symbol-change-history/", params=params, ticker="symbol-change-history")
        events: list[EODHDSymbolChangeEvent] = []
        if not isinstance(payload, list):
            return events
        for row in payload:
            try:
                events.append(
                    EODHDSymbolChangeEvent(
                        exchange=str(row.get("exchange", "")),
                        old_symbol=str(row.get("old_symbol", "")),
                        new_symbol=str(row.get("new_symbol", "")),
                        company_name=str(row.get("company_name", "")),
                        effective=str(row.get("effective", "")),
                    )
                )
            except (TypeError, ValueError):
                continue
        return events

    # -- covered exchanges -----------------------------------------------

    def list_exchanges(self) -> list[EODHDExchange]:
        payload = self._get("exchanges-list/", ticker="exchanges-list")
        exchanges: list[EODHDExchange] = []
        if not isinstance(payload, list):
            return exchanges
        for row in payload:
            try:
                exchanges.append(
                    EODHDExchange(
                        code=str(row.get("Code", "")),
                        name=str(row.get("Name", "")),
                        country=str(row.get("Country", "")),
                        currency=str(row.get("Currency", "")),
                        operating_mic=str(row.get("OperatingMIC", "")),
                    )
                )
            except (TypeError, ValueError):
                continue
        return exchanges


__all__ = [
    "EODHDIdentityClient",
    "EODHDSearchHit",
    "EODHDSymbolListEntry",
    "EODHDSymbolChangeEvent",
    "EODHDExchange",
    "EODHDAPIError",
]
