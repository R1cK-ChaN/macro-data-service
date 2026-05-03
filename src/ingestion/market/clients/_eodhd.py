"""EODHD market-data provider backed by ClickHouse."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

from ingestion.market.scrapers._eodhd import (
    EODHDAPIError,
    EODHDClient,
    EODHDDailyBar,
    EODHDDividend,
    EODHDNotFoundError,
    EODHDSplit,
    EODHDSymbol,
)
from storage.clickhouse.records import CHBar, CHDividend, CHInstrument, CHSplit
from storage.clickhouse.store import (
    ClickHouseMarketStore,
    compute_dividend_hash,
    compute_split_hash,
)

logger = logging.getLogger(__name__)

SUPPORTED_US_SYMBOL_TYPES = frozenset({"COMMON STOCK", "ETF", "INDEX"})
_ASSET_CLASS_BY_EODHD_TYPE = {
    "COMMON STOCK": "equity",
    "ETF": "equity_etf",
    "INDEX": "index",
}
_US_CLOSE_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class RefreshStats:
    source: str
    count: int = 0
    instruments: int = 0
    bars: int = 0
    dividends: int = 0
    splits: int = 0
    corp_actions_changed: int = 0


class EODHDMarketDataProvider:
    """High-level EODHD provider for the US daily market lane."""

    source_name = "eodhd"

    def __init__(self, client: EODHDClient | None = None) -> None:
        self.client = client or EODHDClient()

    def seed_us_universe(
        self,
        store: ClickHouseMarketStore,
        *,
        exchange: str = "US",
        last_seen: datetime | None = None,
    ) -> RefreshStats:
        """Discover active plus delisted US symbols and write instruments."""
        active = self.client.list_symbols_active(exchange)
        with_delisted = self.client.list_symbols_with_delisted(exchange)
        instruments = build_us_instruments(
            active_symbols=active,
            active_plus_delisted_symbols=with_delisted,
            exchange=exchange,
            last_seen=last_seen,
        )
        count = store.upsert_market_instruments(instruments)
        return RefreshStats(source=self.source_name, count=count, instruments=count)

    def refresh_market_history(
        self,
        store: ClickHouseMarketStore,
        symbol: str,
        *,
        start: str | None = None,
        end: str | None = None,
        include_corp_actions: bool = True,
    ) -> RefreshStats:
        """Fetch one instrument's lifetime bars and corporate actions."""
        instrument = self._resolve_instrument(store, symbol)
        if instrument is None:
            logger.warning("EODHD symbol is absent from market.instruments: %s", symbol)
            return RefreshStats(source=self.source_name)
        return self.refresh_instrument_history(
            store,
            instrument,
            start=start,
            end=end,
            include_corp_actions=include_corp_actions,
        )

    def refresh_instrument_history(
        self,
        store: ClickHouseMarketStore,
        instrument: dict[str, Any],
        *,
        start: str | None = None,
        end: str | None = None,
        include_corp_actions: bool = True,
    ) -> RefreshStats:
        ticker = _eodhd_ticker_for_instrument(instrument)
        fetched_at = datetime.now(timezone.utc)
        try:
            bars = self.client.get_daily_bars(
                ticker,
                start_date=start,
                end_date=end,
            )
        except EODHDNotFoundError:
            logger.warning("EODHD ticker missing during history fetch: %s", ticker)
            return RefreshStats(source=self.source_name)
        except EODHDAPIError:
            logger.warning("EODHD bars fetch failed for %s", ticker, exc_info=True)
            return RefreshStats(source=self.source_name)

        ch_bars = [
            _bar_to_ch(instrument=instrument, bar=bar, fetched_at=fetched_at)
            for bar in bars
        ]
        bars_written = store.upsert_market_bars(ch_bars)

        div_written = 0
        split_written = 0
        if include_corp_actions:
            dividends = self.client.get_historical_dividends(ticker)
            splits = self.client.get_historical_splits(ticker)
            ch_divs = [
                _dividend_to_ch(instrument=instrument, dividend=div, fetched_at=fetched_at)
                for div in dividends
            ]
            ch_splits = [
                _split_to_ch(instrument=instrument, split=split, fetched_at=fetched_at)
                for split in splits
            ]
            div_written, split_written = store.upsert_corp_actions(
                dividends=ch_divs,
                splits=ch_splits,
            )

        total = bars_written + div_written + split_written
        return RefreshStats(
            source=self.source_name,
            count=total,
            bars=bars_written,
            dividends=div_written,
            splits=split_written,
        )

    def refresh_daily_bulk(
        self,
        store: ClickHouseMarketStore,
        *,
        date: str | None = None,
        exchange: str = "US",
        refetch_changed_corp_actions: bool = True,
    ) -> RefreshStats:
        """Run the daily US bulk refresh and refill bars on new corp actions."""
        fetched_at = datetime.now(timezone.utc)
        instruments = store.list_instruments(active_only=True)
        by_ticker = {str(row["ticker"]).upper(): row for row in instruments}
        by_id = {str(row["instrument_id"]): row for row in instruments}

        bars = self.client.get_bulk_last_day(exchange, date=date)
        ch_bars: list[CHBar] = []
        for bar in bars:
            instrument = by_ticker.get(_provider_code(bar.ticker).upper())
            if instrument is None:
                continue
            ch_bars.append(_bar_to_ch(instrument=instrument, bar=bar, fetched_at=fetched_at))
        bars_written = store.upsert_market_bars(ch_bars)

        changed_instruments: set[str] = set()
        ch_dividends: list[CHDividend] = []
        for dividend in self.client.get_bulk_dividends(exchange, date=date):
            instrument = by_ticker.get(_provider_code(dividend.ticker).upper())
            if instrument is None:
                continue
            ch_div = _dividend_to_ch(
                instrument=instrument,
                dividend=dividend,
                fetched_at=fetched_at,
            )
            is_new = not store.has_dividend_hash(
                instrument_id=ch_div.instrument_id,
                ex_date=ch_div.ex_date,
                content_hash=ch_div.content_hash,
            )
            if is_new:
                changed_instruments.add(ch_div.instrument_id)
            ch_dividends.append(ch_div)

        ch_splits: list[CHSplit] = []
        for split in self.client.get_bulk_splits(exchange, date=date):
            instrument = by_ticker.get(_provider_code(split.ticker).upper())
            if instrument is None:
                continue
            ch_split = _split_to_ch(
                instrument=instrument,
                split=split,
                fetched_at=fetched_at,
            )
            is_new = not store.has_split_hash(
                instrument_id=ch_split.instrument_id,
                execution_date=ch_split.execution_date,
                content_hash=ch_split.content_hash,
            )
            if is_new:
                changed_instruments.add(ch_split.instrument_id)
            ch_splits.append(ch_split)

        refetched_bars = 0
        if refetch_changed_corp_actions:
            for instrument_id in sorted(changed_instruments):
                instrument = by_id.get(instrument_id)
                if instrument is None:
                    continue
                ticker = _eodhd_ticker_for_instrument(instrument)
                try:
                    replacement_bars = self.client.get_daily_bars(ticker)
                except EODHDAPIError as exc:
                    raise EODHDAPIError(
                        f"corp-action refill failed for {ticker}"
                    ) from exc
                if not replacement_bars:
                    raise EODHDAPIError(
                        f"corp-action refill returned zero bars for {ticker}"
                    )
                replacement_rows = [
                    _bar_to_ch(instrument=instrument, bar=bar, fetched_at=fetched_at)
                    for bar in replacement_bars
                ]
                store.delete_bars_for_instrument(instrument_id)
                refetched_bars += store.upsert_market_bars(replacement_rows)

        div_written, split_written = store.upsert_corp_actions(
            dividends=ch_dividends,
            splits=ch_splits,
        )

        total = bars_written + div_written + split_written + refetched_bars
        return RefreshStats(
            source=self.source_name,
            count=total,
            bars=bars_written + refetched_bars,
            dividends=div_written,
            splits=split_written,
            corp_actions_changed=len(changed_instruments),
        )

    def _resolve_instrument(
        self, store: ClickHouseMarketStore, symbol: str
    ) -> dict[str, Any] | None:
        clean = symbol.strip()
        if not clean:
            return None
        instrument = store.lookup_instrument(instrument_id=clean)
        if instrument is not None:
            return instrument
        return store.lookup_instrument(ticker=_provider_code(clean))


def build_us_instruments(
    *,
    active_symbols: list[EODHDSymbol],
    active_plus_delisted_symbols: list[EODHDSymbol],
    exchange: str = "US",
    last_seen: datetime | None = None,
) -> list[CHInstrument]:
    """Project EODHD symbol-list rows into ``market.instruments``."""
    seen_at = last_seen or datetime.now(timezone.utc)
    active_codes = {_symbol_key(row) for row in active_symbols}
    by_code: dict[str, EODHDSymbol] = {
        _symbol_key(row): row for row in active_plus_delisted_symbols
    }
    if not by_code:
        by_code = {_symbol_key(row): row for row in active_symbols}

    instruments: list[CHInstrument] = []
    for code, row in sorted(by_code.items()):
        type_key = row.type.strip().upper()
        if type_key not in SUPPORTED_US_SYMBOL_TYPES:
            continue
        instruments.append(
            CHInstrument(
                instrument_id=_us_instrument_id(row.code),
                isin=row.isin,
                figi=row.figi,
                composite_figi=row.composite_figi,
                ticker=row.code,
                exchange=row.exchange or "US",
                asset_class=_ASSET_CLASS_BY_EODHD_TYPE[type_key],
                currency=row.currency or "USD",
                name=row.name,
                list_date=_date_or_none(row.list_date),
                is_active=code in active_codes,
                last_seen=seen_at,
                metadata=json.dumps(
                    {
                        "provider": "eodhd",
                        "eodhd_code": row.code,
                        "eodhd_exchange": exchange,
                        "eodhd_type": row.type,
                        "source_exchange": row.exchange,
                        "raw": row.raw,
                    },
                    sort_keys=True,
                    ensure_ascii=False,
                ),
            )
        )
    return instruments


def _bar_to_ch(
    *,
    instrument: dict[str, Any],
    bar: EODHDDailyBar,
    fetched_at: datetime,
) -> CHBar:
    adjusted_close = bar.adj_close if bar.adj_close is not None else bar.close
    if bar.close and adjusted_close and adjusted_close > 0:
        factor = adjusted_close / bar.close
    else:
        factor = 1.0
        adjusted_close = bar.close
    adjusted_volume = bar.volume / factor if factor else bar.volume
    return CHBar(
        instrument_id=str(instrument["instrument_id"]),
        ticker=str(instrument["ticker"]),
        exchange=str(instrument.get("exchange") or "US"),
        time=_us_close_time(bar.date),
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        adjusted_open=bar.open * factor,
        adjusted_high=bar.high * factor,
        adjusted_low=bar.low * factor,
        adjusted_close=adjusted_close,
        adjusted_volume=adjusted_volume,
        fetched_at=fetched_at,
    )


def _dividend_to_ch(
    *,
    instrument: dict[str, Any],
    dividend: EODHDDividend,
    fetched_at: datetime,
) -> CHDividend:
    ex_date = Date.fromisoformat(dividend.date)
    declaration_date = _date_or_none(dividend.declaration_date)
    record_date = _date_or_none(dividend.record_date)
    payment_date = _date_or_none(dividend.payment_date)
    unadjusted = (
        dividend.unadjusted_value
        if dividend.unadjusted_value is not None
        else dividend.value
    )
    currency = dividend.currency or str(instrument.get("currency") or "")
    content_hash = compute_dividend_hash(
        instrument_id=str(instrument["instrument_id"]),
        ex_date=ex_date,
        cash_amount=dividend.value,
        unadjusted_amount=unadjusted,
        currency=currency,
        declaration_date=declaration_date,
        record_date=record_date,
        payment_date=payment_date,
        period=dividend.period or "",
    )
    return CHDividend(
        instrument_id=str(instrument["instrument_id"]),
        ticker=str(instrument["ticker"]),
        ex_date=ex_date,
        declaration_date=declaration_date,
        record_date=record_date,
        payment_date=payment_date,
        period=dividend.period or "",
        cash_amount=dividend.value,
        unadjusted_amount=unadjusted,
        currency=currency,
        fetched_at=fetched_at,
        content_hash=content_hash,
    )


def _split_to_ch(
    *,
    instrument: dict[str, Any],
    split: EODHDSplit,
    fetched_at: datetime,
) -> CHSplit:
    execution_date = Date.fromisoformat(split.date)
    content_hash = compute_split_hash(
        instrument_id=str(instrument["instrument_id"]),
        execution_date=execution_date,
        to_factor=split.new_shares,
        from_factor=split.old_shares,
    )
    return CHSplit(
        instrument_id=str(instrument["instrument_id"]),
        ticker=str(instrument["ticker"]),
        execution_date=execution_date,
        to_factor=split.new_shares,
        from_factor=split.old_shares,
        fetched_at=fetched_at,
        content_hash=content_hash,
    )


def _eodhd_ticker_for_instrument(instrument: dict[str, Any]) -> str:
    metadata = _metadata(instrument)
    code = str(metadata.get("eodhd_code") or instrument.get("ticker") or "").strip()
    exchange = str(metadata.get("eodhd_exchange") or "US").strip()
    if "." in code:
        return code
    return f"{code}.{exchange}"


def _metadata(instrument: dict[str, Any]) -> dict[str, Any]:
    raw = instrument.get("metadata") or "{}"
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _provider_code(ticker: str) -> str:
    return str(ticker).strip().split(".", 1)[0]


def _symbol_key(row: EODHDSymbol) -> str:
    return row.code.strip().upper()


def _us_instrument_id(code: str) -> str:
    clean = re.sub(r"[^A-Z0-9]+", "_", code.upper()).strip("_")
    return f"US_{clean}"


def _date_or_none(value: str | None) -> Date | None:
    if not value:
        return None
    return Date.fromisoformat(value[:10])


def _us_close_time(date_str: str) -> datetime:
    session_date = Date.fromisoformat(date_str[:10])
    local_close = datetime.combine(session_date, time(16, 0), tzinfo=_US_CLOSE_TZ)
    return local_close.astimezone(timezone.utc)


__all__ = [
    "EODHDMarketDataProvider",
    "RefreshStats",
    "SUPPORTED_US_SYMBOL_TYPES",
    "build_us_instruments",
]
