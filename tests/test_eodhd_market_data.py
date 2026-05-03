from __future__ import annotations

import sys
import json
from datetime import date as Date
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.market.clients._eodhd import (
    EODHDMarketDataProvider,
    _bar_to_ch,
    build_us_instruments,
)
from ingestion.market.scrapers._eodhd import (
    EODHDAPIError,
    EODHDClient,
    EODHDDailyBar,
    EODHDDividend,
    EODHDSymbol,
)


def _symbol(code: str, *, type_: str = "Common Stock") -> EODHDSymbol:
    return EODHDSymbol(
        code=code,
        name=f"{code} Inc.",
        exchange="NASDAQ",
        currency="USD",
        type=type_,
        isin=f"ISIN{code}",
        figi=f"FIGI{code}",
        composite_figi=f"COMP{code}",
        list_date="2020-01-02",
        raw={"Code": code, "Type": type_},
    )


def test_build_us_instruments_marks_delisted_by_set_difference() -> None:
    active = [_symbol("AAPL"), _symbol("SPY", type_="ETF")]
    with_delisted = [*active, _symbol("LEH")]
    seen = datetime(2026, 5, 3, tzinfo=timezone.utc)

    instruments = build_us_instruments(
        active_symbols=active,
        active_plus_delisted_symbols=with_delisted,
        last_seen=seen,
    )

    by_id = {row.instrument_id: row for row in instruments}
    assert set(by_id) == {"US_AAPL", "US_SPY", "US_LEH"}
    assert by_id["US_AAPL"].is_active is True
    assert by_id["US_SPY"].asset_class == "equity_etf"
    assert by_id["US_LEH"].is_active is False


def test_build_us_instruments_filters_unsupported_types() -> None:
    instruments = build_us_instruments(
        active_symbols=[_symbol("AAA"), _symbol("BOND", type_="Bond")],
        active_plus_delisted_symbols=[_symbol("AAA"), _symbol("BOND", type_="Bond")],
        last_seen=datetime(2026, 5, 3, tzinfo=timezone.utc),
    )

    assert [row.ticker for row in instruments] == ["AAA"]


def test_build_us_instruments_keeps_requested_exchange_metadata() -> None:
    instruments = build_us_instruments(
        active_symbols=[_symbol("VOD")],
        active_plus_delisted_symbols=[_symbol("VOD")],
        exchange="LSE",
        last_seen=datetime(2026, 5, 3, tzinfo=timezone.utc),
    )

    metadata = json.loads(instruments[0].metadata)

    assert metadata["eodhd_exchange"] == "LSE"


def test_bar_projection_derives_adjusted_ohlcv_from_adjusted_close() -> None:
    instrument = {
        "instrument_id": "US_AAPL",
        "ticker": "AAPL",
        "exchange": "NASDAQ",
    }
    bar = EODHDDailyBar(
        ticker="AAPL.US",
        date="2026-01-05",
        open=100.0,
        high=110.0,
        low=90.0,
        close=100.0,
        volume=1_000.0,
        adj_open=None,
        adj_high=None,
        adj_low=None,
        adj_close=50.0,
        adj_volume=None,
        div_cash=0.0,
        split_factor=1.0,
    )

    row = _bar_to_ch(
        instrument=instrument,
        bar=bar,
        fetched_at=datetime(2026, 5, 3, tzinfo=timezone.utc),
    )

    assert row.adjusted_open == 50.0
    assert row.adjusted_high == 55.0
    assert row.adjusted_low == 45.0
    assert row.adjusted_close == 50.0
    assert row.adjusted_volume == 2_000.0
    assert row.time.date() == Date(2026, 1, 5)


class _FakeClient:
    def __init__(self) -> None:
        self.active = [_symbol("AAPL")]
        self.with_delisted = [_symbol("AAPL"), _symbol("LEH")]

    def list_symbols_active(self, exchange: str = "US"):
        return self.active

    def list_symbols_with_delisted(self, exchange: str = "US"):
        return self.with_delisted


class _FakeStore:
    def __init__(self) -> None:
        self.rows = []

    def upsert_market_instruments(self, instruments):
        self.rows.extend(instruments)
        return len(instruments)


def test_provider_seeds_clickhouse_instruments() -> None:
    store = _FakeStore()
    stats = EODHDMarketDataProvider(client=_FakeClient()).seed_us_universe(store)

    assert stats.instruments == 2
    assert {row.instrument_id for row in store.rows} == {"US_AAPL", "US_LEH"}


class _CorpActionFailureClient:
    def get_bulk_last_day(self, exchange: str, *, date: str | None = None):
        return []

    def get_bulk_dividends(self, exchange: str, *, date: str | None = None):
        return [
            EODHDDividend(
                ticker="AAPL.US",
                date="2026-02-09",
                declaration_date=None,
                record_date=None,
                payment_date=None,
                period=None,
                value=0.25,
                unadjusted_value=None,
                currency="USD",
            )
        ]

    def get_bulk_splits(self, exchange: str, *, date: str | None = None):
        return []

    def get_daily_bars(self, ticker: str):
        raise EODHDAPIError("unit-test failure")


class _CorpActionFailureStore:
    def __init__(self) -> None:
        self.corp_action_writes = 0

    def list_instruments(self, *, active_only=None):
        return [
            {
                "instrument_id": "US_AAPL",
                "ticker": "AAPL",
                "exchange": "NASDAQ",
                "currency": "USD",
                "metadata": json.dumps(
                    {"eodhd_code": "AAPL", "eodhd_exchange": "US"}
                ),
            }
        ]

    def upsert_market_bars(self, rows):
        return len(rows)

    def has_dividend_hash(self, *, instrument_id, ex_date, content_hash):
        return False

    def has_split_hash(self, *, instrument_id, execution_date, content_hash):
        return False

    def upsert_corp_actions(self, *, dividends, splits):
        self.corp_action_writes += 1
        return len(dividends), len(splits)

    def delete_bars_for_instrument(self, instrument_id):
        return None


def test_daily_bulk_keeps_failed_corp_action_hash_retryable() -> None:
    store = _CorpActionFailureStore()
    provider = EODHDMarketDataProvider(client=_CorpActionFailureClient())

    try:
        provider.refresh_daily_bulk(store, date="2026-02-09")
    except EODHDAPIError:
        pass
    else:
        raise AssertionError("expected corp-action refill failure")

    assert store.corp_action_writes == 0


class _Response:
    def __init__(self, payload) -> None:
        self.payload = payload
        self.content = b"json"
        self.text = json.dumps(payload)
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self.payload


class _Session:
    def __init__(self, payload) -> None:
        self.payload = payload
        self.calls = []

    def get(self, url, *, params, timeout):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return _Response(self.payload)


def test_client_symbol_list_parses_identity_fields() -> None:
    session = _Session([
        {
            "Code": "AAPL",
            "Name": "Apple Inc.",
            "Exchange": "NASDAQ",
            "Currency": "USD",
            "Type": "Common Stock",
            "Isin": "US0378331005",
            "FIGI": "BBG000B9XRY4",
            "CompositeFIGI": "BBG000B9XRY4",
            "ListingDate": "1980-12-12",
        }
    ])
    client = EODHDClient(api_key="unit-test")
    client.session = session

    rows = client.list_symbols_with_delisted("US")

    assert rows[0].code == "AAPL"
    assert rows[0].isin == "US0378331005"
    assert session.calls[0]["params"]["delisted"] == "1"


def test_client_bulk_dividend_accepts_dividend_field_name() -> None:
    session = _Session([
        {
            "code": "AAPL",
            "exchange_short_name": "US",
            "date": "2026-02-09",
            "dividend": 0.25,
            "currency": "USD",
        }
    ])
    client = EODHDClient(api_key="unit-test")
    client.session = session

    rows = client.get_bulk_dividends("US", date="2026-02-09")

    assert rows[0].ticker == "AAPL.US"
    assert rows[0].value == 0.25
    assert session.calls[0]["params"]["type"] == "dividends"
