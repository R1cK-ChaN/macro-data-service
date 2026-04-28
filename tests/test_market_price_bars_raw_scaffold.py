"""Scaffold tests for the market price-bars audit lane (issue #69 slice 2).

Mirrors the obs_raw / cal_corp_raw test surface — content-hash stability
across volatile envelope variation, revision detection, INSERT OR IGNORE
idempotency, and a re-projection check that re-running the bar parser
against a stored ``market_price_bars_raw`` payload yields the same typed
bar rows the live ingest would have produced (no HTTP).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ingestion.market._bars_canonicalize import (
    bars_content_hash,
    canonicalize_bars_payload,
)
from ingestion.market.scrapers._eodhd import EODHDDailyBar, _parse_eodhd_bars
from ingestion.market.scrapers._tiingo import TiingoDailyBar, _parse_tiingo_bars
from storage import MarketPriceBarsRawRecord, SQLiteEngineStore


# ── Canonicalization ─────────────────────────────────────────────────────


class TestBarsCanonicalization:
    def test_sorts_bars_by_date(self) -> None:
        a = [
            {"date": "2024-01-01", "close": 100.0},
            {"date": "2024-01-02", "close": 101.0},
        ]
        b = [
            {"date": "2024-01-02", "close": 101.0},
            {"date": "2024-01-01", "close": 100.0},
        ]
        assert bars_content_hash(a) == bars_content_hash(b)

    def test_value_revision_changes_hash(self) -> None:
        a = [{"date": "2024-01-01", "close": 100.0}]
        b = [{"date": "2024-01-01", "close": 101.5}]
        assert bars_content_hash(a) != bars_content_hash(b)

    def test_new_bar_changes_hash(self) -> None:
        a = [{"date": "2024-01-01", "close": 100.0}]
        b = [
            {"date": "2024-01-01", "close": 100.0},
            {"date": "2024-01-02", "close": 101.0},
        ]
        assert bars_content_hash(a) != bars_content_hash(b)

    def test_skips_non_dict_rows(self) -> None:
        a = [{"date": "2024-01-01", "close": 100.0}, "garbage", None]
        b = [{"date": "2024-01-01", "close": 100.0}]
        assert bars_content_hash(a) == bars_content_hash(b)


# ── Storage idempotency: INSERT OR IGNORE on (provider, ticker, content_hash)


class TestMarketPriceBarsRawIdempotency:
    @pytest.fixture
    def store(self, tmp_path: Path) -> SQLiteEngineStore:
        return SQLiteEngineStore(tmp_path / "engine.db")

    def _record(
        self, *, provider: str = "eodhd", content_hash: str,
        snapshot_ms: int = 1700000000000,
    ) -> MarketPriceBarsRawRecord:
        return MarketPriceBarsRawRecord(
            provider=provider,
            ticker="AAPL.US",
            snapshot_epoch_ms=snapshot_ms,
            content_hash=content_hash,
            payload_json="[{}]",
            fetched_at="2025-01-01T00:00:00Z",
        )

    def test_first_insert_writes_one_row(self, store: SQLiteEngineStore) -> None:
        assert store.insert_market_price_bars_raw([self._record(content_hash="h1")]) == 1

    def test_duplicate_hash_inserts_zero(self, store: SQLiteEngineStore) -> None:
        rec = self._record(content_hash="h1")
        store.insert_market_price_bars_raw([rec])
        assert store.insert_market_price_bars_raw([rec]) == 0

    def test_revised_hash_inserts_one(self, store: SQLiteEngineStore) -> None:
        store.insert_market_price_bars_raw([self._record(content_hash="h1")])
        assert store.insert_market_price_bars_raw([
            self._record(content_hash="h2", snapshot_ms=1700000005000),
        ]) == 1

    def test_latest_returns_newest_snapshot(self, store: SQLiteEngineStore) -> None:
        store.insert_market_price_bars_raw([
            self._record(content_hash="h1", snapshot_ms=1700000000000),
            self._record(content_hash="h2", snapshot_ms=1700000005000),
        ])
        latest = store.latest_market_price_bars_raw("eodhd", "AAPL.US")
        assert latest is not None
        assert latest.content_hash == "h2"

    def test_distinct_provider_partitions(self, store: SQLiteEngineStore) -> None:
        store.insert_market_price_bars_raw([self._record(provider="eodhd", content_hash="h1")])
        store.insert_market_price_bars_raw([self._record(provider="tiingo", content_hash="h1")])
        assert store.latest_market_price_bars_raw("eodhd", "AAPL.US") is not None
        assert store.latest_market_price_bars_raw("tiingo", "AAPL.US") is not None


# ── Re-projection: parser replay against a stored payload ───────────────


class TestReProjectionFromBarsRaw:
    """Issue #69 acceptance: re-running the bar parser against
    ``market_price_bars_raw`` (no upstream call) yields byte-identical
    typed bars."""

    def test_eodhd_payload_round_trips(self, tmp_path: Path) -> None:
        store = SQLiteEngineStore(tmp_path / "engine.db")
        payload = [
            {"date": "2024-01-02", "open": 100.0, "high": 102.0, "low": 99.0,
             "close": 101.0, "volume": 1_000_000, "adjusted_close": 101.0},
            {"date": "2024-01-03", "open": 101.0, "high": 103.0, "low": 100.0,
             "close": 102.5, "volume": 1_200_000, "adjusted_close": 102.5},
        ]
        rec = MarketPriceBarsRawRecord(
            provider="eodhd",
            ticker="AAPL.US",
            snapshot_epoch_ms=1700000000000,
            content_hash=bars_content_hash(payload),
            payload_json=json.dumps(payload, sort_keys=True),
            fetched_at="2025-01-01T00:00:00Z",
        )
        store.insert_market_price_bars_raw([rec])
        latest = store.latest_market_price_bars_raw("eodhd", "AAPL.US")
        assert latest is not None
        replayed = _parse_eodhd_bars(json.loads(latest.payload_json), ticker="AAPL.US")
        assert len(replayed) == 2
        assert replayed[0].date == "2024-01-02"
        assert replayed[0].close == 101.0
        assert replayed[1].date == "2024-01-03"
        assert replayed[1].close == 102.5

    def test_tiingo_payload_round_trips(self, tmp_path: Path) -> None:
        store = SQLiteEngineStore(tmp_path / "engine.db")
        payload = [
            {"date": "2024-01-02T00:00:00.000Z", "open": 100.0, "high": 102.0,
             "low": 99.0, "close": 101.0, "volume": 1_000_000,
             "adjOpen": 100.0, "adjHigh": 102.0, "adjLow": 99.0,
             "adjClose": 101.0, "adjVolume": 1_000_000,
             "divCash": 0.0, "splitFactor": 1.0},
        ]
        rec = MarketPriceBarsRawRecord(
            provider="tiingo",
            ticker="SPY",
            snapshot_epoch_ms=1700000000000,
            content_hash=bars_content_hash(payload),
            payload_json=json.dumps(payload, sort_keys=True),
            fetched_at="2025-01-01T00:00:00Z",
        )
        store.insert_market_price_bars_raw([rec])
        latest = store.latest_market_price_bars_raw("tiingo", "SPY")
        assert latest is not None
        replayed = _parse_tiingo_bars(json.loads(latest.payload_json), ticker="SPY")
        assert len(replayed) == 1
        assert replayed[0].close == 101.0
        assert replayed[0].adj_close == 101.0


# ── Provider-side raw capture: refresh_market_history writes one row ────


class TestEodhdRefreshWritesRaw:
    def test_refresh_writes_one_raw_row_first_call(self, tmp_path: Path) -> None:
        from ingestion.market._eodhd_universe import EODHDUniverseEntry
        from ingestion.market.clients._eodhd import EODHDMarketDataProvider
        from ingestion.market.scrapers._eodhd import EODHDDailyBar

        store = SQLiteEngineStore(tmp_path / "engine.db")
        bar_payload = [
            {"date": "2024-01-02", "open": 100.0, "high": 102.0, "low": 99.0,
             "close": 101.0, "volume": 1_000_000, "adjusted_close": 101.0},
        ]
        parsed_bars = [
            EODHDDailyBar(
                ticker="TEST.US", date="2024-01-02",
                open=100.0, high=102.0, low=99.0, close=101.0, volume=1_000_000,
                adj_open=None, adj_high=None, adj_low=None,
                adj_close=101.0, adj_volume=None,
                div_cash=0.0, split_factor=1.0,
            ),
        ]
        mock_client = MagicMock()
        mock_client.get_daily_bars_with_raw.return_value = (
            parsed_bars, bar_payload, {"fmt": "json"},
        )
        entry = EODHDUniverseEntry(
            instrument_id="US_TEST",
            eodhd_ticker="TEST.US",
            primary_ticker="TEST",
            exchange_code="NYSE",
            name="Test Instrument",
            asset_class="equity",
            market="United States equity market",
            currency="USD",
            isin="",
            composite_figi="",
            share_class_figi="",
            description_for_agent="",
        )
        provider = EODHDMarketDataProvider(
            client=mock_client, universe=(entry,), request_sleep=0,
        )
        provider.refresh_market_history(store, "TEST.US")

        latest = store.latest_market_price_bars_raw("eodhd", "TEST.US")
        assert latest is not None
        assert latest.content_hash == bars_content_hash(bar_payload)

        # Re-run with identical payload — INSERT OR IGNORE dedupes.
        provider.refresh_market_history(store, "TEST.US")
        latest_again = store.latest_market_price_bars_raw("eodhd", "TEST.US")
        # Same hash (no revision) so the latest row is unchanged.
        assert latest_again.content_hash == latest.content_hash


class TestTiingoRefreshWritesRaw:
    def test_refresh_writes_one_raw_row_first_call(self, tmp_path: Path) -> None:
        from ingestion.market._tiingo_universe import TiingoUniverseEntry
        from ingestion.market.clients._tiingo import TiingoMarketDataProvider
        from ingestion.market.scrapers._tiingo import TiingoDailyBar

        store = SQLiteEngineStore(tmp_path / "engine.db")
        bar_payload = [
            {"date": "2024-01-02T00:00:00.000Z",
             "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0,
             "volume": 1_000_000,
             "adjOpen": 100.0, "adjHigh": 102.0, "adjLow": 99.0,
             "adjClose": 101.0, "adjVolume": 1_000_000,
             "divCash": 0.0, "splitFactor": 1.0},
        ]
        parsed_bars = [
            TiingoDailyBar(
                ticker="SPY", date="2024-01-02",
                open=100.0, high=102.0, low=99.0, close=101.0, volume=1_000_000,
                adj_open=100.0, adj_high=102.0, adj_low=99.0,
                adj_close=101.0, adj_volume=1_000_000,
                div_cash=0.0, split_factor=1.0,
            ),
        ]
        mock_client = MagicMock()
        mock_client.get_daily_bars_with_raw.return_value = (
            parsed_bars, bar_payload, {"format": "json"},
        )
        entry = TiingoUniverseEntry(
            instrument_id="US_SPY",
            ticker="SPY",
            name="SPDR S&P 500 ETF",
            asset_class="equity_etf",
            market="United States equity market",
            exchange_code="NYSEARCA",
            isin="US78462F1030",
            composite_figi="",
            share_class_figi="",
            description_for_agent="",
        )
        with patch(
            "ingestion.market.clients._tiingo.TIINGO_UNIVERSE_BY_TICKER",
            {"SPY": entry},
        ):
            provider = TiingoMarketDataProvider(client=mock_client)
            provider.refresh_market_history(store, "SPY")

        latest = store.latest_market_price_bars_raw("tiingo", "SPY")
        assert latest is not None
        assert latest.content_hash == bars_content_hash(bar_payload)
