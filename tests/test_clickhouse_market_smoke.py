"""ClickHouse market-lane smoke test (issue #118 P5).

End-to-end validation that the bilingual storage swap works:

* Apply the CH schema via ``apply_clickhouse_schema`` against the
  configured server (env vars per ``.env`` — same as the live
  service).
* Write a handful of bars + corp actions + an instrument through the
  ``ClickHouseMarketStore`` adapter.
* Read them back via ``LocalMacroDataService.invoke("get_market_history")``
  and ``invoke("get_market_snapshot")`` — i.e. through the production
  service-layer routing wired up in P3, not by talking to the store
  directly. That's the contract the follow-up backfill issue must
  preserve.

Module-level ``pytest.mark.integration`` because the test requires a
running ClickHouse server. Operators run the suite with the default
``-m "not integration"`` exclusion (CI without docker, local-only
macro work); this file is opt-in via ``pytest -m integration`` once
``docker compose up -d clickhouse`` is healthy. Picked the env-driven
fixture over a docker-from-pytest fixture so the same smoke test can
hit a remote CH cluster (future ops need) without rewriting it.

If the CH connection cannot be established (driver missing, server
down), every test in the module is skipped at collection time so a
green run on a CH-less host stays distinguishable from a green run
that actually exercised the lane.
"""

from __future__ import annotations

import os
import sys
import tempfile
from datetime import date as Date, datetime, timezone
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

clickhouse_connect = pytest.importorskip(
    "clickhouse_connect",
    reason="clickhouse-connect not installed — bilingual storage smoke test skipped",
)

from macro_data.factory import build_local_macro_data_service  # noqa: E402
from storage.clickhouse.records import (  # noqa: E402
    CHBar,
    CHDividend,
    CHInstrument,
    CHSplit,
)
from storage.clickhouse.schema import (  # noqa: E402
    apply_clickhouse_schema,
    clickhouse_database_from_env,
)
from storage.clickhouse.store import (  # noqa: E402
    ClickHouseMarketStore,
    clickhouse_client_from_env,
    compute_dividend_hash,
    compute_split_hash,
)


@pytest.fixture(scope="module")
def ch_client():
    """Live CH client per-module — schema applied once, tables truncated
    between tests in the same run.

    Skips the whole module if the connection fails so the suite stays
    green on hosts without a running ClickHouse.
    """
    try:
        client = clickhouse_client_from_env()
        client.command("SELECT 1")
    except Exception as exc:  # pragma: no cover — env-dependent
        pytest.skip(f"ClickHouse not reachable: {exc}")
    apply_clickhouse_schema(client)
    yield client


@pytest.fixture()
def fresh_tables(ch_client):
    """Wipe the four market tables before each test so writes don't
    bleed across test cases."""
    db = clickhouse_database_from_env()
    for table in ("bars_1d", "dividends", "splits", "instruments"):
        ch_client.command(f"TRUNCATE TABLE IF EXISTS {db}.{table}")
    yield


def test_apply_schema_creates_four_market_tables(ch_client) -> None:
    """``apply_clickhouse_schema`` is idempotent on every process start
    — re-applying against an already-populated database must not raise
    and must keep the same four tables."""
    db = clickhouse_database_from_env()
    apply_clickhouse_schema(ch_client)
    apply_clickhouse_schema(ch_client)
    rows = ch_client.query(
        "SELECT name FROM system.tables WHERE database = {db:String} ORDER BY name",
        parameters={"db": db},
    ).result_rows
    table_names = [r[0] for r in rows]
    assert table_names == ["bars_1d", "dividends", "instruments", "splits"]


def test_get_market_history_round_trip_via_service(
    ch_client, fresh_tables, tmp_path: Path
) -> None:
    """The end-to-end contract the issue calls out: write bars through
    the CH store, read them back via ``LocalMacroDataService.invoke
    ("get_market_history")``.

    Builds the service through ``build_local_macro_data_service`` so
    the factory's CH wiring is exercised, not bypassed."""
    fetched = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)

    service = build_local_macro_data_service(db_path=tmp_path / "engine.db")
    assert service._market_store is not None, (
        "factory must wire ClickHouseMarketStore when CH is reachable"
    )

    service._market_store.upsert_market_instrument(
        CHInstrument(
            instrument_id="US_AAPL",
            isin="US0378331005", figi="BBG000B9XRY4",
            composite_figi="BBG000B9XRY4",
            ticker="AAPL", exchange="NASDAQ",
            asset_class="equity", currency="USD",
            name="Apple Inc.", list_date=Date(1980, 12, 12),
            is_active=True, last_seen=fetched, metadata="{}",
        )
    )
    service._market_store.upsert_market_bars([
        CHBar(
            instrument_id="US_AAPL", ticker="AAPL", exchange="NASDAQ",
            time=datetime(2026, 4, 30, 20, 0, tzinfo=timezone.utc),
            open=200.0, high=205.0, low=198.0, close=203.0, volume=1_000_000.0,
            adjusted_open=200.0, adjusted_high=205.0, adjusted_low=198.0,
            adjusted_close=203.0, adjusted_volume=1_000_000.0,
            fetched_at=fetched,
        ),
        CHBar(
            instrument_id="US_AAPL", ticker="AAPL", exchange="NASDAQ",
            time=datetime(2026, 5, 1, 20, 0, tzinfo=timezone.utc),
            open=203.0, high=208.0, low=202.0, close=207.5, volume=1_500_000.0,
            adjusted_open=203.0, adjusted_high=208.0, adjusted_low=202.0,
            adjusted_close=207.5, adjusted_volume=1_500_000.0,
            fetched_at=fetched,
        ),
    ])

    response = service.invoke(
        "get_market_history",
        {"instrument_id": "US_AAPL", "start": "2026-04-29", "end": "2026-05-02"},
    )

    assert response["instrument_id"] == "US_AAPL"
    assert response["total"] == 2
    assert [r["date"] for r in response["rows"]] == ["2026-04-30", "2026-05-01"]
    assert [r["close"] for r in response["rows"]] == [203.0, 207.5]


def test_get_market_snapshot_returns_latest_bar_per_instrument(
    ch_client, fresh_tables, tmp_path: Path
) -> None:
    """``latest_market_snapshot`` must collapse to one row per
    instrument and pick the row with the highest ``time``. Two
    instruments, two bars each, the snapshot returns two rows."""
    fetched = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
    store = ClickHouseMarketStore(ch_client)
    store.upsert_market_bars([
        CHBar("US_AAPL", "AAPL", "NASDAQ",
              datetime(2026, 4, 30, 20, 0, tzinfo=timezone.utc),
              200.0, 205.0, 198.0, 203.0, 1_000_000.0,
              200.0, 205.0, 198.0, 203.0, 1_000_000.0, fetched),
        CHBar("US_AAPL", "AAPL", "NASDAQ",
              datetime(2026, 5, 1, 20, 0, tzinfo=timezone.utc),
              203.0, 208.0, 202.0, 207.5, 1_500_000.0,
              203.0, 208.0, 202.0, 207.5, 1_500_000.0, fetched),
        CHBar("US_MSFT", "MSFT", "NASDAQ",
              datetime(2026, 5, 1, 20, 0, tzinfo=timezone.utc),
              400.0, 410.0, 395.0, 408.0, 700_000.0,
              400.0, 410.0, 395.0, 408.0, 700_000.0, fetched),
    ])

    service = build_local_macro_data_service(db_path=tmp_path / "engine.db")
    snap = service.invoke("get_market_snapshot")

    assert snap["total"] == 2
    by_id = {row["instrument_id"]: row for row in snap["prices"]}
    assert by_id["US_AAPL"]["close"] == 207.5
    assert by_id["US_AAPL"]["time"] == "2026-05-01T20:00:00Z"
    assert by_id["US_MSFT"]["close"] == 408.0


def test_corp_actions_dedupe_on_repeated_ingest(ch_client, fresh_tables) -> None:
    """``ReplacingMergeTree`` collapses repeated ingests of the same
    ``content_hash`` row to one row on merge — the engine swap from
    plain MergeTree (Codex finding 1) is what makes this true. A
    revised payout (different hash) lands as a new row preserving the
    revision chain."""
    fetched = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
    store = ClickHouseMarketStore(ch_client)
    db = clickhouse_database_from_env()

    div_hash = compute_dividend_hash(
        instrument_id="US_AAPL", ex_date="2026-02-09",
        cash_amount=0.25, unadjusted_amount=0.25, currency="USD",
    )
    revised_hash = compute_dividend_hash(
        instrument_id="US_AAPL", ex_date="2026-02-09",
        cash_amount=0.26, unadjusted_amount=0.26, currency="USD",
    )
    split_hash = compute_split_hash(
        instrument_id="US_AAPL", execution_date="2014-06-09",
        to_factor=7, from_factor=1,
    )

    div = CHDividend(
        instrument_id="US_AAPL", ticker="AAPL", ex_date=Date(2026, 2, 9),
        declaration_date=None, record_date=None, payment_date=None,
        period="Quarterly", cash_amount=0.25, unadjusted_amount=0.25,
        currency="USD", fetched_at=fetched, content_hash=div_hash,
    )
    div_revised = CHDividend(
        instrument_id="US_AAPL", ticker="AAPL", ex_date=Date(2026, 2, 9),
        declaration_date=None, record_date=None, payment_date=None,
        period="Quarterly", cash_amount=0.26, unadjusted_amount=0.26,
        currency="USD", fetched_at=fetched, content_hash=revised_hash,
    )
    split = CHSplit(
        instrument_id="US_AAPL", ticker="AAPL",
        execution_date=Date(2014, 6, 9),
        to_factor=7.0, from_factor=1.0,
        fetched_at=fetched, content_hash=split_hash,
    )

    # Same content twice — second insert should collapse on merge.
    store.upsert_corp_actions(dividends=[div], splits=[split])
    store.upsert_corp_actions(dividends=[div], splits=[split])
    # Then a revised dividend (different hash) — lands as a new row.
    store.upsert_corp_actions(dividends=[div_revised])

    ch_client.command(f"OPTIMIZE TABLE {db}.dividends FINAL")
    ch_client.command(f"OPTIMIZE TABLE {db}.splits FINAL")

    div_count = ch_client.query(
        f"SELECT count() FROM {db}.dividends"
    ).result_rows[0][0]
    split_count = ch_client.query(
        f"SELECT count() FROM {db}.splits"
    ).result_rows[0][0]
    assert div_count == 2, "original + revised hash should both survive"
    assert split_count == 1, "duplicate split insert should collapse"


def test_clickhouse_database_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """``clickhouse_database_from_env`` reads the env var. Documents
    the contract the docker-compose CLICKHOUSE_DB / .env CLICKHOUSE_DATABASE
    pair relies on so a future change to either side surfaces here."""
    monkeypatch.setenv("CLICKHOUSE_DATABASE", "smoke_market")
    assert clickhouse_database_from_env() == "smoke_market"
    monkeypatch.delenv("CLICKHOUSE_DATABASE", raising=False)
    assert clickhouse_database_from_env() == "market"
