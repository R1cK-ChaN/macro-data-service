"""Service-op + storage-query tests for the fundamentals lane (issue #68 S2)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx

from ingestion.market.fundamentals.eodhd_fundamentals import (
    build_raw_record,
    parse_payload_records,
    project_fundamentals_company,
    project_fundamentals_financials,
    project_fundamentals_highlights,
    store_fundamentals_raw,
)
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


def _payload(*, revenue: float = 391035000000.0) -> dict:
    return {
        "General": {
            "Code":          "AAPL",
            "Name":          "Apple Inc",
            "Type":          "Common Stock",
            "Sector":        "Technology",
            "Industry":      "Consumer Electronics",
            "FiscalYearEnd": "September",
            "Exchange":      "NASDAQ",
            "CurrencyCode":  "USD",
            "CountryISO":    "US",
            "ISIN":          "US0378331005",
        },
        "Highlights": {
            "MarketCapitalization": 3.0e12,
            "PERatio":              28.5,
            "EarningsShare":        6.42,
        },
        "Valuation":   {"PERatio": 28.5},
        "SharesStats": {"SharesOutstanding": 1.54e10},
        "Financials":  {
            "Income_Statement": {
                "currency_symbol": "USD",
                "yearly": {
                    "2024-09-30": {
                        "date":         "2024-09-30",
                        "totalRevenue": revenue,
                        "netIncome":    93736000000.0,
                    },
                    "2023-09-30": {
                        "date":         "2023-09-30",
                        "totalRevenue": 383285000000.0,
                        "netIncome":    96995000000.0,
                    },
                },
                "quarterly": {
                    "2024-09-30": {
                        "date":         "2024-09-30",
                        "totalRevenue": 94930000000.0,
                        "netIncome":    14736000000.0,
                    },
                },
            },
        },
    }


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


@pytest.fixture()
def service(store: SQLiteEngineStore) -> LocalMacroDataService:
    return LocalMacroDataService(store=store)


def _ingest(store: SQLiteEngineStore, *, payload: dict, snapshot_ms: int) -> None:
    """Helper — write raw + projections directly so tests don't need HTTP."""
    import json
    conn = store.get_connection()
    try:
        text = json.dumps(payload, sort_keys=True)
        raw = build_raw_record(
            ticker="AAPL.US", payload_text=text, snapshot_epoch_ms=snapshot_ms,
        )
        store_fundamentals_raw(conn, [raw])
        company, highlights, financials = parse_payload_records(
            payload, ticker="AAPL.US", snapshot_epoch_ms=snapshot_ms,
        )
        if company is not None:
            project_fundamentals_company(conn, [company])
        if highlights is not None:
            project_fundamentals_highlights(conn, [highlights])
        if financials:
            project_fundamentals_financials(conn, financials)
        conn.commit()
    finally:
        conn.close()


def test_fundamentals_fetch_rejects_missing_tickers(
    service: LocalMacroDataService,
) -> None:
    result = service.invoke("fundamentals_fetch", {})
    assert "error" in result
    assert "tickers" in result["error"]


def test_fundamentals_fetch_dry_run_returns_plan(
    service: LocalMacroDataService,
) -> None:
    result = service.invoke(
        "fundamentals_fetch",
        {"tickers": ["AAPL.US", "MSFT.US"], "dry_run": True},
    )
    assert "error" not in result
    assert result["dry_run"] is True
    assert result["tickers_planned"] == 2
    assert result["tickers_fetched"] == 0
    assert result["stopped_reason"] == "dry_run"


@respx.mock
def test_fundamentals_fetch_executes_and_writes_rows(
    service: LocalMacroDataService, store: SQLiteEngineStore, monkeypatch,
) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/AAPL.US").mock(
        return_value=httpx.Response(200, json=_payload()),
    )
    result = service.invoke(
        "fundamentals_fetch",
        {"tickers": ["AAPL.US"], "dry_run": False, "max_requests": 5},
    )
    assert "error" not in result
    assert result["tickers_fetched"] == 1
    assert result["raw_inserted"] == 1
    assert result["company_upserted"] == 1
    assert result["financials_upserted"] == 3  # IS yearly×2 + IS quarterly×1
    assert result["stopped_reason"] == "completed"
    # Round-trip via the read op without as_of — should see latest projection.
    read = service.invoke("get_fundamentals", {"ticker": "AAPL.US"})
    assert read["company"]["name"] == "Apple Inc"
    assert len(read["financials"]) == 3
    assert read["financials"][0]["period_end"] == "2024-09-30"


def test_fundamentals_fetch_execute_rejects_partial_sections(
    service: LocalMacroDataService,
) -> None:
    """Partial sections in execute mode would corrupt PIT reads."""
    result = service.invoke(
        "fundamentals_fetch",
        {
            "tickers": ["AAPL.US"],
            "dry_run": False,
            "sections": ["General"],
        },
    )
    assert "error" in result
    assert "section set" in result["error"]


def test_fundamentals_fetch_dry_run_allows_partial_sections(
    service: LocalMacroDataService,
) -> None:
    """Dry-run preview still permits partial sections — handy for
    inspecting filter behaviour without spending API budget."""
    result = service.invoke(
        "fundamentals_fetch",
        {
            "tickers": ["AAPL.US"],
            "dry_run": True,
            "sections": ["General"],
        },
    )
    assert "error" not in result
    assert result["dry_run"] is True


def test_get_fundamentals_rejects_missing_ticker(
    service: LocalMacroDataService,
) -> None:
    result = service.invoke("get_fundamentals", {})
    assert "error" in result
    assert "ticker" in result["error"]


def test_get_fundamentals_rejects_invalid_statement(
    service: LocalMacroDataService,
) -> None:
    result = service.invoke(
        "get_fundamentals", {"ticker": "AAPL.US", "statement": "XX"},
    )
    assert "error" in result
    assert "statement" in result["error"]


def test_get_fundamentals_rejects_invalid_period(
    service: LocalMacroDataService,
) -> None:
    result = service.invoke(
        "get_fundamentals", {"ticker": "AAPL.US", "period": "M"},
    )
    assert "error" in result
    assert "period" in result["error"]


def test_get_fundamentals_filters_statement_and_period(
    service: LocalMacroDataService, store: SQLiteEngineStore,
) -> None:
    _ingest(store, payload=_payload(), snapshot_ms=1_700_000_000_000)
    only_q = service.invoke(
        "get_fundamentals",
        {"ticker": "AAPL.US", "statement": "IS", "period": "Q"},
    )
    assert all(
        r["statement"] == "IS" and r["period_type"] == "Q"
        for r in only_q["financials"]
    )
    assert len(only_q["financials"]) == 1
    assert only_q["financials"][0]["period_end"] == "2024-09-30"


def test_get_fundamentals_as_of_returns_pre_restatement_view(
    service: LocalMacroDataService, store: SQLiteEngineStore,
) -> None:
    """A snapshot at T0 has revenue=391035; a snapshot at T1 restates
    to 392000. ``as_of`` between T0 and T1 must surface the original
    391035 from the raw lane, not the restated value."""
    t0_ms = 1_700_000_000_000
    t1_ms = 1_700_000_999_000
    _ingest(store, payload=_payload(revenue=391035000000.0), snapshot_ms=t0_ms)
    _ingest(store, payload=_payload(revenue=392000000000.0), snapshot_ms=t1_ms)

    # Latest projection — revenue is the restated 392000.
    latest = service.invoke(
        "get_fundamentals",
        {"ticker": "AAPL.US", "statement": "IS", "period": "A"},
    )
    fy24 = next(r for r in latest["financials"] if r["period_end"] == "2024-09-30")
    assert fy24["revenue"] == 392000000000.0

    # as_of between T0 and T1 — must return original 391035.
    pit_iso = datetime.fromtimestamp(
        (t0_ms + 500_000) / 1000, tz=timezone.utc,
    ).isoformat()
    pit = service.invoke(
        "get_fundamentals",
        {
            "ticker": "AAPL.US",
            "statement": "IS",
            "period": "A",
            "as_of": pit_iso,
        },
    )
    assert pit["as_of"] is not None
    fy24_pit = next(
        r for r in pit["financials"] if r["period_end"] == "2024-09-30"
    )
    assert fy24_pit["revenue"] == 391035000000.0


def test_get_fundamentals_as_of_isolates_intraday_highlights_revisions(
    service: LocalMacroDataService, store: SQLiteEngineStore,
) -> None:
    """Two snapshots on the same UTC day with different MarketCap —
    PIT cutoff between them must surface only the earlier value.

    Pre-fix this leaked the post-cutoff highlights row because the
    projection grain is one row per ``as_of_date``; the fix re-parses
    Highlights from the raw snapshot at-or-before the cutoff.
    """
    morning = _payload()
    morning["Highlights"]["MarketCapitalization"] = 1.0e12
    evening = _payload()
    evening["Highlights"]["MarketCapitalization"] = 2.0e12
    morning_ms = 1_710_000_000_000  # ~2024-03-09T16:00:00Z
    evening_ms = 1_710_028_800_000  # ~2024-03-10T00:00:00Z (same day or next? — check)
    # Pin both snapshots to the same UTC day by computing relative to a fixed base.
    base = datetime(2024, 3, 9, 8, 0, 0, tzinfo=timezone.utc)
    morning_ms = int(base.timestamp() * 1000)
    evening_ms = int((base.replace(hour=20)).timestamp() * 1000)
    _ingest(store, payload=morning, snapshot_ms=morning_ms)
    _ingest(store, payload=evening, snapshot_ms=evening_ms)

    # Cutoff sits between the two snapshots, same UTC day.
    cutoff_iso = base.replace(hour=12).isoformat()
    pit = service.invoke(
        "get_fundamentals", {"ticker": "AAPL.US", "as_of": cutoff_iso},
    )
    assert pit["highlights"] is not None
    assert pit["highlights"]["market_cap"] == 1.0e12  # not 2.0e12

    # Cutoff after both — surfaces the later value.
    after_iso = base.replace(hour=23).isoformat()
    pit_after = service.invoke(
        "get_fundamentals", {"ticker": "AAPL.US", "as_of": after_iso},
    )
    assert pit_after["highlights"]["market_cap"] == 2.0e12


def test_get_fundamentals_as_of_before_first_snapshot_yields_empty(
    service: LocalMacroDataService, store: SQLiteEngineStore,
) -> None:
    _ingest(store, payload=_payload(), snapshot_ms=1_700_000_000_000)
    # 2020 is before the first snapshot — financials list must be empty.
    pit = service.invoke(
        "get_fundamentals",
        {"ticker": "AAPL.US", "as_of": "2020-01-01T00:00:00Z"},
    )
    assert pit["financials"] == []
    # Highlights with as_of_date <= 2020 also returns None.
    assert pit["highlights"] is None


def test_get_fundamentals_rejects_invalid_as_of(
    service: LocalMacroDataService,
) -> None:
    result = service.invoke(
        "get_fundamentals",
        {"ticker": "AAPL.US", "as_of": "not-a-timestamp"},
    )
    assert "error" in result
    assert "as_of" in result["error"]


def test_get_fundamentals_rejects_future_as_of(
    service: LocalMacroDataService,
) -> None:
    result = service.invoke(
        "get_fundamentals",
        {"ticker": "AAPL.US", "as_of": "9999-01-01T00:00:00Z"},
    )
    assert "error" in result
    assert "future" in result["error"]


def test_get_fundamentals_pit_ordering_matches_latest(
    service: LocalMacroDataService, store: SQLiteEngineStore,
) -> None:
    """PIT and latest paths must return rows in the same order so
    ``limit=1`` queries are consistent across the two read paths.
    EODHD ships annual + quarterly IS rows for the same period_end —
    SQL sorts those by ``statement ASC, period_type ASC`` after
    ``period_end DESC``. The PIT reconstruction must match."""
    _ingest(store, payload=_payload(), snapshot_ms=1_700_000_000_000)

    latest = service.invoke(
        "get_fundamentals", {"ticker": "AAPL.US", "limit": 1},
    )
    assert len(latest["financials"]) == 1
    latest_first = latest["financials"][0]

    pit = service.invoke(
        "get_fundamentals",
        {
            "ticker": "AAPL.US",
            "limit": 1,
            "as_of": "2024-12-31T00:00:00Z",
        },
    )
    assert len(pit["financials"]) == 1
    pit_first = pit["financials"][0]
    assert (latest_first["period_end"], latest_first["statement"],
            latest_first["period_type"]) == (
        pit_first["period_end"], pit_first["statement"],
        pit_first["period_type"],
    )


def test_storage_list_fundamentals_financials_filters(
    store: SQLiteEngineStore,
) -> None:
    _ingest(store, payload=_payload(), snapshot_ms=1_700_000_000_000)
    rows = store.list_fundamentals_financials(
        provider="eodhd", ticker="AAPL.US", statement="IS",
    )
    assert len(rows) == 3
    assert all(r["statement"] == "IS" for r in rows)
    rows_q = store.list_fundamentals_financials(
        provider="eodhd", ticker="AAPL.US", period_type="Q",
    )
    assert len(rows_q) == 1


def test_storage_get_raw_at_returns_at_or_before(
    store: SQLiteEngineStore,
) -> None:
    _ingest(store, payload=_payload(revenue=100.0), snapshot_ms=1_000)
    _ingest(store, payload=_payload(revenue=200.0), snapshot_ms=2_000)
    pre = store.get_fundamentals_raw_at(
        provider="eodhd", ticker="AAPL.US", as_of_epoch_ms=500,
    )
    assert pre is None
    mid = store.get_fundamentals_raw_at(
        provider="eodhd", ticker="AAPL.US", as_of_epoch_ms=1_500,
    )
    assert mid is not None
    assert int(mid["snapshot_epoch_ms"]) == 1_000
    after = store.get_fundamentals_raw_at(
        provider="eodhd", ticker="AAPL.US", as_of_epoch_ms=10_000,
    )
    assert after is not None
    assert int(after["snapshot_epoch_ms"]) == 2_000
