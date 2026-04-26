"""EODHD scaffold tests: calendar_corp_fetch + calendar_corp_fetch_dividend_details service ops.

Split out of the original tests/test_eodhd_api_scaffold.py as part of
issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

from pathlib import Path
import httpx
import pytest
import respx
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def test_calendar_corp_fetch_dividend_details_rejects_missing_symbols(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    result = service.invoke("calendar_corp_fetch_dividend_details", {})
    assert "error" in result


def test_calendar_corp_fetch_dividend_details_rejects_malformed_dates(
    store: SQLiteEngineStore,
) -> None:
    """A typo in from/to must fail loudly — silently dropping the bound
    would turn /api/div/{symbol} into a full-history pull for every
    requested symbol."""
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    result = service.invoke(
        "calendar_corp_fetch_dividend_details",
        {"symbols": ["AAPL.US"], "from": "2026-02-30", "dry_run": True},
    )
    assert "error" in result
    assert "from" in result["error"]


def test_calendar_corp_fetch_dividend_details_dry_run(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    with respx.mock(assert_all_called=False) as router:
        router.route().mock(return_value=httpx.Response(500, text="must_not_call"))
        result = service.invoke(
            "calendar_corp_fetch_dividend_details",
            {"symbols": ["AAPL.US", "MSFT.US"], "dry_run": True},
        )
        assert router.calls.call_count == 0
    assert result["dry_run"] is True
    assert result["subtype"] == "dividend_detail"
    assert result["symbols_planned"] == 2
    assert result["stopped_reason"] == "dry_run"


def test_calendar_corp_fetch_dry_run_emits_no_http(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    with respx.mock(assert_all_called=False) as router:
        router.route().mock(return_value=httpx.Response(500, text="must_not_call"))
        result = service.invoke(
            "calendar_corp_fetch",
            {
                "subtype": "earnings",
                "from": "2026-05-01",
                "to": "2026-05-14",
                "dry_run": True,
            },
        )
        assert router.calls.call_count == 0
    assert result["dry_run"] is True
    assert result["subtype"] == "earnings"
    assert result["windows_planned"] >= 1
    assert result["stopped_reason"] == "dry_run"


def test_calendar_corp_fetch_rejects_missing_subtype(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    result = service.invoke("calendar_corp_fetch", {})
    assert "error" in result
