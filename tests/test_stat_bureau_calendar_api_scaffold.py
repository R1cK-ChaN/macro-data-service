"""Mocked tests for the Statistics Bureau calendar connector (issue #14 P2)."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.stat_bureau_api import (
    fetch_stat_bureau_calendar,
    fetch_stat_bureau_values,
    parse_cpi_release_schedule_html,
    parse_estat_value_json,
    parse_lfs_release_schedule_html,
    project_schedule_events,
    schedule_entry_to_records,
    store_raw,
    time_code_for_month,
)
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture(path: str) -> str:
    return (FIXTURES / path).read_text(encoding="utf-8")


def _json_fixture(path: str) -> dict:
    return json.loads(_fixture(path))


def _epoch_ms(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp() * 1000)


def test_cpi_schedule_parser_uses_japan_column() -> None:
    entries = parse_cpi_release_schedule_html(
        _fixture("stat_bureau_cpi/release_schedule.html")
    )

    assert [(e.reference_date, e.release_date) for e in entries] == [
        (date(2025, 12, 1), date(2026, 1, 23)),
        (date(2026, 1, 1), date(2026, 2, 20)),
        (date(2026, 2, 1), date(2026, 3, 24)),
        (date(2026, 3, 1), date(2026, 4, 24)),
        (date(2027, 1, 1), date(2027, 2, 19)),
    ]
    assert {e.indicator for e in entries} == {"CORE_CPI"}
    assert entries[0].release_time_local == "08:30"


def test_lfs_schedule_parser_uses_basic_tabulation_column() -> None:
    entries = parse_lfs_release_schedule_html(
        _fixture("stat_bureau_lfs/release_schedule.html")
    )

    assert [(e.reference_date, e.release_date) for e in entries] == [
        (date(2025, 12, 1), date(2026, 1, 30)),
        (date(2026, 1, 1), date(2026, 3, 3)),
        (date(2026, 2, 1), date(2026, 3, 31)),
        (date(2026, 3, 1), date(2026, 4, 28)),
    ]
    assert {e.indicator for e in entries} == {"UNEMPLOYMENT_RATE"}


def test_schedule_entry_projects_jst_release_time() -> None:
    entry = parse_cpi_release_schedule_html(
        _fixture("stat_bureau_cpi/release_schedule.html")
    )[3]
    raw, event = schedule_entry_to_records(entry, snapshot_epoch_ms=1)

    assert raw.provider == "stat-bureau-jp"
    assert event.title == "Core CPI YoY"
    assert event.event_time_utc == "2026-04-23T23:30:00+00:00"
    assert event.reference_date == "2026-03-01"
    assert event.actual is None


def test_estat_time_code_and_value_parser() -> None:
    cpi = parse_estat_value_json(
        _json_fixture("stat_bureau_estat/cpi_core_202603.json"),
        indicator="CORE_CPI",
        reference=date(2026, 3, 1),
    )
    unemployment = parse_estat_value_json(
        _json_fixture("stat_bureau_estat/unemployment_202602.json"),
        indicator="UNEMPLOYMENT_RATE",
        reference=date(2026, 2, 1),
    )

    assert time_code_for_month(date(2026, 3, 1)) == "2026000303"
    assert cpi.actual == "1.8"
    assert cpi.attrs["cat01"] == "0161"
    assert unemployment.actual == "2.6"
    assert unemployment.attrs["cat02"] == "08"


def test_fetch_stat_bureau_calendar_projects_schedule_rows(
    store: SQLiteEngineStore,
) -> None:
    conn = store.get_connection()
    try:
        summary = fetch_stat_bureau_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=_epoch_ms("2026-04-01T00:00:00+00:00"),
            cpi_html_fetcher=lambda: _fixture(
                "stat_bureau_cpi/release_schedule.html"
            ),
            lfs_html_fetcher=lambda: _fixture(
                "stat_bureau_lfs/release_schedule.html"
            ),
        )
        conn.commit()
    finally:
        conn.close()

    assert summary.releases_parsed == 9
    assert summary.events_upserted == 9
    with store.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT title, reference_date, event_time_utc, actual
            FROM cal_econ_event
            WHERE provider = 'stat-bureau-jp'
            ORDER BY title, reference_date
            """
        ).fetchall()
    assert (
        "Core CPI YoY",
        "2026-03-01",
        "2026-04-23T23:30:00+00:00",
        None,
    ) in [tuple(r) for r in rows]
    assert (
        "Unemployment Rate",
        "2026-02-01",
        "2026-03-30T23:30:00+00:00",
        None,
    ) in [tuple(r) for r in rows]


def test_fetch_stat_bureau_values_fills_pending_rows(
    store: SQLiteEngineStore,
) -> None:
    cpi_entry = parse_cpi_release_schedule_html(
        _fixture("stat_bureau_cpi/release_schedule.html")
    )[3]
    lfs_entry = parse_lfs_release_schedule_html(
        _fixture("stat_bureau_lfs/release_schedule.html")
    )[2]
    with store.get_connection() as conn:
        raw_records = []
        event_records = []
        for entry in (cpi_entry, lfs_entry):
            raw, event = schedule_entry_to_records(
                entry,
                snapshot_epoch_ms=_epoch_ms("2026-04-01T00:00:00+00:00"),
            )
            raw_records.append(raw)
            event_records.append(event)
        store_raw(conn, raw_records)
        project_schedule_events(conn, event_records)
        conn.commit()

    def _json_fetcher(spec, reference: date) -> dict:
        if spec.indicator == "CORE_CPI" and reference == date(2026, 3, 1):
            return _json_fixture("stat_bureau_estat/cpi_core_202603.json")
        if (
            spec.indicator == "UNEMPLOYMENT_RATE"
            and reference == date(2026, 2, 1)
        ):
            return _json_fixture("stat_bureau_estat/unemployment_202602.json")
        raise AssertionError((spec.indicator, reference))

    with store.get_connection() as conn:
        summary = fetch_stat_bureau_values(
            conn,
            dry_run=False,
            snapshot_epoch_ms=_epoch_ms("2026-04-24T01:00:00+00:00"),
            json_fetcher=_json_fetcher,
        )
        conn.commit()

    assert summary.releases_planned == 2
    assert summary.releases_fetched == 2
    assert summary.fetch_failures == []
    assert summary.parse_failures == []
    with store.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT title, reference_date, event_time_utc, actual, source_url
            FROM cal_econ_event
            WHERE provider = 'stat-bureau-jp'
            ORDER BY title
            """
        ).fetchall()
    by_title = {r["title"]: r for r in rows}
    assert by_title["Core CPI YoY"]["actual"] == "1.8"
    assert by_title["Core CPI YoY"]["event_time_utc"] == (
        "2026-04-23T23:30:00+00:00"
    )
    assert by_title["Core CPI YoY"]["source_url"].endswith("sid=0003427113")
    assert by_title["Unemployment Rate"]["actual"] == "2.6"

    with store.get_connection() as conn:
        raw, event = schedule_entry_to_records(
            cpi_entry,
            snapshot_epoch_ms=_epoch_ms("2026-04-25T00:00:00+00:00"),
        )
        store_raw(conn, [raw])
        project_schedule_events(conn, [event])
        conn.commit()

    with store.get_connection() as conn:
        cpi = conn.execute(
            """
            SELECT actual, source_url
            FROM cal_econ_event
            WHERE provider = 'stat-bureau-jp'
              AND title = 'Core CPI YoY'
            """
        ).fetchone()
    assert tuple(cpi) == ("1.8", "https://www.e-stat.go.jp/en/dbview?sid=0003427113")


def test_fetch_stat_bureau_values_replays_dates_without_schedule_rows(
    store: SQLiteEngineStore,
) -> None:
    reference = date(2026, 1, 1)

    def _json_fetcher(spec, value_reference: date) -> dict:
        attrs = {
            f"@{name[2:3].lower()}{name[3:]}": value
            for name, value in spec.estat_params.items()
        }
        actual = "2.0" if spec.indicator == "CORE_CPI" else "2.4"
        return {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0, "ERROR_MSG": "Normal end"},
                "STATISTICAL_DATA": {
                    "DATA_INF": {
                        "VALUE": {
                            **attrs,
                            "@time": time_code_for_month(value_reference),
                            "@unit": "%",
                            "$": actual,
                        }
                    }
                },
            }
        }

    with store.get_connection() as conn:
        dry_run = fetch_stat_bureau_values(
            conn,
            dry_run=True,
            reference_dates=[reference],
        )
        summary = fetch_stat_bureau_values(
            conn,
            dry_run=False,
            snapshot_epoch_ms=_epoch_ms("2026-04-24T01:00:00+00:00"),
            reference_dates=[reference],
            json_fetcher=_json_fetcher,
        )
        conn.commit()

    assert dry_run.releases_planned == 2
    assert summary.releases_planned == 2
    assert summary.releases_fetched == 2
    assert summary.fetch_failures == []
    assert summary.parse_failures == []
    with store.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT title, reference_date, event_time_utc, actual,
                   event_time_precision
            FROM cal_econ_event
            WHERE provider = 'stat-bureau-jp'
            ORDER BY title
            """
        ).fetchall()

    assert [tuple(r) for r in rows] == [
        (
            "Core CPI YoY",
            "2026-01-01",
            "2026-01-01T00:00:00+00:00",
            "2.0",
            "approximate",
        ),
        (
            "Unemployment Rate",
            "2026-01-01",
            "2026-01-01T00:00:00+00:00",
            "2.4",
            "approximate",
        ),
    ]


def test_stat_bureau_canonicalize_aliases() -> None:
    assert canonicalize_indicator("Core CPI YoY") == "CORE_CPI"
    assert canonicalize_indicator("Japan Core CPI") == "CORE_CPI"
    assert canonicalize_indicator("Japan Unemployment Rate") == (
        "UNEMPLOYMENT_RATE"
    )


def test_service_dry_runs_expose_stat_bureau_ops(
    store: SQLiteEngineStore,
) -> None:
    service = LocalMacroDataService(store=store)

    assert service.invoke("calendar_econ_fetch_stat_bureau", {"dry_run": True}) == {
        "dry_run": True,
        "indicators_planned": ["CORE_CPI", "UNEMPLOYMENT_RATE"],
        "stopped_reason": "dry_run",
    }
    values = service.invoke(
        "calendar_econ_fetch_stat_bureau_values",
        {"dry_run": True},
    )
    assert values["dry_run"] is True
    assert values["indicators_planned"] == ["CORE_CPI", "UNEMPLOYMENT_RATE"]
