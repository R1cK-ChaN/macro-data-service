"""Mocked tests for the Destatis calendar connector (issue #15 P2)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.destatis_api import (
    INDICATOR_REGISTRY,
    DestatisCalendarEventRecord,
    DestatisCalendarRawRecord,
    DestatisGenesisClient,
    fetch_destatis_calendar,
    fetch_release_table_html,
    parse_genesis_csv_table,
    parse_observation,
    parse_period,
    parse_release_table_html,
    project_schedule_events,
    schedule_destatis_calendar,
    schedule_entry_to_records,
    store_raw,
)
from ingestion.calendar.destatis_api.parser import PROVIDER
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_text(*parts: str) -> str:
    return (Path(__file__).parent / "fixtures" / Path(*parts)).read_text()


class _FakeDestatisClient:
    def __init__(self, by_table_name: dict[str, str]):
        self.by_table_name = by_table_name
        self.calls: list[dict] = []

    def tablefile(
        self,
        table_name,
        *,
        start_year=None,
        end_year=None,
        extra_params=None,
    ) -> str:
        self.calls.append({
            "table_name": table_name,
            "start_year": start_year,
            "end_year": end_year,
            "extra_params": dict(extra_params or {}),
        })
        return self.by_table_name[table_name]


def test_registry_contains_issue_15_p2_anchors() -> None:
    cpi = INDICATOR_REGISTRY["DESTATIS_CPI_PREL_YOY"]
    assert cpi.table_name == "61111-0004"
    assert cpi.country_code == "DE"
    assert cpi.reference_cadence == "monthly"

    gdp = INDICATOR_REGISTRY["DESTATIS_GDP_FLASH_QOQ"]
    assert gdp.table_name == "81000-0001"
    assert gdp.reference_cadence == "quarterly"
    assert gdp.importance == "high"


def test_parse_period_handles_german_month_and_quarter_labels() -> None:
    assert parse_period("April 2026", cadence="monthly") == date(2026, 4, 1)
    assert parse_period("1. Quartal 2026", cadence="quarterly") == date(
        2026, 3, 31,
    )


def test_genesis_parser_extracts_cpi_and_gdp_rows() -> None:
    cpi_rows = parse_genesis_csv_table(
        _fixture_text("destatis_genesis", "cpi_61111_0004.csv"),
        spec=INDICATOR_REGISTRY["DESTATIS_CPI_PREL_YOY"],
    )
    assert [(row.period, row.value) for row in cpi_rows] == [
        ("2026-03", "2.0"),
        ("2026-04", "2.1"),
    ]

    gdp_rows = parse_genesis_csv_table(
        _fixture_text("destatis_genesis", "gdp_81000_0001.csv"),
        spec=INDICATOR_REGISTRY["DESTATIS_GDP_FLASH_QOQ"],
    )
    assert [(row.period, row.value) for row in gdp_rows] == [
        ("2025-Q4", "0.3"),
        ("2026-Q1", "0.2"),
    ]


def test_genesis_parser_handles_utf8_bom_header() -> None:
    rows = parse_genesis_csv_table(
        (
            "\ufeffZeit;Merkmal;Wert\n"
            "2026-04;Gesamtindex Verbraucherpreisindex "
            "Veraenderung gegenueber Vorjahreszeitraum;2,1\n"
        ),
        spec=INDICATOR_REGISTRY["DESTATIS_CPI_PREL_YOY"],
    )
    assert [(row.period, row.value) for row in rows] == [("2026-04", "2.1")]


def test_parser_projects_value_rows_to_calendar_shape() -> None:
    obs = parse_genesis_csv_table(
        _fixture_text("destatis_genesis", "cpi_61111_0004.csv"),
        spec=INDICATOR_REGISTRY["DESTATIS_CPI_PREL_YOY"],
    )[-1]
    _, event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.provider == PROVIDER == "destatis"
    assert event.reference_date == "2026-04-01"
    assert event.event_time_utc == "2026-04-30T00:00:00+00:00"
    assert event.event_time_precision == "approximate"
    assert event.actual == "2.1"
    assert event.title == "Germany CPI Preliminary YoY"


def test_parser_projects_quarterly_gdp_reference_to_quarter_end() -> None:
    obs = parse_genesis_csv_table(
        _fixture_text("destatis_genesis", "gdp_81000_0001.csv"),
        spec=INDICATOR_REGISTRY["DESTATIS_GDP_FLASH_QOQ"],
    )[-1]
    _, event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.reference_date == "2026-03-31"
    assert event.event_time_utc == "2026-03-31T00:00:00+00:00"
    assert event.title == "Germany GDP Flash QoQ"


def test_schedule_parser_extracts_whitelisted_releases() -> None:
    entries = parse_release_table_html(
        _fixture_text("destatis_schedule", "release_table.html")
    )
    assert [entry.series_id for entry in entries] == [
        "DESTATIS_CPI_PREL_YOY",
        "DESTATIS_GDP_FLASH_QOQ",
    ]
    assert entries[0].reference_date == "2026-04-01"
    assert entries[0].event_time_precision == "date"
    assert entries[0].event_time_utc == "2026-04-29T00:00:00+00:00"
    assert entries[1].reference_date == "2026-03-31"
    assert entries[1].event_time_utc == "2026-04-30T08:00:00+00:00"


def test_schedule_parser_handles_german_umlaut_release_dates() -> None:
    entries = parse_release_table_html(
        """
        <table>
          <tr>
            <th>LfdNr</th><th>EVAS-Nummer</th><th>Pressemitteilung</th>
            <th>Berichtszeitraum</th><th>Erscheinungstermin</th>
          </tr>
          <tr>
            <td>149</td><td>61111</td>
            <td>Verbraucherpreisindex, vorl\u00e4ufige Ergebnisse,
                im Laufe des Tages</td>
            <td>M\u00e4rz 2026</td><td>29. M\u00e4rz 2026</td>
          </tr>
        </table>
        """
    )
    assert entries[0].reference_date == "2026-03-01"
    assert entries[0].event_time_utc == "2026-03-29T00:00:00+00:00"


def test_schedule_filter_keeps_requested_series_only() -> None:
    entries = parse_release_table_html(
        _fixture_text("destatis_schedule", "release_table.html"),
        series_ids={"DESTATIS_GDP_FLASH_QOQ"},
    )
    assert [entry.series_id for entry in entries] == ["DESTATIS_GDP_FLASH_QOQ"]


def test_schedule_and_value_share_provider_event_id() -> None:
    entry = parse_release_table_html(
        _fixture_text("destatis_schedule", "release_table.html")
    )[0]
    _, schedule_event = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    obs = parse_genesis_csv_table(
        _fixture_text("destatis_genesis", "cpi_61111_0004.csv"),
        spec=INDICATOR_REGISTRY["DESTATIS_CPI_PREL_YOY"],
    )[-1]
    _, value_event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_fetcher_preserves_schedule_time_when_value_lands(
    store: SQLiteEngineStore,
) -> None:
    entry = parse_release_table_html(
        _fixture_text("destatis_schedule", "release_table.html")
    )[0]
    raw_schedule, event_schedule = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    client = _FakeDestatisClient({
        "61111-0004": _fixture_text("destatis_genesis", "cpi_61111_0004.csv"),
    })
    with store.get_connection() as conn:
        store_raw(conn, [raw_schedule])
        project_schedule_events(conn, [event_schedule])
        summary = fetch_destatis_calendar(
            conn,
            client,  # type: ignore[arg-type]
            start_year=2026,
            end_year=2026,
            series_ids=["DESTATIS_CPI_PREL_YOY"],
            dry_run=False,
            snapshot_epoch_ms=1_800_000_001_000,
        )
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual "
            "FROM cal_econ_event WHERE provider = 'destatis' "
            "AND reference_date = '2026-04-01'"
        ).fetchone()
    assert summary.series_ok == ["DESTATIS_CPI_PREL_YOY"]
    assert tuple(row) == ("2026-04-29T00:00:00+00:00", "date", "2.1")
    assert client.calls[0]["table_name"] == "61111-0004"
    assert client.calls[0]["start_year"] == 2026


def test_schedule_fetcher_projects_fixture_rows(store: SQLiteEngineStore) -> None:
    with store.get_connection() as conn:
        summary = schedule_destatis_calendar(
            conn,
            start_date="2026-04-01",
            end_date="2026-04-30",
            dry_run=False,
            html_fetcher=lambda: _fixture_text(
                "destatis_schedule", "release_table.html",
            ),
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'destatis'"
        ).fetchone()[0]
    assert summary.entries_parsed == 2
    assert summary.series_ok == [
        "DESTATIS_CPI_PREL_YOY",
        "DESTATIS_GDP_FLASH_QOQ",
    ]
    assert count == 2


def test_release_table_fetch_uses_browser_headers() -> None:
    class _Response:
        text = "<html></html>"

        def raise_for_status(self) -> None:
            return None

    class _Session:
        def __init__(self) -> None:
            self.headers = None

        def get(self, url, *, headers, timeout):  # noqa: ANN001
            self.headers = headers
            return _Response()

    session = _Session()
    assert fetch_release_table_html(session=session) == "<html></html>"  # type: ignore[arg-type]
    assert "Mozilla" in session.headers["User-Agent"]


def test_genesis_client_posts_tablefile_request() -> None:
    class _Response:
        content = b"Zeit;Merkmal;Wert\n2026-04;x;2,1\n"
        encoding = "utf-8"

        def raise_for_status(self) -> None:
            return None

    class _Session:
        def __init__(self) -> None:
            self.request = None

        def post(self, url, *, data, headers, timeout):  # noqa: ANN001
            self.request = {
                "url": url,
                "data": data,
                "headers": headers,
                "timeout": timeout,
            }
            return _Response()

    session = _Session()
    client = DestatisGenesisClient(
        username="token",
        password="GAST",
        session=session,  # type: ignore[arg-type]
    )
    text = client.tablefile("61111-0004", start_year=2026, end_year=2026)
    assert text.startswith("Zeit;Merkmal;Wert")
    assert session.request["data"]["name"] == "61111-0004"
    assert session.request["data"]["format"] == "datencsv"
    assert session.request["headers"]["username"] == "token"


def test_genesis_client_extracts_json_wrapped_table_content() -> None:
    class _Response:
        content = (
            b'{"Status":{"Code":"0","Content":"OK"},'
            b'"Object":{"Content":"Zeit;Merkmal;Wert\\n2026-04;x;2,1\\n"}}'
        )
        encoding = "utf-8"

        def raise_for_status(self) -> None:
            return None

    class _Session:
        def post(self, url, *, data, headers, timeout):  # noqa: ANN001
            return _Response()

    client = DestatisGenesisClient(session=_Session())  # type: ignore[arg-type]
    assert client.tablefile("61111-0004").startswith("Zeit;Merkmal;Wert")


def test_genesis_client_unwraps_zip_response() -> None:
    """Genesis tablefile now returns a ZIP archive containing the CSV.

    Verified against the live API on 2026-04-26: the endpoint always
    answers ``application/zip`` regardless of the ``compress=false``
    request param. The client must detect the PK\\x03\\x04 magic and
    extract the inner ``.csv`` file before decoding.
    """
    import io
    import zipfile

    csv_payload = b"Zeit;Merkmal;Wert\n2026-04;x;2,1\n"
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        zf.writestr("61111-0001_1234_en_flat.csv", csv_payload)
    zip_bytes = zip_buf.getvalue()

    class _Response:
        content = zip_bytes
        encoding = None

        def raise_for_status(self) -> None:
            return None

    class _Session:
        def post(self, url, *, data, headers, timeout):  # noqa: ANN001
            return _Response()

    client = DestatisGenesisClient(session=_Session())  # type: ignore[arg-type]
    assert client.tablefile("61111-0001").startswith("Zeit;Merkmal;Wert")


def test_service_dry_runs_return_plan(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke("calendar_econ_fetch_destatis", {"dry_run": True})
    assert fetch_result["series_planned"] == list(INDICATOR_REGISTRY)
    assert fetch_result["stopped_reason"] == "dry_run"

    schedule_result = svc.invoke(
        "calendar_econ_schedule_destatis",
        {"dry_run": True, "series_ids": ["DESTATIS_GDP_FLASH_QOQ"]},
    )
    assert schedule_result["series_planned"] == ["DESTATIS_GDP_FLASH_QOQ"]
    assert schedule_result["series_unknown"] == []


def test_canonical_aliases_cover_destatis_titles() -> None:
    assert canonicalize_indicator("Germany CPI Preliminary YoY") == "CPI"
    assert canonicalize_indicator("German CPI") == "CPI"
    assert canonicalize_indicator("Germany GDP Flash QoQ") == "GDP"
    assert canonicalize_indicator("Bruttoinlandsprodukt Schnellmeldung") == "GDP"


def test_record_dataclasses_match_shared_projector_shape() -> None:
    assert DestatisCalendarRawRecord.__name__ == "DestatisCalendarRawRecord"
    assert DestatisCalendarEventRecord.__name__ == "DestatisCalendarEventRecord"
