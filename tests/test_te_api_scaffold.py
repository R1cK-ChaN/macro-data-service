"""Scaffold tests for the TE Calendar API fetcher (issue #8 slice 2).

Everything is mocked — these tests never call TE. Key coverage:

- Parser: 22-field round-trip, content-hash stability, revision detection.
- Projector: idempotent raw inserts, event upsert honoring snapshot order.
- Backfill planner: era window widths, phase scoping.
- Backfill runner: dry-run returns plan with no HTTP, adaptive shrink on
  truncated response, cursor advances, budget halt.
- Updates reconciler: new-id pass-through, stale-LastUpdate skip, drop
  reconciliation, URL-length-aware batching.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx

from ingestion.calendar.te_api import (
    BackfillRunner,
    MUTABLE_FIELDS,
    TEAPIClient,
    TEAuthMissing,
    UpdatesReconciler,
    parse_calendar_row,
    plan_windows,
    project_events,
    record_drops,
    row_content_hash,
    store_raw,
)
from ingestion.calendar.te_api.client import URL_MAX_LEN
from storage.sqlite import SQLiteEngineStore


# ──────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────


def _te_row(**overrides):
    base = {
        "CalendarId": "12345",
        "Date": "2026-05-01T12:30:00",
        "Country": "United States",
        "Category": "Inflation",
        "Event": "Core Inflation Rate YoY",
        "Reference": "Apr",
        "ReferenceDate": "2026-04-30T00:00:00",
        "Source": "BLS",
        "SourceURL": "https://bls.gov/cpi",
        "Actual": "3.1%",
        "Previous": "3.2%",
        "Forecast": "3.0%",
        "TEForecast": "3.1%",
        "Revised": "",
        "URL": "/united-states/core-inflation-rate",
        "DateSpan": "0",
        "Importance": 3,
        "Currency": "USD",
        "Unit": "%",
        "Ticker": "USCORECPI",
        "Symbol": "USCORECPI",
        "LastUpdate": "2026-05-01T12:35:00",
    }
    base.update(overrides)
    return base


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


@pytest.fixture()
def connection(store: SQLiteEngineStore):
    conn = store.get_connection()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────
# Parser
# ──────────────────────────────────────────────────────────────────────────


def test_parser_round_trips_all_22_fields() -> None:
    row = _te_row()
    raw, event = parse_calendar_row(row, snapshot_epoch_ms=1_700_000_000_000)
    payload = json.loads(raw.payload_json)
    # Every field in the input is preserved verbatim in the raw payload.
    for key, value in row.items():
        assert payload[key] == value
    # Event projection maps the canonical fields.
    assert event.provider_event_id == "12345"
    assert event.country_code == "US"
    assert event.title == "Core Inflation Rate YoY"
    assert event.importance == "high"
    assert event.actual == "3.1%"
    assert event.forecast == "3.1%"           # TEForecast → forecast
    assert event.consensus_forecast == "3.0%"  # Forecast   → consensus_forecast
    assert event.event_time_precision == "datetime"
    assert event.last_update_epoch_ms is not None


def test_content_hash_stable_across_non_mutable_changes() -> None:
    a = _te_row()
    b = _te_row(Ticker="DIFFERENT", Symbol="DIFFERENT", URL="/other")
    assert row_content_hash(a) == row_content_hash(b)


def test_content_hash_changes_when_actual_updates() -> None:
    baseline = _te_row()
    revision = _te_row(Actual="3.0%", LastUpdate="2026-05-01T13:00:00")
    assert row_content_hash(baseline) != row_content_hash(revision)


def test_mutable_fields_cover_all_revision_columns() -> None:
    assert set(MUTABLE_FIELDS) == {
        "Actual", "Previous", "Revised", "Forecast", "TEForecast", "LastUpdate"
    }


def test_parser_rejects_missing_calendar_id() -> None:
    with pytest.raises(ValueError):
        parse_calendar_row({}, snapshot_epoch_ms=0)


def test_country_code_maps_supra_national_aggregates_from_user_assigned_range() -> None:
    """Supra-national tags have no ISO-3166 alpha-2 code, but downstream
    still needs to filter IMF/OPEC/World/G20/G7 events without string
    matching on `title`. Codes are drawn from ISO's user-assigned QM-QZ
    range so the column stays alpha-2-conformant. These five codes must
    also show up in SUPRA_NATIONAL_CODES — downstream uses that set
    because LIKE 'Q%' would catch Qatar (QA)."""
    from ingestion.calendar.scrapers.tradingeconomics import SUPRA_NATIONAL_CODES
    expected = {
        "IMF":   "QM",
        "OPEC":  "QP",
        "World": "QW",
        "G20":   "QT",
        "G7":    "QS",
    }
    for tag, code in expected.items():
        row = _te_row(Country=tag)
        _, event = parse_calendar_row(row, snapshot_epoch_ms=1)
        assert event.country_code == code, (
            f"{tag!r} should map to {code!r}, got {event.country_code!r}"
        )
        assert code in SUPRA_NATIONAL_CODES
    assert set(expected.values()) == SUPRA_NATIONAL_CODES


def test_country_code_falls_through_to_empty_for_unmapped_tags() -> None:
    """Unknown entities (aggregates we haven't observed yet, typos,
    future sovereigns) fall through to empty rather than onto a wrong
    ISO code. The prior `.upper()[:2]` fallback mis-cast "OECD" → "OE".
    Empty is recoverable (add to TE_COUNTRY_MAP when observed); a wrong
    code silently corrupts country filtering."""
    for tag in ("OECD", "BRICS", "NATO", "ASEAN"):
        row = _te_row(Country=tag)
        _, event = parse_calendar_row(row, snapshot_epoch_ms=1)
        assert event.country_code == "", f"unmapped {tag!r} must stay empty"


def test_country_code_maps_full_sovereign_coverage() -> None:
    """Live P1 backfill (2023→2026) returned 172 distinct Country values.
    All 167 sovereigns must ISO-map; only the 5 supra-national aggregates
    above stay empty. This locks the full mapping table so downstream
    `country_code` filters reach every country the upstream provides."""
    sovereign_cases = {
        # Sampled from every region; regression guard against accidental map shrink.
        "Slovenia": "SI", "Iceland": "IS", "Luxembourg": "LU", "Cyprus": "CY",
        "Estonia": "EE", "Latvia": "LV", "Lithuania": "LT", "Malta": "MT",
        "Albania": "AL", "Bosnia and Herzegovina": "BA", "Kosovo": "XK",
        "United Arab Emirates": "AE", "Qatar": "QA", "Saudi Arabia": "SA",
        "Pakistan": "PK", "Bangladesh": "BD", "Kazakhstan": "KZ",
        "Cambodia": "KH", "Laos": "LA", "Sri Lanka": "LK",
        "Ivory Coast": "CI", "Morocco": "MA", "Kenya": "KE",
        "Congo": "CG", "Republic of the Congo": "CG",
        "Sao Tome and Principe": "ST", "East Timor": "TL",
        "Venezuela": "VE", "Peru": "PE", "Costa Rica": "CR",
        "Trinidad and Tobago": "TT", "Dominican Republic": "DO",
    }
    for country, expected in sovereign_cases.items():
        row = _te_row(Country=country)
        _, event = parse_calendar_row(row, snapshot_epoch_ms=1)
        assert event.country_code == expected, (
            f"{country!r} should map to {expected!r}, got {event.country_code!r}"
        )


def test_country_code_maps_discontinued_coverage_countries() -> None:
    """TE published events for 40 small states / territories through
    2019-2022 before dropping them from the feed — they never appear in
    P1's 2023+ window, so the original map (built from P1 enumeration)
    missed them. P2's 2016-2022 run surfaced 1,773 events across these
    names. Regression guard that every one still ISO-maps — TE never
    sends these anymore, but historical events must stay queryable."""
    discontinued = {
        # Europe micro-states
        "Andorra": "AD", "Liechtenstein": "LI", "Monaco": "MC",
        "San Marino": "SM", "Isle of Man": "IM", "Greenland": "GL",
        # Casing variant observed in raw data
        "Isle Of Man": "IM",
        # Middle East / Africa (small-state coverage)
        "Afghanistan": "AF", "Syria": "SY", "Yemen": "YE", "Sudan": "SD",
        "South Sudan": "SS", "Burkina Faso": "BF", "Chad": "TD",
        "Niger": "NE", "Togo": "TG", "Djibouti": "DJ",
        "Equatorial Guinea": "GQ", "North Korea": "KP",
        # Pacific / island nations
        "Kiribati": "KI", "Micronesia": "FM", "Palau": "PW", "Samoa": "WS",
        "Solomon Islands": "SB", "Tonga": "TO", "Vanuatu": "VU",
        "New Caledonia": "NC", "Northern Mariana Islands": "MP",
        # Caribbean
        "Bahamas": "BS", "Bermuda": "BM", "Cayman Islands": "KY",
        "Aruba": "AW", "Puerto Rico": "PR",
        "Antigua and Barbuda": "AG", "Dominica": "DM", "Grenada": "GD",
        "Haiti": "HT", "Belize": "BZ", "Guyana": "GY",
        "St Kitts and Nevis": "KN", "St Lucia": "LC",
        # "Saint" variants — may appear in future / other TE endpoints
        "Saint Kitts and Nevis": "KN", "Saint Lucia": "LC",
    }
    for country, expected in discontinued.items():
        row = _te_row(Country=country)
        _, event = parse_calendar_row(row, snapshot_epoch_ms=1)
        assert event.country_code == expected, (
            f"{country!r} should map to {expected!r}, got {event.country_code!r}"
        )


# ──────────────────────────────────────────────────────────────────────────
# Projector
# ──────────────────────────────────────────────────────────────────────────


def test_store_raw_is_idempotent(connection: sqlite3.Connection) -> None:
    raw, _event = parse_calendar_row(_te_row(), snapshot_epoch_ms=1_700_000_000_000)
    assert store_raw(connection, [raw]) == 1
    assert store_raw(connection, [raw]) == 0  # same hash — silently ignored
    (count,) = connection.execute("SELECT COUNT(*) FROM cal_econ_raw").fetchone()
    assert count == 1


def test_project_events_upserts_on_revision(connection: sqlite3.Connection) -> None:
    raw1, event1 = parse_calendar_row(_te_row(), snapshot_epoch_ms=1_700_000_000_000)
    raw2, event2 = parse_calendar_row(
        _te_row(Actual="3.0%", LastUpdate="2026-05-01T13:00:00"),
        snapshot_epoch_ms=1_700_000_001_000,
    )
    store_raw(connection, [raw1])
    project_events(connection, [event1])
    (hash_before,) = connection.execute(
        "SELECT content_hash FROM cal_econ_event WHERE provider_event_id='12345'"
    ).fetchone()

    store_raw(connection, [raw2])
    project_events(connection, [event2])
    (hash_after, actual_after) = connection.execute(
        "SELECT content_hash, actual FROM cal_econ_event WHERE provider_event_id='12345'"
    ).fetchone()
    assert hash_after != hash_before
    assert actual_after == "3.0%"

    # Two raw rows preserved (revision history); one event row projected.
    (raw_count,) = connection.execute("SELECT COUNT(*) FROM cal_econ_raw").fetchone()
    (event_count,) = connection.execute("SELECT COUNT(*) FROM cal_econ_event").fetchone()
    assert raw_count == 2
    assert event_count == 1


def test_project_events_rejects_older_snapshot(connection: sqlite3.Connection) -> None:
    """A late-arriving older snapshot must not overwrite a newer projection."""
    _raw_new, event_new = parse_calendar_row(
        _te_row(Actual="FINAL"), snapshot_epoch_ms=2_000_000_000_000
    )
    _raw_old, event_old = parse_calendar_row(
        _te_row(Actual="STALE"), snapshot_epoch_ms=1_000_000_000_000
    )
    project_events(connection, [event_new])
    project_events(connection, [event_old])
    (actual,) = connection.execute(
        "SELECT actual FROM cal_econ_event WHERE provider_event_id='12345'"
    ).fetchone()
    assert actual == "FINAL"


def test_record_drops_upserts(connection: sqlite3.Connection) -> None:
    assert record_drops(connection, "tradingeconomics", ["10", "11"]) == 2
    # Re-recording the same IDs updates last_seen_at but doesn't double-insert.
    assert record_drops(connection, "tradingeconomics", ["10", "11"]) == 2  # UPDATE = change
    (count,) = connection.execute("SELECT COUNT(*) FROM cal_econ_drops").fetchone()
    assert count == 2


# ──────────────────────────────────────────────────────────────────────────
# Backfill planner (no network)
# ──────────────────────────────────────────────────────────────────────────


def test_plan_windows_respects_era_brackets() -> None:
    windows = plan_windows(start=date(2013, 1, 1), end=date(2013, 12, 31))
    for w in windows:
        assert (w.end - w.start).days + 1 <= 180

    windows_recent = plan_windows(start=date(2026, 1, 1), end=date(2026, 1, 31))
    for w in windows_recent:
        assert (w.end - w.start).days + 1 <= 10


def test_plan_windows_splits_across_phase_boundary() -> None:
    # Range crosses p2_mid (ends 2022-12-31) into p1_recent (starts 2023-01-01).
    windows = plan_windows(start=date(2022, 12, 15), end=date(2023, 1, 15))
    phases = {w.phase for w in windows}
    assert phases == {"p1_recent", "p2_mid"}


def test_plan_windows_empty_on_inverted_range() -> None:
    assert plan_windows(start=date(2025, 1, 1), end=date(2024, 1, 1)) == []


# ──────────────────────────────────────────────────────────────────────────
# Client + retry + URL guard
# ──────────────────────────────────────────────────────────────────────────


def test_client_auth_missing_is_raised_only_on_request(monkeypatch) -> None:
    monkeypatch.delenv("TE_API_KEY", raising=False)
    monkeypatch.delenv("TRADINGECONOMICS_API_KEY", raising=False)
    # Isolate env-file fallback from the running user's .env.
    from env import DEFAULT_ENV_FILES, clear_env_cache
    monkeypatch.setattr("env.DEFAULT_ENV_FILES", ())
    clear_env_cache()
    client = TEAPIClient()
    with pytest.raises(TEAuthMissing):
        client.get("/calendar/country/All/2026-05-01/2026-05-02")


@respx.mock
def test_client_retries_on_409(monkeypatch) -> None:
    monkeypatch.setenv("TE_API_KEY", "unit:test")
    route = respx.get(url__startswith="https://api.tradingeconomics.com/calendar/country/All").mock(
        side_effect=[
            httpx.Response(409, text=""),
            httpx.Response(200, json=[_te_row()]),
        ]
    )
    client = TEAPIClient(rate_limit_seconds=0, sleeper=lambda _s: None)
    result = client.get("/calendar/country/All/2026-05-01/2026-05-02")
    assert route.call_count == 2
    assert result.row_count == 1
    assert result.truncated is False


@respx.mock
def test_client_detects_truncation_at_1000_rows(monkeypatch) -> None:
    monkeypatch.setenv("TE_API_KEY", "unit:test")
    rows = [_te_row(CalendarId=str(i)) for i in range(1000)]
    respx.get(url__startswith="https://api.tradingeconomics.com/calendar/country/All").mock(
        return_value=httpx.Response(200, json=rows)
    )
    client = TEAPIClient(rate_limit_seconds=0, sleeper=lambda _s: None)
    result = client.get("/calendar/country/All/2026-05-01/2026-05-10")
    assert result.row_count == 1000
    assert result.truncated is True


def test_client_rejects_overlong_url(monkeypatch) -> None:
    monkeypatch.setenv("TE_API_KEY", "x" * 200)
    client = TEAPIClient()
    from ingestion.calendar.te_api.client import TEURLTooLong
    with pytest.raises(TEURLTooLong):
        client.get("/calendar/country/All/2026-05-01/2026-05-02?padding=" + "a" * 200)


# ──────────────────────────────────────────────────────────────────────────
# Service-level: backfill op (dry run)
# ──────────────────────────────────────────────────────────────────────────


def test_backfill_op_dry_run_emits_no_http(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    with respx.mock(assert_all_called=False) as router:
        router.route().mock(return_value=httpx.Response(500, text="must_not_call"))
        result = service.invoke(
            "calendar_econ_backfill",
            {"from": "2026-04-01", "to": "2026-04-20", "dry_run": True},
        )
        assert router.calls.call_count == 0
    assert result["dry_run"] is True
    assert result["windows_planned"] > 0
    assert "windows" in result
    assert result["stopped_reason"] == "dry_run"


def test_sync_updates_op_dry_run_emits_no_http(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    with respx.mock(assert_all_called=False) as router:
        router.route().mock(return_value=httpx.Response(500, text="must_not_call"))
        result = service.invoke(
            "calendar_econ_sync_updates",
            {"dry_run": True},
        )
        assert router.calls.call_count == 0
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"


# ──────────────────────────────────────────────────────────────────────────
# Backfill runner — executes with mocked TE
# ──────────────────────────────────────────────────────────────────────────


def test_phase_end_marks_is_complete_when_cursor_passes_phase_boundary(
    store: SQLiteEngineStore,
) -> None:
    """Crossing the p3_early boundary (2015-12-31) must flip is_complete."""
    from ingestion.calendar.te_api.backfill import _phase_end

    assert _phase_end("p3_early") == date(2015, 12, 31)
    assert _phase_end("p2_mid") == date(2022, 12, 31)
    assert _phase_end("unknown") is None


@respx.mock
def test_backfill_runner_refetches_truncated_tail_in_session(
    store: SQLiteEngineStore, monkeypatch
) -> None:
    """When TE truncates a response at the 1000-row cap, the runner must
    enqueue a continuation covering [last_date, window.end] **in the same
    session**. Before this fix the runner saved the truncation cursor but
    then advanced to the next planned window, leaving the tail missing
    until a later invocation rewound — and if the final window advanced
    past the phase end, the tail was lost for good (observed on the P2
    2016-2022 backfill: 58 truncated windows → 255 missing days / 51
    consecutive-day runs in `cal_econ_event`)."""
    monkeypatch.setenv("TE_API_KEY", "unit:test")

    # First fetch: 1000 rows clustered on 2026-04-01 → 2026-04-05, triggering
    # truncation at last_date=2026-04-05. Continuation should fetch from
    # 2026-04-05 to 2026-04-10 and return 3 non-overlapping rows.
    truncated_rows = [
        _te_row(CalendarId=str(i), Date=f"2026-04-0{(i % 5) + 1}T00:00:00")
        for i in range(1000)
    ]
    continuation_rows = [
        _te_row(CalendarId=str(2000 + i), Date=f"2026-04-0{5 + i}T00:00:00")
        for i in range(3)  # 2026-04-05..2026-04-07, rest in-range but empty upstream
    ]

    call_log: list[str] = []

    def responder(request):
        call_log.append(str(request.url))
        if len(call_log) == 1:
            return httpx.Response(200, json=truncated_rows)
        return httpx.Response(200, json=continuation_rows)

    respx.get(url__startswith="https://api.tradingeconomics.com/calendar/country/All").mock(
        side_effect=responder
    )

    connection = store.get_connection()
    try:
        client = TEAPIClient(rate_limit_seconds=0, sleeper=lambda _s: None)
        fixed_now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)
        runner = BackfillRunner(
            connection=connection,
            client=client,
            max_requests=5,
            now_utc=lambda: fixed_now,
        )
        # Use a single 10-day window that covers the truncated tail.
        summary = runner.run(
            start=date(2026, 4, 1), end=date(2026, 4, 10), dry_run=False,
        )
        connection.commit()
    finally:
        connection.close()

    assert summary.windows_fetched == 2, "expected 1 original + 1 continuation"
    assert summary.truncated_windows == 1
    assert summary.continuation_fetches == 1
    assert summary.requests_spent == 2
    # Continuation URL must start at last_date=2026-04-05 (inclusive overlap).
    assert "/calendar/country/All/2026-04-05/2026-04-10" in call_log[1]
    # Progress guard — if the continuation also truncates at last_date
    # equal to its own start, no further enqueue. Validated implicitly
    # here because we only returned 2 responses.


@respx.mock
def test_backfill_runner_refetches_tail_when_cap_lands_on_window_end(
    store: SQLiteEngineStore, monkeypatch
) -> None:
    """Boundary case surfaced by Codex review of the P7 truncation fix:
    if the 1000th row's Date equals `window.end`, extra rows on that
    same end date may still have been truncated. Without a continuation
    for the `last_date == window.end` branch, the next planned window
    starts at `end + 1` and the dense end-date tail is silently lost —
    same silent-loss class the deque-enqueue was meant to close."""
    monkeypatch.setenv("TE_API_KEY", "unit:test")
    # All 1000 rows spread across 2026-04-01..2026-04-05 with the 1000th
    # row on the window boundary date.
    truncated_rows = [
        _te_row(CalendarId=str(i), Date=f"2026-04-0{(i // 200) + 1}T00:00:00")
        for i in range(1000)
    ]
    # Continuation should fetch [2026-04-05, 2026-04-05] and return
    # the previously-dropped end-date rows.
    continuation_rows = [
        _te_row(CalendarId=str(2000 + i), Date="2026-04-05T12:00:00")
        for i in range(3)
    ]

    call_log: list[str] = []
    def responder(request):
        call_log.append(str(request.url))
        if len(call_log) == 1:
            return httpx.Response(200, json=truncated_rows)
        return httpx.Response(200, json=continuation_rows)

    respx.get(url__startswith="https://api.tradingeconomics.com/calendar/country/All").mock(
        side_effect=responder
    )

    connection = store.get_connection()
    try:
        client = TEAPIClient(rate_limit_seconds=0, sleeper=lambda _s: None)
        runner = BackfillRunner(
            connection=connection,
            client=client,
            max_requests=5,
            now_utc=lambda: datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc),
        )
        summary = runner.run(
            start=date(2026, 4, 1), end=date(2026, 4, 5), dry_run=False,
        )
        connection.commit()
    finally:
        connection.close()

    assert summary.windows_fetched == 2
    assert summary.truncated_windows == 1
    assert summary.continuation_fetches == 1
    # The continuation must target exactly the boundary date, not [end+1, end].
    assert "/calendar/country/All/2026-04-05/2026-04-05" in call_log[1]


@respx.mock
def test_backfill_runner_progress_guard_advances_past_pathological_day(
    store: SQLiteEngineStore, monkeypatch
) -> None:
    """Pathological 1000+ rows on a single date (window.start) — the
    /calendar/country/All endpoint can't page finer than a date, so the
    rows past the cap on that day are unrecoverable via this path.
    But the runner must not stall on it: cursor advances to
    `window.start + 1` and the remaining tail is enqueued so the rest
    of the window still gets fetched. Saving `cursor=window.start` here
    would cause every later invocation to re-fetch the dense day and
    never advance (surfaced by Codex round 2 on P7)."""
    monkeypatch.setenv("TE_API_KEY", "unit:test")
    # First fetch: 1000 rows all on 2026-04-01 (truncated, last_date ==
    # window.start). Second fetch: tail [2026-04-02..2026-04-05] with 2
    # normal rows so the runner cleanly advances past window.end.
    dense_day_rows = [
        _te_row(CalendarId=str(i), Date="2026-04-01T00:00:00") for i in range(1000)
    ]
    tail_rows = [
        _te_row(CalendarId=str(5000 + i), Date=f"2026-04-0{3 + i}T00:00:00")
        for i in range(2)
    ]
    call_log: list[str] = []
    def responder(request):
        call_log.append(str(request.url))
        if len(call_log) == 1:
            return httpx.Response(200, json=dense_day_rows)
        return httpx.Response(200, json=tail_rows)
    respx.get(url__startswith="https://api.tradingeconomics.com/calendar/country/All").mock(
        side_effect=responder
    )

    connection = store.get_connection()
    try:
        client = TEAPIClient(rate_limit_seconds=0, sleeper=lambda _s: None)
        runner = BackfillRunner(
            connection=connection,
            client=client,
            max_requests=5,
            now_utc=lambda: datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc),
        )
        summary = runner.run(
            start=date(2026, 4, 1), end=date(2026, 4, 5), dry_run=False,
        )
        connection.commit()
    finally:
        connection.close()

    # Runner fetched the dense day + one tail continuation, then stopped.
    assert summary.windows_fetched == 2
    assert summary.continuation_fetches == 1
    assert summary.truncated_windows == 1
    # Tail continuation must start at window.start + 1, not window.start.
    assert "/calendar/country/All/2026-04-02/2026-04-05" in call_log[1]

    # Cursor must be past the dense day, not stuck on it — otherwise a
    # later invocation would re-fetch the same 1000 rows and hit the
    # same truncation on every retry.
    connection = store.get_connection()
    try:
        cur_row = connection.execute(
            "SELECT cursor_date FROM cal_backfill_cursor WHERE phase='p1_recent'"
        ).fetchone()
    finally:
        connection.close()
    assert cur_row is not None
    assert date.fromisoformat(cur_row[0]) > date(2026, 4, 1)


@respx.mock
def test_backfill_runner_executes_and_advances_cursor(
    store: SQLiteEngineStore, monkeypatch
) -> None:
    monkeypatch.setenv("TE_API_KEY", "unit:test")
    respx.get(url__startswith="https://api.tradingeconomics.com/calendar/country/All").mock(
        return_value=httpx.Response(200, json=[_te_row(CalendarId="1"), _te_row(CalendarId="2")])
    )
    connection = store.get_connection()
    try:
        client = TEAPIClient(rate_limit_seconds=0, sleeper=lambda _s: None)
        fixed_now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)
        runner = BackfillRunner(
            connection=connection,
            client=client,
            max_requests=2,
            now_utc=lambda: fixed_now,
        )
        summary = runner.run(
            start=date(2026, 4, 1), end=date(2026, 4, 5), dry_run=False
        )
        connection.commit()
    finally:
        connection.close()

    assert summary.dry_run is False
    assert summary.windows_fetched >= 1
    assert summary.rows_raw_inserted == 2
    assert summary.events_upserted == 2

    # Cursor row was persisted.
    with store._connection(commit=False) as c:
        rows = c.execute(
            "SELECT phase, cursor_date FROM cal_backfill_cursor WHERE provider='tradingeconomics'"
        ).fetchall()
    assert rows, "cursor was not persisted"


# ──────────────────────────────────────────────────────────────────────────
# Updates reconciler — batching + drop reconciliation
# ──────────────────────────────────────────────────────────────────────────


def test_batch_planner_respects_url_length(store: SQLiteEngineStore) -> None:
    connection = store.get_connection()
    try:
        client = TEAPIClient(api_key="unit:test", rate_limit_seconds=0, sleeper=lambda _s: None)
        reconciler = UpdatesReconciler(connection=connection, client=client, batch_id_count=5)
        ids = [str(n) for n in range(30)]  # 30 × 4 chars + 29 commas → 149 chars
        batches = reconciler._plan_batches(ids)
    finally:
        connection.close()
    assert batches, "expected at least one batch"
    assert all(len(b) <= 5 for b in batches)
    # URL overhead check — every batch fits inside budget.
    for batch in batches:
        assert sum(len(cid) for cid in batch) + len(batch) - 1 <= URL_MAX_LEN - 100


@respx.mock
def test_updates_reconciler_detects_drops_and_inserts_new(
    store: SQLiteEngineStore, monkeypatch
) -> None:
    monkeypatch.setenv("TE_API_KEY", "unit:test")
    respx.get(url__startswith="https://api.tradingeconomics.com/calendar/updates").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"CalendarId": "100", "Country": "US", "Event": "CPI", "LastUpdate": "2026-05-01T00:00:00"},
                {"CalendarId": "200", "Country": "US", "Event": "PPI", "LastUpdate": "2026-05-01T00:00:00"},
                {"CalendarId": "300", "Country": "US", "Event": "NFP", "LastUpdate": "2026-05-01T00:00:00"},
            ],
        )
    )
    # /calendarid returns only two of three requested — 300 was retired.
    respx.get(url__regex=r"^https://api\.tradingeconomics\.com/calendar/calendarid/.*").mock(
        return_value=httpx.Response(
            200,
            json=[_te_row(CalendarId="100"), _te_row(CalendarId="200")],
        )
    )
    connection = store.get_connection()
    try:
        client = TEAPIClient(rate_limit_seconds=0, sleeper=lambda _s: None)
        reconciler = UpdatesReconciler(
            connection=connection, client=client, batch_id_count=30,
        )
        summary = reconciler.sync(dry_run=False)
        connection.commit()
    finally:
        connection.close()

    assert summary.updates_fetched == 3
    assert summary.ids_to_rehydrate == 3
    assert summary.batches_fetched == 1
    assert summary.rows_raw_inserted == 2
    assert summary.events_upserted == 2
    assert summary.drops_recorded == 1
    # Only 3 rows returned, well under the 1000-row cap.
    assert summary.updates_truncated is False

    with store._connection(commit=False) as c:
        (drop_id,) = c.execute(
            "SELECT provider_event_id FROM cal_econ_drops"
        ).fetchone()
    assert drop_id == "300"


@respx.mock
def test_updates_reconciler_surfaces_truncation(
    store: SQLiteEngineStore, monkeypatch
) -> None:
    """When /calendar/updates returns 1000 rows the summary must flag
    truncation so the operator knows the tail may be silently dropped."""
    monkeypatch.setenv("TE_API_KEY", "unit:test")
    pointers = [
        {"CalendarId": str(i), "Country": "US", "Event": f"Event {i}",
         "LastUpdate": "2026-05-01T00:00:00"}
        for i in range(1000)
    ]
    respx.get(url__startswith="https://api.tradingeconomics.com/calendar/updates").mock(
        return_value=httpx.Response(200, json=pointers)
    )
    respx.get(url__regex=r"^https://api\.tradingeconomics\.com/calendar/calendarid/.*").mock(
        return_value=httpx.Response(200, json=[])
    )
    connection = store.get_connection()
    try:
        client = TEAPIClient(rate_limit_seconds=0, sleeper=lambda _s: None)
        reconciler = UpdatesReconciler(
            connection=connection, client=client, batch_id_count=50,
        )
        summary = reconciler.sync(dry_run=False)
        connection.commit()
    finally:
        connection.close()
    assert summary.updates_fetched == 1000
    assert summary.updates_truncated is True
