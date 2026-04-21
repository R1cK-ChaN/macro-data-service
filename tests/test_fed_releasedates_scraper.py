"""Tests for the Fed release-dates scraper (issue #9 P4a).

Fixture HTML at ``tests/fixtures/fed_releasedates/releasedates.html``
covers the three whitelisted indicators (Beige Book, H.4.1, H.8)
plus distractors the parser must ignore: SEP (handled via the FOMC
calendar scraper), G.19 Consumer Credit, H.15 Selected Interest
Rates.

No real HTTP.

Covers:

- Parser: whitelisted rows extracted; SEP row dropped; off-whitelist
  rows dropped; unknown-date rows captured in ``row_issues``.
- Entry → (raw, event) records: each release gets a distinct
  ``provider_event_id`` (indicator, release_date).
- Projector: rows land with ``actual=NULL`` +
  ``precision='datetime'`` via the full-writer path.
- Service op ``calendar_econ_fetch_fed_releases`` — dry_run +
  fixture injection via ``html_fetcher`` seam.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.fed_api import (
    FED_RELEASEDATES_URL,
    FedReleaseEntry,
    FedReleasedatesParseError,
    INDICATOR_REGISTRY,
    fetch_fed_releasedates,
    parse_releasedates_html,
    release_entry_to_records,
)
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "fed_releasedates"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture() -> str:
    return (FIXTURE_DIR / "releasedates.html").read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# Registry additions
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "series_id,expected_unit,expected_importance",
    [
        ("BEIGE_BOOK", "event", "high"),
        ("FED_H41",    "event", "medium"),
        ("FED_H8",     "event", "low"),
    ],
)
def test_new_registry_entries(
    series_id: str,
    expected_unit: str,
    expected_importance: str,
) -> None:
    spec = INDICATOR_REGISTRY[series_id]
    assert spec.country_code == "US"
    assert spec.unit == expected_unit
    assert spec.importance == expected_importance
    assert spec.category == "Monetary Policy"


# ──────────────────────────────────────────────────────────────────────────
# parse_releasedates_html
# ──────────────────────────────────────────────────────────────────────────


def test_parse_extracts_whitelisted_rows_only() -> None:
    entries = parse_releasedates_html(_fixture())
    counts: dict[str, int] = {}
    for e in entries:
        counts[e.series_id] = counts.get(e.series_id, 0) + 1
    # Fixture: 2 Beige Book, 5 H.4.1, 5 H.8 (15 entries total).
    assert counts == {
        "BEIGE_BOOK": 2,
        "FED_H41":    5,
        "FED_H8":     5,
    }


def test_parse_drops_sep_row() -> None:
    """SEP rows are explicitly excluded — they're surfaced via the
    FOMC calendar's ``has_sep`` flag, not as a separate calendar row."""
    entries = parse_releasedates_html(_fixture())
    titles = [e.release_title for e in entries]
    assert not any("Economic Projections" in t for t in titles)


def test_parse_drops_off_whitelist_rows() -> None:
    """Consumer Credit (G.19) / Selected Interest Rates (H.15) aren't
    in the P4a whitelist — the parser must drop them, not mis-label."""
    entries = parse_releasedates_html(_fixture())
    titles = [e.release_title for e in entries]
    assert not any("Consumer Credit" in t for t in titles)
    assert not any("Selected Interest Rates" in t for t in titles)


def test_parse_resolves_dst_correct_times() -> None:
    """H.4.1 at 4:30 PM ET in January = 21:30 UTC (EST).
    Beige Book at 2:00 PM ET in March = 19:00 UTC (ET has moved to
    EDT by the fixture's March 4 date — ET spring-forward 2026-03-08)."""
    entries = parse_releasedates_html(_fixture())
    jan_h41 = next(
        e for e in entries
        if e.series_id == "FED_H41" and e.release_date == "2026-01-15"
    )
    assert "T21:30" in jan_h41.event_time_utc
    # The fixture has March 4, 2026 (before DST spring-forward Mar 8).
    # Still EST: 14:00 EST = 19:00 UTC.
    mar_beige = next(
        e for e in entries
        if e.series_id == "BEIGE_BOOK" and e.release_date == "2026-03-04"
    )
    assert "T19:00" in mar_beige.event_time_utc


def test_parse_raises_when_no_tables() -> None:
    with pytest.raises(FedReleasedatesParseError):
        parse_releasedates_html("<html><body>no tables</body></html>")


@pytest.mark.parametrize(
    "time_text",
    ["4:30 PM", "4:30 pm", "4:30 p.m.", "4:30 P.M.", "4:30 PM ET"],
)
def test_parse_accepts_all_pm_suffix_variants(time_text: str) -> None:
    """Codex P4a R2 — dotted ``p.m.`` wasn't matching the time regex,
    so H.4.1 / H.8 releases on rows with that suffix landed 12 hours
    early. All documented PM-suffix variants must round-trip to
    16:30 ET → 21:30 UTC."""
    html = (
        "<table>"
        "<tr><th>Date</th><th>Time</th><th>Release</th></tr>"
        f"<tr><td>January 15, 2026</td><td>{time_text}</td>"
        "<td>Factors Affecting Reserve Balances - H.4.1</td></tr>"
        "</table>"
    )
    entries = parse_releasedates_html(html)
    assert len(entries) == 1
    # 4:30 PM EST = 21:30 UTC.
    assert "T21:30" in entries[0].event_time_utc


def test_parse_bare_time_falls_back_to_default() -> None:
    """Codex P4a R2 — a bare ``4:30`` without AM/PM should fall back
    to the per-indicator default (H.4.1 → ``"4:30 PM"``) rather than
    being read as 04:30 ET."""
    html = (
        "<table>"
        "<tr><th>Date</th><th>Time</th><th>Release</th></tr>"
        "<tr><td>January 15, 2026</td><td>4:30</td>"
        "<td>Factors Affecting Reserve Balances - H.4.1</td></tr>"
        "</table>"
    )
    entries = parse_releasedates_html(html)
    assert len(entries) == 1
    assert "T21:30" in entries[0].event_time_utc  # 4:30 PM EST


def test_parse_raises_when_no_whitelist_matches() -> None:
    """A table with zero rows matching the whitelist is surfaced as
    a parse error — blue-sky DOM drift catches this."""
    html = (
        "<table>"
        "<tr><th>Date</th><th>Release</th></tr>"
        "<tr><td>January 7, 2026</td><td>Consumer Credit - G.19</td></tr>"
        "</table>"
    )
    with pytest.raises(FedReleasedatesParseError):
        parse_releasedates_html(html)


def test_parse_captures_row_issues_for_missing_dates() -> None:
    """A whitelisted title with no parseable date is surfaced in
    ``row_issues`` rather than silently dropped."""
    html = (
        "<table>"
        "<tr><th>Date</th><th>Time</th><th>Release</th></tr>"
        "<tr><td>later this month</td><td>2:00 PM</td>"
        "<td>Beige Book</td></tr>"
        "<tr><td>January 21, 2026</td><td>2:00 PM</td>"
        "<td>Beige Book</td></tr>"
        "</table>"
    )
    issues: list[str] = []
    entries = parse_releasedates_html(html, row_issues=issues)
    assert len(entries) == 1
    assert any("no parseable date" in msg for msg in issues)


def test_parse_raises_when_every_matched_row_fails() -> None:
    """Codex P4a R1 — if every whitelisted row fails row-level
    parsing (Fed date/time format drifted on every match), the
    parser must raise so the outage surfaces loudly. ``row_issues``
    alone would be easy to miss on a successful-looking commit."""
    html = (
        "<table>"
        "<tr><th>Date</th><th>Time</th><th>Release</th></tr>"
        "<tr><td>later this month</td><td>2:00 PM</td>"
        "<td>Beige Book</td></tr>"
        "<tr><td>sometime soon</td><td>4:30 PM</td>"
        "<td>Factors Affecting Reserve Balances - H.4.1</td></tr>"
        "</table>"
    )
    issues: list[str] = []
    with pytest.raises(FedReleasedatesParseError):
        parse_releasedates_html(html, row_issues=issues)
    # ``row_issues`` still carries per-row detail for diagnosis.
    assert len(issues) == 2


def test_fetch_reports_parse_failure_when_all_rows_drift(
    store: SQLiteEngineStore,
) -> None:
    """End-to-end: a Fed-wide format drift raises at parse, the
    fetcher captures it in ``fetch_error``, and DB state stays clean."""
    html = (
        "<table>"
        "<tr><th>Date</th><th>Time</th><th>Release</th></tr>"
        "<tr><td>later this month</td><td>2:00 PM</td>"
        "<td>Beige Book</td></tr>"
        "</table>"
    )

    def fake_fetcher():
        return html

    with store._connection(commit=True) as conn:
        summary = fetch_fed_releasedates(
            conn, dry_run=False, html_fetcher=fake_fetcher,
        )
    assert summary.fetch_error is not None
    assert "every row failed" in summary.fetch_error
    assert summary.entries_parsed == 0


# ──────────────────────────────────────────────────────────────────────────
# release_entry_to_records
# ──────────────────────────────────────────────────────────────────────────


def test_entries_get_distinct_provider_event_ids() -> None:
    """Each fixture row lands under a unique ``provider_event_id``
    — same indicator on different dates, or different indicators
    on the same date, don't collide in cal_econ_event."""
    entries = parse_releasedates_html(_fixture())
    ids = set()
    for e in entries:
        _, event = release_entry_to_records(e, snapshot_epoch_ms=0)
        ids.add(event.provider_event_id)
    assert len(ids) == len(entries)


def test_record_shape() -> None:
    entry = FedReleaseEntry(
        series_id="BEIGE_BOOK",
        release_title="Beige Book",
        release_date="2026-01-21",
        release_time_local="2:00 PM",
        event_time_utc="2026-01-21T19:00:00+00:00",
    )
    raw, event = release_entry_to_records(
        entry, snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.provider == "federal-reserve"
    assert event.event_time_precision == "datetime"
    assert event.actual is None
    assert event.title == "Beige Book"
    assert event.currency == "USD"
    assert event.source_url == FED_RELEASEDATES_URL
    assert raw.provider == "federal-reserve"


# ──────────────────────────────────────────────────────────────────────────
# fetch_fed_releasedates
# ──────────────────────────────────────────────────────────────────────────


def test_fetch_lands_all_whitelisted_rows(
    store: SQLiteEngineStore,
) -> None:
    html = _fixture()

    def fake_fetcher():
        return html

    with store._connection(commit=True) as conn:
        summary = fetch_fed_releasedates(
            conn, dry_run=False, html_fetcher=fake_fetcher,
        )
    assert summary.fetch_error is None
    assert summary.entries_parsed == 12  # 2 + 5 + 5
    assert summary.events_upserted == 12
    assert summary.entries_by_indicator == {
        "BEIGE_BOOK": 2, "FED_H41": 5, "FED_H8": 5,
    }

    with store._connection(commit=False) as conn:
        counts = dict(conn.execute(
            "SELECT title, COUNT(*) FROM cal_econ_event "
            "WHERE provider='federal-reserve' "
            "AND source_url LIKE '%releasedates%' "
            "GROUP BY title"
        ).fetchall())
    assert set(counts) == {
        "Beige Book",
        "H.4.1 — Factors Affecting Reserve Balances",
        "H.8 — Assets and Liabilities of Commercial Banks",
    }


def test_fetch_reports_fetch_error(
    store: SQLiteEngineStore,
) -> None:
    def exploding_fetcher():
        raise RuntimeError("upstream 403")

    with store._connection(commit=True) as conn:
        summary = fetch_fed_releasedates(
            conn, dry_run=False, html_fetcher=exploding_fetcher,
        )
    assert summary.fetch_error == "upstream 403"
    assert summary.entries_parsed == 0


def test_fetch_dry_run(store: SQLiteEngineStore) -> None:
    calls: list[None] = []

    def never():
        calls.append(None)
        return ""

    with store._connection(commit=False) as conn:
        summary = fetch_fed_releasedates(
            conn, dry_run=True, html_fetcher=never,
        )
    assert summary.dry_run is True
    assert summary.indicators_planned == ["BEIGE_BOOK", "FED_H41", "FED_H8"]
    assert calls == []


# ──────────────────────────────────────────────────────────────────────────
# Service op
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_dry_run(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_fed_releases", {"dry_run": True},
    )
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert result["indicators_planned"] == [
        "BEIGE_BOOK", "FED_H41", "FED_H8",
    ]
