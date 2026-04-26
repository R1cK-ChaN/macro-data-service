"""Issue #47 — index-driven URL discovery + PDF parser dispatch.

BoJ migrated newer monetary-policy statements from per-year HTML pages
(`/en/mopo/mpmdeci/state_<YYYY>/k<YYMMDD>a.htm`) to PDFs at a sister
path (`/en/mopo/mpmdeci/mpr_<YYYY>/k<YYMMDD>a.pdf`) during late 2025.
Confirmed live 2026-04-26: 2025-10-30 and earlier are still ``.htm``;
2025-12-19, 2026-01-23, 2026-03-19 are ``.pdf``. The per-year index
page ``state_<YYYY>/index.htm`` lists the canonical URL for every
meeting that year, mixing both shapes.

Fixtures captured live 2026-04-26:
- ``boj_statement_index/state_2025.html`` — per-year index, mixed
  ``.htm`` + ``.pdf`` (2025-10-30 and earlier are HTML; 2025-12-19
  is PDF).
- ``boj_statement_index/state_2026.html`` — per-year index, all
  ``.pdf`` for 2026 (2026-01-23, 2026-03-19).
- ``boj_statements/k260319a.pdf`` — full statement carrying the
  policy-rate sentence "encourage the uncollateralized overnight
  call rate to remain at around 0.75 percent" and the release line
  "Statement on Monetary Policy -- Thursday, March 19 at 11:46".
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.calendar.boj_api.statements import (
    BojStatementParseError,
    BojStatementUrlNotFoundError,
    discover_statement_url,
    parse_statement_index,
    parse_statement_pdf,
)


INDEX_FIXTURES = Path(__file__).parent / "fixtures" / "boj_statement_index"
STMT_FIXTURES = Path(__file__).parent / "fixtures" / "boj_statements"


def test_index_parser_picks_up_legacy_htm_links() -> None:
    html = (INDEX_FIXTURES / "state_2025.html").read_text(encoding="utf-8")
    found = parse_statement_index(html, year=2025)
    # 2025-03-19 is a known legacy HTML statement.
    assert "250319" in found
    assert found["250319"].endswith("state_2025/k250319a.htm")


def test_index_parser_picks_up_new_pdf_links() -> None:
    html = (INDEX_FIXTURES / "state_2025.html").read_text(encoding="utf-8")
    found = parse_statement_index(html, year=2025)
    # 2025-12-19 is a known PDF statement (post-migration).
    assert "251219" in found
    assert found["251219"].endswith("mpr_2025/k251219a.pdf")


def test_index_parser_handles_pdf_only_year() -> None:
    html = (INDEX_FIXTURES / "state_2026.html").read_text(encoding="utf-8")
    found = parse_statement_index(html, year=2026)
    assert "260123" in found
    assert "260319" in found
    assert all(url.endswith(".pdf") for url in found.values())


def test_discover_statement_url_uses_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """A populated cache must short-circuit the HTTP fetch — that is the
    whole point of `index_cache` for the burst loop."""
    cache: dict[int, dict[str, str]] = {
        2026: {"260319": "https://www.boj.or.jp/en/mopo/mpmdeci/mpr_2026/k260319a.pdf"}
    }
    # Sentinel: any HTTP attempt during this call would fail.
    def _no_http(*args, **kwargs):  # pragma: no cover - guard
        raise AssertionError("discover_statement_url must use cache")
    import requests
    monkeypatch.setattr(requests.Session, "get", _no_http)
    url = discover_statement_url(date(2026, 3, 19), index_cache=cache)
    assert url.endswith("mpr_2026/k260319a.pdf")


def test_discover_statement_url_raises_when_index_lacks_meeting() -> None:
    cache: dict[int, dict[str, str]] = {2025: {}}  # Empty index for 2025.
    with pytest.raises(BojStatementUrlNotFoundError, match="k251231a"):
        discover_statement_url(date(2025, 12, 31), index_cache=cache)


def test_pdf_parser_extracts_rate_from_live_fixture() -> None:
    """End-to-end: real BoJ PDF (2026-03-19 hike to 0.75%) → rate 0.75
    plus release time 11:46 lifted from the schedule block."""
    pdf = (STMT_FIXTURES / "k260319a.pdf").read_bytes()
    value = parse_statement_pdf(pdf, closing_date=date(2026, 3, 19))
    assert value.rate == 0.75
    assert value.rate_text == "0.75"
    assert value.release_time_local == "11:46"


def test_pdf_parser_raises_on_missing_sentence() -> None:
    """Loud-fail on upstream drift mirrors the HTML parser's behaviour."""
    bogus = b"%PDF-1.4 not a real bank-of-japan statement"
    with pytest.raises(BojStatementParseError):
        parse_statement_pdf(bogus, closing_date=date(2026, 3, 19))


def test_statement_fetcher_injection_keeps_simple_shape(tmp_path) -> None:
    """A user-supplied `statement_fetcher` must keep the
    `(closing_date) -> StatementValue` shape — index_cache plumbing is
    an internal detail of the default `fetch_statement`. Otherwise
    manual replays and execute-mode tests break with a TypeError.
    """
    from ingestion.calendar.boj_api import (
        StatementValue,
        fetch_boj_statement_values,
        mpm_entry_to_records,
        project_schedule_events,
        store_raw,
    )
    from ingestion.calendar.boj_api.parser import BojMpmEntry
    from storage.sqlite import SQLiteEngineStore
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    entry = BojMpmEntry(
        year=2025, date_cell="Mar. 18, 19", closing_date=date(2025, 3, 19),
    )
    raw, event = mpm_entry_to_records(entry, snapshot_epoch_ms=1_700_000_000)
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw])
        project_schedule_events(conn, [event])

    seen: list[date] = []

    def simple_fetcher(closing: date) -> StatementValue:
        seen.append(closing)
        return StatementValue(
            closing_date=closing,
            rate=0.5,
            rate_text="0.5",
        )

    far_future_ms = 4_000_000_000_000
    with store._connection(commit=True) as conn:
        summary = fetch_boj_statement_values(
            conn,
            dry_run=False,
            snapshot_epoch_ms=far_future_ms,
            closing_dates=[date(2025, 3, 19)],
            statement_fetcher=simple_fetcher,
        )
    assert seen == [date(2025, 3, 19)]
    assert summary.meetings_fetched == 1
    assert summary.fetch_failures == []
    assert summary.parse_failures == []
