"""Tests for NBS yearly-calendar URL auto-discovery (issue #9 P5a).

Fixtures:
- ``tests/fixtures/nbs_calendar/index.html`` — release-calendar index
  with three years of yearly-article links + a distractor.
- ``tests/fixtures/nbs_calendar/nbs_2026.html`` — existing 2026
  calendar article used by the scaffold tests.

No real HTTP — index-page and article-page fetchers are both
overridable via seams.

Covers:

- Index parser: year match returns absolute URL; missing year
  returns None.
- ``fetch_nbs_calendar_index_html``: retry budget absorbs transient
  failures up to ``retries`` attempts; final exhaustion raises.
- ``discover_nbs_calendar_url``: round-trip through injected fetcher.
- ``fetch_nbs_calendar`` auto-discovery: when ``calendar_url`` is
  omitted, the fetcher pulls the index first, matches the year,
  and then scrapes the resolved article URL.
- Service op ``calendar_econ_fetch_nbs`` with ``auto_discover=true``
  dry-run path.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import requests

from ingestion.calendar.nbs_api import (
    NBSCalendarParseError,
    discover_nbs_calendar_url,
    fetch_nbs_calendar,
    fetch_nbs_calendar_index_html,
    parse_nbs_calendar_index,
)
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "nbs_calendar"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# parse_nbs_calendar_index
# ──────────────────────────────────────────────────────────────────────────


def test_parse_index_finds_year_article() -> None:
    html = _fixture("index.html")
    url = parse_nbs_calendar_index(html, year=2026)
    assert url is not None
    assert url.endswith("t20260105_1234567.html")
    # Absolute URL — relative hrefs resolve against the index URL.
    assert url.startswith("http")


def test_parse_index_honors_specific_year() -> None:
    html = _fixture("index.html")
    url_2025 = parse_nbs_calendar_index(html, year=2025)
    url_2026 = parse_nbs_calendar_index(html, year=2026)
    assert url_2025 is not None
    assert url_2026 is not None
    assert url_2025 != url_2026
    assert "202501" in url_2025
    assert "202601" in url_2026


def test_parse_index_returns_none_when_year_absent() -> None:
    html = _fixture("index.html")
    url = parse_nbs_calendar_index(html, year=2030)
    assert url is None


def test_parse_index_skips_non_calendar_links() -> None:
    """The index page carries unrelated anchors (Statistical Communique
    etc.) that match no year — they must not get picked up."""
    html = (
        "<ul>"
        "<li><a href='/english/help/'>Help</a></li>"
        "<li><a href='/english/Statistical/index.html'>"
        "Statistical Communique</a></li>"
        "</ul>"
    )
    assert parse_nbs_calendar_index(html, year=2026) is None


# ──────────────────────────────────────────────────────────────────────────
# fetch_nbs_calendar_index_html — retry budget
# ──────────────────────────────────────────────────────────────────────────


class _FakeSession:
    """Minimal stand-in for ``requests.Session`` for retry tests.

    ``responses`` is a list of outcomes — either an HTML string
    (successful response) or an exception (raised on that attempt).
    Each ``get`` pops one outcome.
    """

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0
        self.closed = False

    def get(self, url, headers=None, timeout=None):
        self.calls += 1
        outcome = self._responses.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        response = _FakeResponse(outcome)
        return response

    def close(self):
        self.closed = True


class _FakeResponse:
    def __init__(self, text: str):
        self.text = text
        self.status_code = 200

    def raise_for_status(self):
        pass


def test_fetch_index_returns_html_on_first_success() -> None:
    sleeps: list[float] = []
    session = _FakeSession(["<html>ok</html>"])
    html = fetch_nbs_calendar_index_html(
        session=session, retries=2, retry_delay=0.0,
        _sleep=sleeps.append,
    )
    assert html == "<html>ok</html>"
    assert session.calls == 1
    assert sleeps == []


def test_fetch_index_retries_transient_failure() -> None:
    sleeps: list[float] = []
    session = _FakeSession([
        requests.exceptions.ConnectionError("reset"),
        "<html>ok on retry</html>",
    ])
    html = fetch_nbs_calendar_index_html(
        session=session, retries=2, retry_delay=0.25,
        _sleep=sleeps.append,
    )
    assert html == "<html>ok on retry</html>"
    assert session.calls == 2
    assert sleeps == [0.25]


def test_fetch_index_raises_after_exhausting_retries() -> None:
    session = _FakeSession([
        requests.exceptions.ConnectTimeout("t1"),
        requests.exceptions.ConnectTimeout("t2"),
        requests.exceptions.ConnectTimeout("t3"),
    ])
    with pytest.raises(requests.exceptions.RequestException):
        fetch_nbs_calendar_index_html(
            session=session, retries=2, retry_delay=0.0,
            _sleep=lambda _d: None,
        )
    assert session.calls == 3


# ──────────────────────────────────────────────────────────────────────────
# discover_nbs_calendar_url
# ──────────────────────────────────────────────────────────────────────────


def test_discover_returns_resolved_url() -> None:
    index_html = _fixture("index.html")

    def fake_index_fetcher(*, session=None, timeout=30.0, retries=2):
        return index_html

    url = discover_nbs_calendar_url(
        2026, index_fetcher=fake_index_fetcher,
    )
    assert url.endswith("t20260105_1234567.html")


def test_discover_raises_when_year_missing() -> None:
    """The index page has no entry for ``2099`` — the discovery
    contract is loud-fail so operators see the gap rather than
    a silent no-op downstream."""
    index_html = _fixture("index.html")

    def fake_index_fetcher(*, session=None, timeout=30.0, retries=2):
        return index_html

    with pytest.raises(NBSCalendarParseError):
        discover_nbs_calendar_url(
            2099, index_fetcher=fake_index_fetcher,
        )


# ──────────────────────────────────────────────────────────────────────────
# fetch_nbs_calendar — auto-discovery integration
# ──────────────────────────────────────────────────────────────────────────


def test_fetch_auto_discovers_url_when_not_supplied(
    store: SQLiteEngineStore,
) -> None:
    """End-to-end: no ``calendar_url``, ``year=2026`` → the fetcher
    hits the index, resolves the 2026 article URL, then fetches that
    article. Summary surfaces both the resolved URL and the discovery
    flag so operators see what happened."""
    index_html = _fixture("index.html")
    article_html = _fixture("nbs_2026.html")
    captured_article_urls: list[str] = []

    def fake_index_fetcher(*, session=None, timeout=30.0, retries=2):
        return index_html

    def fake_article_fetcher(url: str) -> str:
        captured_article_urls.append(url)
        return article_html

    with store._connection(commit=True) as conn:
        summary = fetch_nbs_calendar(
            conn,
            calendar_url=None,
            year=2026,
            dry_run=False,
            html_fetcher=fake_article_fetcher,
            index_fetcher=fake_index_fetcher,
        )

    assert summary.url_auto_discovered is True
    assert summary.calendar_url.endswith("t20260105_1234567.html")
    assert captured_article_urls == [summary.calendar_url]
    # 12 CPI + 12 PPI + 11 Industrial Production + 11 Fixed Asset
    # Investment + 11 Retail Sales + 12×2 PMI + 4 GDP = 85 entries.
    # PMI's March cell carries the Spring-Festival-delayed Feb date
    # plus the regular Mar 31 date; GDP lands 4 entries from the
    # quarterly-month filter on the "National Economic Performance"
    # row.
    assert summary.entries_parsed == 85
    assert summary.events_upserted == 85


def test_fetch_skips_index_when_calendar_url_supplied(
    store: SQLiteEngineStore,
) -> None:
    """Explicit ``calendar_url`` skips the index round-trip — callers
    doing ad-hoc historical scrapes don't pay the extra hop."""
    article_html = _fixture("nbs_2026.html")
    index_calls: list[None] = []

    def fake_index_fetcher(*, session=None, timeout=30.0, retries=2):
        index_calls.append(None)
        return ""  # never called in this path

    with store._connection(commit=True) as conn:
        summary = fetch_nbs_calendar(
            conn,
            calendar_url="https://www.stats.gov.cn/english/.../t20260105.html",
            dry_run=False,
            html_fetcher=lambda url: article_html,
            index_fetcher=fake_index_fetcher,
        )
    assert summary.url_auto_discovered is False
    assert index_calls == []  # explicit URL bypasses discovery
    assert summary.entries_parsed == 85


# ──────────────────────────────────────────────────────────────────────────
# Service op — auto_discover flag
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_dry_run_reports_auto_discover_flag(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_nbs",
        {"dry_run": True, "auto_discover": True, "year": 2026},
    )
    assert result["dry_run"] is True
    assert result["auto_discover"] is True
    assert result["year"] == 2026


def test_service_op_execute_without_opt_in_still_errors(
    store: SQLiteEngineStore,
) -> None:
    """Service op keeps the explicit guard: omitting both
    ``calendar_url`` and ``auto_discover`` still errors rather than
    silently triggering index HTTP. Callers pick their path."""
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_nbs",
        {"dry_run": False},
    )
    assert "error" in result
    assert "auto_discover" in result["error"]
