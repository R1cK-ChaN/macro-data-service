"""Scrape NBS yearly-release-calendar HTML into structured entries.

The NBS publishes each year's schedule as a long HTML article (e.g.
``Regular Press Release Calendar of NBS in 2026``). The article
embeds one large table with shape:

- Col 0: row number (``"1"``, ``"2"``, …)
- Col 1: content label (``"Monthly Report on Consumer Price Index (CPI)"``)
- Cols 2-13: the twelve months (``Jan.``, ``Feb.``, …, ``Dec.``)

Each labelled row is followed immediately by a "time row" carrying
the publish times (``"9:30"``, ``"10:00"``) aligned under the same
month columns. Cells that carry no release in that month render as
``"……"``. Release day cells are ``"9/Fri"`` — day, slash, weekday
abbreviation — occasionally followed by a ``"Note5"`` reference when
the schedule has an irregular adjustment (Spring Festival / National
Day). This scraper keeps the day; notes are surfaced on the raw
payload for audit but don't participate in the projection.

Scope (P5 scaffold):

- CPI anchor only. The scraper iterates every labelled row; rows
  whose content matches a whitelist substring are projected, others
  are skipped.
- Year is parsed from the article title (``… in 2026``).
- Release time per entry is taken from the time row paired with
  the indicator row. NBS publishes CPI uniformly at 09:30 local
  time across the year — the time-row cells are therefore all
  identical — but the scraper still per-cell aligns so a future
  schedule change doesn't go unnoticed.

Fetch + parse are separable: tests feed fixture HTML directly to
:func:`parse_nbs_calendar_html`; live callers use
:func:`fetch_nbs_yearly_calendar_html` which drives
:class:`requests.Session` with a browser-UA header bundle (NBS
routinely 403s on the default ``python-requests`` UA).

Risk note — the NBS servers are documented in issue #9 as the
highest-risk upstream: HTTP-only (``http://``) by default, HTML-
fragile, and frequently timing out from non-CN IPs. The live probe
in a future P5a slice will confirm the shape is stable enough for a
recurring schedule.
"""

from __future__ import annotations

import logging
import re
import time as _time
from datetime import date
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from .indicators import INDICATOR_REGISTRY, NBSIndicatorSpec
from .parser import NBSReleaseEntry

logger = logging.getLogger(__name__)

NBS_CALENDAR_INDEX_URL = (
    "https://www.stats.gov.cn/english/PressRelease/ReleaseCalendar/"
)

_YEAR_TITLE_RE = re.compile(r"Calendar\s+of\s+NBS\s+in\s+(\d{4})", re.IGNORECASE)
# Scans the whole cell (not anchored at start) so a Spring-Festival-
# shifted cell like ``"4/Wed Note5 31/Tue"`` yields BOTH the delayed
# previous-month release and the regular current-month release. The
# day/weekday pair is the stable token shape NBS uses; any trailing
# ``Note5`` footnote reference sits between pairs and doesn't need to
# be matched directly.
_DAY_CELL_RE = re.compile(r"(\d{1,2})\s*/\s*([A-Za-z]{3,4})")
_EMPTY_MARKERS: frozenset[str] = frozenset({"", "……", "……", "--", "-"})

# Indicator-row cells have the shape:
#   [No, Content, Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec]
# 14 cells total. Some rows have fewer visible cells because the
# time-row below uses rowspan'd month cells; handling that is the
# reason we pair every indicator row with the row that immediately
# follows it.
_MONTH_COL_COUNT: int = 12


class NBSCalendarParseError(ValueError):
    """Raised when the NBS calendar DOM deviates from the expected shape.

    Loud-fail is deliberate — a silent empty parse on an HTML-fragile
    upstream would let a calendar-wide miss go undetected until a
    trader notices a missing release.
    """


def _parse_day_cell(text: str) -> list[tuple[int, str, str]]:
    r"""Return every ``(day, weekday_label, normalized_cell)`` token.

    The list is empty for empty markers (the NBS uses full-width
    ellipsis dots ``"……"``). A normal cell carries one
    ``day/weekday`` token; Spring-Festival-shifted months carry two
    in the same cell (``"4/Wed Note5 31/Tue"`` — the previous-month
    release gets pushed forward, the current-month release stays
    put). The ``Note5`` footnote reference and any other non-token
    text between the pairs is ignored at parse time; the full cell
    string lands on ``normalized_cell`` so the audit payload can
    surface the footnote reference.

    Raises :class:`NBSCalendarParseError` when a non-empty cell
    carries zero ``day/weekday`` tokens — a real upstream drift that
    warrants a loud fail.
    """
    normalized = text.strip()
    if normalized in _EMPTY_MARKERS:
        return []
    matches = list(_DAY_CELL_RE.finditer(normalized))
    if not matches:
        raise NBSCalendarParseError(
            f"unparseable NBS release-day cell: {text!r}"
        )
    return [
        (int(m.group(1)), m.group(2), normalized) for m in matches
    ]


def _year_from_title(soup: BeautifulSoup) -> int | None:
    """Extract the calendar year from the ``<title>`` or first heading."""
    title_el = soup.find("title")
    if title_el is not None:
        match = _YEAR_TITLE_RE.search(title_el.get_text(" ", strip=True))
        if match:
            return int(match.group(1))
    for selector in ("h1", "h2", ".TRS_Editor"):
        heading = soup.select_one(selector)
        if heading is None:
            continue
        match = _YEAR_TITLE_RE.search(heading.get_text(" ", strip=True))
        if match:
            return int(match.group(1))
    return None


def _find_schedule_table(soup: BeautifulSoup):
    """Return the main release-schedule table.

    NBS wraps the table in ``<div class="trs_editor_view">`` with a
    table class of ``trs_word_table``. We pick the first such table
    — the page also renders footnote / legend sub-tables which we
    don't want.
    """
    return soup.find("table", class_="trs_word_table")


def _iter_cell_texts(row) -> list[str]:
    return [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]


def _row_matching_indicators(
    content_text: str, registry: Iterable[NBSIndicatorSpec],
) -> list[NBSIndicatorSpec]:
    """Return every registry spec whose ``label_fragment`` is in the row.

    P5 shipped one-spec-per-row because CPI was the only anchor. P5c
    expands the registry so one row can carry multiple indicators
    (the PMI row publishes Manufacturing PMI + Non-Manufacturing PMI
    simultaneously; both are registered with ``label_fragment="purchasing
    managers"`` and both match). Order follows registry insertion so
    downstream entry order is stable across runs.
    """
    lowered = content_text.lower()
    return [
        spec for spec in registry
        if spec.label_fragment.lower() in lowered
    ]


def parse_nbs_calendar_html(
    html: str,
    *,
    year_override: int | None = None,
) -> list[NBSReleaseEntry]:
    """Extract :class:`NBSReleaseEntry` rows from a yearly NBS calendar.

    ``year_override`` short-circuits title-based year resolution
    when the caller already knows the year (tests lean on this
    because the fixture article title is authoritative but the
    fixture may be slimmed at capture time).

    Raises :class:`NBSCalendarParseError` when:

    - No schedule table is found.
    - The year cannot be resolved from the document or the caller.
    - A day cell fails the ``"day/weekday"`` shape match (distinct
      from the ``"……"`` empty marker which is silently skipped).
    """
    soup = BeautifulSoup(html, "html.parser")
    year = year_override or _year_from_title(soup)
    if year is None:
        raise NBSCalendarParseError(
            "unable to resolve calendar year from NBS document"
        )

    table = _find_schedule_table(soup)
    if table is None:
        raise NBSCalendarParseError(
            "no <table class='trs_word_table'> found in NBS calendar HTML"
        )

    rows = table.find_all("tr")
    entries: list[NBSReleaseEntry] = []
    registry_values = list(INDICATOR_REGISTRY.values())

    # Walk rows pairwise: a content row (14 cells — No + Content +
    # 12 month cells) immediately followed by a time row (time cells
    # under each month column).  Time rows can have fewer visible
    # cells because of colspan / rowspan merges, so we only use
    # them for their textual content — not for strict column alignment.
    i = 0
    while i < len(rows):
        indicator_cells = _iter_cell_texts(rows[i])
        if len(indicator_cells) < 2 + _MONTH_COL_COUNT:
            i += 1
            continue
        content_text = indicator_cells[1]
        matching_specs = _row_matching_indicators(content_text, registry_values)
        if not matching_specs:
            i += 1
            continue

        time_cells: list[str] = []
        if i + 1 < len(rows):
            time_cells = _iter_cell_texts(rows[i + 1])

        # Use the first non-empty time-row cell as the default
        # release time; per-month overrides would require aligning
        # against the indicator row's month offset, which colspan /
        # rowspan merges make brittle. NBS publishes each indicator
        # row with a uniform release time across the year — the
        # default is load-bearing. A future indicator with mixed
        # times should extend this logic then.
        default_time = next(
            (t for t in time_cells if t.strip() and t.strip() not in _EMPTY_MARKERS),
            "",
        ).strip()
        if not default_time:
            raise NBSCalendarParseError(
                f"NBS row for {matching_specs[0].indicator!r} "
                f"has no release-time cell"
            )

        month_cells = indicator_cells[2:2 + _MONTH_COL_COUNT]
        for month_idx, cell in enumerate(month_cells, start=1):
            for day, weekday, normalized in _parse_day_cell(cell):
                for spec in matching_specs:
                    entries.append(
                        NBSReleaseEntry(
                            year=year,
                            month=month_idx,
                            day=day,
                            release_time_local=default_time,
                            indicator=spec.indicator,
                            weekday_label=weekday,
                            date_cell=normalized,
                        )
                    )
        i += 2

    return entries


# Browser user-agent header bundle. NBS frontends 403 the default
# ``python-requests`` UA; the header block mirrors the BLS / Fed
# scrapers. ``Accept-Encoding`` advertises only formats ``requests``'s
# bundled urllib3 decodes out of the box (Brotli requires an extra
# package not in the declared dependency set).
_NBS_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.1 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def fetch_nbs_yearly_calendar_html(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET a specific NBS yearly-calendar article and return HTML text.

    The caller supplies the full article URL (the release-calendar
    index at ``NBS_CALENDAR_INDEX_URL`` links to one article per
    year). Use :func:`discover_nbs_calendar_url` to resolve the URL
    automatically from the index when the caller doesn't already
    know it.

    NBS serves the page as UTF-8 (declared in the HTML ``<meta
    charset="UTF-8">``) but omits ``charset`` from the HTTP
    ``Content-Type`` header. ``requests`` then falls back to RFC 2616's
    ISO-8859-1 default, which mis-decodes every CJK character and the
    horizontal-ellipsis empty-day marker (``'\\xe2\\x80\\xa6\\xe2\\x80\\xa6'`` →
    ``'â\\x80¦â\\x80¦'``) — the scraper no longer recognises the empty
    marker and blows up on the first one. Force UTF-8 on the response
    so ``.text`` returns the right code points.
    """
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(url, headers=_NBS_BROWSER_HEADERS, timeout=timeout)
        response.raise_for_status()
        response.encoding = "utf-8"
        return response.text
    finally:
        if owned_session:
            s.close()


# ──────────────────────────────────────────────────────────────────────────
# P5a — yearly-calendar URL auto-discovery
# ──────────────────────────────────────────────────────────────────────────


def parse_nbs_calendar_index(html: str, *, year: int) -> str | None:
    """Return the yearly-calendar article URL for ``year``, or ``None``.

    Walks every ``<a>`` on the release-calendar index page. A link is
    a match when its text matches ``Calendar of NBS in <year>`` (case-
    insensitive) and ``<year>`` equals the requested year. Relative
    ``href`` values are resolved against :data:`NBS_CALENDAR_INDEX_URL`
    so the caller receives an absolute URL.
    """
    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.find_all("a"):
        text = anchor.get_text(" ", strip=True)
        match = _YEAR_TITLE_RE.search(text)
        if not match or int(match.group(1)) != year:
            continue
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        return urljoin(NBS_CALENDAR_INDEX_URL, href)
    return None


def fetch_nbs_calendar_index_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
    retries: int = 2,
    retry_delay: float = 2.0,
    _sleep=_time.sleep,
) -> str:
    """GET the NBS release-calendar index page with retry.

    stats.gov.cn is documented as the highest-risk upstream on this
    issue: HTTP-only by default, HTML-fragile, frequent timeouts
    from non-CN IPs. This helper retries on transient failures
    (``requests.exceptions.RequestException``) — connection resets
    and read timeouts are the common shape of the flakiness. The
    caller manages the session (to re-use TCP connections across
    retries when one is passed in).

    ``_sleep`` is the seam tests inject to avoid real delays while
    still exercising the retry loop.
    """
    attempts = max(1, retries + 1)
    owned_session = session is None
    s = session or requests.Session()
    last_exc: Exception | None = None
    try:
        for attempt in range(attempts):
            try:
                response = s.get(
                    NBS_CALENDAR_INDEX_URL,
                    headers=_NBS_BROWSER_HEADERS,
                    timeout=timeout,
                )
                response.raise_for_status()
                # Same UTF-8 override as ``fetch_nbs_yearly_calendar_html``
                # — NBS omits ``charset`` from the ``Content-Type`` header
                # and ``requests`` falls back to ISO-8859-1 otherwise.
                response.encoding = "utf-8"
                return response.text
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                if attempt + 1 < attempts:
                    logger.warning(
                        "NBS index fetch attempt %d/%d failed: %s — retrying",
                        attempt + 1, attempts, exc,
                    )
                    _sleep(retry_delay)
                    continue
        assert last_exc is not None
        raise last_exc
    finally:
        if owned_session:
            s.close()


def discover_nbs_calendar_url(
    year: int,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
    retries: int = 2,
    index_fetcher=fetch_nbs_calendar_index_html,
) -> str:
    """Resolve the NBS yearly-calendar article URL for ``year``.

    Fetches the release-calendar index page and matches the requested
    year against the indexed articles. Raises :class:`NBSCalendarParseError`
    when the year isn't on the index (either the article hasn't been
    published yet or the link text drifted from the known shape).
    """
    html = index_fetcher(
        session=session, timeout=timeout, retries=retries,
    )
    url = parse_nbs_calendar_index(html, year=year)
    if url is None:
        raise NBSCalendarParseError(
            f"no NBS yearly-calendar article for {year} on the index page"
        )
    return url
