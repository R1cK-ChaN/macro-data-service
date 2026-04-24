"""Scrape ``boj.or.jp/en/mopo/mpmsche_minu/``.

The BoJ Monetary Policy Meeting schedule page renders one HTML
``<table>`` per year under an ``<h2 id="pYYYY">`` heading. The first
column of each ``<tbody>`` row is the "Date of MPM" cell carrying the
two meeting days, e.g.::

    Jan. 22 (Thurs.), 23 (Fri.)
    Apr. 30 (Wed.), May 1 (Thurs.)

The closing date is the **second** date in the cell; cross-month pairs
(``Apr. 30, May 1``) carry an explicit second-month abbreviation, and
for the rare year-boundary pair (``Dec. 30, Jan. 1``) the closing year
rolls forward.

Fetch + parse are separable functions. Tests feed fixture HTML
directly to :func:`parse_boj_mpm_calendar_html`; live callers use
:func:`fetch_boj_mpm_calendar_html` which drives :class:`requests.Session`
with a browser-UA header bundle (BoJ 403s the default
``python-requests`` UA, same pattern as ``federalreserve.gov``).
"""

from __future__ import annotations

import logging
import re
from datetime import date

import requests
from bs4 import BeautifulSoup

from .parser import BojMpmEntry

logger = logging.getLogger(__name__)

BOJ_MPM_CALENDAR_URL = (
    "https://www.boj.or.jp/en/mopo/mpmsche_minu/"
)

# Year heading shape: ``<h2 id="p2026">2026</h2>``. A separate
# ``p01`` heading holds "Past Monetary Policy Meetings" (archive link
# only, no table); we skip ids that don't match a 4-digit year.
_YEAR_HEADER_ID_RE = re.compile(r"^p(\d{4})$")

# Month abbreviations observed on the live page. "May", "June" and
# "July" render without a trailing period; every other month uses a
# 3- or 4-letter abbreviation ("Jan.", "Feb.", "Mar.", "Apr.",
# "Aug.", "Sept.", "Oct.", "Nov.", "Dec."). ``Sep`` / ``Sept`` both
# appear in historical BoJ pages.
_MONTH_ABBREVS: dict[str, int] = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5,
    "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}

# Match a date token like ``Jan. 22`` or ``May 1`` or ``June 16``.
# Trailing day-of-week in parens (``(Thurs.)``, ``(Fri.)``) is
# matched separately.
_DATE_TOKEN_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,5})\.?\s*(?P<day>\d{1,2})\s*"
    r"(?:\((?P<dow>[^)]+)\))?"
)


class BojMpmCalendarParseError(ValueError):
    """Raised when the BoJ MPM schedule DOM deviates from the expected shape.

    Loud-fail is deliberate — upstream DOM drift is the single most
    common breakage mode for HTML scrapers, and silently dropping
    rows would let the calendar miss a meeting until a trader notices
    it's absent.
    """


def _resolve_month(text: str) -> int:
    key = text.strip().lower().rstrip(".")
    month = _MONTH_ABBREVS.get(key)
    if month is None:
        raise BojMpmCalendarParseError(f"unknown BoJ month token: {text!r}")
    return month


def _parse_date_cell(text: str, *, year: int) -> tuple[date, str]:
    """Return ``(closing_date, cleaned_cell_text)``.

    The cell carries two date tokens separated by a comma. Each token
    may optionally include a day-of-week marker in parens. When the
    second token lacks a month (``23 (Fri.)``) the month falls back to
    the first token's month. Cross-month pairs carry an explicit
    second-month abbreviation (``Apr. 30 (Wed.), May 1 (Thurs.)``); a
    Dec→Jan wrap bumps the closing year by one.
    """
    # Strip the bracketed ``[PDF nnnKB]`` size annotation that appears
    # on link-wrapped cells — it's a link-text suffix the anchor text
    # carries when BoJ publishes the PDF statement. The raw audit text
    # keeps the cleaned form.
    cleaned = re.sub(r"\[PDF[^\]]*\]", "", text).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)

    # Split into two parts at the first comma not inside parens.
    parts = [p.strip() for p in re.split(r",(?![^()]*\))", cleaned, maxsplit=1)]
    if len(parts) != 2:
        raise BojMpmCalendarParseError(
            f"BoJ date cell missing comma-separated pair: {text!r}"
        )
    first_match = _DATE_TOKEN_RE.search(parts[0])
    second_match = _DATE_TOKEN_RE.search(parts[1])
    if first_match is None:
        raise BojMpmCalendarParseError(
            f"BoJ date cell first token unparseable: {text!r}"
        )
    first_month = _resolve_month(first_match.group("month"))

    if second_match is None:
        # Second part might be a bare day number without a month
        # ("23 (Fri.)") — re-parse that shape explicitly.
        bare_day = re.match(r"^(?P<day>\d{1,2})\s*(?:\([^)]+\))?$", parts[1])
        if bare_day is None:
            raise BojMpmCalendarParseError(
                f"BoJ date cell second token unparseable: {text!r}"
            )
        second_month = first_month
        second_day = int(bare_day.group("day"))
    else:
        second_month = _resolve_month(second_match.group("month"))
        second_day = int(second_match.group("day"))

    closing_year = year
    if first_month == 12 and second_month == 1:
        closing_year = year + 1
    try:
        closing_date = date(year=closing_year, month=second_month, day=second_day)
    except ValueError as exc:
        raise BojMpmCalendarParseError(
            f"invalid BoJ closing date in {text!r} "
            f"(year={closing_year}, month={second_month}, day={second_day})"
        ) from exc
    return closing_date, cleaned


def _find_year_tables(soup: BeautifulSoup) -> list[tuple[int, Any]]:
    """Return ``[(year, table_element), ...]`` for every yearly MPM table.

    BoJ anchors each year's table with ``<h2 id="pYYYY">YYYY</h2>``
    immediately before the ``<div class="tbl-box">`` holding the
    table. We walk through the document in order, record the most
    recent year header seen, and attach it to the next table. This is
    robust to decorative markup between the heading and the table
    because the order is always ``h2 → table``.
    """
    pairs: list[tuple[int, Any]] = []
    current_year: int | None = None
    for node in soup.find_all(["h2", "table"]):
        if node.name == "h2":
            header_id = node.get("id") or ""
            match = _YEAR_HEADER_ID_RE.match(header_id)
            if match is None:
                # "Past Monetary Policy Meetings" (id="p01") and similar
                # non-year headings fall through silently — they don't
                # precede a yearly table we want to parse.
                current_year = None
                continue
            current_year = int(match.group(1))
            continue
        if current_year is None:
            continue
        pairs.append((current_year, node))
        # Reset so a second table under the same h2 (rare / unlikely
        # but safe) doesn't silently re-attach to the same year.
        current_year = None
    return pairs


def parse_boj_mpm_calendar_html(html: str) -> list[BojMpmEntry]:
    """Extract :class:`BojMpmEntry` rows from BoJ schedule HTML.

    The function walks every ``<h2 id="pYYYY">``-tagged year table,
    iterates its ``<tbody> <tr>`` rows, and turns the first-column
    "Date of MPM" cell into a closing date. Rows with a malformed
    cell raise :class:`BojMpmCalendarParseError` so DOM drift surfaces
    loudly rather than silently dropping meetings.
    """
    soup = BeautifulSoup(html, "html.parser")
    entries: list[BojMpmEntry] = []
    for year, table in _find_year_tables(soup):
        tbody = table.find("tbody")
        if tbody is None:
            continue
        for row in tbody.find_all("tr", recursive=False):
            cells = row.find_all("td")
            if not cells:
                continue
            cell_text = cells[0].get_text(" ", strip=True)
            if not cell_text:
                continue
            closing_date, cleaned = _parse_date_cell(cell_text, year=year)
            entries.append(
                BojMpmEntry(
                    year=year,
                    date_cell=cleaned,
                    closing_date=closing_date,
                )
            )
    return entries


# Browser UA header bundle — ``boj.or.jp`` 403s on the default
# python-requests UA. Same shape as the Fed scraper's bundle; the
# ``Accept-Encoding`` advertises only formats requests' bundled
# urllib3 decodes out of the box.
_BOJ_BROWSER_HEADERS: dict[str, str] = {
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


def fetch_boj_mpm_calendar_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the BoJ MPM calendar page and return HTML text.

    Callers may pass a shared :class:`requests.Session`; the function
    constructs one when ``session is None`` and leaves caller-owned
    sessions unclosed.
    """
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            BOJ_MPM_CALENDAR_URL,
            headers=_BOJ_BROWSER_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()
