"""Scrape ``federalreserve.gov/monetarypolicy/fomccalendars.htm``.

The FOMC calendar page lists every meeting since 2021 plus the two
years forward. Each year sits inside one ``<div class="panel
panel-default">`` element whose header carries a ``<h4>YYYY FOMC
Meetings</h4>``. Inside each panel, every meeting is a
``<div class="row fomc-meeting">`` containing:

- ``<div class="fomc-meeting__month">`` — the month name
  (``"January"``, ``"March"``, …).
- ``<div class="fomc-meeting__date">`` — the date range
  (``"27-28"`` for a two-day meeting, ``"29"`` for one-day,
  ``"17-18*"`` where the asterisk signals a Summary of Economic
  Projections release at this meeting).

Scope:

- Closing day → FOMC_RATE event time (14:00 ET DST-aware).
- ``has_sep=True`` when the asterisk is present.
- Press-conference links, minutes release dates, and statement URLs
  are left on the raw row (``fomc-meeting__minutes``, inner
  ``<a>`` elements) but not extracted into the typed projection.
  Later slices can lift them when a concrete caller needs the data.

Fetch + parse are separable functions. Tests feed fixture HTML
directly to :func:`parse_fomc_calendar_html`; live callers use
:func:`fetch_fomc_calendar_html` which drives :class:`requests.Session`
with a browser-UA header bundle (``federalreserve.gov`` also 403s on
the default ``python-requests`` UA).
"""

from __future__ import annotations

import logging
import re
from datetime import date

import requests
from bs4 import BeautifulSoup

from .parser import FomcMeetingEntry

logger = logging.getLogger(__name__)

FOMC_CALENDAR_URL = (
    "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
)

# Panel header shape: "<h4>2026 FOMC Meetings</h4>".
_YEAR_HEADER_RE = re.compile(r"(\d{4})\s+FOMC\s+Meetings", re.IGNORECASE)

# Date cell shapes:
#   "28"          — single-day meeting
#   "27-28"       — two-day meeting, closing on day 28
#   "17-18*"      — two-day meeting with SEP release
#   "31-1"        — cross-month meeting (paired with month label
#                   "Jan/Feb", "Oct/Nov", "Dec/Jan", …). The closing
#                   day belongs to the *second* month, and a Dec/Jan
#                   pair bumps the year by one.
_DATE_RE = re.compile(
    r"^\s*(\d{1,2})(?:[\s\-–]+(\d{1,2}))?\s*(\*?)\s*$"
)

_MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# Short forms used in cross-month labels ("Jan/Feb", "Oct/Nov",
# "Dec/Jan"). September is truncated to "Sept" on the live page for
# the one month that doesn't fit a clean 3-letter form; the rest
# follow the standard 3-letter abbreviation.
_MONTH_ABBREVS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}


class FomcCalendarParseError(ValueError):
    """Raised when the FOMC calendar DOM deviates from the expected shape.

    Loud-fail is deliberate — upstream DOM drift is the single most
    common breakage mode for HTML scrapers, and silently dropping
    rows would let a calendar-wide miss go undetected until a trader
    notices a missing meeting.
    """


def _resolve_month_label(text: str) -> tuple[int, int | None]:
    """Resolve a meeting-month cell into ``(first_month, second_month)``.

    ``second_month`` is ``None`` for single-month labels (``"January"``,
    ``"March"``). Cross-month pairs (``"Jan/Feb"``, ``"Oct/Nov"``,
    ``"Dec/Jan"``) return both values so the date parser can put the
    closing day in the correct month — the rate decision for a Jan/Feb
    31-1 meeting falls on February 1.

    Raises :class:`FomcCalendarParseError` when the label is neither a
    known full name nor a recognised abbreviation pair.
    """
    key = text.strip().lower()
    full = _MONTH_NAMES.get(key)
    if full is not None:
        return full, None
    if "/" in key:
        left, _, right = key.partition("/")
        left_num = _MONTH_ABBREVS.get(left.strip())
        right_num = _MONTH_ABBREVS.get(right.strip())
        if left_num is not None and right_num is not None:
            return left_num, right_num
    raise FomcCalendarParseError(f"unknown FOMC meeting month: {text!r}")


def _parse_date_cell(
    text: str,
    *,
    year: int,
    first_month: int,
    second_month: int | None,
) -> tuple[date, str, bool]:
    """Return ``(closing_date, normalized_cell, has_sep)``.

    Single-month rows: the closing day is the second number when a
    range is present, or the sole number for a single-day meeting.

    Cross-month rows (``second_month is not None``): the closing day
    belongs to ``second_month``. When the pair wraps the year boundary
    (``"Dec/Jan"``), the closing date uses ``year + 1``.

    ``normalized_cell`` is the verbatim cell (stripped); the connector
    keeps it for audit. ``has_sep`` is ``True`` iff the cell carries a
    trailing asterisk.
    """
    stripped = text.strip()
    match = _DATE_RE.match(stripped)
    if not match:
        raise FomcCalendarParseError(
            f"unparseable FOMC date cell: {text!r}"
        )
    first_raw, second_raw, asterisk = match.groups()
    if second_month is not None:
        # Cross-month rows always come with a range — the closing day
        # lives in ``second_month``. Lone numbers here would mean a
        # single-day meeting straddling the month boundary, which the
        # Fed calendar has never produced; reject to surface drift.
        if second_raw is None:
            raise FomcCalendarParseError(
                f"cross-month meeting without date range: {text!r}"
            )
        closing_day = int(second_raw)
        closing_month = second_month
        closing_year = year + 1 if first_month == 12 and second_month == 1 else year
    else:
        closing_day = int(second_raw) if second_raw is not None else int(first_raw)
        closing_month = first_month
        closing_year = year
    try:
        closing_date = date(year=closing_year, month=closing_month, day=closing_day)
    except ValueError as exc:
        raise FomcCalendarParseError(
            f"invalid closing date in {text!r} "
            f"(year={closing_year}, month={closing_month})"
        ) from exc
    return closing_date, stripped, bool(asterisk)


def _parse_year_from_panel(panel) -> int | None:
    """Extract ``YYYY`` from a panel's ``<h4>`` heading; ``None`` if absent."""
    heading = panel.find(["h4", "h3"])
    if heading is None:
        return None
    match = _YEAR_HEADER_RE.search(heading.get_text(" ", strip=True))
    if match is None:
        return None
    return int(match.group(1))


def parse_fomc_calendar_html(html: str) -> list[FomcMeetingEntry]:
    """Extract :class:`FomcMeetingEntry` rows from FOMC calendar HTML.

    The function walks every ``<div class="panel panel-default">``,
    resolves its year via the inner ``<h4>``, and then iterates every
    ``<div class="row fomc-meeting">`` descendant. Panels without a
    parseable year are skipped silently (the page also renders
    non-meeting panels such as "FOMC Search"); meeting rows with a
    malformed date cell raise :class:`FomcCalendarParseError` so DOM
    drift surfaces loudly.
    """
    soup = BeautifulSoup(html, "html.parser")
    entries: list[FomcMeetingEntry] = []
    for panel in soup.select("div.panel.panel-default"):
        year = _parse_year_from_panel(panel)
        if year is None:
            continue
        # ``fomc-meeting`` is the shared class on both plain and
        # shaded rows; the shaded variant adds ``fomc-meeting--shaded``
        # but retains the base class, so one selector catches both.
        for row in panel.select("div.row.fomc-meeting"):
            month_el = row.find(class_="fomc-meeting__month")
            date_el = row.find(class_="fomc-meeting__date")
            if month_el is None or date_el is None:
                # Header / footer rows inside a panel may share the
                # same container but lack the meeting sub-classes.
                continue
            month_text = month_el.get_text(" ", strip=True)
            first_month, second_month = _resolve_month_label(month_text)
            closing_date, normalized_cell, has_sep = _parse_date_cell(
                date_el.get_text(" ", strip=True),
                year=year,
                first_month=first_month,
                second_month=second_month,
            )
            entries.append(
                FomcMeetingEntry(
                    year=year,
                    month_name=month_text,
                    date_cell=normalized_cell,
                    closing_date=closing_date,
                    has_sep=has_sep,
                )
            )
    return entries


# Browser user-agent header bundle — ``federalreserve.gov`` returns
# 403 Access Denied on the default ``python-requests`` UA. Same
# approach as the BLS schedule scraper; tuned against a live curl
# fetch on 2026-04-21.
#
# ``Accept-Encoding`` advertises only formats ``requests``'s bundled
# urllib3 can decode out of the box. ``br`` (Brotli) needs the
# external ``brotli`` / ``brotlicffi`` package, which isn't in this
# project's declared dependency set — advertising it would mean the
# server could send Brotli-compressed bytes that arrive as undecoded
# binary and fail the downstream parser.
_FED_BROWSER_HEADERS: dict[str, str] = {
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


def fetch_fomc_calendar_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the FOMC calendar page and return HTML text.

    Callers may pass a shared :class:`requests.Session`; the function
    constructs one when ``session is None`` and leaves caller-owned
    sessions unclosed.
    """
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            FOMC_CALENDAR_URL, headers=_FED_BROWSER_HEADERS, timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()
