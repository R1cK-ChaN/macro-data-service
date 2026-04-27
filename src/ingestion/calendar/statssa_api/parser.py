"""Stats SA Publication Schedule HTML → calendar projection.

The Stats SA Publication Schedule at
``statssa.gov.za/?page_id=1874`` is a SPA whose schedule grid is
populated client-side by a POST to
``statssa.gov.za/wp-content/themes/umkhanyakude-v2.1/ajax_server.php?req=recently_scheduled_eddie_t``.
The form-encoded body specifies a ``sel_publication`` token (the
target month, e.g. ``"April 2026"``) and a few constants (``selec=200``,
``start=0``, ``page_no=1``) that mirror the page's default request.
The endpoint returns an HTML fragment — a full ``<table>`` of rows
inside a ``<tbody>`` — one row per scheduled release in the month.

Each row is shaped::

    <tr class="odd">
      <td>P0141 - Consumer Price Index (CPI), April 2026</td>
      <td>21 May 2026 (Wednesday)</td>
      <td>10:00&nbsp;...
          <span class="_start">21-05-2026 10:00:00</span>
          ...
          <span class="_description"> Download link: ...?page_id=1854&PPN=P0141</span>
          ...
      </td>
    </tr>

The first cell carries the publication, parsed as
``"<PPN> - <Title>, <ReferencePeriod>"`` — PPN is the canonical
matcher anchor, the trailing reference-period text is parsed against
the indicator's declared cadence (``April 2026`` for monthly,
``1st Quarter 2026`` for quarterly).

The third cell embeds an AddThisEvent metadata block whose ``_start``
span carries the canonical event datetime in
``DD-MM-YYYY HH:MM:SS`` form. The parser pulls that datetime as the
authoritative scheduled release time — the human ``DD Month YYYY
(Weekday)`` form in cell #2 plus the ``HH:MM`` prefix of cell #3 are
preserved as a defensive cross-check but never the primary source.

Time zone is fixed at ``Africa/Johannesburg`` (UTC+2 year-round; no
DST). The localised datetime is converted to UTC and surfaces as
``event_time_utc``.

``provider_event_id`` keys on
``synthesize_event_id(provider, country, canonical, anchor)`` with
the reference period's first day as the anchor — monthly indicators
on the month's first day, quarterly indicators on the quarter's
first day. A rescheduled release for the same data period updates
the existing row instead of spawning a stale-date duplicate.
"""

from __future__ import annotations

import hashlib
import html as html_lib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import StatsSAIndicatorSpec

PROVIDER = "statssa"
STATSSA_RELEASE_TZ = "Africa/Johannesburg"
STATSSA_BASE_URL = "https://www.statssa.gov.za"
# Public landing page — surfaced as ``source_url`` on the event row so
# an operator can browse the row in context.
STATSSA_PUBLIC_SCHEDULE_URL = f"{STATSSA_BASE_URL}/?page_id=1874"
# AJAX endpoint the SPA POSTs to. Form-encoded body; ``sel_publication``
# is a ``"<MonthName> <YYYY>"`` token (e.g. ``"April 2026"``).
STATSSA_SCHEDULE_API_URL = (
    f"{STATSSA_BASE_URL}/wp-content/themes/umkhanyakude-v2.1/"
    f"ajax_server.php?req=recently_scheduled_eddie_t"
)


class StatsSACalendarParseError(ValueError):
    """Stats SA schedule HTML did not expose a parseable schedule."""


# English month names — Stats SA serves the schedule in English. The
# table maps month token (case-insensitive) to month number.
_EN_MONTHS: dict[str, int] = {
    "january":   1, "february":  2, "march":      3,
    "april":     4, "may":       5, "june":       6,
    "july":      7, "august":    8, "september":  9,
    "october":  10, "november": 11, "december":  12,
}

# English ordinal → quarter number. Stats SA quarterly periods read
# ``"1st Quarter 2026"``, ``"2nd Quarter 2026"``, etc.
_EN_QUARTERS: dict[str, int] = {
    "1st": 1, "first":  1,
    "2nd": 2, "second": 2,
    "3rd": 3, "third":  3,
    "4th": 4, "fourth": 4,
}


# ``DD-MM-YYYY HH:MM:SS`` — the AddThisEvent ``_start`` form.
_START_RE = re.compile(
    r'<span\s+class="_start">\s*'
    r'(?P<d>\d{2})-(?P<m>\d{2})-(?P<y>\d{4})\s+'
    r'(?P<H>\d{2}):(?P<M>\d{2}):(?P<S>\d{2})\s*</span>',
    re.IGNORECASE,
)
# AddThisEvent ``_description`` carries the per-row download link with
# the PPN echoed back. Used as a cross-check on the cell-1 PPN parse.
_DOWNLOAD_RE = re.compile(
    r'<span\s+class="_description">\s*Download link:\s*'
    r'(?P<url>https?://[^<\s]+)\s*</span>',
    re.IGNORECASE,
)
# Cell-1 publication string: ``"<PPN> - <Title>, <ReferencePeriod>"``.
# PPNs include letters, digits, dots, and hyphens (``Report-50-11-01``,
# ``P0142.1``). Reference period spans to end-of-cell — accept any
# trailing text including ``,`` so titles with embedded commas survive.
_CELL_PUBLICATION_RE = re.compile(
    r'^\s*(?P<ppn>[A-Za-z0-9.-]+)\s*-\s*'
    r'(?P<rest>.+?)\s*$',
    re.DOTALL,
)
# ``April 2026`` — month name + year.
_MONTH_YEAR_RE = re.compile(
    r'^\s*([A-Za-z]+)\s+(\d{4})\s*$',
)
# ``1st Quarter 2026`` / ``First Quarter 2026``.
_QUARTER_YEAR_RE = re.compile(
    r'^\s*(\d(?:st|nd|rd|th)|first|second|third|fourth)\s+'
    r'Quarter\s+(\d{4})\s*$',
    re.IGNORECASE,
)


@dataclass(frozen=True)
class StatsSAReleaseAnnouncement:
    """One scheduled release row parsed from the AJAX HTML fragment.

    ``release_datetime_local`` is the ``_start`` span's wall-clock value
    interpreted in ``Africa/Johannesburg``; ``release_datetime_utc`` is
    the UTC conversion. ``ppn`` is the Stats SA Publication Number used
    by the matcher; ``title`` and ``reference_period_text`` are the
    other halves of the cell-1 split. ``download_url`` lands in the
    audit payload so the deferred P2 value scrape can target the
    per-release detail page.
    """

    release_datetime_local: datetime
    release_datetime_utc: datetime
    ppn: str
    title: str
    reference_period_text: str
    download_url: str
    schedule_month: str          # the ``sel_publication`` token used to fetch


@dataclass(frozen=True)
class StatsSACalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class StatsSACalendarEventRecord:
    provider: str
    provider_event_id: str
    event_time_utc: str
    event_time_precision: str
    reference_date: str | None
    reference_label: str
    country_code: str
    indicator_id: str | None
    category: str
    title: str
    importance: str | None
    currency: str
    unit: str
    actual: str | None
    previous: str | None
    revised: str | None
    forecast: str | None
    consensus_forecast: str | None
    ticker: str
    source: str
    source_url: str
    content_hash: str
    last_update_epoch_ms: int | None
    observed_at_epoch_ms: int


def _strip_tags(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text)


def _decode_entities(text: str) -> str:
    """Unescape HTML entities (``&nbsp;`` → space, ``&amp;`` → ``&``).

    Stats SA's table cells use ``&nbsp;`` liberally as table padding,
    plus ampersands inside the download URL's query string. The
    parser leans on :mod:`html` for the canonical mapping.
    """
    return html_lib.unescape(text)


def parse_publication_schedule(
    html: str | bytes,
    *,
    schedule_month: str,
) -> list[StatsSAReleaseAnnouncement]:
    """Walk a Stats SA AJAX HTML fragment for parseable schedule rows.

    Returns one :class:`StatsSAReleaseAnnouncement` per row whose
    ``_start`` datetime + cell-1 PPN parse cleanly. Months that have
    no further publications scheduled (Stats SA emits a ``"No further
    publications are scheduled"`` alert block instead of the table)
    return ``[]`` rather than raising — the same month re-fetched
    later in the cycle may carry rescheduled rows.

    Raises :class:`StatsSACalendarParseError` only on payloads that
    aren't HTML at all (binary / truncated response).
    """
    if isinstance(html, (bytes, bytearray)):
        try:
            text = html.decode("utf-8", errors="replace")
        except UnicodeDecodeError as exc:
            raise StatsSACalendarParseError(
                "Stats SA schedule payload is not parseable UTF-8",
            ) from exc
    elif isinstance(html, str):
        text = html
    else:
        raise StatsSACalendarParseError(
            f"Stats SA schedule payload type not supported: "
            f"{type(html).__name__}",
        )

    # The empty-month variant ships only a header + a literal
    # ``"No further publications are scheduled"`` alert block — no
    # ``<table>`` element. Treat *that specific shape* as "nothing
    # scheduled" rather than a parse failure. Any other no-table
    # payload (Cloudflare challenge, AJAX maintenance page, schedule
    # markup that dropped the table) is layout drift — surface it so
    # the daily run trips ``fetch_error`` instead of silently leaving
    # ZA releases missing.
    text_lower = text.lower()
    has_table = "<table" in text_lower
    has_no_publications_alert = "no further publications are scheduled" in text_lower
    if not has_table:
        if has_no_publications_alert:
            return []
        raise StatsSACalendarParseError(
            "Stats SA schedule payload has no <table> and no "
            "'No further publications are scheduled' alert — DOM/API drift",
        )

    table_match = re.search(
        r"<table[^>]*>(?P<body>.*?)</table>",
        text, re.DOTALL | re.IGNORECASE,
    )
    if table_match is None:
        # Same rationale as above — a malformed ``<table>`` open tag
        # without a matching close is a drift signal, not an empty
        # month.
        raise StatsSACalendarParseError(
            "Stats SA schedule has malformed <table> markup — "
            "DOM/API drift",
        )

    body = table_match.group("body")
    rows = re.findall(
        r"<tr[^>]*>(?P<cells>.*?)</tr>",
        body, re.DOTALL | re.IGNORECASE,
    )
    announcements: list[StatsSAReleaseAnnouncement] = []
    for row_html in rows:
        cells = re.findall(
            r"<td[^>]*>(?P<text>.*?)</td>",
            row_html, re.DOTALL | re.IGNORECASE,
        )
        if len(cells) < 3:
            # Header row uses ``<th>`` so it falls through; defensive
            # against future Stats SA layout drift.
            continue
        publication_text = _decode_entities(_strip_tags(cells[0])).strip()
        # The third cell still carries the AddThisEvent metadata in raw
        # span form — feed it to the regex passes verbatim.
        meta_cell = cells[2]

        cell_match = _CELL_PUBLICATION_RE.match(publication_text)
        if cell_match is None:
            continue
        ppn = cell_match.group("ppn").strip()
        rest = cell_match.group("rest").strip()
        # Split rest on the *last* comma — the reference period sits
        # at the tail; titles like "Manufacturing: Production and
        # sales" use no comma, but defensive titles with embedded
        # commas (none observed in fixtures, but possible) keep their
        # full title intact when we anchor on the last delimiter.
        if "," in rest:
            title, reference_period = rest.rsplit(",", 1)
            title = title.strip()
            reference_period = reference_period.strip()
        else:
            title = rest
            reference_period = ""

        start_match = _START_RE.search(meta_cell)
        if start_match is None:
            continue
        try:
            local_dt = datetime(
                int(start_match.group("y")),
                int(start_match.group("m")),
                int(start_match.group("d")),
                int(start_match.group("H")),
                int(start_match.group("M")),
                int(start_match.group("S")),
                tzinfo=ZoneInfo(STATSSA_RELEASE_TZ),
            )
        except ValueError:
            continue
        utc_dt = local_dt.astimezone(timezone.utc)

        download_match = _DOWNLOAD_RE.search(meta_cell)
        download_url = (
            _decode_entities(download_match.group("url"))
            if download_match else ""
        )

        announcements.append(StatsSAReleaseAnnouncement(
            release_datetime_local=local_dt,
            release_datetime_utc=utc_dt,
            ppn=ppn,
            title=title,
            reference_period_text=reference_period,
            download_url=download_url,
            schedule_month=schedule_month,
        ))
    return announcements


def _is_quarterly_period(text: str) -> bool:
    return _QUARTER_YEAR_RE.match(text or "") is not None


def _is_monthly_period(text: str) -> bool:
    match = _MONTH_YEAR_RE.match(text or "")
    if match is None:
        return False
    return match.group(1).lower() in _EN_MONTHS


def _periodo_matches_frequency(periodo: str, frequency: str) -> bool:
    """True when ``periodo`` is empty or matches the indicator's cadence.

    Stats SA assigns distinct PPNs per cadence (P0211 quarterly QLFS
    vs P0277 monthly QES — different surveys), so this filter is a
    layout-drift safety net. If a future PPN ever ships rows under
    multiple cadences (an INEGI-style dual-cadence collision), the
    cadence filter splits them at parse time.
    """
    text = (periodo or "").strip()
    if not text:
        return True
    if frequency == "quarterly":
        return _is_quarterly_period(text)
    # monthly default — reject anything that looks quarterly so a
    # cross-cadence row doesn't bleed into the wrong bucket.
    if _is_quarterly_period(text):
        return False
    return _is_monthly_period(text)


def announcement_matches_spec(
    announcement: StatsSAReleaseAnnouncement,
    spec: StatsSAIndicatorSpec,
) -> bool:
    """True when the announcement's PPN + cadence match the indicator.

    Two conjoined checks:

    1. **PPN equality** — the canonical Stats SA Publication Number,
       case-insensitive (PPNs are uppercase by convention but the
       matcher folds defensively).
    2. **Cadence** — the row's reference-period text shape must match
       the indicator's declared ``frequency``. Defensive against future
       PPN reuse across cadences.
    """
    if announcement.ppn.lower() != spec.ppn.lower():
        return False
    return _periodo_matches_frequency(
        announcement.reference_period_text, spec.frequency,
    )


def _reference_for(
    announcement: StatsSAReleaseAnnouncement,
    spec: StatsSAIndicatorSpec,
) -> tuple[date, str]:
    """Resolve ``(reference_date, reference_label)`` for a release row.

    Anchors on the first day of the reference period.

    - Monthly: ``"<MonthName> <YYYY>"`` → first day of the named
      month/year.
    - Quarterly: ``"<Ordinal> Quarter <YYYY>"`` → first day of the
      named quarter.

    Falls back to the publication-date-minus-one-month for monthly /
    publication-quarter-minus-one for quarterly indicators when
    ``reference_period_text`` is empty (defensive — no observed empty
    periods in the captured fixtures, but Stats SA has shipped pre-
    publication "TBA" rows in the past).
    """
    text = announcement.reference_period_text
    if not text:
        return _fallback_reference(announcement, spec)

    if spec.frequency == "quarterly":
        match = _QUARTER_YEAR_RE.match(text)
        if match is not None:
            ordinal = match.group(1).lower()
            quarter = _EN_QUARTERS.get(ordinal)
            if quarter is not None:
                year = int(match.group(2))
                ref_month = (quarter - 1) * 3 + 1
                ref = date(year, ref_month, 1)
                return ref, f"Q{quarter} {year}"
        return _fallback_reference(announcement, spec)

    # monthly
    match = _MONTH_YEAR_RE.match(text)
    if match is not None:
        month_token = match.group(1).lower()
        month = _EN_MONTHS.get(month_token)
        if month is not None:
            year = int(match.group(2))
            ref = date(year, month, 1)
            return ref, ref.strftime("%B %Y")
    return _fallback_reference(announcement, spec)


def _fallback_reference(
    announcement: StatsSAReleaseAnnouncement,
    spec: StatsSAIndicatorSpec,
) -> tuple[date, str]:
    pub = announcement.release_datetime_local.date()
    if spec.frequency == "quarterly":
        prior_month = pub.month - 1 if pub.month > 1 else 12
        prior_year = pub.year if pub.month > 1 else pub.year - 1
        quarter = (prior_month - 1) // 3 + 1
        ref_month = (quarter - 1) * 3 + 1
        ref = date(prior_year, ref_month, 1)
        return ref, f"Q{quarter} {prior_year}"
    ref_month = pub.month - 1
    ref_year = pub.year
    if ref_month <= 0:
        ref_month += 12
        ref_year -= 1
    ref = date(ref_year, ref_month, 1)
    return ref, ref.strftime("%B %Y")


_HASH_FIELDS: tuple[str, ...] = (
    "indicator", "reference_date", "release_datetime_utc",
    "ppn", "title", "schedule_month",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def announcement_to_records(
    announcement: StatsSAReleaseAnnouncement,
    *,
    spec: StatsSAIndicatorSpec,
    snapshot_epoch_ms: int,
) -> tuple[StatsSACalendarRawRecord, StatsSACalendarEventRecord]:
    """Project a matched announcement onto (raw, event) records."""
    reference_date, reference_label = _reference_for(announcement, spec)
    event_time_utc = (
        announcement.release_datetime_utc
        .isoformat()
        .replace("+00:00", "Z")
    )

    indicator_canonical = canonicalize_indicator(spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        spec.country_code,
        indicator_canonical,
        reference_date.isoformat(),
    )

    payload: dict[str, Any] = {
        "kind":                 "statssa_release_calendar",
        "indicator":            spec.indicator,
        "ppn":                  announcement.ppn,
        "release_date_local":   announcement.release_datetime_local.date().isoformat(),
        "release_time_local":   announcement.release_datetime_local.strftime("%H:%M:%S"),
        "release_datetime_utc": event_time_utc,
        "reference_date":       reference_date.isoformat(),
        "reference_label":      reference_label,
        "reference_period":     announcement.reference_period_text,
        "title":                announcement.title,
        "download_url":         announcement.download_url,
        "schedule_month":       announcement.schedule_month,
        "source_url":           STATSSA_PUBLIC_SCHEDULE_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = StatsSACalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = StatsSACalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=reference_date.isoformat(),
        reference_label=reference_label,
        country_code=spec.country_code,
        indicator_id=None,
        category=spec.category,
        title=spec.title,
        importance=spec.importance,
        currency="ZAR",
        unit=spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Statistics South Africa",
        source_url=STATSSA_PUBLIC_SCHEDULE_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "PROVIDER",
    "STATSSA_BASE_URL",
    "STATSSA_PUBLIC_SCHEDULE_URL",
    "STATSSA_RELEASE_TZ",
    "STATSSA_SCHEDULE_API_URL",
    "StatsSACalendarEventRecord",
    "StatsSACalendarParseError",
    "StatsSACalendarRawRecord",
    "StatsSAReleaseAnnouncement",
    "announcement_matches_spec",
    "announcement_to_records",
    "parse_publication_schedule",
]
