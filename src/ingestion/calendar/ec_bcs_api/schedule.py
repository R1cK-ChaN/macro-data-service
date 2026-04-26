"""European Commission BCS release-date parser."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import (
    EC_BCS_BASE_URL,
    EC_BCS_PRESS_RELEASES_URL,
    EC_BCS_SURVEY_URL,
    INDICATOR_REGISTRY,
    EcBcsIndicatorSpec,
    reference_label_en,
)
from .parser import (
    PROVIDER,
    EcBcsCalendarEventRecord,
    EcBcsCalendarRawRecord,
)

logger = logging.getLogger(__name__)

EC_BCS_RELEASE_TZ = "Europe/Brussels"

# Time → series_id. The 2026 PDF has two streams:
# - Flash Consumer Confidence Indicator at 16h00 CET → EC_BCS_CCI_FLASH
# - Business and consumer survey results (incl. ESI) at 11h00 CET → EC_BCS_ESI
_RELEASE_TIME_TO_SERIES: dict[str, str] = {
    "16h00": "EC_BCS_CCI_FLASH",
    "11h00": "EC_BCS_ESI",
}


class EcBcsScheduleParseError(ValueError):
    """Raised when an EC BCS schedule document cannot be projected."""


@dataclass(frozen=True)
class EcBcsScheduleEntry:
    """One matched EC BCS schedule row, pre-projection."""

    series_id: str
    reference_date: str
    reference_label: str
    release_title: str
    release_date: date
    event_time_utc: str
    event_time_precision: str
    source_url: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class EcBcsResolvedPressRelease:
    """One press-release link resolved from the EC BCS listing page."""

    title: str
    release_date: str
    series_id: str
    source_url: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class EcBcsScheduleDocument:
    """Fetched calendar PDF after text extraction."""

    text: str
    source_url: str


_MONTHS: dict[str, int] = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_DATE_TIME_RE = re.compile(
    r"\b(\d{1,2})\s+"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(20\d{2})\s+"
    r"(\d{2}h\d{2})",
    re.I,
)


def _reference_date_for(release_date: date, series_id: str) -> date:
    """Reference month for the release.

    Both indicators publish the survey results for the calendar month
    of the release. The December full-survey release lands on
    "07 January 2027" but covers December 2026, so the reference
    month is one before the release month for January-published
    results that follow a December schedule entry.
    """
    if (
        series_id == "EC_BCS_ESI"
        and release_date.month == 1
        and release_date.day < 15
    ):
        prev_year = release_date.year - 1
        return date(prev_year, 12, 1)
    return date(release_date.year, release_date.month, 1)


def _event_time(release_date: date, release_time: str) -> str:
    # PDF prints "16h00" — translate to "16:00" for the shared helper.
    formatted = release_time.replace("h", ":")
    scheduled = parse_scheduled_release_time(
        release_date,
        formatted,
        default_tz=EC_BCS_RELEASE_TZ,
    )
    return scheduled.utc.isoformat()


def parse_release_dates_text(
    text: str | bytes,
    *,
    series_ids: set[str] | None = None,
    source_url: str | None = None,
    row_issues: list[str] | None = None,
) -> list[EcBcsScheduleEntry]:
    """Extract EC BCS schedule rows from the publication-dates document."""
    body = (
        text.decode("utf-8", errors="replace") if isinstance(text, bytes) else text
    )
    entries: list[EcBcsScheduleEntry] = []
    seen: set[tuple[str, date]] = set()

    for match in _DATE_TIME_RE.finditer(body):
        day_raw, month_raw, year_raw, time_raw = match.groups()
        time_key = time_raw.lower()
        sid = _RELEASE_TIME_TO_SERIES.get(time_key)
        if sid is None:
            continue
        if series_ids is not None and sid not in series_ids:
            continue
        spec = INDICATOR_REGISTRY.get(sid)
        if spec is None:
            continue
        try:
            release_date = date(
                int(year_raw),
                _MONTHS[month_raw.lower()],
                int(day_raw),
            )
        except Exception as exc:
            if row_issues is not None:
                row_issues.append(f"{sid}: {type(exc).__name__}: {exc}")
                continue
            raise
        key = (sid, release_date)
        if key in seen:
            continue
        seen.add(key)
        reference = _reference_date_for(release_date, sid)
        label = reference_label_en(reference)
        entries.append(
            EcBcsScheduleEntry(
                series_id=sid,
                reference_date=reference.isoformat(),
                reference_label=label,
                release_title=spec.title,
                release_date=release_date,
                event_time_utc=_event_time(release_date, time_raw),
                event_time_precision="datetime",
                source_url=source_url or EC_BCS_SURVEY_URL,
                raw={
                    "release_date": release_date.isoformat(),
                    "release_time": time_raw,
                    "source_url": source_url or EC_BCS_SURVEY_URL,
                },
            )
        )

    if not entries:
        where = f" at {source_url}" if source_url else ""
        raise EcBcsScheduleParseError(
            f"EC BCS calendar contained no recognised schedule rows{where}"
        )
    entries.sort(key=lambda entry: (entry.event_time_utc, entry.series_id))
    return entries


def schedule_entry_to_records(
    entry: EcBcsScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    spec: EcBcsIndicatorSpec | None = None,
) -> tuple[EcBcsCalendarRawRecord, EcBcsCalendarEventRecord]:
    """Project one EC BCS schedule row to raw + event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {entry.series_id!r} not in EC BCS INDICATOR_REGISTRY"
        )
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        entry.reference_date,
    )
    payload = {
        "series_id": entry.series_id,
        "reference_date": entry.reference_date,
        "reference_label": entry.reference_label,
        "release_title": entry.release_title,
        "release_date": entry.release_date.isoformat(),
        "event_time_utc": entry.event_time_utc,
        "event_time_precision": entry.event_time_precision,
        "source_url": entry.source_url,
        "raw": entry.raw,
    }
    payload_json = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    content_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc
    ).isoformat()

    raw_record = EcBcsCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = EcBcsCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=entry.event_time_utc,
        event_time_precision=entry.event_time_precision,
        reference_date=entry.reference_date,
        reference_label=entry.reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="European Commission",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


_EC_BCS_HTTP_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/pdf,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def document_bytes_to_text(data: bytes) -> str:
    """Convert a fetched calendar document into parseable text.

    Mirrors :func:`ingestion.calendar.umich_api.schedule.document_bytes_to_text`
    — try ``pypdf`` first, fall back to ``pdftotext`` / ``mutool`` from
    PATH. Plain text passes through unchanged.
    """
    if not data.lstrip().startswith(b"%PDF"):
        return data.decode("utf-8", errors="replace")
    errors: list[str] = []
    try:
        from pypdf import PdfReader

        reader = PdfReader(BytesIO(data))
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
        if text.strip():
            return text
        errors.append("pypdf extracted empty text")
    except Exception as exc:  # pragma: no cover - depends on optional wheel
        errors.append(f"pypdf: {type(exc).__name__}: {exc}")

    with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
        tmp.write(data)
        tmp.flush()
        commands = [
            ("pdftotext", "-layout", tmp.name, "-"),
            ("mutool", "draw", "-F", "txt", "-o", "-", tmp.name),
        ]
        for command in commands:
            executable = shutil.which(command[0])
            if executable is None:
                continue
            try:
                completed = subprocess.run(
                    (executable, *command[1:]),
                    check=False,
                    capture_output=True,
                    timeout=30.0,
                )
            except Exception as exc:  # pragma: no cover - environment specific
                errors.append(f"{command[0]}: {type(exc).__name__}: {exc}")
                continue
            if completed.returncode == 0 and completed.stdout.strip():
                return completed.stdout.decode("utf-8", errors="replace")
            stderr = completed.stderr.decode("utf-8", errors="replace").strip()
            errors.append(f"{command[0]} exited {completed.returncode}: {stderr}")
    detail = "; ".join(errors) if errors else "pdftotext/mutool unavailable"
    raise EcBcsScheduleParseError(
        f"could not extract EC BCS PDF text: {detail}"
    )


def fetch_survey_page_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the BCS survey landing page that links the annual calendar PDF."""
    owned = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            EC_BCS_SURVEY_URL,
            headers=_EC_BCS_HTTP_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned:
            s.close()


_PUBLICATION_DATES_HREF_RE = re.compile(
    r"publication[\s%0-9a-z]*?dates[\s%0-9a-z]*?(\d{4})\.pdf", re.I
)


def discover_calendar_pdf_url(
    html: str, *, year: int | None = None,
) -> str:
    """Find the year's `Publication dates <YYYY>.pdf` link on the survey page."""
    wanted_year = year or datetime.now(timezone.utc).year
    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "")
        match = _PUBLICATION_DATES_HREF_RE.search(href)
        if match and int(match.group(1)) == wanted_year:
            return urljoin(EC_BCS_SURVEY_URL, href)
    raise EcBcsScheduleParseError(
        f"{wanted_year} EC BCS publication-dates PDF link not found"
    )


def fetch_release_dates_document(
    *,
    year: int | None = None,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> EcBcsScheduleDocument | None:
    """Fetch and extract text from a year's publication-dates PDF.

    Returns ``None`` when the requested year's PDF is not yet linked
    from the survey landing page — the fetcher driver folds this into
    a per-year skip rather than a connector-wide failure so a late-
    December refresh that crosses into next-year's window doesn't
    abort when the next-year PDF hasn't been published.
    """
    owned = session is None
    s = session or requests.Session()
    try:
        info_html = fetch_survey_page_html(session=s, timeout=timeout)
        try:
            url = discover_calendar_pdf_url(info_html, year=year)
        except EcBcsScheduleParseError:
            return None
        response = s.get(url, headers=_EC_BCS_HTTP_HEADERS, timeout=timeout)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return EcBcsScheduleDocument(
            text=document_bytes_to_text(response.content),
            source_url=url,
        )
    finally:
        if owned:
            s.close()


def fetch_press_releases_listing_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the EC BCS press-releases landing page."""
    owned = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            EC_BCS_PRESS_RELEASES_URL,
            headers=_EC_BCS_HTTP_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned:
            s.close()


def fetch_press_release_pdf(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> bytes:
    """GET one EC BCS press-release PDF and return raw bytes."""
    owned = session is None
    s = session or requests.Session()
    try:
        response = s.get(url, headers=_EC_BCS_HTTP_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response.content
    finally:
        if owned:
            s.close()


_MONTH_NAME_PATTERN = (
    r"(January|February|March|April|May|June|July|August|"
    r"September|October|November|December)"
)
_LISTING_LABEL_PATTERNS: dict[str, re.Pattern[str]] = {
    "EC_BCS_ESI": re.compile(
        r"business\s+and\s+consumer\s+survey\s+results", re.I
    ),
    "EC_BCS_CCI_FLASH": re.compile(
        r"flash\s+consumer\s+confidence\s+indicator", re.I
    ),
}


_ECL_META_DATE_RE = re.compile(
    r"\b(?P<day>\d{1,2})\s+(?P<month>[A-Za-z]+)\s+(?P<year>20\d{2})\b",
)


def _extract_meta_date(container: Any) -> date | None:
    """Pull the authoritative release date from an ``ecl-file__detail-meta-item``.

    The EU listing puts the canonical release date in
    ``<li class="ecl-file__detail-meta-item">22 APRIL 2026</li>``. The
    title text on the same card sometimes carries a typo year (the
    January 2026 Flash CCI is labelled ``"22 January 2025"`` on the
    live page) so we never trust title text for date equality —
    metadata is the source of truth.
    """
    if container is None:
        return None
    meta = container.find(
        class_=lambda c: bool(c) and "ecl-file__detail-meta-item" in c.split(),
    )
    if meta is None:
        return None
    match = _ECL_META_DATE_RE.search(meta.get_text(" ", strip=True))
    if match is None:
        return None
    month_key = match.group("month").lower()
    month = _MONTHS.get(month_key)
    if month is None:
        return None
    try:
        return date(int(match.group("year")), month, int(match.group("day")))
    except ValueError:
        return None


def _anchor_card(anchor: Any) -> tuple[str, str, date | None]:
    """Return ``(title, haystack, ecl_meta_date)`` for one listing anchor.

    The current EU listing wraps each press release in a
    ``<div class="ecl-file">`` widget where the anchor's nearest
    ``<div>`` parent is ``ecl-file__action`` (carrying only the
    "Download" link text). The title lives on a sibling
    ``ecl-file__title`` div, the canonical release date on a
    ``ecl-file__detail-meta-item`` list item, and the anchor itself
    carries a ``data-untranslated-label`` mirror of the title.

    We prefer ``data-untranslated-label`` for the title (most reliable,
    present on the anchor itself), walk up to the outer ``ecl-file``
    container so the haystack covers every sub-div, and pull the
    metadata date out of that container so it can be compared exactly
    against the requested ``release_date``. For legacy listings without
    the ECL widget, ``ecl_meta_date`` is ``None`` and the resolver
    falls back to the existing month/long-date matchers.
    """
    anchor_text = anchor.get_text(" ", strip=True)
    label = str(anchor.get("data-untranslated-label") or "").strip()

    ecl_container = anchor.find_parent(
        class_=lambda c: bool(c) and "ecl-file" in c.split()
    )
    container = ecl_container or anchor.find_parent(
        ["li", "article", "div", "tr", "p"],
    ) or anchor
    container_text = container.get_text(" ", strip=True)
    title = label or anchor_text or container_text[:200]
    haystack = " ".join(
        part for part in (label, anchor_text, container_text) if part
    )
    return title, haystack, _extract_meta_date(ecl_container)


def resolve_press_release_link(
    html: str | bytes,
    *,
    series_id: str,
    release_date: date,
) -> EcBcsResolvedPressRelease:
    """Resolve one EC BCS press-release PDF from the listing page."""
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    spec = INDICATOR_REGISTRY.get(series_id)
    if spec is None:
        raise KeyError(f"series_id {series_id!r} not in INDICATOR_REGISTRY")
    label_pattern = _LISTING_LABEL_PATTERNS.get(series_id)
    if label_pattern is None:
        raise KeyError(f"no listing pattern for series_id {series_id!r}")

    soup = BeautifulSoup(text, "html.parser")
    target_month = release_date.strftime("%B %Y")
    target_long_date = release_date.strftime("%-d %B %Y") if hasattr(date, "strftime") else release_date.isoformat()

    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "")
        if "/document/download/" not in href:
            continue
        title, haystack, ecl_meta_date = _anchor_card(anchor)
        if not label_pattern.search(haystack):
            continue
        if ecl_meta_date is not None:
            # ECL widget present — trust the metadata date over any
            # text in title or haystack. Live page sometimes carries a
            # typo year in the title (Jan 2026 Flash CCI labelled
            # "22 January 2025") while the metadata is correct.
            if ecl_meta_date != release_date:
                continue
        elif target_month.lower() not in haystack.lower():
            # Legacy listing — accept either "Month YYYY" or
            # "DD Month YYYY" anywhere in the haystack.
            if target_long_date.lower() not in haystack.lower():
                continue
        return EcBcsResolvedPressRelease(
            title=title,
            release_date=release_date.isoformat(),
            series_id=series_id,
            source_url=urljoin(EC_BCS_BASE_URL, href),
            raw={"listing_text": haystack[:1000]},
        )
    raise EcBcsScheduleParseError(
        f"EC BCS press release not found for {series_id} on {release_date.isoformat()}"
    )


def default_schedule_window(today: date | None = None) -> tuple[date, date]:
    """Default schedule window for the BCS rolling release calendar."""
    base = today or datetime.now(ZoneInfo(EC_BCS_RELEASE_TZ)).date()
    return base - timedelta(days=14), date(base.year + 1, 1, 31)
