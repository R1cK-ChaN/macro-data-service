"""U Michigan Consumer Sentiment release-date scraper."""

from __future__ import annotations

import hashlib
from io import BytesIO
import json
import logging
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import (
    INDICATOR_REGISTRY,
    UMichIndicatorSpec,
    UMICH_MAIN_URL,
    UMICH_SURVEY_INFO_URL,
)
from .parser import (
    PROVIDER,
    UMichCalendarEventRecord,
    UMichCalendarRawRecord,
    event_anchor,
    normalize_release_stage,
    title_for_stage,
)

logger = logging.getLogger(__name__)

UMICH_RELEASE_TZ = "America/New_York"
UMICH_RELEASE_TIME_LOCAL = "10:00 AM"


class UMichScheduleParseError(ValueError):
    """Raised when the U Michigan release-date surface drifts."""


@dataclass(frozen=True)
class UMichScheduleEntry:
    """One U Michigan release-date row, pre-projection."""

    series_id: str
    reference_date: str
    reference_label: str
    release_stage: str
    release_date: str
    release_time_local: str
    event_time_utc: str
    source_url: str


@dataclass(frozen=True)
class UMichScheduleDocument:
    """Fetched release-date document after text extraction."""

    text: str
    source_url: str


_MONTH_NAMES: dict[str, int] = {
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
_SCHEDULE_YEAR_RE = re.compile(
    r"\brelease\s+dates\s+for\s+(?P<year>\d{4})\b",
    re.IGNORECASE,
)
_RELEASE_ROW_RE = re.compile(
    r"\b(?P<release_month>January|February|March|April|May|June|July|"
    r"August|September|October|November|December)\s+"
    r"(?P<release_day>\d{1,2})\s+"
    r"(?P<survey_month>January|February|March|April|May|June|July|"
    r"August|September|October|November|December)\s+"
    r"(?P<stage>Prelim|Preliminary|Final)\b",
    re.IGNORECASE,
)


def _resolve_series(
    series_ids: set[str] | None,
) -> list[UMichIndicatorSpec]:
    specs = list(INDICATOR_REGISTRY.values())
    if series_ids is None:
        return specs
    return [spec for spec in specs if spec.series_id in series_ids]


def _schedule_year(text: str) -> int:
    match = _SCHEDULE_YEAR_RE.search(text)
    if match is None:
        raise UMichScheduleParseError("release-date year header not found")
    return int(match.group("year"))


def _month_number(name: str) -> int:
    month = _MONTH_NAMES.get(name.lower())
    if month is None:
        raise UMichScheduleParseError(f"unknown month name: {name!r}")
    return month


def parse_release_dates_text(
    text: str,
    *,
    source_url: str = UMICH_SURVEY_INFO_URL,
    series_ids: set[str] | None = None,
) -> list[UMichScheduleEntry]:
    """Extract whitelisted release-date rows from U Michigan text."""
    year = _schedule_year(text)
    compact = " ".join(text.replace("\xa0", " ").split())
    specs = _resolve_series(series_ids)
    entries: list[UMichScheduleEntry] = []
    for match in _RELEASE_ROW_RE.finditer(compact):
        release_month = _month_number(match.group("release_month"))
        release_day = int(match.group("release_day"))
        survey_month_name = match.group("survey_month")
        survey_month = _month_number(survey_month_name)
        stage = normalize_release_stage(match.group("stage"))
        release_date = datetime(year, release_month, release_day).date()
        reference_date = datetime(year, survey_month, 1).date()
        scheduled = parse_scheduled_release_time(
            release_date,
            UMICH_RELEASE_TIME_LOCAL,
            default_tz=UMICH_RELEASE_TZ,
        )
        for spec in specs:
            entries.append(
                UMichScheduleEntry(
                    series_id=spec.series_id,
                    reference_date=reference_date.isoformat(),
                    reference_label=(
                        f"{survey_month_name} {year} "
                        f"{'Prelim' if stage == 'preliminary' else 'Final'}"
                    ),
                    release_stage=stage,
                    release_date=release_date.isoformat(),
                    release_time_local=UMICH_RELEASE_TIME_LOCAL,
                    event_time_utc=scheduled.utc.isoformat(),
                    source_url=source_url,
                )
            )
    return entries


def schedule_entry_to_records(
    entry: UMichScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: UMichIndicatorSpec | None = None,
) -> tuple[UMichCalendarRawRecord, UMichCalendarEventRecord]:
    """Project one U Michigan schedule entry to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {entry.series_id!r} not in INDICATOR_REGISTRY")

    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        event_anchor(entry.reference_date, entry.release_stage),
    )
    payload: dict[str, Any] = {
        "kind": "umich_schedule",
        "series_id": entry.series_id,
        "reference_date": entry.reference_date,
        "reference_label": entry.reference_label,
        "release_stage": entry.release_stage,
        "release_date": entry.release_date,
        "release_time_local": entry.release_time_local,
        "event_time_utc": entry.event_time_utc,
        "source_url": entry.source_url,
    }
    content_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()
    raw_record = UMichCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = UMichCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=entry.event_time_utc,
        event_time_precision="datetime",
        reference_date=entry.reference_date,
        reference_label=entry.reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=title_for_stage(resolved_spec, entry.release_stage),
        importance=resolved_spec.importance,
        currency="",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="University of Michigan Surveys of Consumers",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


_UMICH_HTTP_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/pdf,application/xhtml+xml,*/*;q=0.8",
}


def fetch_current_results_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the public current U Michigan results page."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(UMICH_MAIN_URL, headers=_UMICH_HTTP_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()


def fetch_survey_info_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the Survey Information page that links the release-date PDF."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            UMICH_SURVEY_INFO_URL,
            headers=_UMICH_HTTP_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()


def discover_release_dates_url(html: str, *, year: int | None = None) -> str:
    """Find the release-date document link on Survey Information."""
    wanted_year = year or datetime.now(timezone.utc).year
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("a", href=True):
        text = " ".join(link.get_text(" ", strip=True).split()).lower()
        if text == f"{wanted_year} release dates":
            return urljoin(UMICH_SURVEY_INFO_URL, str(link["href"]))
    raise UMichScheduleParseError(
        f"{wanted_year} U Michigan release-date link not found"
    )


def fetch_release_dates_document(
    *,
    year: int | None = None,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> UMichScheduleDocument:
    """Fetch and extract text from the current-year release-date document."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        info_html = fetch_survey_info_html(session=s, timeout=timeout)
        url = discover_release_dates_url(info_html, year=year)
        response = s.get(url, headers=_UMICH_HTTP_HEADERS, timeout=timeout)
        response.raise_for_status()
        return UMichScheduleDocument(
            text=document_bytes_to_text(response.content),
            source_url=url,
        )
    finally:
        if owned_session:
            s.close()


def document_bytes_to_text(data: bytes) -> str:
    """Convert a fetched release-date document into parseable text."""
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
    raise UMichScheduleParseError(f"could not extract U Michigan PDF text: {detail}")
