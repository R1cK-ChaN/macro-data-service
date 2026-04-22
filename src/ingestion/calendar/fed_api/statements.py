"""Scrape FOMC statement pages for the target-range rate decision.

Each FOMC meeting publishes a statement at 14:00 ET on closing day at
``federalreserve.gov/newsevents/pressreleases/monetary<YYYYMMDD>a.htm``
(``a`` is the statement; ``b`` and higher letters are the
implementation note and related supplementary releases). The
statement's policy paragraph always names the new target range for
the federal funds rate::

    "The Committee decided to maintain the target range for the
     federal funds rate at 4-1/4 to 4-1/2 percent."

    "The Committee decided to lower the target range for the federal
     funds rate by 1/2 percentage point to 4-3/4 to 5 percent."

    "The Committee decided to raise the target range for the federal
     funds rate by 1/4 percentage point to 5-1/4 to 5-1/2 percent."

The range uses mixed-number notation (``4-1/4`` = 4.25). The zero
lower bound version drops the whole-number prefix
(``0 to 1/4 percent``).

Fetch + parse + project are separable: tests feed fixture HTML to
:func:`parse_statement_html`; live callers drive
:func:`fetch_statement_html` (reuses the FOMC-calendar browser-UA
header bundle — ``federalreserve.gov`` 403s on the default
``python-requests`` UA). :func:`statement_value_to_records` emits a
``(raw, event)`` tuple whose ``provider_event_id`` matches the
schedule-side write exactly (same closing-date ISO anchor), so the
``actual`` value upserts onto the existing row via the shared
projector's merge CASE.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import FedIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    FOMC_RELEASE_TIME_LOCAL,
    FOMC_RELEASE_TZ,
    PROVIDER,
    FedCalendarEventRecord,
    FedCalendarRawRecord,
)
from .scraper import _FED_BROWSER_HEADERS

logger = logging.getLogger(__name__)

FOMC_STATEMENT_URL_TEMPLATE = (
    "https://www.federalreserve.gov/newsevents/pressreleases/"
    "monetary{yyyymmdd}a.htm"
)


class FomcStatementParseError(Exception):
    """Statement page didn't carry a matchable target-range sentence."""


@dataclass(frozen=True)
class StatementValue:
    """Parsed FOMC statement outcome.

    ``range_lower`` / ``range_upper`` are decimal endpoints — ``4.25``
    and ``4.50`` for a 4-1/4 to 4-1/2 range. ``direction`` is the
    committee's verb verbatim (``"maintain"`` / ``"keep"`` /
    ``"raise"`` / ``"lower"``). ``by_text`` preserves the optional
    "by 1/4 percentage point" phrase verbatim when present (``None``
    for holds); used for revision diffs, not written to
    ``cal_econ_event``.

    ``release_time_local`` carries the page's own "For release at
    X:XX p.m." clock when parseable — emergency statements can
    publish outside the standing 14:00 ET FOMC convention (the
    2020-03-15 inter-meeting cut went out at 5:00 p.m. EDT).
    ``None`` means the page didn't carry a parseable release line;
    projection falls back to :data:`FOMC_RELEASE_TIME_LOCAL`.
    """

    closing_date: date
    range_lower: float
    range_upper: float
    direction: str
    by_text: str | None
    release_time_local: str | None = None


def build_statement_url(closing_date: date) -> str:
    """Construct the statement URL for a given FOMC closing date."""
    return FOMC_STATEMENT_URL_TEMPLATE.format(
        yyyymmdd=closing_date.strftime("%Y%m%d"),
    )


# ──────────────────────────────────────────────────────────────────────────
# Parser
# ──────────────────────────────────────────────────────────────────────────


def _normalize(text: str) -> str:
    """Collapse whitespace and normalize unicode dash variants."""
    text = text.replace(" ", " ")  # NBSP
    # en-dash, em-dash, minus sign → ASCII hyphen so mixed-number
    # parsing sees a single canonical separator.
    for variant in ("‑", "–", "—", "−"):
        text = text.replace(variant, "-")
    return " ".join(text.split())


# Range endpoint grammar — one of:
#   "0", "4"           — whole number
#   "1/4", "3/4"       — bare fraction (zero lower bound)
#   "4-1/4", "5-1/2"   — mixed number (whole-fraction)
_NUMBER = r"(?:\d+(?:-\d+/\d+)?|\d+/\d+)"

# The committee's decision sentence. Committee-subject and "In support
# of its goals," preambles vary across statements, so the regex anchors
# on "decided to (maintain|keep|raise|lower) the target range for the
# federal funds rate" and tolerates an optional "by N <unit>" phrase
# before the range. ``keep`` is the Fed's preferred hold verb during
# the 2020-2022 zero-bound era ("decided to keep the target range ...
# at 0 to 1/4 percent"); dropping it would make auto-discovered
# pre-2022 FOMC rows land as parse failures and leave ``actual``
# empty.
_TARGET_RANGE_RE = re.compile(
    r"decided\s+to\s+"
    r"(?P<direction>maintain|keep|raise|lower)\s+"
    r"(?:the\s+)?target\s+range\s+for\s+the\s+federal\s+funds\s+rate"
    r"(?:\s+by\s+(?P<by_text>[^.]+?))?"
    r"\s+(?:at|to)\s+(?P<lower>" + _NUMBER + r")"
    r"\s+to\s+(?P<upper>" + _NUMBER + r")\s+percent",
    re.IGNORECASE,
)


# "For release at 2:00 p.m. EST" / "For release at 5:00 p.m. EDT" —
# Fed statements include the release clock verbatim on the page.
# Scheduled meetings are always 14:00 ET; emergency statements can
# publish at other times (2020-03-15 at 5:00 p.m. EDT). Parse the
# page's own release line so emergency / off-schedule meetings land
# at the published time rather than the standing 14:00 ET default.
_RELEASE_TIME_RE = re.compile(
    r"For\s+release\s+at\s+"
    r"(?P<time>\d{1,2}:\d{2}\s*[apAP]\.?\s*[mM]\.?)",
    re.IGNORECASE,
)


def _parse_mixed_number(text: str) -> float:
    """Parse ``"4"`` / ``"1/4"`` / ``"4-1/4"`` into a decimal."""
    text = text.strip()
    if "/" not in text:
        return float(text)
    if "-" in text:
        whole, frac = text.split("-", 1)
        numerator, denominator = frac.split("/")
        return float(whole) + float(numerator) / float(denominator)
    numerator, denominator = text.split("/")
    return float(numerator) / float(denominator)


def parse_statement_html(html: str, closing_date: date) -> StatementValue:
    """Extract the target-range outcome from an FOMC statement page.

    Raises :class:`FomcStatementParseError` if no target-range sentence
    is found — upstream drift must surface loudly rather than silently
    emit a ``None`` value onto an existing schedule row.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = _normalize(soup.get_text(separator=" "))
    match = _TARGET_RANGE_RE.search(text)
    if match is None:
        raise FomcStatementParseError(
            "target-range sentence not found on statement page "
            f"(closing_date={closing_date.isoformat()})"
        )
    by_text = (match.group("by_text") or "").strip() or None
    release_match = _RELEASE_TIME_RE.search(text)
    release_time_local = release_match.group("time") if release_match else None
    return StatementValue(
        closing_date=closing_date,
        range_lower=_parse_mixed_number(match.group("lower")),
        range_upper=_parse_mixed_number(match.group("upper")),
        direction=match.group("direction").lower(),
        by_text=by_text,
        release_time_local=release_time_local,
    )


# ──────────────────────────────────────────────────────────────────────────
# HTTP fetch
# ──────────────────────────────────────────────────────────────────────────


def fetch_statement_html(
    closing_date: date,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the FOMC statement page for a given closing date.

    Reuses the browser-UA header bundle from :mod:`.scraper` — the
    same UA works for every ``federalreserve.gov`` page we scrape.
    """
    url = build_statement_url(closing_date)
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(url, headers=_FED_BROWSER_HEADERS, timeout=timeout)
        response.raise_for_status()
        # Content-Type carries no charset; requests defaults to ISO-8859-1
        # which mis-decodes the UTF-8 non-breaking hyphens in mixed-number
        # rate strings (e.g. "3\xe2\x80\x911/2" → "3â€˜1/2").
        return response.content.decode("utf-8")
    finally:
        if owned_session:
            s.close()


# ──────────────────────────────────────────────────────────────────────────
# Value-side projection
# ──────────────────────────────────────────────────────────────────────────


def _format_range(lower: float, upper: float) -> str:
    """Format the target range as ``"X.XX-Y.YY"``.

    Two decimals because 25-basis-point increments always land on a
    two-decimal boundary; the zero lower bound ``0.00-0.25`` fits the
    same shape.
    """
    return f"{lower:.2f}-{upper:.2f}"


_HASH_FIELDS: tuple[str, ...] = (
    "range_lower", "range_upper", "direction", "by_text",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def statement_value_to_records(
    value: StatementValue,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: FedIndicatorSpec | None = None,
    has_sep: bool = False,
) -> tuple[FedCalendarRawRecord, FedCalendarEventRecord]:
    """Project a :class:`StatementValue` into (raw, event) records.

    ``provider_event_id`` uses the same closing-date ISO anchor as the
    schedule-side write in :mod:`.parser`, so the value row upserts
    onto the existing schedule row through the shared projector's
    merge CASE (stored datetime precision survives the merge; the
    ``actual`` column is filled).

    ``has_sep`` must be set to the flag from the existing schedule
    row so the value write preserves the ``"+ SEP"`` title suffix on
    quarterly projection-materials meetings. The value-side
    ``project_events`` upsert overwrites the ``title`` column, so
    dropping the SEP marker here would silently strip the flag from
    the calendar row each time a statement value lands.
    """
    resolved_spec = spec or INDICATOR_REGISTRY["FOMC_RATE"]

    release_time_local = value.release_time_local or FOMC_RELEASE_TIME_LOCAL
    scheduled = parse_scheduled_release_time(
        value.closing_date,
        release_time_local,
        default_tz=FOMC_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        value.closing_date.isoformat(),
    )

    actual = _format_range(value.range_lower, value.range_upper)
    statement_url = build_statement_url(value.closing_date)
    reference_label = value.closing_date.strftime("%B %Y")
    title = resolved_spec.title + (" + SEP" if has_sep else "")

    payload: dict[str, Any] = {
        "kind":           "fomc_statement",
        "closing_date":   value.closing_date.isoformat(),
        "range_lower":    value.range_lower,
        "range_upper":    value.range_upper,
        "direction":      value.direction,
        "by_text":        value.by_text,
        "event_time_utc": event_time_utc,
        "statement_url":  statement_url,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)

    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = FedCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    event_record = FedCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=value.closing_date.isoformat(),
        reference_label=reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=title,
        importance=resolved_spec.importance,
        currency="USD",
        unit=resolved_spec.unit,
        actual=actual,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Federal Reserve",
        source_url=statement_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )

    return raw_record, event_record
