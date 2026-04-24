"""Scrape BoJ statement pages for the policy-rate decision.

Each MPM publishes a statement on closing day at
``boj.or.jp/en/mopo/mpmdeci/state_<YYYY>/k<YYMMDD>a.htm``. The
statement's policy paragraph names the new target for the
uncollateralized overnight call rate in a stable sentence::

    "The Bank will encourage the uncollateralized overnight call
     rate to remain at around 0.5 percent."

The sentence uses "remain" as a forward-looking guideline even when
the decision changes the rate — the 2024-07-31 hike from ~0% to
0.25% still phrased the new target as "remain at around 0.25 percent"
from the intermeeting period forward. We therefore extract the
numeric rate only; direction ("hold" vs "hike" vs "cut") belongs to
a downstream diff against the previous MPM's rate, not to the
sentence itself.

Fetch + parse + project are separable: tests feed fixture HTML to
:func:`parse_statement_html`; live callers drive
:func:`fetch_statement_html` (reuses the schedule-scraper browser-UA
header bundle — ``boj.or.jp`` 403s on the default python-requests UA).
:func:`statement_value_to_records` emits a ``(raw, event)`` tuple
whose ``provider_event_id`` matches the schedule-side write exactly
(same closing-date ISO anchor), so the ``actual`` value upserts onto
the existing row via the shared projector's merge CASE.
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

from .indicators import BojIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    BOJ_RELEASE_TIME_LOCAL,
    BOJ_RELEASE_TZ,
    PROVIDER,
    BojCalendarEventRecord,
    BojCalendarRawRecord,
)
from .scraper import _BOJ_BROWSER_HEADERS

logger = logging.getLogger(__name__)

BOJ_STATEMENT_URL_TEMPLATE = (
    "https://www.boj.or.jp/en/mopo/mpmdeci/state_{year}/k{yymmdd}a.htm"
)


class BojStatementParseError(Exception):
    """Statement page didn't carry a parseable policy-rate sentence."""


@dataclass(frozen=True)
class StatementValue:
    """Parsed BoJ statement outcome.

    ``rate`` is the decimal policy-rate target (``0.5`` for
    ``"around 0.5 percent"``). ``rate_text`` preserves the raw number
    string (``"0.5"``, ``"0.25"``) for the revision-diff audit trail.

    ``release_time_local`` carries the page's own "Release dates and
    times: Statement on Monetary Policy -- <Day>, <Month> <D> at
    HH:MM" clock when parseable — BoJ decisions can publish anywhere
    between 11:25 and 12:58 JST depending on committee deliberation
    length (fixture sample spans 11:25 → 12:56), so pinning the
    value row to a 12:00 JST placeholder would stamp the wrong
    timestamp on every upgrade. ``None`` means the page didn't carry
    a parseable release line; projection falls back to
    :data:`BOJ_RELEASE_TIME_LOCAL`.
    """

    closing_date: date
    rate: float
    rate_text: str
    release_time_local: str | None = None


def build_statement_url(closing_date: date) -> str:
    """Construct the statement URL for a given MPM closing date."""
    return BOJ_STATEMENT_URL_TEMPLATE.format(
        year=closing_date.strftime("%Y"),
        yymmdd=closing_date.strftime("%y%m%d"),
    )


# ──────────────────────────────────────────────────────────────────────────
# Parser
# ──────────────────────────────────────────────────────────────────────────


def _normalize(text: str) -> str:
    """Collapse whitespace and normalize unicode dash variants."""
    text = text.replace("\xa0", " ")  # NBSP
    for variant in ("‑", "–", "—", "−"):
        text = text.replace(variant, "-")
    return " ".join(text.split())


# BoJ policy-rate sentence. Shape since the 2024 liftoff:
#   "The Bank will encourage the uncollateralized overnight call rate
#    to remain at around 0.5 percent."
# "around" may also render as "around" with the word; some historical
# statements used "at around" without "to remain" (direct
# "encourage... at around 0.1 percent"). The regex tolerates either.
_POLICY_RATE_RE = re.compile(
    r"encourage\s+the\s+uncollateralized\s+overnight\s+call\s+rate\s+"
    r"(?:to\s+(?:remain|be|continue to be)\s+)?"
    r"at\s+around\s+(?P<rate>\d+(?:\.\d+)?)\s*percent",
    re.IGNORECASE,
)


# "Release dates and times: <Statement title> -- <Day>, <Month> <D>
# at HH:MM" — BoJ prints the schedule block near the top of every
# statement page, always in JST, with the statement itself as the
# first entry. The regex finds the first "at HH:MM" following the
# block header so the statement-title wording variation doesn't
# matter (the hike statements carry a longer title like "Change in
# the Guideline for Money Market Operations ..." rather than
# "Statement on Monetary Policy"; both parse identically).
_RELEASE_TIME_RE = re.compile(
    r"Release\s+dates?\s+and\s+times?.*?"
    r"at\s+(?P<time>\d{1,2}:\d{2})",
    re.IGNORECASE | re.DOTALL,
)


def parse_statement_html(html: str, closing_date: date) -> StatementValue:
    """Extract the policy-rate target from a BoJ statement page.

    Raises :class:`BojStatementParseError` if no policy-rate sentence
    is found — upstream drift must surface loudly rather than silently
    emit a ``None`` value onto an existing schedule row.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = _normalize(soup.get_text(separator=" "))
    match = _POLICY_RATE_RE.search(text)
    if match is None:
        raise BojStatementParseError(
            "policy-rate sentence not found on BoJ statement page "
            f"(closing_date={closing_date.isoformat()})"
        )
    rate_text = match.group("rate")
    release_match = _RELEASE_TIME_RE.search(text)
    release_time_local = (
        release_match.group("time") if release_match else None
    )
    return StatementValue(
        closing_date=closing_date,
        rate=float(rate_text),
        rate_text=rate_text,
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
    """GET the BoJ statement page for a given MPM closing date.

    Reuses the browser-UA header bundle from :mod:`.scraper` — the
    same UA works for every ``boj.or.jp`` page.
    """
    url = build_statement_url(closing_date)
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(url, headers=_BOJ_BROWSER_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response.content.decode("utf-8")
    finally:
        if owned_session:
            s.close()


# ──────────────────────────────────────────────────────────────────────────
# Value-side projection
# ──────────────────────────────────────────────────────────────────────────


def _format_rate(rate: float) -> str:
    """Format the policy-rate target as ``"X.XX"``.

    Two decimals keep ``0.5`` and ``0.25`` on a consistent display
    surface (``"0.50"`` / ``"0.25"``). Matches the FOMC target-range
    output shape.
    """
    return f"{rate:.2f}"


_HASH_FIELDS: tuple[str, ...] = ("rate", "event_time_utc")


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
    spec: BojIndicatorSpec | None = None,
) -> tuple[BojCalendarRawRecord, BojCalendarEventRecord]:
    """Project a :class:`StatementValue` into (raw, event) records.

    ``provider_event_id`` uses the same closing-date ISO anchor as the
    schedule-side write in :mod:`.parser`, so the value row upserts
    onto the existing schedule row through the shared projector's
    merge CASE (stored datetime precision survives the merge; the
    ``actual`` column is filled).
    """
    resolved_spec = spec or INDICATOR_REGISTRY["BOJ_RATE"]

    release_time_local = value.release_time_local or BOJ_RELEASE_TIME_LOCAL
    scheduled = parse_scheduled_release_time(
        value.closing_date,
        release_time_local,
        default_tz=BOJ_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        value.closing_date.isoformat(),
    )

    actual = _format_rate(value.rate)
    statement_url = build_statement_url(value.closing_date)
    reference_label = value.closing_date.strftime("%B %Y")

    payload: dict[str, Any] = {
        "kind":           "boj_statement",
        "closing_date":   value.closing_date.isoformat(),
        "rate":           value.rate,
        "rate_text":      value.rate_text,
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

    raw_record = BojCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    event_record = BojCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=value.closing_date.isoformat(),
        reference_label=reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="JPY",
        unit=resolved_spec.unit,
        actual=actual,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Bank of Japan",
        source_url=statement_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )

    return raw_record, event_record
