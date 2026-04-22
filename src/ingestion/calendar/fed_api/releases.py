"""Consume ``federalreserve.gov/json/calendar.json`` for the non-FOMC
Fed calendar surface (issue #9 P4a + P4b-live-follow-up).

Scope covers three indicators the Fed publishes outside the FOMC
calendar:

- **Beige Book** — narrative regional summary, ~8 per year, 2 weeks
  before each FOMC meeting, 14:00 ET.
- **H.4.1** — Factors Affecting Reserve Balances, weekly (Thursdays),
  16:30 ET.
- **H.8** — Assets and Liabilities of Commercial Banks, weekly
  (Fridays), 16:15 ET.

Summary of Economic Projections (SEP) is explicitly excluded: it
already rides on the FOMC calendar via :attr:`FomcMeetingEntry.has_sep`
and the title ``"FOMC Rate Decision + SEP"``, so emitting a separate
SEP row from this feed would create a duplicate calendar event for
every quarterly FOMC meeting.

Scheduled Fed Chair / Vice-Chair speeches on the calendar feed are
out of P4a scope — they belong in the news / speech pipeline, not
here.

Fetch + parse are separable functions. Tests feed fixture JSON
directly to :func:`parse_fed_calendar_json`; live callers use
:func:`fetch_fed_calendar_json` which drives :class:`requests.Session`
against the plain JSON endpoint. The Fed's ``fomccalendars.htm``
still requires a browser UA, but ``/json/calendar.json`` accepts the
default ``python-requests`` UA.

The JSON feed replaces the HTML scrape at
``/newsevents/releasedates.htm`` that 404'd during the 2026-04-22
live probe (issue #9 P4b-live). The JSON surface carries the same
Beige Book / H.4.1 / H.8 content alongside FOMC meetings, speeches,
and testimony; the title-substring whitelist carries over unchanged.
The wire payload is UTF-8 with a leading BOM — ``fetch_fed_calendar_json``
strips it so ``json.loads`` accepts the returned text directly.
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

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import FedIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    FOMC_RELEASE_TZ,
    PROVIDER,
    FedCalendarEventRecord,
    FedCalendarRawRecord,
)

logger = logging.getLogger(__name__)

FED_CALENDAR_JSON_URL = "https://www.federalreserve.gov/json/calendar.json"

# Release-title match table. Entry order matters: longer / more-
# specific fragments first so ``"H.4.1"`` matches before the
# less-specific ``"H."`` never would. Each entry maps a lowercase
# substring to the target indicator id + default release time
# (applied when the event has no parseable ``time`` field).
_MATCH_ENTRIES: tuple[tuple[str, str, str], ...] = (
    ("beige book",      "BEIGE_BOOK", "2:00 PM"),
    ("h.4.1",           "FED_H41",    "4:30 PM"),
    ("h4.1",            "FED_H41",    "4:30 PM"),
    ("h.8",             "FED_H8",     "4:15 PM"),
    # "h8" bare substring is too common — require the dotted form.
)

# Feed ``type`` values the parser will consider for whitelist matching.
# The Fed's calendar JSON carries an explicit ``type`` on every entry
# — ``"Beige"`` for Beige Book rows and ``"Stat"`` for the weekly
# statistical releases that include H.4.1 / H.8. Gating matches on
# ``type`` before title prevents a governor speech titled
# ``"Rethinking the Beige Book"`` (type ``"Speeches"``) or a
# testimony transcript mentioning ``"H.4.1"`` from getting projected
# as a spurious BEIGE_BOOK / FED_H41 release row — Codex P2 on
# 2026-04-22. The current feed has zero such collisions but the
# categories trade on our whitelist keywords routinely enough to
# make this inevitable over a multi-year horizon.
_RELEASE_TYPES: frozenset[str] = frozenset({"Beige", "Stat"})

# Event titles that must be ignored even when they substring-match
# the table above — currently SEP (rides on FOMC calendar) and the
# G.19 / H.15 titles that happen to tokenise similarly to whitelisted
# ids. The SEP entry is belt-and-suspenders: the JSON feed groups
# SEP under the FOMC meeting's description rather than a standalone
# row, so the fragment rarely matches; keeping the exclude keeps the
# parser stable if the feed layout shifts.
_EXCLUDE_FRAGMENTS: tuple[str, ...] = (
    "summary of economic projections",
    "consumer credit - g.19",
    "consumer credit g.19",
    "selected interest rates - h.15",
    "selected interest rates h.15",
)


class FedCalendarJsonParseError(ValueError):
    """Raised when the calendar JSON feed deviates from expectations."""


@dataclass(frozen=True)
class FedReleaseEntry:
    """One matched event from the Fed calendar JSON feed, pre-projection."""

    series_id: str          # matches INDICATOR_REGISTRY key
    release_title: str      # verbatim "title" field
    release_date: str       # ISO YYYY-MM-DD
    release_time_local: str # normalized "4:30 PM" (or default from match)
    event_time_utc: str     # ISO datetime with UTC offset


# ``parse_scheduled_release_time`` reads the AM/PM suffix case-
# insensitively but requires a period-free form. The feed writes
# ``"4:30 p.m."`` uniformly; normalize to ``"4:30 PM"`` so the shared
# helper accepts it.
_TIME_FULLMATCH_RE = re.compile(
    r"\s*(\d{1,2})[:.](\d{2})\s*([AaPp]\.?\s*[Mm]\.?)?\s*(?:ET|EST|EDT)?\s*",
)


def _normalize_time(text: str | None, *, default: str) -> str:
    """Normalize feed ``"4:30 p.m."`` → ``"4:30 PM"``, fall back on misses.

    Bare ``HH:MM`` (no suffix) falls back to the per-indicator default
    rather than reading as 24-hour — a naive 24-hour reading of
    ``"4:30"`` would place H.4.1 at 04:30 ET instead of 16:30 ET,
    flipping every PM release 12 hours early.
    """
    if not text:
        return default
    match = _TIME_FULLMATCH_RE.fullmatch(text)
    if not match:
        return default
    hh, mm, suffix = match.groups()
    if not suffix:
        return default
    suffix_clean = suffix.upper().replace(".", "").replace(" ", "")
    return f"{int(hh)}:{mm} {suffix_clean}"


_YM_RE = re.compile(r"^\s*(\d{4})-(\d{1,2})\s*$")


def _parse_year_month(text: str) -> tuple[int, int]:
    """Parse a feed ``month`` field ``"YYYY-MM"`` into ``(year, month)``.

    Raises :class:`ValueError` for any out-of-range or unparseable
    form; caller converts to a row-level issue.
    """
    match = _YM_RE.match(text)
    if not match:
        raise ValueError(f"unparseable month: {text!r}")
    year = int(match.group(1))
    month = int(match.group(2))
    if not 1 <= month <= 12:
        raise ValueError(f"month out of range: {month}")
    return year, month


def _split_days(text: str) -> list[int]:
    """Split ``"3, 10, 17, 24, 31"`` (or ``"25"``) into ``[int, ...]``.

    Empty / non-numeric tokens are silently skipped — the feed is
    well-formed in practice but a stray separator shouldn't abort
    the whole event.
    """
    out: list[int] = []
    for token in text.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            out.append(int(token))
        except ValueError:
            continue
    return out


def _match_release(title: str) -> tuple[str, str] | None:
    """Return ``(series_id, default_time)`` or ``None`` for the title."""
    lowered = title.lower()
    for fragment in _EXCLUDE_FRAGMENTS:
        if fragment in lowered:
            return None
    for fragment, series_id, default_time in _MATCH_ENTRIES:
        if fragment in lowered:
            return series_id, default_time
    return None


def parse_fed_calendar_json(
    text: str,
    *,
    row_issues: list[str] | None = None,
) -> list[FedReleaseEntry]:
    """Extract :class:`FedReleaseEntry` rows from the Fed calendar JSON feed.

    Walks every element of the top-level ``events`` array. An event
    matches when its ``title`` substring-matches one of
    ``_MATCH_ENTRIES`` and doesn't substring-match an
    ``_EXCLUDE_FRAGMENTS`` phrase. Events with unparseable month / day /
    time fields are captured in ``row_issues`` rather than silently
    dropped — same shape as BEA P2a's parser. Multi-day events (one
    feed entry with ``"days": "3, 10, 17, 24, 31"``) emit one
    :class:`FedReleaseEntry` per day.

    The feed ships with a UTF-8 BOM on the wire;
    :func:`fetch_fed_calendar_json` strips it before handing off, but
    test callers may pass either form — the parser tolerates a leading
    BOM here too.
    """
    stripped = text.lstrip("\ufeff")
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise FedCalendarJsonParseError(
            f"calendar JSON payload did not parse: {exc}"
        ) from exc
    events = payload.get("events") if isinstance(payload, dict) else None
    if not isinstance(events, list):
        raise FedCalendarJsonParseError(
            "calendar JSON payload missing 'events' array"
        )

    entries: list[FedReleaseEntry] = []
    matched_any = False
    for event in events:
        if not isinstance(event, dict):
            continue
        # Gate on ``type`` first — the feed tags every entry and
        # release rows always land as ``"Beige"`` or ``"Stat"``. Any
        # other value (``"Speeches"``, ``"Testimony"``, ``"FOMC"``,
        # ``"Conferences"``, the ``"events"`` orphan sub-entries with
        # empty ``month``, …) is off-scope for this connector, so we
        # skip before the title match. The skip is silent: feeds
        # routinely carry hundreds of these and ``row_issues`` is
        # reserved for genuine month/day drift on a release row.
        if event.get("type") not in _RELEASE_TYPES:
            continue
        title_raw = event.get("title")
        if not isinstance(title_raw, str):
            continue
        title = title_raw.strip()
        if not title:
            continue
        match = _match_release(title)
        if match is None:
            continue
        matched_any = True
        series_id, default_time = match
        INDICATOR_REGISTRY[series_id]  # fail-fast if registry drifts

        month_raw = event.get("month")
        days_raw = event.get("days")
        if not isinstance(month_raw, str) or not isinstance(days_raw, str):
            if row_issues is not None:
                row_issues.append(
                    f"{title!r}: missing month/days "
                    f"(month={month_raw!r}, days={days_raw!r})"
                )
            continue
        try:
            year, month_num = _parse_year_month(month_raw)
        except ValueError as exc:
            if row_issues is not None:
                row_issues.append(f"{title!r}: {exc}")
            continue
        day_tokens = _split_days(days_raw)
        if not day_tokens:
            if row_issues is not None:
                row_issues.append(
                    f"{title!r}: unparseable days {days_raw!r}"
                )
            continue

        release_time = _normalize_time(event.get("time"), default=default_time)
        for day in day_tokens:
            try:
                release_date = date(year=year, month=month_num, day=day)
            except ValueError as exc:
                if row_issues is not None:
                    row_issues.append(
                        f"{title!r} day={day} month={month_raw!r}: {exc}"
                    )
                continue
            try:
                scheduled = parse_scheduled_release_time(
                    release_date, release_time,
                    default_tz=FOMC_RELEASE_TZ,
                )
            except Exception as exc:
                if row_issues is not None:
                    row_issues.append(
                        f"{title!r} date={release_date.isoformat()} "
                        f"time={release_time!r}: {exc}"
                    )
                continue
            entries.append(
                FedReleaseEntry(
                    series_id=series_id,
                    release_title=title,
                    release_date=release_date.isoformat(),
                    release_time_local=release_time,
                    event_time_utc=scheduled.utc.isoformat(),
                )
            )

    if not matched_any:
        raise FedCalendarJsonParseError(
            "no calendar events matching the Fed whitelist fragments"
        )
    if not entries:
        # Codex P4a — if every whitelisted event hit a row-level parse
        # failure (feed month/day/time format drifted on every match),
        # returning an empty list would let ``fetch_fed_releasedates``
        # commit a successful zero-row run and mask the outage.
        # ``row_issues`` still carries the per-event detail for
        # operator diagnosis.
        detail = f" ({len(row_issues or ())} row issues)" if row_issues else ""
        raise FedCalendarJsonParseError(
            "whitelist matched but every event failed parsing" + detail
        )
    return entries


# ──────────────────────────────────────────────────────────────────────────
# Projection
# ──────────────────────────────────────────────────────────────────────────


def release_entry_to_records(
    entry: FedReleaseEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: FedIndicatorSpec | None = None,
) -> tuple[FedCalendarRawRecord, FedCalendarEventRecord]:
    """Project a :class:`FedReleaseEntry` to (raw, event) records.

    ``provider_event_id`` anchors on ``(indicator, release_date)`` so
    each release is a distinct calendar event. H.4.1 / H.8 publish
    weekly, so two release dates in the same week still resolve to
    different ids. Beige Book ships 8 times per year, each with a
    unique release date.
    """
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {entry.series_id!r} not in Fed INDICATOR_REGISTRY"
        )

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        entry.release_date,
    )

    payload: dict[str, Any] = {
        "kind":               "fed_release",
        "series_id":          entry.series_id,
        "release_title":      entry.release_title,
        "release_date":       entry.release_date,
        "release_time_local": entry.release_time_local,
        "event_time_utc":     entry.event_time_utc,
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
        event_time_utc=entry.event_time_utc,
        event_time_precision="datetime",
        reference_date=entry.release_date,
        reference_label=entry.release_date,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="USD",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Federal Reserve",
        source_url=FED_CALENDAR_JSON_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


# ──────────────────────────────────────────────────────────────────────────
# HTTP fetch
# ──────────────────────────────────────────────────────────────────────────


def fetch_fed_calendar_json(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the Fed calendar JSON feed and return BOM-stripped UTF-8 text.

    The feed ships with a UTF-8 BOM on the wire; the caller receives
    BOM-stripped text so :func:`json.loads` accepts it directly. Plain
    ``python-requests`` UA is accepted — unlike ``fomccalendars.htm``
    which 403s the default UA, the JSON endpoint has no UA gate.
    """
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(FED_CALENDAR_JSON_URL, timeout=timeout)
        response.raise_for_status()
        # ``response.text`` guesses encoding from headers and can land
        # on ISO-8859-1 when the server's ``Content-Type`` omits an
        # explicit ``charset``. Decode from raw bytes against
        # UTF-8-with-BOM so the parser sees plain JSON regardless of
        # how the server labels the response.
        return response.content.decode("utf-8-sig")
    finally:
        if owned_session:
            s.close()
