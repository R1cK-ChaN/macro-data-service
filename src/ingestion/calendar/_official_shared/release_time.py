"""Parse a scheduled-release-time string into an aware UTC datetime.

BLS publishes ``"08:30 AM ET"``. ECB publishes ``"13:45 CET"``. NBS
publishes Beijing time unadorned (``"09:30"``) on its release calendar.
This module normalises all three shapes to UTC so the connectors can
store a single ``event_time_utc`` column.

Design:

- Caller passes the release *date* alongside the time text. The parser
  uses that date to resolve DST (ET flips to EDT in the spring), so
  historical backfill stays correct across DST boundaries.
- Explicit timezone abbreviations in the text (``"ET"``, ``"CET"``,
  ``"CST"``, …) override ``default_tz``. The abbreviation → IANA map
  lives in :data:`TIMEZONE_ALIASES`; callers may pass ``default_tz``
  as a fallback for unadorned times.
- The parser is total for the shapes listed below; unparseable input
  raises :class:`ValueError` rather than silently returning ``None``,
  because an unparsed release time is a schema-drift signal the
  connector must surface, not swallow.

Supported time shapes:

- 24h: ``"08:30"``, ``"13:45"``, ``"09:30 CST"``
- 12h: ``"8:30 AM"``, ``"8:30 AM ET"``, ``"2:00 PM ET"``
- 24h with offset: ``"08:30+00:00"``, ``"13:45+02:00"``

Note on ``CST``: CST is ambiguous — US Central Time and China Standard
Time share the abbreviation. The parser resolves it to **Asia/Shanghai**
by default because this module's callers (NBS in the issue-9 scope) all
operate in that timezone. US Central Time callers pass ``default_tz=
"America/Chicago"`` and the parser honours the default when the input
is bare (no abbreviation). Explicit ``"CST"`` text always resolves to
Shanghai.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

# Abbreviation → IANA zone. Keys are uppercase; the parser uppercases
# the text before lookup. Only the abbreviations the five P1-P5
# connectors actually encounter are listed. Adding a new one is a
# one-liner; keeping the set small prevents accidental matches.
TIMEZONE_ALIASES: dict[str, str] = {
    "UTC":  "UTC",
    "GMT":  "Etc/GMT",
    "ET":   "America/New_York",
    "EST":  "America/New_York",
    "EDT":  "America/New_York",
    "CET":  "Europe/Paris",
    "CEST": "Europe/Paris",
    "BST":  "Europe/London",
    "CST":  "Asia/Shanghai",      # NBS context — see module docstring
    "JST":  "Asia/Tokyo",
    "KST":  "Asia/Seoul",
    "HKT":  "Asia/Hong_Kong",
    "AEST": "Australia/Sydney",
    "AEDT": "Australia/Sydney",
    "IST":  "Asia/Kolkata",       # MoSPI / RBI context — issue #54
}

_TZ_ABBREV_RE = re.compile(
    r"\s+([A-Za-z]{2,5})\s*$"
)
_MERIDIEM_MARKERS: frozenset[str] = frozenset({"AM", "PM"})
_OFFSET_RE = re.compile(
    r"([+-]\d{2}:?\d{2})\s*$"
)
_TIME_24H_RE = re.compile(
    r"^\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$"
)
_TIME_12H_RE = re.compile(
    r"^\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*([AaPp])\.?[Mm]\.?\s*$"
)


@dataclass(frozen=True)
class ScheduledReleaseTime:
    """Parsed release time with enough metadata to round-trip for audit.

    ``utc`` is the normalised value consumers care about. ``local_tz``
    names the IANA zone the release was originally scheduled in so a
    later debug pass can reconstruct *why* the UTC value landed where
    it did (DST edge, timezone-alias fallback, etc.)."""

    utc: datetime
    local_tz: str


def _strip_trailing_tz(text: str) -> tuple[str, str | None]:
    """Return ``(time_body, tz_abbrev_or_offset_string)``.

    AM/PM look superficially like timezone abbreviations under the tail
    regex but are part of the 12-hour time shape; leave them attached
    to the body so :func:`_parse_time_body` can consume them.
    """
    offset_match = _OFFSET_RE.search(text)
    if offset_match:
        return text[: offset_match.start()].rstrip(), offset_match.group(1)
    abbrev_match = _TZ_ABBREV_RE.search(text)
    if abbrev_match:
        marker = abbrev_match.group(1).upper()
        if marker in _MERIDIEM_MARKERS:
            return text.strip(), None
        return text[: abbrev_match.start()].rstrip(), marker
    return text.strip(), None


def _parse_offset_string(value: str) -> timezone:
    # Accept both ``+02:00`` and ``+0200``.
    sign = 1 if value[0] == "+" else -1
    rest = value[1:]
    if ":" in rest:
        hours_str, minutes_str = rest.split(":", 1)
    else:
        hours_str, minutes_str = rest[:2], rest[2:]
    hours = int(hours_str)
    minutes = int(minutes_str)
    return timezone(sign * timedelta(hours=hours, minutes=minutes))


def _resolve_tz(marker: str | None, default_tz: str | None) -> str | timezone:
    """Turn an optional tz marker into an IANA name or fixed offset.

    Return value intentionally polymorphic — an IANA string is needed to
    compute DST-aware offsets for the release *date*, while a fixed
    offset is returned directly for offset-annotated inputs."""
    if marker is None:
        if not default_tz:
            raise ValueError(
                "no timezone marker on input and no default_tz provided"
            )
        return default_tz
    if marker[0] in ("+", "-") and any(ch.isdigit() for ch in marker):
        return _parse_offset_string(marker)
    iana = TIMEZONE_ALIASES.get(marker.upper())
    if iana is None:
        raise ValueError(f"unknown timezone marker: {marker!r}")
    return iana


def _parse_time_body(body: str) -> time:
    match24 = _TIME_24H_RE.match(body)
    if match24:
        hour = int(match24.group(1))
        minute = int(match24.group(2))
        second = int(match24.group(3) or 0)
        return time(hour=hour, minute=minute, second=second)
    match12 = _TIME_12H_RE.match(body)
    if match12:
        hour = int(match12.group(1))
        minute = int(match12.group(2))
        second = int(match12.group(3) or 0)
        meridiem = match12.group(4).upper()
        if meridiem == "A":
            hour = 0 if hour == 12 else hour
        else:
            hour = 12 if hour == 12 else hour + 12
        return time(hour=hour, minute=minute, second=second)
    raise ValueError(f"unparseable release time body: {body!r}")


def parse_scheduled_release_time(
    release_date: date,
    time_text: str,
    *,
    default_tz: str | None = None,
) -> ScheduledReleaseTime:
    """Convert a release-time string to an aware UTC datetime.

    Parameters
    ----------
    release_date:
        The scheduled release date (used for DST resolution). The
        parser combines this with the parsed wall-clock time to form
        a local-zone datetime, then converts to UTC.
    time_text:
        One of the supported shapes listed in the module docstring.
        Empty / whitespace-only text raises :class:`ValueError`.
    default_tz:
        IANA zone name to use when ``time_text`` carries no timezone
        marker. Required for bare-time input; ignored when the text
        has a trailing marker or offset.

    Returns
    -------
    ScheduledReleaseTime
        ``.utc`` is a timezone-aware ``datetime`` in UTC; ``.local_tz``
        is the zone the release was originally scheduled in.

    Raises
    ------
    ValueError
        If the text is empty, unparseable, carries an unknown timezone
        abbreviation, or has no marker and no ``default_tz``.
    """
    if not time_text or not time_text.strip():
        raise ValueError("empty release-time string")

    body, marker = _strip_trailing_tz(time_text.strip())
    local_time = _parse_time_body(body)
    resolved = _resolve_tz(marker, default_tz)

    naive_local = datetime.combine(release_date, local_time)
    if isinstance(resolved, timezone):
        aware_local = naive_local.replace(tzinfo=resolved)
        local_tz_label = f"UTC{int(resolved.utcoffset(None).total_seconds() / 3600):+d}"
    else:
        aware_local = naive_local.replace(tzinfo=ZoneInfo(resolved))
        local_tz_label = resolved

    return ScheduledReleaseTime(
        utc=aware_local.astimezone(ZoneInfo("UTC")),
        local_tz=local_tz_label,
    )
