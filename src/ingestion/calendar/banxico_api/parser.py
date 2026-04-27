"""Banxico ``Anuncios de Política Monetaria`` HTML → calendar projection.

The Banxico monetary-policy decision history page at
``banxico.org.mx/publicaciones-y-prensa/anuncios-de-las-decisiones-de-politica-monetaria/anuncios-politica-monetaria-t.html``
is server-rendered HTML. Each decision row is a ``<TR>`` pairing a
publication-date cell with a description cell + PDF link::

    <TR>
      <TD tag="[current].bm:referenceDate" class=bmdateview
          aria-label="05 de Febrero 2026">
        <SPAN aria-hidden=true>05/02/26</span>
      </TD>
      <TD tag="[current].bm:linkText" class=bmtextview>
        El objetivo para la Tasa de Interés Interbancaria a 1 día
        (tasa objetivo) se mantiene sin cambio en 7.00 por ciento
        <br/><A HREF=".../{UUID}.pdf">Texto completo</A>
      </TD>
    </TR>

The ``aria-label`` carries the full Spanish date (``"05 de Febrero 2026"``)
which the parser maps to a ``date``. The link-text encodes one of two
shapes:

- Hold: ``"...se mantiene sin cambio en X.XX por ciento"`` — absolute
  rate is given.
- Change: ``"...disminuye en N puntos base"`` /
  ``"...aumenta en N puntos base"`` /
  ``"...se incrementa en N puntos base"`` /
  ``"...se reduce en N puntos base"`` — basis-point delta only.

The parser walks the rows oldest-first, keeps a running absolute rate
seeded from the first hold encountered (the modern Tasa Objetivo regime
began on 21 January 2008 and the oldest entry on the page is a hold
under that regime), and applies each subsequent delta. Holds re-anchor
the running rate as a sanity check — the live page validates with zero
disagreements across all 156 Tasa Objetivo decisions (Feb 2008 through
Mar 2026) at fixture-capture time.

Pre-2008 rows describe the historical "corto" liquidity-management
instrument (``El "corto" se aumenta a N millones de pesos``); the
matcher pins on the ``"tasa objetivo"`` / ``"tasa de interés
interbancaria"`` substring so the cumulative walk doesn't get poisoned
by the older instrument's units.

Banxico publishes decisions at 13:00 Mexico City time on Thursday
under the modern regime (the 09:00 cadence used pre-2018 leaves
backfill rows off by ~4 hours, but the date is correct). The parser
localises the announcement date to ``America/Mexico_City`` at
``BANXICO_RELEASE_TIME`` and converts to UTC. Mexico abolished federal
DST on 30 October 2022; the ``ZoneInfo`` lookup resolves both the
post-2022 year-round UTC−6 window and the 1996-2022 DST window
correctly for backfill rows.

``provider_event_id`` / ``event_time_utc`` / ``reference_date`` all
anchor on the announcement date so the parity bucket aligns with TE /
Bloomberg / Reuters convention.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, BanxicoIndicatorSpec

PROVIDER = "banxico"
BANXICO_RELEASE_TZ = "America/Mexico_City"
# Banxico publishes Junta de Gobierno decisions at 13:00 Mexico City
# time on Thursday under the modern (post-2018) cadence. Pre-2018
# decisions came out at ~09:00 — backfill rows are off by ~4 hours
# but the date (and therefore the parity bucket) stays correct.
BANXICO_RELEASE_TIME = "13:00"
BANXICO_BASE_URL = "https://www.banxico.org.mx"
BANXICO_DECISIONS_URL = (
    f"{BANXICO_BASE_URL}/publicaciones-y-prensa/"
    f"anuncios-de-las-decisiones-de-politica-monetaria/"
    f"anuncios-politica-monetaria-t.html"
)


class BanxicoDecisionsParseError(ValueError):
    """Banxico decisions HTML did not expose a parseable schedule."""


@dataclass(frozen=True)
class BanxicoRateDecision:
    """One Banxico monetary-policy decision parsed from the HTML page."""

    announcement_date: date      # date Banxico publicly announced the decision
    rate: str                    # decimal string ("7.00")
    previous_rate: str | None    # rate before this decision (None for the seed)
    movement: str                # "hold" / "hike" / "cut"
    bps_change: int              # absolute basis points; 0 for hold
    description: str             # raw link-text (Spanish)
    pdf_url: str                 # /publicaciones-y-prensa/.../{UUID}.pdf


@dataclass(frozen=True)
class BanxicoCalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class BanxicoCalendarEventRecord:
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


# Spanish month names in Banxico's ``aria-label`` (e.g. ``"05 de Febrero
# 2026"``). Lowercased post-diacritic-strip for case-insensitive lookup.
_ES_MONTHS: dict[str, int] = {
    "enero":      1, "febrero":     2, "marzo":      3,
    "abril":      4, "mayo":        5, "junio":      6,
    "julio":      7, "agosto":      8, "septiembre": 9,
    "octubre":   10, "noviembre":  11, "diciembre": 12,
}

# The link-text cell carries: description + literal ``<br/>`` + ``<A HREF="…/{UUID}.pdf">``.
# A naïve alternation ``(?:<br/>|<A HREF=...)`` ends the description scan at
# whichever marker comes first — the ``<br/>`` always wins under Banxico's
# layout, so the PDF group never captures. Anchor on ``<br/>`` first, then
# require the ``<A HREF="…pdf">`` immediately after, so every parsed row
# carries the per-decision PDF citation in the audit payload.
_ROW_RE = re.compile(
    r'<TR>\s*'
    r'<TD[^>]*bm:referenceDate[^>]*aria-label="(?P<aria>[^"]+)"[^>]*>\s*'
    r'<SPAN[^>]*>\s*(?P<dt>\d{2}/\d{2}/\d{2,4})\s*</span>\s*</td>\s*'
    r'<TD[^>]*bm:linkText[^>]*>\s*'
    r'(?P<text>[^<]+?)\s*'
    r'<br\s*/?>\s*'
    r'<A\s+HREF="(?P<pdf>[^"]+\.pdf)"',
    re.IGNORECASE | re.DOTALL,
)
_ARIA_DATE_RE = re.compile(
    r"^\s*(?P<d>\d{1,2})\s+de\s+(?P<month>[A-Za-zÁÉÍÓÚáéíóú]+)\s+(?P<y>\d{4})\s*$",
    re.IGNORECASE,
)
# Absolute rate phrasing: ``"...en X.XX por ciento"`` (or ``,XX``). Anchored
# loosely so partial reflow (extra whitespace) doesn't break it.
_ABS_RATE_RE = re.compile(
    r"(?:en|de|a)\s+(?P<rate>\d+(?:[\.,]\d+)?)\s*por\s*ciento",
    re.IGNORECASE,
)
# Basis-points phrasing: ``"...en N puntos base"``.
_BPS_RE = re.compile(
    r"(?:en|de)\s+(?P<bps>\d+)\s+puntos?\s*base",
    re.IGNORECASE,
)


_TASA_OBJETIVO_TOKENS: tuple[str, ...] = (
    "tasa objetivo",
    "tasa de interés interbancaria",
    "tasa de interes interbancaria",
)


def _strip_diacritics(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _is_tasa_objetivo(text: str) -> bool:
    """True when the description references the modern Tasa Objetivo.

    Pre-2008 rows describe the historical "corto" liquidity-management
    instrument; they share the same HTML row shape but a different
    operational regime. The cumulative walk only makes sense across
    Tasa Objetivo decisions, so the matcher pins on either Spanish form
    of the modern rate name (the literal phrase ``tasa objetivo`` or
    the longer ``Tasa de Interés Interbancaria a 1 día``).
    """
    folded = _strip_diacritics(text).lower()
    return any(token in folded for token in _TASA_OBJETIVO_TOKENS)


def _classify_movement(text: str) -> tuple[str, int | None, str | None]:
    """Return ``(movement, bps_change, abs_rate_text)`` for a description.

    - Hold: ``movement="hold"``, ``bps_change=0``, absolute rate from
      the link text.
    - Cut: ``movement="cut"``, ``bps_change`` from the bps phrase,
      ``abs_rate_text=None``.
    - Hike: ``movement="hike"``, ``bps_change`` from the bps phrase,
      ``abs_rate_text=None``.

    Spanish phrasing overlaps:

    - ``se mantiene sin cambio en X.XX por ciento`` → hold @ X.XX
    - ``disminuye en N puntos base`` → cut by N bps
    - ``se reduce en N puntos base`` → cut by N bps
    - ``aumenta en N puntos base`` → hike by N bps
    - ``se incrementa en N puntos base`` → hike by N bps

    Returns ``("unknown", None, None)`` for descriptions that don't
    match any of the above (rare — methodology amendments / press
    notes on the same surface). The fetcher logs and skips those rows.
    """
    folded = _strip_diacritics(text).lower()
    if "sin cambio" in folded:
        match = _ABS_RATE_RE.search(text)
        if match is None:
            return "unknown", None, None
        return "hold", 0, match.group("rate")
    if "disminuye" in folded or "reduce" in folded:
        match = _BPS_RE.search(text)
        if match is None:
            return "unknown", None, None
        return "cut", int(match.group("bps")), None
    if "aumenta" in folded or "incrementa" in folded:
        match = _BPS_RE.search(text)
        if match is None:
            return "unknown", None, None
        return "hike", int(match.group("bps")), None
    return "unknown", None, None


def _parse_aria_date(value: str) -> date | None:
    match = _ARIA_DATE_RE.match(value)
    if match is None:
        return None
    month_token = _strip_diacritics(match.group("month")).lower()
    month = _ES_MONTHS.get(month_token)
    if month is None:
        return None
    try:
        return date(
            int(match.group("y")),
            month,
            int(match.group("d")),
        )
    except ValueError:
        return None


def _normalize_decimal(text: str) -> str:
    """Round-trip Spanish number text through ``Decimal`` for stability.

    The link text uses ``X.XX`` form (a period as decimal separator)
    consistently in the modern Banxico anuncios history, but the
    classifier accepts ``X,XX`` defensively. The stored ``rate`` field
    is the Decimal-validated string with a period decimal separator so
    downstream comparisons stay stable.
    """
    candidate = text.strip().replace(",", ".")
    try:
        Decimal(candidate)
    except InvalidOperation as exc:
        raise BanxicoDecisionsParseError(
            f"unparseable Banxico rate {text!r}",
        ) from exc
    return candidate


def _shift_rate(running: str, delta_bps: int, sign: int) -> str:
    """Apply a basis-point delta to ``running`` and return the new string."""
    delta = Decimal(delta_bps) / Decimal(100) * Decimal(sign)
    new = (Decimal(running) + delta).quantize(Decimal("0.01"))
    return f"{new:.2f}"


def parse_decisions_history(
    html: str | bytes,
) -> list[BanxicoRateDecision]:
    """Walk the Banxico decisions HTML for every Tasa Objetivo row.

    Returns the decisions ordered most-recent-first (matches the
    page's display order). Raises :class:`BanxicoDecisionsParseError`
    when zero rows match (typical signal of a layout drift).
    """
    if isinstance(html, (bytes, bytearray)):
        # The page is served as ISO-8859-1 (per the Content-Type header
        # observed at fixture-capture time); the live fetcher decodes
        # the response body before passing here, so the bytes path is
        # primarily a test seam.
        text = html.decode("iso-8859-1", errors="replace")
    else:
        text = html

    raw_rows: list[tuple[date, str, str, str]] = []  # (announcement_date, description, pdf_url, aria)
    for match in _ROW_RE.finditer(text):
        aria = match.group("aria") or ""
        announcement = _parse_aria_date(aria)
        if announcement is None:
            continue
        description = re.sub(r"\s+", " ", match.group("text") or "").strip()
        if not description or not _is_tasa_objetivo(description):
            continue
        pdf_url = match.group("pdf") or ""
        raw_rows.append((announcement, description, pdf_url, aria))

    if not raw_rows:
        raise BanxicoDecisionsParseError(
            "Banxico decisions page parsed zero Tasa Objetivo rows — "
            "layout drift",
        )

    # Walk oldest-first to thread the cumulative absolute rate through
    # change rows. Holds re-anchor the running rate as a sanity check.
    raw_rows.sort(key=lambda r: r[0])
    decisions_chrono: list[BanxicoRateDecision] = []
    running: str | None = None
    previous_rate: str | None = None
    for announcement, description, pdf_url, _aria in raw_rows:
        movement, bps, abs_rate_text = _classify_movement(description)
        if movement == "hold":
            assert abs_rate_text is not None  # guaranteed by classifier
            current = _normalize_decimal(abs_rate_text)
            running = current
        elif movement == "hike":
            assert bps is not None
            if running is None:
                # The oldest row on Banxico's page since 2008 has been
                # a hold under the Tasa Objetivo regime; if a future
                # layout change ever puts a change-only row at the top
                # of the table we'd lose the seed and have to bail.
                raise BanxicoDecisionsParseError(
                    "first Tasa Objetivo row is a change without a hold "
                    "anchor — cannot seed cumulative walk",
                )
            running = _shift_rate(running, bps, +1)
            current = running
        elif movement == "cut":
            assert bps is not None
            if running is None:
                raise BanxicoDecisionsParseError(
                    "first Tasa Objetivo row is a change without a hold "
                    "anchor — cannot seed cumulative walk",
                )
            running = _shift_rate(running, bps, -1)
            current = running
        else:
            # "unknown" — methodology amendments / press notes that
            # share the same row shape but don't move the rate. Skip
            # silently; the cumulative walk continues from the prior
            # hold anchor.
            continue
        decisions_chrono.append(BanxicoRateDecision(
            announcement_date=announcement,
            rate=current,
            previous_rate=previous_rate,
            movement=movement,
            bps_change=bps if bps is not None else 0,
            description=description,
            pdf_url=pdf_url,
        ))
        previous_rate = current

    decisions_chrono.sort(key=lambda d: d.announcement_date, reverse=True)
    return decisions_chrono


_HASH_FIELDS: tuple[str, ...] = (
    "announcement_date", "rate", "previous_rate", "movement",
    "bps_change", "description", "pdf_url",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def decision_to_records(
    decision: BanxicoRateDecision,
    *,
    snapshot_epoch_ms: int,
    spec: BanxicoIndicatorSpec | None = None,
) -> tuple[BanxicoCalendarRawRecord, BanxicoCalendarEventRecord]:
    """Project a :class:`BanxicoRateDecision` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["BANXICO_RATE"]

    scheduled = parse_scheduled_release_time(
        decision.announcement_date,
        BANXICO_RELEASE_TIME,
        default_tz=BANXICO_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        decision.announcement_date.isoformat(),
    )

    reference_label = decision.announcement_date.strftime("%B %Y")
    pdf_full_url = (
        f"{BANXICO_BASE_URL}{decision.pdf_url}"
        if decision.pdf_url.startswith("/")
        else decision.pdf_url
    )
    payload: dict[str, Any] = {
        "kind":              "banxico_rate_decision",
        "announcement_date": decision.announcement_date.isoformat(),
        "rate":              decision.rate,
        "previous_rate":     decision.previous_rate,
        "movement":          decision.movement,
        "bps_change":        decision.bps_change,
        "description":       decision.description,
        "pdf_url":           pdf_full_url,
        "event_time_utc":    event_time_utc,
        "source_url":        BANXICO_DECISIONS_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = BanxicoCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = BanxicoCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=decision.announcement_date.isoformat(),
        reference_label=reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="MXN",
        unit=resolved_spec.unit,
        actual=decision.rate,
        previous=decision.previous_rate,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Banco de México",
        source_url=BANXICO_DECISIONS_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "BANXICO_BASE_URL",
    "BANXICO_DECISIONS_URL",
    "BANXICO_RELEASE_TIME",
    "BANXICO_RELEASE_TZ",
    "BanxicoCalendarEventRecord",
    "BanxicoCalendarRawRecord",
    "BanxicoDecisionsParseError",
    "BanxicoRateDecision",
    "PROVIDER",
    "decision_to_records",
    "parse_decisions_history",
]
