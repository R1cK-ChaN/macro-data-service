"""INEGI ``saladeprensa`` calendar JSON → calendar projection.

The INEGI release calendar at
``inegi.org.mx/app/saladeprensa/calendario/`` is a SPA whose ``Calendar``
view is populated client-side by a POST to
``inegi.org.mx/app/api/saladeprensa/api/saladeprensa/ObtenerFechasTabla/v3``.
The form-encoded body specifies a ``fechaDesde`` / ``fechaHasta`` window
and an optional ``idPrograma`` server-side filter. The endpoint returns
a JSON array, one element per scheduled or already-published release
in the window.

Each row is shaped::

    {
      "idFechaPublicacion": "10154",
      "fecha":              "26/03/2026",        # DD/MM/YYYY, no time
      "programa":           "Balanza Comercial de Mercancías de México. Información oportuna",
      "periodo":            "Febrero de 2026",   # Spanish ref-period text
      "idNoticia":          "10747",
      "urlConsulta":        "https://www.inegi.org.mx/temas/balanza/",
      "subtitulo":          "...",
      "comunicadoEsUrlPdf": "/saladeprensa/.../bc.pdf",
      ...
    }

The endpoint exposes only ``fecha`` (DD/MM/YYYY) — no per-row release
time. INEGI's public ``Calendario de difusión`` rules pin every
boletín de difusión to 06:00 hora local (America/Mexico_City) at
publication. The parser localises ``fecha`` to America/Mexico_City at
each indicator's declared release_time_local, then converts to UTC.
Mexico abolished federal DST on 30 October 2022; the ``America/
Mexico_City`` zone resolves both the post-2022 year-round UTC−6 window
and the 1996-2022 DST window correctly for backfill rows.

``periodo`` carries the reference period as Spanish text:

- Monthly:        ``"Marzo de 2026"`` (month name + year).
- Quarterly:      ``"Primer trimestre de 2024"`` (Spanish ordinal +
  ``"trimestre de"`` + year).
- Biweekly:       ``"Primera quincena de Enero"`` (no year — the
  publication is always in the same month as the quincenal period, so
  the year is the publication year).

The parser maps the twelve Spanish month names + the four Spanish
quarter ordinals to canonical buckets and lets the indicator-spec
frequency disambiguate when the same idPrograma exposes both monthly
and quarterly variants (ENOE, INPC).

``provider_event_id`` keys on
``synthesize_event_id(provider, country, canonical, anchor)`` with the
reference period's first day as the anchor — monthly indicators on the
month's first day, quarterly indicators on the quarter's first day,
biweekly indicators on the quincenal month's first day (since INPC_15's
own canonical token already disambiguates from CPI). A rescheduled
release for the same data period updates the existing row instead of
spawning a stale-date duplicate.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import INEGIIndicatorSpec

PROVIDER = "inegi"
INEGI_RELEASE_TZ = "America/Mexico_City"
INEGI_BASE_URL = "https://www.inegi.org.mx"
# Public calendar URL — landed in the audit payload as ``source_url``
# so an operator can browse the row in context.
INEGI_CALENDAR_PUBLIC_URL = f"{INEGI_BASE_URL}/app/saladeprensa/calendario/"
# JSON endpoint the SPA POSTs to. Form-encoded body; date fields are
# ``YYYY-MM-DD``.
INEGI_CALENDAR_API_URL = (
    f"{INEGI_BASE_URL}/app/api/saladeprensa/api/saladeprensa/"
    f"ObtenerFechasTabla/v3"
)


class INEGICalendarParseError(ValueError):
    """INEGI release-calendar JSON did not expose a parseable schedule."""


# Spanish month names used in INEGI's ``periodo`` text. Lowercased for
# case-insensitive matching after diacritic strip — INEGI's text uses
# ``Marzo`` / ``febrero`` / ``Enero`` interchangeably; the diacritic-
# strip pass also normalises ``Diciembre`` → ``diciembre``.
_ES_MONTHS: dict[str, int] = {
    "enero":      1, "febrero":     2, "marzo":      3,
    "abril":      4, "mayo":        5, "junio":      6,
    "julio":      7, "agosto":      8, "septiembre": 9,
    "octubre":   10, "noviembre":  11, "diciembre": 12,
}

# Spanish ordinal → quarter number. INEGI quarterly periods read
# ``"Primer trimestre de 2024"``, ``"Segundo trimestre de 2024"``, etc.
_ES_QUARTERS: dict[str, int] = {
    "primer":  1, "primero":  1,
    "segundo": 2,
    "tercer":  3, "tercero":  3,
    "cuarto":  4,
}


_DDMMYYYY_RE = re.compile(r"^(?P<d>\d{2})/(?P<m>\d{2})/(?P<y>\d{4})$")
_HHMM_RE = re.compile(r"^(?P<H>\d{1,2}):(?P<M>\d{2})$")
# ``Marzo de 2026`` — month name + ``de`` + 4-digit year.
_MONTH_YEAR_RE = re.compile(
    r"^\s*([A-Za-zÁÉÍÓÚáéíóúÑñ]+)\s+de\s+(\d{4})\s*$",
    re.IGNORECASE,
)
# ``Primer trimestre de 2024`` — quarter ordinal + ``trimestre de`` +
# 4-digit year.
_QUARTER_YEAR_RE = re.compile(
    r"^\s*([A-Za-zÁÉÍÓÚáéíóú]+)\s+trimestre\s+de\s+(\d{4})\s*$",
    re.IGNORECASE,
)
# ``Primera quincena de Enero`` — biweekly preview, optionally with a
# trailing ``de YYYY`` if INEGI ever annotates the year explicitly.
_QUINCENAL_RE = re.compile(
    r"^\s*Primera\s+quincena\s+de\s+"
    r"(?P<month>[A-Za-zÁÉÍÓÚáéíóúÑñ]+)"
    r"(?:\s+de\s+(?P<year>\d{4}))?\s*$",
    re.IGNORECASE,
)


def _strip_diacritics(text: str) -> str:
    """Remove Spanish accent marks for case-folded matching.

    INEGI capitalises the first letter of month names but uses
    diacritics (``Marzo`` / ``febrero`` / ``Diciembre``) consistently.
    Stripping NFKD combining marks normalises the lookup surface so the
    ``_ES_MONTHS`` table can be plain-ASCII keyed without ``unidecode``.
    """
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


@dataclass(frozen=True)
class INEGIReleaseAnnouncement:
    """One scheduled release row parsed from the calendar JSON.

    ``release_datetime_utc`` is the row's ``fecha`` localised to
    ``America/Mexico_City`` at the indicator-spec's ``release_time_local``
    and converted to UTC. ``id_fecha_publicacion`` is the INEGI publication
    id (``idFechaPublicacion``) — a stable per-row anchor independent of
    the title text. ``id_noticia`` is the noticia id used by the public
    ``/saladeprensa/noticia/<idNoticia>`` URL for an operator-facing
    audit link. ``programa`` and ``reference_period_text`` are kept in
    the announcement so the matcher can post-filter by ``programa``
    substring and frequency-cadence shape.
    """

    fecha: date
    id_fecha_publicacion: str
    id_noticia: str
    programa: str
    reference_period_text: str
    subtitulo: str
    detail_url: str
    pdf_url: str                # full URL of the boletín PDF (or empty)
    fetched_pid: str            # idPrograma the row was fetched under
    schedule_year: int          # year of the request window's start (audit)


@dataclass(frozen=True)
class INEGICalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class INEGICalendarEventRecord:
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


def _parse_fecha(value: str) -> date:
    match = _DDMMYYYY_RE.match(value or "")
    if match is None:
        raise INEGICalendarParseError(
            f"unparseable INEGI fecha {value!r}",
        )
    return date(
        int(match.group("y")),
        int(match.group("m")),
        int(match.group("d")),
    )


def _parse_release_time_local(value: str) -> tuple[int, int]:
    match = _HHMM_RE.match(value or "")
    if match is None:
        raise INEGICalendarParseError(
            f"unparseable INEGI release_time_local {value!r}",
        )
    hour = int(match.group("H"))
    minute = int(match.group("M"))
    if not (0 <= hour < 24 and 0 <= minute < 60):
        raise INEGICalendarParseError(
            f"out-of-range INEGI release_time_local {value!r}",
        )
    return hour, minute


def parse_release_calendar(
    payload: str | bytes | list[Any],
    *,
    fetched_pid: str,
    schedule_year: int,
) -> list[INEGIReleaseAnnouncement]:
    """Walk an ``ObtenerFechasTabla/v3`` JSON response for parseable rows.

    Returns one :class:`INEGIReleaseAnnouncement` per row whose ``fecha``
    parses cleanly; rows missing ``fecha`` / ``programa`` are skipped.
    Empty arrays are a legitimate response for a window that pins to a
    program with no scheduled releases (the ``Calendario`` SPA's default
    "next 30 days" view returns ``[]`` between the boletín cycles), so
    the parser does **not** raise on an empty list — it returns ``[]``
    and lets the fetcher carry on.

    Raises :class:`INEGICalendarParseError` only on payloads that aren't
    a JSON array at all (DOM/API drift signal).
    """
    if isinstance(payload, (bytes, bytearray)):
        try:
            data = json.loads(payload.decode("utf-8", errors="replace"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise INEGICalendarParseError(
                "INEGI calendar payload is not parseable JSON",
            ) from exc
    elif isinstance(payload, str):
        try:
            data = json.loads(payload)
        except ValueError as exc:
            raise INEGICalendarParseError(
                "INEGI calendar payload is not parseable JSON",
            ) from exc
    elif isinstance(payload, list):
        data = payload
    else:
        raise INEGICalendarParseError(
            f"INEGI calendar payload type not supported: "
            f"{type(payload).__name__}",
        )

    if not isinstance(data, list):
        raise INEGICalendarParseError(
            "INEGI ObtenerFechasTabla response is not a JSON array — "
            "DOM/API drift",
        )

    announcements: list[INEGIReleaseAnnouncement] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        fecha_text = str(row.get("fecha") or "").strip()
        programa = str(row.get("programa") or "").strip()
        if not fecha_text or not programa:
            continue
        try:
            fecha = _parse_fecha(fecha_text)
        except INEGICalendarParseError:
            continue
        # Resolve the per-release PDF (``comunicadoEsUrlPdf``) to its full
        # URL — INEGI returns site-relative paths like
        # ``"/saladeprensa/boletines/2026/inpc/inpc_2q2026_01.pdf"``. The
        # boletín PDF is what carries the values for the deferred P2
        # value-side scrape, so the audit payload must preserve it
        # whether or not the row also has a ``urlConsulta`` topic page.
        pdf_path = str(row.get("comunicadoEsUrlPdf") or "").strip()
        if pdf_path and not pdf_path.lower().startswith(("http://", "https://")):
            pdf_url = INEGI_BASE_URL + pdf_path
        else:
            pdf_url = pdf_path
        url_consulta = str(row.get("urlConsulta") or "").strip()
        # ``detail_url`` is the operator-facing link. Prefer the topic
        # landing page when present; fall back to the boletín PDF for
        # rows where INEGI ships only the PDF (CPI / IMAI in the
        # captured fixtures).
        detail_url = url_consulta or pdf_url
        announcements.append(INEGIReleaseAnnouncement(
            fecha=fecha,
            id_fecha_publicacion=str(row.get("idFechaPublicacion") or "").strip(),
            id_noticia=str(row.get("idNoticia") or "").strip(),
            programa=programa,
            reference_period_text=str(row.get("periodo") or "").strip(),
            subtitulo=str(row.get("subtitulo") or "").strip(),
            detail_url=detail_url,
            pdf_url=pdf_url,
            fetched_pid=fetched_pid,
            schedule_year=schedule_year,
        ))
    return announcements


def _is_quincenal(text: str) -> bool:
    return _QUINCENAL_RE.match(text) is not None


def _is_quarterly_period(text: str) -> bool:
    match = _QUARTER_YEAR_RE.match(text)
    if match is None:
        return False
    ordinal = _strip_diacritics(match.group(1)).lower()
    return ordinal in _ES_QUARTERS


def _is_monthly_period(text: str) -> bool:
    match = _MONTH_YEAR_RE.match(text)
    if match is None:
        return False
    month_token = _strip_diacritics(match.group(1)).lower()
    return month_token in _ES_MONTHS


def _periodo_matches_frequency(
    periodo: str,
    frequency: str,
) -> bool:
    """True when ``periodo`` is empty or matches the indicator's cadence.

    ENOE publishes both a monthly headline (``"Enero de 2024"``) and a
    quarterly bulletin (``"Cuarto trimestre de 2023"``) under the same
    idPrograma. The cadence filter pins ``UNEMPLOYMENT_RATE`` to the
    monthly variant; without it the quarterly row would fall through to
    the publication-month-minus-one fallback and collide with the actual
    monthly January release on the same reference key.

    INPC publishes both a monthly headline (``"Enero de 2024"``) and a
    quincenal mid-month preview (``"Primera quincena de Enero"``) under
    the same idPrograma. The cadence filter splits the two indicators
    cleanly: ``CPI`` (frequency=``monthly``) accepts only the monthly
    shape; ``INPC_15`` (frequency=``biweekly``) accepts only the
    quincenal shape.
    """
    text = (periodo or "").strip()
    if not text:
        return True
    if frequency == "biweekly":
        return _is_quincenal(text)
    if frequency == "quarterly":
        return _is_quarterly_period(text)
    # monthly default — reject anything that looks like quarterly /
    # quincenal so cross-cadence rows under the same idPrograma don't
    # bleed into the wrong bucket.
    if _is_quincenal(text):
        return False
    if _is_quarterly_period(text):
        return False
    return _is_monthly_period(text)


def announcement_matches_spec(
    announcement: INEGIReleaseAnnouncement,
    spec: INEGIIndicatorSpec,
) -> bool:
    """True when the announcement matches all three filters.

    Three conjoined checks:

    1. **idPrograma** — the row was fetched under one of the spec's
       programme ids. Server-side filter at request time guarantees
       this for the standard fetcher, but the test seam allows the
       parser to be invoked on a multi-program payload, so the matcher
       checks again here for safety.

    2. **``programa`` substring** — case-insensitive substring match
       against every entry in ``programa_includes``. Empty tuple is a
       no-op (every row matches). Used to discriminate variants that
       share an idPrograma — Trade Balance "Información oportuna" vs
       "Cifras revisadas" both come from idPrograma 2355.

    3. **Cadence** — the row's ``periodo`` text shape must match the
       indicator's declared ``frequency``. Splits INPC monthly vs
       INPC_15 biweekly (same idPrograma 2353) and ENOE monthly vs
       quarterly (same idPrograma 2303). See
       :func:`_periodo_matches_frequency`.
    """
    if announcement.fetched_pid not in spec.tematica_ids:
        return False
    programa_lower = announcement.programa.lower()
    for needle in spec.programa_includes:
        if needle.lower() not in programa_lower:
            return False
    return _periodo_matches_frequency(
        announcement.reference_period_text, spec.frequency,
    )


def _reference_for(
    announcement: INEGIReleaseAnnouncement,
    spec: INEGIIndicatorSpec,
) -> tuple[date, str]:
    """Resolve ``(reference_date, reference_label)`` for a release row.

    Anchors on the first day of the reference period.

    - Monthly: ``"<MesNombre> de <YYYY>"`` → first day of the named
      month/year.
    - Quarterly: ``"<Ordinal> trimestre de <YYYY>"`` → first day of the
      named quarter.
    - Biweekly (INPC_15): ``"Primera quincena de <MesNombre>"`` → first
      day of the named month at the **publication year**, since INEGI
      publishes the quincenal preview within the same month as the
      reference period (Jan 24 → first half of January). When INEGI ever
      annotates an explicit ``de <YYYY>`` suffix, the parser honours it.

    A non-empty ``periodo`` whose shape doesn't match the indicator's
    declared frequency is rejected during
    :func:`announcement_matches_spec`, so this function only runs on
    matched rows. When ``periodo`` is empty (rare — ad-hoc methodology
    notes), the fallback uses publication-month-minus-one for monthly
    indicators and the calendar quarter ending in the publication month
    for quarterly indicators.
    """
    text = announcement.reference_period_text
    if not text:
        return _fallback_reference(announcement, spec)

    if spec.frequency == "biweekly":
        match = _QUINCENAL_RE.match(text)
        if match is not None:
            month_token = _strip_diacritics(match.group("month")).lower()
            month = _ES_MONTHS.get(month_token)
            if month is not None:
                year_match = match.group("year")
                year = int(year_match) if year_match else announcement.fecha.year
                ref = date(year, month, 1)
                label = f"H1 {ref.strftime('%B %Y')}"
                return ref, label
        return _fallback_reference(announcement, spec)

    if spec.frequency == "quarterly":
        match = _QUARTER_YEAR_RE.match(text)
        if match is not None:
            ordinal = _strip_diacritics(match.group(1)).lower()
            year = int(match.group(2))
            quarter = _ES_QUARTERS.get(ordinal)
            if quarter is not None:
                ref_month = (quarter - 1) * 3 + 1
                ref = date(year, ref_month, 1)
                label = f"Q{quarter} {year}"
                return ref, label
        return _fallback_reference(announcement, spec)

    # monthly
    match = _MONTH_YEAR_RE.match(text)
    if match is not None:
        month_token = _strip_diacritics(match.group(1)).lower()
        month = _ES_MONTHS.get(month_token)
        if month is not None:
            year = int(match.group(2))
            ref = date(year, month, 1)
            label = ref.strftime("%B %Y")
            return ref, label
    return _fallback_reference(announcement, spec)


def _fallback_reference(
    announcement: INEGIReleaseAnnouncement,
    spec: INEGIIndicatorSpec,
) -> tuple[date, str]:
    pub = announcement.fecha
    if spec.frequency == "quarterly":
        prior_month = pub.month - 1 if pub.month > 1 else 12
        prior_year = pub.year if pub.month > 1 else pub.year - 1
        quarter = (prior_month - 1) // 3 + 1
        ref_month = (quarter - 1) * 3 + 1
        ref = date(prior_year, ref_month, 1)
        return ref, f"Q{quarter} {prior_year}"
    if spec.frequency == "biweekly":
        ref = date(pub.year, pub.month, 1)
        return ref, f"H1 {ref.strftime('%B %Y')}"
    ref_month = pub.month - 1
    ref_year = pub.year
    if ref_month <= 0:
        ref_month += 12
        ref_year -= 1
    ref = date(ref_year, ref_month, 1)
    return ref, ref.strftime("%B %Y")


def _resolve_event_time_utc(
    fecha: date,
    release_time_local: str,
) -> str:
    hour, minute = _parse_release_time_local(release_time_local)
    local = datetime(
        fecha.year, fecha.month, fecha.day,
        hour, minute, 0,
        tzinfo=ZoneInfo(INEGI_RELEASE_TZ),
    )
    return (
        local.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


_HASH_FIELDS: tuple[str, ...] = (
    "indicator", "reference_date", "release_datetime_utc",
    "title", "id_fecha_publicacion", "id_noticia",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def announcement_to_records(
    announcement: INEGIReleaseAnnouncement,
    *,
    spec: INEGIIndicatorSpec,
    snapshot_epoch_ms: int,
) -> tuple[INEGICalendarRawRecord, INEGICalendarEventRecord]:
    """Project a matched announcement onto (raw, event) records."""
    reference_date, reference_label = _reference_for(announcement, spec)
    event_time_utc = _resolve_event_time_utc(
        announcement.fecha, spec.release_time_local,
    )

    indicator_canonical = canonicalize_indicator(spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        spec.country_code,
        indicator_canonical,
        reference_date.isoformat(),
    )

    payload: dict[str, Any] = {
        "kind":                  "inegi_release_calendar",
        "indicator":             spec.indicator,
        "id_fecha_publicacion":  announcement.id_fecha_publicacion,
        "id_noticia":            announcement.id_noticia,
        "fetched_pid":           announcement.fetched_pid,
        "release_date_local":    announcement.fecha.isoformat(),
        "release_datetime_utc":  event_time_utc,
        "reference_date":        reference_date.isoformat(),
        "reference_label":       reference_label,
        "reference_period":      announcement.reference_period_text,
        "title":                 announcement.programa,
        "subtitulo":             announcement.subtitulo,
        "detail_url":            announcement.detail_url,
        # Boletín PDF preserved on the audit payload independent of
        # ``detail_url`` so the deferred P2 value scrape can target it
        # even when ``urlConsulta`` was non-empty.
        "pdf_url":               announcement.pdf_url,
        "schedule_year":         announcement.schedule_year,
        "source_url":            INEGI_CALENDAR_PUBLIC_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = INEGICalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = INEGICalendarEventRecord(
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
        currency="MXN",
        unit=spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Instituto Nacional de Estadística y Geografía",
        source_url=INEGI_CALENDAR_PUBLIC_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "INEGI_BASE_URL",
    "INEGI_CALENDAR_API_URL",
    "INEGI_CALENDAR_PUBLIC_URL",
    "INEGI_RELEASE_TZ",
    "INEGICalendarEventRecord",
    "INEGICalendarParseError",
    "INEGICalendarRawRecord",
    "INEGIReleaseAnnouncement",
    "PROVIDER",
    "announcement_matches_spec",
    "announcement_to_records",
    "parse_release_calendar",
]
