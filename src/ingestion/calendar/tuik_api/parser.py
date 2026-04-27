"""TÜİK national-calendar JSON → calendar projection.

The TÜİK national release calendar at
``www.tuik.gov.tr/Kurumsal/GetYillikHaberBulteniListesi?yil=YYYY``
returns a single object with two arrays — published
(``yayindaOlanlarList``) and upcoming (``yayindaOlmayanlarList``).
Combined, every row covers one scheduled or past official-statistics
release for the requested calendar year, across ~20 Turkish agencies.
The TÜİK connector filters to ``sorumluKisaAd == 'TÜİK'`` and matches
the row's ``adi`` against an indicator allowlist.

Each row is shaped::

    {
      "sorumluKisaAd":   "TÜİK",
      "sorumluKurum":    "Türkiye İstatistik Kurumu",
      "link":            "https://data.tuik.gov.tr/Bulten/Index?p=...-58295",
      "dilId":           1,
      "gTarih":          "2026-04-03T10:00:00",
      "donemi":          "Mart 2026",
      "birimi":          null,
      "adi":             "Tüketici Fiyat Endeksi (TÜFE)",
      "id":              58295
    }

``gTarih`` is an ISO-8601 string with no offset — TÜİK publishes it as
Istanbul wall-clock time. Türkiye sits on UTC+3 year-round (DST
abolished September 2016, ``Europe/Istanbul`` resolves the 2010-2016
backfill window correctly), so the parser localises the naive
timestamp to ``Europe/Istanbul`` and converts to UTC for storage.

``donemi`` carries the reference period as Turkish text — monthly
releases as ``"Mart 2026"`` (month-name + year), quarterly releases as
``"I. Çeyrek: Ocak-Mart 2026"`` (Roman-numeral quarter + month range +
year), and the rare annual release as ``"2025"``. The parser maps the
twelve Turkish month names + the four Roman-numeral quarter tokens to
their canonical bucket and lets the indicator-spec frequency
disambiguate.

``provider_event_id`` keys on
``synthesize_event_id(provider, country, canonical, anchor)`` with
the reference period's first day as the anchor — monthly indicators
key on the reference month's first day, quarterly indicators on the
quarter's first day. A rescheduled release for the same data period
updates the existing row instead of spawning a stale-date duplicate.
"""

from __future__ import annotations

import hashlib
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

from .indicators import TUIKIndicatorSpec

PROVIDER = "tuik"
TUIK_RELEASE_TZ = "Europe/Istanbul"
TUIK_BASE_URL = "https://www.tuik.gov.tr"
TUIK_CALENDAR_URL_TEMPLATE = (
    f"{TUIK_BASE_URL}/Kurumsal/GetYillikHaberBulteniListesi?yil={{year}}"
)
# TÜİK's ``sorumluKisaAd`` for releases owned by the statistical
# institute itself. Other Turkish agencies (TCMB, BDDK, SPK, …)
# share the same calendar feed — this filter pins the connector to
# the TÜİK-owned subset.
TUIK_RESPONSIBLE_CODE = "TÜİK"


class TUIKCalendarParseError(ValueError):
    """TÜİK national calendar JSON did not expose a parseable schedule."""


_NAIVE_DATETIME_RE = re.compile(
    r"^(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})"
    r"T(?P<H>\d{2}):(?P<M>\d{2}):(?P<S>\d{2})$",
)


# Turkish month names — TÜİK's reference-period text uses these
# verbatim. Lowercased for case-insensitive matching; the table is
# the canonical month-token → month-number map for the connector.
_TR_MONTHS: dict[str, int] = {
    "ocak": 1, "şubat": 2, "subat": 2,
    "mart": 3, "nisan": 4,
    "mayıs": 5, "mayis": 5,
    "haziran": 6, "temmuz": 7, "ağustos": 8, "agustos": 8,
    "eylül": 9, "eylul": 9,
    "ekim": 10, "kasım": 11, "kasim": 11,
    "aralık": 12, "aralik": 12,
}

# Roman-numeral quarter tokens. TÜİK quarterly periods read
# ``"I. Çeyrek: Ocak-Mart 2026"`` (period prefix + range + year);
# the matcher pulls the leading Roman numeral.
_TR_QUARTERS: dict[str, int] = {
    "I": 1, "II": 2, "III": 3, "IV": 4,
}

_MONTH_YEAR_RE = re.compile(
    r"^\s*([A-Za-zÇĞİıİŞÖÜçğıöşü]+)\s+(\d{4})\s*$",
)
_QUARTER_RE = re.compile(
    r"^\s*(I{1,3}|IV)\.\s*Çeyrek\s*:?",
    re.IGNORECASE,
)
_QUARTER_YEAR_RE = re.compile(r"(\d{4})\s*$")


@dataclass(frozen=True)
class TUIKReleaseAnnouncement:
    """One scheduled release row parsed from the TÜİK calendar JSON.

    ``release_datetime_utc`` is the row's ``gTarih`` localised to
    ``Europe/Istanbul`` and converted to UTC. ``bulletin_id`` is the
    TÜİK ``id`` field — a stable per-bulletin anchor independent of
    the title, used in the audit payload. ``reference_period_text`` is
    the literal ``donemi`` text. ``schedule_year`` is the calendar-page
    year the row was fetched from.
    """

    release_datetime_utc: datetime
    bulletin_id: int
    title: str
    reference_period_text: str
    schedule_year: int
    detail_url: str


@dataclass(frozen=True)
class TUIKCalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class TUIKCalendarEventRecord:
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


def _parse_gtarih(value: str) -> datetime:
    """Convert TÜİK's naive ``gTarih`` to a UTC datetime.

    TÜİK publishes the ``gTarih`` field with no offset — the timestamp
    is Istanbul wall-clock time. Türkiye observes UTC+3 year-round
    since 2016; the ``Europe/Istanbul`` zone resolves the 2010-2016
    DST window correctly for backfill.
    """
    match = _NAIVE_DATETIME_RE.match(value)
    if match is None:
        raise TUIKCalendarParseError(
            f"unparseable TÜİK gTarih timestamp: {value!r}",
        )
    local = datetime(
        int(match.group("y")),
        int(match.group("m")),
        int(match.group("d")),
        int(match.group("H")),
        int(match.group("M")),
        int(match.group("S")),
        tzinfo=ZoneInfo(TUIK_RELEASE_TZ),
    )
    return local.astimezone(timezone.utc)


def parse_release_calendar(
    payload: str | bytes | dict[str, Any],
    *,
    schedule_year: int,
) -> list[TUIKReleaseAnnouncement]:
    """Walk a TÜİK national-calendar JSON response for TÜİK-owned rows.

    Combines the published + upcoming arrays, filters to rows whose
    ``sorumluKisaAd`` equals :data:`TUIK_RESPONSIBLE_CODE`, and yields
    one announcement per parseable row. Other Turkish agencies'
    rows (TCMB, BDDK, SPK, …) are excluded — they share the same
    feed and would noisy up the projection if the connector accepted
    them.

    Raises :class:`TUIKCalendarParseError` when the response shape is
    malformed (missing arrays, every row malformed) so a layout drift
    is loud rather than silent.
    """
    if isinstance(payload, (bytes, bytearray)):
        try:
            data = json.loads(payload.decode("utf-8", errors="replace"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise TUIKCalendarParseError(
                "TÜİK calendar payload is not parseable JSON",
            ) from exc
    elif isinstance(payload, str):
        try:
            data = json.loads(payload)
        except ValueError as exc:
            raise TUIKCalendarParseError(
                "TÜİK calendar payload is not parseable JSON",
            ) from exc
    elif isinstance(payload, dict):
        data = payload
    else:
        raise TUIKCalendarParseError(
            f"TÜİK calendar payload type not supported: "
            f"{type(payload).__name__}",
        )

    published = data.get("yayindaOlanlarList") if isinstance(data, dict) else None
    upcoming = data.get("yayindaOlmayanlarList") if isinstance(data, dict) else None
    if not isinstance(published, list) or not isinstance(upcoming, list):
        raise TUIKCalendarParseError(
            "TÜİK calendar JSON missing yayindaOlanlarList / "
            "yayindaOlmayanlarList — DOM/API drift",
        )

    announcements: list[TUIKReleaseAnnouncement] = []
    for row in (*published, *upcoming):
        if not isinstance(row, dict):
            continue
        if str(row.get("sorumluKisaAd") or "").strip() != TUIK_RESPONSIBLE_CODE:
            continue
        gtarih = row.get("gTarih")
        if not isinstance(gtarih, str):
            continue
        try:
            release_dt = _parse_gtarih(gtarih)
        except TUIKCalendarParseError:
            continue
        title = str(row.get("adi") or "").strip()
        if not title:
            continue
        donemi = str(row.get("donemi") or "").strip()
        bulletin_id_raw = row.get("id")
        try:
            bulletin_id = int(bulletin_id_raw or 0)
        except (TypeError, ValueError):
            bulletin_id = 0
        detail_url = str(row.get("link") or "").strip()
        announcements.append(TUIKReleaseAnnouncement(
            release_datetime_utc=release_dt,
            bulletin_id=bulletin_id,
            title=title,
            reference_period_text=donemi,
            schedule_year=schedule_year,
            detail_url=detail_url,
        ))

    # Empty arrays are a legitimate response for an unpublished future
    # year (TÜİK posts the next-year calendar from December onward;
    # before that the JSON returns ``{[], []}`` with HTTP 200). The
    # rolling-window fetcher always plans current + next year, so the
    # daily run would otherwise store a fetch_error and trip the
    # circuit breaker for ten months out of every twelve. The DOM-
    # drift signal is *missing arrays* (caught above), not *empty
    # arrays* — return an empty list and let the fetcher carry on.
    return announcements


def announcement_matches_spec(
    announcement: TUIKReleaseAnnouncement,
    spec: TUIKIndicatorSpec,
) -> bool:
    """True when the announcement matches both ``adi`` prefix and frequency.

    Two conjoined filters:

    1. **Title prefix** — anchored on exact ``adi`` prefix rather than
       substring. ``İşgücü İstatistikleri`` (the headline labour-force
       release) and ``Tarımsal İşletme İşgücü Ücret Yapısı`` (an annual
       agricultural variant) both contain the substring ``"İşgücü"`` —
       only the first should land under ``UNEMPLOYMENT_RATE``.

    2. **Reference-period cadence** — the row's ``donemi`` must match
       the spec's declared frequency (monthly month-name + year, or
       quarterly Roman-numeral period + year). ``İşgücü İstatistikleri``
       publishes both monthly (``donemi='Şubat 2026'``) and annual
       (``donemi='2025'``) variants under the same title; without the
       cadence filter the annual row would fall through the monthly
       fallback and collide with the actual February release on the
       reference key. Empty ``donemi`` (rare ad-hoc methodology notes)
       passes through to the publication-month-minus-one fallback in
       :func:`announcement_to_records`.
    """
    title = announcement.title
    if not any(title.startswith(prefix) for prefix in spec.adi_prefixes):
        return False
    return _donemi_matches_frequency(announcement.reference_period_text, spec)


def _donemi_matches_frequency(
    donemi: str,
    spec: TUIKIndicatorSpec,
) -> bool:
    """True when ``donemi`` is empty or matches the spec's frequency."""
    text = (donemi or "").strip()
    if not text:
        return True
    if spec.frequency == "quarterly":
        return _QUARTER_RE.match(text) is not None
    month_match = _MONTH_YEAR_RE.match(text)
    if month_match is None:
        return False
    return month_match.group(1).lower() in _TR_MONTHS


def _reference_for(
    announcement: TUIKReleaseAnnouncement,
    spec: TUIKIndicatorSpec,
) -> tuple[date, str]:
    """Resolve ``(reference_date, reference_label)`` for a release row.

    Anchors on the first day of the reference period. Monthly periods
    use the Turkish month-name table; quarterly periods anchor on the
    leading Roman numeral.

    A non-empty ``donemi`` whose shape doesn't match the indicator's
    declared frequency is rejected by raising
    :class:`TUIKCalendarParseError`. This filters out cross-cadence
    annual roll-ups that share an ``adi`` with a monthly headline —
    e.g. ``İşgücü İstatistikleri`` ships both monthly (``donemi='Şubat
    2026'``) and annual (``donemi='2025'``) variants; without the
    rejection the annual row would fall through the monthly fallback
    and collide with the actual monthly Feb release on the same
    reference key.

    When ``donemi`` is empty (rare — ad-hoc methodology notes), the
    parser falls back to publication month minus one for monthly
    indicators and to the calendar quarter that ends in the
    publication month for quarterly indicators.
    """
    text = announcement.reference_period_text
    if not text:
        return _fallback_reference(announcement, spec)

    if spec.frequency == "quarterly":
        quarter_match = _QUARTER_RE.match(text)
        year_match = _QUARTER_YEAR_RE.search(text) if quarter_match else None
        if quarter_match is not None and year_match is not None:
            roman = quarter_match.group(1).upper()
            quarter = _TR_QUARTERS.get(roman)
            if quarter is not None:
                year = int(year_match.group(1))
                ref_month = (quarter - 1) * 3 + 1
                ref = date(year, ref_month, 1)
                label = f"Q{quarter} {year}"
                return ref, label
        return _fallback_reference(announcement, spec)

    month_match = _MONTH_YEAR_RE.match(text)
    if month_match is not None:
        month_token = month_match.group(1).lower()
        month = _TR_MONTHS.get(month_token)
        if month is not None:
            year = int(month_match.group(2))
            ref = date(year, month, 1)
            label = ref.strftime("%B %Y")
            return ref, label
    return _fallback_reference(announcement, spec)


def _fallback_reference(
    announcement: TUIKReleaseAnnouncement,
    spec: TUIKIndicatorSpec,
) -> tuple[date, str]:
    publication = announcement.release_datetime_utc.astimezone(timezone.utc)
    pub_year = publication.year
    pub_month = publication.month
    if spec.frequency == "quarterly":
        prior_month = pub_month - 1 if pub_month > 1 else 12
        prior_year = pub_year if pub_month > 1 else pub_year - 1
        quarter = (prior_month - 1) // 3 + 1
        ref_month = (quarter - 1) * 3 + 1
        ref = date(prior_year, ref_month, 1)
        return ref, f"Q{quarter} {prior_year}"
    ref_month = pub_month - 1
    ref_year = pub_year
    if ref_month <= 0:
        ref_month += 12
        ref_year -= 1
    ref = date(ref_year, ref_month, 1)
    return ref, ref.strftime("%B %Y")


_HASH_FIELDS: tuple[str, ...] = (
    "indicator", "reference_date", "release_datetime_utc",
    "title", "bulletin_id",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def announcement_to_records(
    announcement: TUIKReleaseAnnouncement,
    *,
    spec: TUIKIndicatorSpec,
    snapshot_epoch_ms: int,
) -> tuple[TUIKCalendarRawRecord, TUIKCalendarEventRecord]:
    """Project a matched announcement onto (raw, event) records."""
    reference_date, reference_label = _reference_for(announcement, spec)
    event_time_utc = (
        announcement.release_datetime_utc
        .astimezone(timezone.utc)
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

    source_url = TUIK_CALENDAR_URL_TEMPLATE.format(
        year=announcement.schedule_year,
    )

    payload: dict[str, Any] = {
        "kind":                 "tuik_release_calendar",
        "indicator":            spec.indicator,
        "bulletin_id":          announcement.bulletin_id,
        "release_datetime_utc": event_time_utc,
        "reference_date":       reference_date.isoformat(),
        "reference_label":      reference_label,
        "reference_period":     announcement.reference_period_text,
        "title":                announcement.title,
        "schedule_year":        announcement.schedule_year,
        "detail_url":           announcement.detail_url,
        "source_url":           source_url,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = TUIKCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = TUIKCalendarEventRecord(
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
        currency="TRY",
        unit=spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Türkiye İstatistik Kurumu",
        source_url=source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "PROVIDER",
    "TUIK_BASE_URL",
    "TUIK_CALENDAR_URL_TEMPLATE",
    "TUIK_RELEASE_TZ",
    "TUIK_RESPONSIBLE_CODE",
    "TUIKCalendarEventRecord",
    "TUIKCalendarParseError",
    "TUIKCalendarRawRecord",
    "TUIKReleaseAnnouncement",
    "announcement_matches_spec",
    "announcement_to_records",
    "parse_release_calendar",
]
