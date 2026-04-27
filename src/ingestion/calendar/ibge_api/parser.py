"""IBGE monthly-calendar HTML → calendar projection.

The IBGE release calendar at
``ibge.gov.br/calendario/mensal.html?mes=N&ano=YYYY`` is server-rendered
HTML carrying every scheduled and recently-published statistical
release for the requested month. Each event row is a single ``<li>``
with two child blocks::

    <div class="agenda--lista__data">
        <span data-divulgacao="YYYY-MM-DD HH:MM:SS-03:00">DD/MM/YYYY</span>
    </div>
    <div class="agenda--lista__evento">
        <p>
            <a href="..." data-produto-id='NNNN'>{title}</a>
        </p>
        <p class="metadados metadados--agenda">
            Período de referência: M/YYYY
        </p>
    </div>

The ``data-divulgacao`` attribute is an ISO-8601 timestamp with the
São Paulo (UTC−3) offset baked in — that's the publication moment we
project as ``event_time_utc``. The reference period is parsed from the
``Período de referência`` line; for a quarterly release IBGE writes it
as ``"<quarter>/<year>"`` (``"4/2025"`` → Q4 2025), and for monthly
releases as ``"<month>/<year>"``. The parser captures the literal
``M/YYYY`` text (no quarter / month coercion at parse time) and lets the
projector interpret it against the indicator's declared frequency.

P1 ships five headline indicators — IPCA, IPCA-15, Industrial
Production, Unemployment Rate (PNAD-Contínua Mensal), and GDP
(quarterly Contas Nacionais). The slice is **schedule-only**: events
publish with ``actual=NULL``. Per-release values live on the linked
press-release pages; the value-side scrape is deferred to P2 (mirrors
the ABS / KOSTAT schedule-only pattern).

``provider_event_id`` keys on the standard
``synthesize_event_id(provider, country, canonical, anchor)`` with the
reference period as the anchor — monthly indicators key on the
reference month's first day (``"2026-03-01"``); quarterly indicators
key on the quarter's first day (``"2026-01-01"`` for Q1 2026). A
rescheduled release for the same data period (publication moves Apr 28
→ Apr 29 for IPCA-15 March 2026) updates the existing row instead of
spawning a stale-date duplicate.
"""

from __future__ import annotations

import hashlib
import html as html_lib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import IBGEIndicatorSpec

PROVIDER = "ibge"
IBGE_RELEASE_TZ = "America/Sao_Paulo"
IBGE_BASE_URL = "https://www.ibge.gov.br"
IBGE_CALENDAR_URL_TEMPLATE = (
    f"{IBGE_BASE_URL}/calendario/mensal.html?mes={{month}}&ano={{year}}"
)


class IBGECalendarParseError(ValueError):
    """IBGE release-calendar HTML did not expose a parseable schedule."""


# Each event row sits inside a single ``<li>`` block — the parser
# walks one block at a time and extracts the row-local fields with
# bounded sub-regexes so a missing / empty field can't bleed into the
# next row. Naively combining the three field captures into a single
# regex makes the optional ``Período de referência`` group greedy
# across rows (``.*?`` + an optional group's ``?`` prefers a match)
# and silently swallows neighbouring rows.
_LI_BLOCK_RE = re.compile(
    r'<li>(?P<body>(?:(?!<li[\s>]|</li>).)*?)</li>',
    re.IGNORECASE | re.DOTALL,
)
_DIVULG_ATTR_RE = re.compile(
    r'data-divulgacao="(?P<divulg>[^"]+)"',
    re.IGNORECASE,
)
_PID_LINK_RE = re.compile(
    r"data-produto-id\s*=\s*['\"](?P<pid>\d+)['\"]\s*>"
    r'\s*(?P<title>[^<]+?)\s*</a>',
    re.IGNORECASE | re.DOTALL,
)
_REF_PERIOD_LINE_RE = re.compile(
    r'Per[ií]odo de refer[êe]ncia:\s*(?P<refperiod>[^<\n]*)',
    re.IGNORECASE,
)


# ``data-divulgacao`` shape: ``YYYY-MM-DD HH:MM:SS-03:00``. Brazil
# does not observe DST since 2019, so the UTC-3 offset is constant for
# every release the connector encounters.
_DIVULG_RE = re.compile(
    r"^(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})"
    r"\s+(?P<H>\d{2}):(?P<M>\d{2}):(?P<S>\d{2})"
    r"(?P<off>[+-]\d{2}:\d{2})$",
)


# Reference-period text comes in two shapes:
# - Single bucket: ``"3/2026"`` — month or quarter and year.
# - Range: ``"10/2025 a 12/2025"`` — IBGE writes a quarterly release's
#   period as the inclusive month range that defines the quarter.
# Both forms anchor on the END month/quarter; the parser picks the
# range's start to identify the quarter and lets the projector map it.
_REF_PERIOD_RE = re.compile(
    r"^\s*(?P<bucket>\d{1,2})\s*/\s*(?P<year>\d{4})\s*$",
)
_REF_PERIOD_RANGE_RE = re.compile(
    r"^\s*(?P<start_month>\d{1,2})\s*/\s*(?P<start_year>\d{4})"
    r"\s+a\s+"
    r"(?P<end_month>\d{1,2})\s*/\s*(?P<end_year>\d{4})\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class IBGEReleaseAnnouncement:
    """One scheduled release row parsed from a monthly calendar page.

    ``release_datetime_utc`` is the page's ``data-divulgacao`` value
    converted to UTC. ``produto_id`` is the IBGE product number from
    the ``data-produto-id`` attribute — a stable per-product anchor
    independent of the title text, useful for audit-side joins to the
    rest of the IBGE catalog. ``reference_period_text`` is the literal
    ``"M/YYYY"`` text from the ``Período de referência`` line, or the
    empty string when the row has no reference period (e.g. ad-hoc
    releases).
    """

    release_datetime_utc: datetime
    produto_id: str
    title: str
    reference_period_text: str
    schedule_year: int           # year of the calendar page being parsed
    schedule_month: int          # 1..12, month of the calendar page being parsed


@dataclass(frozen=True)
class IBGECalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class IBGECalendarEventRecord:
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


def _parse_divulgacao(value: str) -> datetime:
    """Convert ``"YYYY-MM-DD HH:MM:SS-03:00"`` to a UTC datetime."""
    m = _DIVULG_RE.match(value)
    if m is None:
        raise IBGECalendarParseError(
            f"unparseable IBGE data-divulgacao timestamp: {value!r}",
        )
    iso = (
        f"{m.group('y')}-{m.group('m')}-{m.group('d')}"
        f"T{m.group('H')}:{m.group('M')}:{m.group('S')}{m.group('off')}"
    )
    try:
        local = datetime.fromisoformat(iso)
    except ValueError as exc:
        raise IBGECalendarParseError(
            f"unparseable IBGE data-divulgacao timestamp: {value!r}",
        ) from exc
    return local.astimezone(timezone.utc)


def parse_release_calendar(
    html: str | bytes,
    *,
    schedule_year: int,
    schedule_month: int,
) -> list[IBGEReleaseAnnouncement]:
    """Walk a single monthly-calendar page for embedded event rows.

    Returns one :class:`IBGEReleaseAnnouncement` per parseable ``<li>``
    event row. Raises :class:`IBGECalendarParseError` when zero rows
    parse — typical signal of a layout drift.

    Parameters
    ----------
    html:
        Body of the IBGE monthly-calendar HTML response.
    schedule_year, schedule_month:
        The (year, month) the page was requested for. Stored on the
        announcement so the projector can attribute a row to the
        calendar page that produced it (audit-side trace) without
        re-parsing the URL query string.
    """
    if isinstance(html, (bytes, bytearray)):
        html = html.decode("utf-8", errors="replace")
    text = html_lib.unescape(html)

    announcements: list[IBGEReleaseAnnouncement] = []
    for block_match in _LI_BLOCK_RE.finditer(text):
        block = block_match.group("body")
        divulg_match = _DIVULG_ATTR_RE.search(block)
        pid_match = _PID_LINK_RE.search(block)
        if divulg_match is None or pid_match is None:
            continue
        try:
            release_dt = _parse_divulgacao(divulg_match.group("divulg"))
        except IBGECalendarParseError:
            continue
        title = re.sub(r"\s+", " ", pid_match.group("title")).strip()
        if not title:
            continue
        ref_match = _REF_PERIOD_LINE_RE.search(block)
        ref_text = re.sub(
            r"\s+", " ",
            (ref_match.group("refperiod") if ref_match is not None else ""),
        ).strip()
        announcements.append(IBGEReleaseAnnouncement(
            release_datetime_utc=release_dt,
            produto_id=pid_match.group("pid"),
            title=title,
            reference_period_text=ref_text,
            schedule_year=schedule_year,
            schedule_month=schedule_month,
        ))

    if not announcements:
        raise IBGECalendarParseError(
            "IBGE monthly calendar page parsed zero event rows — "
            "layout drift",
        )
    return announcements


def announcement_matches_spec(
    announcement: IBGEReleaseAnnouncement,
    spec: IBGEIndicatorSpec,
) -> bool:
    """True when the announcement's IBGE product id matches the spec.

    Anchoring on ``data-produto-id`` rather than title-substring sidesteps
    the issue that several IBGE products share a common title prefix —
    IPCA (9256), IPCA-15 (9260), IPCA Especial (9270) all start with
    ``"Índice Nacional de Preços ao Consumidor Amplo"``. The product id
    is stable and unambiguous.
    """
    return announcement.produto_id in spec.produto_ids


def _reference_for(
    announcement: IBGEReleaseAnnouncement,
    spec: IBGEIndicatorSpec,
) -> tuple[date, str]:
    """Resolve ``(reference_date, reference_label)`` for a release row.

    Anchors on the first day of the reference period. For monthly
    indicators the period bucket is the reference month; for
    quarterly indicators the bucket is the reference quarter (1..4)
    and the reference date is the quarter's first day. The page
    encodes both shapes as ``"<bucket>/<year>"`` — a 1..12 bucket for
    monthly products and a 1..4 bucket for quarterly products — so
    the indicator's declared ``frequency`` disambiguates.

    When the row carries no ``Período de referência`` line (rare —
    ad-hoc supplemental releases / methodology notes), the projector
    falls back to the publication month minus one for monthly
    indicators and to the calendar quarter that ends in the
    publication month for quarterly indicators. The lag-1 fallback is
    correct for the standard release cadence of every P1 indicator.
    """
    range_match = _REF_PERIOD_RANGE_RE.match(announcement.reference_period_text)
    if range_match is not None:
        start_month = int(range_match.group("start_month"))
        start_year = int(range_match.group("start_year"))
        if spec.frequency == "quarterly":
            # Range form ``"10/2025 a 12/2025"`` — pick the start
            # month and let the quarterly mapper anchor on its
            # quarter (Q4 in this example).
            quarter = (start_month - 1) // 3 + 1
            return _bucket_to_reference(quarter, start_year, spec)
        # Monthly indicators don't normally use the range form, but
        # fall back to the start month so the row still projects with
        # a reference date.
        return _bucket_to_reference(start_month, start_year, spec)

    period_match = _REF_PERIOD_RE.match(announcement.reference_period_text)
    if period_match is None:
        return _fallback_reference(announcement, spec)

    bucket = int(period_match.group("bucket"))
    year = int(period_match.group("year"))
    return _bucket_to_reference(bucket, year, spec)


def _bucket_to_reference(
    bucket: int,
    year: int,
    spec: IBGEIndicatorSpec,
) -> tuple[date, str]:
    if spec.frequency == "quarterly":
        if not (1 <= bucket <= 4):
            raise IBGECalendarParseError(
                f"unexpected quarterly reference bucket {bucket} for "
                f"{spec.indicator}",
            )
        ref_month = (bucket - 1) * 3 + 1
        ref = date(year, ref_month, 1)
        label = f"Q{bucket} {year}"
        return ref, label
    if not (1 <= bucket <= 12):
        raise IBGECalendarParseError(
            f"unexpected monthly reference bucket {bucket} for "
            f"{spec.indicator}",
        )
    ref = date(year, bucket, 1)
    label = ref.strftime("%B %Y")
    return ref, label


def _fallback_reference(
    announcement: IBGEReleaseAnnouncement,
    spec: IBGEIndicatorSpec,
) -> tuple[date, str]:
    publication = announcement.release_datetime_utc.astimezone(timezone.utc)
    pub_year = publication.year
    pub_month = publication.month
    if spec.frequency == "quarterly":
        # Quarterly publications land in the *first* month after the
        # reference quarter closes — Mar release covers Q4 of the
        # previous year, May/Jun release covers Q1, etc. Map the
        # publication month back to the prior quarter.
        prior_month = pub_month - 1 if pub_month > 1 else 12
        prior_year = pub_year if pub_month > 1 else pub_year - 1
        bucket = (prior_month - 1) // 3 + 1
        return _bucket_to_reference(bucket, prior_year, spec)
    ref_month = pub_month - 1
    ref_year = pub_year
    if ref_month <= 0:
        ref_month += 12
        ref_year -= 1
    return _bucket_to_reference(ref_month, ref_year, spec)


_HASH_FIELDS: tuple[str, ...] = (
    "indicator", "reference_date", "release_datetime_utc",
    "title", "produto_id",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def announcement_to_records(
    announcement: IBGEReleaseAnnouncement,
    *,
    spec: IBGEIndicatorSpec,
    snapshot_epoch_ms: int,
) -> tuple[IBGECalendarRawRecord, IBGECalendarEventRecord]:
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

    source_url = IBGE_CALENDAR_URL_TEMPLATE.format(
        month=announcement.schedule_month,
        year=announcement.schedule_year,
    )

    payload: dict[str, Any] = {
        "kind":                 "ibge_release_calendar",
        "indicator":            spec.indicator,
        "produto_id":           announcement.produto_id,
        "release_datetime_utc": event_time_utc,
        "reference_date":       reference_date.isoformat(),
        "reference_label":      reference_label,
        "reference_period":     announcement.reference_period_text,
        "title":                announcement.title,
        "schedule_year":        announcement.schedule_year,
        "schedule_month":       announcement.schedule_month,
        "source_url":           source_url,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = IBGECalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = IBGECalendarEventRecord(
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
        currency="BRL",
        unit=spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Instituto Brasileiro de Geografia e Estatística",
        source_url=source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "IBGE_BASE_URL",
    "IBGE_CALENDAR_URL_TEMPLATE",
    "IBGE_RELEASE_TZ",
    "IBGECalendarEventRecord",
    "IBGECalendarParseError",
    "IBGECalendarRawRecord",
    "IBGEReleaseAnnouncement",
    "PROVIDER",
    "announcement_matches_spec",
    "announcement_to_records",
    "parse_release_calendar",
]
