"""Daily TE-vs-official parity comparator (issue #22 P3).

Pure computation over a caller-supplied SQLite connection. The caller
runs the TE daily pull first (issue #22 P1), then invokes
:func:`compare_daily` for the same target date; the function returns
one :class:`AgencyReport` per agency with at least one anomaly.

Comparison shape:

* For every ``(country, canonical_indicator)`` pair the agency owns,
  collect TE actuals on ``target_date`` and the agency's first-release
  vintage from ``calendar_event_vintages`` for the same release.
* ``missing`` — TE has an actual on ``target_date`` but the agency has
  no event for the same ``(country, canonical, reference_period)``.
* ``mismatches`` — TE actual ≠ agency first-release actual after
  alignment normalisation.

The comparator is strict: TE / agency strings are normalised (strip
``%``, whitespace, commas, surrounding quotes), parsed as
:class:`decimal.Decimal`, scaled by the per-canonical
:class:`AlignmentSpec`, then compared by exact string equality of the
resulting Decimal (trailing zeros stripped). Anything that fails to
parse is recorded as a mismatch with ``parse_failed=True`` so a parser
regression surfaces instead of being silently classified as a match.

No HTTP, no GitHub side effects, no clock dependency beyond the
``target_date`` parameter — the filer (P4) is the side-effecting layer.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation

from ._official_shared import canonicalize_indicator
from .agency_registry import AGENCIES, AgencyDecl, AlignmentSpec, agency_for

TE_PROVIDER = "tradingeconomics"


@dataclass(frozen=True)
class MissingRelease:
    """TE carries an actual the agency has no event for."""

    country: str
    canonical: str
    reference_date: str | None
    te_event_id: str
    te_actual: str
    te_title: str


@dataclass(frozen=True)
class ValueMismatch:
    """TE and agency disagree on the first-release actual."""

    country: str
    canonical: str
    reference_date: str | None
    te_event_id: str
    agency_event_id: str
    te_actual: str
    agency_actual: str | None
    parse_failed: bool
    likely_alignment_bug: bool
    note: str


@dataclass
class AgencyReport:
    agency_id: str
    label: str
    target_date: str
    missing: list[MissingRelease] = field(default_factory=list)
    mismatches: list[ValueMismatch] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not self.missing and not self.mismatches

    @property
    def total_anomalies(self) -> int:
        return len(self.missing) + len(self.mismatches)


# ---------------------------------------------------------------------------
# Decimal normalisation
# ---------------------------------------------------------------------------

_DEFAULT_STRIP_SUFFIXES: tuple[str, ...] = ("%",)
_DEFAULT_STRIP_CHARS: tuple[str, ...] = (",", " ", "\t", "\xa0")


def _normalise(value: str | None, alignment: AlignmentSpec | None) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    suffixes = _DEFAULT_STRIP_SUFFIXES + (
        alignment.strip_suffixes if alignment else ()
    )
    for suffix in suffixes:
        if text.endswith(suffix):
            text = text[: -len(suffix)].rstrip()
    for ch in _DEFAULT_STRIP_CHARS:
        text = text.replace(ch, "")
    if not text:
        return None
    try:
        decimal_value = Decimal(text)
    except InvalidOperation:
        return None
    if alignment is not None and alignment.scale != "1":
        decimal_value = decimal_value * Decimal(alignment.scale)
    return decimal_value.normalize()


def _decimal_to_canonical_str(value: Decimal) -> str:
    """Stable string form for equality: strips trailing zeros, no scientific."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


# ---------------------------------------------------------------------------
# Vintage lookup
# ---------------------------------------------------------------------------


def _first_release_vintage(
    connection: sqlite3.Connection, *, provider: str, event_id: str,
) -> sqlite3.Row | None:
    """Earliest vintage carrying a non-null actual.

    The first non-null actual is the agency's first-release print —
    later rows in ``calendar_event_vintages`` are subsequent revisions.
    """
    return connection.execute(
        """
        SELECT actual, observed_at
        FROM calendar_event_vintages
        WHERE provider = ? AND event_id = ? AND actual IS NOT NULL
        ORDER BY julianday(observed_at) ASC, id ASC
        LIMIT 1
        """,
        (provider, event_id),
    ).fetchone()


def _bucket_key(reference_date: str | None) -> str | None:
    """Reduce reference_date to a YYYY-MM bucket so TE and agency rows
    that disagree on the day-component (TE uses end-of-month, BLS uses
    start) still match."""
    if not reference_date:
        return None
    text = reference_date[:10] if "T" in reference_date else reference_date
    if len(text) >= 7 and text[4] == "-":
        return text[:7]
    return text


# ---------------------------------------------------------------------------
# Comparator
# ---------------------------------------------------------------------------


def compare_daily(
    connection: sqlite3.Connection, *, target_date: date,
) -> list[AgencyReport]:
    """Run the full per-agency parity sweep for ``target_date``.

    Empty ``[]`` when every agency is clean. Reports with anomalies are
    included; clean agencies are dropped (the filer's clean-streak
    bookkeeping is its own concern).
    """
    iso = target_date.isoformat()
    next_iso = date.fromordinal(target_date.toordinal() + 1).isoformat()

    te_rows = connection.execute(
        """
        SELECT provider_event_id, country_code, title, reference_date,
               actual, event_time_utc
        FROM cal_econ_event
        WHERE provider = ?
          AND date(event_time_utc) = ?
          AND actual IS NOT NULL
        """,
        (TE_PROVIDER, iso),
    ).fetchall()

    reports: list[AgencyReport] = []
    for agency in AGENCIES:
        report = _compare_agency(
            connection, agency=agency, te_rows=te_rows,
            target_iso=iso, next_iso=next_iso,
        )
        if not report.clean:
            reports.append(report)
    return reports


def _compare_agency(
    connection: sqlite3.Connection,
    *,
    agency: AgencyDecl,
    te_rows: list[sqlite3.Row],
    target_iso: str,
    next_iso: str,
) -> AgencyReport:
    report = AgencyReport(
        agency_id=agency.agency_id, label=agency.label, target_date=target_iso,
    )
    placeholders = ",".join("?" * len(agency.providers))
    agency_rows = connection.execute(
        f"""
        SELECT provider, provider_event_id, country_code, title,
               reference_date, event_time_utc
        FROM cal_econ_event
        WHERE provider IN ({placeholders})
          AND date(event_time_utc) >= date(?, '-3 days')
          AND date(event_time_utc) <= date(?, '+3 days')
        """,
        (*agency.providers, target_iso, target_iso),
    ).fetchall()

    # Bucket agency rows by (country, canonical, ref_bucket). The ±3-day
    # window absorbs the case where TE timestamps a release at one UTC
    # offset and the official source timestamps it at another.
    agency_by_key: dict[tuple[str, str, str | None], sqlite3.Row] = {}
    for row in agency_rows:
        canonical = canonicalize_indicator(row["title"])
        if not canonical:
            continue
        key = (row["country_code"] or "", canonical, _bucket_key(row["reference_date"]))
        if (key not in agency_by_key) or (
            (row["event_time_utc"] or "") <
            (agency_by_key[key]["event_time_utc"] or "")
        ):
            # Prefer the earliest (= first) print when multiple stages
            # land in the window (e.g. BEA GDP advance/second/third).
            agency_by_key[key] = row

    seen_keys: set[tuple[str, str, str | None]] = set()
    for te_row in te_rows:
        canonical = canonicalize_indicator(te_row["title"])
        if not canonical:
            continue
        country = te_row["country_code"] or ""
        if agency_for(country, canonical) is not agency:
            continue
        key = (country, canonical, _bucket_key(te_row["reference_date"]))
        seen_keys.add(key)
        alignment = agency.alignment.get((country, canonical))
        agency_row = agency_by_key.get(key)
        if agency_row is None:
            report.missing.append(MissingRelease(
                country=country, canonical=canonical,
                reference_date=te_row["reference_date"],
                te_event_id=te_row["provider_event_id"],
                te_actual=te_row["actual"],
                te_title=te_row["title"],
            ))
            continue

        first_release = _first_release_vintage(
            connection,
            provider=agency_row["provider"],
            event_id=agency_row["provider_event_id"],
        )
        agency_actual = first_release["actual"] if first_release else None

        te_decimal = _normalise(te_row["actual"], alignment)
        agency_decimal = _normalise(agency_actual, None)

        if te_decimal is None or agency_decimal is None:
            report.mismatches.append(ValueMismatch(
                country=country, canonical=canonical,
                reference_date=te_row["reference_date"],
                te_event_id=te_row["provider_event_id"],
                agency_event_id=agency_row["provider_event_id"],
                te_actual=te_row["actual"],
                agency_actual=agency_actual,
                parse_failed=True,
                likely_alignment_bug=False,
                note=(
                    "agency vintage missing first-release actual"
                    if agency_actual is None and first_release is None
                    else "value did not parse as Decimal"
                ),
            ))
            continue

        te_str = _decimal_to_canonical_str(te_decimal)
        agency_str = _decimal_to_canonical_str(agency_decimal)
        if te_str == agency_str:
            continue

        likely_alignment = _looks_like_clean_factor(te_decimal, agency_decimal)
        report.mismatches.append(ValueMismatch(
            country=country, canonical=canonical,
            reference_date=te_row["reference_date"],
            te_event_id=te_row["provider_event_id"],
            agency_event_id=agency_row["provider_event_id"],
            te_actual=te_row["actual"],
            agency_actual=agency_actual,
            parse_failed=False,
            likely_alignment_bug=likely_alignment,
            note=(
                f"TE={te_str} != {agency.agency_id}={agency_str}"
                + (" (likely unit-scale alignment bug)" if likely_alignment else "")
            ),
        ))

    return report


def _looks_like_clean_factor(te: Decimal, agency: Decimal) -> bool:
    """Return True when the ratio is one of the common alignment factors."""
    if agency == 0:
        return False
    try:
        ratio = (te / agency).normalize()
    except (InvalidOperation, ZeroDivisionError):
        return False
    factor_strs = {"1000", "0.001", "100", "0.01", "0.1", "10", "-1"}
    return _decimal_to_canonical_str(ratio) in factor_strs


__all__ = [
    "AgencyReport",
    "MissingRelease",
    "ValueMismatch",
    "compare_daily",
]
