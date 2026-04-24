"""Cross-provider parity harness (issue #9 P6).

Compares `cal_econ_event` rows carrying the TE provider id
(``"tradingeconomics"``) against rows from the official-source
connectors (``"bls"`` / ``"bea"`` / ``"census"`` / ``"ism"`` /
``"umich"`` / ``"conference-board"`` / ``"nar"`` / ``"ecb"`` /
``"eurostat"`` / ``"destatis"`` / ``"zew"`` / ``"insee"`` / ``"ine"`` / ``"istat"`` / ``"federal-reserve"`` / ``"nbs"``) and reports per-indicator match coverage. Used to verify
that the official-source scheduler is collecting the same releases TE
has historically carried before the TE subscription is retired
(issue #9 P8).

Bucketing key is ``(country_code, canonicalize_indicator(title),
normalized_reference_date, event_date)``. Both sides must converge on
the same canonical token for the match to count — per-provider title
differences ("Consumer Price Index" vs "CPI") collapse through the
shared ``_official_shared.canonicalize_indicator`` alias table;
reference-date format divergence ("2026-03-31T00:00:00" vs
"2026-03-01") collapses through ``_normalize_reference_date``.

A live parity run is the feedback loop for the alias table: gaps in
the initial report are typically labels that haven't been aliased
yet. The operator workflow is: run parity → inspect TE-only / official-
only lists → add the upstream label to the alias table → re-run. The
table grows over time; we don't pre-enumerate labels we haven't seen. A bucket
with at least one TE row AND at least one official row is a match;
TE-only and official-only buckets are listed separately so the
operator can eyeball the gaps.

A TE-only gap means the scheduler is missing a release TE carries —
actionable; the indicator registry or schedule scraper needs work.
An official-only row means TE missed something the official source
caught — TE's known blind spot (NBS ad-hoc MOF / PBOC releases, per
issue #9 P6 caveat); documented but not a P6 regression.

This module is pure computation over a caller-supplied SQLite
connection. Report formatting (markdown) lives alongside; no HTTP /
filesystem side effects. The service op layer decides when to
persist the markdown under ``docs/validation/``.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

from ._official_shared import canonicalize_indicator

# Provider id written by the TE ingestion path (matches
# ``te_api.parser.PROVIDER``).
TE_PROVIDER = "tradingeconomics"

# Provider ids written by the official-source connectors (match each
# connector's ``parser.PROVIDER`` constant). Fed uses the hyphenated
# form because ``cal_provider`` carries the kebab-case id.
OFFICIAL_PROVIDERS: tuple[str, ...] = (
    "bls", "bea", "census", "ism", "umich", "conference-board",
    "nar", "ecb", "eurostat", "destatis", "zew", "insee", "ine", "istat", "federal-reserve", "nbs", "mof-jp", "cao",
    "meti", "stat-bureau-jp",
)


@dataclass(frozen=True)
class ParityEvent:
    """One TE-only or official-only row surfaced by the parity run.

    Carries enough context for the operator to locate the event in
    ``cal_econ_event`` without re-running the query.
    """

    provider: str
    provider_event_id: str
    country_code: str
    canonical_indicator: str
    reference_date: str | None
    title: str
    event_time_utc: str


@dataclass
class IndicatorParity:
    """Per-(country, canonical-indicator) match rollup."""

    canonical_indicator: str
    country_code: str
    total_events: int = 0
    matched: int = 0
    te_only: int = 0
    official_only: int = 0

    @property
    def match_percentage(self) -> float:
        if self.total_events == 0:
            return 0.0
        return round(self.matched / self.total_events * 100, 1)


@dataclass
class ParityRunSummary:
    """Outcome of a single :func:`calendar_econ_parity` invocation."""

    from_date: str
    to_date: str
    total_events: int = 0
    matched: int = 0
    te_only_count: int = 0
    official_only_count: int = 0
    indicators: list[IndicatorParity] = field(default_factory=list)
    te_only_events: list[ParityEvent] = field(default_factory=list)
    official_only_events: list[ParityEvent] = field(default_factory=list)

    @property
    def match_percentage(self) -> float:
        if self.total_events == 0:
            return 0.0
        return round(self.matched / self.total_events * 100, 1)


def _normalize_reference_date(reference_date: str | None) -> str | None:
    """Collapse provider-specific reference_date formats to a bucket key.

    Providers disagree on how to represent the same reference period:
    TE's ``ReferenceDate`` lands as ``"2026-03-31T00:00:00"``; BLS
    monthly observations as ``"2026-03-01"``; BEA quarterly as the
    end-of-quarter ``"2026-03-31"``. Keying the bucket on the raw
    string fragments the same release into multiple buckets — the
    report then shows paired TE-only / official-only gaps for
    releases both sides actually carry.

    Truncating to ``YYYY-MM`` collapses monthly and quarterly formats
    to a common key. Weekly releases (e.g. Jobless Claims) share the
    month bucket but are separated by the ``event_date`` component of
    the full key (see :func:`_normalize_bucket_key`), so four weekly
    prints in a month stay in distinct buckets.
    """
    if reference_date is None:
        return None
    date_part = reference_date[:10] if "T" in reference_date else reference_date
    # YYYY-MM prefix when the string is a well-formed ISO date.
    if len(date_part) >= 7 and date_part[4] == "-":
        return date_part[:7]
    return date_part


def _normalize_bucket_key(
    country: str | None,
    canonical: str,
    reference_date: str | None,
    event_time_utc: str | None,
) -> tuple[str, str, str | None, str]:
    """Bucket key for parity matching.

    ``event_date`` (UTC calendar day derived from ``event_time_utc``)
    separates staged releases that share a reference period — e.g.
    BEA GDP's advance / second / third prints for the same quarter
    land on different release days. Without this, a TE row from the
    advance release would incorrectly match a BEA row from the
    second-stage print, masking a gap.
    """
    event_date = (event_time_utc or "")[:10]
    return (
        country or "",
        canonical,
        _normalize_reference_date(reference_date),
        event_date,
    )


def _normalize_inclusive_end(to_date: str) -> str:
    """Make a date-only ``to_date`` cover every datetime on that day.

    ``event_time_utc`` stores ISO datetimes like
    ``"2026-04-22T12:30:00+00:00"``. Lexicographic comparison against
    a plain-date bound like ``"2026-04-22"`` excludes every same-day
    datetime (``"T"`` sorts after end-of-string), so the advertised-
    inclusive upper bound would silently omit the final day's rows.
    A YYYY-MM-DD bound becomes end-of-day UTC; longer strings (already
    datetime-form) pass through unchanged.
    """
    if len(to_date) == 10 and to_date.count("-") == 2:
        return f"{to_date}T23:59:59.999999+00:00"
    return to_date


def _row_to_event(row: sqlite3.Row, canonical: str) -> ParityEvent:
    return ParityEvent(
        provider=row["provider"],
        provider_event_id=row["provider_event_id"],
        country_code=row["country_code"] or "",
        canonical_indicator=canonical,
        reference_date=row["reference_date"],
        title=row["title"],
        event_time_utc=row["event_time_utc"],
    )


def calendar_econ_parity(
    connection: sqlite3.Connection,
    *,
    from_date: str,
    to_date: str,
    indicators: Iterable[str] | None = None,
) -> ParityRunSummary:
    """Compare TE and official-source rows in ``cal_econ_event``.

    Parameters
    ----------
    connection:
        Open SQLite connection with ``row_factory=sqlite3.Row``.
    from_date, to_date:
        Inclusive ISO-8601 window (date or datetime). Rows with
        ``event_time_utc`` in this window are considered. Callers
        should match this to the scheduler's operational window —
        older dates pull in TE-historical rows that have no
        official-source counterpart by construction.
    indicators:
        Optional filter on canonical indicator tokens
        (``["CPI", "NFP"]``). Omit to cover every canonicalizable
        row in the window; pass the whitelist when producing a
        production parity report so TE's long-tail low-importance
        rows don't drown the signal.

    Returns
    -------
    :class:`ParityRunSummary`
        Totals, per-indicator breakdown, and the full TE-only /
        official-only lists for the caller to persist or inspect.
    """
    indicator_filter = (
        set(indicators) if indicators is not None else None
    )
    effective_to = _normalize_inclusive_end(to_date)

    rows = connection.execute(
        """
        SELECT provider, provider_event_id, country_code, title,
               reference_date, event_time_utc
        FROM cal_econ_event
        WHERE event_time_utc >= ?
          AND event_time_utc <= ?
        """,
        (from_date, effective_to),
    ).fetchall()

    # Bucket rows by (country, canonical_indicator, reference_date,
    # event_date). Inside each bucket, group by provider so we can
    # detect "TE has it, no official" vs the inverse without
    # double-counting when a provider publishes multiple
    # revision-adjacent rows for the same release. ``event_date`` is
    # part of the key so staged releases on different days (BEA GDP's
    # advance / second / third prints of the same quarter) stay in
    # separate buckets and each stage's gaps surface independently.
    buckets: dict[
        tuple[str, str, str | None, str], dict[str, list[sqlite3.Row]]
    ] = {}
    for row in rows:
        canonical = canonicalize_indicator(row["title"])
        if not canonical:
            continue
        if indicator_filter is not None and canonical not in indicator_filter:
            continue
        key = _normalize_bucket_key(
            row["country_code"], canonical, row["reference_date"],
            row["event_time_utc"],
        )
        buckets.setdefault(key, {}).setdefault(row["provider"], []).append(row)

    indicator_stats: dict[tuple[str, str], IndicatorParity] = {}
    te_only_events: list[ParityEvent] = []
    official_only_events: list[ParityEvent] = []
    matched = 0
    te_only = 0
    official_only = 0

    official_set = set(OFFICIAL_PROVIDERS)

    for (country, canonical, _reference_date, _event_date), providers_in_bucket in (
        buckets.items()
    ):
        te_rows = providers_in_bucket.get(TE_PROVIDER, [])
        official_rows = [
            r for p, rs in providers_in_bucket.items() if p in official_set
            for r in rs
        ]
        te_present = bool(te_rows)
        official_present = bool(official_rows)

        stat = indicator_stats.setdefault(
            (country, canonical),
            IndicatorParity(
                canonical_indicator=canonical, country_code=country,
            ),
        )
        stat.total_events += 1

        if te_present and official_present:
            stat.matched += 1
            matched += 1
        elif te_present:
            stat.te_only += 1
            te_only += 1
            # Report the first TE row as the representative — the
            # bucket may carry several revisions, but the operator
            # only needs one handle to locate the group.
            te_only_events.append(_row_to_event(te_rows[0], canonical))
        elif official_present:
            stat.official_only += 1
            official_only += 1
            official_only_events.append(
                _row_to_event(official_rows[0], canonical),
            )
        # else: both sides absent (canonicalization yielded empty, or
        # indicator filter matched with zero providers). Should not
        # happen given the filter above; skip defensively.

    indicators_list = sorted(
        indicator_stats.values(),
        key=lambda x: (x.country_code, x.canonical_indicator),
    )
    te_only_events.sort(
        key=lambda e: (e.country_code, e.canonical_indicator, e.reference_date or ""),
    )
    official_only_events.sort(
        key=lambda e: (e.country_code, e.canonical_indicator, e.reference_date or ""),
    )

    total = sum(s.total_events for s in indicators_list)
    return ParityRunSummary(
        from_date=from_date,
        to_date=to_date,
        total_events=total,
        matched=matched,
        te_only_count=te_only,
        official_only_count=official_only,
        indicators=indicators_list,
        te_only_events=te_only_events,
        official_only_events=official_only_events,
    )


def format_parity_report(summary: ParityRunSummary) -> str:
    """Render a :class:`ParityRunSummary` as a markdown report.

    Output is suitable for checking into ``docs/validation/`` under
    ``calendar_parity_<date>.md``. The report is kept compact — the
    per-indicator table is usually more useful than a full per-event
    dump, so TE-only / official-only lists are truncated at 50 entries
    each with a tail marker.
    """
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines: list[str] = []
    lines.append(f"# Calendar parity report — {generated_at}")
    lines.append("")
    lines.append(f"Window: `{summary.from_date}` → `{summary.to_date}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Total whitelisted events: **{summary.total_events}**")
    lines.append(
        f"- Matched (both sides): **{summary.matched}** "
        f"({summary.match_percentage}%)"
    )
    lines.append(f"- TE-only: **{summary.te_only_count}**")
    lines.append(f"- Official-only: **{summary.official_only_count}**")
    lines.append("")

    if summary.indicators:
        lines.append("## Per-indicator breakdown")
        lines.append("")
        lines.append(
            "| Country | Indicator | Total | Matched | TE-only | Official-only | Match % |"
        )
        lines.append(
            "|---------|-----------|-------|---------|---------|---------------|---------|"
        )
        for stat in summary.indicators:
            lines.append(
                f"| {stat.country_code} | {stat.canonical_indicator} "
                f"| {stat.total_events} | {stat.matched} "
                f"| {stat.te_only} | {stat.official_only} "
                f"| {stat.match_percentage}% |"
            )
        lines.append("")

    _append_event_list(
        lines,
        title="TE-only events (scheduler missing a release TE carries)",
        events=summary.te_only_events,
    )
    _append_event_list(
        lines,
        title="Official-only events (TE missed a release the scheduler caught)",
        events=summary.official_only_events,
    )

    return "\n".join(lines) + "\n"


_EVENT_LIST_CAP = 50


def _append_event_list(
    lines: list[str], *, title: str, events: list[ParityEvent],
) -> None:
    lines.append(f"## {title}")
    lines.append("")
    if not events:
        lines.append("_none_")
        lines.append("")
        return
    shown = events[:_EVENT_LIST_CAP]
    for event in shown:
        lines.append(
            f"- `{event.reference_date or event.event_time_utc}` "
            f"{event.country_code} {event.canonical_indicator} "
            f"({event.provider}:{event.provider_event_id}) — {event.title}"
        )
    if len(events) > _EVENT_LIST_CAP:
        lines.append(
            f"_…and {len(events) - _EVENT_LIST_CAP} more not shown_"
        )
    lines.append("")
