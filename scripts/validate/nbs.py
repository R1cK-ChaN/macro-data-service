"""China NBS yearly-calendar validation — planner, runner.

Single-page HTML scrape per run — the NBS yearly calendar is one
article listing every registered indicator's 12-month (or quarterly)
release schedule. Probe coverage is therefore breadth (how many
indicators matched) rather than depth (per-indicator surfaces).
"""

from __future__ import annotations

import time as _time
from collections import Counter
from datetime import datetime, timezone
from dataclasses import dataclass

from ingestion.calendar.nbs_api import (
    INDICATOR_REGISTRY as NBS_INDICATOR_REGISTRY,
    NBSReleaseEntry,
    discover_nbs_calendar_url,
    fetch_nbs_calendar_index_html,
    fetch_nbs_yearly_calendar_html,
    parse_nbs_calendar_html,
    release_entry_to_records as nbs_release_entry_to_records,
)

from scripts.validate._shared import Probe, ProbeResult


# NBS is documented as the highest-risk upstream on this issue:
# HTTP-only, HTML-fragile, frequent timeouts from non-CN IPs.
# The probe runner catches any exception cleanly — a single-probe
# timeout surfaces as a loud ``http_error`` card rather than crashing
# the whole run; on the current single-indicator registry that's
# also the only probe in flight, but the tolerance shape is in place
# for when P5c-live expands the whitelist.


@dataclass
class NBSProbe:
    """One NBS yearly-calendar live-validation probe."""

    name: str
    year: int
    description: str


def plan_nbs_probes() -> list[NBSProbe]:
    """One probe per run — the yearly calendar article for the current
    UTC year. The article lists every registered indicator together,
    so a single fetch validates the breadth of
    :data:`INDICATOR_REGISTRY` coverage.
    """
    year = datetime.now(timezone.utc).year
    return [
        NBSProbe(
            name=f"nbs_yearly_calendar_{year}",
            year=year,
            description=(
                f"NBS yearly release calendar article for {year} — covers "
                f"every registered indicator in one fetch"
            ),
        )
    ]


def _try_project_nbs_entry(entry: NBSReleaseEntry) -> tuple[bool, str]:
    """Dry-run the NBS calendar projection on one entry."""
    try:
        raw, event = nbs_release_entry_to_records(
            entry,
            snapshot_epoch_ms=1_700_000_000_000,
            calendar_url="https://example.test/nbs_fixture",
        )
        return True, (
            f"ok indicator={entry.indicator} "
            f"date={entry.year}-{entry.month:02d}-{entry.day:02d} "
            f"event_id={raw.provider_event_id[:10]}…"
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run_nbs_probe(
    probe: NBSProbe,
    *,
    index_fetcher=fetch_nbs_calendar_index_html,
    html_fetcher=fetch_nbs_yearly_calendar_html,
) -> ProbeResult:
    """Execute one NBS probe and return a populated :class:`ProbeResult`.

    Auto-discovers the calendar URL via the index page, fetches the
    article, parses entries, and dry-projects each through
    ``release_entry_to_records``. ``index_fetcher`` + ``html_fetcher``
    seams let tests feed fixture HTML without touching
    ``stats.gov.cn``.

    NBS upstream is notorious for intermittent timeouts from outside
    CN; the single ``except Exception`` branch carries every failure
    mode (index timeout, article timeout, parse raise) into a
    ``http_error`` card rather than letting the exception propagate
    and kill a multi-probe run (forward-compatible for P5c-live's
    whitelist expansion).
    """
    generic = Probe(
        name=probe.name,
        path=(
            f"GET http://www.stats.gov.cn/english/PressRelease/"
            f"ReleaseCalendar/ → {probe.year} article"
        ),
        description=probe.description,
        expected_shape="list[NBSReleaseEntry] after HTML parse",
        expected_fields=frozenset(),  # HTML surface — diff not meaningful
    )
    result = ProbeResult(probe=generic, status="skipped")

    t0 = _time.monotonic()
    try:
        calendar_url = discover_nbs_calendar_url(
            probe.year, index_fetcher=index_fetcher,
        )
        result.request_path = f"GET {calendar_url}"
        html = html_fetcher(calendar_url)
        entries = parse_nbs_calendar_html(html, year_override=probe.year)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        result.notes.append(
            "NBS upstream is the highest-risk source on this issue "
            "(HTTP-only, HTML-fragile, frequent non-CN timeouts). "
            "Transient failures here are expected — retry the probe "
            "before treating it as a genuine drift signal."
        )
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000

    result.row_count = len(entries)
    if not entries:
        # The production fetcher raises ``NBSCalendarParseError`` on
        # this exact path; mirror the loud-fail shape per the Codex
        # review lesson from P4b — a green card on zero entries would
        # let an interstitial slip through.
        result.status = "http_error"
        result.notes.append(
            "zero NBS entries parsed — DOM drift or interstitial "
            "(production fetcher raises on this path)"
        )
        return result
    result.status = "ok"

    # Newest-first by (year, month, day).
    ordered = sorted(
        entries,
        key=lambda e: (e.year, e.month, e.day),
        reverse=True,
    )
    sample = ordered[0]
    result.sample_row = {
        "year":               sample.year,
        "month":              sample.month,
        "day":                sample.day,
        "indicator":          sample.indicator,
        "release_time_local": sample.release_time_local,
        "date_cell":          sample.date_cell,
    }

    # Per-indicator tally. The yearly calendar is the one place every
    # registered indicator converges, so the count-per-indicator is
    # the best drift signal: if PPI / GDP / etc. (P5c-live additions)
    # land with zero matches, their label_fragment drifted off the
    # page's actual headers.
    by_indicator: Counter = Counter()
    for e in entries:
        by_indicator[e.indicator] += 1
    result.enum_counters = {"indicator": by_indicator}
    result.notes.append(
        "entries by indicator: "
        + ", ".join(f"{k}={v}" for k, v in by_indicator.most_common())
    )

    # Dry-project up to 10 newest entries — the set whose projection
    # failure would matter most for an imminent release.
    sample_n = min(10, len(ordered))
    result.parse_attempts = sample_n
    for entry in ordered[:sample_n]:
        ok, msg = _try_project_nbs_entry(entry)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


__all__ = [
    "NBS_INDICATOR_REGISTRY",
    "NBSProbe",
    "plan_nbs_probes",
    "run_nbs_probe",
]
