#!/usr/bin/env python3
"""Live validation of the calendar *acquisition layer* against upstream APIs.

Scope: only the ``获取`` (fetch + parse) step. Storage and downstream API
are under our control and can be adjusted once we know the upstream
shape is understood correctly.

Default is ``--dry-run`` (plans and prints requests, no HTTP). Pass
``--execute`` to actually hit the upstream. The script prompts once
before spending requests unless ``--yes`` is passed.

Usage::

    # Dry run — see what would happen, zero HTTP
    PYTHONPATH=src python3 scripts/validate_calendar_acquisition.py \\
        --provider te

    # Live run — hits TE with ~5–6 requests
    PYTHONPATH=src python3 scripts/validate_calendar_acquisition.py \\
        --provider te --execute

Output: a markdown report under ``docs/validation/`` with per-probe
field diffs, enum observations, and parser dry-parse results.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

# Scaffold under test — we import and exercise exactly what production uses.
from ingestion.calendar.te_api import TEAPIClient, parse_calendar_row  # noqa: E402
from ingestion.calendar.te_api.client import TECallResult  # noqa: E402
from ingestion.calendar.te_api.parser import ALL_TE_FIELDS, MUTABLE_FIELDS  # noqa: E402


# Fields the TE parser actively reads (grep parser.py — these are the
# only row.get() keys that flow into CalendarEventRecord). Anything
# observed outside this set is ignored at parse time.
TE_PARSER_READS: frozenset[str] = frozenset({
    "CalendarId", "Date", "Country", "Category", "Event",
    "Reference", "ReferenceDate", "Source", "SourceURL",
    "Actual", "Previous", "Forecast", "TEForecast", "Revised",
    "Importance", "Currency", "Unit", "Ticker", "LastUpdate",
})

# /calendar/updates returns a reduced pointer shape by design. Flagging
# the missing value/classification fields as "MISSING_EXPECTED" against
# the full 22-field read set is noise — check only the pointer set for
# those probes.
TE_UPDATES_POINTER_READS: frozenset[str] = frozenset({
    "CalendarId", "Country", "Event", "LastUpdate",
})


# ──────────────────────────────────────────────────────────────────────────
# Probe definitions
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class Probe:
    """One planned upstream request.

    ``row_extractor`` lets the updates probe extract the 4-field pointer
    list; the all-calendar and calendarid probes just return the list
    directly.
    """

    name: str
    path: str
    description: str
    expected_shape: str  # human-readable
    # Parser-reads set to compare against. Differs for /calendar/updates
    # which legitimately returns fewer fields than /country/All.
    expected_fields: frozenset[str] = TE_PARSER_READS
    row_extractor: Callable[[list[dict[str, Any]]], list[dict[str, Any]]] = (
        lambda rows: rows
    )


def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _days_ago_iso(n: int) -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=n)).isoformat()


def plan_te_probes() -> list[Probe]:
    """Fixed 5-probe budget for round 1. Extend only with evidence."""
    return [
        Probe(
            name="country_all_last_7d",
            path=f"/calendar/country/All/{_days_ago_iso(7)}/{_today_iso()}",
            description="baseline 22-field shape over a dense recent window",
            expected_shape="list[22-field dict]",
        ),
        Probe(
            name="country_all_2024_01",
            path="/calendar/country/All/2024-01-01/2024-01-07",
            description="older era — shape drift vs recent window",
            expected_shape="list[22-field dict]",
        ),
        Probe(
            # Country name with a space exercises URL encoding.
            name="country_us_last_7d",
            path=f"/calendar/country/{quote('United States')}/{_days_ago_iso(7)}/{_today_iso()}",
            description="country-scoped + URL encoding on spaces",
            expected_shape="list[22-field dict]",
        ),
        Probe(
            name="updates_pointer",
            path="/calendar/updates",
            description="pointer shape: is it really 4 fields?",
            expected_shape="list[pointer dict (CalendarId/Country/Event/LastUpdate)]",
            expected_fields=TE_UPDATES_POINTER_READS,
        ),
        Probe(
            # The 3 ids to rehydrate come from probe #1 at runtime — see
            # resolve_dynamic_probes below. If probe #1 returns nothing
            # we skip this probe entirely.
            name="calendarid_rehydrate",
            path="/calendar/calendarid/{ids}",  # template; filled at runtime
            description="rehydration shape vs /country/All full-row shape",
            expected_shape="list[22-field dict]",
        ),
    ]


# ──────────────────────────────────────────────────────────────────────────
# Diff helpers
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class RowDiff:
    """Field-level audit of one observed row vs parser expectations."""

    observed_fields: list[str] = field(default_factory=list)
    read_by_parser: list[str] = field(default_factory=list)
    ignored_by_parser: list[str] = field(default_factory=list)
    unknown_observed: list[str] = field(default_factory=list)
    missing_expected: list[str] = field(default_factory=list)
    type_warnings: list[str] = field(default_factory=list)


@dataclass
class ProbeResult:
    probe: Probe
    status: str  # "skipped" | "ok" | "http_error" | "auth_missing"
    request_path: str = ""
    http_elapsed_ms: float = 0.0
    row_count: int = 0
    truncated: bool = False
    sample_row: dict[str, Any] | None = None
    # First N CalendarIds captured from this probe's rows — feeds the
    # calendarid_rehydrate probe so we hit real ids that were just
    # returned by /country/All.
    dynamic_ids_sample: list[str] = field(default_factory=list)
    field_diff: RowDiff | None = None
    parse_attempts: int = 0
    parse_successes: int = 0
    parse_error_samples: list[str] = field(default_factory=list)
    enum_counters: dict[str, Counter] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


def diff_te_row(row: dict[str, Any], expected_fields: frozenset[str]) -> RowDiff:
    """Compare one observed TE row to the parser's known field set.

    ``expected_fields`` is the probe-specific subset parser reads
    (full 22-field for /country/All and /calendarid, pointer-only for
    /calendar/updates). Using one global set would over-report
    MISSING_EXPECTED on the updates probe.
    """
    observed = set(row.keys())
    diff = RowDiff(
        observed_fields=sorted(observed),
        read_by_parser=sorted(observed & expected_fields),
        # Known-to-TE fields we saw but parser never reads (e.g. DateSpan,
        # URL, Symbol). Informational — not a bug, but worth surfacing.
        ignored_by_parser=sorted((ALL_TE_FIELDS & observed) - expected_fields),
        unknown_observed=sorted(observed - ALL_TE_FIELDS),
        missing_expected=sorted(expected_fields - observed),
    )

    # Type spot-checks — the fields parser does explicit type handling on.
    imp = row.get("Importance")
    if imp is not None and not isinstance(imp, int):
        diff.type_warnings.append(
            f"Importance is {type(imp).__name__}={imp!r} — parser expects int "
            f"(falls through to None → importance column always null)"
        )
    cid = row.get("CalendarId")
    if cid is None:
        diff.type_warnings.append("CalendarId is None — parser raises ValueError on this row")
    elif not isinstance(cid, (int, str)):
        diff.type_warnings.append(f"CalendarId is {type(cid).__name__}={cid!r}")

    # Note: TE Date/LastUpdate strings arrive without timezone markers
    # (no Z / no offset suffix), but the values are already UTC —
    # verified against the NFIB release schedule on 2026-04-21. The
    # earlier "missing tz marker" heuristic was a false positive and has
    # been removed.
    return diff


def try_parse(row: dict[str, Any]) -> tuple[bool, str]:
    try:
        raw, event = parse_calendar_row(row, snapshot_epoch_ms=1_700_000_000_000)
        return True, f"ok provider_event_id={raw.provider_event_id[:10]}… country={event.country_code}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


# Enum-type fields we want to tally to surface the real vocabulary.
TE_ENUM_FIELDS: tuple[str, ...] = ("Importance", "Currency", "Country", "Category")


# ──────────────────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────────────────


def run_probe(
    client: TEAPIClient,
    probe: Probe,
    *,
    dynamic_ids: list[str] | None = None,
) -> ProbeResult:
    result = ProbeResult(probe=probe, status="skipped")

    path = probe.path
    if probe.name == "calendarid_rehydrate":
        if not dynamic_ids:
            result.notes.append("skipped — no ids from probe 1 to rehydrate")
            return result
        path = f"/calendar/calendarid/{','.join(dynamic_ids)}"
    result.request_path = path

    try:
        call: TECallResult = client.get(path)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result

    result.status = "ok"
    result.http_elapsed_ms = call.elapsed_ms
    result.row_count = call.row_count
    result.truncated = call.truncated

    if call.rows:
        rows = call.rows
        result.sample_row = rows[0]
        result.field_diff = diff_te_row(rows[0], probe.expected_fields)
        # Capture up to 3 CalendarIds for downstream rehydrate probe.
        for row in rows[:50]:
            cid = row.get("CalendarId")
            if cid is not None:
                result.dynamic_ids_sample.append(str(cid))
                if len(result.dynamic_ids_sample) >= 3:
                    break

        # Enum counters — tally across all rows to see real vocabulary.
        counters: dict[str, Counter] = {k: Counter() for k in TE_ENUM_FIELDS}
        for row in rows:
            for key in TE_ENUM_FIELDS:
                counters[key][repr(row.get(key))] += 1
        result.enum_counters = counters

        # Dry-parse up to 10 rows — enough to catch systematic breakage
        # without re-scanning 1000-row responses.
        sample_n = min(10, len(rows))
        result.parse_attempts = sample_n
        for row in rows[:sample_n]:
            ok, msg = try_parse(row)
            if ok:
                result.parse_successes += 1
            else:
                if len(result.parse_error_samples) < 3:
                    result.parse_error_samples.append(msg)
    return result


def resolve_dynamic_ids(prior: list[ProbeResult]) -> list[str]:
    """Pull up to 3 CalendarIds from the baseline recent-window probe."""
    for pr in prior:
        if pr.status == "ok" and pr.probe.name == "country_all_last_7d":
            return list(pr.dynamic_ids_sample)
    return []


# ──────────────────────────────────────────────────────────────────────────
# Report writer
# ──────────────────────────────────────────────────────────────────────────


def _json_pretty(obj: Any) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=True, default=str)


def render_report(results: list[ProbeResult], *, requests_spent: int) -> str:
    today = datetime.now(timezone.utc).date().isoformat()
    lines: list[str] = [
        f"# TE Calendar Acquisition Validation — {today}",
        "",
        "Scope: verifies the **acquisition** step only (fetch + parse).",
        "Storage and downstream API are under our control and may be "
        "adjusted once upstream shape is understood correctly.",
        "",
        "## Budget",
        "",
        f"- Requests spent this run: **{requests_spent}**",
        f"- TE basic-plan monthly cap: 1000",
        f"- Probes planned: {len(results)} / executed: {sum(1 for r in results if r.status == 'ok')}",
        "",
        "## Probes",
        "",
    ]

    for idx, r in enumerate(results, 1):
        lines.extend(_render_probe_section(idx, r))

    lines.append("## Summary")
    lines.append("")
    any_unknown = any(r.field_diff and r.field_diff.unknown_observed for r in results if r.field_diff)
    any_missing = any(r.field_diff and r.field_diff.missing_expected for r in results if r.field_diff)
    any_type = any(r.field_diff and r.field_diff.type_warnings for r in results if r.field_diff)
    any_parse_err = any(r.parse_successes < r.parse_attempts for r in results)
    lines.append(f"- Unknown-observed fields: {'⚠️ found' if any_unknown else '✓ none'}")
    lines.append(f"- Missing-expected fields: {'⚠️ found' if any_missing else '✓ none'}")
    lines.append(f"- Type mismatches: {'⚠️ found' if any_type else '✓ none'}")
    lines.append(f"- Parse failures in sample: {'⚠️ found' if any_parse_err else '✓ none'}")
    lines.append("")
    lines.append("### Action items")
    lines.append("")
    if not (any_unknown or any_missing or any_type or any_parse_err):
        lines.append("- Acquisition layer matches parser expectations. No scaffold changes required.")
    else:
        if any_unknown:
            lines.append("- Review UNKNOWN_OBSERVED fields per probe — may be new TE columns "
                         "worth reading or ignoring explicitly.")
        if any_missing:
            lines.append("- Review MISSING_EXPECTED — parser reads fields that never arrived. "
                         "Either defensive defaults are masking it or we're overspec'd.")
        if any_type:
            lines.append("- Review type warnings — type coercion quirks that silently corrupt "
                         "event rows.")
        if any_parse_err:
            lines.append("- Parser dry-parse failed on real rows. Check error samples per probe.")
    lines.append("")
    return "\n".join(lines)


def _render_probe_section(idx: int, r: ProbeResult) -> list[str]:
    lines: list[str] = [
        f"### Probe {idx} — `{r.probe.name}`",
        "",
        f"- Purpose: {r.probe.description}",
        f"- Expected shape: `{r.probe.expected_shape}`",
        f"- Request path: `{r.request_path or r.probe.path}`",
        f"- Status: **{r.status}**",
    ]
    if r.status == "ok":
        lines.append(f"- HTTP elapsed: {r.http_elapsed_ms:.0f} ms")
        lines.append(f"- Row count: {r.row_count}{' (⚠️ truncated at 1000)' if r.truncated else ''}")
    if r.notes:
        for note in r.notes:
            lines.append(f"- Note: {note}")
    lines.append("")

    if r.field_diff is not None:
        d = r.field_diff
        lines.append("#### Field diff (first row)")
        lines.append("")
        lines.append(f"- Observed: {_fmt_field_list(d.observed_fields)}")
        lines.append(f"- Read by parser: {_fmt_field_list(d.read_by_parser)}")
        lines.append(f"- Ignored by parser (known-but-unread): {_fmt_field_list(d.ignored_by_parser)}")
        if d.unknown_observed:
            lines.append(f"- ⚠️ **UNKNOWN_OBSERVED**: {_fmt_field_list(d.unknown_observed)}")
        else:
            lines.append("- UNKNOWN_OBSERVED: ✓ none")
        if d.missing_expected:
            lines.append(f"- ⚠️ **MISSING_EXPECTED**: {_fmt_field_list(d.missing_expected)}")
        else:
            lines.append("- MISSING_EXPECTED: ✓ none")
        if d.type_warnings:
            lines.append("- ⚠️ **Type warnings**:")
            for w in d.type_warnings:
                lines.append(f"  - {w}")
        lines.append("")

    if r.enum_counters:
        lines.append("#### Enum observations (all rows)")
        lines.append("")
        for key, counter in r.enum_counters.items():
            if not counter:
                continue
            top = counter.most_common(8)
            rendered = ", ".join(f"{k}={v}" for k, v in top)
            more = f" (+{len(counter) - len(top)} more values)" if len(counter) > len(top) else ""
            lines.append(f"- `{key}`: {rendered}{more}")
        lines.append("")

    if r.parse_attempts:
        lines.append(
            f"#### Parser dry-parse: {r.parse_successes}/{r.parse_attempts} rows parsed"
        )
        if r.parse_error_samples:
            lines.append("")
            for sample in r.parse_error_samples:
                lines.append(f"- {sample}")
        lines.append("")

    if r.sample_row is not None:
        lines.append("<details><summary>Sample row JSON</summary>")
        lines.append("")
        lines.append("```json")
        lines.append(_json_pretty(r.sample_row))
        lines.append("```")
        lines.append("")
        lines.append("</details>")
        lines.append("")

    return lines


def _fmt_field_list(fields: list[str]) -> str:
    if not fields:
        return "(none)"
    return ", ".join(f"`{f}`" for f in fields)


# ──────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--provider", choices=["te"], default="te",
        help="which acquisition lane to validate (EODHD will ship as a follow-up)",
    )
    ap.add_argument(
        "--execute", action="store_true",
        help="actually hit the upstream; default is dry-run (plan only)",
    )
    ap.add_argument(
        "--yes", action="store_true",
        help="skip the budget-confirmation prompt",
    )
    ap.add_argument(
        "--report-dir",
        default=str(REPO_ROOT / "docs" / "validation"),
        help="where to write the markdown report (default: docs/validation/)",
    )
    return ap.parse_args(argv)


def confirm_budget(probes: list[Probe]) -> bool:
    print(f"Planned probes ({len(probes)}):")
    for i, p in enumerate(probes, 1):
        print(f"  {i}. {p.name} — {p.path}")
    print(f"Estimated upstream requests: {len(probes)}")
    resp = input("Proceed with live run? [y/N] ").strip().lower()
    return resp in {"y", "yes"}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    probes = plan_te_probes()

    if not args.execute:
        print("DRY RUN — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   path: {p.path}")
            print(f"   purpose: {p.description}")
            print(f"   expected shape: {p.expected_shape}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes and not confirm_budget(probes):
        print("Aborted.")
        return 1

    results: list[ProbeResult] = []
    with TEAPIClient() as client:
        for probe in probes:
            if probe.name == "calendarid_rehydrate":
                dynamic_ids = resolve_dynamic_ids(results)
                result = run_probe(client, probe, dynamic_ids=dynamic_ids)
            else:
                result = run_probe(client, probe)
            results.append(result)
            _print_probe_summary(result)

    report = render_report(results, requests_spent=sum(
        1 for r in results if r.status == "ok"
    ))
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"calendar_acquisition_te_{datetime.now(timezone.utc).date().isoformat()}.md"
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _print_probe_summary(r: ProbeResult) -> None:
    tag = {"ok": "✓", "skipped": "-", "http_error": "✗", "auth_missing": "✗"}.get(r.status, "?")
    row_info = f"{r.row_count} rows" if r.status == "ok" else r.status
    print(f"  {tag} {r.probe.name}: {row_info}")
    if r.field_diff and r.field_diff.unknown_observed:
        print(f"      ⚠️ unknown fields: {r.field_diff.unknown_observed}")
    if r.field_diff and r.field_diff.missing_expected:
        print(f"      ⚠️ missing fields: {r.field_diff.missing_expected}")
    if r.field_diff and r.field_diff.type_warnings:
        for w in r.field_diff.type_warnings[:2]:
            print(f"      ⚠️ {w}")


if __name__ == "__main__":
    sys.exit(main())
