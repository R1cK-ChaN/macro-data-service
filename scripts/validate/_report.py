"""Markdown-report rendering for the calendar acquisition validator.

Pure formatting — takes a list of :class:`ProbeResult` plus the
provider key + request count, returns a markdown report. No source
coupling: every per-source dispatcher in
``scripts/validate_calendar_acquisition.py`` calls
:func:`render_report` with the same shape.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from scripts.validate._shared import ProbeResult


def _json_pretty(obj: Any) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=True, default=str)


def render_report(
    results: list[ProbeResult], *, requests_spent: int, provider: str,
) -> str:
    today = datetime.now(timezone.utc).date().isoformat()
    provider_label = {
        "te": "TE", "eodhd": "EODHD", "bls": "BLS", "bea": "BEA",
        "census": "Census", "ism": "ISM", "umich": "U Michigan",
        "conference-board": "Conference Board", "nar": "NAR",
        "ecb": "ECB", "fed": "Fed", "nbs": "NBS", "meti": "METI",
        "stat-bureau-jp": "Statistics Bureau JP",
    }.get(provider, provider.upper())
    budget_line = {
        "te":    "- TE basic-plan monthly cap: 1000",
        "eodhd": "- EODHD All-in-One plan: per-call consumption (no tight cap)",
        "bls":   "- BLS Public Data API v2 free-tier daily cap: 500",
        "bea":   "- BEA REST API free-tier daily cap: 1000",
        "census": "- Census EITS API: optional key, unspecified rate limit (polite)",
        "ism":    "- ISM public HTML: no auth, unspecified rate limit (polite)",
        "umich": "- U Michigan public HTML/PDF: no auth, unspecified rate limit (polite)",
        "conference-board": "- Conference Board public HTML/JSON: no auth, unspecified rate limit (polite)",
        "nar": "- NAR public HTML: no auth, unspecified rate limit (polite)",
        "ecb":   "- ECB Data Portal: no auth, unspecified rate limit (polite)",
        "fed":   "- federalreserve.gov: no auth, HTML scrape (browser-UA required)",
        "nbs":   "- stats.gov.cn: no auth, HTTP-only, flaky from non-CN IPs",
        "meti": "- meti.go.jp public XML/HTML schedules: no auth",
        "stat-bureau-jp": "- stat.go.jp schedules + e-Stat API; ESTAT_APP_ID required for value probes",
    }.get(provider, f"- {provider_label} plan: unknown cap")
    lines: list[str] = [
        f"# {provider_label} Calendar Acquisition Validation — {today}",
        "",
        "Scope: verifies the **acquisition** step only (fetch + parse).",
        "Storage and downstream API are under our control and may be "
        "adjusted once upstream shape is understood correctly.",
        "",
        "## Budget",
        "",
        f"- Requests spent this run: **{requests_spent}**",
        budget_line,
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
    # Probes that http-errored or were skipped for missing auth never
    # populate ``field_diff`` / ``parse_attempts``, so the field-diff
    # counters alone can't tell the acquisition-layer-clean story. Without
    # this guard the summary claims "No scaffold changes required" on
    # runs where every probe 404'd (observed on the 2026-04-22 P4b-live
    # Fed run). Split the signal: good-run iff every probe returned ``ok``.
    failed_probes = [r for r in results if r.status != "ok"]
    lines.append(f"- Unknown-observed fields: {'⚠️ found' if any_unknown else '✓ none'}")
    lines.append(f"- Missing-expected fields: {'⚠️ found' if any_missing else '✓ none'}")
    lines.append(f"- Type mismatches: {'⚠️ found' if any_type else '✓ none'}")
    lines.append(f"- Parse failures in sample: {'⚠️ found' if any_parse_err else '✓ none'}")
    lines.append(
        f"- Probe-level failures: "
        f"{'⚠️ ' + str(len(failed_probes)) + ' of ' + str(len(results)) if failed_probes else '✓ none'}"
    )
    lines.append("")
    lines.append("### Action items")
    lines.append("")
    if not (any_unknown or any_missing or any_type or any_parse_err or failed_probes):
        lines.append("- Acquisition layer matches parser expectations. No scaffold changes required.")
    else:
        if failed_probes:
            lines.append(
                f"- {len(failed_probes)} probe(s) failed outright "
                f"(status ≠ ``ok``). Each probe's Note lines carry the "
                f"error — upstream drift (URL / DOM / payload shape) is "
                f"the most common cause. Resolve before treating the "
                f"remaining field-diff signal as authoritative."
            )
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


__all__ = [
    "_fmt_field_list",
    "_json_pretty",
    "_print_probe_summary",
    "_render_probe_section",
    "render_report",
]
