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
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

from scripts.validate._shared import (  # noqa: E402
    Probe,
    ProbeResult,
    RowDiff,
    days_ago_iso,
    days_ahead_iso,
    render_params,
    today_iso,
)
from scripts.validate.te import (  # noqa: E402
    ALL_TE_FIELDS,
    TE_ENUM_FIELDS,
    TE_PARSER_READS,
    TE_UPDATES_POINTER_READS,
    TEAPIClient,
    diff_te_row,
    plan_te_probes,
    resolve_dynamic_ids,
    run_probe,
    try_parse,
)
from scripts.validate.eodhd import (  # noqa: E402
    EODHDAPIClient,
    EODHD_DIVIDEND_DETAIL_READS,
    EODHD_DIVIDEND_READS,
    EODHD_EARNINGS_READS,
    EODHD_ENUM_FIELDS_BY_SUBTYPE,
    EODHD_IPO_READS,
    EODHD_SPLIT_READS,
    EODHD_SUBTYPE_PARSERS,
    EODHD_SUBTYPE_READS,
    EODHD_TREND_READS,
    diff_eodhd_row,
    plan_eodhd_probes,
    run_eodhd_probe,
    try_parse_eodhd,
)
from scripts.validate.bls import (  # noqa: E402
    BLSClient,
    BLSProbe,
    BLS_INDICATOR_REGISTRY,
    BLS_OBS_EXPECTED_FIELDS,
    _diff_bls_observation,
    _try_parse_bls,
    plan_bls_probes,
    run_bls_probe,
)
from scripts.validate.bea import (  # noqa: E402
    BEAClient,
    BEAProbe,
    BEA_INDICATOR_REGISTRY,
    BEA_OBS_EXPECTED_FIELDS,
    _diff_bea_observation,
    _try_parse_bea,
    plan_bea_probes,
    run_bea_probe,
)
from scripts.validate.census import (  # noqa: E402
    CENSUS_EITS_EXPECTED_FIELDS,
    CENSUS_INDICATOR_REGISTRY,
    CensusEITSClient,
    CensusProbe,
    _diff_census_row,
    _row_matches_census_probe,
    _try_parse_census,
    plan_census_probes,
    run_census_probe,
)

from scripts.validate.ism import (  # noqa: E402
    ISMProbe,
    plan_ism_probes,
    run_ism_probe,
)
from scripts.validate.umich import (  # noqa: E402
    UMichProbe,
    plan_umich_probes,
    run_umich_probe,
)
from scripts.validate.conf_board import (  # noqa: E402
    ConferenceBoardProbe,
    plan_conference_board_probes,
    run_conference_board_probe,
)
from scripts.validate.nar import (  # noqa: E402
    NARProbe,
    plan_nar_probes,
    run_nar_probe,
)

from scripts.validate.ecb import (  # noqa: E402
    ECBClient,
    ECBProbe,
    ECB_INDICATOR_REGISTRY,
    ECB_OBS_EXPECTED_FIELDS,
    _diff_ecb_observation,
    _try_parse_ecb,
    plan_ecb_probes,
    run_ecb_probe,
)
from scripts.validate.fed import (  # noqa: E402
    FED_INDICATOR_REGISTRY,
    FedProbe,
    _try_project_fomc_entry,
    _try_project_release_entry,
    plan_fed_probes,
    run_fed_probe,
)
from scripts.validate.nbs import (  # noqa: E402
    NBS_INDICATOR_REGISTRY,
    NBSProbe,
    _try_project_nbs_entry,
    plan_nbs_probes,
    run_nbs_probe,
)
from scripts.validate.meti import (  # noqa: E402
    plan_meti_probes,
    run_meti_probe,
)
from scripts.validate.stat_bureau import (  # noqa: E402
    plan_stat_bureau_probes,
    run_stat_bureau_probe,
)




# ──────────────────────────────────────────────────────────────────────────
# Report writer
# ──────────────────────────────────────────────────────────────────────────


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



# ──────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────


_OFFICIAL_PROVIDERS: frozenset[str] = frozenset(
    {
        "bls", "bea", "census", "ism", "umich", "conference-board",
        "nar", "fed", "ecb", "nbs", "stat-bureau-jp", "meti",
    }
)
_OFFICIAL_PROVIDERS_WITH_PROBES: frozenset[str] = frozenset(
    {
        "bls", "bea", "census", "ism", "umich", "conference-board",
        "nar", "ecb", "fed", "nbs", "meti", "stat-bureau-jp",
    }
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--provider",
        choices=[
            "te", "eodhd", "bls", "bea", "census", "ism", "umich",
            "conference-board", "nar", "fed", "ecb", "nbs",
            "stat-bureau-jp", "meti",
        ],
        default="te",
        help=(
            "which acquisition lane to validate. "
            "bls / bea / census / ism / umich / conference-board / "
            "nar / ecb / fed / nbs / meti / stat-bureau-jp have live probes."
        ),
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

    # Official-source connectors with no probe body yet land on the
    # scaffold-only stub. METI ships its connector before its live
    # acquisition probe, so it stays in this set for issue #14 P5.
    unwired = _OFFICIAL_PROVIDERS - _OFFICIAL_PROVIDERS_WITH_PROBES
    if args.provider in unwired:
        print(
            f"--provider {args.provider}: scaffold registered. "
            f"Probe bodies land with the {args.provider} live-probe phase. "
            f"Dry-run stub completed."
        )
        return 0

    if args.provider == "bls":
        bls_probes = plan_bls_probes()
        return _run_bls(args, bls_probes)

    if args.provider == "bea":
        bea_probes = plan_bea_probes()
        return _run_bea(args, bea_probes)

    if args.provider == "census":
        census_probes = plan_census_probes()
        return _run_census(args, census_probes)

    if args.provider == "ism":
        ism_probes = plan_ism_probes()
        return _run_ism(args, ism_probes)

    if args.provider == "umich":
        umich_probes = plan_umich_probes()
        return _run_umich(args, umich_probes)

    if args.provider == "conference-board":
        conference_board_probes = plan_conference_board_probes()
        return _run_conference_board(args, conference_board_probes)

    if args.provider == "nar":
        nar_probes = plan_nar_probes()
        return _run_nar(args, nar_probes)

    if args.provider == "ecb":
        ecb_probes = plan_ecb_probes()
        return _run_ecb(args, ecb_probes)

    if args.provider == "fed":
        fed_probes = plan_fed_probes()
        return _run_fed(args, fed_probes)

    if args.provider == "nbs":
        nbs_probes = plan_nbs_probes()
        return _run_nbs(args, nbs_probes)

    if args.provider == "meti":
        meti_probes = plan_meti_probes()
        return _run_meti(args, meti_probes)

    if args.provider == "stat-bureau-jp":
        stat_bureau_probes = plan_stat_bureau_probes()
        return _run_stat_bureau(args, stat_bureau_probes)

    probes = plan_te_probes() if args.provider == "te" else plan_eodhd_probes()

    if not args.execute:
        print(f"DRY RUN ({args.provider}) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            query = render_params(p.params)
            print(f"   path: {p.path}{'?' + query if query else ''}")
            print(f"   purpose: {p.description}")
            print(f"   expected shape: {p.expected_shape}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes and not confirm_budget(probes):
        print("Aborted.")
        return 1

    results: list[ProbeResult] = []
    if args.provider == "te":
        with TEAPIClient() as client:
            for probe in probes:
                if probe.name == "calendarid_rehydrate":
                    dynamic_ids = resolve_dynamic_ids(results)
                    result = run_probe(client, probe, dynamic_ids=dynamic_ids)
                else:
                    result = run_probe(client, probe)
                results.append(result)
                _print_probe_summary(result)
    else:
        with EODHDAPIClient() as client:
            for probe in probes:
                result = run_eodhd_probe(client, probe)
                results.append(result)
                _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider=args.provider,
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_{args.provider}_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_bls(args: argparse.Namespace, probes: list[BLSProbe]) -> int:
    """Dispatch the BLS probe flow — dry-run plan vs --execute live run.

    BLS doesn't need the TE / EODHD budget-confirm prompt because the
    probe set is small (2 requests for P1) and BLS_API_KEY is a free-
    tier key with a 500-req-daily budget. Still honours ``--yes`` to
    match the other providers' muscle memory.
    """
    if not args.execute:
        print(f"DRY RUN (bls) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   series: {p.series_id} ({p.indicator})")
            print(f"   window: {p.start_year}-{p.end_year}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes:
        print(f"Planned BLS probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.series_id} {p.start_year}-{p.end_year}")
        print(f"Estimated upstream requests: {len(probes)}")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    client = BLSClient()
    for probe in probes:
        result = run_bls_probe(client, probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider="bls",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_bls_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_bea(args: argparse.Namespace, probes: list[BEAProbe]) -> int:
    """Dispatch the BEA probe flow — same shape as :func:`_run_bls`.

    BEA's 1000-req-daily free tier makes the 2-probe P2b run cheap; the
    confirm prompt mirrors BLS muscle memory and honours ``--yes``.
    """
    if not args.execute:
        print("DRY RUN (bea) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(
                f"   coordinate: {p.dataset} {p.table} line={p.line_number} "
                f"({p.indicator})"
            )
            print(f"   window: {p.start_year}-{p.end_year} freq={p.frequency}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes:
        print(f"Planned BEA probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(
                f"  {i}. {p.name} — {p.dataset} {p.table} line={p.line_number} "
                f"{p.start_year}-{p.end_year}"
            )
        print(f"Estimated upstream requests: {len(probes)}")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    client = BEAClient()
    for probe in probes:
        result = run_bea_probe(client, probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider="bea",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_bea_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_census(args: argparse.Namespace, probes: list[CensusProbe]) -> int:
    """Dispatch the Census EITS probe flow."""
    if not args.execute:
        print("DRY RUN (census) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(
                f"   coordinate: {p.dataset} data_type={p.data_type_code} "
                f"seasonally_adj={p.seasonally_adj} category={p.category_code}"
            )
            print(f"   year: {p.year}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes:
        print(f"Planned Census probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(
                f"  {i}. {p.name} — {p.dataset} {p.data_type_code} "
                f"{p.category_code} {p.year}"
            )
        print(f"Estimated upstream requests: {len(probes)}")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    client = CensusEITSClient()
    for probe in probes:
        result = run_census_probe(client, probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=client.requests_made,
        provider="census",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_census_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_ism(args: argparse.Namespace, probes: list[ISMProbe]) -> int:
    """Dispatch the ISM public-HTML probe flow."""
    if not args.execute:
        print("DRY RUN (ism) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   url: {p.url}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes:
        print(f"Planned ISM probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.url}")
        print("Estimated upstream requests: 3")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_ism_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=3,
        provider="ism",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_ism_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_umich(args: argparse.Namespace, probes: list[UMichProbe]) -> int:
    """Dispatch the U Michigan public HTML/PDF probe flow."""
    if not args.execute:
        print("DRY RUN (umich) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   url: {p.url}")
            print(f"   purpose: {p.description}")
        print()
        print("Total planned requests: 3")
        return 0

    if not args.yes:
        print(f"Planned U Michigan probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.url}")
        print("Estimated upstream requests: 3")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_umich_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=3,
        provider="umich",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_umich_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_conference_board(
    args: argparse.Namespace,
    probes: list[ConferenceBoardProbe],
) -> int:
    """Dispatch the Conference Board public JSON/HTML probe flow."""
    if not args.execute:
        print("DRY RUN (conference-board) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   url: {p.url}")
            print(f"   purpose: {p.description}")
        print()
        print("Total planned requests: 3")
        return 0

    if not args.yes:
        print(f"Planned Conference Board probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.url}")
        print("Estimated upstream requests: 3")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_conference_board_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=3,
        provider="conference-board",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_conference_board_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_nar(args: argparse.Namespace, probes: list[NARProbe]) -> int:
    """Dispatch the NAR public HTML probe flow."""
    if not args.execute:
        print("DRY RUN (nar) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   url: {p.url}")
            print(f"   purpose: {p.description}")
        print()
        print("Total planned requests: 3")
        return 0

    if not args.yes:
        print(f"Planned NAR probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.url}")
        print("Estimated upstream requests: 3")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_nar_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=3,
        provider="nar",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_nar_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_ecb(args: argparse.Namespace, probes: list[ECBProbe]) -> int:
    """Dispatch the ECB probe flow — same shape as :func:`_run_bls`.

    ECB Data Portal requires no auth, so the ``api_key`` bail-out
    branch is not applicable. Three probes (MRO / DFR / MLFR) make
    for a cheap live run; honours ``--yes`` for muscle memory.
    """
    if not args.execute:
        print("DRY RUN (ecb) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   series: {p.series_id} ({p.indicator})")
            print(f"   window: {p.start_period} → {p.end_period}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes:
        print(f"Planned ECB probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.series_id} {p.start_period}..{p.end_period}")
        print(f"Estimated upstream requests: {len(probes)}")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    client = ECBClient()
    for probe in probes:
        result = run_ecb_probe(client, probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider="ecb",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_ecb_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_fed(args: argparse.Namespace, probes: list[FedProbe]) -> int:
    """Dispatch the Fed probe flow — same shape as :func:`_run_ecb`.

    Fed pages are public + require no auth, so the ``api_key`` bail-
    out branch is not applicable. Two probes (FOMC calendar + release
    dates) — a cheap, lightweight live run.
    """
    if not args.execute:
        print("DRY RUN (fed) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   source: {p.source}")
            print(f"   url: {p.url}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes:
        print(f"Planned Fed probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.url}")
        print(f"Estimated upstream requests: {len(probes)}")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_fed_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider="fed",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_fed_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_nbs(args: argparse.Namespace, probes: list[NBSProbe]) -> int:
    """Dispatch the NBS probe flow — same shape as :func:`_run_fed`.

    NBS is the highest-risk upstream (HTTP-only, HTML-fragile, non-CN
    timeouts). One probe covers every registered indicator in a single
    article fetch; the runner's ``except Exception`` branch absorbs
    transient network failures cleanly rather than crashing the run.
    """
    if not args.execute:
        print("DRY RUN (nbs) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   year: {p.year}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)} (+ 1 index-page fetch)")
        return 0

    if not args.yes:
        print(f"Planned NBS probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — yearly calendar for {p.year}")
        print(f"Estimated upstream requests: {len(probes)} (+ 1 index-page fetch)")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_nbs_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    # Each NBS probe expends two upstream requests on the happy path —
    # the index-page discovery plus the yearly article fetch. The
    # dry-run summary advertises this ("+ 1 index-page fetch"); the
    # Budget section should match. When discovery fails the article
    # fetch never runs, so the probe's ``request_path`` (set only
    # after ``discover_nbs_calendar_url`` returns) acts as the
    # proxy for whether 1 or 2 requests were spent.
    def _nbs_requests_spent(r: ProbeResult) -> int:
        if r.status == "skipped":
            return 0
        return 2 if r.request_path else 1

    report = render_report(
        results,
        requests_spent=sum(_nbs_requests_spent(r) for r in results),
        provider="nbs",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_nbs_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_meti(args: argparse.Namespace, probes: list[Probe]) -> int:
    """Dispatch the METI probe flow."""
    if not args.execute:
        print("DRY RUN (meti) — pass --execute to actually hit upstream.")
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
    for probe in probes:
        result = run_meti_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider="meti",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_meti_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_stat_bureau(args: argparse.Namespace, probes: list[Probe]) -> int:
    """Dispatch the Statistics Bureau probe flow."""
    if not args.execute:
        print("DRY RUN (stat-bureau-jp) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            query = render_params(p.params)
            print(f"   path: {p.path}{'?' + query if query else ''}")
            print(f"   purpose: {p.description}")
            print(f"   expected shape: {p.expected_shape}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes and not confirm_budget(probes):
        print("Aborted.")
        return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_stat_bureau_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider="stat-bureau-jp",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_stat_bureau_jp_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
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
