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
import sys
from datetime import datetime, timezone
from pathlib import Path

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
from scripts.validate._report import (  # noqa: E402
    _fmt_field_list,
    _json_pretty,
    _print_probe_summary,
    _render_probe_section,
    render_report,
)






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


def _run_provider(
    args: argparse.Namespace,
    probes: list,
    *,
    provider: str,
    run_one,
    dry_lines,
    confirm: str = "list",
    confirm_line=None,
    label: str | None = None,
    upstream_count=lambda probes: str(len(probes)),
    requests_spent=None,
) -> int:
    """Drive the dry-run-vs-execute flow for one provider.

    All 12 single-provider flows share this shape; per-source
    variability lives in the kwargs:

    - ``provider`` — provider key string (used as the report-section
      tag and as the source for the report filename, with ``-``
      mapped to ``_``).
    - ``run_one`` — ``probe -> ProbeResult`` (closes over a client
      where the source needs one; e.g. BLS / BEA / Census / ECB).
    - ``dry_lines`` — ``probe -> list[str]`` for the indented per-probe
      lines printed during ``--dry-run``.
    - ``confirm`` — ``"list"`` (per-probe enumeration) or ``"budget"``
      (the generic :func:`confirm_budget` prompt used by TE / EODHD /
      METI / Stat Bureau).
    - ``confirm_line`` — ``probe -> str`` summary line shown in the
      ``confirm == "list"`` prompt; required when ``confirm == "list"``.
    - ``label`` — display name in the confirm prompt header (e.g.
      ``"U Michigan"`` for ``provider="umich"``); defaults to
      ``provider.upper()``.
    - ``upstream_count`` — ``probes -> str`` for the count shown in
      both the dry-run footer and the confirm prompt (default
      ``str(len(probes))``; ISM / UMich / ConfBoard / NAR hardcode
      ``"3"``; NBS appends ``" (+ 1 index-page fetch)"``).
    - ``requests_spent`` — ``results -> int`` for the budget figure
      embedded in the markdown report; default counts ``status == "ok"``
      results.
    """
    count_str = upstream_count(probes)
    if not args.execute:
        print(f"DRY RUN ({provider}) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            for line in dry_lines(p):
                print(f"   {line}")
        print()
        print(f"Total planned requests: {count_str}")
        return 0

    if confirm == "budget":
        if not args.yes and not confirm_budget(probes):
            print("Aborted.")
            return 1
    else:
        if not args.yes:
            assert confirm_line is not None, (
                "confirm_line is required when confirm='list'"
            )
            display = label if label is not None else provider.upper()
            print(f"Planned {display} probes ({len(probes)}):")
            for i, p in enumerate(probes, 1):
                print(f"  {i}. {confirm_line(p)}")
            print(f"Estimated upstream requests: {count_str}")
            resp = input("Proceed with live run? [y/N] ").strip().lower()
            if resp not in {"y", "yes"}:
                print("Aborted.")
                return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_one(probe)
        results.append(result)
        _print_probe_summary(result)

    spent = (
        requests_spent(results)
        if requests_spent is not None
        else sum(1 for r in results if r.status == "ok")
    )
    report = render_report(results, requests_spent=spent, provider=provider)
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_{provider.replace('-', '_')}_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


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
    """Dispatch the BLS probe flow — dry-run plan vs --execute live run."""
    client = BLSClient()
    return _run_provider(
        args, probes, provider="bls",
        run_one=lambda p: run_bls_probe(client, p),
        dry_lines=lambda p: [
            f"series: {p.series_id} ({p.indicator})",
            f"window: {p.start_year}-{p.end_year}",
            f"purpose: {p.description}",
        ],
        confirm_line=lambda p: f"{p.name} — {p.series_id} {p.start_year}-{p.end_year}",
    )


def _run_bea(args: argparse.Namespace, probes: list[BEAProbe]) -> int:
    """Dispatch the BEA probe flow."""
    client = BEAClient()
    return _run_provider(
        args, probes, provider="bea",
        run_one=lambda p: run_bea_probe(client, p),
        dry_lines=lambda p: [
            f"coordinate: {p.dataset} {p.table} line={p.line_number} ({p.indicator})",
            f"window: {p.start_year}-{p.end_year} freq={p.frequency}",
            f"purpose: {p.description}",
        ],
        confirm_line=lambda p: (
            f"{p.name} — {p.dataset} {p.table} line={p.line_number} "
            f"{p.start_year}-{p.end_year}"
        ),
    )


def _run_census(args: argparse.Namespace, probes: list[CensusProbe]) -> int:
    """Dispatch the Census EITS probe flow."""
    client = CensusEITSClient()
    return _run_provider(
        args, probes, provider="census", label="Census",
        run_one=lambda p: run_census_probe(client, p),
        dry_lines=lambda p: [
            f"coordinate: {p.dataset} data_type={p.data_type_code} "
            f"seasonally_adj={p.seasonally_adj} category={p.category_code}",
            f"year: {p.year}",
            f"purpose: {p.description}",
        ],
        confirm_line=lambda p: (
            f"{p.name} — {p.dataset} {p.data_type_code} "
            f"{p.category_code} {p.year}"
        ),
        requests_spent=lambda _: client.requests_made,
    )


# All four "schedule + current values" HTML scraper providers (ISM,
# UMich, ConfBoard, NAR) hit a fixed three upstream surfaces (one
# schedule + two indicators); both the dry-run footer and the report
# Budget line use that constant rather than ``len(probes)``.
def _three_requests(_probes) -> str:
    return "3"


def _three_spent(_results) -> int:
    return 3


def _run_ism(args: argparse.Namespace, probes: list[ISMProbe]) -> int:
    """Dispatch the ISM public-HTML probe flow."""
    return _run_provider(
        args, probes, provider="ism",
        run_one=run_ism_probe,
        dry_lines=lambda p: [f"url: {p.url}", f"purpose: {p.description}"],
        confirm_line=lambda p: f"{p.name} — {p.url}",
        upstream_count=_three_requests,
        requests_spent=_three_spent,
    )


def _run_umich(args: argparse.Namespace, probes: list[UMichProbe]) -> int:
    """Dispatch the U Michigan public HTML/PDF probe flow."""
    return _run_provider(
        args, probes, provider="umich", label="U Michigan",
        run_one=run_umich_probe,
        dry_lines=lambda p: [f"url: {p.url}", f"purpose: {p.description}"],
        confirm_line=lambda p: f"{p.name} — {p.url}",
        upstream_count=_three_requests,
        requests_spent=_three_spent,
    )


def _run_conference_board(
    args: argparse.Namespace,
    probes: list[ConferenceBoardProbe],
) -> int:
    """Dispatch the Conference Board public JSON/HTML probe flow."""
    return _run_provider(
        args, probes, provider="conference-board", label="Conference Board",
        run_one=run_conference_board_probe,
        dry_lines=lambda p: [f"url: {p.url}", f"purpose: {p.description}"],
        confirm_line=lambda p: f"{p.name} — {p.url}",
        upstream_count=_three_requests,
        requests_spent=_three_spent,
    )


def _run_nar(args: argparse.Namespace, probes: list[NARProbe]) -> int:
    """Dispatch the NAR public HTML probe flow."""
    return _run_provider(
        args, probes, provider="nar",
        run_one=run_nar_probe,
        dry_lines=lambda p: [f"url: {p.url}", f"purpose: {p.description}"],
        confirm_line=lambda p: f"{p.name} — {p.url}",
        upstream_count=_three_requests,
        requests_spent=_three_spent,
    )


def _run_ecb(args: argparse.Namespace, probes: list[ECBProbe]) -> int:
    """Dispatch the ECB probe flow."""
    client = ECBClient()
    return _run_provider(
        args, probes, provider="ecb",
        run_one=lambda p: run_ecb_probe(client, p),
        dry_lines=lambda p: [
            f"series: {p.series_id} ({p.indicator})",
            f"window: {p.start_period} → {p.end_period}",
            f"purpose: {p.description}",
        ],
        confirm_line=lambda p: (
            f"{p.name} — {p.series_id} {p.start_period}..{p.end_period}"
        ),
    )


def _run_fed(args: argparse.Namespace, probes: list[FedProbe]) -> int:
    """Dispatch the Fed probe flow."""
    return _run_provider(
        args, probes, provider="fed",
        run_one=run_fed_probe,
        dry_lines=lambda p: [
            f"source: {p.source}",
            f"url: {p.url}",
            f"purpose: {p.description}",
        ],
        confirm_line=lambda p: f"{p.name} — {p.url}",
    )


def _run_nbs(args: argparse.Namespace, probes: list[NBSProbe]) -> int:
    """Dispatch the NBS probe flow."""
    # Each NBS probe expends two upstream requests on the happy path —
    # the index-page discovery plus the yearly article fetch. The
    # dry-run summary advertises this ("+ 1 index-page fetch"); the
    # Budget section matches. When discovery fails the article fetch
    # never runs, so the probe's ``request_path`` (set only after
    # ``discover_nbs_calendar_url`` returns) acts as the proxy for
    # whether 1 or 2 requests were spent.
    def _spent(r: ProbeResult) -> int:
        if r.status == "skipped":
            return 0
        return 2 if r.request_path else 1

    return _run_provider(
        args, probes, provider="nbs",
        run_one=run_nbs_probe,
        dry_lines=lambda p: [f"year: {p.year}", f"purpose: {p.description}"],
        confirm_line=lambda p: f"{p.name} — yearly calendar for {p.year}",
        upstream_count=lambda probes: f"{len(probes)} (+ 1 index-page fetch)",
        requests_spent=lambda results: sum(_spent(r) for r in results),
    )


def _run_meti(args: argparse.Namespace, probes: list[Probe]) -> int:
    """Dispatch the METI probe flow."""
    return _run_provider(
        args, probes, provider="meti",
        run_one=run_meti_probe,
        dry_lines=lambda p: [
            f"path: {p.path}",
            f"purpose: {p.description}",
            f"expected shape: {p.expected_shape}",
        ],
        confirm="budget",
    )


def _run_stat_bureau(args: argparse.Namespace, probes: list[Probe]) -> int:
    """Dispatch the Statistics Bureau probe flow."""
    return _run_provider(
        args, probes, provider="stat-bureau-jp",
        run_one=run_stat_bureau_probe,
        dry_lines=lambda p: (
            (lambda q: [
                f"path: {p.path}{'?' + q if q else ''}",
                f"purpose: {p.description}",
                f"expected shape: {p.expected_shape}",
            ])(render_params(p.params))
        ),
        confirm="budget",
    )


if __name__ == "__main__":
    sys.exit(main())
