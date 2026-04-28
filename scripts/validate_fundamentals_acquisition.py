#!/usr/bin/env python3
"""Live validation of the EODHD fundamentals acquisition layer (issue #68 S3).

Sibling script to ``validate_calendar_acquisition.py`` — kept separate
because the calendar validator's ``Probe`` row-diff machinery doesn't
fit the nested fundamentals payload shape (one big dict per ticker,
not flat row arrays).

Scope: only the fetch + parse stage. Hits 4 representative tickers
covering the asset-class spread the acceptance criteria call out:

* US large-cap   (``AAPL.US``) — full Financials block, dense Highlights.
* EU listing     (``ASML.AS``) — confirms non-US exchange suffix path.
* HK listing     (``0700.HK``) — Asia-Pacific listing, JPY/HKD currency.
* US ETF         (``SPY.US``)  — empty Financials block, Holdings shape;
                                 confirms parser graceful-no-op behaviour.

Default is dry-run. Pass ``--execute`` to actually call EODHD; the
script prompts once before spending requests unless ``--yes`` is set.

Output: a markdown report under ``docs/validation/`` with per-ticker
section presence flags, sample numeric ratios, and a parse-attempt
status from ``parse_payload_records``.

Usage::

    PYTHONPATH=src python3 scripts/validate_fundamentals_acquisition.py
    PYTHONPATH=src python3 scripts/validate_fundamentals_acquisition.py \\
        --execute --yes
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

from ingestion.market.fundamentals.eodhd_fundamentals import (  # noqa: E402
    EODHDFundamentalsAuthMissing,
    EODHDFundamentalsClient,
    EODHDFundamentalsNotFound,
    parse_payload_records,
)

logger = logging.getLogger("validate_fundamentals_acquisition")

DEFAULT_TICKERS: tuple[tuple[str, str], ...] = (
    ("AAPL.US",  "US large-cap equity"),
    ("ASML.AS",  "EU listing"),
    ("0700.HK",  "HK listing"),
    ("SPY.US",   "US ETF (Holdings rather than Financials)"),
)

REPORT_FIELDS: tuple[str, ...] = (
    "General", "Highlights", "Valuation", "SharesStats",
    "Financials", "Earnings", "AnalystRatings", "ESGScores",
)


def _confirm_execute(prompt: str) -> bool:
    try:
        answer = input(f"{prompt} [y/N] ").strip().lower()
    except EOFError:
        return False
    return answer in {"y", "yes"}


def _summarise_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sections_present = {
        name: name in payload and bool(payload.get(name))
        for name in REPORT_FIELDS
    }
    general = payload.get("General") or {}
    highlights = payload.get("Highlights") or {}
    return {
        "name":           general.get("Name", ""),
        "type":           general.get("Type", ""),
        "sector":         general.get("Sector", ""),
        "country_iso":    general.get("CountryISO", ""),
        "currency":       general.get("CurrencyCode", ""),
        "market_cap":     highlights.get("MarketCapitalization"),
        "pe_ratio":       highlights.get("PERatio"),
        "eps_ttm":        highlights.get("EarningsShare"),
        "sections":       sections_present,
    }


def _probe_ticker(
    client: EODHDFundamentalsClient,
    ticker: str,
    *,
    description: str,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "ticker":      ticker,
        "description": description,
        "status":      "skipped",
    }
    try:
        result = client.get_fundamentals(ticker)
    except EODHDFundamentalsNotFound as exc:
        record["status"] = "not_found"
        record["error"] = str(exc)
        return record
    except EODHDFundamentalsAuthMissing as exc:
        record["status"] = "auth_missing"
        record["error"] = str(exc)
        return record
    except Exception as exc:  # pragma: no cover — surfaced in report
        record["status"] = "http_error"
        record["error"] = f"{type(exc).__name__}: {exc}"
        return record

    record["status"] = "ok"
    record["elapsed_ms"] = round(result.elapsed_ms, 1)
    record["payload_bytes"] = len(result.payload_text)
    record["summary"] = _summarise_payload(result.payload)

    snapshot_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        company, highlights, financials = parse_payload_records(
            result.payload, ticker=ticker, snapshot_epoch_ms=snapshot_ms,
        )
        record["parse"] = {
            "company":          company is not None,
            "highlights":       highlights is not None,
            "financials_count": len(financials),
        }
    except Exception as exc:
        # A parser regression must downgrade the probe so the summary
        # reflects validation failure (otherwise an HTTP 200 + crashing
        # parser still counts as ``ok``).
        record["status"] = "parse_error"
        record["parse"] = {"error": f"{type(exc).__name__}: {exc}"}
    return record


def _format_report(records: list[dict[str, Any]], *, dry_run: bool) -> str:
    now = datetime.now(timezone.utc).isoformat()
    lines = [
        "# EODHD Fundamentals — Acquisition Validation",
        "",
        f"Generated: {now}",
        f"Mode: {'dry-run plan' if dry_run else 'live execute'}",
        "",
        "## Per-ticker probes",
        "",
    ]
    for rec in records:
        lines.append(f"### {rec['ticker']} — {rec['description']}")
        lines.append("")
        if dry_run:
            lines.append("*No HTTP performed (dry-run).*")
            lines.append("")
            continue
        if rec["status"] != "ok":
            lines.append(f"- status: **{rec['status']}**")
            if rec.get("error"):
                lines.append(f"- error: `{rec['error']}`")
            lines.append("")
            continue
        summary = rec["summary"]
        lines.append(f"- name: `{summary['name']}` ({summary['type']})")
        lines.append(f"- sector: `{summary['sector']}`")
        lines.append(f"- country/currency: `{summary['country_iso']} / {summary['currency']}`")
        lines.append(f"- market_cap / PE / EPS: `{summary['market_cap']} / {summary['pe_ratio']} / {summary['eps_ttm']}`")
        lines.append(f"- payload bytes: `{rec['payload_bytes']}`")
        lines.append(f"- elapsed_ms: `{rec['elapsed_ms']}`")
        lines.append("- sections present: " + ", ".join(
            f"{name}={'yes' if rec['summary']['sections'][name] else 'no'}"
            for name in REPORT_FIELDS
        ))
        parse = rec.get("parse", {})
        if "error" in parse:
            lines.append(f"- parse error: `{parse['error']}`")
        else:
            lines.append(
                f"- parse: company={parse['company']}, "
                f"highlights={parse['highlights']}, "
                f"financials_rows={parse['financials_count']}"
            )
        lines.append("")
    lines.append("## Summary")
    if dry_run:
        lines.append("- ran in dry-run; no requests sent.")
    else:
        ok = sum(1 for r in records if r["status"] == "ok")
        lines.append(f"- {ok}/{len(records)} probes succeeded.")
        for rec in records:
            if rec["status"] != "ok":
                lines.append(f"  - {rec['ticker']}: {rec['status']}")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tickers", nargs="+", default=None,
        help="Override the default 4-ticker probe set.",
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Actually hit EODHD; default is plan-only dry-run.",
    )
    parser.add_argument(
        "--yes", action="store_true",
        help="Skip the confirmation prompt before live execution.",
    )
    parser.add_argument(
        "--report-path", type=Path, default=None,
        help="Markdown report output path "
             "(default: docs/validation/eodhd_fundamentals.md).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if args.tickers:
        probes: list[tuple[str, str]] = [(t, "user-specified") for t in args.tickers]
    else:
        probes = list(DEFAULT_TICKERS)

    if args.execute and not args.yes:
        if not _confirm_execute(
            f"About to spend {len(probes)} EODHD fundamentals requests. Proceed?"
        ):
            print("aborted", file=sys.stderr)
            return 1

    records: list[dict[str, Any]] = []
    if args.execute:
        with EODHDFundamentalsClient() as client:
            for ticker, description in probes:
                logger.info("probing %s (%s)", ticker, description)
                records.append(
                    _probe_ticker(client, ticker, description=description),
                )
    else:
        records = [
            {"ticker": t, "description": d, "status": "dry_run"}
            for t, d in probes
        ]

    report = _format_report(records, dry_run=not args.execute)
    output = args.report_path or (
        REPO_ROOT / "docs" / "validation" / "eodhd_fundamentals.md"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report)
    print(json.dumps({
        "dry_run":      not args.execute,
        "tickers":      [t for t, _ in probes],
        "report_path":  str(output),
        "ok":           sum(1 for r in records if r.get("status") == "ok"),
        "errors":       sum(1 for r in records if r.get("status") not in {"ok", "dry_run"}),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
