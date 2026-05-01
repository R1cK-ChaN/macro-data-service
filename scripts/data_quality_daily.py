#!/usr/bin/env python3
"""Daily macro data-quality auto-filer entry-point (issue #102 P3).

Mirrors :mod:`scripts.parity_daily` for the data-quality lane:

1. Run concept-level validation across the engine DB.
2. Compute the shadow digest snapshot.
3. Roll those signals up into a :class:`DataQualityReport`.
4. File / comment / close the single ``data-quality`` GitHub issue via
   :func:`ingestion.quality.data_quality_filer.file_data_quality_report`.
5. Append a structured log line to ``.macro-data/logs/data_quality.log``.

Exit codes:

- 0 on success (clean run, finding run, or dry-run).
- 1 on validation / digest failure that prevents the run from
  reaching the filer.
- 2 on argument errors.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ingestion._shared.redaction import redact_secrets  # noqa: E402
from ingestion.calendar.parity_filer import GhRunner  # noqa: E402
from ingestion.quality.data_quality_filer import (  # noqa: E402
    DataQualityFinding,
    DataQualityReport,
    coverage_drop_from_digest,
    default_state_path,
    file_data_quality_report,
    findings_from_concept_reports,
    secret_leak_from_text,
)
from ingestion.validation import ValidationEngine, ValidationStore  # noqa: E402
from storage import SQLiteEngineStore, default_engine_db_path  # noqa: E402

LOG_FILENAME = "data_quality.log"

logger = logging.getLogger("data_quality_daily")


def _today_utc(now: dt.datetime | None = None) -> dt.date:
    base = now or dt.datetime.now(dt.timezone.utc)
    return base.date()


def _append_log(log_path: Path, payload: dict) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


def _scan_log_for_secrets(log_path: Path) -> DataQualityFinding | None:
    """Scan the tail of a persisted log for unredacted secrets."""
    if not log_path.is_file():
        return None
    try:
        # Only look at the last ~256 KB — older log lines pre-date the
        # redaction guard and would create permanent noise.
        with log_path.open("rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            offset = max(0, size - 256 * 1024)
            fh.seek(offset)
            sample = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    return secret_leak_from_text(sample, source_label=log_path.name)


def _build_report(
    *,
    target_date: dt.date,
    engine_db: Path,
) -> tuple[DataQualityReport, dict]:
    """Run validation + digest, assemble findings, return report+summary."""
    summary: dict = {"target_date": target_date.isoformat()}

    db_path = str(engine_db)
    store = SQLiteEngineStore(db_path=engine_db)
    store.seed_concept_map()
    validation_store = ValidationStore(db_path)
    engine = ValidationEngine(validation_store)
    reports = engine.validate_all_concepts(store)
    summary["concept_reports"] = len(reports)
    summary["concept_failures"] = sum(1 for r in reports if not r.passed)

    # Lazy import to avoid heavy paths during unit tests of the filer.
    from shadow_runner import compute_digest

    digest = compute_digest(db_path)
    digest = {"timestamp": dt.datetime.now(dt.timezone.utc).isoformat(), **digest}

    findings: list[DataQualityFinding] = []
    if not reports:
        # An empty concept_map means every concept-level guard was a
        # no-op. Without a finding here the run would auto-close the
        # data-quality issue based on a vacuous "clean" comparator.
        findings.append(
            DataQualityFinding(
                kind="zero_validation_reports",
                severity="error",
                detail=(
                    "validate_all_concepts returned 0 reports; "
                    "concept_map is empty or unseeded"
                ),
            ),
        )
    findings.extend(findings_from_concept_reports(reports))
    drop = coverage_drop_from_digest(digest)
    if drop is not None:
        findings.append(drop)

    log_dir = engine_db.parent / "logs"
    for log_name in ("shadow.log", "daily_digest.jsonl"):
        leak = _scan_log_for_secrets(log_dir / log_name)
        if leak is not None:
            findings.append(leak)

    report = DataQualityReport(
        target_date=target_date,
        findings=findings,
        digest_summary={
            k: digest.get(k)
            for k in (
                "timestamp", "cycle", "concepts_covered", "concepts_total",
                "coverage_pct", "confirmed_24h", "error_sources",
            )
            if digest.get(k) is not None
        },
    )
    summary["findings"] = [f.to_dict() for f in findings]
    summary["clean"] = report.clean
    return report, summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        type=lambda s: dt.date.fromisoformat(s),
        default=None,
        help="Run date label (default: today UTC).",
    )
    parser.add_argument(
        "--db-path", type=Path, default=None,
        help="engine.db override (default: .macro-data/engine.db).",
    )
    parser.add_argument(
        "--state-path", type=Path, default=None,
        help="Filer state path override.",
    )
    parser.add_argument(
        "--log-path", type=Path, default=None,
        help="Log file path override.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run validation + digest but stub out gh side effects.",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Log to stderr at DEBUG.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    target = args.date or _today_utc()
    engine_db = args.db_path or default_engine_db_path()
    log_path = args.log_path or (engine_db.parent / "logs" / LOG_FILENAME)
    state_path = args.state_path or default_state_path(engine_db.parent.parent)

    started = dt.datetime.now(dt.timezone.utc)
    summary: dict = {
        "started_at": started.isoformat(),
        "target_date": target.isoformat(),
        "dry_run": args.dry_run,
    }

    runner = GhRunner(dry_run=args.dry_run)

    try:
        report, build_summary = _build_report(
            target_date=target, engine_db=engine_db,
        )
        summary.update(build_summary)

        action = file_data_quality_report(
            report=report, runner=runner, state_path=state_path,
        )
        summary["created"] = [n for n, _ in action.created]
        summary["commented"] = list(action.commented)
        summary["closed"] = list(action.closed)
        summary["skipped_clean"] = action.skipped_clean
        summary["status"] = "ok"
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)
        return 0

    except Exception as exc:
        summary["status"] = "error"
        summary["error"] = redact_secrets(repr(exc))
        summary["traceback"] = redact_secrets(traceback.format_exc())
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)
        logger.exception("data_quality_daily run failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
