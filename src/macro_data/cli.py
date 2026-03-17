from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path

from ingestion.sdmx.providers.oecd import OECDClient
from ingestion.sources import (
    OECDIngestionClient,
    WorldBankIngestionClient,
    render_oecd_series_configs,
    render_wb_series_configs,
)

from .server import main as serve_main


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Macro-data service CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve")
    serve.add_argument("--host", default=None)
    serve.add_argument("--port", type=int, default=None)
    serve.add_argument("--db-path", default=None)
    serve.add_argument("--api-token", default=None)

    refresh = subparsers.add_parser("refresh")
    refresh.add_argument("--db-path", default=None)

    schedule = subparsers.add_parser("schedule")
    schedule.add_argument("--show", action="store_true", help="Show all concepts with next expected release")
    schedule.add_argument("--due", action="store_true", help="Show concepts due for refresh now")
    schedule.add_argument("--status", action="store_true", help="Show availability status per concept")
    schedule.add_argument("--run", action="store_true", help="Run release-aware scheduler")
    schedule.add_argument("--window", type=int, default=120, help="Due window in minutes")
    schedule.add_argument("--poll-interval", type=int, default=300, help="Poll interval seconds")
    schedule.add_argument("--json", action="store_true", dest="json_output")
    schedule.add_argument("--db-path", default=None)

    health = subparsers.add_parser("health")
    health.add_argument("--indicator", default=None, help="Filter to a single indicator")
    health.add_argument("--alerts", action="store_true", help="Show only active alerts")
    health.add_argument("--json", action="store_true", dest="json_output")
    health.add_argument("--db-path", default=None)

    news_refresh = subparsers.add_parser("news-refresh")
    news_refresh.add_argument("--category", default=None)
    news_refresh.add_argument("--db-path", default=None)

    refresh_source = subparsers.add_parser("refresh-source")
    refresh_source.add_argument("--source", required=True)
    refresh_source.add_argument("--db-path", default=None)

    oecd_dataflows = subparsers.add_parser("oecd-dataflows")
    oecd_dataflows.add_argument("--query", default=None)
    oecd_dataflows.add_argument("--limit", type=int, default=20)
    oecd_dataflows.add_argument("--agency-prefix", default="OECD")

    oecd_structure = subparsers.add_parser("oecd-structure")
    oecd_structure.add_argument("--dataflow", required=True)
    oecd_structure.add_argument("--agency-id", default=OECDClient.DEFAULT_AGENCY_ID)
    oecd_structure.add_argument("--version", default="latest")

    oecd_generate = subparsers.add_parser("oecd-generate-configs")
    oecd_generate.add_argument("--dataflow", action="append", dest="dataflows", default=None)
    oecd_generate.add_argument("--agency-id", default=None)
    oecd_generate.add_argument("--query", default=None)
    oecd_generate.add_argument("--agency-prefix", default="OECD")
    oecd_generate.add_argument("--dataflow-limit", type=int, default=3)
    oecd_generate.add_argument("--series-per-dataflow", type=int, default=3)

    oecd_refresh = subparsers.add_parser("oecd-refresh-catalog")
    oecd_refresh.add_argument("--dataflow", action="append", dest="dataflows", default=None)
    oecd_refresh.add_argument("--agency-id", default=None)
    oecd_refresh.add_argument("--query", default=None)
    oecd_refresh.add_argument("--agency-prefix", default="OECD")
    oecd_refresh.add_argument("--dataflow-limit", type=int, default=3)
    oecd_refresh.add_argument("--latest-observations", type=int, default=1)
    oecd_refresh.add_argument("--sleep-seconds", type=float, default=1.2)
    oecd_refresh.add_argument("--db-path", default=None)

    # -- World Bank catalog commands --
    subparsers.add_parser("wb-sources")

    wb_indicators = subparsers.add_parser("wb-indicators")
    wb_indicators.add_argument("--source-id", default=None)
    wb_indicators.add_argument("--topic-id", default=None)
    wb_indicators.add_argument("--query", default=None)
    wb_indicators.add_argument("--limit", type=int, default=50)

    wb_generate = subparsers.add_parser("wb-generate-configs")
    wb_generate.add_argument("--source-id", default=None)
    wb_generate.add_argument("--topic-id", default=None)
    wb_generate.add_argument("--query", default=None)
    wb_generate.add_argument("--indicator-limit", type=int, default=10)
    wb_generate.add_argument("--countries", nargs="*", default=None)

    wb_refresh = subparsers.add_parser("wb-refresh-catalog")
    wb_refresh.add_argument("--source-id", default=None)
    wb_refresh.add_argument("--topic-id", default=None)
    wb_refresh.add_argument("--query", default=None)
    wb_refresh.add_argument("--indicator-limit", type=int, default=10)
    wb_refresh.add_argument("--countries", nargs="*", default=None)
    wb_refresh.add_argument("--latest-observations", type=int, default=5)
    wb_refresh.add_argument("--sleep-seconds", type=float, default=0.3)
    wb_refresh.add_argument("--db-path", default=None)

    rag_parser = subparsers.add_parser("rag")
    rag_sub = rag_parser.add_subparsers(dest="rag_command", required=True)
    rag_sub.add_parser("calibrate")
    rag_sub.add_parser("sync")
    rag_sub.add_parser("status")

    validate = subparsers.add_parser("validate")
    validate.add_argument("--source", default=None, help="Validate a single source")
    validate.add_argument("--cross-source", action="store_true", help="Run cross-source checks")
    validate.add_argument("--report", action="store_true", help="Show last validation report")
    validate.add_argument("--history", action="store_true", help="Show check history")
    validate.add_argument("--days", type=int, default=7, help="History lookback days")
    validate.add_argument("--json", action="store_true", dest="json_output", help="JSON output")
    validate.add_argument("--db-path", default=None)

    refresh_indicator = subparsers.add_parser("refresh-indicator")
    refresh_indicator.add_argument("concept_id", help="Concept ID (e.g. CPI_US)")
    refresh_indicator.add_argument("--lookback-days", type=int, default=365 * 3, help="History depth")
    refresh_indicator.add_argument("--validate", action="store_true", help="Run validation after ingestion")
    refresh_indicator.add_argument("--json", action="store_true", dest="json_output", help="JSON output")
    refresh_indicator.add_argument("--db-path", default=None)

    validate_concept = subparsers.add_parser("validate-concept")
    validate_concept.add_argument("concept_id", nargs="?", default=None, help="Concept ID (e.g. CPI_US)")
    validate_concept.add_argument("--all", action="store_true", dest="all_concepts", help="Validate all concepts")
    validate_concept.add_argument("--country", default=None, help="Filter by country code")
    validate_concept.add_argument("--max-staleness-days", type=int, default=90, help="Freshness threshold")
    validate_concept.add_argument("--tolerance-pct", type=float, default=1.0, help="Cross-source value tolerance %%")
    validate_concept.add_argument("--lookback", type=int, default=12, help="Lookback periods for cross-source")
    validate_concept.add_argument("--json", action="store_true", dest="json_output", help="JSON output")
    validate_concept.add_argument("--db-path", default=None)

    resolve = subparsers.add_parser("resolve")
    resolve.add_argument("concept_id", help="Concept ID (e.g. CPI_US)")
    resolve.add_argument("--date", default=None, help="Specific date (YYYY-MM-DD)")
    resolve.add_argument("--history", action="store_true", help="Show resolved time series")
    resolve.add_argument("--limit", type=int, default=12, help="History limit (default 12)")
    resolve.add_argument("--json", action="store_true", dest="json_output", help="JSON output")
    resolve.add_argument("--db-path", default=None)

    return parser


def _run_rag(args: argparse.Namespace) -> int:
    from rag.bridge import MacroIngestionBridge
    from rag.config import RAGConfig
    from rag.embeddings import Embedder
    from rag.vector_store import VectorStore

    cfg = RAGConfig.from_env()
    store = VectorStore(cfg)
    store.init_collection()
    embedder = Embedder(cfg)
    bridge = MacroIngestionBridge(store, embedder, cfg)
    if args.rag_command == "calibrate":
        print(json.dumps(bridge.calibrate(), ensure_ascii=False, indent=2))
        return 0
    if args.rag_command == "sync":
        print(json.dumps(bridge.sync(), ensure_ascii=False, indent=2))
        return 0
    if args.rag_command == "status":
        print(json.dumps(bridge.status(), ensure_ascii=False, indent=2))
        return 0
    return 2


def _run_oecd_dataflows(args: argparse.Namespace) -> int:
    ingestion = OECDIngestionClient()
    dataflows = ingestion.list_catalog_dataflows(
        query=args.query,
        agency_prefix=args.agency_prefix,
        limit=args.limit,
    )
    if not dataflows:
        print("No OECD dataflows found.")
        return 0
    for dataflow in dataflows:
        print(f"{dataflow.agency_id}\t{dataflow.id}\t{dataflow.version}\t{dataflow.name}")
    return 0


def _run_oecd_structure(args: argparse.Namespace) -> int:
    ingestion = OECDIngestionClient()
    summary = ingestion.get_structure_summary(
        args.dataflow,
        agency_id=args.agency_id,
        version=args.version,
    )
    print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _run_oecd_generate_configs(args: argparse.Namespace) -> int:
    ingestion = OECDIngestionClient()
    configs = ingestion.generate_catalog_series_configs(
        dataflow_ids=args.dataflows,
        agency_id=args.agency_id,
        query=args.query,
        agency_prefix=args.agency_prefix,
        dataflow_limit=args.dataflow_limit,
        series_per_dataflow=args.series_per_dataflow,
    )
    if not configs:
        print("generated_oecd_series = {}")
        return 0
    print(render_oecd_series_configs(configs))
    return 0


def _run_oecd_refresh_catalog(args: argparse.Namespace) -> int:
    from storage import SQLiteEngineStore

    store = SQLiteEngineStore(db_path=Path(args.db_path) if args.db_path else None)
    ingestion = OECDIngestionClient()
    stats = ingestion.refresh_catalog(
        store,
        dataflow_ids=args.dataflows,
        agency_id=args.agency_id,
        query=args.query,
        agency_prefix=args.agency_prefix,
        dataflow_limit=args.dataflow_limit,
        latest_observations=args.latest_observations,
        sleep_seconds=args.sleep_seconds,
    )
    print(json.dumps({stats.source: stats.count}, ensure_ascii=False, sort_keys=True))
    return 0


# -- World Bank CLI handlers ------------------------------------------------

def _run_wb_sources(args: argparse.Namespace) -> int:
    ingestion = WorldBankIngestionClient()
    sources = ingestion.list_catalog_sources()
    if not sources:
        print("No World Bank sources found.")
        return 0
    for src in sources:
        print(f"{src.id}\t{src.name}\t{src.description[:80] if src.description else ''}")
    return 0


def _run_wb_indicators(args: argparse.Namespace) -> int:
    ingestion = WorldBankIngestionClient()
    indicators = ingestion.list_catalog_indicators(
        source_id=args.source_id,
        topic_id=args.topic_id,
        query=args.query,
        limit=args.limit,
    )
    if not indicators:
        print("No World Bank indicators found.")
        return 0
    for ind in indicators:
        print(f"{ind.id}\t{ind.name}\t{ind.source_name}")
    return 0


def _run_wb_generate_configs(args: argparse.Namespace) -> int:
    ingestion = WorldBankIngestionClient()
    configs = ingestion.generate_catalog_series_configs(
        source_id=args.source_id,
        topic_id=args.topic_id,
        query=args.query,
        indicator_limit=args.indicator_limit,
        countries=args.countries,
    )
    if not configs:
        print("generated_wb_series = {}")
        return 0
    print(render_wb_series_configs(configs))
    return 0


def _run_wb_refresh_catalog(args: argparse.Namespace) -> int:
    from storage import SQLiteEngineStore

    store = SQLiteEngineStore(db_path=Path(args.db_path) if args.db_path else None)
    ingestion = WorldBankIngestionClient()
    stats = ingestion.refresh_catalog(
        store,
        source_id=args.source_id,
        topic_id=args.topic_id,
        query=args.query,
        indicator_limit=args.indicator_limit,
        countries=args.countries,
        latest_observations=args.latest_observations,
        sleep_seconds=args.sleep_seconds,
    )
    print(json.dumps({stats.source: stats.count}, ensure_ascii=False, sort_keys=True))
    return 0


def _run_health(args: argparse.Namespace) -> int:
    from ingestion.sources import IngestionOrchestrator
    from storage import SQLiteEngineStore

    store = SQLiteEngineStore(db_path=Path(args.db_path) if args.db_path else None)
    orchestrator = IngestionOrchestrator(store)

    if args.alerts:
        alerts = orchestrator.get_alerts()
        if args.json_output:
            print(json.dumps(alerts, ensure_ascii=False, indent=2))
        else:
            if not alerts:
                print("No active alerts.")
            for a in alerts:
                print(f"  [ALERT][{a['type']}] {a['message']}")
        return 1 if alerts else 0

    rows = orchestrator.get_health(indicator=args.indicator)
    if args.json_output:
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    else:
        print(f"  {'indicator':<25} {'source':<12} {'status':<12} "
              f"{'freshness':<10} {'retries':<8} {'note'}")
        print(f"  {'-' * 85}")
        for r in rows:
            print(f"  {r['indicator']:<25} {r['source']:<12} {r['status']:<12} "
                  f"{r['freshness']:<10} {r['retries']:<8} {r['note']}")
    return 0


def _run_schedule_command(args: argparse.Namespace) -> int:
    from ingestion.sources import IngestionOrchestrator
    from storage import SQLiteEngineStore

    store = SQLiteEngineStore(db_path=Path(args.db_path) if args.db_path else None)
    orchestrator = IngestionOrchestrator(store)

    if args.show:
        status = orchestrator.get_schedule_status()
        if args.json_output:
            print(json.dumps(status, ensure_ascii=False, indent=2))
        else:
            for s in status:
                last = f"last={s['last_released']}" if s["last_released"] else "last=never"
                print(f"  {s['concept_id']:<25} {s['frequency']:<12} "
                      f"{s['rule_type']:<20} {s['next_expected']:<22} "
                      f"{s['confidence']:<12} {last}")
        return 0

    if args.due:
        from ingestion.release_schedule import is_due
        status = orchestrator.get_schedule_status()
        due = [s for s in status if is_due(s["next_expected"], window_minutes=args.window)]
        if args.json_output:
            print(json.dumps(due, ensure_ascii=False, indent=2))
        else:
            if not due:
                print("No concepts due within window.")
            for s in due:
                last = f"last={s['last_released']}" if s["last_released"] else "last=never"
                print(f"  {s['concept_id']:<25} {s['frequency']:<12} "
                      f"{s['rule_type']:<20} {s['next_expected']:<22} "
                      f"{s['confidence']:<12} {last}")
        return 0

    if args.status:
        avail = orchestrator.get_availability_status()
        if args.json_output:
            print(json.dumps(avail, ensure_ascii=False, indent=2))
        else:
            if not avail:
                print("No release status data yet. Run --run first.")
            for a in avail:
                prov = " [provisional]" if a["provisional"] else ""
                retry = f"  retry={a['next_retry']}" if a["next_retry"] else ""
                err = f"  err={a['error']}" if a["error"] else ""
                data = f"data={a['data_date']}" if a["data_date"] else "data=none"
                src = f"src={a['source_used']}" if a["source_used"] else ""
                print(f"  {a['concept_id']:<25} {a['status']:<12} "
                      f"{a['frequency']:<12} {data:<18} {src}{prov}{retry}{err}")
        return 0

    if args.run:
        orchestrator.run_release_schedule(
            poll_interval_seconds=args.poll_interval,
            due_window_minutes=args.window,
        )
        return 0

    # No flag: show help
    print("Use --show, --due, --status, or --run. See --help for details.")
    return 0


def _run_validate(args: argparse.Namespace) -> int:
    from ingestion.validation import ValidationEngine, ValidationStore

    db_path = args.db_path or ".macro-data/engine.db"
    validation_store = ValidationStore(db_path)

    if args.report:
        reports = validation_store.list_reports(source=args.source, limit=5)
        if not reports:
            print("No validation reports found.")
            return 0
        if args.json_output:
            print(json.dumps(reports, ensure_ascii=False, indent=2))
        else:
            for r in reports:
                print(f"  {r['timestamp']}  {r['source']:<20}  "
                      f"{'PASS' if r['passed'] else 'FAIL'}  "
                      f"errors={r['error_count']}  warnings={r['warning_count']}  "
                      f"checks={r['total_checks']}  {r['duration_ms']}ms")
        return 0

    if args.history:
        history = validation_store.get_history(source=args.source, days=args.days)
        if not history:
            print("No validation history found.")
            return 0
        if args.json_output:
            print(json.dumps(history, ensure_ascii=False, indent=2))
        else:
            for h in history:
                status = "PASS" if h.get("passed") else "FAIL"
                print(f"  {h.get('timestamp', '')}  {h.get('source', ''):<15}  "
                      f"{h.get('layer', ''):<14}  {h.get('check_name', ''):<35}  "
                      f"{status}  {h.get('message', '')}")
        return 0

    engine = ValidationEngine(validation_store)
    report = engine.validate_full(args.source or "all")

    if args.json_output:
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    else:
        print(report.format_text())
    return 0 if report.passed else 1


def _run_refresh_indicator(args: argparse.Namespace) -> int:
    from ingestion.sources import IngestionOrchestrator
    from storage import SQLiteEngineStore

    store = SQLiteEngineStore(db_path=Path(args.db_path) if args.db_path else None)
    orchestrator = IngestionOrchestrator(store)

    report = orchestrator.refresh_indicator(
        args.concept_id,
        lookback_days=args.lookback_days,
    )

    if args.json_output:
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    else:
        status = "OK" if not report.error else "ERROR"
        print(f"  {report.source}  {status}  stored={report.stored}  "
              f"fetched={report.fetched}  {report.duration_ms}ms")
        if report.error:
            print(f"  errors: {report.error}")

    if args.validate:
        from ingestion.validation import ValidationEngine, ValidationStore
        vs = ValidationStore(str(store.db_path))
        engine = ValidationEngine(vs)
        store.seed_concept_map()
        val_report = engine.validate_concept(args.concept_id, store)
        print()
        print(val_report.format_text())

    return 0 if not report.error else 1


def _run_resolve(args: argparse.Namespace) -> int:
    from dataclasses import asdict
    from storage import SQLiteEngineStore

    store = SQLiteEngineStore(db_path=Path(args.db_path) if args.db_path else None)
    store.seed_concept_map()

    if args.history:
        results = store.resolve_indicator_history(args.concept_id, limit=args.limit)
        if not results:
            print(f"No data found for {args.concept_id}")
            return 1
        if args.json_output:
            print(json.dumps([asdict(r) for r in results], ensure_ascii=False, indent=2))
        else:
            for r in results:
                alt = f"+{r.alternates}" if r.alternates else ""
                print(f"  {r.date}  {r.value:>12.4f}  {r.source_id:<18}  "
                      f"p={r.priority}  {r.role}{alt and '  alt=' + alt}")
        return 0

    obs = store.resolve_indicator(args.concept_id, date=args.date)
    if obs is None:
        print(f"No data found for {args.concept_id}" + (f" on {args.date}" if args.date else ""))
        return 1
    if args.json_output:
        print(json.dumps(asdict(obs), ensure_ascii=False, indent=2))
    else:
        alt = f"  alt={obs.alternates}" if obs.alternates else ""
        print(f"  {obs.concept_id}  {obs.date}  {obs.value:.4f}  "
              f"source={obs.source_id}  series={obs.provider_series_id}  "
              f"p={obs.priority}  {obs.role}{alt}")
    return 0


def _run_validate_concept(args: argparse.Namespace) -> int:
    from ingestion.validation import ValidationEngine, ValidationStore
    from storage import SQLiteEngineStore

    db_path = args.db_path or ".macro-data/engine.db"
    store = SQLiteEngineStore(db_path=Path(db_path))
    store.seed_concept_map()
    validation_store = ValidationStore(db_path)
    engine = ValidationEngine(validation_store)

    if args.all_concepts or (not args.concept_id and not args.country):
        reports = engine.validate_all_concepts(
            store,
            max_staleness_days=args.max_staleness_days,
            value_tolerance_pct=args.tolerance_pct,
            lookback_periods=args.lookback,
            country_code=args.country,
        )
        if args.json_output:
            print(json.dumps([r.to_dict() for r in reports], ensure_ascii=False, indent=2))
        else:
            for report in reports:
                print(report.format_text())
                print()
        failed = sum(1 for r in reports if not r.passed)
        return 1 if failed > 0 else 0

    if args.country and not args.concept_id:
        reports = engine.validate_all_concepts(
            store,
            max_staleness_days=args.max_staleness_days,
            value_tolerance_pct=args.tolerance_pct,
            lookback_periods=args.lookback,
            country_code=args.country,
        )
        if args.json_output:
            print(json.dumps([r.to_dict() for r in reports], ensure_ascii=False, indent=2))
        else:
            for report in reports:
                print(report.format_text())
                print()
        failed = sum(1 for r in reports if not r.passed)
        return 1 if failed > 0 else 0

    report = engine.validate_concept(
        args.concept_id,
        store,
        max_staleness_days=args.max_staleness_days,
        value_tolerance_pct=args.tolerance_pct,
        lookback_periods=args.lookback,
    )
    if args.json_output:
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    else:
        print(report.format_text())
    return 0 if report.passed else 1


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "serve":
        serve_argv: list[str] = []
        if args.host is not None:
            serve_argv.extend(["--host", args.host])
        if args.port is not None:
            serve_argv.extend(["--port", str(args.port)])
        if args.db_path is not None:
            serve_argv.extend(["--db-path", args.db_path])
        if args.api_token is not None:
            serve_argv.extend(["--api-token", args.api_token])
        return serve_main(serve_argv)

    # World Bank commands (no service needed)
    if args.command == "wb-sources":
        return _run_wb_sources(args)
    if args.command == "wb-indicators":
        return _run_wb_indicators(args)
    if args.command == "wb-generate-configs":
        return _run_wb_generate_configs(args)
    if args.command == "wb-refresh-catalog":
        return _run_wb_refresh_catalog(args)

    # Indicator refresh (no service needed)
    if args.command == "refresh-indicator":
        return _run_refresh_indicator(args)

    # Resolve indicator (no service needed)
    if args.command == "resolve":
        return _run_resolve(args)

    # Concept validation (no service needed)
    if args.command == "validate-concept":
        return _run_validate_concept(args)

    # Health dashboard (no service needed)
    if args.command == "health":
        return _run_health(args)

    # Release schedule (no service needed for --show/--due/--status/--run)
    if args.command == "schedule":
        if args.show or args.due or args.status or args.run:
            return _run_schedule_command(args)
        # Legacy: fall through to service-based run_schedule

    from macro_data.factory import build_local_macro_data_service

    service = build_local_macro_data_service(db_path=Path(args.db_path) if hasattr(args, "db_path") and args.db_path else None)

    if args.command == "refresh":
        print(json.dumps(service.invoke("refresh_all_sources", {}), ensure_ascii=False, sort_keys=True))
        return 0
    if args.command == "schedule":
        service.invoke("run_schedule", {})
        return 0
    if args.command == "news-refresh":
        print(json.dumps(service.invoke("refresh_news", {"category": args.category}), ensure_ascii=False, sort_keys=True))
        return 0
    if args.command == "refresh-source":
        print(json.dumps(service.invoke("refresh_source", {"source": args.source}), ensure_ascii=False, sort_keys=True))
        return 0
    if args.command == "oecd-dataflows":
        return _run_oecd_dataflows(args)
    if args.command == "oecd-structure":
        return _run_oecd_structure(args)
    if args.command == "oecd-generate-configs":
        return _run_oecd_generate_configs(args)
    if args.command == "oecd-refresh-catalog":
        return _run_oecd_refresh_catalog(args)
    if args.command == "rag":
        return _run_rag(args)
    if args.command == "validate":
        return _run_validate(args)
    parser.error("unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
