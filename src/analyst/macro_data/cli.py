from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path

from analyst.ingestion.scrapers.oecd import OECDClient
from analyst.ingestion.sources import OECDIngestionClient, render_oecd_series_configs

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

    subparsers.add_parser("schedule")

    news_refresh = subparsers.add_parser("news-refresh")
    news_refresh.add_argument("--category", default=None)
    news_refresh.add_argument("--db-path", default=None)

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

    rag_parser = subparsers.add_parser("rag")
    rag_sub = rag_parser.add_subparsers(dest="rag_command", required=True)
    rag_sub.add_parser("calibrate")
    rag_sub.add_parser("sync")
    rag_sub.add_parser("status")

    return parser


def _run_rag(args: argparse.Namespace) -> int:
    from analyst.rag.bridge import MacroIngestionBridge
    from analyst.rag.config import RAGConfig
    from analyst.rag.embeddings import Embedder
    from analyst.rag.vector_store import VectorStore

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
    from analyst.storage import SQLiteEngineStore

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

    from analyst.macro_data.factory import build_local_macro_data_service

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
    parser.error("unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
