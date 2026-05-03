#!/usr/bin/env python3
"""Run EODHD bulk daily refreshes into ClickHouse market tables."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from ingestion.market.clients._eodhd import EODHDMarketDataProvider  # noqa: E402
from storage.clickhouse.store import (  # noqa: E402
    ClickHouseMarketStore,
    clickhouse_client_from_env,
)

LOG_FILENAME = "market_daily_refresh.log"
logger = logging.getLogger("market_daily_refresh")


def _append_log(log_path: Path, payload: dict) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


def _trading_days(start: dt.date, end: dt.date) -> list[str]:
    days: list[str] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            days.append(cursor.isoformat())
        cursor += dt.timedelta(days=1)
    return days


def _resolve_days(args: argparse.Namespace) -> list[str | None]:
    if args.date:
        return [args.date]
    if not args.start and not args.end:
        return [None]
    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    return _trading_days(start, end)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exchange", default="US")
    parser.add_argument("--date", help="Single YYYY-MM-DD date.")
    parser.add_argument("--start", help="Inclusive YYYY-MM-DD start.")
    parser.add_argument("--end", help="Inclusive YYYY-MM-DD end.")
    parser.add_argument(
        "--no-refetch-corp-actions",
        action="store_true",
        help="Write bulk corp actions and skip the DELETE plus per-ticker bars refill.",
    )
    parser.add_argument(
        "--log-dir",
        default=str(REPO_ROOT / ".macro-data" / "logs"),
    )
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

    if args.date and (args.start or args.end):
        parser.error("--date is mutually exclusive with --start/--end")
    if (args.start and not args.end) or (args.end and not args.start):
        parser.error("provide both --start and --end")

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    client = clickhouse_client_from_env()
    store = ClickHouseMarketStore(client)
    store.init_schema()
    provider = EODHDMarketDataProvider()
    if not provider.client.api_key:
        print(json.dumps({"status": "error", "error": "EODHD_API_KEY missing"}))
        return 2
    log_path = Path(args.log_dir) / LOG_FILENAME

    total = {"bars": 0, "dividends": 0, "splits": 0, "corp_actions_changed": 0}
    for day in _resolve_days(args):
        day_label = day or "latest"
        started = time.perf_counter()
        try:
            stats = provider.refresh_daily_bulk(
                store,
                date=day,
                exchange=args.exchange,
                refetch_changed_corp_actions=not args.no_refetch_corp_actions,
            )
        except Exception as exc:
            logger.exception("market daily refresh failed for %s", day)
            _append_log(
                log_path,
                {
                    "ts": dt.datetime.now(dt.UTC).isoformat(),
                    "date": day_label,
                    "exchange": args.exchange,
                    "status": "error",
                    "error": str(exc),
                },
            )
            return 1
        total["bars"] += stats.bars
        total["dividends"] += stats.dividends
        total["splits"] += stats.splits
        total["corp_actions_changed"] += stats.corp_actions_changed
        payload = {
            "ts": dt.datetime.now(dt.UTC).isoformat(),
            "date": day_label,
            "exchange": args.exchange,
            "status": "ok",
            "bars": stats.bars,
            "dividends": stats.dividends,
            "splits": stats.splits,
            "corp_actions_changed": stats.corp_actions_changed,
            "duration_ms": int((time.perf_counter() - started) * 1000),
        }
        _append_log(log_path, payload)
        logger.info("%s", json.dumps(payload, sort_keys=True))

    summary = {
        "status": "ok",
        "exchange": args.exchange,
        "days": len(_resolve_days(args)),
        **total,
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
