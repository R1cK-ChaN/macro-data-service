#!/usr/bin/env python3
"""Per-ticker EODHD lifetime backfill into ClickHouse market tables."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from ingestion.market.clients._eodhd import (  # noqa: E402
    _bar_to_ch,
    _dividend_to_ch,
    _eodhd_ticker_for_instrument,
    _split_to_ch,
)
from ingestion.market.scrapers._eodhd import EODHDClient  # noqa: E402
from storage.clickhouse.store import ClickHouseMarketStore, clickhouse_client_from_env  # noqa: E402

logger = logging.getLogger("backfill_market_history")
ENDPOINTS = ("eod", "div", "splits")


class RateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        self.delay = 1.0 / max(requests_per_second, 0.01)
        self.lock = threading.Lock()
        self.next_at = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            if now < self.next_at:
                time.sleep(self.next_at - now)
            self.next_at = time.monotonic() + self.delay


class BackfillCursor:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()
        if path.exists():
            payload = json.loads(path.read_text())
        else:
            payload = {}
        self.done: dict[str, dict[str, str]] = {
            str(k): dict(v) for k, v in payload.get("done", {}).items()
        }

    def is_done(self, ticker: str, endpoint: str) -> bool:
        with self.lock:
            return endpoint in self.done.get(ticker, {})

    def mark_done(self, ticker: str, endpoint: str) -> None:
        with self.lock:
            self.done.setdefault(ticker, {})[endpoint] = dt.datetime.now(dt.UTC).isoformat()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.path.with_suffix(".tmp")
            tmp_path.write_text(json.dumps({"done": self.done}, sort_keys=True, indent=2))
            tmp_path.replace(self.path)


_thread_local = threading.local()


def _thread_store() -> ClickHouseMarketStore:
    store = getattr(_thread_local, "store", None)
    if store is None:
        store = ClickHouseMarketStore(clickhouse_client_from_env())
        _thread_local.store = store
    return store


def _thread_client() -> EODHDClient:
    client = getattr(_thread_local, "client", None)
    if client is None:
        client = EODHDClient()
        _thread_local.client = client
    return client


def _load_instruments(status: str, symbols: list[str] | None) -> list[dict[str, Any]]:
    store = ClickHouseMarketStore(clickhouse_client_from_env())
    store.init_schema()
    if symbols:
        out: list[dict[str, Any]] = []
        for symbol in symbols:
            row = store.lookup_instrument(instrument_id=symbol)
            if row is None:
                row = store.lookup_instrument(ticker=symbol.split(".", 1)[0])
            if row is not None:
                out.append(row)
        return out
    active_only = {"active": True, "delisted": False, "all": None}[status]
    return store.list_instruments(active_only=active_only)


def _process_instrument(
    instrument: dict[str, Any],
    *,
    cursor: BackfillCursor,
    limiter: RateLimiter,
) -> dict[str, Any]:
    store = _thread_store()
    client = _thread_client()
    ticker = _eodhd_ticker_for_instrument(instrument)
    fetched_at = dt.datetime.now(dt.UTC)
    result = {"ticker": ticker, "bars": 0, "dividends": 0, "splits": 0, "errors": []}

    if not cursor.is_done(ticker, "eod"):
        try:
            limiter.wait()
            bars = client.get_daily_bars(ticker)
            rows = [
                _bar_to_ch(instrument=instrument, bar=bar, fetched_at=fetched_at)
                for bar in bars
            ]
            result["bars"] = store.upsert_market_bars(rows)
            cursor.mark_done(ticker, "eod")
        except Exception as exc:
            logger.warning("bars backfill failed for %s: %s", ticker, exc)
            result["errors"].append(f"eod:{exc}")

    if not cursor.is_done(ticker, "div"):
        try:
            limiter.wait()
            dividends = client.get_historical_dividends(ticker)
            rows = [
                _dividend_to_ch(
                    instrument=instrument,
                    dividend=dividend,
                    fetched_at=fetched_at,
                )
                for dividend in dividends
            ]
            result["dividends"], _ = store.upsert_corp_actions(dividends=rows)
            cursor.mark_done(ticker, "div")
        except Exception as exc:
            logger.warning("dividend backfill failed for %s: %s", ticker, exc)
            result["errors"].append(f"div:{exc}")

    if not cursor.is_done(ticker, "splits"):
        try:
            limiter.wait()
            splits = client.get_historical_splits(ticker)
            rows = [
                _split_to_ch(instrument=instrument, split=split, fetched_at=fetched_at)
                for split in splits
            ]
            _, result["splits"] = store.upsert_corp_actions(splits=rows)
            cursor.mark_done(ticker, "splits")
        except Exception as exc:
            logger.warning("split backfill failed for %s: %s", ticker, exc)
            result["errors"].append(f"splits:{exc}")

    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status", choices=("active", "delisted", "all"), default="active")
    parser.add_argument("--symbols", nargs="+", default=None)
    parser.add_argument("--max-tickers", type=int, default=None)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--requests-per-second", type=float, default=2.0)
    parser.add_argument(
        "--cursor",
        type=Path,
        default=REPO_ROOT / ".macro-data" / "market_backfill_cursor.json",
    )
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if not EODHDClient().api_key:
        print(json.dumps({"status": "error", "error": "EODHD_API_KEY missing"}))
        return 2

    instruments = _load_instruments(args.status, args.symbols)
    if args.max_tickers is not None:
        instruments = instruments[: args.max_tickers]
    cursor = BackfillCursor(args.cursor)
    limiter = RateLimiter(args.requests_per_second)

    summary = {
        "status": "ok",
        "tickers": len(instruments),
        "bars": 0,
        "dividends": 0,
        "splits": 0,
        "errors": [],
    }
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(_process_instrument, instrument, cursor=cursor, limiter=limiter)
            for instrument in instruments
        ]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            summary["bars"] += int(result["bars"])
            summary["dividends"] += int(result["dividends"])
            summary["splits"] += int(result["splits"])
            summary["errors"].extend(result["errors"])
            if not args.quiet:
                logger.info("%s", json.dumps(result, sort_keys=True))

    if summary["errors"]:
        summary["status"] = "partial"
    print(json.dumps(summary, sort_keys=True))
    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
