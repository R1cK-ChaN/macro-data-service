#!/usr/bin/env python3
"""Self-heal stale market bars and detect delisted EODHD US tickers."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from ingestion.market.clients._eodhd import EODHDMarketDataProvider  # noqa: E402
from storage.clickhouse.store import ClickHouseMarketStore, clickhouse_client_from_env  # noqa: E402

logger = logging.getLogger("detect_missing_bars")


def _trading_day_gap(latest: str | None, as_of: dt.date) -> int:
    if not latest:
        return 9999
    start = dt.date.fromisoformat(latest[:10])
    gap = 0
    cursor = start + dt.timedelta(days=1)
    while cursor <= as_of:
        if cursor.weekday() < 5:
            gap += 1
        cursor += dt.timedelta(days=1)
    return gap


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=dt.date.today().isoformat())
    parser.add_argument("--missing-threshold", type=int, default=5)
    parser.add_argument("--delist-threshold", type=int, default=10)
    parser.add_argument("--max-refetch", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    as_of = dt.date.fromisoformat(args.as_of)
    store = ClickHouseMarketStore(clickhouse_client_from_env())
    store.init_schema()
    provider = EODHDMarketDataProvider()
    if not provider.client.api_key:
        print(json.dumps({"status": "error", "error": "EODHD_API_KEY missing"}))
        return 2
    active_rows = store.list_instruments(active_only=True)
    latest_dates = store.latest_bar_dates(active_only=True)
    active_symbols = provider.client.list_symbols_active("US")
    active_list_available = bool(active_symbols)
    eodhd_active_codes = {row.code.upper() for row in active_symbols}

    missing: list[dict] = []
    delisted: list[dict] = []
    for row in active_rows:
        instrument_id = str(row["instrument_id"])
        ticker = str(row["ticker"]).upper()
        gap = _trading_day_gap(latest_dates.get(instrument_id), as_of)
        if gap > args.missing_threshold:
            missing.append({"instrument_id": instrument_id, "ticker": ticker, "gap": gap})
        if (
            active_list_available
            and gap > args.delist_threshold
            and ticker not in eodhd_active_codes
        ):
            delisted.append({"instrument_id": instrument_id, "ticker": ticker, "gap": gap})

    refetched = 0
    flipped_inactive = 0
    if not args.dry_run:
        delisted_ids = {row["instrument_id"] for row in delisted}
        for row in delisted:
            if store.set_instrument_active(row["instrument_id"], is_active=False):
                flipped_inactive += 1
        for row in missing[: args.max_refetch]:
            if row["instrument_id"] in delisted_ids:
                continue
            stats = provider.refresh_market_history(store, row["instrument_id"])
            refetched += stats.bars

    summary = {
        "status": "ok",
        "as_of": as_of.isoformat(),
        "active_instruments": len(active_rows),
        "missing": len(missing),
        "delisted": len(delisted),
        "refetched_bars": refetched,
        "flipped_inactive": flipped_inactive,
        "dry_run": args.dry_run,
        "active_list_available": active_list_available,
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
