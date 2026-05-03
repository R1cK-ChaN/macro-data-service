#!/usr/bin/env python3
"""Spot-check ClickHouse market closes against EODHD realtime quotes."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import random
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from ingestion.market.clients._eodhd import (  # noqa: E402
    EODHDMarketDataProvider,
    _eodhd_ticker_for_instrument,
)
from storage.clickhouse.store import ClickHouseMarketStore, clickhouse_client_from_env  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-n", "--sample-size", type=int, default=10)
    parser.add_argument("--threshold-pct", type=float, default=0.5)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args(argv)

    store = ClickHouseMarketStore(clickhouse_client_from_env())
    store.init_schema()
    active_by_id = {
        str(row["instrument_id"]): row
        for row in store.list_instruments(active_only=True)
    }
    snapshot = [
        row
        for row in store.latest_market_snapshot()
        if str(row["instrument_id"]) in active_by_id
    ]
    if args.seed is not None:
        random.seed(args.seed)
    sample = random.sample(snapshot, k=min(args.sample_size, len(snapshot)))
    provider = EODHDMarketDataProvider()
    if not provider.client.api_key:
        print(json.dumps({"status": "error", "error": "EODHD_API_KEY missing"}))
        return 2

    checks: list[dict] = []
    failures = 0
    for row in sample:
        instrument = active_by_id[str(row["instrument_id"])]
        ticker = str(row["ticker"])
        quote = provider.client.get_realtime_quote(_eodhd_ticker_for_instrument(instrument))
        if quote is None:
            checks.append({"ticker": ticker, "status": "missing_realtime"})
            failures += 1
            continue
        close = float(row.get("adjusted_close") or row["close"])
        diff_pct = abs((quote.close - close) / close) * 100 if close else 0.0
        ok = diff_pct <= args.threshold_pct
        if not ok:
            failures += 1
        checks.append(
            {
                "ticker": ticker,
                "stored_close": close,
                "realtime_close": quote.close,
                "diff_pct": diff_pct,
                "status": "ok" if ok else "diff_exceeded",
            }
        )

    summary = {
        "status": "ok" if failures == 0 else "error",
        "checked_at": dt.datetime.now(dt.UTC).isoformat(),
        "sample_size": len(sample),
        "failures": failures,
        "threshold_pct": args.threshold_pct,
        "checks": checks,
    }
    print(json.dumps(summary, sort_keys=True))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
