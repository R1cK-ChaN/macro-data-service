#!/usr/bin/env python3
"""Seed ClickHouse market.instruments from EODHD US symbol lists."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from ingestion.market.clients._eodhd import EODHDMarketDataProvider  # noqa: E402
from storage.clickhouse.store import ClickHouseMarketStore, clickhouse_client_from_env  # noqa: E402

logger = logging.getLogger("seed_market_universe")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exchange", default="US")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

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
    stats = provider.seed_us_universe(store, exchange=args.exchange)
    print(
        json.dumps(
            {
                "status": "ok",
                "exchange": args.exchange,
                "instruments": stats.instruments,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
