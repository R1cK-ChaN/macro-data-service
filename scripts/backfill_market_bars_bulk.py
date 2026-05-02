#!/usr/bin/env python3
"""Bulk-EOD-driven historical backfill for ``market_price_bars`` — issue #67 slice 3.

Walks one trading day at a time across a date window, calling
``/api/eod-bulk-last-day/{exchange}`` once per day to pull every
actively-traded symbol on that exchange in a single request. Filters
the returned rows against ``EODHD_GLOBAL_UNIVERSE`` (the seeded
universe — only symbols we already track land in
``market_price_bars``) and writes via ``EODHDMarketDataProvider`` so
the same idempotent path as :meth:`refresh_market_history` runs.

Why bulk: a 5y backfill of ~5000 US tickers using per-ticker EOD costs
~5000 calls; using bulk costs ~252 trading days/year × 5 years ≈ 1260
calls. ~75% quota saving at the same data quality.

Universe filter at WRITE (not read): the bulk endpoint returns the
entire exchange (~50k US rows per day at probe time), but
``market_price_bars`` is a curated table — bloating it with 50k × 1260
≈ 63M unwanted rows would inflate index sizes and slow every read.
Filter-at-write keeps the table aligned with the universe registry.

Operator usage::

    PYTHONPATH=src python3 scripts/backfill_market_bars_bulk.py \\
        --exchange US --start 2020-01-01 --end 2026-04-25

    # Single-day spot check:
    PYTHONPATH=src python3 scripts/backfill_market_bars_bulk.py \\
        --exchange US --date 2024-01-15

Exit codes:

- 0 on success (per-day throttles are logged but don't fail the run).
- 1 on unhandled exception.
- 2 on argument errors.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from ingestion.market._eodhd_universe import (  # noqa: E402
    EODHD_GLOBAL_UNIVERSE,
    EODHDUniverseEntry,
)
from ingestion.market.clients._eodhd import (  # noqa: E402
    EODHDMarketDataProvider,
    _entry_carries_corp_actions,
)  # noqa: E402
from ingestion.market.clients._tiingo import (  # noqa: E402
    PRE2018_CUTOFF,
    check_ohlc_sanity,
)
from ingestion.market.scrapers._eodhd import (  # noqa: E402
    EODHDAPIError,
    EODHDAuthError,
    EODHDClient,
    EODHDDailyBar,
)
from storage import (  # noqa: E402
    MarketPriceBarRecord,
    SQLiteEngineStore,
    default_engine_db_path,
)

LOG_FILENAME = "backfill_market_bars_bulk.log"

logger = logging.getLogger("backfill_market_bars_bulk")


def _append_log(log_path: Path, payload: dict) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


# Exchanges that trade 24/7 — Sat/Sun bars are real and the bulk
# endpoint returns them. Skip the weekday filter for these so
# crypto backfills don't drop 2/7 of every series.
_CONTINUOUS_EXCHANGES = frozenset({"CC"})


def _trading_days(start: dt.date, end: dt.date, *, exchange: str) -> list[str]:
    """Inclusive date sequence between ``start`` and ``end``.

    For weekday-only exchanges, returns Mon-Fri dates only — bulk EOD
    silently returns 0 rows on weekends/holidays. For continuous
    exchanges (``.CC`` crypto), returns every calendar day so the
    Saturday/Sunday bars EODHD produces are not dropped.
    """
    continuous = exchange in _CONTINUOUS_EXCHANGES
    days: list[str] = []
    d = start
    while d <= end:
        if continuous or d.weekday() < 5:
            days.append(d.strftime("%Y-%m-%d"))
        d += dt.timedelta(days=1)
    return days


def _universe_index_by_exchange(exchange: str) -> dict[str, EODHDUniverseEntry]:
    """Map ``CODE.EXCHANGE`` → entry for tickers on the requested exchange.

    The bulk endpoint returns ``code + exchange_short_name`` per row;
    the scraper normalizes them into ``CODE.EX``. Restricting to one
    exchange per invocation keeps the filter cheap and matches the
    endpoint's per-exchange shape.
    """
    return {
        e.eodhd_ticker: e
        for e in EODHD_GLOBAL_UNIVERSE
        if e.exchange_code == exchange
    }


def _persist_bar(
    store: SQLiteEngineStore,
    *,
    bar: EODHDDailyBar,
    entry: EODHDUniverseEntry,
) -> None:
    """Idempotent write through the existing ``upsert_market_price_bar``
    contract. Same flag policy as :meth:`refresh_market_history` —
    corp-action flags gated to corp-action-bearing asset classes so
    non-equity bars don't surface bogus warnings.

    ``has_pre2018_delisted`` and ``has_break_detected`` are NOT set
    here. Both signals require a per-ticker time series to compute
    (``check_adjustment_applied`` and ``detect_history_breaks``) — the
    bulk endpoint hands us one bar per ticker per day. Operators that
    care about those flags should run ``refresh_market_history`` per
    ticker after the backfill — that path uses the full series and
    sets the flags correctly. Setting them from a single-bar window
    here would surface false warnings on adjusted history (the case
    Codex flagged on AAPL pre-2018 bars).
    """
    carries_corp_actions = _entry_carries_corp_actions(entry)
    flags_json: dict[str, Any] = {}
    if not check_ohlc_sanity(bar):
        flags_json["ohlc_sanity"] = "failed"
    if carries_corp_actions:
        flags_json["corp_acts_missing"] = "eodhd_eod_endpoint_has_no_div_split"

    segment_id = f"{entry.instrument_id}:eodhd:{entry.eodhd_ticker}"
    store.upsert_market_price_bar(
        MarketPriceBarRecord(
            instrument_id=entry.instrument_id,
            source_segment_id=segment_id,
            date=bar.date,
            bar_interval="1d",
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            adjusted_open=bar.adj_open,
            adjusted_high=bar.adj_high,
            adjusted_low=bar.adj_low,
            adjusted_close=bar.adj_close,
            adjusted_volume=bar.adj_volume,
            dividend_cash=bar.div_cash,
            split_factor=bar.split_factor,
            source_name="EODHD",
            source_symbol=entry.eodhd_ticker,
            has_break_detected=False,
            has_pre2018_delisted=False,
            has_missing_corp_acts=carries_corp_actions,
            has_mapping_review_needed=False,
            quality_flags_json=flags_json,
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--exchange", required=True,
        help="EODHD exchange code, e.g. 'US', 'XETRA', 'LSE'. "
             "Filter is per-exchange so one invocation covers one exchange.",
    )
    parser.add_argument(
        "--start", help="ISO date floor (YYYY-MM-DD). Mutually exclusive with --date.",
    )
    parser.add_argument(
        "--end", help="ISO date ceiling (YYYY-MM-DD). Mutually exclusive with --date.",
    )
    parser.add_argument(
        "--date",
        help="Single-day spot check. Mutually exclusive with --start/--end.",
    )
    parser.add_argument(
        "--db-path", default=str(default_engine_db_path()),
        help="Engine DB path. Defaults to %(default)s.",
    )
    parser.add_argument(
        "--max-requests", type=int, default=2000,
        help="Hard cap on EODHD calls per invocation (default 2000).",
    )
    parser.add_argument(
        "--request-sleep", type=float, default=0.5,
        help="Sleep between requests in seconds (default 0.5).",
    )
    parser.add_argument(
        "--log-dir", default=str(REPO_ROOT / ".macro-data" / "logs"),
        help="Directory for the JSONL run log.",
    )
    parser.add_argument(
        "--quiet", action="store_true", help="Suppress per-day INFO logs.",
    )
    args = parser.parse_args(argv)

    if args.date and (args.start or args.end):
        parser.error("--date is mutually exclusive with --start/--end")
    if not args.date and not (args.start and args.end):
        parser.error("provide either --date or both --start and --end")

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    universe = _universe_index_by_exchange(args.exchange)
    if not universe:
        logger.warning(
            "No universe entries match exchange=%s — nothing to write. "
            "Add entries to ingestion/market/_eodhd_universe.py first.",
            args.exchange,
        )
        return 0

    store = SQLiteEngineStore(db_path=Path(args.db_path))
    client = EODHDClient()

    # Fail fast on missing credentials — otherwise every bulk request
    # returns [] and the run logs "ok days_skipped=N" with zero rows
    # written, looking like a clean no-data day to the operator.
    if not client.api_key:
        logger.error(
            "EODHD_API_KEY is not set; aborting before any request. "
            "Add EODHD_API_KEY=<token> to .env or export it."
        )
        return 2

    # Seed identity rows for every entry on this exchange. Without
    # this, market_price_bars writes would land but
    # market_instruments / market_symbol_history stay empty, and the
    # public read path (resolves through the identity tables before
    # listing bars) would return empty results. Mirrors the seed step
    # ``refresh_market_history`` runs implicitly on first call.
    for entry in universe.values():
        EODHDMarketDataProvider._seed_single_entry(store, entry)

    if args.date:
        days = [args.date]
    else:
        start = dt.date.fromisoformat(args.start)
        end = dt.date.fromisoformat(args.end)
        days = _trading_days(start, end, exchange=args.exchange)

    log_path = Path(args.log_dir) / LOG_FILENAME
    requests_made = 0
    bars_written = 0
    days_with_data = 0
    days_skipped = 0

    for day in days:
        if requests_made >= args.max_requests:
            logger.warning(
                "Hit --max-requests=%d cap; stopping at %s",
                args.max_requests, day,
            )
            break
        try:
            bars = client.get_bulk_last_day(args.exchange, date=day)
        except EODHDAuthError as exc:
            # Auth failures during the loop indicate the key was
            # revoked or rate-limited at the auth tier — never recover
            # by sleeping. Abort so operator sees the failure instead
            # of a silent "every day was empty" run.
            logger.error("EODHD auth failure on %s — aborting: %s", day, exc)
            _append_log(log_path, {
                "ts": dt.datetime.now(dt.UTC).isoformat(),
                "exchange": args.exchange,
                "date": day,
                "status": "auth_error",
                "error": str(exc),
            })
            return 2
        except EODHDAPIError as exc:
            logger.warning("Bulk EOD failed for %s on %s: %s", args.exchange, day, exc)
            requests_made += 1
            _append_log(log_path, {
                "ts": dt.datetime.now(dt.UTC).isoformat(),
                "exchange": args.exchange,
                "date": day,
                "status": "error",
                "error": str(exc),
            })
            time.sleep(args.request_sleep)
            continue

        requests_made += 1
        if not bars:
            days_skipped += 1
            _append_log(log_path, {
                "ts": dt.datetime.now(dt.UTC).isoformat(),
                "exchange": args.exchange,
                "date": day,
                "status": "empty",
                "rows_received": 0,
            })
            time.sleep(args.request_sleep)
            continue

        days_with_data += 1
        wrote = 0
        touched_entries: dict[str, EODHDUniverseEntry] = {}
        for bar in bars:
            entry = universe.get(bar.ticker)
            if entry is None:
                continue
            _persist_bar(store, bar=bar, entry=entry)
            touched_entries[entry.instrument_id] = entry
            wrote += 1
        bars_written += wrote

        # Re-project corp actions for each ticker we just wrote a bar
        # for — the bulk-write path uses the same INSERT OR REPLACE
        # contract as refresh_market_history, so it would otherwise
        # wipe any dividend_cash / split_factor values an earlier
        # refresh_corp_actions had projected. Cheap when raw is empty
        # (one indexed SELECT per ticker).
        for entry in touched_entries.values():
            EODHDMarketDataProvider._reproject_corp_actions(store, entry)

        if not args.quiet:
            logger.info(
                "%s: %d rows received, %d in universe, %d written "
                "(cumulative: %d bars across %d days; %d/%d requests)",
                day, len(bars), wrote, wrote,
                bars_written, days_with_data,
                requests_made, args.max_requests,
            )
        _append_log(log_path, {
            "ts": dt.datetime.now(dt.UTC).isoformat(),
            "exchange": args.exchange,
            "date": day,
            "status": "ok",
            "rows_received": len(bars),
            "rows_written": wrote,
        })
        time.sleep(args.request_sleep)

    logger.info(
        "DONE — exchange=%s requests=%d days_with_data=%d days_skipped=%d bars_written=%d",
        args.exchange, requests_made, days_with_data, days_skipped, bars_written,
    )
    _append_log(log_path, {
        "ts": dt.datetime.now(dt.UTC).isoformat(),
        "exchange": args.exchange,
        "status": "summary",
        "requests_made": requests_made,
        "days_with_data": days_with_data,
        "days_skipped": days_skipped,
        "bars_written": bars_written,
    })
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        logger.exception("backfill_market_bars_bulk crashed")
        sys.exit(1)
