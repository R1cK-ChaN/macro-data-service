"""ClickHouse schema for the market-data lane (issue #118 P1).

Tables (all under the ``market`` database):

* ``market.bars_1d`` — daily OHLCV, raw + adjusted in the same row.
  ``time`` is a ``DateTime('UTC')`` so future intraday tables
  (``bars_1m`` / ``bars_5m`` / …) share the same column type and queries
  do not need cross-table casts. Partitioned by ``toYYYYMM(time)`` so a
  full backfill (~30 years × ~6500 active US tickers ≈ 50M rows) keeps
  per-partition pruning effective. ``ReplacingMergeTree(fetched_at)``
  collapses duplicate ``(instrument_id, time)`` keys to the most-recent
  fetch — the projection step writes a single row per session, late
  corrections (vendor restates a close) overwrite without manual cleanup.

* ``market.dividends`` — corporate-action dividends.
  ``ReplacingMergeTree(fetched_at)`` on
  ``(instrument_id, ex_date, content_hash)`` — putting ``content_hash``
  in the sort key means a revised payout from EODHD (different hash)
  lands as a new row preserving the revision chain, while a re-fetch
  of the same payout (same hash) collapses to one row on merge.
  ``MergeTree`` would have left every repeated ingest as a duplicate
  visible row even when the content was identical, which would inflate
  ``sum(cash_amount)`` after retries / scheduled refreshes.
  EODHD's per-ticker endpoint fills the declaration / record / payment
  dates; the bulk endpoint leaves them ``null`` for most non-major
  tickers — ``Nullable(Date)`` carries that natively.

* ``market.splits`` — corporate-action splits. Same pattern as
  ``dividends``. ``to_factor`` / ``from_factor`` stored as ``Float64``
  because non-integer ratios occur in practice (``5/4`` ascending,
  ``1/17`` reverse-split).

* ``market.instruments`` — instrument identity rows.
  ``ReplacingMergeTree(last_seen)`` so the universe scan dedupes by
  ``instrument_id`` to the most recently seen identity (ticker rename,
  delisting, FIGI assignment).

Naming convention: future intraday work adds parallel tables
(``market.bars_1m`` etc.) with the same ``DateTime('UTC')`` ``time``
type. No migration of existing tables.
"""

from __future__ import annotations

import os
from typing import Any

CLICKHOUSE_DATABASE = "market"


def clickhouse_database_from_env(default: str = CLICKHOUSE_DATABASE) -> str:
    """Read ``CLICKHOUSE_DATABASE`` from the environment.

    Centralized so the schema apply, store adapter, and tests pick the
    same name without each re-reading ``os.environ``.
    """
    return os.environ.get("CLICKHOUSE_DATABASE") or default


_TABLES: tuple[tuple[str, str], ...] = (
    (
        "bars_1d",
        """
        CREATE TABLE IF NOT EXISTS {db}.bars_1d (
            instrument_id     LowCardinality(String),
            ticker            LowCardinality(String),
            exchange          LowCardinality(String),
            time              DateTime('UTC'),
            open              Float64,
            high              Float64,
            low               Float64,
            close             Float64,
            volume            Float64,
            adjusted_open     Float64,
            adjusted_high     Float64,
            adjusted_low      Float64,
            adjusted_close    Float64,
            adjusted_volume   Float64,
            fetched_at        DateTime('UTC')
        )
        ENGINE = ReplacingMergeTree(fetched_at)
        PARTITION BY toYYYYMM(time)
        ORDER BY (instrument_id, time)
        """,
    ),
    (
        "dividends",
        """
        CREATE TABLE IF NOT EXISTS {db}.dividends (
            instrument_id      LowCardinality(String),
            ticker             LowCardinality(String),
            ex_date            Date,
            declaration_date   Nullable(Date),
            record_date        Nullable(Date),
            payment_date       Nullable(Date),
            period             LowCardinality(String),
            cash_amount        Float64,
            unadjusted_amount  Float64,
            currency           LowCardinality(String),
            fetched_at         DateTime('UTC'),
            content_hash       FixedString(64)
        )
        ENGINE = ReplacingMergeTree(fetched_at)
        PARTITION BY toYear(ex_date)
        ORDER BY (instrument_id, ex_date, content_hash)
        """,
    ),
    (
        "splits",
        """
        CREATE TABLE IF NOT EXISTS {db}.splits (
            instrument_id    LowCardinality(String),
            ticker           LowCardinality(String),
            execution_date   Date,
            to_factor        Float64,
            from_factor      Float64,
            fetched_at       DateTime('UTC'),
            content_hash     FixedString(64)
        )
        ENGINE = ReplacingMergeTree(fetched_at)
        PARTITION BY toYear(execution_date)
        ORDER BY (instrument_id, execution_date, content_hash)
        """,
    ),
    (
        "instruments",
        """
        CREATE TABLE IF NOT EXISTS {db}.instruments (
            instrument_id    String,
            isin             LowCardinality(String),
            figi             LowCardinality(String),
            composite_figi   LowCardinality(String),
            ticker           LowCardinality(String),
            exchange         LowCardinality(String),
            asset_class      LowCardinality(String),
            currency         LowCardinality(String),
            name             String,
            list_date        Nullable(Date),
            is_active        UInt8,
            last_seen        DateTime('UTC'),
            metadata         String
        )
        ENGINE = ReplacingMergeTree(last_seen)
        ORDER BY instrument_id
        """,
    ),
)


def apply_clickhouse_schema(client: Any, *, database: str | None = None) -> None:
    """Apply the market schema to ``client``.

    Idempotent — every statement is ``CREATE … IF NOT EXISTS``. Safe to
    call on every service start; safe to call against a database that
    already contains data.

    ``client`` is a ``clickhouse_connect`` ``Client`` (or anything with
    a ``.command(sql)`` method). Database is created if absent.
    """
    db = database or clickhouse_database_from_env()
    client.command(f"CREATE DATABASE IF NOT EXISTS {db}")
    for _, ddl in _TABLES:
        client.command(ddl.format(db=db))
