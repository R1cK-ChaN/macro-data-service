"""Persist :mod:`parser` records into the ``fundamentals_*`` tables.

All four ops are idempotent:

* :func:`store_fundamentals_raw` — ``INSERT OR IGNORE`` on the
  ``(provider, ticker, content_hash)`` PK; re-fetching the same payload
  bytes is a no-op, while a single-byte change lands a new row.
* :func:`project_fundamentals_company` — upsert on ``(provider, ticker)``.
* :func:`project_fundamentals_financials` — upsert on
  ``(provider, ticker, period_end, period_type, statement)``.
* :func:`project_fundamentals_highlights` — upsert on
  ``(provider, ticker, as_of_date)``.

All three projection ops guard the ``ON CONFLICT`` UPDATE branch on
``observed_at_epoch_ms`` — a late-arriving older snapshot cannot
overwrite a newer projection. Same PIT discipline as
``project_corp_events`` (issue #65/#66).

The raw-lane revision logger mirrors ``store_corp_raw`` (#66): when an
insert pushes any ``(provider, ticker)`` from one distinct content_hash
to two-or-more, a single INFO line per affected ticker surfaces the
restatement so downstream observability can pick it up without
re-querying.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from storage.models.fundamentals import (
    FundamentalsCompanyRecord,
    FundamentalsFinancialsRecord,
    FundamentalsHighlightsRecord,
    FundamentalsRawRecord,
)

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def store_fundamentals_raw(
    connection: sqlite3.Connection,
    records: Iterable[FundamentalsRawRecord],
) -> int:
    """Insert raw rows. Returns the number of new rows actually written.

    Duplicates (same provider + ticker + content_hash) are silently
    ignored. When the insert pushes any ``(provider, ticker)`` from
    one distinct content_hash to two-or-more, an INFO log line per
    affected ticker surfaces the revision (mirrors the cal_corp_raw
    logger from #66).
    """
    rows: list[tuple[str, str, int, str, str, str]] = []
    by_provider: dict[str, list[str]] = {}
    for r in records:
        rows.append(
            (
                r.provider,
                r.ticker,
                r.snapshot_epoch_ms,
                r.content_hash,
                r.payload_json,
                r.fetched_at,
            )
        )
        by_provider.setdefault(r.provider, []).append(r.ticker)
    if not rows:
        return 0

    prior_counts = _versions_per_ticker(connection, by_provider)

    before = connection.total_changes
    connection.executemany(
        """
        INSERT OR IGNORE INTO fundamentals_raw (
            provider, ticker, snapshot_epoch_ms,
            content_hash, payload_json, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    inserted = connection.total_changes - before

    if inserted > 0:
        new_counts = _versions_per_ticker(connection, by_provider)
        for key, new in new_counts.items():
            prior = prior_counts.get(key, 0)
            if new >= 2 and new > prior:
                provider, ticker = key
                logger.info(
                    "fundamentals revised provider=%s ticker=%s versions=%d",
                    provider, ticker, new,
                )

    return inserted


def _versions_per_ticker(
    connection: sqlite3.Connection,
    by_provider: dict[str, list[str]],
) -> dict[tuple[str, str], int]:
    """``COUNT(DISTINCT content_hash)`` per ``(provider, ticker)``.

    Sliced into 500-id chunks to stay below SQLite's variable-binding
    limit on a wide multi-ticker batch.
    """
    out: dict[tuple[str, str], int] = {}
    for provider, tickers in by_provider.items():
        unique = list({t for t in tickers})
        for chunk_start in range(0, len(unique), 500):
            chunk = unique[chunk_start:chunk_start + 500]
            placeholders = ",".join("?" * len(chunk))
            for row in connection.execute(
                f"""
                SELECT ticker,
                       COUNT(DISTINCT content_hash) AS versions
                FROM fundamentals_raw
                WHERE provider = ?
                  AND ticker IN ({placeholders})
                GROUP BY ticker
                """,
                (provider, *chunk),
            ).fetchall():
                out[(provider, row["ticker"])] = int(row["versions"])
    return out


def project_fundamentals_company(
    connection: sqlite3.Connection,
    records: Iterable[FundamentalsCompanyRecord],
) -> int:
    """Upsert ``fundamentals_company``. Returns number of inserts +
    updates that actually applied (PIT guard may skip an older snapshot).
    """
    now = _now_iso()
    changed = 0
    for r in records:
        params = (
            r.provider, r.ticker, r.name, r.asset_type,
            r.sector, r.industry, r.fiscal_year_end,
            r.listing_exchange, r.currency_code,
            r.country_iso, r.isin, r.cusip,
            r.payload_json, r.content_hash,
            r.observed_at_epoch_ms,
            now, now,
        )
        cursor = connection.execute(
            """
            INSERT INTO fundamentals_company (
                provider, ticker, name, asset_type,
                sector, industry, fiscal_year_end,
                listing_exchange, currency_code,
                country_iso, isin, cusip,
                payload_json, content_hash,
                observed_at_epoch_ms, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (provider, ticker) DO UPDATE SET
                name             = excluded.name,
                asset_type       = excluded.asset_type,
                sector           = excluded.sector,
                industry         = excluded.industry,
                fiscal_year_end  = excluded.fiscal_year_end,
                listing_exchange = excluded.listing_exchange,
                currency_code    = excluded.currency_code,
                country_iso      = excluded.country_iso,
                isin             = excluded.isin,
                cusip            = excluded.cusip,
                payload_json     = excluded.payload_json,
                content_hash     = excluded.content_hash,
                observed_at_epoch_ms = excluded.observed_at_epoch_ms,
                updated_at       = excluded.updated_at
            WHERE excluded.observed_at_epoch_ms >= fundamentals_company.observed_at_epoch_ms
            """,
            params,
        )
        if cursor.rowcount > 0:
            changed += 1
    return changed


def project_fundamentals_financials(
    connection: sqlite3.Connection,
    records: Iterable[FundamentalsFinancialsRecord],
) -> int:
    """Upsert ``fundamentals_financials`` rows."""
    now = _now_iso()
    changed = 0
    for r in records:
        params = (
            r.provider, r.ticker, r.period_end, r.period_type, r.statement,
            r.currency, r.filing_date,
            r.revenue, r.net_income, r.eps_basic,
            r.total_assets, r.total_equity, r.total_liabilities,
            r.cash_from_ops, r.capex,
            r.payload_json, r.content_hash,
            r.observed_at_epoch_ms,
            now, now,
        )
        cursor = connection.execute(
            """
            INSERT INTO fundamentals_financials (
                provider, ticker, period_end, period_type, statement,
                currency, filing_date,
                revenue, net_income, eps_basic,
                total_assets, total_equity, total_liabilities,
                cash_from_ops, capex,
                payload_json, content_hash,
                observed_at_epoch_ms, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (provider, ticker, period_end, period_type, statement) DO UPDATE SET
                currency             = excluded.currency,
                filing_date          = excluded.filing_date,
                revenue              = excluded.revenue,
                net_income           = excluded.net_income,
                eps_basic            = excluded.eps_basic,
                total_assets         = excluded.total_assets,
                total_equity         = excluded.total_equity,
                total_liabilities    = excluded.total_liabilities,
                cash_from_ops        = excluded.cash_from_ops,
                capex                = excluded.capex,
                payload_json         = excluded.payload_json,
                content_hash         = excluded.content_hash,
                observed_at_epoch_ms = excluded.observed_at_epoch_ms,
                updated_at           = excluded.updated_at
            WHERE excluded.observed_at_epoch_ms >= fundamentals_financials.observed_at_epoch_ms
            """,
            params,
        )
        if cursor.rowcount > 0:
            changed += 1
    return changed


def project_fundamentals_highlights(
    connection: sqlite3.Connection,
    records: Iterable[FundamentalsHighlightsRecord],
) -> int:
    """Upsert ``fundamentals_highlights`` rows."""
    now = _now_iso()
    changed = 0
    for r in records:
        params = (
            r.provider, r.ticker, r.as_of_date,
            r.market_cap, r.pe_ratio, r.eps_ttm,
            r.dividend_yield, r.book_value, r.shares_outstanding,
            r.payload_json, r.content_hash,
            r.observed_at_epoch_ms,
            now, now,
        )
        cursor = connection.execute(
            """
            INSERT INTO fundamentals_highlights (
                provider, ticker, as_of_date,
                market_cap, pe_ratio, eps_ttm,
                dividend_yield, book_value, shares_outstanding,
                payload_json, content_hash,
                observed_at_epoch_ms, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (provider, ticker, as_of_date) DO UPDATE SET
                market_cap           = excluded.market_cap,
                pe_ratio             = excluded.pe_ratio,
                eps_ttm              = excluded.eps_ttm,
                dividend_yield       = excluded.dividend_yield,
                book_value           = excluded.book_value,
                shares_outstanding   = excluded.shares_outstanding,
                payload_json         = excluded.payload_json,
                content_hash         = excluded.content_hash,
                observed_at_epoch_ms = excluded.observed_at_epoch_ms,
                updated_at           = excluded.updated_at
            WHERE excluded.observed_at_epoch_ms >= fundamentals_highlights.observed_at_epoch_ms
            """,
            params,
        )
        if cursor.rowcount > 0:
            changed += 1
    return changed
