"""Market-domain query helpers for SQLiteEngineStore.

Covers market_prices, market_instruments, market_symbol_history,
market_price_bars, plus the portfolio side (portfolio_holdings,
portfolio_vol_snapshots, portfolio_alerts).

Extracted from storage.sqlite in issue #71 Tier 2.1B-2. Methods rely on
the ``self._connection`` context manager defined on the SQLiteEngineStore
base class — composition wires them together via multiple inheritance.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from contracts import utc_now
from storage.models.market import (
    MarketCorpActionsRawRecord,
    MarketInstrumentRecord,
    MarketPriceBarRecord,
    MarketPriceRecord,
    MarketSymbolHistoryRecord,
)


class _MarketQueriesMixin:
    def insert_market_price(self, price: MarketPriceRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO market_prices (
                    symbol,
                    asset_class,
                    name,
                    price,
                    change_pct,
                    timestamp,
                    scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    price.symbol,
                    price.asset_class,
                    price.name,
                    price.price,
                    price.change_pct,
                    price.timestamp,
                    utc_now().isoformat(),
                ),
            )

    def upsert_market_instrument(self, instrument: MarketInstrumentRecord) -> None:
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            existing = connection.execute(
                "SELECT created_at FROM market_instruments WHERE instrument_id = ?",
                (instrument.instrument_id,),
            ).fetchone()
            created_at = existing["created_at"] if existing else now
            connection.execute(
                """
                INSERT OR REPLACE INTO market_instruments (
                    instrument_id, primary_ticker, name, asset_class, market,
                    exchange_code, currency, isin, openfigi, composite_figi,
                    share_class_figi, cusip, lei, primary_provider,
                    provider_symbols_json, history_status, description_for_agent,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    instrument.instrument_id,
                    instrument.primary_ticker,
                    instrument.name,
                    instrument.asset_class,
                    instrument.market,
                    instrument.exchange_code,
                    instrument.currency,
                    instrument.isin,
                    instrument.openfigi,
                    instrument.composite_figi,
                    instrument.share_class_figi,
                    instrument.cusip,
                    instrument.lei,
                    instrument.primary_provider,
                    json.dumps(instrument.provider_symbols_json, ensure_ascii=True, sort_keys=True),
                    instrument.history_status,
                    instrument.description_for_agent,
                    created_at,
                    now,
                ),
            )

    def get_market_instrument(self, instrument_id: str) -> MarketInstrumentRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM market_instruments WHERE instrument_id = ?",
                (instrument_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_market_instrument(row)

    def find_market_instrument_by_ticker(self, ticker: str) -> MarketInstrumentRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM market_instruments WHERE primary_ticker = ? LIMIT 1",
                (ticker.upper(),),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_market_instrument(row)

    @staticmethod
    def _row_to_market_instrument(row: sqlite3.Row) -> MarketInstrumentRecord:
        return MarketInstrumentRecord(
            instrument_id=row["instrument_id"],
            primary_ticker=row["primary_ticker"],
            name=row["name"],
            asset_class=row["asset_class"],
            market=row["market"],
            exchange_code=row["exchange_code"],
            currency=row["currency"],
            isin=row["isin"],
            openfigi=row["openfigi"],
            composite_figi=row["composite_figi"],
            share_class_figi=row["share_class_figi"],
            cusip=row["cusip"],
            lei=row["lei"],
            primary_provider=row["primary_provider"],
            provider_symbols_json=json.loads(row["provider_symbols_json"] or "{}"),
            history_status=row["history_status"],
            description_for_agent=row["description_for_agent"],
        )

    def update_instrument_history_status(self, instrument_id: str, history_status: str) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                "UPDATE market_instruments SET history_status = ?, updated_at = ? WHERE instrument_id = ?",
                (history_status, utc_now().isoformat(), instrument_id),
            )

    def upsert_market_symbol_segment(self, segment: MarketSymbolHistoryRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO market_symbol_history (
                    segment_id, instrument_id, ticker, provider_name,
                    exchange_code, isin, figi, valid_from, valid_to,
                    event_type, mapping_confidence, source_name, raw_json, collected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    segment.segment_id,
                    segment.instrument_id,
                    segment.ticker,
                    segment.provider_name,
                    segment.exchange_code,
                    segment.isin,
                    segment.figi,
                    segment.valid_from,
                    segment.valid_to,
                    segment.event_type,
                    segment.mapping_confidence,
                    segment.source_name,
                    json.dumps(segment.raw_json, ensure_ascii=True, sort_keys=True),
                    utc_now().isoformat(),
                ),
            )

    def list_symbol_segments(self, instrument_id: str) -> list[MarketSymbolHistoryRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT * FROM market_symbol_history WHERE instrument_id = ? ORDER BY valid_from",
                (instrument_id,),
            ).fetchall()
        return [
            MarketSymbolHistoryRecord(
                segment_id=row["segment_id"],
                instrument_id=row["instrument_id"],
                ticker=row["ticker"],
                provider_name=row["provider_name"],
                valid_from=row["valid_from"],
                valid_to=row["valid_to"],
                exchange_code=row["exchange_code"],
                isin=row["isin"],
                figi=row["figi"],
                event_type=row["event_type"],
                mapping_confidence=row["mapping_confidence"],
                source_name=row["source_name"],
                raw_json=json.loads(row["raw_json"] or "{}"),
            )
            for row in rows
        ]

    def upsert_market_price_bar(self, bar: MarketPriceBarRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO market_price_bars (
                    instrument_id, source_segment_id, date, bar_interval,
                    open, high, low, close, volume,
                    adjusted_open, adjusted_high, adjusted_low, adjusted_close, adjusted_volume,
                    dividend_cash, split_factor, source_name, source_symbol,
                    has_break_detected, has_pre2018_delisted,
                    has_missing_corp_acts, has_mapping_review_needed,
                    quality_flags_json, collected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bar.instrument_id,
                    bar.source_segment_id,
                    bar.date,
                    bar.bar_interval,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    bar.adjusted_open,
                    bar.adjusted_high,
                    bar.adjusted_low,
                    bar.adjusted_close,
                    bar.adjusted_volume,
                    bar.dividend_cash,
                    bar.split_factor,
                    bar.source_name,
                    bar.source_symbol,
                    1 if bar.has_break_detected else 0,
                    1 if bar.has_pre2018_delisted else 0,
                    1 if bar.has_missing_corp_acts else 0,
                    1 if bar.has_mapping_review_needed else 0,
                    json.dumps(bar.quality_flags_json, ensure_ascii=True, sort_keys=True),
                    utc_now().isoformat(),
                ),
            )

    def list_market_price_bars(
        self,
        instrument_id: str,
        *,
        start: str | None = None,
        end: str | None = None,
        bar_interval: str = "1d",
    ) -> list[MarketPriceBarRecord]:
        sql = [
            "SELECT * FROM market_price_bars WHERE instrument_id = ? AND bar_interval = ?",
        ]
        params: list[Any] = [instrument_id, bar_interval]
        if start:
            sql.append("AND date >= ?")
            params.append(start)
        if end:
            sql.append("AND date <= ?")
            params.append(end)
        sql.append("ORDER BY date")
        with self._connection(commit=False) as connection:
            rows = connection.execute(" ".join(sql), params).fetchall()
        return [
            MarketPriceBarRecord(
                instrument_id=row["instrument_id"],
                source_segment_id=row["source_segment_id"],
                date=row["date"],
                bar_interval=row["bar_interval"],
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"],
                adjusted_open=row["adjusted_open"],
                adjusted_high=row["adjusted_high"],
                adjusted_low=row["adjusted_low"],
                adjusted_close=row["adjusted_close"],
                adjusted_volume=row["adjusted_volume"],
                dividend_cash=row["dividend_cash"],
                split_factor=row["split_factor"],
                source_name=row["source_name"],
                source_symbol=row["source_symbol"],
                has_break_detected=bool(row["has_break_detected"]),
                has_pre2018_delisted=bool(row["has_pre2018_delisted"]),
                has_missing_corp_acts=bool(row["has_missing_corp_acts"]),
                has_mapping_review_needed=bool(row["has_mapping_review_needed"]),
                quality_flags_json=json.loads(row["quality_flags_json"] or "{}"),
            )
            for row in rows
        ]

    # ── Market corp-actions audit lane (issue #67 slice 2) ─────────────

    def insert_market_corp_actions_raw(
        self,
        records: list[MarketCorpActionsRawRecord],
    ) -> int:
        """Insert raw corp-action snapshots; return number of new rows.

        ``INSERT OR IGNORE`` on the
        ``(provider, ticker, action_type, event_date, content_hash)`` PK,
        matching the ``cal_corp_raw`` idempotency contract — same hash =
        same row, no duplicate write. A revised dividend amount changes
        the hash and lands as a new row, preserving the revision chain.
        """
        if not records:
            return 0
        rows = [
            (
                r.provider,
                r.ticker,
                r.action_type,
                r.event_date,
                r.snapshot_epoch_ms,
                r.content_hash,
                r.payload_json,
                r.fetched_at,
            )
            for r in records
        ]
        with self._connection(commit=True) as connection:
            before = connection.total_changes
            connection.executemany(
                """
                INSERT OR IGNORE INTO market_corp_actions_raw (
                    provider, ticker, action_type, event_date,
                    snapshot_epoch_ms, content_hash, payload_json, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return connection.total_changes - before

    def latest_market_corp_actions_for_ticker(
        self,
        provider: str,
        ticker: str,
    ) -> list[MarketCorpActionsRawRecord]:
        """Latest snapshot per ``(action_type, event_date)`` for the
        ``(provider, ticker)`` pair. Drives the projection step:
        re-running projection from raw never re-fetches EODHD.

        Hitting ``idx_market_corp_actions_raw_latest``
        (``provider, ticker, action_type, event_date, snapshot_epoch_ms DESC``)
        keeps this O(distinct events) regardless of revision-chain depth.
        """
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT t.provider, t.ticker, t.action_type, t.event_date,
                       t.snapshot_epoch_ms, t.content_hash, t.payload_json,
                       t.fetched_at
                FROM market_corp_actions_raw t
                INNER JOIN (
                    SELECT provider, ticker, action_type, event_date,
                           MAX(snapshot_epoch_ms) AS max_epoch
                    FROM market_corp_actions_raw
                    WHERE provider = ? AND ticker = ?
                    GROUP BY provider, ticker, action_type, event_date
                ) latest
                  ON t.provider = latest.provider
                 AND t.ticker = latest.ticker
                 AND t.action_type = latest.action_type
                 AND t.event_date = latest.event_date
                 AND t.snapshot_epoch_ms = latest.max_epoch
                ORDER BY t.action_type, t.event_date
                """,
                (provider, ticker),
            ).fetchall()
        return [
            MarketCorpActionsRawRecord(
                provider=row["provider"],
                ticker=row["ticker"],
                action_type=row["action_type"],
                event_date=row["event_date"],
                snapshot_epoch_ms=int(row["snapshot_epoch_ms"]),
                content_hash=row["content_hash"],
                payload_json=row["payload_json"],
                fetched_at=row["fetched_at"],
            )
            for row in rows
        ]

    def update_market_price_bar_corp_action(
        self,
        *,
        instrument_id: str,
        date: str,
        bar_interval: str = "1d",
        dividend_cash: float | None = None,
        split_factor: float | None = None,
    ) -> int:
        """Set ``dividend_cash`` and/or ``split_factor`` on every bar
        matching ``(instrument_id, date, bar_interval)``.

        Returns the number of bar rows actually updated. Used by the
        slice-2 projection step — the bar PK is
        ``(instrument_id, date, bar_interval, source_name, source_symbol)``
        so multiple-source rows on the same date all get refreshed by a
        single call (rare, but possible if both EODHD and Tiingo write
        the same instrument).

        ``has_missing_corp_acts`` is also cleared to 0 because the
        bar now carries a real value from the audit lane — the flag is
        a "we never had a corp-action snapshot for this bar" signal,
        not a "we don't have one right now" signal.
        """
        sets: list[str] = []
        params: list[Any] = []
        if dividend_cash is not None:
            sets.append("dividend_cash = ?")
            params.append(float(dividend_cash))
        if split_factor is not None:
            sets.append("split_factor = ?")
            params.append(float(split_factor))
        if not sets:
            return 0
        sets.append("has_missing_corp_acts = 0")
        params.extend([instrument_id, date, bar_interval])
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                f"UPDATE market_price_bars SET {', '.join(sets)} "
                "WHERE instrument_id = ? AND date = ? AND bar_interval = ?",
                params,
            )
            return cursor.rowcount

    def clear_market_price_bars_missing_corp_acts(
        self,
        *,
        instrument_id: str,
        start: str | None = None,
        end: str | None = None,
        bar_interval: str = "1d",
    ) -> int:
        """Clear ``has_missing_corp_acts`` on bars in the window.

        After ``refresh_corp_actions`` lands every dividend / split
        EODHD knows for the ticker, the audit lane has *confirmed*
        there are no corp actions on the bars where the projection
        loop had no event — they are not "missing", they're "verified
        empty". Walking the window once and clearing the flag matches
        the new ground truth. Bars outside the window keep their prior
        flag (we haven't yet fetched corp actions for those dates).

        Returns the number of rows updated.
        """
        sql = [
            "UPDATE market_price_bars SET has_missing_corp_acts = 0 "
            "WHERE instrument_id = ? AND bar_interval = ? "
            "AND has_missing_corp_acts = 1"
        ]
        params: list[Any] = [instrument_id, bar_interval]
        if start:
            sql.append("AND date >= ?")
            params.append(start)
        if end:
            sql.append("AND date <= ?")
            params.append(end)
        with self._connection(commit=True) as connection:
            cursor = connection.execute(" ".join(sql), params)
            return cursor.rowcount

    def latest_market_prices(self) -> list[MarketPriceRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT latest.* FROM market_prices latest
                INNER JOIN (
                    SELECT symbol, MAX(id) AS max_id
                    FROM market_prices
                    GROUP BY symbol
                ) grouped ON latest.id = grouped.max_id
                ORDER BY latest.asset_class ASC, latest.symbol ASC
                """
            ).fetchall()
        return [self._row_to_market_price(row) for row in rows]

    def _row_to_market_price(self, row: sqlite3.Row) -> MarketPriceRecord:
        return MarketPriceRecord(
            symbol=row["symbol"],
            asset_class=row["asset_class"],
            name=row["name"],
            price=float(row["price"]),
            change_pct=float(row["change_pct"]) if row["change_pct"] is not None else None,
            timestamp=int(row["timestamp"]),
        )

    def replace_portfolio_holdings(
        self,
        holdings: list[dict[str, Any]],
        portfolio_id: str = "default",
    ) -> None:
        """Replace all holdings for a portfolio (atomic swap)."""
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                "DELETE FROM portfolio_holdings WHERE portfolio_id = ?",
                (portfolio_id,),
            )
            for h in holdings:
                connection.execute(
                    """
                    INSERT INTO portfolio_holdings
                        (portfolio_id, symbol, name, asset_class, weight, notional, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        portfolio_id,
                        h["symbol"],
                        h["name"],
                        h["asset_class"],
                        h["weight"],
                        h["notional"],
                        now,
                    ),
                )

    def list_portfolio_holdings(
        self, portfolio_id: str = "default",
    ) -> list[dict[str, Any]]:
        """Return holdings for a portfolio as list of dicts."""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT symbol, name, asset_class, weight, notional, updated_at
                FROM portfolio_holdings
                WHERE portfolio_id = ?
                ORDER BY weight DESC
                """,
                (portfolio_id,),
            ).fetchall()
        return [
            {
                "symbol": row["symbol"],
                "name": row["name"],
                "asset_class": row["asset_class"],
                "weight": row["weight"],
                "notional": row["notional"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    def save_vol_snapshot(
        self,
        portfolio_id: str,
        snapshot_json: dict[str, Any],
    ) -> int:
        """Persist a volatility snapshot, return its id."""
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO portfolio_vol_snapshots (portfolio_id, snapshot_json, created_at)
                VALUES (?, ?, ?)
                """,
                (
                    portfolio_id,
                    json.dumps(snapshot_json, ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def latest_vol_snapshot(
        self, portfolio_id: str = "default",
    ) -> dict[str, Any] | None:
        """Return the most recent snapshot dict, or None."""
        with self._connection(commit=False) as connection:
            row = connection.execute(
                """
                SELECT snapshot_json FROM portfolio_vol_snapshots
                WHERE portfolio_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (portfolio_id,),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row["snapshot_json"])

    def list_vol_snapshots(
        self, portfolio_id: str = "default", *, limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Return recent snapshots newest-first."""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT snapshot_json, created_at FROM portfolio_vol_snapshots
                WHERE portfolio_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (portfolio_id, limit),
            ).fetchall()
        return [
            {**json.loads(row["snapshot_json"]), "stored_at": row["created_at"]}
            for row in rows
        ]

    def save_portfolio_alert(
        self,
        portfolio_id: str,
        alert_type: str,
        severity: str,
        message: str,
    ) -> int:
        """Persist an alert, return its id."""
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO portfolio_alerts
                    (portfolio_id, alert_type, severity, message, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (portfolio_id, alert_type, severity, message, now),
            )
            return int(cursor.lastrowid)

    def list_portfolio_alerts(
        self,
        portfolio_id: str = "default",
        *,
        limit: int = 20,
        unacknowledged_only: bool = False,
    ) -> list[dict[str, Any]]:
        """Return recent portfolio alerts."""
        conditions = ["portfolio_id = ?"]
        params: list[Any] = [portfolio_id]
        if unacknowledged_only:
            conditions.append("acknowledged = 0")
        where = " AND ".join(conditions)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT id, alert_type, severity, message, acknowledged, created_at
                FROM portfolio_alerts
                WHERE {where}
                ORDER BY id DESC
                LIMIT ?
                """,
                [*params, limit],
            ).fetchall()
        return [
            {
                "id": row["id"],
                "alert_type": row["alert_type"],
                "severity": row["severity"],
                "message": row["message"],
                "acknowledged": bool(row["acknowledged"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]
