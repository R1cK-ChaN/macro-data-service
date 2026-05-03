"""ClickHouse market-lane store adapter (issue #118 P2).

``ClickHouseMarketStore`` exposes the methods ``LocalMacroDataService``
needs to serve market reads and route market writes:

* ``get_market_history`` — daily-bar window for an instrument, raw or
  adjusted OHLCV per the caller flag.
* ``latest_market_snapshot`` — most-recent bar per instrument_id (powers
  ``_op_get_market_snapshot``).
* ``get_manifest_stats`` — low-cost market availability stats for the
  public data manifest.
* ``upsert_market_bars`` / ``upsert_corp_actions`` /
  ``upsert_market_instrument`` — batch writers used by ingestion.
* ``lookup_instrument`` — by ``instrument_id`` or ``ticker``, used by
  ingestion clients to resolve identity before writing bars.

Internally wraps a ``clickhouse_connect`` ``Client``. The package's
HTTP client uses ``urllib3`` connection pooling, so a single ``Client``
instance amortizes TCP/TLS across requests — no separate pool object.

``ReplacingMergeTree`` collapses duplicate ``(instrument_id, time)``
keys asynchronously on merge. Read paths add ``FINAL`` so the latest
row per key is returned without waiting for background merge. The cost
is per-query deduplication; the alternative (waiting for merge) gives
inconsistent reads. ``FINAL`` is the documented pattern for
``ReplacingMergeTree`` reads (CH docs §
"ReplacingMergeTree → SELECT … FINAL").
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Iterable
from datetime import date as Date, datetime, timezone
from typing import Any

from .records import CHBar, CHDividend, CHInstrument, CHSplit
from .schema import (
    CLICKHOUSE_DATABASE,
    apply_clickhouse_schema,
    clickhouse_database_from_env,
)

logger = logging.getLogger(__name__)


def clickhouse_client_from_env(*, autocommit: bool = True) -> Any:
    """Build a ``clickhouse_connect`` client from env vars.

    Uses the HTTP interface (``CLICKHOUSE_HTTP_PORT``, default 8123) —
    the python driver speaks HTTP+JSON natively, native-TCP requires a
    separate driver. HTTP perf is fine for the projected workload (200M
    bars, batch inserts of ~1k rows).
    """
    import clickhouse_connect

    host = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
    http_port = int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"))
    user = os.environ.get("CLICKHOUSE_USER", "default")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    database = clickhouse_database_from_env()
    return clickhouse_connect.get_client(
        host=host,
        port=http_port,
        username=user,
        password=password,
        database=database,
    )


class ClickHouseMarketStore:
    """Adapter exposing the market-store contract over ClickHouse."""

    _BAR_COLUMNS: tuple[str, ...] = (
        "instrument_id", "ticker", "exchange", "time",
        "open", "high", "low", "close", "volume",
        "adjusted_open", "adjusted_high", "adjusted_low",
        "adjusted_close", "adjusted_volume", "fetched_at",
    )
    _DIVIDEND_COLUMNS: tuple[str, ...] = (
        "instrument_id", "ticker", "ex_date",
        "declaration_date", "record_date", "payment_date",
        "period", "cash_amount", "unadjusted_amount",
        "currency", "fetched_at", "content_hash",
    )
    _SPLIT_COLUMNS: tuple[str, ...] = (
        "instrument_id", "ticker", "execution_date",
        "to_factor", "from_factor", "fetched_at", "content_hash",
    )
    _INSTRUMENT_COLUMNS: tuple[str, ...] = (
        "instrument_id", "isin", "figi", "composite_figi",
        "ticker", "exchange", "asset_class", "currency",
        "name", "list_date", "is_active", "last_seen", "metadata",
    )

    def __init__(
        self,
        client: Any,
        *,
        database: str | None = None,
    ) -> None:
        self._client = client
        self._database = database or clickhouse_database_from_env()

    @property
    def database(self) -> str:
        return self._database

    @property
    def client(self) -> Any:
        return self._client

    def init_schema(self) -> None:
        """Apply the market schema. Idempotent — safe to call on every
        process start."""
        apply_clickhouse_schema(self._client, database=self._database)

    # ─────────────────────────────────────────────────────────────────
    # Reads
    # ─────────────────────────────────────────────────────────────────

    def get_market_history(
        self,
        instrument_id: str,
        *,
        start: str | None = None,
        end: str | None = None,
        adjusted: bool = False,
    ) -> list[dict[str, Any]]:
        """Daily bars for ``instrument_id`` in ``[start, end]``.

        Returns rows shaped for the agent-native response —
        ``[{date, open, high, low, close, volume, ticker, exchange}, …]``
        — with ``adjusted_*`` columns selected when ``adjusted=True``.
        Window inclusive on both ends; ``start`` / ``end`` are
        ``YYYY-MM-DD`` strings (the column is ``DateTime``, so we cast
        via ``toDate(time) >= …``).

        ``date`` and ``time`` are returned as ``YYYY-MM-DD`` and
        ISO-8601 UTC strings respectively, not native ``date`` /
        ``datetime`` objects, so callers can ``json.dumps`` the
        response without a custom encoder. The CLI + HTTP server both
        run rows through ``json.dumps``, so the convention is "stringify
        at the storage boundary".
        """
        if adjusted:
            cols = (
                "adjusted_open AS open", "adjusted_high AS high",
                "adjusted_low AS low", "adjusted_close AS close",
                "adjusted_volume AS volume",
            )
        else:
            cols = ("open", "high", "low", "close", "volume")
        where = ["instrument_id = {instrument_id:String}"]
        params: dict[str, Any] = {"instrument_id": instrument_id}
        if start:
            where.append("toDate(time) >= {start:Date}")
            params["start"] = start
        if end:
            where.append("toDate(time) <= {end:Date}")
            params["end"] = end
        sql = (
            f"SELECT toDate(time) AS date, time, ticker, exchange, "
            f"{', '.join(cols)} "
            f"FROM {self._database}.bars_1d FINAL "
            f"WHERE {' AND '.join(where)} "
            f"ORDER BY time"
        )
        result = self._client.query(sql, parameters=params)
        return [
            self._stringify_temporal(
                dict(zip(result.column_names, row, strict=True))
            )
            for row in result.result_rows
        ]

    def latest_market_snapshot(self) -> list[dict[str, Any]]:
        """Most-recent bar per instrument across the entire universe.

        Backs ``_op_get_market_snapshot``. ``FINAL`` collapses any
        duplicate ``(instrument_id, time)`` rows; ``LIMIT 1 BY
        instrument_id`` after ``ORDER BY time DESC`` keeps the latest
        bar per instrument — CH-native pattern, single pass over the
        primary index.

        ``time`` is returned as an ISO-8601 UTC string for
        json-serializability — same boundary contract as
        :meth:`get_market_history`.
        """
        sql = (
            f"SELECT instrument_id, ticker, exchange, time, "
            f"close, adjusted_close, volume "
            f"FROM {self._database}.bars_1d FINAL "
            f"ORDER BY instrument_id, time DESC "
            f"LIMIT 1 BY instrument_id"
        )
        result = self._client.query(sql)
        return [
            self._stringify_temporal(
                dict(zip(result.column_names, row, strict=True))
            )
            for row in result.result_rows
        ]

    def get_manifest_stats(self) -> dict[str, Any]:
        """Metadata-backed availability stats for the consumer manifest.

        ``system.parts`` keeps this public endpoint cheap on large
        backfills. ``row_count`` reflects active MergeTree part rows, so
        it may include replacements that background merges have yet to
        collapse; the manifest only needs availability and freshness.
        """
        sql = (
            "SELECT sum(rows) AS row_count, max(max_time) AS latest_timestamp, "
            "max(modification_time) AS latest_ingested_at "
            "FROM system.parts "
            "WHERE database = {database:String} "
            "AND table = {table:String} "
            "AND active"
        )
        result = self._client.query(
            sql,
            parameters={"database": self._database, "table": "bars_1d"},
        )
        row = (
            dict(zip(result.column_names, result.result_rows[0], strict=True))
            if result.result_rows
            else {}
        )
        row = self._stringify_temporal(row)
        row_count = int(row.get("row_count") or 0)
        return {
            "row_count": row_count,
            "latest_timestamp": row.get("latest_timestamp") if row_count else None,
            "latest_ingested_at": row.get("latest_ingested_at") if row_count else None,
        }

    def lookup_instrument(
        self,
        *,
        instrument_id: str | None = None,
        ticker: str | None = None,
    ) -> dict[str, Any] | None:
        """Resolve an instrument by id or ticker. ``FINAL`` collapses
        the ``ReplacingMergeTree`` to the latest ``last_seen`` row.

        Exactly one of ``instrument_id`` / ``ticker`` must be given.
        """
        if (instrument_id is None) == (ticker is None):
            raise ValueError("exactly one of instrument_id or ticker required")
        if instrument_id is not None:
            sql = (
                f"SELECT * FROM {self._database}.instruments FINAL "
                f"WHERE instrument_id = {{instrument_id:String}} LIMIT 1"
            )
            params: dict[str, Any] = {"instrument_id": instrument_id}
        else:
            sql = (
                f"SELECT * FROM {self._database}.instruments FINAL "
                f"WHERE ticker = {{ticker:String}} LIMIT 1"
            )
            params = {"ticker": ticker or ""}
        result = self._client.query(sql, parameters=params)
        if not result.result_rows:
            return None
        return self._stringify_temporal(
            dict(zip(result.column_names, result.result_rows[0], strict=True))
        )

    # ─────────────────────────────────────────────────────────────────
    # Writes
    # ─────────────────────────────────────────────────────────────────

    def upsert_market_bars(self, bars: Iterable[CHBar]) -> int:
        """Batch-insert daily bars. Returns the number of rows written.

        ``ReplacingMergeTree(fetched_at)`` collapses duplicate
        ``(instrument_id, time)`` keys asynchronously on merge — a
        re-fetch with a higher ``fetched_at`` wins. Caller is
        responsible for filling raw + adjusted OHLCV; if EODHD only
        provides ``adjusted_close`` natively, use
        ``factor = adjusted_close / close`` to derive
        ``adjusted_{open,high,low}`` and divide ``volume`` by the
        factor for ``adjusted_volume``.
        """
        rows = [self._bar_row(b) for b in bars]
        if not rows:
            return 0
        self._client.insert(
            f"{self._database}.bars_1d",
            rows,
            column_names=list(self._BAR_COLUMNS),
        )
        return len(rows)

    def upsert_corp_actions(
        self,
        *,
        dividends: Iterable[CHDividend] = (),
        splits: Iterable[CHSplit] = (),
    ) -> tuple[int, int]:
        """Batch-insert dividends + splits. Returns ``(div_n, split_n)``.

        Append-only ``MergeTree`` — the row's ``content_hash`` is the
        third column of the sort key, so a revised payout from EODHD
        flips the hash and lands as a new row, preserving the full
        revision chain. Caller is responsible for hashing the payload;
        :func:`compute_dividend_hash` / :func:`compute_split_hash` give
        a canonical hash so revisions match across runs.
        """
        div_rows = [self._dividend_row(d) for d in dividends]
        split_rows = [self._split_row(s) for s in splits]
        if div_rows:
            self._client.insert(
                f"{self._database}.dividends",
                div_rows,
                column_names=list(self._DIVIDEND_COLUMNS),
            )
        if split_rows:
            self._client.insert(
                f"{self._database}.splits",
                split_rows,
                column_names=list(self._SPLIT_COLUMNS),
            )
        return len(div_rows), len(split_rows)

    def upsert_market_instrument(self, instrument: CHInstrument) -> None:
        """Insert or replace one instrument row.

        ``ReplacingMergeTree(last_seen)`` so the most recently seen row
        wins on background merge; reads use ``FINAL`` to surface the
        latest immediately.
        """
        self._client.insert(
            f"{self._database}.instruments",
            [self._instrument_row(instrument)],
            column_names=list(self._INSTRUMENT_COLUMNS),
        )

    def upsert_market_instruments(self, instruments: Iterable[CHInstrument]) -> int:
        """Batch-insert instrument rows. Returns the number written."""
        rows = [self._instrument_row(i) for i in instruments]
        if not rows:
            return 0
        self._client.insert(
            f"{self._database}.instruments",
            rows,
            column_names=list(self._INSTRUMENT_COLUMNS),
        )
        return len(rows)

    def list_instruments(self, *, active_only: bool | None = None) -> list[dict[str, Any]]:
        """Return latest instrument rows from ``market.instruments``."""
        where = ""
        if active_only is True:
            where = " WHERE is_active = 1"
        elif active_only is False:
            where = " WHERE is_active = 0"
        sql = f"SELECT * FROM {self._database}.instruments FINAL{where} ORDER BY ticker"
        result = self._client.query(sql)
        return [
            self._stringify_temporal(
                dict(zip(result.column_names, row, strict=True))
            )
            for row in result.result_rows
        ]

    def has_dividend_hash(
        self, *, instrument_id: str, ex_date: Date | str, content_hash: str
    ) -> bool:
        sql = (
            f"SELECT 1 FROM {self._database}.dividends FINAL "
            f"WHERE instrument_id = {{instrument_id:String}} "
            f"AND ex_date = {{ex_date:Date}} "
            f"AND content_hash = {{content_hash:String}} LIMIT 1"
        )
        result = self._client.query(
            sql,
            parameters={
                "instrument_id": instrument_id,
                "ex_date": str(ex_date),
                "content_hash": content_hash,
            },
        )
        return bool(result.result_rows)

    def has_split_hash(
        self, *, instrument_id: str, execution_date: Date | str, content_hash: str
    ) -> bool:
        sql = (
            f"SELECT 1 FROM {self._database}.splits FINAL "
            f"WHERE instrument_id = {{instrument_id:String}} "
            f"AND execution_date = {{execution_date:Date}} "
            f"AND content_hash = {{content_hash:String}} LIMIT 1"
        )
        result = self._client.query(
            sql,
            parameters={
                "instrument_id": instrument_id,
                "execution_date": str(execution_date),
                "content_hash": content_hash,
            },
        )
        return bool(result.result_rows)

    def delete_bars_for_instrument(self, instrument_id: str, *, sync: bool = True) -> None:
        """Delete all bars for an instrument before a provider restatement refill."""
        sql = (
            f"ALTER TABLE {self._database}.bars_1d "
            f"DELETE WHERE instrument_id = {{instrument_id:String}}"
        )
        if sync:
            sql += " SETTINGS mutations_sync = 1"
        self._client.command(sql, parameters={"instrument_id": instrument_id})

    def latest_bar_dates(self, *, active_only: bool = True) -> dict[str, str]:
        """Return ``instrument_id -> latest YYYY-MM-DD bar date``."""
        join = ""
        if active_only:
            join = (
                f" INNER JOIN {self._database}.instruments FINAL AS i "
                f"ON b.instrument_id = i.instrument_id AND i.is_active = 1"
            )
        sql = (
            f"SELECT b.instrument_id, toDate(max(b.time)) AS latest_date "
            f"FROM {self._database}.bars_1d FINAL AS b{join} "
            f"GROUP BY b.instrument_id"
        )
        result = self._client.query(sql)
        rows = [
            self._stringify_temporal(
                dict(zip(result.column_names, row, strict=True))
            )
            for row in result.result_rows
        ]
        return {str(row["instrument_id"]): str(row["latest_date"]) for row in rows}

    def set_instrument_active(
        self,
        instrument_id: str,
        *,
        is_active: bool,
        last_seen: datetime | None = None,
    ) -> bool:
        """Write a new latest identity row with the desired active flag."""
        row = self.lookup_instrument(instrument_id=instrument_id)
        if row is None:
            return False
        instrument = CHInstrument(
            instrument_id=str(row["instrument_id"]),
            isin=str(row.get("isin") or ""),
            figi=str(row.get("figi") or ""),
            composite_figi=str(row.get("composite_figi") or ""),
            ticker=str(row.get("ticker") or ""),
            exchange=str(row.get("exchange") or ""),
            asset_class=str(row.get("asset_class") or ""),
            currency=str(row.get("currency") or ""),
            name=str(row.get("name") or ""),
            list_date=_date_from_value(row.get("list_date")),
            is_active=is_active,
            last_seen=last_seen or datetime.now(timezone.utc),
            metadata=str(row.get("metadata") or "{}"),
        )
        self.upsert_market_instrument(instrument)
        return True

    # ─────────────────────────────────────────────────────────────────
    # Row marshalling
    # ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _stringify_temporal(row: dict[str, Any]) -> dict[str, Any]:
        """Convert ``date`` / ``datetime`` values to ISO strings in-place.

        ``clickhouse_connect`` returns native temporal objects; the
        service layer feeds rows directly into ``json.dumps`` (CLI +
        HTTP), which can't encode them. Stringify at the storage
        boundary so downstream stays free of custom encoders.
        ``datetime`` -> ``YYYY-MM-DDTHH:MM:SSZ`` (UTC); ``date`` ->
        ``YYYY-MM-DD``. Naive datetimes from CH always carry UTC
        wall-clock, so the trailing ``Z`` reflects truth.
        """
        from datetime import date as _Date, datetime as _DateTime
        for key, value in row.items():
            if isinstance(value, _DateTime):
                row[key] = value.strftime("%Y-%m-%dT%H:%M:%SZ")
            elif isinstance(value, _Date):
                row[key] = value.strftime("%Y-%m-%d")
        return row

    @staticmethod
    def _bar_row(b: CHBar) -> tuple[Any, ...]:
        return (
            b.instrument_id, b.ticker, b.exchange, b.time,
            b.open, b.high, b.low, b.close, b.volume,
            b.adjusted_open, b.adjusted_high, b.adjusted_low,
            b.adjusted_close, b.adjusted_volume, b.fetched_at,
        )

    @staticmethod
    def _dividend_row(d: CHDividend) -> tuple[Any, ...]:
        return (
            d.instrument_id, d.ticker, d.ex_date,
            d.declaration_date, d.record_date, d.payment_date,
            d.period, d.cash_amount, d.unadjusted_amount,
            d.currency, d.fetched_at, d.content_hash,
        )

    @staticmethod
    def _split_row(s: CHSplit) -> tuple[Any, ...]:
        return (
            s.instrument_id, s.ticker, s.execution_date,
            s.to_factor, s.from_factor, s.fetched_at, s.content_hash,
        )

    @staticmethod
    def _instrument_row(i: CHInstrument) -> tuple[Any, ...]:
        return (
            i.instrument_id, i.isin, i.figi, i.composite_figi,
            i.ticker, i.exchange, i.asset_class, i.currency,
            i.name, i.list_date, 1 if i.is_active else 0,
            i.last_seen, i.metadata,
        )


def compute_dividend_hash(
    *, instrument_id: str, ex_date: Date | str,
    cash_amount: float, unadjusted_amount: float,
    currency: str = "",
    declaration_date: Date | str | None = None,
    record_date: Date | str | None = None,
    payment_date: Date | str | None = None,
    period: str = "",
) -> str:
    """Canonical hash for one dividend row.

    SHA-256 over a stable JSON encoding of the mutable EODHD fields. The
    per-ticker and bulk paths share a stable revision detector.
    """
    del instrument_id
    payload = {
        "ex_date": str(ex_date),
        "value": float(cash_amount),
        "unadjustedValue": float(unadjusted_amount),
        "period": period or "",
        "currency": currency,
        "declarationDate": str(declaration_date) if declaration_date else "",
        "recordDate": str(record_date) if record_date else "",
        "paymentDate": str(payment_date) if payment_date else "",
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    ).hexdigest()


def compute_split_hash(
    *, instrument_id: str, execution_date: Date | str,
    to_factor: float, from_factor: float,
) -> str:
    """Canonical hash for one split row. Same contract as
    :func:`compute_dividend_hash`."""
    del instrument_id
    payload = {
        "execution_date": str(execution_date),
        "to_factor": float(to_factor),
        "from_factor": float(from_factor),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    ).hexdigest()


def _date_from_value(value: Any) -> Date | None:
    if value in (None, ""):
        return None
    if isinstance(value, Date):
        return value
    return Date.fromisoformat(str(value)[:10])


__all__ = [
    "ClickHouseMarketStore",
    "clickhouse_client_from_env",
    "compute_dividend_hash",
    "compute_split_hash",
    "CHBar",
    "CHDividend",
    "CHSplit",
    "CHInstrument",
]
