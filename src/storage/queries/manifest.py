"""Consumer-facing data availability manifest queries.

The manifest is intentionally table-count based: consumers need a fast,
read-only inventory before choosing a data surface. SQLite owns the macro
line; the service passes ClickHouse market stats into this mixin.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from contracts import format_epoch_iso, format_epoch_ms_iso, utc_now


@dataclass(frozen=True)
class _DatasetDefinition:
    dataset: str
    label: str
    storage: str
    launch_state: str


_DATASETS: tuple[_DatasetDefinition, ...] = (
    _DatasetDefinition(
        "macro_timeseries",
        "Macro time series",
        "indicator_vintages",
        "available",
    ),
    _DatasetDefinition(
        "calendar",
        "Economic calendar",
        "cal_econ_event",
        "available",
    ),
    _DatasetDefinition(
        "documents",
        "Documents",
        "document",
        "available",
    ),
    _DatasetDefinition(
        "news",
        "News",
        "news_articles",
        "available",
    ),
    _DatasetDefinition(
        "market_bars",
        "Market bars",
        "clickhouse.bars_1d",
        "empty",
    ),
    _DatasetDefinition(
        "fundamentals",
        "Fundamentals",
        "fundamentals_*",
        "empty",
    ),
    _DatasetDefinition(
        "corp_calendar",
        "Corporate calendar",
        "cal_corp_event",
        "empty",
    ),
)

_QUALITY_ALIASES: dict[str, tuple[str, ...]] = {
    "macro_timeseries": (
        "macro_timeseries",
        "timeseries",
        "indicators",
        "indicator_vintages",
        "concept:",
    ),
    "calendar": ("calendar", "economic_calendar", "cal_econ_event"),
    "documents": ("documents", "document"),
    "news": ("news", "news_articles"),
    "market_bars": ("market_bars", "market_price", "market"),
    "fundamentals": ("fundamentals",),
    "corp_calendar": ("corp_calendar", "corporate_calendar", "cal_corp_event"),
}
_GLOBAL_QUALITY_SOURCES = {"all", "data_quality", "macro_data", "macro-data"}


class _ManifestQueriesMixin:
    def get_data_manifest(
        self,
        *,
        market_bars: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return the launch-facing dataset availability contract."""
        with self._connection(commit=False) as connection:
            quality_reports = _load_quality_reports(connection)
            rows = [
                self._dataset_manifest_row(
                    connection,
                    definition,
                    quality_reports=quality_reports,
                    market_bars=market_bars or {},
                )
                for definition in _DATASETS
            ]
        summary = {
            "total": len(rows),
            "available": sum(1 for row in rows if row["status"] == "available"),
            "empty": sum(1 for row in rows if row["status"] == "empty"),
            "degraded": sum(1 for row in rows if row["status"] == "degraded"),
            "experimental": sum(1 for row in rows if row["status"] == "experimental"),
        }
        return {
            "version": "v1",
            "generated_at": utc_now().isoformat(),
            "summary": summary,
            "datasets": rows,
        }

    def _dataset_manifest_row(
        self,
        connection: sqlite3.Connection,
        definition: _DatasetDefinition,
        *,
        quality_reports: list[dict[str, Any]],
        market_bars: dict[str, Any],
    ) -> dict[str, Any]:
        stats = _dataset_stats(connection, definition.dataset, market_bars=market_bars)
        quality = _quality_for_dataset(definition.dataset, quality_reports)
        status = _dataset_status(
            row_count=int(stats.get("row_count") or 0),
            launch_state=definition.launch_state,
            quality_status=quality["quality_status"],
        )
        row = {
            "dataset": definition.dataset,
            "label": definition.label,
            "status": status,
            "launch_state": definition.launch_state,
            "row_count": int(stats.get("row_count") or 0),
            "latest_timestamp": stats.get("latest_timestamp"),
            "latest_ingested_at": stats.get("latest_ingested_at"),
            "last_quality_run": quality["last_quality_run"],
            "quality_status": quality["quality_status"],
            "storage": definition.storage,
        }
        if stats.get("notes"):
            row["notes"] = list(stats["notes"])
        if stats.get("error"):
            row["error"] = str(stats["error"])
        return row


def _dataset_stats(
    connection: sqlite3.Connection,
    dataset: str,
    *,
    market_bars: dict[str, Any],
) -> dict[str, Any]:
    if dataset == "macro_timeseries":
        return {
            "row_count": _int_scalar(connection, "SELECT COUNT(*) FROM indicator_vintages"),
            "latest_timestamp": _text_scalar(
                connection,
                "SELECT MAX(observation_date) FROM indicator_vintages",
            ),
            "latest_ingested_at": _text_scalar(
                connection,
                "SELECT MAX(scraped_at) FROM indicator_vintages",
            ),
        }
    if dataset == "calendar":
        return {
            "row_count": _int_scalar(connection, "SELECT COUNT(*) FROM cal_econ_event"),
            "latest_timestamp": _text_scalar(
                connection,
                "SELECT MAX(event_time_utc) FROM cal_econ_event",
            ),
            "latest_ingested_at": _text_scalar(
                connection,
                "SELECT MAX(updated_at) FROM cal_econ_event",
            ),
        }
    if dataset == "documents":
        return {
            "row_count": _int_scalar(connection, "SELECT COUNT(*) FROM document"),
            "latest_timestamp": _text_scalar(
                connection,
                "SELECT MAX(COALESCE(published_at, published_date)) FROM document",
            ),
            "latest_ingested_at": _text_scalar(
                connection,
                "SELECT MAX(updated_at) FROM document",
            ),
        }
    if dataset == "news":
        latest_epoch = _optional_int_scalar(
            connection,
            "SELECT MAX(timestamp) FROM news_articles",
        )
        return {
            "row_count": _int_scalar(connection, "SELECT COUNT(*) FROM news_articles"),
            "latest_timestamp": (
                format_epoch_iso(latest_epoch) if latest_epoch is not None else None
            ),
            "latest_ingested_at": _text_scalar(
                connection,
                "SELECT MAX(scraped_at) FROM news_articles",
            ),
        }
    if dataset == "market_bars":
        return {
            "row_count": int(market_bars.get("row_count") or 0),
            "latest_timestamp": market_bars.get("latest_timestamp"),
            "latest_ingested_at": market_bars.get("latest_ingested_at"),
            "notes": market_bars.get("notes", []),
            "error": market_bars.get("error"),
        }
    if dataset == "fundamentals":
        row_count = sum(
            _int_scalar(connection, f"SELECT COUNT(*) FROM {table}")
            for table in (
                "fundamentals_company",
                "fundamentals_financials",
                "fundamentals_highlights",
                "fundamentals_estimates",
            )
        )
        latest_epoch_ms = _max_optional_int([
            _optional_int_scalar(
                connection,
                "SELECT MAX(observed_at_epoch_ms) FROM fundamentals_company",
            ),
            _optional_int_scalar(
                connection,
                "SELECT MAX(observed_at_epoch_ms) FROM fundamentals_financials",
            ),
            _optional_int_scalar(
                connection,
                "SELECT MAX(observed_at_epoch_ms) FROM fundamentals_highlights",
            ),
            _optional_int_scalar(
                connection,
                "SELECT MAX(observed_at_epoch_ms) FROM fundamentals_estimates",
            ),
        ])
        return {
            "row_count": row_count,
            "latest_timestamp": _max_text([
                _text_scalar(
                    connection,
                    "SELECT MAX(period_end) FROM fundamentals_financials",
                ),
                _text_scalar(
                    connection,
                    "SELECT MAX(as_of_date) FROM fundamentals_highlights",
                ),
                _text_scalar(
                    connection,
                    "SELECT MAX(period_end) FROM fundamentals_estimates",
                ),
            ]),
            "latest_ingested_at": (
                format_epoch_ms_iso(latest_epoch_ms)
                if latest_epoch_ms is not None
                else None
            ),
        }
    if dataset == "corp_calendar":
        return {
            "row_count": _int_scalar(connection, "SELECT COUNT(*) FROM cal_corp_event"),
            "latest_timestamp": _text_scalar(
                connection,
                "SELECT MAX(event_time_utc) FROM cal_corp_event",
            ),
            "latest_ingested_at": _text_scalar(
                connection,
                "SELECT MAX(updated_at) FROM cal_corp_event",
            ),
        }
    raise KeyError(f"unknown manifest dataset: {dataset}")


def _load_quality_reports(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(connection, "validation_reports"):
        return []
    rows = connection.execute(
        """
        SELECT source, timestamp, passed, error_count, warning_count,
               total_checks, report_json
        FROM validation_reports
        ORDER BY timestamp DESC, id DESC
        """
    ).fetchall()
    reports: list[dict[str, Any]] = []
    for row in rows:
        report_json: dict[str, Any] = {}
        raw = row["report_json"]
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    report_json = parsed
            except json.JSONDecodeError:
                report_json = {}
        reports.append({
            "source": str(row["source"] or report_json.get("source") or ""),
            "timestamp": str(row["timestamp"] or report_json.get("timestamp") or ""),
            "passed": bool(row["passed"]),
            "error_count": int(row["error_count"] or 0),
            "warning_count": int(row["warning_count"] or 0),
            "total_checks": int(row["total_checks"] or 0),
        })
    return reports


def _quality_for_dataset(
    dataset: str,
    reports: list[dict[str, Any]],
) -> dict[str, str | None]:
    aliases = _QUALITY_ALIASES.get(dataset, ())
    for report in reports:
        source = str(report.get("source") or "")
        if (
            _source_matches_aliases(source, aliases)
            or source in _GLOBAL_QUALITY_SOURCES
        ):
            return _quality_payload(report)
    return {"last_quality_run": None, "quality_status": "unknown"}


def _quality_payload(report: dict[str, Any]) -> dict[str, str | None]:
    if not bool(report.get("passed")):
        status = "fail"
    elif int(report.get("warning_count") or 0) > 0:
        status = "warning"
    else:
        status = "pass"
    return {
        "last_quality_run": str(report.get("timestamp") or "") or None,
        "quality_status": status,
    }


def _source_matches_aliases(source: str, aliases: tuple[str, ...]) -> bool:
    for alias in aliases:
        if alias.endswith(":"):
            if source.startswith(alias):
                return True
        elif source == alias:
            return True
    return False


def _dataset_status(
    *,
    row_count: int,
    launch_state: str,
    quality_status: str | None,
) -> str:
    if row_count <= 0:
        return "empty"
    if quality_status == "fail":
        return "degraded"
    if launch_state == "experimental":
        return "experimental"
    return "available"


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type IN ('table', 'view') AND name = ?
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _int_scalar(connection: sqlite3.Connection, sql: str) -> int:
    row = connection.execute(sql).fetchone()
    if row is None:
        return 0
    return int(row[0] or 0)


def _optional_int_scalar(connection: sqlite3.Connection, sql: str) -> int | None:
    row = connection.execute(sql).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def _text_scalar(connection: sqlite3.Connection, sql: str) -> str | None:
    row = connection.execute(sql).fetchone()
    if row is None or row[0] in (None, ""):
        return None
    return str(row[0])


def _max_optional_int(values: list[int | None]) -> int | None:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _max_text(values: list[str | None]) -> str | None:
    present = [value for value in values if value]
    return max(present) if present else None
