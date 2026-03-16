from __future__ import annotations

import json
import sqlite3
from typing import Any


class ValidationStore:
    """Manages validation-specific SQLite tables.

    Wraps a raw sqlite3 connection to avoid bloating the main
    SQLiteEngineStore with validation concerns.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self._db_path)
            self._conn.row_factory = sqlite3.Row
            self._init_tables()
        return self._conn

    def _init_tables(self) -> None:
        conn = self._conn
        assert conn is not None
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS validation_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                run_id TEXT NOT NULL UNIQUE,
                timestamp TEXT NOT NULL,
                passed INTEGER NOT NULL,
                error_count INTEGER NOT NULL,
                warning_count INTEGER NOT NULL,
                total_checks INTEGER NOT NULL,
                duration_ms INTEGER NOT NULL,
                report_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS validation_baselines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                series_id TEXT NOT NULL,
                baseline_type TEXT NOT NULL,
                baseline_json TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                UNIQUE(source, series_id, baseline_type)
            );

            CREATE TABLE IF NOT EXISTS validation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                check_name TEXT NOT NULL,
                layer TEXT NOT NULL,
                passed INTEGER NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                series_id TEXT,
                timestamp TEXT NOT NULL,
                details_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_val_reports_source
                ON validation_reports(source, timestamp);
            CREATE INDEX IF NOT EXISTS idx_val_history_source
                ON validation_history(source, timestamp);
            CREATE INDEX IF NOT EXISTS idx_val_baselines_lookup
                ON validation_baselines(source, series_id, baseline_type);
            """
        )

    # ── Reports ──────────────────────────────────────────────────────

    def save_report(self, report_dict: dict[str, Any]) -> None:
        conn = self._get_conn()
        conn.execute(
            """
            INSERT OR REPLACE INTO validation_reports
                (source, run_id, timestamp, passed, error_count,
                 warning_count, total_checks, duration_ms, report_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                report_dict["source"],
                report_dict["run_id"],
                report_dict["timestamp"],
                1 if report_dict["passed"] else 0,
                report_dict["error_count"],
                report_dict["warning_count"],
                report_dict["total_checks"],
                report_dict["duration_ms"],
                json.dumps(report_dict),
            ),
        )
        conn.commit()

    def get_latest_report(self, source: str) -> dict[str, Any] | None:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT report_json FROM validation_reports WHERE source = ? ORDER BY timestamp DESC LIMIT 1",
            (source,),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row["report_json"])

    def list_reports(
        self, source: str | None = None, limit: int = 20
    ) -> list[dict[str, Any]]:
        conn = self._get_conn()
        if source:
            rows = conn.execute(
                "SELECT report_json FROM validation_reports WHERE source = ? ORDER BY timestamp DESC LIMIT ?",
                (source, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT report_json FROM validation_reports ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [json.loads(r["report_json"]) for r in rows]

    # ── Baselines ────────────────────────────────────────────────────

    def save_baseline(
        self,
        source: str,
        series_id: str,
        baseline_type: str,
        baseline: dict[str, Any],
        captured_at: str,
    ) -> None:
        conn = self._get_conn()
        conn.execute(
            """
            INSERT OR REPLACE INTO validation_baselines
                (source, series_id, baseline_type, baseline_json, captured_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (source, series_id, baseline_type, json.dumps(baseline), captured_at),
        )
        conn.commit()

    def get_baseline(
        self, source: str, series_id: str, baseline_type: str
    ) -> dict[str, Any] | None:
        conn = self._get_conn()
        row = conn.execute(
            """
            SELECT baseline_json FROM validation_baselines
            WHERE source = ? AND series_id = ? AND baseline_type = ?
            """,
            (source, series_id, baseline_type),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row["baseline_json"])

    # ── History ──────────────────────────────────────────────────────

    def save_check_results(self, checks: list[dict[str, Any]]) -> None:
        if not checks:
            return
        conn = self._get_conn()
        conn.executemany(
            """
            INSERT INTO validation_history
                (source, check_name, layer, passed, severity, message,
                 series_id, timestamp, details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    c.get("source", ""),
                    c["check_name"],
                    c["layer"],
                    1 if c["passed"] else 0,
                    c["severity"],
                    c["message"],
                    c.get("series_id", ""),
                    c.get("timestamp", ""),
                    json.dumps(c.get("details", {})),
                )
                for c in checks
            ],
        )
        conn.commit()

    def get_history(
        self,
        source: str | None = None,
        days: int = 7,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        conn = self._get_conn()
        if source:
            rows = conn.execute(
                """
                SELECT * FROM validation_history
                WHERE source = ?
                  AND timestamp >= datetime('now', ? || ' days')
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (source, f"-{days}", limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM validation_history
                WHERE timestamp >= datetime('now', ? || ' days')
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (f"-{days}", limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
