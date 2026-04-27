"""Trading-domain query helpers for SQLiteEngineStore.

Extracted from storage.sqlite in issue #71 Tier 2.1B-2. Methods rely on
the ``self._connection`` context manager defined on the SQLiteEngineStore
base class — composition wires them together via multiple inheritance.
"""

from __future__ import annotations

import json
from typing import Any

from contracts import utc_now
from storage.models.trading import (
    DecisionLogRecord,
    PerformanceRecord,
    PositionStateRecord,
    TradeSignalRecord,
    TradingArtifactRecord,
)


class _TradingQueriesMixin:
    def save_trade_signal(
        self,
        *,
        signal_type: str,
        title: str,
        summary: str,
        rationale_markdown: str,
        signal: dict[str, Any],
        confidence: float,
        metadata: dict[str, Any] | None = None,
    ) -> TradeSignalRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO trade_signals (
                    signal_type,
                    title,
                    summary,
                    rationale_markdown,
                    signal_json,
                    confidence,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_type,
                    title,
                    summary,
                    rationale_markdown,
                    json.dumps(signal, ensure_ascii=False, sort_keys=True),
                    confidence,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            signal_id = int(cursor.lastrowid)
        return TradeSignalRecord(
            signal_id=signal_id,
            signal_type=signal_type,
            title=title,
            summary=summary,
            rationale_markdown=rationale_markdown,
            signal=signal,
            confidence=confidence,
            created_at=created_at,
            metadata=metadata or {},
        )

    def log_trading_decision(
        self,
        *,
        decision_type: str,
        title: str,
        summary: str,
        rationale_markdown: str,
        research_artifact_id: int | None,
        signal_id: int | None,
        metadata: dict[str, Any] | None = None,
    ) -> DecisionLogRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO decision_log (
                    decision_type,
                    title,
                    summary,
                    rationale_markdown,
                    research_artifact_id,
                    signal_id,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_type,
                    title,
                    summary,
                    rationale_markdown,
                    research_artifact_id,
                    signal_id,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            decision_id = int(cursor.lastrowid)
        return DecisionLogRecord(
            decision_id=decision_id,
            decision_type=decision_type,
            title=title,
            summary=summary,
            rationale_markdown=rationale_markdown,
            research_artifact_id=research_artifact_id,
            signal_id=signal_id,
            created_at=created_at,
            metadata=metadata or {},
        )

    def list_recent_decisions(self, *, limit: int = 5) -> list[DecisionLogRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM decision_log
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            DecisionLogRecord(
                decision_id=int(row["id"]),
                decision_type=row["decision_type"],
                title=row["title"],
                summary=row["summary"],
                rationale_markdown=row["rationale_markdown"],
                research_artifact_id=int(row["research_artifact_id"]) if row["research_artifact_id"] is not None else None,
                signal_id=int(row["signal_id"]) if row["signal_id"] is not None else None,
                created_at=row["created_at"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    def upsert_position_state(
        self,
        *,
        symbol: str,
        exposure: float,
        direction: str,
        thesis: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO position_state (
                    symbol,
                    exposure,
                    direction,
                    thesis,
                    metadata_json,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    exposure = excluded.exposure,
                    direction = excluded.direction,
                    thesis = excluded.thesis,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    symbol,
                    exposure,
                    direction,
                    thesis,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    utc_now().isoformat(),
                ),
            )

    def list_position_state(self, *, limit: int = 10) -> list[PositionStateRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM position_state
                ORDER BY updated_at DESC, symbol ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            PositionStateRecord(
                symbol=row["symbol"],
                exposure=float(row["exposure"]),
                direction=row["direction"],
                thesis=row["thesis"],
                updated_at=row["updated_at"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    def record_performance(
        self,
        *,
        metric_name: str,
        metric_value: float,
        period_label: str,
        metadata: dict[str, Any] | None = None,
    ) -> PerformanceRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO performance_records (
                    metric_name,
                    metric_value,
                    period_label,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    metric_name,
                    metric_value,
                    period_label,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            record_id = int(cursor.lastrowid)
        return PerformanceRecord(
            record_id=record_id,
            metric_name=metric_name,
            metric_value=metric_value,
            period_label=period_label,
            created_at=created_at,
            metadata=metadata or {},
        )

    def list_recent_performance_records(self, *, limit: int = 5) -> list[PerformanceRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM performance_records
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            PerformanceRecord(
                record_id=int(row["id"]),
                metric_name=row["metric_name"],
                metric_value=float(row["metric_value"]),
                period_label=row["period_label"],
                created_at=row["created_at"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    def publish_trading_artifact(
        self,
        *,
        artifact_type: str,
        title: str,
        summary: str,
        rationale_markdown: str,
        research_artifact_id: int,
        signal: dict[str, Any],
        confidence: float,
        decision_log_id: int | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TradingArtifactRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO trading_artifacts (
                    artifact_type,
                    title,
                    summary,
                    rationale_markdown,
                    research_artifact_id,
                    decision_log_id,
                    signal_json,
                    confidence,
                    tags_json,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_type,
                    title,
                    summary,
                    rationale_markdown,
                    research_artifact_id,
                    decision_log_id,
                    json.dumps(signal, ensure_ascii=False, sort_keys=True),
                    confidence,
                    json.dumps(tags or [], ensure_ascii=False, sort_keys=True),
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            artifact_id = int(cursor.lastrowid)
        return TradingArtifactRecord(
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            title=title,
            summary=summary,
            rationale_markdown=rationale_markdown,
            research_artifact_id=research_artifact_id,
            decision_log_id=decision_log_id,
            signal=signal,
            confidence=confidence,
            created_at=created_at,
            tags=tags or [],
            metadata=metadata or {},
        )

    def list_recent_trading_artifacts(self, *, limit: int = 5) -> list[TradingArtifactRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM trading_artifacts
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            TradingArtifactRecord(
                artifact_id=int(row["id"]),
                artifact_type=row["artifact_type"],
                title=row["title"],
                summary=row["summary"],
                rationale_markdown=row["rationale_markdown"],
                research_artifact_id=int(row["research_artifact_id"]),
                decision_log_id=int(row["decision_log_id"]) if row["decision_log_id"] is not None else None,
                signal=json.loads(row["signal_json"]),
                confidence=float(row["confidence"]),
                created_at=row["created_at"],
                tags=json.loads(row["tags_json"]),
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]
