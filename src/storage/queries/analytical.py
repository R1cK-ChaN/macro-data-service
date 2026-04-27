"""Analytical-domain query helpers for SQLiteEngineStore.

Extracted from storage.sqlite in issue #71 Tier 2.1B-2. Methods rely on
the ``self._connection`` context manager defined on the SQLiteEngineStore
base class — composition wires them together via multiple inheritance.
"""

from __future__ import annotations

import json
from typing import Any

from contracts import utc_now
from storage.models.analytical import (
    AnalyticalObservationRecord,
    GeneratedNoteRecord,
    RegimeSnapshotRecord,
    ResearchArtifactRecord,
)
from storage.queries.calendar import _matches_scope_tags


class _AnalyticalQueriesMixin:
    def save_regime_snapshot(self, regime_json: dict[str, Any], trigger_event: str, summary: str) -> RegimeSnapshotRecord:
        timestamp = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO regime_snapshots (
                    timestamp,
                    regime_json,
                    trigger_event,
                    summary
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    timestamp,
                    json.dumps(regime_json, ensure_ascii=False, sort_keys=True),
                    trigger_event,
                    summary,
                ),
            )
            snapshot_id = int(cursor.lastrowid)
        return RegimeSnapshotRecord(
            snapshot_id=snapshot_id,
            timestamp=timestamp,
            regime_json=regime_json,
            trigger_event=trigger_event,
            summary=summary,
        )

    def save_generated_note(
        self,
        note_type: str,
        title: str,
        summary: str,
        body_markdown: str,
        regime_json: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> GeneratedNoteRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO generated_notes (
                    created_at,
                    note_type,
                    title,
                    summary,
                    body_markdown,
                    regime_json,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,
                    note_type,
                    title,
                    summary,
                    body_markdown,
                    json.dumps(regime_json, ensure_ascii=False, sort_keys=True) if regime_json else None,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                ),
            )
            note_id = int(cursor.lastrowid)
        return GeneratedNoteRecord(
            note_id=note_id,
            created_at=created_at,
            note_type=note_type,
            title=title,
            summary=summary,
            body_markdown=body_markdown,
            regime_json=regime_json,
            metadata=metadata or {},
        )

    def list_recent_regime_snapshots(self, *, limit: int = 3) -> list[RegimeSnapshotRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM regime_snapshots
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            RegimeSnapshotRecord(
                snapshot_id=int(row["id"]),
                timestamp=row["timestamp"],
                regime_json=json.loads(row["regime_json"]),
                trigger_event=row["trigger_event"],
                summary=row["summary"],
            )
            for row in rows
        ]

    def list_recent_generated_notes(
        self,
        *,
        limit: int = 5,
        note_type: str | None = None,
    ) -> list[GeneratedNoteRecord]:
        conditions: list[str] = []
        params: list[Any] = []
        if note_type:
            conditions.append("note_type = ?")
            params.append(note_type)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM generated_notes
                {where_clause}
                ORDER BY id DESC
                LIMIT ?
                """.format(where_clause=where_clause),
                [*params, limit],
            ).fetchall()
        return [
            GeneratedNoteRecord(
                note_id=int(row["id"]),
                created_at=row["created_at"],
                note_type=row["note_type"],
                title=row["title"],
                summary=row["summary"],
                body_markdown=row["body_markdown"],
                regime_json=json.loads(row["regime_json"]) if row["regime_json"] else None,
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    def add_analytical_observation(
        self,
        *,
        observation_type: str,
        summary: str,
        detail: str,
        source_kind: str,
        source_id: int,
        metadata: dict[str, Any] | None = None,
    ) -> AnalyticalObservationRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO analytical_observations (
                    observation_type,
                    summary,
                    detail,
                    source_kind,
                    source_id,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observation_type,
                    summary,
                    detail,
                    source_kind,
                    source_id,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            observation_id = int(cursor.lastrowid)
        return AnalyticalObservationRecord(
            observation_id=observation_id,
            observation_type=observation_type,
            summary=summary,
            detail=detail,
            source_kind=source_kind,
            source_id=source_id,
            created_at=created_at,
            metadata=metadata or {},
        )

    def list_recent_analytical_observations(self, *, limit: int = 5) -> list[AnalyticalObservationRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM analytical_observations
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            AnalyticalObservationRecord(
                observation_id=int(row["id"]),
                observation_type=row["observation_type"],
                summary=row["summary"],
                detail=row["detail"],
                source_kind=row["source_kind"],
                source_id=int(row["source_id"]),
                created_at=row["created_at"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    def list_tagged_observations(self, *, tags: list[str], limit: int = 4) -> list[AnalyticalObservationRecord]:
        if not tags:
            return self.list_recent_analytical_observations(limit=limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM analytical_observations
                ORDER BY id DESC
                """,
            ).fetchall()
        matched: list[AnalyticalObservationRecord] = []
        for row in rows:
            if not _matches_scope_tags(row["summary"], tags):
                continue
            matched.append(
                AnalyticalObservationRecord(
                    observation_id=int(row["id"]),
                    observation_type=row["observation_type"],
                    summary=row["summary"],
                    detail=row["detail"],
                    source_kind=row["source_kind"],
                    source_id=int(row["source_id"]),
                    created_at=row["created_at"],
                    metadata=json.loads(row["metadata_json"]),
                )
            )
            if len(matched) >= limit:
                break
        return matched

    def list_tagged_regime_snapshots(self, *, tags: list[str], limit: int = 2) -> list[RegimeSnapshotRecord]:
        if not tags:
            return self.list_recent_regime_snapshots(limit=limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM regime_snapshots
                ORDER BY id DESC
                """,
            ).fetchall()
        matched: list[RegimeSnapshotRecord] = []
        for row in rows:
            if not _matches_scope_tags(row["summary"], tags):
                continue
            matched.append(
                RegimeSnapshotRecord(
                    snapshot_id=int(row["id"]),
                    timestamp=row["timestamp"],
                    regime_json=json.loads(row["regime_json"]),
                    trigger_event=row["trigger_event"],
                    summary=row["summary"],
                )
            )
            if len(matched) >= limit:
                break
        return matched

    def save_subagent_run(
        self,
        *,
        task_id: str,
        parent_agent: str,
        task_type: str,
        objective: str,
        scope_tags: list[str],
        status: str,
        summary: str,
        elapsed_seconds: float,
    ) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO subagent_runs (
                    task_id, parent_agent, task_type, objective,
                    scope_tags_json, status, summary, elapsed_seconds, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    parent_agent,
                    task_type,
                    objective,
                    json.dumps(scope_tags, ensure_ascii=False),
                    status,
                    summary,
                    elapsed_seconds,
                    utc_now().isoformat(),
                ),
            )

    def list_recent_subagent_runs(
        self,
        *,
        parent_agent: str | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        with self._connection(commit=False) as connection:
            if parent_agent:
                rows = connection.execute(
                    "SELECT * FROM subagent_runs WHERE parent_agent = ? ORDER BY id DESC LIMIT ?",
                    (parent_agent, limit),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM subagent_runs ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [
            {
                "id": row["id"],
                "task_id": row["task_id"],
                "parent_agent": row["parent_agent"],
                "task_type": row["task_type"],
                "objective": row["objective"],
                "scope_tags": json.loads(row["scope_tags_json"]),
                "status": row["status"],
                "summary": row["summary"],
                "elapsed_seconds": row["elapsed_seconds"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def publish_research_artifact(
        self,
        *,
        artifact_type: str,
        title: str,
        summary: str,
        content_markdown: str,
        source_kind: str,
        source_id: int,
        tags: list[str],
        metadata: dict[str, Any] | None = None,
    ) -> ResearchArtifactRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO research_artifacts (
                    artifact_type,
                    title,
                    summary,
                    content_markdown,
                    source_kind,
                    source_id,
                    tags_json,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_type,
                    title,
                    summary,
                    content_markdown,
                    source_kind,
                    source_id,
                    json.dumps(tags, ensure_ascii=False, sort_keys=True),
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            artifact_id = int(cursor.lastrowid)
        return ResearchArtifactRecord(
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            title=title,
            summary=summary,
            content_markdown=content_markdown,
            source_kind=source_kind,
            source_id=source_id,
            created_at=created_at,
            tags=tags,
            metadata=metadata or {},
        )

    def list_recent_research_artifacts(
        self,
        *,
        limit: int = 5,
        artifact_types: tuple[str, ...] = (),
    ) -> list[ResearchArtifactRecord]:
        conditions: list[str] = []
        params: list[Any] = []
        if artifact_types:
            conditions.append("artifact_type IN (" + ",".join("?" for _ in artifact_types) + ")")
            params.extend(artifact_types)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM research_artifacts
                {where_clause}
                ORDER BY id DESC
                LIMIT ?
                """.format(where_clause=where_clause),
                [*params, limit],
            ).fetchall()
        return [
            ResearchArtifactRecord(
                artifact_id=int(row["id"]),
                artifact_type=row["artifact_type"],
                title=row["title"],
                summary=row["summary"],
                content_markdown=row["content_markdown"],
                source_kind=row["source_kind"],
                source_id=int(row["source_id"]),
                created_at=row["created_at"],
                tags=json.loads(row["tags_json"]),
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    def search_research_artifacts(
        self,
        *,
        query: str,
        limit: int = 5,
        artifact_types: tuple[str, ...] = (),
    ) -> list[ResearchArtifactRecord]:
        terms = self._search_terms(query)
        candidates = self.list_recent_research_artifacts(limit=max(limit * 20, 100), artifact_types=artifact_types)
        scored: list[tuple[float, ResearchArtifactRecord]] = []
        for artifact in candidates:
            haystack = " ".join([artifact.title, artifact.summary, artifact.content_markdown])
            score = self._score_text_match(haystack, terms)
            if score <= 0:
                continue
            scored.append((score, artifact))
        scored.sort(key=lambda item: (item[0], item[1].created_at), reverse=True)
        return [record for _, record in scored[:limit]]

    def latest_regime_snapshot(self) -> RegimeSnapshotRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                """
                SELECT * FROM regime_snapshots
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        return RegimeSnapshotRecord(
            snapshot_id=int(row["id"]),
            timestamp=row["timestamp"],
            regime_json=json.loads(row["regime_json"]),
            trigger_event=row["trigger_event"],
            summary=row["summary"],
        )
