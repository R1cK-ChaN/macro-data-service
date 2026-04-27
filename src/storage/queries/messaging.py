"""Messaging-domain query helpers for SQLiteEngineStore.

Covers client_profiles + conversation_threads + delivery_queue +
group_profiles/members/messages, plus the local search helpers used by
the delivery-queue scoring path.

Extracted from storage.sqlite in issue #71 Tier 2.1B-2. Methods rely on
the ``self._connection`` context manager defined on the SQLiteEngineStore
base class — composition wires them together via multiple inheritance.
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

from contracts import utc_now
from storage.models.messaging import (
    ClientProfileRecord,
    ConversationMessageRecord,
    DeliveryQueueRecord,
    GroupMemberRecord,
    GroupMessageRecord,
    GroupProfileRecord,
)


class _MessagingQueriesMixin:
    def get_client_profile(self, client_id: str) -> ClientProfileRecord:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                """
                SELECT * FROM client_profiles
                WHERE client_id = ?
                LIMIT 1
                """,
                (client_id,),
            ).fetchone()
        return self._row_to_client_profile(row, client_id=client_id)

    def upsert_client_profile(
        self,
        client_id: str,
        *,
        preferred_language: str | None = None,
        watchlist_topics: list[str] | None = None,
        response_style: str | None = None,
        risk_appetite: str | None = None,
        investment_horizon: str | None = None,
        institution_type: str | None = None,
        risk_preference: str | None = None,
        asset_focus: list[str] | None = None,
        market_focus: list[str] | None = None,
        expertise_level: str | None = None,
        activity: str | None = None,
        current_mood: str | None = None,
        emotional_trend: str | None = None,
        stress_level: str | None = None,
        confidence: str | None = None,
        notes: str | None = None,
        personal_facts: list[str] | None = None,
        last_active_at: str | None = None,
        interaction_increment: int = 0,
    ) -> ClientProfileRecord:
        with self._connection(commit=True) as connection:
            return self._upsert_client_profile_in_connection(
                connection,
                client_id=client_id,
                preferred_language=preferred_language,
                watchlist_topics=watchlist_topics,
                response_style=response_style,
                risk_appetite=risk_appetite,
                investment_horizon=investment_horizon,
                institution_type=institution_type,
                risk_preference=risk_preference,
                asset_focus=asset_focus,
                market_focus=market_focus,
                expertise_level=expertise_level,
                activity=activity,
                current_mood=current_mood,
                emotional_trend=emotional_trend,
                stress_level=stress_level,
                confidence=confidence,
                notes=notes,
                personal_facts=personal_facts,
                last_active_at=last_active_at,
                interaction_increment=interaction_increment,
            )

    def ensure_conversation_thread(self, *, client_id: str, channel: str, thread_id: str) -> None:
        with self._connection(commit=True) as connection:
            self._ensure_conversation_thread_in_connection(
                connection,
                client_id=client_id,
                channel=channel,
                thread_id=thread_id,
            )

    def append_conversation_message(
        self,
        *,
        client_id: str,
        channel: str,
        thread_id: str,
        role: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> ConversationMessageRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            self._ensure_conversation_thread_in_connection(
                connection,
                client_id=client_id,
                channel=channel,
                thread_id=thread_id,
                timestamp=created_at,
            )
            cursor = connection.execute(
                """
                INSERT INTO conversation_messages (
                    client_id,
                    channel,
                    thread_id,
                    role,
                    content,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    client_id,
                    channel,
                    thread_id,
                    role,
                    content,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            message_id = int(cursor.lastrowid)
            connection.execute(
                """
                UPDATE conversation_threads
                SET last_active_at = ?
                WHERE client_id = ? AND channel = ? AND thread_id = ?
                """,
                (created_at, client_id, channel, thread_id),
            )
        return ConversationMessageRecord(
            message_id=message_id,
            client_id=client_id,
            channel=channel,
            thread_id=thread_id,
            role=role,
            content=content,
            created_at=created_at,
            metadata=metadata or {},
        )

    def list_conversation_messages(
        self,
        *,
        client_id: str,
        channel: str,
        thread_id: str,
        limit: int = 12,
    ) -> list[ConversationMessageRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM conversation_messages
                WHERE client_id = ? AND channel = ? AND thread_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (client_id, channel, thread_id, limit),
            ).fetchall()
        records = [
            ConversationMessageRecord(
                message_id=int(row["id"]),
                client_id=row["client_id"],
                channel=row["channel"],
                thread_id=row["thread_id"],
                role=row["role"],
                content=row["content"],
                created_at=row["created_at"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]
        records.reverse()
        return records

    def enqueue_delivery(
        self,
        *,
        client_id: str,
        channel: str,
        thread_id: str,
        source_type: str,
        content_rendered: str,
        source_artifact_id: int | None = None,
        status: str = "delivered",
        delivered_at: str | None = None,
        client_reaction: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> DeliveryQueueRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            self._ensure_conversation_thread_in_connection(
                connection,
                client_id=client_id,
                channel=channel,
                thread_id=thread_id,
                timestamp=created_at,
            )
            cursor = connection.execute(
                """
                INSERT INTO delivery_queue (
                    client_id,
                    channel,
                    thread_id,
                    source_type,
                    source_artifact_id,
                    content_rendered,
                    status,
                    delivered_at,
                    client_reaction,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    client_id,
                    channel,
                    thread_id,
                    source_type,
                    source_artifact_id,
                    content_rendered,
                    status,
                    delivered_at,
                    client_reaction,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            delivery_id = int(cursor.lastrowid)
        return DeliveryQueueRecord(
            delivery_id=delivery_id,
            client_id=client_id,
            channel=channel,
            thread_id=thread_id,
            source_type=source_type,
            source_artifact_id=source_artifact_id,
            content_rendered=content_rendered,
            status=status,
            delivered_at=delivered_at,
            client_reaction=client_reaction,
            created_at=created_at,
            metadata=metadata or {},
        )

    def list_recent_deliveries(
        self,
        *,
        client_id: str,
        channel: str | None = None,
        thread_id: str | None = None,
        limit: int = 5,
    ) -> list[DeliveryQueueRecord]:
        conditions = ["client_id = ?"]
        params: list[Any] = [client_id]
        if channel is not None:
            conditions.append("channel = ?")
            params.append(channel)
        if thread_id is not None:
            conditions.append("thread_id = ?")
            params.append(thread_id)
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM delivery_queue
                WHERE {' AND '.join(conditions)}
                ORDER BY id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [
            DeliveryQueueRecord(
                delivery_id=int(row["id"]),
                client_id=row["client_id"],
                channel=row["channel"],
                thread_id=row["thread_id"],
                source_type=row["source_type"],
                source_artifact_id=int(row["source_artifact_id"]) if row["source_artifact_id"] is not None else None,
                content_rendered=row["content_rendered"],
                status=row["status"],
                delivered_at=row["delivered_at"],
                client_reaction=row["client_reaction"],
                created_at=row["created_at"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    @staticmethod
    def _recency_decay(created_at: str, *, half_life_hours: float = 24.0) -> float:
        """Exponential decay factor: 1.0 for now, 0.5 at half_life_hours ago, etc."""
        try:
            created = datetime.fromisoformat(created_at)
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            age_hours = max((utc_now() - created).total_seconds() / 3600.0, 0.0)
            return math.pow(0.5, age_hours / half_life_hours)
        except (ValueError, TypeError):
            return 0.5

    def search_delivery_queue(
        self,
        *,
        client_id: str,
        query: str,
        channel: str | None = None,
        thread_id: str | None = None,
        limit: int = 3,
    ) -> list[DeliveryQueueRecord]:
        terms = self._search_terms(query)
        candidates = self.list_recent_deliveries(
            client_id=client_id,
            channel=channel,
            thread_id=thread_id,
            limit=max(limit * 12, 50),
        )
        scored: list[tuple[float, DeliveryQueueRecord]] = []
        for item in candidates:
            score = self._score_text_match(item.content_rendered, terms)
            if score <= 0:
                continue
            score *= self._recency_decay(item.created_at)
            scored.append((score, item))
        scored.sort(key=lambda pair: (pair[0], pair[1].created_at), reverse=True)
        return [record for _, record in scored[:limit]]

    def record_sales_interaction(
        self,
        *,
        client_id: str,
        channel: str,
        thread_id: str,
        user_text: str,
        assistant_text: str,
        tool_audit: list[dict[str, Any]],
        profile_updates: dict[str, Any],
    ) -> None:
        user_timestamp = utc_now().isoformat()
        assistant_timestamp = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            self._upsert_client_profile_in_connection(
                connection,
                client_id=client_id,
                preferred_language=profile_updates.get("preferred_language"),
                watchlist_topics=profile_updates.get("watchlist_topics"),
                response_style=profile_updates.get("response_style"),
                risk_appetite=profile_updates.get("risk_appetite"),
                investment_horizon=profile_updates.get("investment_horizon"),
                institution_type=profile_updates.get("institution_type"),
                risk_preference=profile_updates.get("risk_preference"),
                asset_focus=profile_updates.get("asset_focus"),
                market_focus=profile_updates.get("market_focus"),
                expertise_level=profile_updates.get("expertise_level"),
                activity=profile_updates.get("activity"),
                current_mood=profile_updates.get("current_mood"),
                emotional_trend=profile_updates.get("emotional_trend"),
                stress_level=profile_updates.get("stress_level"),
                confidence=profile_updates.get("confidence"),
                notes=profile_updates.get("notes"),
                personal_facts=profile_updates.get("personal_facts"),
                last_active_at=assistant_timestamp,
                interaction_increment=1,
            )
            self._ensure_conversation_thread_in_connection(
                connection,
                client_id=client_id,
                channel=channel,
                thread_id=thread_id,
                timestamp=assistant_timestamp,
            )
            connection.executemany(
                """
                INSERT INTO conversation_messages (
                    client_id,
                    channel,
                    thread_id,
                    role,
                    content,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        client_id,
                        channel,
                        thread_id,
                        "user",
                        user_text,
                        json.dumps({"channel": channel}, ensure_ascii=False, sort_keys=True),
                        user_timestamp,
                    ),
                    (
                        client_id,
                        channel,
                        thread_id,
                        "assistant",
                        assistant_text,
                        json.dumps(
                            {"channel": channel, "tool_audit": tool_audit},
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        assistant_timestamp,
                    ),
                ],
            )
            connection.execute(
                """
                INSERT INTO delivery_queue (
                    client_id,
                    channel,
                    thread_id,
                    source_type,
                    source_artifact_id,
                    content_rendered,
                    status,
                    delivered_at,
                    client_reaction,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    client_id,
                    channel,
                    thread_id,
                    "sales_reply",
                    None,
                    assistant_text,
                    "delivered",
                    assistant_timestamp,
                    "",
                    json.dumps(
                        {"user_text": user_text, "tool_audit": tool_audit},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    assistant_timestamp,
                ),
            )

    def _row_to_client_profile(self, row: sqlite3.Row | None, *, client_id: str) -> ClientProfileRecord:
        if row is None:
            return ClientProfileRecord(
                client_id=client_id,
                preferred_language="",
                watchlist_topics=[],
                response_style="",
                risk_appetite="",
                investment_horizon="",
                institution_type="",
                risk_preference="",
                asset_focus=[],
                market_focus=[],
                expertise_level="",
                activity="",
                current_mood="",
                emotional_trend="",
                stress_level="",
                confidence="",
                notes="",
                personal_facts=[],
                last_active_at="",
                total_interactions=0,
                updated_at="",
            )
        return ClientProfileRecord(
            client_id=row["client_id"],
            preferred_language=row["preferred_language"],
            watchlist_topics=json.loads(row["watchlist_topics_json"]),
            response_style=row["response_style"],
            risk_appetite=row["risk_appetite"],
            investment_horizon=row["investment_horizon"],
            institution_type=row["institution_type"],
            risk_preference=row["risk_preference"],
            asset_focus=json.loads(row["asset_focus_json"]),
            market_focus=json.loads(row["market_focus_json"]),
            expertise_level=row["expertise_level"],
            activity=row["activity"],
            current_mood=row["current_mood"],
            emotional_trend=row["emotional_trend"],
            stress_level=row["stress_level"],
            confidence=row["confidence"],
            notes=row["notes"],
            personal_facts=json.loads(row["personal_facts_json"]),
            last_active_at=row["last_active_at"],
            total_interactions=int(row["total_interactions"]),
            updated_at=row["updated_at"],
        )

    def _get_client_profile_in_connection(
        self,
        connection: sqlite3.Connection,
        *,
        client_id: str,
    ) -> ClientProfileRecord:
        row = connection.execute(
            """
            SELECT * FROM client_profiles
            WHERE client_id = ?
            LIMIT 1
            """,
            (client_id,),
        ).fetchone()
        return self._row_to_client_profile(row, client_id=client_id)

    def _upsert_client_profile_in_connection(
        self,
        connection: sqlite3.Connection,
        *,
        client_id: str,
        preferred_language: str | None = None,
        watchlist_topics: list[str] | None = None,
        response_style: str | None = None,
        risk_appetite: str | None = None,
        investment_horizon: str | None = None,
        institution_type: str | None = None,
        risk_preference: str | None = None,
        asset_focus: list[str] | None = None,
        market_focus: list[str] | None = None,
        expertise_level: str | None = None,
        activity: str | None = None,
        current_mood: str | None = None,
        emotional_trend: str | None = None,
        stress_level: str | None = None,
        confidence: str | None = None,
        notes: str | None = None,
        personal_facts: list[str] | None = None,
        last_active_at: str | None = None,
        interaction_increment: int = 0,
    ) -> ClientProfileRecord:
        current = self._get_client_profile_in_connection(connection, client_id=client_id)
        merged_topics = current.watchlist_topics
        if watchlist_topics:
            merged_topics = sorted(set(current.watchlist_topics).union(watchlist_topics))
        merged_asset_focus = current.asset_focus
        if asset_focus:
            merged_asset_focus = sorted(set(current.asset_focus).union(asset_focus))
        merged_market_focus = current.market_focus
        if market_focus:
            merged_market_focus = sorted(set(current.market_focus).union(market_focus))
        merged_personal_facts = current.personal_facts
        if personal_facts:
            # Dedup by last occurrence so re-mentioned facts refresh recency.
            combined = [*current.personal_facts, *personal_facts]
            seen: set[str] = set()
            deduped: list[str] = []
            for item in reversed(combined):
                if item not in seen:
                    seen.add(item)
                    deduped.append(item)
            deduped.reverse()
            merged_personal_facts = deduped[-20:]
        next_language = preferred_language if preferred_language is not None else current.preferred_language
        next_response_style = response_style if response_style is not None else current.response_style
        next_risk_appetite = risk_appetite if risk_appetite is not None else current.risk_appetite
        next_investment_horizon = (
            investment_horizon if investment_horizon is not None else current.investment_horizon
        )
        next_institution_type = institution_type if institution_type is not None else current.institution_type
        next_risk_preference = risk_preference if risk_preference is not None else current.risk_preference
        next_expertise_level = expertise_level if expertise_level is not None else current.expertise_level
        next_activity = activity if activity is not None else current.activity
        next_current_mood = current_mood if current_mood is not None else current.current_mood
        next_emotional_trend = emotional_trend if emotional_trend is not None else current.emotional_trend
        next_stress_level = stress_level if stress_level is not None else current.stress_level
        next_confidence = confidence if confidence is not None else current.confidence
        next_notes = notes if notes is not None else current.notes
        next_last_active = last_active_at if last_active_at is not None else current.last_active_at
        updated_at = utc_now().isoformat()
        total_interactions = current.total_interactions + interaction_increment
        connection.execute(
            """
            INSERT INTO client_profiles (
                client_id,
                preferred_language,
                watchlist_topics_json,
                response_style,
                risk_appetite,
                investment_horizon,
                institution_type,
                risk_preference,
                asset_focus_json,
                market_focus_json,
                expertise_level,
                activity,
                current_mood,
                emotional_trend,
                stress_level,
                confidence,
                notes,
                personal_facts_json,
                last_active_at,
                total_interactions,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(client_id) DO UPDATE SET
                preferred_language = excluded.preferred_language,
                watchlist_topics_json = excluded.watchlist_topics_json,
                response_style = excluded.response_style,
                risk_appetite = excluded.risk_appetite,
                investment_horizon = excluded.investment_horizon,
                institution_type = excluded.institution_type,
                risk_preference = excluded.risk_preference,
                asset_focus_json = excluded.asset_focus_json,
                market_focus_json = excluded.market_focus_json,
                expertise_level = excluded.expertise_level,
                activity = excluded.activity,
                current_mood = excluded.current_mood,
                emotional_trend = excluded.emotional_trend,
                stress_level = excluded.stress_level,
                confidence = excluded.confidence,
                notes = excluded.notes,
                personal_facts_json = excluded.personal_facts_json,
                last_active_at = excluded.last_active_at,
                total_interactions = excluded.total_interactions,
                updated_at = excluded.updated_at
            """,
            (
                client_id,
                next_language,
                json.dumps(merged_topics, ensure_ascii=False, sort_keys=True),
                next_response_style,
                next_risk_appetite,
                next_investment_horizon,
                next_institution_type,
                next_risk_preference,
                json.dumps(merged_asset_focus, ensure_ascii=False, sort_keys=True),
                json.dumps(merged_market_focus, ensure_ascii=False, sort_keys=True),
                next_expertise_level,
                next_activity,
                next_current_mood,
                next_emotional_trend,
                next_stress_level,
                next_confidence,
                next_notes,
                json.dumps(merged_personal_facts, ensure_ascii=False, sort_keys=True),
                next_last_active,
                total_interactions,
                updated_at,
            ),
        )
        return ClientProfileRecord(
            client_id=client_id,
            preferred_language=next_language,
            watchlist_topics=merged_topics,
            response_style=next_response_style,
            risk_appetite=next_risk_appetite,
            investment_horizon=next_investment_horizon,
            institution_type=next_institution_type,
            risk_preference=next_risk_preference,
            asset_focus=merged_asset_focus,
            market_focus=merged_market_focus,
            expertise_level=next_expertise_level,
            activity=next_activity,
            current_mood=next_current_mood,
            emotional_trend=next_emotional_trend,
            stress_level=next_stress_level,
            confidence=next_confidence,
            notes=next_notes,
            personal_facts=merged_personal_facts,
            last_active_at=next_last_active,
            total_interactions=total_interactions,
            updated_at=updated_at,
        )

    def _ensure_conversation_thread_in_connection(
        self,
        connection: sqlite3.Connection,
        *,
        client_id: str,
        channel: str,
        thread_id: str,
        timestamp: str | None = None,
    ) -> None:
        active_at = timestamp or utc_now().isoformat()
        connection.execute(
            """
            INSERT INTO conversation_threads (
                client_id,
                channel,
                thread_id,
                opened_at,
                last_active_at,
                status
            ) VALUES (?, ?, ?, ?, ?, 'active')
            ON CONFLICT(client_id, channel, thread_id) DO UPDATE SET
                last_active_at = excluded.last_active_at,
                status = 'active'
            """,
            (client_id, channel, thread_id, active_at, active_at),
        )

    def _search_terms(self, query: str) -> list[str]:
        terms: list[str] = []
        for token in re.findall(r"[A-Za-z0-9_]+|[\u4e00-\u9fff]+", query):
            cleaned = token.strip()
            if len(cleaned) < 2:
                continue
            normalized = cleaned.casefold()
            terms.append(normalized)
            if re.fullmatch(r"[\u4e00-\u9fff]+", cleaned) and len(cleaned) > 2:
                terms.extend(cleaned[index : index + 2] for index in range(len(cleaned) - 1))
        if not terms and query.strip():
            fallback = query.casefold().strip()
            if len(fallback) >= 2:
                terms.append(fallback)
        return list(dict.fromkeys(terms))

    def _score_text_match(self, haystack: str, terms: list[str]) -> float:
        if not terms:
            return 0.0
        normalized = haystack.casefold()
        score = 0.0
        for term in terms:
            score += float(normalized.count(term))
        return score

    def upsert_group_profile(
        self,
        *,
        group_id: str,
        group_name: str = "",
        group_topic: str = "",
        group_notes: str = "",
        member_count: int = 0,
    ) -> None:
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO group_profiles (group_id, group_name, group_topic, group_notes, member_count, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                    group_name = CASE WHEN excluded.group_name != '' THEN excluded.group_name ELSE group_profiles.group_name END,
                    group_topic = CASE WHEN excluded.group_topic != '' THEN excluded.group_topic ELSE group_profiles.group_topic END,
                    group_notes = CASE WHEN excluded.group_notes != '' THEN excluded.group_notes ELSE group_profiles.group_notes END,
                    member_count = CASE WHEN excluded.member_count > 0 THEN excluded.member_count ELSE group_profiles.member_count END,
                    updated_at = excluded.updated_at
                """,
                (group_id, group_name, group_topic, group_notes, member_count, now, now),
            )

    def get_group_profile(self, group_id: str) -> GroupProfileRecord:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM group_profiles WHERE group_id = ? LIMIT 1",
                (group_id,),
            ).fetchone()
        if row is None:
            now = utc_now().isoformat()
            return GroupProfileRecord(
                group_id=group_id,
                group_name="",
                group_topic="",
                group_notes="",
                member_count=0,
                created_at=now,
                updated_at=now,
            )
        return GroupProfileRecord(
            group_id=row["group_id"],
            group_name=row["group_name"],
            group_topic=row["group_topic"],
            group_notes=row["group_notes"],
            member_count=row["member_count"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def upsert_group_member(
        self,
        *,
        group_id: str,
        user_id: str,
        display_name: str = "",
        role_in_group: str = "",
        personality_notes: str = "",
    ) -> None:
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO group_members (group_id, user_id, display_name, role_in_group, personality_notes, first_seen_at, last_seen_at, message_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(group_id, user_id) DO UPDATE SET
                    display_name = CASE WHEN excluded.display_name != '' THEN excluded.display_name ELSE group_members.display_name END,
                    role_in_group = CASE WHEN excluded.role_in_group != '' THEN excluded.role_in_group ELSE group_members.role_in_group END,
                    personality_notes = CASE WHEN excluded.personality_notes != '' THEN excluded.personality_notes ELSE group_members.personality_notes END,
                    last_seen_at = excluded.last_seen_at,
                    message_count = group_members.message_count + 1
                """,
                (group_id, user_id, display_name, role_in_group, personality_notes, now, now),
            )

    def list_group_members(self, group_id: str, *, limit: int = 20) -> list[GroupMemberRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT * FROM group_members WHERE group_id = ? ORDER BY last_seen_at DESC LIMIT ?",
                (group_id, limit),
            ).fetchall()
        return [
            GroupMemberRecord(
                group_id=row["group_id"],
                user_id=row["user_id"],
                display_name=row["display_name"],
                role_in_group=row["role_in_group"],
                personality_notes=row["personality_notes"],
                first_seen_at=row["first_seen_at"],
                last_seen_at=row["last_seen_at"],
                message_count=row["message_count"],
            )
            for row in rows
        ]

    def append_group_message(
        self,
        *,
        group_id: str,
        thread_id: str = "main",
        user_id: str,
        display_name: str,
        content: str,
    ) -> None:
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO group_messages (group_id, thread_id, user_id, display_name, content, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (group_id, thread_id, user_id, display_name, content, now),
            )

    def list_group_messages(
        self,
        group_id: str,
        thread_id: str = "main",
        *,
        limit: int = 30,
    ) -> list[GroupMessageRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT id, group_id, thread_id, user_id, display_name, content, created_at
                FROM group_messages
                WHERE group_id = ? AND thread_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (group_id, thread_id, limit),
            ).fetchall()
        records = [
            GroupMessageRecord(
                message_id=row["id"],
                group_id=row["group_id"],
                thread_id=row["thread_id"],
                user_id=row["user_id"],
                display_name=row["display_name"],
                content=row["content"],
                created_at=row["created_at"],
            )
            for row in rows
        ]
        records.reverse()  # chronological order
        return records
