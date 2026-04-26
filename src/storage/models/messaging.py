"""Storage records — client + conversation + delivery + group-chat records.

Extracted out of src/storage/sqlite.py as part of issue #58 Tier 2.1A —
pure mechanical split, no behavior change. The records are re-exported by
storage.sqlite for backwards compatibility, so existing
``from storage.sqlite import XRecord`` consumers keep working.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ClientProfileRecord:
    client_id: str
    preferred_language: str
    watchlist_topics: list[str]
    response_style: str
    risk_appetite: str
    investment_horizon: str
    institution_type: str
    risk_preference: str
    asset_focus: list[str]
    market_focus: list[str]
    expertise_level: str
    activity: str
    current_mood: str
    emotional_trend: str
    stress_level: str
    confidence: str
    notes: str
    personal_facts: list[str]
    last_active_at: str
    total_interactions: int
    updated_at: str


@dataclass(frozen=True)
class ConversationMessageRecord:
    message_id: int
    client_id: str
    channel: str
    thread_id: str
    role: str
    content: str
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DeliveryQueueRecord:
    delivery_id: int
    client_id: str
    channel: str
    thread_id: str
    source_type: str
    source_artifact_id: int | None
    content_rendered: str
    status: str
    delivered_at: str | None
    client_reaction: str
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GroupProfileRecord:
    group_id: str
    group_name: str
    group_topic: str
    group_notes: str
    member_count: int
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class GroupMemberRecord:
    group_id: str
    user_id: str
    display_name: str
    role_in_group: str
    personality_notes: str
    first_seen_at: str
    last_seen_at: str
    message_count: int


@dataclass(frozen=True)
class GroupMessageRecord:
    message_id: int
    group_id: str
    thread_id: str
    user_id: str
    display_name: str
    content: str
    created_at: str
