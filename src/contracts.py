from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone, tzinfo
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def epoch_to_datetime(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def epoch_ms_to_datetime(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)


def format_epoch(ts: int) -> str:
    """'2026-03-08 14:30' — for CLI display."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def format_epoch_iso(ts: int) -> str:
    """'2026-03-08T14:30:00+00:00' — for LLM-readable dicts and prompts."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def format_epoch_ms_iso(ts_ms: int) -> str:
    """'2026-03-08T14:30:00+00:00' — for milliseconds-based UTC timestamps."""
    return epoch_ms_to_datetime(ts_ms).isoformat()


def _coerce_utc_datetime(value: str | datetime, *, default_tz: tzinfo = timezone.utc) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = value.strip()
        if not text:
            raise ValueError("timestamp is empty")
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                try:
                    parsed = datetime.strptime(text, pattern)
                    break
                except ValueError:
                    continue
            else:
                raise
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=default_tz)
    return parsed.astimezone(timezone.utc)


def normalize_utc_iso(value: str | datetime, *, default_tz: tzinfo = timezone.utc) -> str:
    return _coerce_utc_datetime(value, default_tz=default_tz).isoformat()


def to_epoch_ms(value: str | datetime, *, default_tz: tzinfo = timezone.utc) -> int:
    return int(_coerce_utc_datetime(value, default_tz=default_tz).timestamp() * 1000)


def format_epoch_iso_in_timezone(ts: int, timezone_name: str, *, milliseconds: bool = False) -> str:
    try:
        zone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown timezone: {timezone_name}") from exc
    moment = epoch_ms_to_datetime(ts) if milliseconds else epoch_to_datetime(ts)
    return moment.astimezone(zone).isoformat()


class Serializable:
    def to_dict(self) -> dict[str, Any]:
        return _serialize_value(asdict(self))


def _serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return _serialize_value(asdict(value))
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    return value


class Importance(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class InteractionMode(str, Enum):
    QA = "qa"
    DRAFT = "draft"
    FOLLOW_UP = "follow_up"
    MEETING_PREP = "meeting_prep"
    REGIME = "regime"
    CALENDAR = "calendar"
    PREMARKET = "premarket"


@dataclass(frozen=True)
class SourceReference(Serializable):
    title: str
    url: str
    source: str
    excerpt: str = ""


@dataclass(frozen=True)
class Event(Serializable):
    event_id: str
    timestamp: datetime
    source: str
    source_type: str
    category: str
    title: str
    summary: str
    country: str
    importance: Importance = Importance.MEDIUM
    actual: str | None = None
    forecast: str | None = None
    previous: str | None = None
    surprise: str | None = None
    tags: list[str] = field(default_factory=list)
    references: list[SourceReference] = field(default_factory=list)


@dataclass(frozen=True)
class CalendarItem(Serializable):
    event_id: str
    release_time: datetime
    indicator: str
    country: str
    importance: Importance
    expected: str | None = None
    previous: str | None = None
    notes: str = ""
    references: list[SourceReference] = field(default_factory=list)


@dataclass(frozen=True)
class MarketSnapshot(Serializable):
    as_of: datetime
    focus: str
    headline_summary: list[str]
    key_events: list[Event]
    market_prices: dict[str, float]
    citations: list[SourceReference] = field(default_factory=list)


@dataclass(frozen=True)
class RegimeScore(Serializable):
    axis: str
    score: float
    label: str
    rationale: str


@dataclass(frozen=True)
class RegimeState(Serializable):
    as_of: datetime
    summary: str
    scores: list[RegimeScore]
    evidence: list[Event]
    confidence: float


@dataclass(frozen=True)
class ResearchNote(Serializable):
    note_id: str
    created_at: datetime
    note_type: str
    title: str
    summary: str
    body_markdown: str
    regime_state: RegimeState | None = None
    citations: list[SourceReference] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DraftResponse(Serializable):
    request_id: str
    created_at: datetime
    mode: InteractionMode
    audience: str
    markdown: str
    plain_text: str
    disclaimer: str = ""
    citations: list[SourceReference] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ChannelMessage(Serializable):
    message_id: str
    channel: str
    mode: InteractionMode
    markdown: str
    plain_text: str
    citations: list[SourceReference] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)
