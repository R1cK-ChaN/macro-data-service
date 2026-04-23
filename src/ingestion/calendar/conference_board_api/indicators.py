"""Conference Board calendar-event metadata whitelist."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConferenceBoardIndicatorSpec:
    """Downstream-shape metadata for one Conference Board indicator."""

    series_id: str
    indicator: str
    country_code: str
    title: str
    unit: str
    importance: str
    category: str
    source_url: str
    schedule_url: str
    schedule_title_fragment: str
    reference_month_lag: int
    value_fetch: bool = True


CONFERENCE_BOARD_CALENDAR_URL = (
    "https://www.conference-board.org/data/calendar/events.json.cfm"
)
CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL = (
    "https://www.conference-board.org/topics/consumer-confidence"
)
CONFERENCE_BOARD_LEADING_INDICATORS_URL = (
    "https://www.conference-board.org/topics/us-leading-indicators"
)


INDICATOR_REGISTRY: dict[str, ConferenceBoardIndicatorSpec] = {
    "TCB_CONSUMER_CONFIDENCE": ConferenceBoardIndicatorSpec(
        series_id="TCB_CONSUMER_CONFIDENCE",
        indicator="CB Consumer Confidence",
        country_code="US",
        title="CB Consumer Confidence",
        unit="index",
        importance="high",
        category="Consumer",
        source_url=CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL,
        schedule_url=CONFERENCE_BOARD_CALENDAR_URL,
        schedule_title_fragment="Consumer Confidence Index",
        reference_month_lag=0,
    ),
    "TCB_LEADING_INDEX": ConferenceBoardIndicatorSpec(
        series_id="TCB_LEADING_INDEX",
        indicator="CB Leading Index",
        country_code="US",
        title="CB Leading Index",
        unit="%",
        importance="high",
        category="Leading Indicators",
        source_url=CONFERENCE_BOARD_LEADING_INDICATORS_URL,
        schedule_url=CONFERENCE_BOARD_CALENDAR_URL,
        schedule_title_fragment="Leading Index",
        reference_month_lag=2,
    ),
}
