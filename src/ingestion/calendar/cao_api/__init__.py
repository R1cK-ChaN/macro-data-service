"""Cabinet Office (ESRI) calendar connector (issue #14 P3).

Projects the Cabinet Office / Economic and Social Research Institute
release calendar into ``cal_econ_raw`` / ``cal_econ_event``. Single
indicator anchor — Consumer Confidence Survey, ESRI's highest-count
Japan indicator (142 "high" importance events in TE historical,
see issue #14 TBD-Repro resolution).

Sources:

- ``esri.cao.go.jp/en/stat/stat-schedule-e.html`` — schedule-side:
  the Cabinet Office release-schedule page carries a single 5-column
  table whose column 4 ("Consumer Confidence Survey") enumerates
  forward-looking release dates + reference months.
- ``esri.cao.go.jp/en/stat/shouhi/shouhi-e.html`` — value-side: the
  Consumer Confidence landing page re-publishes each release in
  place. Two deterministic sentences carry the headline:

    "The Survey of March 2026 was released on April 9th, 2026"
    "The Consumer Confidence Index (seasonally adjusted series)
     in March 2026 was 33.3, down 6.4 points from the previous
     month."

``provider_event_id`` anchors on ``(indicator, reference_date)`` so
schedule and value sides share the same id, and the merge CASE in
the shared projector preserves the schedule-side datetime on
upsert. Schedule-side writes route through
:func:`project_schedule_events`; the value-side writer uses the
full :func:`project_events` upsert.

Follow-on phases in issue #14 cover the remaining Cabinet Office
surfaces (GDP / Machinery Orders / Business Outlook / BC indexes);
they land under the same ``cao`` provider id with separate
connector modules.
"""

from __future__ import annotations

from .fetcher import (
    ALL_INDICATORS,
    ConsumerConfidenceValuesRunSummary,
    FetchRunSummary,
    fetch_cao_calendar,
    fetch_cao_consumer_confidence_values,
)
from .indicators import INDICATOR_REGISTRY, CaoIndicatorSpec
from .parser import (
    CAO_CONSUMER_CONFIDENCE_RELEASE_TIME_LOCAL,
    CAO_CONSUMER_CONFIDENCE_URL,
    CAO_ESRI_SCHEDULE_URL,
    CAO_RELEASE_TZ,
    CaoCalendarEventRecord,
    CaoCalendarRawRecord,
    PROVIDER,
)
from .projector import project_events, project_schedule_events, store_raw
from .scraper import (
    CAO_BROWSER_HEADERS,
    CaoCalendarParseError,
    CaoConsumerConfidenceEntry,
    fetch_cao_schedule_html,
    parse_cao_schedule_html,
    schedule_entry_to_records,
)
from .surveys import (
    CaoConsumerConfidenceParseError,
    ConsumerConfidenceSummary,
    consumer_confidence_to_records,
    fetch_consumer_confidence_summary_html,
    parse_consumer_confidence_summary,
)

__all__ = [
    "ALL_INDICATORS",
    "CAO_BROWSER_HEADERS",
    "CAO_CONSUMER_CONFIDENCE_RELEASE_TIME_LOCAL",
    "CAO_CONSUMER_CONFIDENCE_URL",
    "CAO_ESRI_SCHEDULE_URL",
    "CAO_RELEASE_TZ",
    "CaoCalendarEventRecord",
    "CaoCalendarParseError",
    "CaoCalendarRawRecord",
    "CaoConsumerConfidenceEntry",
    "CaoConsumerConfidenceParseError",
    "CaoIndicatorSpec",
    "ConsumerConfidenceSummary",
    "ConsumerConfidenceValuesRunSummary",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "consumer_confidence_to_records",
    "fetch_cao_calendar",
    "fetch_cao_consumer_confidence_values",
    "fetch_cao_schedule_html",
    "fetch_consumer_confidence_summary_html",
    "parse_cao_schedule_html",
    "parse_consumer_confidence_summary",
    "project_events",
    "project_schedule_events",
    "schedule_entry_to_records",
    "store_raw",
]
