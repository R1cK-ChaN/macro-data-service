"""DOL UI Weekly Claims calendar connector — issue #50.

Walks the DOL Employment & Training Administration newsroom
listing for recent ``Unemployment Insurance Weekly Claims Report``
rows, downloads each release's PDF (DOL serves the press release
as ``application/pdf`` directly off the ``/newsroom/releases/eta/
etaYYYYMMDD`` URL), and writes the headline Initial Claims +
Continuing Claims figures into ``cal_econ_event``.

Schedule and value land together — DOL doesn't publish a forward
calendar, so each press-release fetch produces both rows.

Public surface:

- :data:`INDICATOR_REGISTRY` — INITIAL_CLAIMS + CONTINUING_CLAIMS.
- :class:`DOLValueObservation` — one (indicator, release, ref, value).
- :func:`extract_press_release_value` — PDF text → headline ``actual``.
- :func:`fetch_dol_calendar` — orchestrates listing + PDF + projector.
- :func:`store_raw` / :func:`project_events` — idempotent writers.

DOL.gov sits behind Akamai bot protection. The default fetch path
uses :func:`session_for_dol`, which carries the browser headers +
cookie jar that get past the ``sec_cpt`` challenge in our
verified probe. Tests inject fixture seams to avoid live HTTP.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_dol_calendar
from .indicators import DOLIndicatorSpec, INDICATOR_REGISTRY
from .listing import (
    DOL_ETA_LISTING_URL,
    DOL_NEWSROOM_BASE,
    DOLListingParseError,
    DOLReleaseEntry,
    fetch_listing_html,
    fetch_release_pdf_bytes,
    parse_listing_html,
    session_for_dol,
)
from .parser import (
    DOL_RELEASE_TIME,
    DOL_RELEASE_TZ,
    DOLCalendarEventRecord,
    DOLCalendarRawRecord,
    DOLPressReleaseParseError,
    DOLValueObservation,
    PROVIDER,
    extract_press_release_value,
    parse_press_release_pdf,
    value_observation_to_records,
)
from .projector import project_events, store_raw

__all__ = [
    "DOL_ETA_LISTING_URL",
    "DOL_NEWSROOM_BASE",
    "DOL_RELEASE_TIME",
    "DOL_RELEASE_TZ",
    "DOLCalendarEventRecord",
    "DOLCalendarRawRecord",
    "DOLIndicatorSpec",
    "DOLListingParseError",
    "DOLPressReleaseParseError",
    "DOLReleaseEntry",
    "DOLValueObservation",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "extract_press_release_value",
    "fetch_dol_calendar",
    "fetch_listing_html",
    "fetch_release_pdf_bytes",
    "parse_listing_html",
    "parse_press_release_pdf",
    "project_events",
    "session_for_dol",
    "store_raw",
    "value_observation_to_records",
]
