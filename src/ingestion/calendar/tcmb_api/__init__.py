"""TCMB (Türkiye Cumhuriyet Merkez Bankası) calendar connector — issue #86 P1.

The 1-Week Repo Auction Rate has been TCMB's policy rate since
20 May 2010. The bank publishes the full rate-decision history as a
static HTML table at
``tcmb.gov.tr/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Temel+Faaliyetler/
Para+Politikasi/Merkez+Bankasi+Faiz+Oranlari/1+Hafta+Repo`` —
``<table id="midTable">`` with three columns: Tarih (date),
Borç Alma (borrowing rate, dash for one-week repo), Borç Verme
(lending rate = the 1-week repo).

The connector ships **rate-change events**: each row in the table
maps to one MPC announcement that *changed* the policy rate. Hold
decisions don't appear on this surface (the table only lists
changes); a P2 slice can fold in TCMB's PPK press releases for
hold-decision coverage. The slice is value-bearing — every event
publishes with ``actual=<new rate>`` and ``previous=<prior rate>`` —
so ``(TR, TCMB_RATE)`` joins the parity whitelist on day one.

Public surface mirrors the other ``<src>_api`` calendar packages.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_tcmb_calendar
from .indicators import INDICATOR_REGISTRY, TCMBIndicatorSpec
from .parser import (
    PROVIDER,
    TCMB_BASE_URL,
    TCMB_RATE_HISTORY_URL,
    TCMB_RELEASE_TIME,
    TCMB_RELEASE_TZ,
    TCMBCalendarEventRecord,
    TCMBCalendarRawRecord,
    TCMBRateDecision,
    TCMBRateHistoryParseError,
    decision_to_records,
    parse_rate_history,
)
from .projector import project_events, store_raw

__all__ = [
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "PROVIDER",
    "TCMB_BASE_URL",
    "TCMB_RATE_HISTORY_URL",
    "TCMB_RELEASE_TIME",
    "TCMB_RELEASE_TZ",
    "TCMBCalendarEventRecord",
    "TCMBCalendarRawRecord",
    "TCMBIndicatorSpec",
    "TCMBRateDecision",
    "TCMBRateHistoryParseError",
    "decision_to_records",
    "fetch_tcmb_calendar",
    "parse_rate_history",
    "project_events",
    "store_raw",
]
