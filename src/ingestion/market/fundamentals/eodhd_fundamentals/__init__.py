"""EODHD ``/api/fundamentals/`` ingestion package — issue #68 slice 1.

Mirrors the structure of :mod:`ingestion.calendar.eodhd_api`:

* :class:`EODHDFundamentalsClient` — HTTP wrapper (auth, 429 backoff,
  fmt-json injection, optional ``filter`` section whitelist).
* per-section parsers (:func:`parse_company_section`,
  :func:`parse_highlights_section`, :func:`parse_financials_section`)
  plus :func:`build_raw_record` for the audit lane.
* :func:`store_fundamentals_raw` / :func:`project_fundamentals_company`
  / :func:`project_fundamentals_financials`
  / :func:`project_fundamentals_highlights` — idempotent upserts with
  observed-at PIT guards.
* :class:`FundamentalsFetcher` — per-ticker dispatch with budget cap.

Slice-1 scope covers the four highest-impact section streams (General,
Highlights/Valuation/SharesStats, Income Statement, Balance Sheet,
Cash Flow). ``Earnings`` actuals already live in ``cal_corp_event``;
``fundamentals_estimates`` (forward-looking) is schema-only here and
gets populated in a later slice that wires ``Earnings.Trend`` /
``AnalystRatings``.
"""

from __future__ import annotations

from .client import (
    EODHDFundamentalsAuthMissing,
    EODHDFundamentalsClient,
    EODHDFundamentalsNotFound,
    EODHDFundamentalsThrottled,
    FundamentalsCallResult,
)
from .fetcher import FundamentalsFetcher, FundamentalsFetchSummary
from .parser import (
    PERIOD_KEYS,
    PROVIDER,
    STATEMENT_KEYS,
    build_raw_record,
    parse_company_section,
    parse_financials_section,
    parse_highlights_section,
    parse_payload_records,
)
from .projector import (
    project_fundamentals_company,
    project_fundamentals_financials,
    project_fundamentals_highlights,
    store_fundamentals_raw,
)

__all__ = [
    "EODHDFundamentalsAuthMissing",
    "EODHDFundamentalsClient",
    "EODHDFundamentalsNotFound",
    "EODHDFundamentalsThrottled",
    "FundamentalsCallResult",
    "FundamentalsFetchSummary",
    "FundamentalsFetcher",
    "PERIOD_KEYS",
    "PROVIDER",
    "STATEMENT_KEYS",
    "build_raw_record",
    "parse_company_section",
    "parse_financials_section",
    "parse_highlights_section",
    "parse_payload_records",
    "project_fundamentals_company",
    "project_fundamentals_financials",
    "project_fundamentals_highlights",
    "store_fundamentals_raw",
]
