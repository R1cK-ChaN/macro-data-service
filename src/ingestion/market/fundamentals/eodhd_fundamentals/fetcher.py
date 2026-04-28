"""Per-ticker fetcher for the EODHD fundamentals endpoint.

Drives :mod:`client` for one ticker at a time, parses via
:mod:`parser`, persists via :mod:`projector`. Default ``dry_run=True``
so a misfired call cannot spend the API budget; callers must pass
``dry_run=False`` to actually transact.

A ``max_requests`` cap bounds a single invocation to ``len(tickers)``
HTTP calls (one per ticker). Tickers that 404 or otherwise raise are
skipped — the run continues so a single bad symbol cannot abort the
whole batch — and a per-ticker error count surfaces in the summary.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .client import (
    EODHDFundamentalsClient,
    EODHDFundamentalsNotFound,
    EODHDFundamentalsThrottled,
)
from .parser import build_raw_record, parse_payload_records
from .projector import (
    project_fundamentals_company,
    project_fundamentals_financials,
    project_fundamentals_highlights,
    store_fundamentals_raw,
)

logger = logging.getLogger(__name__)

PROVIDER = "eodhd"


@dataclass
class FundamentalsFetchSummary:
    tickers_planned:         int = 0
    tickers_fetched:         int = 0
    tickers_skipped_error:   int = 0
    tickers:                 list[str] = field(default_factory=list)
    requests_spent:          int = 0
    raw_inserted:            int = 0
    company_upserted:        int = 0
    financials_upserted:     int = 0
    highlights_upserted:     int = 0
    parse_errors:            int = 0
    stopped_reason:          str = ""
    dry_run:                 bool = True
    errors:                  list[dict[str, str]] = field(default_factory=list)


class FundamentalsFetcher:
    """Bounded per-ticker fundamentals fetcher.

    Single entry point :meth:`fetch` accepts a ticker list and a
    ``sections`` whitelist (defaults to the slice-1 set: General +
    Highlights + Valuation + SharesStats + Financials). Optional
    section filtering shortens the response — the projector still
    only writes the sections present in the payload.
    """

    DEFAULT_SECTIONS: tuple[str, ...] = (
        "General",
        "Highlights",
        "Valuation",
        "SharesStats",
        "Financials",
    )

    def __init__(
        self,
        *,
        connection: sqlite3.Connection,
        client: EODHDFundamentalsClient,
        max_requests: int = 50,
        clock_ms: Any = None,
    ) -> None:
        if max_requests < 1:
            raise ValueError("max_requests must be >= 1")
        self._connection = connection
        self._client = client
        self._max_requests = max_requests
        self._clock_ms = clock_ms or _utc_now_ms

    def fetch(
        self,
        *,
        tickers: list[str],
        sections: list[str] | None = None,
        dry_run: bool = True,
    ) -> FundamentalsFetchSummary:
        cleaned_tickers = [
            t.strip() for t in (tickers or []) if isinstance(t, str) and t.strip()
        ]
        if not cleaned_tickers:
            raise ValueError("at least one ticker required")
        section_list: list[str] | None
        if sections is None:
            section_list = list(self.DEFAULT_SECTIONS)
        elif sections == []:
            section_list = None  # caller asked for "everything"
        else:
            section_list = [s.strip() for s in sections if isinstance(s, str) and s.strip()]
            if not section_list:
                section_list = list(self.DEFAULT_SECTIONS)

        summary = FundamentalsFetchSummary(
            tickers_planned=len(cleaned_tickers),
            tickers=list(cleaned_tickers),
            dry_run=dry_run,
        )
        if dry_run:
            summary.stopped_reason = "dry_run"
            return summary

        # Anchor the request budget at this call's starting point so a
        # reused client / fetcher across batches doesn't trip
        # ``budget_exhausted`` on the first ticker just because earlier
        # runs already burned requests on this client.
        baseline_requests = self._client.requests_made

        def _spent_now() -> int:
            return self._client.requests_made - baseline_requests

        for ticker in cleaned_tickers:
            if _spent_now() >= self._max_requests:
                summary.requests_spent = _spent_now()
                summary.stopped_reason = "budget_exhausted"
                return summary
            # Each ticker can spend up to (max_retries + 1) requests on
            # 429 backoff. Clamp the per-call retries so the client
            # cannot push the cumulative spend past ``max_requests`` —
            # a 429-storm on one ticker now stops the run within
            # budget instead of blowing through it.
            remaining = self._max_requests - _spent_now()
            per_call_retries = max(0, remaining - 1)
            try:
                result = self._client.get_fundamentals(
                    ticker, sections=section_list,
                    max_retries=per_call_retries,
                )
            except EODHDFundamentalsNotFound as exc:
                summary.tickers_skipped_error += 1
                summary.errors.append({"ticker": ticker, "error": str(exc), "kind": "not_found"})
                summary.requests_spent = _spent_now()
                logger.warning(
                    "fundamentals fetch 404 ticker=%s: %s", ticker, exc
                )
                continue
            except EODHDFundamentalsThrottled as exc:
                summary.tickers_skipped_error += 1
                summary.errors.append({"ticker": ticker, "error": str(exc), "kind": "throttled"})
                summary.requests_spent = _spent_now()
                summary.stopped_reason = "throttled"
                logger.warning(
                    "fundamentals fetch throttled ticker=%s: %s", ticker, exc
                )
                return summary
            except Exception as exc:  # network / shape error — skip the ticker
                summary.tickers_skipped_error += 1
                summary.errors.append({"ticker": ticker, "error": str(exc), "kind": "error"})
                summary.requests_spent = _spent_now()
                logger.warning(
                    "fundamentals fetch error ticker=%s: %s", ticker, exc
                )
                continue

            summary.requests_spent = _spent_now()
            summary.tickers_fetched += 1
            snapshot_ms = self._clock_ms()

            try:
                raw = build_raw_record(
                    ticker=ticker,
                    payload_text=result.payload_text,
                    snapshot_epoch_ms=snapshot_ms,
                )
                company, highlights, financials = parse_payload_records(
                    result.payload,
                    ticker=ticker,
                    snapshot_epoch_ms=snapshot_ms,
                )
            except Exception as exc:
                summary.parse_errors += 1
                summary.errors.append({"ticker": ticker, "error": str(exc), "kind": "parse"})
                logger.warning(
                    "fundamentals parse error ticker=%s: %s", ticker, exc
                )
                continue

            summary.raw_inserted += store_fundamentals_raw(
                self._connection, [raw]
            )
            if company is not None:
                summary.company_upserted += project_fundamentals_company(
                    self._connection, [company]
                )
            if highlights is not None:
                summary.highlights_upserted += project_fundamentals_highlights(
                    self._connection, [highlights]
                )
            if financials:
                summary.financials_upserted += project_fundamentals_financials(
                    self._connection, financials
                )

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"
        return summary


def _utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)
