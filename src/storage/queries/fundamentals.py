"""Fundamentals-domain query helpers for ``SQLiteEngineStore``.

Covers the five tables introduced in issue #68 slice 1
(``fundamentals_{raw,company,financials,highlights,estimates}``).

PIT semantics: when ``as_of`` is None the methods return whatever the
latest projection holds; when ``as_of`` is set, the helpers reconstruct
the section from the most recent ``fundamentals_raw`` row whose
``snapshot_epoch_ms`` is ≤ the cutoff. The reconstruction re-runs the
slice-1 parsers, so it's only as good as the fields the parsers
actually pull through — payload_json on every row keeps the raw bytes
available for callers that need fields not in the typed projection.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any


class _FundamentalsQueriesMixin:
    def get_fundamentals_company_row(
        self, *, provider: str, ticker: str,
    ) -> dict[str, Any] | None:
        """Return the latest ``fundamentals_company`` row as a dict.

        ``None`` when the ticker has no company projection yet.
        """
        with self._connection(commit=False) as connection:
            row = connection.execute(
                """
                SELECT provider, ticker, name, asset_type, sector,
                       industry, fiscal_year_end, listing_exchange,
                       currency_code, country_iso, isin, cusip,
                       payload_json, content_hash, observed_at_epoch_ms
                FROM fundamentals_company
                WHERE provider = ? AND ticker = ?
                """,
                (provider, ticker),
            ).fetchone()
        if row is None:
            return None
        return dict(row)

    def get_fundamentals_highlights_row(
        self,
        *,
        provider: str,
        ticker: str,
        as_of_date: str | None = None,
    ) -> dict[str, Any] | None:
        """Return one ``fundamentals_highlights`` row.

        With ``as_of_date`` set, returns the row for that exact
        calendar date or ``None``. Without, returns the most recent
        row (highest ``as_of_date``).
        """
        with self._connection(commit=False) as connection:
            if as_of_date is not None:
                row = connection.execute(
                    """
                    SELECT * FROM fundamentals_highlights
                    WHERE provider = ? AND ticker = ?
                          AND as_of_date <= ?
                    ORDER BY as_of_date DESC
                    LIMIT 1
                    """,
                    (provider, ticker, as_of_date),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT * FROM fundamentals_highlights
                    WHERE provider = ? AND ticker = ?
                    ORDER BY as_of_date DESC
                    LIMIT 1
                    """,
                    (provider, ticker),
                ).fetchone()
        return dict(row) if row is not None else None

    def list_fundamentals_financials(
        self,
        *,
        provider: str,
        ticker: str,
        statement: str | None = None,
        period_type: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return matching ``fundamentals_financials`` rows ordered
        by ``period_end DESC``.

        ``statement`` ∈ ``{'IS','BS','CF'}``; ``period_type`` ∈
        ``{'Q','A'}``. Both default to no filter.
        """
        sql = [
            """
            SELECT provider, ticker, period_end, period_type, statement,
                   currency, filing_date,
                   revenue, net_income, eps_basic,
                   total_assets, total_equity, total_liabilities,
                   cash_from_ops, capex,
                   payload_json, content_hash, observed_at_epoch_ms
            FROM fundamentals_financials
            WHERE provider = ? AND ticker = ?
            """
        ]
        params: list[Any] = [provider, ticker]
        if statement:
            sql.append(" AND statement = ?")
            params.append(statement)
        if period_type:
            sql.append(" AND period_type = ?")
            params.append(period_type)
        sql.append(" ORDER BY period_end DESC, statement, period_type")
        if limit is not None:
            sql.append(" LIMIT ?")
            params.append(int(limit))
        with self._connection(commit=False) as connection:
            rows = connection.execute("".join(sql), params).fetchall()
        return [dict(row) for row in rows]

    def get_fundamentals_raw_at(
        self,
        *,
        provider: str,
        ticker: str,
        as_of_epoch_ms: int,
    ) -> dict[str, Any] | None:
        """Return the most recent ``fundamentals_raw`` row whose
        ``snapshot_epoch_ms`` is at or before ``as_of_epoch_ms``.

        Used by PIT reads to anchor reconstruction against a known
        upstream snapshot. ``None`` when no snapshot for the ticker is
        old enough — caller should fall through to "no data at as_of".
        """
        with self._connection(commit=False) as connection:
            row = connection.execute(
                """
                SELECT provider, ticker, snapshot_epoch_ms,
                       content_hash, payload_json, fetched_at
                FROM fundamentals_raw
                WHERE provider = ? AND ticker = ?
                      AND snapshot_epoch_ms <= ?
                ORDER BY snapshot_epoch_ms DESC
                LIMIT 1
                """,
                (provider, ticker, as_of_epoch_ms),
            ).fetchone()
        return dict(row) if row is not None else None

    def list_fundamentals_raw_versions(
        self,
        *,
        provider: str,
        ticker: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return ``fundamentals_raw`` revision chain for a ticker,
        most-recent first. Used by future revision-surfacing UIs.
        """
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT provider, ticker, snapshot_epoch_ms,
                       content_hash, fetched_at,
                       LENGTH(payload_json) AS payload_bytes
                FROM fundamentals_raw
                WHERE provider = ? AND ticker = ?
                ORDER BY snapshot_epoch_ms DESC
                LIMIT ?
                """,
                (provider, ticker, max(1, int(limit))),
            ).fetchall()
        return [dict(row) for row in rows]


def parse_as_of_to_epoch_ms(as_of_iso: str) -> int:
    """Parse an ISO-8601 string to an epoch-ms cutoff.

    Naive timestamps are interpreted as UTC. Raises ``ValueError`` on
    malformed input — service-layer callers should catch and surface as
    ``{"error": "invalid as_of: ..."}``.
    """
    parsed = datetime.fromisoformat(as_of_iso.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return int(parsed.timestamp() * 1000)
