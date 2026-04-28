"""Parse EODHD ``/api/fundamentals/`` payload sections into storage records.

Pure functions — no DB, no HTTP. The payload is one big dict keyed by
section (``General``, ``Highlights``, ``Financials``, …). Each
section parser walks just its own slice and emits zero-or-more typed
records.

Slice 1 covers the four highest-impact sections:

* :func:`parse_company_section`     — ``General`` → company profile.
* :func:`parse_highlights_section`  — ``Highlights`` + ``Valuation`` +
  ``SharesStats`` merged into a single ratio snapshot row.
* :func:`parse_financials_section`  — ``Financials.{Income_Statement,
  Balance_Sheet,Cash_Flow}`` × ``{quarterly,yearly}`` → one record per
  ``(period_end, period_type, statement)``.

Plus the snapshot-level helpers:

* :func:`build_raw_record`     — wraps the verbatim payload bytes into
  a :class:`FundamentalsRawRecord` with content hash + fetched-at ISO.
* :func:`parse_payload_records` — convenience that runs all three
  section parsers off one payload, returning one tuple of all records.

``content_hash`` is the same SHA-256-over-canonical-JSON pattern used
by ``cal_corp_*`` — sorted keys, ``ensure_ascii=False`` — so byte
equivalence between two snapshots collapses to the same hash regardless
of upstream key-ordering differences. Numeric fields are normalised to
``float`` (or ``None`` when missing/empty); the typed columns stay
queryable while ``payload_json`` retains the verbatim section dict for
schema-drift insurance.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from storage.models.fundamentals import (
    FundamentalsCompanyRecord,
    FundamentalsFinancialsRecord,
    FundamentalsHighlightsRecord,
    FundamentalsRawRecord,
)

PROVIDER = "eodhd"

STATEMENT_KEYS: dict[str, str] = {
    # EODHD section name → our canonical statement code
    "Income_Statement": "IS",
    "Balance_Sheet":    "BS",
    "Cash_Flow":        "CF",
}

PERIOD_KEYS: dict[str, str] = {
    "quarterly": "Q",
    "yearly":    "A",
}

# Per-statement typed-column mapping. (sql_column, eodhd_field) — fields
# not listed stay in payload_json. Picked for the highest-traffic line
# items per CLAUDE.md rule 3 ("typed columns only for fields that have
# proven downstream queries"). EODHD does not ship per-period basic EPS
# on the income statement (it lives on ``Highlights.EarningsShare`` as
# a TTM scalar) — the ``eps_basic`` column stays NULL on every IS row
# but exists for shape parity with future providers (Compustat /
# Refinitiv ship per-period basic EPS).
_IS_TYPED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("revenue",    "totalRevenue"),
    ("net_income", "netIncome"),
)
_BS_TYPED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("total_assets",      "totalAssets"),
    ("total_equity",      "totalStockholderEquity"),
    ("total_liabilities", "totalLiab"),
)
_CF_TYPED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("cash_from_ops", "totalCashFromOperatingActivities"),
    ("capex",         "capitalExpenditures"),
)
_TYPED_BY_STATEMENT: dict[str, tuple[tuple[str, str], ...]] = {
    "IS": _IS_TYPED_COLUMNS,
    "BS": _BS_TYPED_COLUMNS,
    "CF": _CF_TYPED_COLUMNS,
}


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def _content_hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _content_hash_text(text: str) -> str:
    """Hash a verbatim JSON string. Used for the raw lane where we
    keep the bytes EODHD returned, not a re-serialised normalisation."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _fetched_at(snapshot_epoch_ms: int) -> str:
    return datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc
    ).isoformat()


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _first_present(*values: Any) -> Any:
    """Return the first ``value`` that is not ``None``.

    Plain ``or`` would treat numeric ``0`` as missing — a real concern
    for fields like ``PERatio`` (loss-making companies show 0 in
    Highlights but have a populated Valuation block) and
    ``SharesOutstanding`` (zero never legitimate, but other zero-
    coercible numerics could). Explicit ``is not None`` preserves them.
    """
    for v in values:
        if v is not None:
            return v
    return None


def build_raw_record(
    *,
    ticker: str,
    payload_text: str,
    snapshot_epoch_ms: int,
) -> FundamentalsRawRecord:
    """Wrap a verbatim ``/api/fundamentals/`` response body into a
    :class:`FundamentalsRawRecord` ready for ``store_fundamentals_raw``.

    The hash is over the response bytes — re-fetching the same payload
    is a no-op even if a future EODHD endpoint change reorders the
    sections, because re-serialisation isn't involved.
    """
    return FundamentalsRawRecord(
        provider=PROVIDER,
        ticker=ticker,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=_content_hash_text(payload_text),
        payload_json=payload_text,
        fetched_at=_fetched_at(snapshot_epoch_ms),
    )


def parse_company_section(
    payload: dict[str, Any],
    *,
    ticker: str,
    snapshot_epoch_ms: int,
) -> FundamentalsCompanyRecord | None:
    """Project the ``General`` block to a :class:`FundamentalsCompanyRecord`.

    Returns ``None`` for payloads with no ``General`` block (some
    micro-cap or recently-delisted tickers ship empty fundamentals).
    """
    section = payload.get("General")
    if not isinstance(section, dict) or not section:
        return None
    return FundamentalsCompanyRecord(
        provider=PROVIDER,
        ticker=ticker,
        name=_to_text(section.get("Name")),
        asset_type=_to_text(section.get("Type")),
        sector=_to_text(section.get("Sector") or section.get("GicSector")),
        industry=_to_text(section.get("Industry") or section.get("GicIndustry")),
        fiscal_year_end=_to_text(section.get("FiscalYearEnd")),
        listing_exchange=_to_text(
            section.get("Exchange") or section.get("PrimaryExchange")
        ),
        currency_code=_to_text(section.get("CurrencyCode")),
        country_iso=_to_text(section.get("CountryISO") or section.get("CountryName")),
        isin=_to_text(section.get("ISIN")),
        cusip=_to_text(section.get("CUSIP")),
        payload_json=_stable_json(section),
        content_hash=_content_hash(section),
        observed_at_epoch_ms=snapshot_epoch_ms,
    )


def parse_highlights_section(
    payload: dict[str, Any],
    *,
    ticker: str,
    snapshot_epoch_ms: int,
    as_of_date: str | None = None,
) -> FundamentalsHighlightsRecord | None:
    """Merge ``Highlights`` + ``Valuation`` + ``SharesStats`` into one
    :class:`FundamentalsHighlightsRecord`.

    EODHD updates these blocks ~daily, so the natural row grain is
    ``(ticker, as_of_date)``. ``as_of_date`` defaults to the UTC date
    of the snapshot epoch — explicit override is for tests.
    """
    highlights_raw = payload.get("Highlights")
    valuation_raw = payload.get("Valuation")
    shares_raw = payload.get("SharesStats")
    highlights = highlights_raw if isinstance(highlights_raw, dict) else {}
    valuation = valuation_raw if isinstance(valuation_raw, dict) else {}
    shares_stats = shares_raw if isinstance(shares_raw, dict) else {}
    if not highlights and not valuation and not shares_stats:
        return None
    if as_of_date is None:
        as_of_date = datetime.fromtimestamp(
            snapshot_epoch_ms / 1000, tz=timezone.utc
        ).date().isoformat()
    merged = {
        "Highlights":  highlights,
        "Valuation":   valuation,
        "SharesStats": shares_stats,
    }
    return FundamentalsHighlightsRecord(
        provider=PROVIDER,
        ticker=ticker,
        as_of_date=as_of_date,
        market_cap=_to_float(highlights.get("MarketCapitalization")),
        pe_ratio=_to_float(_first_present(
            highlights.get("PERatio"), valuation.get("PERatio")
        )),
        eps_ttm=_to_float(highlights.get("EarningsShare")),
        dividend_yield=_to_float(highlights.get("DividendYield")),
        book_value=_to_float(highlights.get("BookValue")),
        shares_outstanding=_to_float(_first_present(
            shares_stats.get("SharesOutstanding"),
            highlights.get("SharesOutstanding"),
        )),
        payload_json=_stable_json(merged),
        content_hash=_content_hash(merged),
        observed_at_epoch_ms=snapshot_epoch_ms,
    )


def parse_financials_section(
    payload: dict[str, Any],
    *,
    ticker: str,
    snapshot_epoch_ms: int,
) -> list[FundamentalsFinancialsRecord]:
    """Project ``Financials.{IS,BS,CF}.{Q,A}`` into one record per period.

    Returns an empty list if the ``Financials`` block is missing or
    empty (ETFs / indices, recently-IPO'd names without filings yet).
    """
    financials = payload.get("Financials")
    if not isinstance(financials, dict) or not financials:
        return []
    out: list[FundamentalsFinancialsRecord] = []
    for section_name, statement_code in STATEMENT_KEYS.items():
        section = financials.get(section_name)
        if not isinstance(section, dict):
            continue
        currency = _to_text(section.get("currency_symbol"))
        for period_key, period_code in PERIOD_KEYS.items():
            period_block = section.get(period_key)
            if not isinstance(period_block, dict):
                continue
            for period_end, row in period_block.items():
                if not isinstance(row, dict):
                    continue
                period_end_clean = _to_text(period_end) or _to_text(row.get("date"))
                if not period_end_clean:
                    continue
                typed_cols = _TYPED_BY_STATEMENT[statement_code]
                typed_values: dict[str, float | None] = {
                    col: _to_float(row.get(field)) for col, field in typed_cols
                }
                row_currency = _to_text(row.get("currency_symbol")) or currency
                out.append(
                    FundamentalsFinancialsRecord(
                        provider=PROVIDER,
                        ticker=ticker,
                        period_end=period_end_clean,
                        period_type=period_code,
                        statement=statement_code,
                        currency=row_currency,
                        filing_date=_to_text(row.get("filing_date")),
                        revenue=typed_values.get("revenue"),
                        net_income=typed_values.get("net_income"),
                        eps_basic=None,  # see _IS_TYPED_COLUMNS comment
                        total_assets=typed_values.get("total_assets"),
                        total_equity=typed_values.get("total_equity"),
                        total_liabilities=typed_values.get("total_liabilities"),
                        cash_from_ops=typed_values.get("cash_from_ops"),
                        capex=typed_values.get("capex"),
                        payload_json=_stable_json(row),
                        content_hash=_content_hash(row),
                        observed_at_epoch_ms=snapshot_epoch_ms,
                    )
                )
    return out


def parse_payload_records(
    payload: dict[str, Any],
    *,
    ticker: str,
    snapshot_epoch_ms: int,
) -> tuple[
    FundamentalsCompanyRecord | None,
    FundamentalsHighlightsRecord | None,
    list[FundamentalsFinancialsRecord],
]:
    """Run all three slice-1 section parsers off one payload.

    Returns ``(company, highlights, financials)``. Any item may be
    ``None`` / empty when the corresponding block is absent — the
    fetcher / projector handle missing items as a clean no-op.
    """
    company = parse_company_section(
        payload, ticker=ticker, snapshot_epoch_ms=snapshot_epoch_ms
    )
    highlights = parse_highlights_section(
        payload, ticker=ticker, snapshot_epoch_ms=snapshot_epoch_ms
    )
    financials = parse_financials_section(
        payload, ticker=ticker, snapshot_epoch_ms=snapshot_epoch_ms
    )
    return company, highlights, financials
