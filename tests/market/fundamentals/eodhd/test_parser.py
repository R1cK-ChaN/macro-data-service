"""Parser tests for the EODHD fundamentals package (issue #68 S1)."""

from __future__ import annotations

import json

from ingestion.market.fundamentals.eodhd_fundamentals import (
    build_raw_record,
    parse_company_section,
    parse_financials_section,
    parse_highlights_section,
    parse_payload_records,
)
from ingestion.market.fundamentals.eodhd_fundamentals.parser import _content_hash


def _general() -> dict:
    return {
        "Code":          "AAPL",
        "Name":          "Apple Inc",
        "Type":          "Common Stock",
        "Sector":        "Technology",
        "Industry":      "Consumer Electronics",
        "FiscalYearEnd": "September",
        "Exchange":      "NASDAQ",
        "CurrencyCode":  "USD",
        "CountryISO":    "US",
        "ISIN":          "US0378331005",
        "CUSIP":         "037833100",
    }


def _highlights() -> dict:
    return {
        "MarketCapitalization": 3.0e12,
        "PERatio":              28.5,
        "EarningsShare":        6.42,
        "DividendYield":        0.0044,
        "BookValue":            4.10,
    }


def _financials_block() -> dict:
    return {
        "Income_Statement": {
            "currency_symbol": "USD",
            "yearly": {
                "2024-09-30": {
                    "date":          "2024-09-30",
                    "filing_date":   "2024-11-01",
                    "totalRevenue":  391035000000.0,
                    "netIncome":     93736000000.0,
                },
                "2023-09-30": {
                    "date":          "2023-09-30",
                    "totalRevenue":  383285000000.0,
                    "netIncome":     96995000000.0,
                },
            },
            "quarterly": {
                "2024-09-30": {
                    "date":          "2024-09-30",
                    "totalRevenue":  94930000000.0,
                    "netIncome":     14736000000.0,
                },
            },
        },
        "Balance_Sheet": {
            "currency_symbol": "USD",
            "yearly": {
                "2024-09-30": {
                    "date":                       "2024-09-30",
                    "totalAssets":                364980000000.0,
                    "totalStockholderEquity":     56950000000.0,
                    "totalLiab":                  308030000000.0,
                },
            },
            "quarterly": {},
        },
        "Cash_Flow": {
            "currency_symbol": "USD",
            "yearly": {
                "2024-09-30": {
                    "date":                              "2024-09-30",
                    "totalCashFromOperatingActivities":  118254000000.0,
                    "capitalExpenditures":               -9447000000.0,
                },
            },
            "quarterly": {},
        },
    }


def _payload() -> dict:
    return {
        "General":     _general(),
        "Highlights":  _highlights(),
        "Valuation":   {"PERatio": 28.5, "PriceBookMRQ": 45.0},
        "SharesStats": {"SharesOutstanding": 15400000000.0},
        "Financials":  _financials_block(),
    }


def test_build_raw_record_hashes_payload_text() -> None:
    text = json.dumps(_payload(), sort_keys=True)
    raw = build_raw_record(
        ticker="AAPL.US", payload_text=text, snapshot_epoch_ms=1_700_000_000_000
    )
    assert raw.provider == "eodhd"
    assert raw.ticker == "AAPL.US"
    assert raw.payload_json == text
    assert raw.snapshot_epoch_ms == 1_700_000_000_000
    assert len(raw.content_hash) == 64
    raw2 = build_raw_record(
        ticker="AAPL.US", payload_text=text, snapshot_epoch_ms=1_800_000_000_000
    )
    assert raw2.content_hash == raw.content_hash  # bytes-stable across snapshots
    raw3 = build_raw_record(
        ticker="AAPL.US",
        payload_text=text + " ",
        snapshot_epoch_ms=1_700_000_000_000,
    )
    assert raw3.content_hash != raw.content_hash


def test_parse_company_section_typed_columns() -> None:
    rec = parse_company_section(
        _payload(), ticker="AAPL.US", snapshot_epoch_ms=1_700_000_000_000
    )
    assert rec is not None
    assert rec.name == "Apple Inc"
    assert rec.asset_type == "Common Stock"
    assert rec.sector == "Technology"
    assert rec.industry == "Consumer Electronics"
    assert rec.fiscal_year_end == "September"
    assert rec.listing_exchange == "NASDAQ"
    assert rec.currency_code == "USD"
    assert rec.country_iso == "US"
    assert rec.isin == "US0378331005"
    assert rec.cusip == "037833100"
    assert json.loads(rec.payload_json)["Name"] == "Apple Inc"
    assert rec.content_hash == _content_hash(_general())


def test_parse_company_section_returns_none_for_missing_general() -> None:
    assert parse_company_section({}, ticker="X.US", snapshot_epoch_ms=1) is None
    assert (
        parse_company_section({"General": {}}, ticker="X.US", snapshot_epoch_ms=1)
        is None
    )


def test_parse_highlights_merges_sections_and_typed_columns() -> None:
    rec = parse_highlights_section(
        _payload(),
        ticker="AAPL.US",
        snapshot_epoch_ms=1_700_000_000_000,
        as_of_date="2024-11-01",
    )
    assert rec is not None
    assert rec.as_of_date == "2024-11-01"
    assert rec.market_cap == 3.0e12
    assert rec.pe_ratio == 28.5
    assert rec.eps_ttm == 6.42
    assert rec.dividend_yield == 0.0044
    assert rec.book_value == 4.10
    assert rec.shares_outstanding == 15400000000.0
    payload = json.loads(rec.payload_json)
    assert payload["Valuation"]["PriceBookMRQ"] == 45.0


def test_parse_highlights_default_as_of_is_utc_date() -> None:
    rec = parse_highlights_section(
        _payload(), ticker="AAPL.US", snapshot_epoch_ms=1_700_000_000_000
    )
    assert rec is not None
    # 2023-11-14 04:13:20 UTC
    assert rec.as_of_date == "2023-11-14"


def test_parse_financials_emits_one_row_per_period_statement() -> None:
    rows = parse_financials_section(
        _payload(), ticker="AAPL.US", snapshot_epoch_ms=1_700_000_000_000
    )
    keys = sorted({(r.period_end, r.period_type, r.statement) for r in rows})
    assert keys == [
        ("2023-09-30", "A", "IS"),
        ("2024-09-30", "A", "BS"),
        ("2024-09-30", "A", "CF"),
        ("2024-09-30", "A", "IS"),
        ("2024-09-30", "Q", "IS"),
    ]
    fy24_is = next(
        r for r in rows
        if r.period_end == "2024-09-30" and r.period_type == "A" and r.statement == "IS"
    )
    assert fy24_is.revenue == 391035000000.0
    assert fy24_is.net_income == 93736000000.0
    assert fy24_is.eps_basic is None
    assert fy24_is.currency == "USD"
    assert fy24_is.filing_date == "2024-11-01"
    fy24_bs = next(
        r for r in rows
        if r.period_end == "2024-09-30" and r.statement == "BS"
    )
    assert fy24_bs.total_assets == 364980000000.0
    assert fy24_bs.total_equity == 56950000000.0
    assert fy24_bs.total_liabilities == 308030000000.0
    fy24_cf = next(
        r for r in rows
        if r.period_end == "2024-09-30" and r.statement == "CF"
    )
    assert fy24_cf.cash_from_ops == 118254000000.0
    assert fy24_cf.capex == -9447000000.0


def test_parse_financials_handles_missing_block() -> None:
    assert parse_financials_section({}, ticker="X.US", snapshot_epoch_ms=1) == []
    assert parse_financials_section(
        {"Financials": {}}, ticker="X.US", snapshot_epoch_ms=1
    ) == []


def test_parse_financials_skips_empty_periods_and_non_dict_rows() -> None:
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": {
                    "2024-12-31": "not-a-dict",  # malformed
                    "":            {"totalRevenue": 1.0},  # empty period_end
                    "2024-09-30":  {"totalRevenue": 100.0, "netIncome": 10.0},
                },
            },
        },
    }
    rows = parse_financials_section(
        payload, ticker="X.US", snapshot_epoch_ms=1
    )
    assert len(rows) == 1
    assert rows[0].period_end == "2024-09-30"
    assert rows[0].revenue == 100.0


def test_parse_payload_records_combines_sections() -> None:
    company, highlights, financials = parse_payload_records(
        _payload(), ticker="AAPL.US", snapshot_epoch_ms=1_700_000_000_000
    )
    assert company is not None
    assert highlights is not None
    assert len(financials) == 5  # 4 yearly + 1 quarterly across IS/BS/CF


def test_parse_highlights_preserves_zero_pe_ratio_in_highlights_block() -> None:
    """Zero in Highlights must not silently fall through to Valuation."""
    payload = {
        "Highlights": {"PERatio": 0, "MarketCapitalization": 1.0e12},
        "Valuation":  {"PERatio": 25.0},
        "SharesStats": {"SharesOutstanding": 1.0e10},
    }
    rec = parse_highlights_section(
        payload, ticker="X.US", snapshot_epoch_ms=1
    )
    assert rec is not None
    # 0 is a valid PE in Highlights — must NOT be replaced by Valuation's 25.
    assert rec.pe_ratio == 0.0


def test_parse_highlights_falls_through_only_when_highlights_pe_is_none() -> None:
    payload = {
        "Highlights": {"MarketCapitalization": 1.0e12},  # no PERatio key
        "Valuation":  {"PERatio": 18.5},
        "SharesStats": {"SharesOutstanding": 1.0e10},
    }
    rec = parse_highlights_section(
        payload, ticker="X.US", snapshot_epoch_ms=1
    )
    assert rec is not None
    assert rec.pe_ratio == 18.5


def test_parse_handles_missing_typed_fields_as_none() -> None:
    payload = {
        "General": {"Name": "X", "Type": "ETF"},
        "Highlights": {"MarketCapitalization": ""},  # empty string normalises to None
        "Financials": {
            "Income_Statement": {
                "yearly": {
                    "2024-12-31": {"date": "2024-12-31", "totalRevenue": ""}
                }
            }
        },
    }
    company, highlights, financials = parse_payload_records(
        payload, ticker="X.US", snapshot_epoch_ms=1
    )
    assert company is not None and company.sector == ""
    assert highlights is not None and highlights.market_cap is None
    assert len(financials) == 1
    assert financials[0].revenue is None
