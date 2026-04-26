"""EODHD scaffold tests: subtype registry + per-row parsers (earnings, trend, IPO, split, dividend, dividend_detail).

Split out of the original tests/test_eodhd_api_scaffold.py as part of
issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

import json
import pytest
import respx

from ingestion.calendar.eodhd_api import (
    SUBTYPES,
    parse_dividend_detail_row,
    parse_dividend_row,
    parse_earnings_row,
    parse_ipo_row,
    parse_split_row,
    parse_trend_row,
    synthesize_provider_event_id,
)


def _earnings_row(**overrides):
    base = {
        "code": "AAPL.US",
        "report_date": "2026-05-01",
        "date": "2026-04-30",
        "before_after_market": "AfterMarket",
        "currency": "USD",
        "actual": 1.53,
        "estimate": 1.50,
        "difference": 0.03,
        "percent": 2.0,
    }
    base.update(overrides)
    return base


def _trend_row(**overrides):
    base = {
        "code": "AAPL.US",
        "date": "2026-05-01",
        "period": "0q",
        "growth": "0.05",
        "earningsEstimateAvg": "1.50",
        "earningsEstimateLow": "1.40",
        "earningsEstimateHigh": "1.60",
        "earningsEstimateYearAgoEps": "1.40",
        "earningsEstimateNumberOfAnalysts": "30",
        "earningsEstimateGrowth": "0.07",
        "revenueEstimateAvg": "90000000000",
        "revenueEstimateLow": "88000000000",
        "revenueEstimateHigh": "92000000000",
        "revenueEstimateNumberOfAnalysts": "28",
        "epsTrendCurrent": "1.50",
        "epsTrend7daysAgo": "1.49",
        "epsTrend30daysAgo": "1.48",
        "epsTrend60daysAgo": "1.47",
        "epsTrend90daysAgo": "1.45",
        "epsRevisionsUpLast7days": "3",
        "epsRevisionsUpLast30days": "5",
    }
    base.update(overrides)
    return base


def _ipo_row(**overrides):
    base = {
        "code": "NEWCO.US",
        "name": "New Company Inc",
        "exchange": "NASDAQ",
        "currency": "USD",
        "start_date": "2026-06-15",
        "filing_date": "2026-04-01",
        "amended_date": None,
        "price_from": 18.0,
        "price_to": 22.0,
        "offer_price": 20.0,
        "shares": 10_000_000,
        "deal_type": "Expected",
    }
    base.update(overrides)
    return base


def _split_row(**overrides):
    base = {
        "code": "NVDA.US",
        "split_date": "2026-06-10",
        "optionable": "Y",
        "old_shares": 1,
        "new_shares": 10,
    }
    base.update(overrides)
    return base


def _dividend_row(**overrides):
    # /calendar/dividends is discovery-only — rows are just (symbol, date).
    # Validated against live EODHD on 2026-04-21 for AAPL.US / filter[date_eq]:
    # no value, period, currency, or declaration/record/payment fields
    # arrived even for major US tickers. EODHD's blog on the "extended"
    # dividend fields refers to the per-ticker /api/div/{TICKER} endpoint,
    # not to this calendar feed.
    base = {
        "symbol": "MSFT.US",
        "date": "2026-05-15",
    }
    base.update(overrides)
    return base


def _dividend_detail_row(**overrides):
    # /api/div/{TICKER}.{EXCHANGE} extended shape. Major US/EU tickers
    # return this rich form; smaller symbols may return just
    # {date, value}. Tests cover both.
    base = {
        "date": "2026-02-09",
        "value": 0.24,
        "unadjustedValue": 0.24,
        "currency": "USD",
        "declarationDate": "2026-01-30",
        "recordDate": "2026-02-10",
        "paymentDate": "2026-02-13",
        "period": "Quarterly",
    }
    base.update(overrides)
    return base


def test_subtype_registry_matches_schema_check() -> None:
    assert SUBTYPES == {"earnings", "earnings_trend", "ipo", "split", "dividend"}


def test_parse_earnings_round_trip() -> None:
    row = _earnings_row()
    raw, event = parse_earnings_row(row, snapshot_epoch_ms=1_700_000_000_000)
    assert event.event_subtype == "earnings"
    assert event.ticker == "AAPL"
    assert event.exchange == "US"
    assert event.event_time_utc == "2026-05-01"
    assert event.event_time_precision == "date"
    assert event.currency == "USD"
    assert event.reference_date == "2026-04-30"
    # payload_json preserves every input field.
    for k, v in row.items():
        assert json.loads(raw.payload_json)[k] == v


def test_earnings_id_stable_across_value_revision() -> None:
    baseline = _earnings_row()
    revision = _earnings_row(actual=1.60, difference=0.10, percent=6.0)
    _, ev_a = parse_earnings_row(baseline, snapshot_epoch_ms=1)
    _, ev_b = parse_earnings_row(revision, snapshot_epoch_ms=2)
    # Same code + report_date + baf → same provider_event_id
    assert ev_a.provider_event_id == ev_b.provider_event_id
    # But the content hash moved (actual changed)
    assert ev_a.content_hash != ev_b.content_hash


def test_earnings_id_flips_when_before_after_market_changes() -> None:
    morning = _earnings_row(before_after_market="BeforeMarket")
    evening = _earnings_row(before_after_market="AfterMarket")
    _, ev_m = parse_earnings_row(morning, snapshot_epoch_ms=1)
    _, ev_e = parse_earnings_row(evening, snapshot_epoch_ms=1)
    assert ev_m.provider_event_id != ev_e.provider_event_id


def test_earnings_rejects_missing_dates() -> None:
    with pytest.raises(ValueError):
        parse_earnings_row({"code": "AAPL.US"}, snapshot_epoch_ms=0)


def test_trend_id_forks_by_period() -> None:
    q0 = _trend_row(period="0q")
    q1 = _trend_row(period="+1q")
    _, ev_q0 = parse_trend_row(q0, snapshot_epoch_ms=1)
    _, ev_q1 = parse_trend_row(q1, snapshot_epoch_ms=1)
    assert ev_q0.provider_event_id != ev_q1.provider_event_id
    assert ev_q0.event_subtype == "earnings_trend"
    assert ev_q0.reference_date == "0q"


def test_ipo_primary_date_falls_back_to_filing_date() -> None:
    row = _ipo_row(start_date=None)
    _, event = parse_ipo_row(row, snapshot_epoch_ms=1)
    assert event.event_time_utc == "2026-04-01"
    assert event.event_time_precision == "approximate"  # deal_type=Expected


def test_ipo_precision_date_when_priced() -> None:
    row = _ipo_row(deal_type="Priced")
    _, event = parse_ipo_row(row, snapshot_epoch_ms=1)
    assert event.event_time_precision == "date"


def test_ipo_id_stable_when_start_date_appears_later() -> None:
    """At filing time EODHD ships filing_date only; start_date arrives
    later. The event id must anchor on filing_date so the same listing
    keeps a single cal_corp_event row instead of forking into two."""
    early = _ipo_row(start_date=None, amended_date=None)  # filing stage
    later = _ipo_row(start_date="2026-06-15")             # start_date populated
    _, ev_early = parse_ipo_row(early, snapshot_epoch_ms=1)
    _, ev_later = parse_ipo_row(later, snapshot_epoch_ms=2)
    assert ev_early.provider_event_id == ev_later.provider_event_id
    # The projected event_time_utc still advances to the best-known date.
    assert ev_early.event_time_utc == "2026-04-01"
    assert ev_later.event_time_utc == "2026-06-15"


def test_ipo_id_independent_of_deal_type() -> None:
    """IPO id must stay stable across the Filed → Priced lifecycle so the
    two raw snapshots land on the same event row rather than forking
    into two different events."""
    filed = _ipo_row(deal_type="Filed")
    priced = _ipo_row(deal_type="Priced")
    _, ev_f = parse_ipo_row(filed, snapshot_epoch_ms=1)
    _, ev_p = parse_ipo_row(priced, snapshot_epoch_ms=2)
    assert ev_f.provider_event_id == ev_p.provider_event_id
    assert ev_f.content_hash != ev_p.content_hash  # lifecycle flipped


def test_split_ratio_encoded_in_id() -> None:
    one_ten = _split_row(old_shares=1, new_shares=10)
    two_ten = _split_row(old_shares=2, new_shares=10)
    _, ev_a = parse_split_row(one_ten, snapshot_epoch_ms=1)
    _, ev_b = parse_split_row(two_ten, snapshot_epoch_ms=1)
    assert ev_a.provider_event_id != ev_b.provider_event_id


def test_dividend_parses_discovery_only_shape() -> None:
    """Live EODHD /calendar/dividends returns just {symbol, date} — no
    value/period/currency/declaration/record/payment dates. The parser
    must accept that minimal shape without raising, and the resulting
    event row must leave currency/reference_date empty (the richer
    fields are sourced from /api/div/{TICKER} in a follow-up slice)."""
    row = _dividend_row()
    _raw, event = parse_dividend_row(row, snapshot_epoch_ms=1)
    assert event.ticker == "MSFT"
    assert event.exchange == "US"
    assert event.event_time_utc == "2026-05-15"
    assert event.event_subtype == "dividend"
    assert event.currency == ""
    assert event.reference_date is None


def test_dividend_id_stable_for_symbol_date_pair() -> None:
    """(symbol, date) is the natural key for a dividend calendar entry.
    Two identical rows produce the same provider_event_id; changing
    either component forks it."""
    a = _dividend_row(symbol="AAPL.US", date="2026-02-09")
    b = _dividend_row(symbol="AAPL.US", date="2026-02-09")
    c = _dividend_row(symbol="AAPL.US", date="2026-05-15")  # different date
    _, ev_a = parse_dividend_row(a, snapshot_epoch_ms=1)
    _, ev_b = parse_dividend_row(b, snapshot_epoch_ms=1)
    _, ev_c = parse_dividend_row(c, snapshot_epoch_ms=1)
    assert ev_a.provider_event_id == ev_b.provider_event_id
    assert ev_a.provider_event_id != ev_c.provider_event_id


def test_synthesize_provider_event_id_is_deterministic() -> None:
    a = synthesize_provider_event_id(
        subtype="earnings", code="AAPL.US", primary_date="2026-05-01", subtype_key="AfterMarket"
    )
    b = synthesize_provider_event_id(
        subtype="earnings", code="AAPL.US", primary_date="2026-05-01", subtype_key="AfterMarket"
    )
    assert a == b
    c = synthesize_provider_event_id(
        subtype="earnings", code="AAPL.US", primary_date="2026-05-01", subtype_key="BeforeMarket"
    )
    assert a != c


@respx.mock
def test_dividend_detail_parser_populates_extended_fields() -> None:
    row = _dividend_detail_row()
    raw, event = parse_dividend_detail_row(
        row, code="AAPL.US", snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.event_subtype == "dividend"
    assert event.ticker == "AAPL"
    assert event.exchange == "US"
    assert event.event_time_utc == "2026-02-09"
    assert event.currency == "USD"
    assert event.reference_date == "Quarterly"
    # Raw payload preserves every detail field for downstream extraction.
    payload = json.loads(raw.payload_json)
    for k in ("value", "unadjustedValue", "declarationDate", "recordDate", "paymentDate"):
        assert payload[k] == row[k]


def test_dividend_detail_shares_identity_with_discovery_row() -> None:
    """Detail parser must produce the same provider_event_id the
    discovery parser produced for the same (symbol, ex_date) — that is
    how the enrichment snapshot lands on the existing cal_corp_event
    row instead of forking into a new event."""
    code = "AAPL.US"
    ex_date = "2026-02-09"
    discovery = _dividend_row(symbol=code, date=ex_date)
    detail = _dividend_detail_row(date=ex_date)
    _, ev_d = parse_dividend_row(discovery, snapshot_epoch_ms=1)
    _, ev_x = parse_dividend_detail_row(detail, code=code, snapshot_epoch_ms=2)
    assert ev_d.provider_event_id == ev_x.provider_event_id
    # Content hashes differ — discovery hashes {date}, detail hashes 8
    # extended fields — so both land as separate cal_corp_raw snapshots.
    assert ev_d.content_hash != ev_x.content_hash


def test_dividend_detail_handles_minimal_shape() -> None:
    """Smaller tickers only return {date, value}. Parser must not raise
    on the missing extended fields; currency/reference_date stay empty."""
    row = {"date": "2026-02-09", "value": 0.05}
    _raw, event = parse_dividend_detail_row(
        row, code="SMALL.US", snapshot_epoch_ms=1,
    )
    assert event.currency == ""
    assert event.reference_date is None


def test_dividend_detail_requires_code_and_date() -> None:
    with pytest.raises(ValueError):
        parse_dividend_detail_row({"date": "2026-02-09"}, code="", snapshot_epoch_ms=1)
    with pytest.raises(ValueError):
        parse_dividend_detail_row({}, code="AAPL.US", snapshot_epoch_ms=1)
