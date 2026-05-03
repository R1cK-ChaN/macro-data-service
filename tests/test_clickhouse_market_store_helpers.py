from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from storage.clickhouse.store import compute_dividend_hash, compute_split_hash


def test_dividend_hash_tracks_mutable_eodhd_fields() -> None:
    base = compute_dividend_hash(
        instrument_id="US_AAPL",
        ex_date="2026-02-09",
        cash_amount=0.25,
        unadjusted_amount=0.25,
        currency="USD",
        period="Quarterly",
        declaration_date="2026-01-30",
    )
    changed_payment_date = compute_dividend_hash(
        instrument_id="US_AAPL",
        ex_date="2026-02-09",
        cash_amount=0.25,
        unadjusted_amount=0.25,
        currency="USD",
        period="Quarterly",
        declaration_date="2026-01-30",
        payment_date="2026-02-15",
    )

    assert base != changed_payment_date


def test_split_hash_is_independent_of_instrument_id() -> None:
    aapl = compute_split_hash(
        instrument_id="US_AAPL",
        execution_date="2020-08-31",
        to_factor=4.0,
        from_factor=1.0,
    )
    msft = compute_split_hash(
        instrument_id="US_MSFT",
        execution_date="2020-08-31",
        to_factor=4.0,
        from_factor=1.0,
    )

    assert aapl == msft
