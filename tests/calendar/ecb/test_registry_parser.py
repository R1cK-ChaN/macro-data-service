"""ECB calendar scaffold tests: INDICATOR_REGISTRY shape + parse_observation behaviour.

Split out of the original tests/test_ecb_calendar_api_scaffold.py as part of
issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

from pathlib import Path
import pytest
from ingestion.calendar.ecb_api import (
    INDICATOR_REGISTRY,
    ECBCalendarEventRecord,
    ECBCalendarRawRecord,
    fetch_ecb_calendar,
    parse_observation,
    project_events,
    store_raw,
)
from ingestion.calendar.ecb_api.parser import PROVIDER, _content_hash
from ingestion.timeseries.sdmx._types import SDMXObservation
from storage.sqlite import SQLiteEngineStore


def _dfr_obs(
    value: float = 4.0, date_str: str = "2024-06-12",
) -> SDMXObservation:
    return SDMXObservation(
        series_id="FM.B.U2.EUR.4F.KR.DFR.LEV",
        date=date_str,
        value=value,
        dataflow="FM",
    )


def _mro_obs(
    value: float = 4.25, date_str: str = "2024-06-12",
) -> SDMXObservation:
    return SDMXObservation(
        series_id="FM.B.U2.EUR.4F.KR.MRR_FR.LEV",
        date=date_str,
        value=value,
        dataflow="FM",
    )


def _mlf_obs(
    value: float = 4.5, date_str: str = "2024-06-12",
) -> SDMXObservation:
    return SDMXObservation(
        series_id="FM.B.U2.EUR.4F.KR.MLFR.LEV",
        date=date_str,
        value=value,
        dataflow="FM",
    )


def test_registry_contains_three_policy_rates_with_expected_shape() -> None:
    mro = INDICATOR_REGISTRY["FM.B.U2.EUR.4F.KR.MRR_FR.LEV"]
    assert mro.indicator == "ECB_MRO"
    assert mro.country_code == "EU"
    assert mro.importance == "high"

    dfr = INDICATOR_REGISTRY["FM.B.U2.EUR.4F.KR.DFR.LEV"]
    assert dfr.indicator == "ECB_DFR"
    assert dfr.dataflow_id == "FM"

    mlf = INDICATOR_REGISTRY["FM.B.U2.EUR.4F.KR.MLFR.LEV"]
    assert mlf.indicator == "ECB_MLF"
    assert mlf.country_code == "EU"


def test_registry_series_keys_are_sdmx_dot_separated() -> None:
    """SDMX keys must not contain the dataflow prefix — the dataflow
    goes in ``dataflow_id`` and is joined by the URL builder."""
    for spec in INDICATOR_REGISTRY.values():
        assert "." in spec.series_key
        assert not spec.series_key.startswith(spec.dataflow_id + ".")


def test_parser_keeps_observation_date_as_event_time() -> None:
    """ECB observations already come back as ISO YYYY-MM-DD effective
    dates; the parser treats them as-is (no promotion needed)."""
    _, event = parse_observation(
        _dfr_obs(value=4.0, date_str="2024-06-12"),
        snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.event_time_utc == "2024-06-12T00:00:00+00:00"
    assert event.event_time_precision == "approximate"
    assert event.reference_date == "2024-06-12"


def test_parser_applies_whitelist_metadata() -> None:
    _, event = parse_observation(
        _dfr_obs(value=4.0),
        snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.provider == PROVIDER == "ecb"
    assert event.country_code == "EU"
    assert event.title == "ECB Deposit Facility Rate"
    assert event.importance == "high"
    assert event.unit == "percent"
    assert event.currency == "EUR"
    assert event.actual == "4.0"
    assert event.source == "ECB"
    assert "data-api.ecb.europa.eu" in event.source_url
    assert "FM/B.U2.EUR.4F.KR.DFR.LEV" in event.source_url


def test_parser_synthesises_deterministic_event_id() -> None:
    """Same series + same observation date → same id; different value
    is a revision, not a new event."""
    a = parse_observation(
        _dfr_obs(value=4.0, date_str="2024-06-12"),
        snapshot_epoch_ms=1_700_000_000_000,
    )[1]
    b = parse_observation(
        _dfr_obs(value=4.25, date_str="2024-06-12"),  # revision
        snapshot_epoch_ms=1_700_000_000_000,
    )[1]
    assert a.provider_event_id == b.provider_event_id

    # Different effective date → different event.
    c = parse_observation(
        _dfr_obs(value=4.0, date_str="2024-09-18"),
        snapshot_epoch_ms=1_700_000_000_000,
    )[1]
    assert c.provider_event_id != a.provider_event_id


def test_parser_distinguishes_sibling_rates_on_same_date() -> None:
    """MRO / DFR / MLF all move together on the same day, but they're
    distinct events — ids must not collide."""
    mro = parse_observation(_mro_obs(date_str="2024-06-12"),
                            snapshot_epoch_ms=0)[1]
    dfr = parse_observation(_dfr_obs(date_str="2024-06-12"),
                            snapshot_epoch_ms=0)[1]
    mlf = parse_observation(_mlf_obs(date_str="2024-06-12"),
                            snapshot_epoch_ms=0)[1]
    ids = {mro.provider_event_id, dfr.provider_event_id, mlf.provider_event_id}
    assert len(ids) == 3


def test_parser_uses_canonical_ecb_tokens_for_id() -> None:
    """``ECB_MRO`` / ``ECB_DFR`` / ``ECB_MLF`` must round-trip through
    ``canonicalize_indicator`` so P6's parity harness can match them
    against TE's "MRO rate" / "Main Refinancing Operations Rate" /
    etc."""
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator("ECB_MRO") == "ECB_MRO"
    assert canonicalize_indicator("ECB_DFR") == "ECB_DFR"
    assert canonicalize_indicator("ECB_MLF") == "ECB_MLF"
    # TE-style labels also canonicalise to the same token.
    assert canonicalize_indicator("MRO rate") == "ECB_MRO"
    assert canonicalize_indicator("Deposit Facility Rate") == "ECB_DFR"


def test_parser_hashes_value_and_date() -> None:
    base = _content_hash({
        "value": 4.0, "date": "2024-06-12",
        "series_id": "FM.B.U2.EUR.4F.KR.DFR.LEV",
    })
    same = _content_hash({
        "value": 4.0, "date": "2024-06-12",
        "series_id": "FM.B.U2.EUR.4F.KR.DFR.LEV",
    })
    revised = _content_hash({
        "value": 4.25, "date": "2024-06-12",
        "series_id": "FM.B.U2.EUR.4F.KR.DFR.LEV",
    })
    assert base == same
    assert base != revised


def test_parser_rejects_unknown_series() -> None:
    rogue = SDMXObservation(
        series_id="FM.B.U2.EUR.NOT.A.REAL.KEY",
        date="2024-06-12",
        value=0.0,
        dataflow="FM",
    )
    with pytest.raises(KeyError):
        parse_observation(rogue, snapshot_epoch_ms=0)
