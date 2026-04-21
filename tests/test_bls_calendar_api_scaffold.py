"""Mocked tests for the BLS calendar connector (issue #9 P1).

No real HTTP. The existing :class:`ingestion.timeseries.scrapers.bls.BLSClient`
already has its own live-HTTP integration suite; this file covers only
the calendar projection layer (parser + projector + fetcher + service
op), using a duck-typed fake client wherever the fetcher needs one.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.bls_api import (
    INDICATOR_REGISTRY,
    BLSCalendarEventRecord,
    BLSCalendarRawRecord,
    fetch_bls_calendar,
    parse_observation,
    project_events,
    store_raw,
)
from ingestion.calendar.bls_api.parser import PROVIDER, _content_hash
from ingestion.timeseries.scrapers.bls import BLSObservation
from storage.sqlite import SQLiteEngineStore


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _cpi_obs(value: float = 312.5, period: str = "M04") -> BLSObservation:
    return BLSObservation(
        series_id="CUUR0000SA0",
        date=f"2024-{int(period[1:]):02d}-01",
        value=value,
        period=period,
    )


def _nfp_obs(value: float = 158_234.0) -> BLSObservation:
    return BLSObservation(
        series_id="CES0000000001",
        date="2024-04-01",
        value=value,
        period="M04",
    )


class _FakeBLSClient:
    """Duck-typed stand-in for the BLS HTTP client.

    ``fetch_bls_calendar`` calls exactly one method: ``get_series``.
    We return a preseeded mapping so tests don't touch the network."""

    def __init__(self, series_to_obs: dict[str, list[BLSObservation]]):
        self._data = series_to_obs
        self.api_key = "fake-key"
        self.calls: list[tuple[tuple, dict]] = []

    def get_series(
        self,
        series_ids,
        *,
        start_year=None,
        end_year=None,
    ) -> dict[str, list[BLSObservation]]:
        self.calls.append(((tuple(series_ids),), {
            "start_year": start_year, "end_year": end_year,
        }))
        return {sid: self._data.get(sid, []) for sid in series_ids}


# ──────────────────────────────────────────────────────────────────────────
# INDICATOR_REGISTRY
# ──────────────────────────────────────────────────────────────────────────


def test_registry_contains_cpi_and_nfp_with_expected_shape() -> None:
    cpi = INDICATOR_REGISTRY["CUUR0000SA0"]
    assert cpi.indicator == "CPI"
    assert cpi.country_code == "US"
    assert cpi.importance == "high"
    nfp = INDICATOR_REGISTRY["CES0000000001"]
    assert nfp.indicator == "NFP"
    assert nfp.country_code == "US"


@pytest.mark.parametrize(
    "series_id,expected_indicator,expected_unit,expected_category",
    [
        # P1c inflation additions.
        ("CUUR0000SA0L1E",           "Core CPI",                "index",     "Inflation"),
        ("WPSFD4",                   "PPI",                     "index",     "Inflation"),
        ("WPSFD49116",               "Core PPI",                "index",     "Inflation"),
        # P1c employment additions.
        ("LNS14000000",              "Unemployment Rate",       "percent",   "Employment"),
        ("CES0500000003",            "Average Hourly Earnings", "usd",       "Employment"),
        ("CES0500000002",            "Average Weekly Hours",    "hours",     "Employment"),
        ("JTS000000000000000JOL",    "JOLTS",                   "thousands", "Employment"),
        ("CIU1010000000000A",        "Employment Cost Index",   "percent",   "Employment"),
        # P1c productivity addition (quarterly).
        ("PRS85006092",              "Productivity",            "index",     "Productivity"),
    ],
)
def test_registry_p1c_additions_have_expected_shape(
    series_id: str,
    expected_indicator: str,
    expected_unit: str,
    expected_category: str,
) -> None:
    spec = INDICATOR_REGISTRY[series_id]
    assert spec.series_id == series_id
    assert spec.indicator == expected_indicator
    assert spec.country_code == "US"
    assert spec.unit == expected_unit
    assert spec.category == expected_category
    assert spec.importance in {"high", "medium", "low"}
    assert spec.title  # non-empty


def test_registry_productivity_opts_out_of_default_api_fetch() -> None:
    """Codex P1c round 2 — Productivity has staged preliminary /
    revised releases; until their schedule/API merge alignment is
    designed, the API path is opt-in rather than on by default."""
    spec = INDICATOR_REGISTRY["PRS85006092"]
    assert spec.api_fetch is False


def test_registry_non_productivity_specs_opt_in_to_default_api_fetch() -> None:
    """Every other P1c indicator still defaults to API-fetched."""
    for series_id, spec in INDICATOR_REGISTRY.items():
        if series_id == "PRS85006092":
            continue
        assert spec.api_fetch is True, f"{series_id} should default to api_fetch=True"


def test_registry_indicators_canonicalize_to_distinct_tokens() -> None:
    """Codex P1c — the canonicalised ``indicator`` field is what
    :func:`synthesize_event_id` hashes on. Two registry entries that
    collapse to the same canonical token would silently merge two
    distinct indicators into one cal_econ_event row."""
    from ingestion.calendar._official_shared import canonicalize_indicator

    canonical_to_ids: dict[str, list[str]] = {}
    for series_id, spec in INDICATOR_REGISTRY.items():
        token = canonicalize_indicator(spec.indicator)
        canonical_to_ids.setdefault(token, []).append(series_id)
    collisions = {t: ids for t, ids in canonical_to_ids.items() if len(ids) > 1}
    assert collisions == {}, f"indicator-token collisions: {collisions}"


# ──────────────────────────────────────────────────────────────────────────
# parse_observation
# ──────────────────────────────────────────────────────────────────────────


def test_parser_promotes_monthly_reference_to_month_end() -> None:
    """Monthly BLS observations come back as YYYY-MM-01; the calendar
    event should use month-end (30 Apr, not 1 Apr) as ``event_time_utc``
    so the approximate release-time placeholder sits after the
    reference period closed."""
    obs = _cpi_obs(value=312.5, period="M04")  # → 2024-04-01
    raw, event = parse_observation(obs, snapshot_epoch_ms=1_700_000_000_000)

    assert event.event_time_utc == "2024-04-30T00:00:00+00:00"
    assert event.event_time_precision == "approximate"
    assert event.reference_date == "2024-04-01"
    assert event.reference_label == "M04"


def test_parser_applies_whitelist_metadata() -> None:
    raw, event = parse_observation(
        _cpi_obs(value=312.5),
        snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.provider == PROVIDER == "bls"
    assert event.country_code == "US"
    assert event.title == "Consumer Price Index"
    assert event.importance == "high"
    assert event.unit == "index"
    assert event.actual == "312.5"
    assert event.source == "BLS"
    assert event.source_url.endswith("/CUUR0000SA0")


def test_parser_synthesises_deterministic_event_id() -> None:
    """Same observation → same id; different value is a *revision*,
    not a new event — same id."""
    a = parse_observation(
        _cpi_obs(value=312.5),
        snapshot_epoch_ms=1_700_000_000_000,
    )[1]
    b = parse_observation(
        _cpi_obs(value=313.0),  # revised
        snapshot_epoch_ms=1_700_000_000_000,
    )[1]
    assert a.provider_event_id == b.provider_event_id

    # Different period = different event.
    c = parse_observation(
        _cpi_obs(value=312.5, period="M05"),
        snapshot_epoch_ms=1_700_000_000_000,
    )[1]
    assert c.provider_event_id != a.provider_event_id


def test_parser_hashes_value_and_footnotes() -> None:
    """Revision signal is the content hash — same value + footnotes
    hash identically, any change bumps the hash."""
    base = _content_hash({"value": "312.5", "footnotes": [], "periodName": "April"})
    same = _content_hash({"value": "312.5", "footnotes": [], "periodName": "April"})
    revised = _content_hash({"value": "313.0", "footnotes": [], "periodName": "April"})
    assert base == same
    assert base != revised


def test_parser_id_is_stable_when_event_time_utc_changes() -> None:
    """Codex P2 — the P1a schedule scraper will promote the placeholder
    ``event_time_utc`` (reference month-end) to the true scheduled
    release datetime. The ``provider_event_id`` anchors on the
    observation's reference-period date (``obs.date``, stable across
    that promotion), not on ``event_time_utc``, so the projector
    upserts the same row instead of inserting a duplicate."""
    _, event = parse_observation(_cpi_obs(), snapshot_epoch_ms=0)
    # Same reference period, different synthesised placeholder: the
    # hash must not depend on any value that's about to change.
    from ingestion.calendar._official_shared import (
        canonicalize_indicator,
        synthesize_event_id,
    )
    recomputed_with_real_schedule = synthesize_event_id(
        "bls", "US", canonicalize_indicator("CPI"), "2024-04-01",
    )
    assert event.provider_event_id == recomputed_with_real_schedule


def test_parser_hashes_footnote_revisions_when_raw_dict_is_available() -> None:
    """Codex P2 — a footnote-only BLS revision must produce a new
    content hash so the raw audit lane captures the correction.
    Requires ``obs.raw`` to be populated with the full upstream dict
    (BLSClient does this; tests that construct BLSObservation without
    ``raw`` fall back to a minimal reconstruction and lose footnote
    changes — accepted trade-off documented in the parser)."""
    base = BLSObservation(
        series_id="CUUR0000SA0",
        date="2024-04-01",
        value=312.5,
        period="M04",
        raw={"year": "2024", "period": "M04", "value": "312.5",
             "footnotes": [{}], "periodName": "April"},
    )
    footnote_only_revision = BLSObservation(
        series_id="CUUR0000SA0",
        date="2024-04-01",
        value=312.5,
        period="M04",
        raw={"year": "2024", "period": "M04", "value": "312.5",
             "footnotes": [{"code": "P", "text": "preliminary"}],
             "periodName": "April"},
    )
    raw_a, _ = parse_observation(base, snapshot_epoch_ms=1_700_000_000_000)
    raw_b, _ = parse_observation(
        footnote_only_revision, snapshot_epoch_ms=1_700_000_100_000,
    )
    assert raw_a.content_hash != raw_b.content_hash


def test_parser_rejects_unknown_series() -> None:
    rogue = BLSObservation(
        series_id="UNKNOWN_SERIES",
        date="2024-04-01",
        value=0.0,
        period="M04",
    )
    with pytest.raises(KeyError):
        parse_observation(rogue, snapshot_epoch_ms=0)


# ──────────────────────────────────────────────────────────────────────────
# projector
# ──────────────────────────────────────────────────────────────────────────


def test_store_raw_inserts_then_deduplicates_by_content_hash(
    store: SQLiteEngineStore,
) -> None:
    raw, _ = parse_observation(_cpi_obs(), snapshot_epoch_ms=1_700_000_000_000)
    with store._connection(commit=True) as conn:
        first = store_raw(conn, [raw])
        second = store_raw(conn, [raw])  # same content hash → no-op
    assert first == 1
    assert second == 0


def test_project_events_upserts_and_honors_observed_at_ordering(
    store: SQLiteEngineStore,
) -> None:
    _, first_event = parse_observation(
        _cpi_obs(value=312.5),
        snapshot_epoch_ms=1_700_000_000_000,
        observed_at_epoch_ms=1_700_000_000_000,
    )
    _, revised_event = parse_observation(
        _cpi_obs(value=313.0),
        snapshot_epoch_ms=1_700_000_100_000,
        observed_at_epoch_ms=1_700_000_100_000,
    )
    _, stale_event = parse_observation(
        _cpi_obs(value=999.0),
        snapshot_epoch_ms=1_699_999_900_000,
        observed_at_epoch_ms=1_699_999_900_000,
    )

    with store._connection(commit=True) as conn:
        project_events(conn, [first_event])
        project_events(conn, [revised_event])
        project_events(conn, [stale_event])  # older → must be ignored

    with store._connection(commit=False) as conn:
        (actual,) = conn.execute(
            "SELECT actual FROM cal_econ_event WHERE provider='bls'"
        ).fetchone()
    assert actual == "313.0"


def test_projected_rows_surface_in_v_calendar_item(
    store: SQLiteEngineStore,
) -> None:
    raw, event = parse_observation(
        _cpi_obs(value=312.5),
        snapshot_epoch_ms=1_700_000_000_000,
    )
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw])
        project_events(conn, [event])

    with store._connection(commit=False) as conn:
        rows = [
            tuple(r)
            for r in conn.execute(
                "SELECT provider, country, indicator_id, actual, importance "
                "FROM v_calendar_item WHERE provider='bls'"
            ).fetchall()
        ]
    assert rows == [("bls", "US", None, "312.5", "high")]


# ──────────────────────────────────────────────────────────────────────────
# fetch_bls_calendar
# ──────────────────────────────────────────────────────────────────────────


def test_fetch_dry_run_returns_plan_without_calling_client(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeBLSClient({})
    with store._connection(commit=False) as conn:
        summary = fetch_bls_calendar(
            conn, client,
            start_year=2023, end_year=2024,
            dry_run=True,
        )
    assert summary.dry_run is True
    expected = {
        sid for sid, spec in INDICATOR_REGISTRY.items() if spec.api_fetch
    }
    assert set(summary.series_planned) == expected
    assert summary.rows_raw_inserted == 0
    assert summary.events_upserted == 0
    assert client.calls == []  # no HTTP call attempted


def test_fetch_default_plan_excludes_api_fetch_false_series(
    store: SQLiteEngineStore,
) -> None:
    """Codex P1c round 2 — series with ``api_fetch=False`` (currently
    Productivity) are excluded from the default-full-registry API
    fetch because their staged schedule has no clean anchor alignment
    with the bare-date API observation."""
    client = _FakeBLSClient({})
    with store._connection(commit=False) as conn:
        summary = fetch_bls_calendar(
            conn, client,
            start_year=2024, end_year=2024,
            dry_run=True,
        )
    opted_out = {
        sid for sid, spec in INDICATOR_REGISTRY.items() if not spec.api_fetch
    }
    assert opted_out, "expected at least one api_fetch=False spec"
    assert not (set(summary.series_planned) & opted_out)


def test_fetch_honors_explicit_api_fetch_false_series(
    store: SQLiteEngineStore,
) -> None:
    """Explicit ``series_ids=`` override the default filter: callers
    who know what they're doing can still exercise the API path for
    an ``api_fetch=False`` series (e.g. ad-hoc backfill)."""
    client = _FakeBLSClient({})
    opted_out = [
        sid for sid, spec in INDICATOR_REGISTRY.items() if not spec.api_fetch
    ]
    assert opted_out
    with store._connection(commit=False) as conn:
        summary = fetch_bls_calendar(
            conn, client,
            start_year=2024, end_year=2024,
            series_ids=opted_out,
            dry_run=True,
        )
    assert set(summary.series_planned) == set(opted_out)
    assert summary.series_unknown == []


def test_fetch_writes_rows_and_reports_counts(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeBLSClient({
        "CUUR0000SA0": [_cpi_obs(312.5, "M03"), _cpi_obs(313.0, "M04")],
        "CES0000000001": [_nfp_obs(158_234.0)],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_bls_calendar(
            conn, client,
            start_year=2024, end_year=2024,
            series_ids=["CUUR0000SA0", "CES0000000001"],
            dry_run=False,
        )
    assert summary.observations_seen == 3
    assert summary.rows_raw_inserted == 3
    assert summary.events_upserted == 3
    assert set(summary.series_ok) == {"CUUR0000SA0", "CES0000000001"}
    assert summary.series_empty == []
    assert summary.series_unknown == []


def test_fetch_skips_unknown_series_without_silent_coercion(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeBLSClient({
        "CUUR0000SA0": [_cpi_obs()],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_bls_calendar(
            conn, client,
            start_year=2024, end_year=2024,
            series_ids=["CUUR0000SA0", "BOGUS_SERIES_ID"],
            dry_run=False,
        )
    assert summary.series_unknown == ["BOGUS_SERIES_ID"]
    assert summary.series_ok == ["CUUR0000SA0"]
    # Unknown series shouldn't be included in the HTTP call.
    assert client.calls[0][0][0] == ("CUUR0000SA0",)


def test_fetch_reports_empty_series_separately(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeBLSClient({
        "CUUR0000SA0": [_cpi_obs()],
        "CES0000000001": [],  # BLS key unset upstream returns no rows
    })
    with store._connection(commit=True) as conn:
        summary = fetch_bls_calendar(
            conn, client,
            start_year=2024, end_year=2024,
            dry_run=False,
        )
    assert "CES0000000001" in summary.series_empty
    assert "CUUR0000SA0" in summary.series_ok


# ──────────────────────────────────────────────────────────────────────────
# Service op wiring
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_bls", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    expected = {
        sid for sid, spec in INDICATOR_REGISTRY.items() if spec.api_fetch
    }
    assert set(result["series_planned"]) == expected


def test_service_op_honors_explicit_series_ids(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_bls",
        {"dry_run": True, "series_ids": ["CUUR0000SA0"]},
    )
    assert result["series_planned"] == ["CUUR0000SA0"]


def test_service_op_dry_run_surfaces_unknown_series(
    store: SQLiteEngineStore,
) -> None:
    """Codex P3 — dry-run and execute must agree on the known/unknown
    split so callers can't misread an invalid series id as runnable."""
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_bls",
        {"dry_run": True, "series_ids": ["CUUR0000SA0", "BOGUS"]},
    )
    assert result["series_planned"] == ["CUUR0000SA0"]
    assert result["series_unknown"] == ["BOGUS"]
