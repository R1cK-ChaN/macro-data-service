"""Mocked tests for the ECB calendar connector (issue #9 P3).

No real HTTP. The existing SDMX client already has its own live-HTTP
integration suite; this file covers only the calendar projection
layer (parser + projector + fetcher + service op), using a duck-typed
fake client wherever the fetcher needs one.
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


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


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


class _FakeECBClient:
    """Duck-typed stand-in for the ECB SDMX client.

    ``fetch_ecb_calendar`` calls ``get_data`` twice for most paths
    (main window + a one-obs lookback when DB has no prior state), so
    the fake honours ``start_period`` / ``end_period`` / ``limit`` to
    mirror the real server's slicing. Filtering lets the same seeded
    series act as both the main response and the lookback response.
    """

    def __init__(self, by_series_id: dict[str, list[SDMXObservation]]):
        self._data = by_series_id
        self.calls: list[dict] = []

    def get_data(
        self,
        dataflow_id,
        key=".",
        *,
        series_id="",
        start_period=None,
        end_period=None,
        limit=0,
        **kwargs,
    ) -> list[SDMXObservation]:
        self.calls.append({
            "dataflow_id": dataflow_id,
            "key": key,
            "series_id": series_id,
            "start_period": start_period,
            "end_period": end_period,
            "limit": limit,
        })
        rows = list(self._data.get(series_id, []))
        if start_period:
            rows = [r for r in rows if r.date >= start_period]
        if end_period:
            rows = [r for r in rows if r.date <= end_period]
        if limit and limit > 0 and len(rows) > limit:
            rows = sorted(rows, key=lambda r: r.date, reverse=True)[:limit]
        return rows


# ──────────────────────────────────────────────────────────────────────────
# INDICATOR_REGISTRY
# ──────────────────────────────────────────────────────────────────────────


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


# ──────────────────────────────────────────────────────────────────────────
# parse_observation
# ──────────────────────────────────────────────────────────────────────────


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


# ──────────────────────────────────────────────────────────────────────────
# projector
# ──────────────────────────────────────────────────────────────────────────


def test_store_raw_inserts_then_deduplicates_by_content_hash(
    store: SQLiteEngineStore,
) -> None:
    raw, _ = parse_observation(
        _dfr_obs(), snapshot_epoch_ms=1_700_000_000_000,
    )
    with store._connection(commit=True) as conn:
        first = store_raw(conn, [raw])
        second = store_raw(conn, [raw])  # same hash → no-op
    assert first == 1
    assert second == 0


def test_project_events_upserts_and_honors_observed_at_ordering(
    store: SQLiteEngineStore,
) -> None:
    _, first = parse_observation(
        _dfr_obs(value=4.0),
        snapshot_epoch_ms=1_700_000_000_000,
        observed_at_epoch_ms=1_700_000_000_000,
    )
    _, revised = parse_observation(
        _dfr_obs(value=4.25),
        snapshot_epoch_ms=1_700_000_100_000,
        observed_at_epoch_ms=1_700_000_100_000,
    )
    _, stale = parse_observation(
        _dfr_obs(value=99.0),
        snapshot_epoch_ms=1_699_999_900_000,
        observed_at_epoch_ms=1_699_999_900_000,
    )

    with store._connection(commit=True) as conn:
        project_events(conn, [first])
        project_events(conn, [revised])
        project_events(conn, [stale])  # older → ignored

    with store._connection(commit=False) as conn:
        (actual,) = conn.execute(
            "SELECT actual FROM cal_econ_event WHERE provider='ecb'"
        ).fetchone()
    assert actual == "4.25"


def test_projected_rows_surface_in_v_calendar_item(
    store: SQLiteEngineStore,
) -> None:
    raw, event = parse_observation(
        _dfr_obs(value=4.0), snapshot_epoch_ms=1_700_000_000_000,
    )
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw])
        project_events(conn, [event])

    with store._connection(commit=False) as conn:
        rows = [
            tuple(r)
            for r in conn.execute(
                "SELECT provider, country, indicator_id, actual, importance "
                "FROM v_calendar_item WHERE provider='ecb'"
            ).fetchall()
        ]
    assert rows == [("ecb", "EU", None, "4.0", "high")]


# ──────────────────────────────────────────────────────────────────────────
# fetch_ecb_calendar
# ──────────────────────────────────────────────────────────────────────────


def test_fetch_dry_run_returns_plan_without_calling_client(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeECBClient({})
    with store._connection(commit=False) as conn:
        summary = fetch_ecb_calendar(
            conn, client,
            start_period="2024-01-01", end_period="2024-12-31",
            dry_run=True,
        )
    assert summary.dry_run is True
    assert set(summary.series_planned) == set(INDICATOR_REGISTRY.keys())
    assert summary.rows_raw_inserted == 0
    assert summary.events_upserted == 0
    assert client.calls == []


def test_fetch_writes_rows_and_reports_counts(
    store: SQLiteEngineStore,
) -> None:
    """Unbounded backfill: the earliest observation of each series is
    the historical baseline and IS a real calendar event."""
    client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.MRR_FR.LEV": [_mro_obs(4.25, "2024-06-12")],
        "FM.B.U2.EUR.4F.KR.DFR.LEV":    [
            _dfr_obs(4.0, "2024-06-12"),
            _dfr_obs(3.75, "2024-09-18"),
        ],
        "FM.B.U2.EUR.4F.KR.MLFR.LEV":   [_mlf_obs(4.5, "2024-06-12")],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_ecb_calendar(
            conn, client, dry_run=False,
        )
    assert summary.observations_seen == 4
    assert summary.rows_raw_inserted == 4
    assert summary.events_upserted == 4
    assert set(summary.series_ok) == set(INDICATOR_REGISTRY.keys())
    assert summary.series_empty == []
    assert summary.series_unknown == []


def test_fetch_passes_window_params_to_client(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV": [_dfr_obs()],
    })
    with store._connection(commit=True) as conn:
        fetch_ecb_calendar(
            conn, client,
            start_period="2020-01-01", end_period="2025-01-01",
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"],
            dry_run=False, limit=50,
        )
    call = client.calls[0]
    assert call["start_period"] == "2020-01-01"
    assert call["end_period"] == "2025-01-01"
    assert call["limit"] == 50
    assert call["dataflow_id"] == "FM"
    assert call["key"] == "B.U2.EUR.4F.KR.DFR.LEV"
    # The registry key is passed as series_id so SDMXObservation maps
    # back to the correct INDICATOR_REGISTRY entry.
    assert call["series_id"] == "FM.B.U2.EUR.4F.KR.DFR.LEV"


def test_fetch_skips_unknown_series_without_silent_coercion(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV": [_dfr_obs()],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_ecb_calendar(
            conn, client,
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV", "BOGUS_SERIES_ID"],
            dry_run=False,
        )
    assert summary.series_unknown == ["BOGUS_SERIES_ID"]
    assert summary.series_ok == ["FM.B.U2.EUR.4F.KR.DFR.LEV"]


def test_fetch_collapses_flat_rate_periods_to_step_changes(
    store: SQLiteEngineStore,
) -> None:
    """ECB FM policy rates publish at business-daily frequency, so a
    flat six-week period between Governing Council decisions produces
    ~30 identical observations. The fetcher must reduce the series to
    baseline + each step change — otherwise the calendar fills with
    "rate unchanged" rows that aren't real events."""
    flat_then_change = [
        _dfr_obs(4.0,  "2024-06-03"),  # baseline
        _dfr_obs(4.0,  "2024-06-04"),  # flat
        _dfr_obs(4.0,  "2024-06-05"),  # flat
        _dfr_obs(4.0,  "2024-06-06"),  # flat
        _dfr_obs(3.75, "2024-06-12"),  # ← step: rate cut
        _dfr_obs(3.75, "2024-06-13"),  # flat
        _dfr_obs(3.75, "2024-06-14"),  # flat
        _dfr_obs(3.5,  "2024-09-18"),  # ← step: second rate cut
    ]
    client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV": flat_then_change,
    })
    with store._connection(commit=True) as conn:
        summary = fetch_ecb_calendar(
            conn, client,
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"],
            dry_run=False,
        )
    # Baseline + two step changes = 3, not 8.
    assert summary.observations_seen == 3
    assert summary.events_upserted == 3

    with store._connection(commit=False) as conn:
        actuals = sorted(
            row[0] for row in conn.execute(
                "SELECT reference_date FROM cal_econ_event "
                "WHERE provider='ecb' ORDER BY reference_date"
            ).fetchall()
        )
    assert actuals == ["2024-06-03", "2024-06-12", "2024-09-18"]


def test_fetch_keeps_unordered_step_changes(
    store: SQLiteEngineStore,
) -> None:
    """The fetcher must sort observations by date before detecting
    rate changes — the ECB API does not guarantee chronological order
    in the response body."""
    client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV": [
            _dfr_obs(3.75, "2024-06-12"),
            _dfr_obs(4.0,  "2024-06-03"),  # earlier date, arrives second
            _dfr_obs(4.0,  "2024-06-04"),
        ],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_ecb_calendar(
            conn, client,
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"],
            dry_run=False,
        )
    # Sorted chronologically, only two distinct levels exist.
    assert summary.observations_seen == 2


def test_fetch_classifies_first_obs_using_prior_stored_rate(
    store: SQLiteEngineStore,
) -> None:
    """Codex P2 round 3 — bounded windows should use the calendar's
    own prior rate to classify the first observation, not a
    ``bool(start_period)`` heuristic. Two cases in one test:

    1. Prior stored rate = 4.0; first in-window obs = 3.75 → that
       first obs IS a step change and must be projected.
    2. Prior stored rate = 3.75; next in-window obs = 3.75 → flat
       continuation, must NOT be projected.
    """
    # Step 1: seed the DB with a prior 4.0% DFR level on 2024-06-03.
    prior_client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV": [_dfr_obs(4.0, "2024-06-03")],
    })
    with store._connection(commit=True) as conn:
        fetch_ecb_calendar(
            conn, prior_client,
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"], dry_run=False,
        )

    # Step 2: bounded window starting on the actual rate-change date
    # (2024-06-12). The first in-window obs = 3.75 differs from the
    # stored 4.0 → project it.
    client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV": [
            _dfr_obs(3.75, "2024-06-12"),  # real cut
            _dfr_obs(3.75, "2024-06-13"),  # flat continuation
            _dfr_obs(3.5,  "2024-09-18"),  # next real cut
        ],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_ecb_calendar(
            conn, client,
            start_period="2024-06-12", end_period="2024-12-31",
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"],
            dry_run=False,
        )
    # Both step changes at 2024-06-12 and 2024-09-18 are projected;
    # the flat 2024-06-13 is dropped.
    assert summary.observations_seen == 2

    with store._connection(commit=False) as conn:
        refs = sorted(
            r[0] for r in conn.execute(
                "SELECT reference_date FROM cal_econ_event "
                "WHERE provider='ecb' ORDER BY reference_date"
            ).fetchall()
        )
    assert refs == ["2024-06-03", "2024-06-12", "2024-09-18"]


def test_fetch_limit_only_refresh_uses_prior_stored_rate(
    store: SQLiteEngineStore,
) -> None:
    """Codex P2 round 4 — a recurring ``limit``-only refresh with no
    ``start_period`` must still classify the oldest returned level
    against the prior stored rate. Deriving the lookup bound from the
    earliest returned observation makes the path correct regardless
    of which window parameter the caller passed."""
    # Seed: store the current 3.75% level on 2024-06-12.
    with store._connection(commit=True) as conn:
        fetch_ecb_calendar(
            conn, _FakeECBClient({
                "FM.B.U2.EUR.4F.KR.DFR.LEV": [_dfr_obs(3.75, "2024-06-12")],
            }),
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"], dry_run=False,
        )
    # Refresh: lastN-style fetch (no start_period) during a flat rate
    # regime. Client returns the most recent 30 business-day levels,
    # all equal to the stored 3.75.
    refresh = [_dfr_obs(3.75, f"2024-07-{d:02d}") for d in range(1, 11)]
    with store._connection(commit=True) as conn:
        summary = fetch_ecb_calendar(
            conn, _FakeECBClient({
                "FM.B.U2.EUR.4F.KR.DFR.LEV": refresh,
            }),
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"],
            dry_run=False, limit=10,
        )
    # Zero new events — the oldest returned level equals the prior
    # stored rate, so no synthetic window-boundary event is written.
    assert summary.observations_seen == 0
    assert summary.events_upserted == 0

    with store._connection(commit=False) as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider='ecb'"
        ).fetchone()[0]
    assert total == 1


def test_fetch_drops_flat_continuation_when_prior_rate_matches(
    store: SQLiteEngineStore,
) -> None:
    """Mirror case to the prior test: the first in-window obs equals
    the prior stored rate — purely a sliding-window refresh, no new
    event. The fetcher must emit zero rows."""
    # Seed: prior 3.75% level on 2024-06-12.
    with store._connection(commit=True) as conn:
        fetch_ecb_calendar(
            conn, _FakeECBClient({
                "FM.B.U2.EUR.4F.KR.DFR.LEV": [_dfr_obs(3.75, "2024-06-12")],
            }),
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"], dry_run=False,
        )
    # Second fetch: same rate, later dates.
    refresh_client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV": [
            _dfr_obs(3.75, "2024-07-15"),
            _dfr_obs(3.75, "2024-08-12"),
        ],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_ecb_calendar(
            conn, refresh_client,
            start_period="2024-07-01", end_period="2024-08-31",
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"],
            dry_run=False,
        )
    assert summary.observations_seen == 0
    assert summary.events_upserted == 0

    with store._connection(commit=False) as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider='ecb'"
        ).fetchone()[0]
    # DB still contains exactly the 2024-06-12 row from the seed.
    assert total == 1


def test_fetch_reports_empty_series_separately(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV":    [_dfr_obs()],
        "FM.B.U2.EUR.4F.KR.MRR_FR.LEV": [],  # upstream returned nothing
        "FM.B.U2.EUR.4F.KR.MLFR.LEV":   [_mlf_obs()],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_ecb_calendar(conn, client, dry_run=False)
    assert "FM.B.U2.EUR.4F.KR.MRR_FR.LEV" in summary.series_empty
    assert "FM.B.U2.EUR.4F.KR.DFR.LEV" in summary.series_ok
    assert "FM.B.U2.EUR.4F.KR.MLFR.LEV" in summary.series_ok


# ──────────────────────────────────────────────────────────────────────────
# Service op wiring
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_ecb", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert set(result["series_planned"]) == set(INDICATOR_REGISTRY.keys())


def test_service_op_honors_explicit_series_ids(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_ecb",
        {"dry_run": True, "series_ids": ["FM.B.U2.EUR.4F.KR.DFR.LEV"]},
    )
    assert result["series_planned"] == ["FM.B.U2.EUR.4F.KR.DFR.LEV"]


def test_service_op_dry_run_surfaces_unknown_series(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_ecb",
        {"dry_run": True,
         "series_ids": ["FM.B.U2.EUR.4F.KR.DFR.LEV", "BOGUS"]},
    )
    assert result["series_planned"] == ["FM.B.U2.EUR.4F.KR.DFR.LEV"]
    assert result["series_unknown"] == ["BOGUS"]


def test_service_op_passes_window_through(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_ecb",
        {"dry_run": True,
         "start_period": "2023-01-01", "end_period": "2024-01-01"},
    )
    assert result["start_period"] == "2023-01-01"
    assert result["end_period"] == "2024-01-01"
