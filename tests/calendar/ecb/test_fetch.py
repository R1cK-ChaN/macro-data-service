"""ECB calendar scaffold tests: fetch_ecb_calendar dry-run / persistence / step-change collapsing.

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


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


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
