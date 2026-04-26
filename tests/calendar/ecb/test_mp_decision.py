"""ECB calendar scaffold tests: MP-decision ↔ SDMX rate reconciliation (P3a follow-up).

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


def _seed_mp_decision_row(
    conn,
    *,
    decision_date: str,
    event_time_utc: str,
) -> str:
    """Write one ECB_MP_DECISION schedule row under the anchor its
    schedule scraper would emit. Returns the ``provider_event_id``
    for assertions."""
    from ingestion.calendar._official_shared import (
        canonicalize_indicator,
        synthesize_event_id,
    )
    from ingestion.calendar.ecb_api import ECB_MP_DECISION_SPEC

    event_id = synthesize_event_id(
        PROVIDER,
        ECB_MP_DECISION_SPEC.country_code,
        canonicalize_indicator(ECB_MP_DECISION_SPEC.indicator),
        decision_date,
    )
    conn.execute(
        """
        INSERT INTO cal_econ_event (
            provider, provider_event_id, event_time_utc, event_time_precision,
            reference_date, reference_label, country_code, indicator_id,
            category, title, importance, currency, unit,
            actual, previous, revised, forecast, consensus_forecast,
            ticker, source, source_url, content_hash,
            last_update_epoch_ms, observed_at_epoch_ms,
            created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            PROVIDER, event_id, event_time_utc, "datetime",
            decision_date, decision_date,
            ECB_MP_DECISION_SPEC.country_code, None,
            ECB_MP_DECISION_SPEC.category,
            ECB_MP_DECISION_SPEC.title,
            ECB_MP_DECISION_SPEC.importance,
            "EUR", ECB_MP_DECISION_SPEC.unit,
            None, None, None, None, None,
            "", "ECB",
            "https://www.ecb.europa.eu/press/calendars/mgcgc/html/index.en.html",
            "schedule:mp_decision",
            None, 1_700_000_000_000,
            "2025-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00",
        ),
    )
    return event_id


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def test_fetch_promotes_rate_timestamp_onto_preceding_mp_decision(
    store: SQLiteEngineStore,
) -> None:
    """The rate-level observation on effective date 2024-06-19 gets
    its ``event_time_utc`` promoted to the 14:15 CET decision
    datetime and ``event_time_precision`` upgraded to ``datetime``.
    The row's ``provider_event_id`` stays anchored on the effective
    date so identical re-fetches upsert onto the same row rather
    than forking into a decision-anchored duplicate."""
    decision_utc = "2024-06-12T12:15:00+00:00"  # 14:15 CEST
    with store._connection(commit=True) as conn:
        _seed_mp_decision_row(
            conn,
            decision_date="2024-06-12",
            event_time_utc=decision_utc,
        )

    client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV": [_dfr_obs(3.75, "2024-06-19")],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_ecb_calendar(
            conn, client,
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"],
            dry_run=False,
        )

    assert summary.observations_seen == 1
    assert summary.decision_anchor_fallbacks == 0

    from ingestion.calendar._official_shared import (
        canonicalize_indicator,
        synthesize_event_id,
    )
    effective_date_id = synthesize_event_id(
        PROVIDER, "EU", canonicalize_indicator("ECB_DFR"), "2024-06-19",
    )
    decision_date_id = synthesize_event_id(
        PROVIDER, "EU", canonicalize_indicator("ECB_DFR"), "2024-06-12",
    )
    with store._connection(commit=False) as conn:
        row = conn.execute(
            """
            SELECT event_time_utc, event_time_precision,
                   reference_date, actual
            FROM cal_econ_event
            WHERE provider='ecb' AND provider_event_id = ?
            """,
            (effective_date_id,),
        ).fetchone()
        # No decision-date-anchored duplicate.
        (decision_id_count,) = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider_event_id = ?",
            (decision_date_id,),
        ).fetchone()
    assert row is not None
    assert row[0] == decision_utc
    assert row[1] == "datetime"
    assert row[2] == "2024-06-19"  # effective date preserved
    assert row[3] == "3.75"
    assert decision_id_count == 0


def test_refetch_is_idempotent_with_rate_timestamp_promotion(
    store: SQLiteEngineStore,
) -> None:
    """Two fetches of the same observation (e.g. pre-slice-2 run then
    post-slice-2 run with the schedule scraped in between) converge
    on one row — not two. The effective-date id is stable across
    the timestamp promotion, so the second fetch upserts onto the
    first via the corrected merge CASE in the shared projector."""
    from ingestion.calendar._official_shared import (
        canonicalize_indicator,
        synthesize_event_id,
    )

    # First fetch: schedule hasn't been scraped yet → fallback,
    # effective-date timestamp, 'approximate' precision.
    client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV": [_dfr_obs(3.75, "2024-06-19")],
    })
    with store._connection(commit=True) as conn:
        first = fetch_ecb_calendar(
            conn, client,
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"],
            dry_run=False,
        )
    assert first.decision_anchor_fallbacks == 1

    # Between runs the schedule scrape landed the decision row.
    decision_utc = "2024-06-12T12:15:00+00:00"
    with store._connection(commit=True) as conn:
        _seed_mp_decision_row(
            conn,
            decision_date="2024-06-12",
            event_time_utc=decision_utc,
        )

    # Second fetch on a slightly later snapshot — re-emits the same
    # observation (the collapse-to-rate-changes path re-emits the
    # baseline because it's the earliest in window).
    client2 = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV": [_dfr_obs(3.75, "2024-06-19")],
    })
    with store._connection(commit=True) as conn:
        second = fetch_ecb_calendar(
            conn, client2,
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"],
            dry_run=False,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert second.decision_anchor_fallbacks == 0

    with store._connection(commit=False) as conn:
        rows = conn.execute(
            """
            SELECT provider_event_id, event_time_utc, event_time_precision
            FROM cal_econ_event
            WHERE provider='ecb' AND title='ECB Deposit Facility Rate'
            """
        ).fetchall()
    assert len(rows) == 1  # one row, not two
    effective_date_id = synthesize_event_id(
        PROVIDER, "EU", canonicalize_indicator("ECB_DFR"), "2024-06-19",
    )
    assert rows[0][0] == effective_date_id
    # Promoted to the decision datetime + datetime precision.
    assert rows[0][1] == decision_utc
    assert rows[0][2] == "datetime"


def test_fetch_three_sibling_rates_share_decision_timestamp(
    store: SQLiteEngineStore,
) -> None:
    """MRO / DFR / MLF all move together; each stays under its own
    effective-date id but all three get the same ``event_time_utc``
    so a time-range calendar query at decision-day 14:15 CET
    surfaces all three rates on the same minute."""
    decision_utc = "2024-09-12T12:15:00+00:00"
    with store._connection(commit=True) as conn:
        _seed_mp_decision_row(
            conn,
            decision_date="2024-09-12",
            event_time_utc=decision_utc,
        )

    client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.MRR_FR.LEV": [_mro_obs(3.65, "2024-09-18")],
        "FM.B.U2.EUR.4F.KR.DFR.LEV":    [_dfr_obs(3.50, "2024-09-18")],
        "FM.B.U2.EUR.4F.KR.MLFR.LEV":   [_mlf_obs(3.90, "2024-09-18")],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_ecb_calendar(conn, client, dry_run=False)
    assert summary.observations_seen == 3
    assert summary.decision_anchor_fallbacks == 0

    with store._connection(commit=False) as conn:
        rate_rows = conn.execute(
            """
            SELECT event_time_utc, reference_date, title
            FROM cal_econ_event
            WHERE provider='ecb' AND title LIKE 'ECB %Rate%'
            ORDER BY title
            """
        ).fetchall()
    # All three rate rows share the decision-day timestamp.
    assert all(row[0] == decision_utc for row in rate_rows)
    # Effective date survives on reference_date.
    assert all(row[1] == "2024-09-18" for row in rate_rows)
    # Three distinct indicators → three rows.
    titles = sorted(row[2] for row in rate_rows)
    assert titles == [
        "ECB Deposit Facility Rate",
        "ECB Main Refinancing Operations Rate",
        "ECB Marginal Lending Facility Rate",
    ]


def test_fetch_falls_back_to_effective_date_without_matching_decision(
    store: SQLiteEngineStore,
    caplog,
) -> None:
    """No MP_DECISION row within 14 days — the rate observation keeps
    the parser's effective-date anchor and a warning surfaces so the
    operator knows to run the schedule scrape."""
    import logging

    client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV": [_dfr_obs(3.75, "2024-06-19")],
    })
    with caplog.at_level(logging.WARNING, logger="ingestion.calendar.ecb_api.fetcher"):
        with store._connection(commit=True) as conn:
            summary = fetch_ecb_calendar(
                conn, client,
                series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"],
                dry_run=False,
            )

    assert summary.observations_seen == 1
    assert summary.decision_anchor_fallbacks == 1
    assert any(
        "no preceding MP_DECISION" in rec.getMessage()
        for rec in caplog.records
    ), f"expected a decision-anchor fallback warning, got: {[r.getMessage() for r in caplog.records]}"

    # Row anchors on effective date (original parser behavior).
    from ingestion.calendar._official_shared import (
        canonicalize_indicator,
        synthesize_event_id,
    )
    fallback_id = synthesize_event_id(
        PROVIDER, "EU", canonicalize_indicator("ECB_DFR"), "2024-06-19",
    )
    with store._connection(commit=False) as conn:
        (actual,) = conn.execute(
            "SELECT actual FROM cal_econ_event WHERE provider_event_id = ?",
            (fallback_id,),
        ).fetchone()
    assert actual == "3.75"


def test_fetch_falls_back_when_decision_is_too_far_in_the_past(
    store: SQLiteEngineStore,
) -> None:
    """Decision 20 days before effective date — outside the 14-day
    window. Must fall back (otherwise we'd match a decision from the
    previous meeting cycle to an unrelated rate change)."""
    with store._connection(commit=True) as conn:
        _seed_mp_decision_row(
            conn,
            decision_date="2024-06-01",
            event_time_utc="2024-06-01T12:15:00+00:00",
        )

    client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV": [_dfr_obs(3.75, "2024-06-21")],  # 20d gap
    })
    with store._connection(commit=True) as conn:
        summary = fetch_ecb_calendar(
            conn, client,
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"],
            dry_run=False,
        )
    assert summary.decision_anchor_fallbacks == 1


def test_fetch_picks_latest_of_multiple_decisions_in_window(
    store: SQLiteEngineStore,
) -> None:
    """When two MP_DECISION rows both fall within the 14-day window
    (unlikely in practice — ECB meetings are six weeks apart — but
    possible around an unscheduled decision), the promotion picks
    the one closest to the effective date."""
    with store._connection(commit=True) as conn:
        _seed_mp_decision_row(
            conn,
            decision_date="2024-06-01",
            event_time_utc="2024-06-01T12:15:00+00:00",
        )
        _seed_mp_decision_row(
            conn,
            decision_date="2024-06-12",  # the real one
            event_time_utc="2024-06-12T12:15:00+00:00",
        )

    client = _FakeECBClient({
        "FM.B.U2.EUR.4F.KR.DFR.LEV": [_dfr_obs(3.75, "2024-06-14")],
    })
    with store._connection(commit=True) as conn:
        fetch_ecb_calendar(
            conn, client,
            series_ids=["FM.B.U2.EUR.4F.KR.DFR.LEV"],
            dry_run=False,
        )

    from ingestion.calendar._official_shared import (
        canonicalize_indicator,
        synthesize_event_id,
    )
    effective_date_id = synthesize_event_id(
        PROVIDER, "EU", canonicalize_indicator("ECB_DFR"), "2024-06-14",
    )
    with store._connection(commit=False) as conn:
        row = conn.execute(
            "SELECT event_time_utc, actual FROM cal_econ_event "
            "WHERE provider_event_id = ?",
            (effective_date_id,),
        ).fetchone()
    # Timestamp promoted to the later (closer) decision, not the earlier one.
    assert row[0] == "2024-06-12T12:15:00+00:00"
    assert row[1] == "3.75"
