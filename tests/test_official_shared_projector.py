"""Shared-projector regression suite (issue #9).

Locks in the corrected merge-rule CASE across every official-source
connector that imports
:mod:`ingestion.calendar._official_shared.projector`. Before the
consolidation each per-connector ``project_events`` shipped its own
CASE shape; BLS / BEA / ECB / Fed preserved an existing
``datetime`` unconditionally, which swallowed schedule revisions
where both rows were ``datetime``-precise. The shared projector
adopts NBS's corrected rule — an incoming ``datetime`` overwrites
a stored ``datetime`` — and this file guards that invariant on
each provider so a future SQL edit can't quietly
re-regress one of them.

No new test infrastructure; the fixtures reuse each connector's
own ``*CalendarEventRecord`` dataclass to assert the shared
projector accepts them duck-typed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion.calendar._official_shared.projector import (
    project_events,
    project_schedule_events,
    store_raw,
)
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _make_event(
    record_cls,
    *,
    provider: str,
    provider_event_id: str = "abcd" * 16,
    event_time_utc: str = "2026-05-13T12:30:00+00:00",
    precision: str = "datetime",
    observed_at: int = 2_000_000_000,
    country_code: str = "US",
    title: str = "Regression Fixture",
):
    """Construct an *EventRecord dataclass for a given connector.

    Accepts the record class itself so the test can iterate across
    BLS / BEA / Census / U Michigan / NAR / ECB / Fed / NBS record types without hard-coding
    constructor signatures — every official-source connector
    defines an ``@dataclass(frozen=True)`` with the same 24
    fields, so the same kwargs work for every connector.
    """
    return record_cls(
        provider=provider,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision=precision,
        reference_date="2026-05-01",
        reference_label="May 2026",
        country_code=country_code,
        indicator_id=None,
        category="Regression",
        title=title,
        importance="high",
        currency="USD",
        unit="index",
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Regression",
        source_url="https://example.test/",
        content_hash="0" * 64,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed_at,
    )


def _iter_connectors():
    """Yield ``(label, provider, EventRecord_cls)`` for each connector."""
    from ingestion.calendar.bls_api.parser import (
        BLSCalendarEventRecord, PROVIDER as BLS_PROVIDER,
    )
    from ingestion.calendar.bea_api.parser import (
        BEACalendarEventRecord, PROVIDER as BEA_PROVIDER,
    )
    from ingestion.calendar.census_api.parser import (
        CensusCalendarEventRecord, PROVIDER as CENSUS_PROVIDER,
    )
    from ingestion.calendar.conference_board_api.parser import (
        ConferenceBoardCalendarEventRecord,
        PROVIDER as CONFERENCE_BOARD_PROVIDER,
    )
    from ingestion.calendar.ecb_api.parser import (
        ECBCalendarEventRecord, PROVIDER as ECB_PROVIDER,
    )
    from ingestion.calendar.fed_api.parser import (
        FedCalendarEventRecord, PROVIDER as FED_PROVIDER,
    )
    from ingestion.calendar.insee_api.parser import (
        INSEECalendarEventRecord, PROVIDER as INSEE_PROVIDER,
    )
    from ingestion.calendar.zew_api.parser import (
        ZEWCalendarEventRecord, PROVIDER as ZEW_PROVIDER,
    )
    from ingestion.calendar.nar_api.parser import (
        NARCalendarEventRecord, PROVIDER as NAR_PROVIDER,
    )
    from ingestion.calendar.nbs_api.parser import (
        NBSCalendarEventRecord, PROVIDER as NBS_PROVIDER,
    )
    from ingestion.calendar.meti_api.parser import (
        MetiCalendarEventRecord, PROVIDER as METI_PROVIDER,
    )
    from ingestion.calendar.stat_bureau_api.parser import (
        StatBureauCalendarEventRecord, PROVIDER as STAT_BUREAU_PROVIDER,
    )
    from ingestion.calendar.umich_api.parser import (
        UMichCalendarEventRecord, PROVIDER as UMICH_PROVIDER,
    )
    return [
        ("bls", BLS_PROVIDER, BLSCalendarEventRecord),
        ("bea", BEA_PROVIDER, BEACalendarEventRecord),
        ("census", CENSUS_PROVIDER, CensusCalendarEventRecord),
        ("umich", UMICH_PROVIDER, UMichCalendarEventRecord),
        (
            "conference_board",
            CONFERENCE_BOARD_PROVIDER,
            ConferenceBoardCalendarEventRecord,
        ),
        ("nar", NAR_PROVIDER, NARCalendarEventRecord),
        ("ecb", ECB_PROVIDER, ECBCalendarEventRecord),
        ("fed", FED_PROVIDER, FedCalendarEventRecord),
        ("zew", ZEW_PROVIDER, ZEWCalendarEventRecord),
        ("insee", INSEE_PROVIDER, INSEECalendarEventRecord),
        ("nbs", NBS_PROVIDER, NBSCalendarEventRecord),
        ("meti", METI_PROVIDER, MetiCalendarEventRecord),
        ("stat_bureau", STAT_BUREAU_PROVIDER, StatBureauCalendarEventRecord),
    ]


# ──────────────────────────────────────────────────────────────────────────
# Corrected merge CASE — datetime overwrites datetime
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "label,provider,record_cls", _iter_connectors(),
    ids=[c[0] for c in _iter_connectors()],
)
def test_datetime_overwrites_datetime_across_all_connectors(
    label: str, provider: str, record_cls,
    store: SQLiteEngineStore,
) -> None:
    """A schedule rescrape with a revised release time must land.
    Before the consolidation, BLS / BEA / ECB / Fed silently kept
    the stale time while ``observed_at`` and ``content_hash``
    advanced."""
    first = _make_event(
        record_cls,
        provider=provider,
        event_time_utc="2026-05-13T12:30:00+00:00",
        observed_at=1_000_000_000,
    )
    revised = _make_event(
        record_cls,
        provider=provider,
        event_time_utc="2026-05-13T13:00:00+00:00",
        observed_at=2_000_000_000,
    )
    with store._connection(commit=True) as conn:
        project_events(conn, [first])
        project_events(conn, [revised])
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision "
            "FROM cal_econ_event WHERE provider=?",
            (provider,),
        ).fetchone()
    assert row[1] == "datetime"
    assert row[0] == "2026-05-13T13:00:00+00:00"


@pytest.mark.parametrize(
    "label,provider,record_cls", _iter_connectors(),
    ids=[c[0] for c in _iter_connectors()],
)
def test_approximate_does_not_clobber_stored_datetime(
    label: str, provider: str, record_cls,
    store: SQLiteEngineStore,
) -> None:
    """Schedule-vs-API invariant (BLS P1a pattern): an API-side
    ``approximate`` write arriving after a schedule-side
    ``datetime`` must preserve the datetime."""
    scheduled = _make_event(
        record_cls,
        provider=provider,
        event_time_utc="2026-05-13T12:30:00+00:00",
        precision="datetime",
        observed_at=1_500_000_000,
    )
    api_side = _make_event(
        record_cls,
        provider=provider,
        event_time_utc="2026-05-01T00:00:00+00:00",
        precision="approximate",
        observed_at=2_000_000_000,
    )
    with store._connection(commit=True) as conn:
        project_events(conn, [scheduled])
        project_events(conn, [api_side])
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision "
            "FROM cal_econ_event WHERE provider=?",
            (provider,),
        ).fetchone()
    assert row[1] == "datetime"
    assert row[0] == "2026-05-13T12:30:00+00:00"


# ──────────────────────────────────────────────────────────────────────────
# observed_at monotonicity guard still applies
# ──────────────────────────────────────────────────────────────────────────


def test_older_snapshot_does_not_overwrite_newer_row(
    store: SQLiteEngineStore,
) -> None:
    from ingestion.calendar.nbs_api.parser import (
        NBSCalendarEventRecord, PROVIDER as NBS_PROVIDER,
    )
    newer = _make_event(
        NBSCalendarEventRecord, provider=NBS_PROVIDER,
        event_time_utc="2026-05-13T02:00:00+00:00",
        observed_at=2_000_000_000,
    )
    older = _make_event(
        NBSCalendarEventRecord, provider=NBS_PROVIDER,
        event_time_utc="2026-05-13T01:30:00+00:00",
        observed_at=1_000_000_000,
    )
    with store._connection(commit=True) as conn:
        project_events(conn, [newer])
        project_events(conn, [older])
        row = conn.execute(
            "SELECT event_time_utc FROM cal_econ_event WHERE provider=?",
            (NBS_PROVIDER,),
        ).fetchone()
    assert row[0] == "2026-05-13T02:00:00+00:00"


# ──────────────────────────────────────────────────────────────────────────
# Schedule-side upsert still leaves value columns + freshness guard alone
# ──────────────────────────────────────────────────────────────────────────


def test_project_schedule_events_leaves_observed_at_alone(
    store: SQLiteEngineStore,
) -> None:
    """BLS P1a invariant: the schedule side must not touch
    ``observed_at_epoch_ms``. If it did, out-of-order API
    revisions could be silently skipped."""
    from ingestion.calendar.bls_api.parser import (
        BLSCalendarEventRecord, PROVIDER as BLS_PROVIDER,
    )
    api_row = _make_event(
        BLSCalendarEventRecord, provider=BLS_PROVIDER,
        precision="approximate",
        observed_at=3_000_000_000,
    )
    with store._connection(commit=True) as conn:
        project_events(conn, [api_row])
    schedule_row = _make_event(
        BLSCalendarEventRecord, provider=BLS_PROVIDER,
        precision="datetime",
        event_time_utc="2026-05-13T12:30:00+00:00",
        observed_at=1_000_000_000,   # older — must not regress the guard
    )
    with store._connection(commit=True) as conn:
        project_schedule_events(conn, [schedule_row])
        row = conn.execute(
            "SELECT observed_at_epoch_ms, event_time_precision "
            "FROM cal_econ_event WHERE provider=?",
            (BLS_PROVIDER,),
        ).fetchone()
    assert row[0] == 3_000_000_000
    assert row[1] == "datetime"


# ──────────────────────────────────────────────────────────────────────────
# store_raw still idempotent across connectors
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "label,provider,record_cls", _iter_connectors(),
    ids=[c[0] for c in _iter_connectors()],
)
def test_store_raw_idempotent_across_all_connectors(
    label: str, provider: str, record_cls,
    store: SQLiteEngineStore,
) -> None:
    # Reuse the event-record shape's raw sibling — pull the matching
    # RawRecord class by substitution.
    raw_cls = None
    for module_name, raw_name in {
        "bls": "BLSCalendarRawRecord",
        "bea": "BEACalendarRawRecord",
        "census": "CensusCalendarRawRecord",
        "umich": "UMichCalendarRawRecord",
        "conference_board": "ConferenceBoardCalendarRawRecord",
        "nar": "NARCalendarRawRecord",
        "ecb": "ECBCalendarRawRecord",
        "fed": "FedCalendarRawRecord",
        "zew": "ZEWCalendarRawRecord",
        "insee": "INSEECalendarRawRecord",
        "nbs": "NBSCalendarRawRecord",
        "meti": "MetiCalendarRawRecord",
        "stat_bureau": "StatBureauCalendarRawRecord",
    }.items():
        if module_name == label:
            mod = __import__(
                f"ingestion.calendar.{module_name}_api.parser",
                fromlist=[raw_name],
            )
            raw_cls = getattr(mod, raw_name)
    assert raw_cls is not None
    raw = raw_cls(
        provider=provider,
        provider_event_id=("r" * 64),
        snapshot_epoch_ms=1_700_000_000,
        content_hash="9" * 64,
        payload_json="{}",
        fetched_at=datetime.now(timezone.utc).isoformat(),
    )
    with store._connection(commit=True) as conn:
        first = store_raw(conn, [raw])
        second = store_raw(conn, [raw])
    assert first == 1
    assert second == 0
