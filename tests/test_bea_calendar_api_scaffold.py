"""Mocked tests for the BEA calendar connector (issue #9 P2).

No real HTTP. The existing :class:`ingestion.timeseries.scrapers.bea.BEAClient`
already has its own live-HTTP integration suite; this file covers only
the calendar projection layer (parser + projector + fetcher + service
op), using a duck-typed fake client wherever the fetcher needs one.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.bea_api import (
    INDICATOR_REGISTRY,
    BEACalendarEventRecord,
    BEACalendarRawRecord,
    fetch_bea_calendar,
    parse_observation,
    project_events,
    store_raw,
)
from ingestion.calendar.bea_api.parser import PROVIDER, _content_hash
from ingestion.timeseries.scrapers.bea import BEAObservation
from storage.sqlite import SQLiteEngineStore


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _gdp_obs(
    value: float = 2.4, reference_date: str = "2024-03-31",
) -> BEAObservation:
    return BEAObservation(
        series_id="BEA_NIPA_T10101_1",
        date=reference_date,
        value=value,
        table_name="T10101",
        line_number="1",
        line_description="Gross domestic product",
    )


def _pi_obs(
    value: float = 24_050.0, reference_date: str = "2024-04-01",
) -> BEAObservation:
    return BEAObservation(
        series_id="BEA_NIPA_T20600_1",
        date=reference_date,
        value=value,
        table_name="T20600",
        line_number="1",
        line_description="Personal income",
    )


class _FakeBEAClient:
    """Duck-typed stand-in for the BEA HTTP client.

    ``fetch_bea_calendar`` calls exactly one method: ``get_data``.
    We return a preseeded mapping keyed on ``(dataset, table, frequency)``
    so tests don't touch the network."""

    def __init__(
        self,
        table_to_obs: dict[tuple[str, str, str], list[BEAObservation]],
    ):
        self._data = table_to_obs
        self.api_key = "fake-key"
        self.calls: list[tuple[str, dict]] = []

    def get_data(self, dataset_name: str, **params: str) -> list[BEAObservation]:
        self.calls.append((dataset_name, dict(params)))
        key = (
            dataset_name,
            params.get("TableName", ""),
            params.get("Frequency", ""),
        )
        return list(self._data.get(key, []))


# ──────────────────────────────────────────────────────────────────────────
# INDICATOR_REGISTRY
# ──────────────────────────────────────────────────────────────────────────


def test_registry_contains_gdp_and_personal_income_with_expected_shape() -> None:
    gdp = INDICATOR_REGISTRY["BEA_NIPA_T10101_1"]
    assert gdp.indicator == "GDP"
    assert gdp.country_code == "US"
    assert gdp.importance == "high"
    assert gdp.frequency == "Q"
    pi = INDICATOR_REGISTRY["BEA_NIPA_T20600_1"]
    assert pi.indicator == "PERSONAL_INCOME"
    assert pi.country_code == "US"
    assert pi.frequency == "M"


# ──────────────────────────────────────────────────────────────────────────
# parse_observation
# ──────────────────────────────────────────────────────────────────────────


def test_parser_keeps_quarterly_end_date_as_event_time() -> None:
    """BEA quarterly obs already come back as end-of-quarter; the parser
    must keep them there (no promotion needed)."""
    obs = _gdp_obs(value=2.4, reference_date="2024-03-31")
    _, event = parse_observation(obs, snapshot_epoch_ms=1_700_000_000_000)
    assert event.event_time_utc == "2024-03-31T00:00:00+00:00"
    assert event.event_time_precision == "approximate"
    assert event.reference_date == "2024-03-31"


def test_parser_promotes_monthly_reference_to_month_end() -> None:
    """Monthly BEA obs come back as YYYY-MM-01; the parser must promote
    them to month-end so ``event_time_utc`` sits after the reference
    window closed (same convention as BLS)."""
    obs = _pi_obs(value=16_150.0, reference_date="2024-04-01")
    _, event = parse_observation(obs, snapshot_epoch_ms=1_700_000_000_000)
    assert event.event_time_utc == "2024-04-30T00:00:00+00:00"
    assert event.event_time_precision == "approximate"


def test_parser_applies_whitelist_metadata() -> None:
    _, event = parse_observation(
        _gdp_obs(value=2.4),
        snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.provider == PROVIDER == "bea"
    assert event.country_code == "US"
    assert event.title == "Real Gross Domestic Product"
    assert event.importance == "high"
    assert event.unit == "percent"
    assert event.actual == "2.4"
    assert event.source == "BEA"
    assert "DatasetName=NIPA" in event.source_url
    assert "TableName=T10101" in event.source_url


def test_parser_synthesises_deterministic_event_id() -> None:
    """Same reference period → same id; different value is a revision,
    not a new event — same id."""
    a = parse_observation(
        _gdp_obs(value=2.4),
        snapshot_epoch_ms=1_700_000_000_000,
    )[1]
    b = parse_observation(
        _gdp_obs(value=2.5),  # revised: advance → second → third
        snapshot_epoch_ms=1_700_000_000_000,
    )[1]
    assert a.provider_event_id == b.provider_event_id

    # Different quarter → different event.
    c = parse_observation(
        _gdp_obs(value=2.4, reference_date="2024-06-30"),
        snapshot_epoch_ms=1_700_000_000_000,
    )[1]
    assert c.provider_event_id != a.provider_event_id


def test_parser_id_is_stable_when_event_time_utc_changes() -> None:
    """P2a will promote the placeholder ``event_time_utc`` (reference
    period end) to the true scheduled release datetime. The
    ``provider_event_id`` anchors on the observation's reference-period
    date (``obs.date``), not ``event_time_utc``, so the projector
    upserts the same row instead of inserting a duplicate."""
    _, event = parse_observation(_gdp_obs(), snapshot_epoch_ms=0)
    from ingestion.calendar._official_shared import (
        canonicalize_indicator,
        synthesize_event_id,
    )
    recomputed_with_real_schedule = synthesize_event_id(
        "bea", "US", canonicalize_indicator("GDP"), "2024-03-31",
    )
    assert event.provider_event_id == recomputed_with_real_schedule


def test_parser_hashes_value_and_line_metadata() -> None:
    """Revision signal is the content hash — same fields hash identically,
    any change bumps the hash."""
    base = _content_hash({
        "DataValue": "2.4", "LineDescription": "Gross domestic product",
        "NoteRef": "T10101",
    })
    same = _content_hash({
        "DataValue": "2.4", "LineDescription": "Gross domestic product",
        "NoteRef": "T10101",
    })
    revised = _content_hash({
        "DataValue": "2.5", "LineDescription": "Gross domestic product",
        "NoteRef": "T10101",
    })
    assert base == same
    assert base != revised


def test_parser_hashes_note_revisions_when_raw_dict_is_available() -> None:
    """A NoteRef-only revision (methodology correction) must produce a
    new content hash so the raw audit lane captures the event.
    Requires ``obs.raw`` to be populated with the full upstream row —
    BEAClient.get_data does this; tests that construct BEAObservation
    manually without ``raw`` fall back to a minimal reconstruction and
    lose note-only changes (documented in parser)."""
    base = BEAObservation(
        series_id="BEA_NIPA_T10101_1",
        date="2024-03-31",
        value=2.4,
        table_name="T10101",
        line_number="1",
        line_description="Gross domestic product",
        raw={
            "TimePeriod": "2024Q1", "DataValue": "2.4",
            "LineNumber": "1", "LineDescription": "Gross domestic product",
            "NoteRef": "T10101",
        },
    )
    note_revision = BEAObservation(
        series_id="BEA_NIPA_T10101_1",
        date="2024-03-31",
        value=2.4,
        table_name="T10101",
        line_number="1",
        line_description="Gross domestic product",
        raw={
            "TimePeriod": "2024Q1", "DataValue": "2.4",
            "LineNumber": "1", "LineDescription": "Gross domestic product",
            "NoteRef": "T10101,N1",
        },
    )
    raw_a, _ = parse_observation(base, snapshot_epoch_ms=1_700_000_000_000)
    raw_b, _ = parse_observation(
        note_revision, snapshot_epoch_ms=1_700_000_100_000,
    )
    assert raw_a.content_hash != raw_b.content_hash


def test_parser_handles_suppressed_values() -> None:
    """BEA uses ``value=None`` for suppressed/NA observations
    (``(D)`` / ``(NA)`` upstream). The parser must emit ``actual=None``
    rather than coercing to a string."""
    obs = BEAObservation(
        series_id="BEA_NIPA_T10101_1",
        date="2024-03-31",
        value=None,
        table_name="T10101",
        line_number="1",
        line_description="Gross domestic product",
    )
    _, event = parse_observation(obs, snapshot_epoch_ms=0)
    assert event.actual is None


def test_parser_rejects_unknown_series() -> None:
    rogue = BEAObservation(
        series_id="UNKNOWN_SERIES",
        date="2024-03-31",
        value=0.0,
        table_name="",
        line_number="",
        line_description="",
    )
    with pytest.raises(KeyError):
        parse_observation(rogue, snapshot_epoch_ms=0)


# ──────────────────────────────────────────────────────────────────────────
# projector
# ──────────────────────────────────────────────────────────────────────────


def test_store_raw_inserts_then_deduplicates_by_content_hash(
    store: SQLiteEngineStore,
) -> None:
    raw, _ = parse_observation(_gdp_obs(), snapshot_epoch_ms=1_700_000_000_000)
    with store._connection(commit=True) as conn:
        first = store_raw(conn, [raw])
        second = store_raw(conn, [raw])  # same content hash → no-op
    assert first == 1
    assert second == 0


def test_project_events_upserts_and_honors_observed_at_ordering(
    store: SQLiteEngineStore,
) -> None:
    _, first_event = parse_observation(
        _gdp_obs(value=2.4),
        snapshot_epoch_ms=1_700_000_000_000,
        observed_at_epoch_ms=1_700_000_000_000,
    )
    _, revised_event = parse_observation(
        _gdp_obs(value=2.5),  # second estimate
        snapshot_epoch_ms=1_700_000_100_000,
        observed_at_epoch_ms=1_700_000_100_000,
    )
    _, stale_event = parse_observation(
        _gdp_obs(value=99.0),
        snapshot_epoch_ms=1_699_999_900_000,
        observed_at_epoch_ms=1_699_999_900_000,
    )

    with store._connection(commit=True) as conn:
        project_events(conn, [first_event])
        project_events(conn, [revised_event])
        project_events(conn, [stale_event])  # older → ignored

    with store._connection(commit=False) as conn:
        (actual,) = conn.execute(
            "SELECT actual FROM cal_econ_event WHERE provider='bea'"
        ).fetchone()
    assert actual == "2.5"


def test_projected_rows_surface_in_v_calendar_item(
    store: SQLiteEngineStore,
) -> None:
    raw, event = parse_observation(
        _gdp_obs(value=2.4),
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
                "FROM v_calendar_item WHERE provider='bea'"
            ).fetchall()
        ]
    assert rows == [("bea", "US", None, "2.4", "high")]


# ──────────────────────────────────────────────────────────────────────────
# fetch_bea_calendar
# ──────────────────────────────────────────────────────────────────────────


def test_fetch_dry_run_returns_plan_without_calling_client(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeBEAClient({})
    with store._connection(commit=False) as conn:
        summary = fetch_bea_calendar(
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
    """Codex P2a — GDP has ``api_fetch=False`` because its staged
    schedule has no clean anchor alignment with the bare-date API
    observation. Default iteration must exclude it; callers who
    know what they're doing can still opt in via ``series_ids=``."""
    client = _FakeBEAClient({})
    with store._connection(commit=False) as conn:
        summary = fetch_bea_calendar(
            conn, client,
            start_year=2024, end_year=2024,
            dry_run=True,
        )
    opted_out = {
        sid for sid, spec in INDICATOR_REGISTRY.items() if not spec.api_fetch
    }
    assert opted_out, "expected at least one api_fetch=False spec"
    assert not (set(summary.series_planned) & opted_out)


def test_fetch_writes_rows_and_reports_counts(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeBEAClient({
        ("NIPA", "T10101", "Q"): [
            _gdp_obs(2.4, "2024-03-31"),
            _gdp_obs(1.6, "2024-06-30"),
        ],
        ("NIPA", "T20600", "M"): [_pi_obs(16_150.0, "2024-04-01")],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_bea_calendar(
            conn, client,
            start_year=2024, end_year=2024,
            series_ids=["BEA_NIPA_T10101_1", "BEA_NIPA_T20600_1"],
            dry_run=False,
        )
    assert summary.observations_seen == 3
    assert summary.rows_raw_inserted == 3
    assert summary.events_upserted == 3
    assert set(summary.series_ok) == {"BEA_NIPA_T10101_1", "BEA_NIPA_T20600_1"}
    assert summary.series_empty == []
    assert summary.series_unknown == []
    # Year parameter is rendered as a comma-separated list.
    nipa_q_call = next(
        p for _, p in client.calls if p.get("TableName") == "T10101"
    )
    assert nipa_q_call["Year"] == "2024"


def test_fetch_year_param_covers_inclusive_range(
    store: SQLiteEngineStore,
) -> None:
    """Year range 2022–2024 should render as ``"2022,2023,2024"`` so
    BEA returns the full window in a single call."""
    client = _FakeBEAClient({
        ("NIPA", "T10101", "Q"): [_gdp_obs()],
    })
    with store._connection(commit=True) as conn:
        fetch_bea_calendar(
            conn, client,
            start_year=2022, end_year=2024,
            series_ids=["BEA_NIPA_T10101_1"],
            dry_run=False,
        )
    nipa_call = next(
        p for _, p in client.calls if p.get("TableName") == "T10101"
    )
    assert nipa_call["Year"] == "2022,2023,2024"


def test_fetch_filters_to_whitelisted_lines(
    store: SQLiteEngineStore,
) -> None:
    """BEA returns every line of a table in one call; the fetcher must
    discard lines the whitelist doesn't cover rather than projecting
    them as unknown-indicator rows."""
    extra_line = BEAObservation(
        series_id="BEA_NIPA_T10101_99",   # not in registry
        date="2024-03-31",
        value=0.5,
        table_name="T10101",
        line_number="99",
        line_description="Some other aggregate",
    )
    client = _FakeBEAClient({
        ("NIPA", "T10101", "Q"): [_gdp_obs(), extra_line],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_bea_calendar(
            conn, client,
            start_year=2024, end_year=2024,
            series_ids=["BEA_NIPA_T10101_1"],
            dry_run=False,
        )
    assert summary.observations_seen == 1   # extra line ignored
    assert summary.events_upserted == 1


def test_fetch_skips_unknown_series_without_silent_coercion(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeBEAClient({
        ("NIPA", "T10101", "Q"): [_gdp_obs()],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_bea_calendar(
            conn, client,
            start_year=2024, end_year=2024,
            series_ids=["BEA_NIPA_T10101_1", "BOGUS_SERIES_ID"],
            dry_run=False,
        )
    assert summary.series_unknown == ["BOGUS_SERIES_ID"]
    assert summary.series_ok == ["BEA_NIPA_T10101_1"]


def test_fetch_reports_empty_series_separately(
    store: SQLiteEngineStore,
) -> None:
    """A known series that returned no matching-line observations lands
    in ``series_empty`` — distinct from ``series_unknown`` (not in
    registry) and ``series_ok`` (got at least one row)."""
    client = _FakeBEAClient({
        ("NIPA", "T10101", "Q"): [_gdp_obs()],
        ("NIPA", "T20600", "M"): [],     # whitelisted, upstream returned nothing
    })
    with store._connection(commit=True) as conn:
        summary = fetch_bea_calendar(
            conn, client,
            start_year=2024, end_year=2024,
            series_ids=["BEA_NIPA_T10101_1", "BEA_NIPA_T20600_1"],
            dry_run=False,
        )
    assert "BEA_NIPA_T20600_1" in summary.series_empty
    assert "BEA_NIPA_T10101_1" in summary.series_ok


# ──────────────────────────────────────────────────────────────────────────
# Service op wiring
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_bea", {"dry_run": True})
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
        "calendar_econ_fetch_bea",
        {"dry_run": True, "series_ids": ["BEA_NIPA_T10101_1"]},
    )
    assert result["series_planned"] == ["BEA_NIPA_T10101_1"]


def test_service_op_dry_run_surfaces_unknown_series(
    store: SQLiteEngineStore,
) -> None:
    """Dry-run and execute must agree on the known/unknown split so
    callers can't misread an invalid series id as runnable."""
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_bea",
        {"dry_run": True, "series_ids": ["BEA_NIPA_T10101_1", "BOGUS"]},
    )
    assert result["series_planned"] == ["BEA_NIPA_T10101_1"]
    assert result["series_unknown"] == ["BOGUS"]
