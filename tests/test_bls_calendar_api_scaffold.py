"""Mocked tests for the BLS calendar connector (issue #9 P1).

No real HTTP. The existing :class:`ingestion.timeseries.scrapers.bls.BLSClient`
already has its own live-HTTP integration suite; this file covers only
the calendar projection layer (parser + projector + fetcher + service
op), using a duck-typed fake client wherever the fetcher needs one.
"""

from __future__ import annotations

from datetime import datetime, timezone
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


def test_registry_productivity_flags_staged_schedule() -> None:
    """P1c follow-up — Productivity's preliminary/revised schedule
    is rebased against the API's single bare-date observation via
    ``staged_schedule=True``; the indicator is back in the default
    API-fetch plan."""
    spec = INDICATOR_REGISTRY["PRS85006092"]
    assert spec.api_fetch is True
    assert spec.staged_schedule is True


def test_registry_all_specs_currently_opt_in_to_default_api_fetch() -> None:
    """Post P1c follow-up: no indicator opts out of the default
    API fetch. The ``api_fetch`` switch stays on the spec as a
    quarantine lever for future problem indicators."""
    for series_id, spec in INDICATOR_REGISTRY.items():
        assert spec.api_fetch is True, f"{series_id} should default to api_fetch=True"


def test_registry_only_productivity_marks_staged_schedule() -> None:
    """Productivity is the only BLS series with multi-stage releases
    today. Any other indicator marked ``staged_schedule=True`` without
    a corresponding schedule-scraper update would write staged rows
    under an anchor the bare-date API observation can't rebase onto."""
    staged = {
        sid for sid, spec in INDICATOR_REGISTRY.items()
        if spec.staged_schedule
    }
    assert staged == {"PRS85006092"}


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


def test_fetch_default_plan_includes_productivity_after_staged_merge_landed(
    store: SQLiteEngineStore,
) -> None:
    """P1c follow-up — Productivity is now rebased against its
    schedule row via ``staged_schedule=True`` and takes the default
    API-fetch plan alongside CPI / NFP / etc."""
    client = _FakeBLSClient({})
    with store._connection(commit=False) as conn:
        summary = fetch_bls_calendar(
            conn, client,
            start_year=2024, end_year=2024,
            dry_run=True,
        )
    assert "PRS85006092" in summary.series_planned


def test_fetch_plan_honors_api_fetch_opt_out_if_any_spec_has_it(
    store: SQLiteEngineStore,
) -> None:
    """Quarantine switch regression test: no registry entry sets
    ``api_fetch=False`` today, but the fetcher must still exclude any
    such spec from the default plan so the lever remains usable."""
    opted_out = {
        sid for sid, spec in INDICATOR_REGISTRY.items() if not spec.api_fetch
    }
    assert opted_out == set(), "adjust the expectation when a spec opts out"
    client = _FakeBLSClient({})
    with store._connection(commit=False) as conn:
        summary = fetch_bls_calendar(
            conn, client,
            start_year=2024, end_year=2024,
            dry_run=True,
        )
    assert not (set(summary.series_planned) & opted_out)


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
# Staged-schedule rebase (P1c follow-up — Productivity)
# ──────────────────────────────────────────────────────────────────────────


def _prod_obs(value: float = 115.2, date: str = "2025-09-30") -> BLSObservation:
    """Synthetic Productivity observation. BLSClient normalises the
    quarterly period to the end-of-quarter date — match that shape so
    the staged-rebase lookup key equals the schedule row's anchor."""
    return BLSObservation(
        series_id="PRS85006092",
        date=date,
        value=value,
        period="Q03",
    )


def _seed_productivity_schedule_row(
    conn,
    *,
    reference_date: str,
    stage: str,
    release_date_utc: str,
    observed_at_epoch_ms: int,
) -> str:
    """Write one Productivity schedule row under a stage-qualified
    anchor and return its ``provider_event_id`` for assertions."""
    from ingestion.calendar._official_shared import (
        canonicalize_indicator,
        synthesize_event_id,
    )

    spec = INDICATOR_REGISTRY["PRS85006092"]
    event_id = synthesize_event_id(
        PROVIDER,
        spec.country_code,
        canonicalize_indicator(spec.indicator),
        f"{reference_date}|{stage}",
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
            PROVIDER, event_id, release_date_utc, "datetime",
            reference_date, f"3rd Quarter {reference_date[:4]} ({stage})",
            spec.country_code, None,
            spec.category, spec.title, spec.importance, "", spec.unit,
            None, None, None, None, None,
            "", "BLS",
            "https://www.bls.gov/schedule/news_release/prod2.htm",
            f"schedule:{stage}",
            None, observed_at_epoch_ms,
            "2025-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00",
        ),
    )
    return event_id


def test_staged_fetch_rebases_onto_preliminary_before_revised_releases(
    store: SQLiteEngineStore,
) -> None:
    """The API observation is fetched after the preliminary release
    but before the revised release — the value lands on the
    preliminary schedule row's id, keeping the two stages distinct.

    Also verifies the schedule row's ``reference_label`` survives
    the merge: ``project_events`` always writes
    ``excluded.reference_label``, so the fetcher must inject the
    stored label (``"3rd Quarter 2025 (Preliminary)"``) into the
    rebased event record or the parser's bare ``"Q03"`` label would
    overwrite the stage marker."""
    preliminary_released_ms = 1_700_000_000_000  # e.g. "2023-11-14T13:30Z"
    revised_release_ms     = 1_702_000_000_000   # ~a month later
    snapshot_between       = 1_701_000_000_000   # between the two

    with store._connection(commit=True) as conn:
        prelim_id = _seed_productivity_schedule_row(
            conn,
            reference_date="2025-09-30",
            stage="preliminary",
            release_date_utc=datetime.fromtimestamp(
                preliminary_released_ms / 1000, tz=timezone.utc,
            ).isoformat(),
            observed_at_epoch_ms=preliminary_released_ms,
        )
        revised_id = _seed_productivity_schedule_row(
            conn,
            reference_date="2025-09-30",
            stage="revised",
            release_date_utc=datetime.fromtimestamp(
                revised_release_ms / 1000, tz=timezone.utc,
            ).isoformat(),
            observed_at_epoch_ms=preliminary_released_ms,
        )

    client = _FakeBLSClient({"PRS85006092": [_prod_obs(value=115.2)]})
    with store._connection(commit=True) as conn:
        summary = fetch_bls_calendar(
            conn, client,
            start_year=2025, end_year=2025,
            series_ids=["PRS85006092"],
            dry_run=False,
            snapshot_epoch_ms=snapshot_between,
        )

    assert summary.events_upserted == 1
    assert summary.staged_skipped == 0
    with store._connection(commit=False) as conn:
        prelim_row = conn.execute(
            "SELECT actual, event_time_precision, reference_label "
            "FROM cal_econ_event WHERE provider_event_id = ?",
            (prelim_id,),
        ).fetchone()
        revised_row = conn.execute(
            "SELECT actual, reference_label FROM cal_econ_event "
            "WHERE provider_event_id = ?",
            (revised_id,),
        ).fetchone()

    # API value merged onto the preliminary row, and the schedule's
    # datetime precision + stage label both survived the merge.
    assert prelim_row[0] == "115.2"
    assert prelim_row[1] == "datetime"
    assert prelim_row[2] == "3rd Quarter 2025 (preliminary)"
    # Revised row untouched — still has actual=NULL and its own label.
    assert revised_row[0] is None
    assert revised_row[1] == "3rd Quarter 2025 (revised)"


def test_staged_fetch_rebases_onto_revised_after_both_releases(
    store: SQLiteEngineStore,
) -> None:
    """Once both schedule rows' release times are in the past, the API
    observation represents the revised value and merges onto the
    revised row — with the revised stage label surviving the merge."""
    preliminary_released_ms = 1_700_000_000_000
    revised_release_ms     = 1_702_000_000_000
    snapshot_after_revised = 1_703_000_000_000

    with store._connection(commit=True) as conn:
        prelim_id = _seed_productivity_schedule_row(
            conn,
            reference_date="2025-09-30",
            stage="preliminary",
            release_date_utc=datetime.fromtimestamp(
                preliminary_released_ms / 1000, tz=timezone.utc,
            ).isoformat(),
            observed_at_epoch_ms=preliminary_released_ms,
        )
        revised_id = _seed_productivity_schedule_row(
            conn,
            reference_date="2025-09-30",
            stage="revised",
            release_date_utc=datetime.fromtimestamp(
                revised_release_ms / 1000, tz=timezone.utc,
            ).isoformat(),
            observed_at_epoch_ms=preliminary_released_ms,
        )

    client = _FakeBLSClient({"PRS85006092": [_prod_obs(value=116.8)]})
    with store._connection(commit=True) as conn:
        summary = fetch_bls_calendar(
            conn, client,
            start_year=2025, end_year=2025,
            series_ids=["PRS85006092"],
            dry_run=False,
            snapshot_epoch_ms=snapshot_after_revised,
        )

    assert summary.events_upserted == 1
    assert summary.staged_skipped == 0
    with store._connection(commit=False) as conn:
        prelim_actual, prelim_label = conn.execute(
            "SELECT actual, reference_label FROM cal_econ_event "
            "WHERE provider_event_id = ?",
            (prelim_id,),
        ).fetchone()
        revised_actual, revised_label = conn.execute(
            "SELECT actual, reference_label FROM cal_econ_event "
            "WHERE provider_event_id = ?",
            (revised_id,),
        ).fetchone()

    assert revised_actual == "116.8"
    assert revised_label == "3rd Quarter 2025 (revised)"
    assert prelim_actual is None
    assert prelim_label == "3rd Quarter 2025 (preliminary)"


def test_staged_fetch_skips_and_warns_when_no_schedule_row(
    store: SQLiteEngineStore,
    caplog,
) -> None:
    """Cold start: schedule hasn't been scraped yet, no stage-qualified
    rows exist. The observation is skipped rather than written under a
    bare-date anchor — otherwise a later schedule scrape would land a
    stage-qualified row under a different id, orphaning the bare-date
    row. Operator learns via the warning + ``staged_skipped`` counter."""
    import logging

    snapshot_ms = 1_703_000_000_000
    client = _FakeBLSClient({"PRS85006092": [_prod_obs(value=117.4)]})

    with caplog.at_level(logging.WARNING, logger="ingestion.calendar.bls_api.fetcher"):
        with store._connection(commit=True) as conn:
            summary = fetch_bls_calendar(
                conn, client,
                start_year=2025, end_year=2025,
                series_ids=["PRS85006092"],
                dry_run=False,
                snapshot_epoch_ms=snapshot_ms,
            )

    assert summary.events_upserted == 0
    assert summary.rows_raw_inserted == 0
    assert summary.staged_skipped == 1
    assert any(
        "no eligible schedule row" in rec.getMessage()
        and "skipping" in rec.getMessage()
        and "PRS85006092" in rec.getMessage()
        for rec in caplog.records
    ), f"expected a staged-skip warning, got: {[r.getMessage() for r in caplog.records]}"

    with store._connection(commit=False) as conn:
        (count,) = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider='bls'"
        ).fetchone()
    assert count == 0


def test_staged_fetch_skips_when_schedule_rows_are_all_future(
    store: SQLiteEngineStore,
    caplog,
) -> None:
    """Both schedule rows exist but neither has released yet (release
    time > snapshot) — the API observation can't belong to a stage
    that hasn't been published. Skip rather than rebase onto a future
    stage."""
    import logging

    preliminary_future = 1_800_000_000_000
    revised_future    = 1_810_000_000_000
    snapshot_before_any = 1_700_000_000_000

    with store._connection(commit=True) as conn:
        _seed_productivity_schedule_row(
            conn,
            reference_date="2025-09-30",
            stage="preliminary",
            release_date_utc=datetime.fromtimestamp(
                preliminary_future / 1000, tz=timezone.utc,
            ).isoformat(),
            observed_at_epoch_ms=snapshot_before_any,
        )
        _seed_productivity_schedule_row(
            conn,
            reference_date="2025-09-30",
            stage="revised",
            release_date_utc=datetime.fromtimestamp(
                revised_future / 1000, tz=timezone.utc,
            ).isoformat(),
            observed_at_epoch_ms=snapshot_before_any,
        )

    client = _FakeBLSClient({"PRS85006092": [_prod_obs(value=118.5)]})
    with caplog.at_level(logging.WARNING, logger="ingestion.calendar.bls_api.fetcher"):
        with store._connection(commit=True) as conn:
            summary = fetch_bls_calendar(
                conn, client,
                start_year=2025, end_year=2025,
                series_ids=["PRS85006092"],
                dry_run=False,
                snapshot_epoch_ms=snapshot_before_any,
            )

    assert summary.staged_skipped == 1
    # Schedule rows still have actual=NULL; no bare-date orphan row.
    with store._connection(commit=False) as conn:
        (value_rows,) = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event "
            "WHERE provider='bls' AND actual IS NOT NULL"
        ).fetchone()
    assert value_rows == 0


def test_non_staged_indicator_never_triggers_staged_lookup(
    store: SQLiteEngineStore,
) -> None:
    """CPI has ``staged_schedule=False`` — the fetcher must not enter
    the staged rebase path for it. This keeps the per-observation
    DB lookup cost scoped to the single indicator that actually
    needs it (Productivity today)."""
    from ingestion.calendar._official_shared import (
        canonicalize_indicator,
        synthesize_event_id,
    )

    client = _FakeBLSClient({"CUUR0000SA0": [_cpi_obs(value=312.5, period="M09")]})
    with store._connection(commit=True) as conn:
        fetch_bls_calendar(
            conn, client,
            start_year=2024, end_year=2024,
            series_ids=["CUUR0000SA0"],
            dry_run=False,
        )

    # CPI's id is bare-date per parser.parse_observation.
    cpi_id = synthesize_event_id(
        "bls", "US",
        canonicalize_indicator("CPI"),
        "2024-09-01",
    )
    with store._connection(commit=False) as conn:
        (actual,) = conn.execute(
            "SELECT actual FROM cal_econ_event WHERE provider_event_id = ?",
            (cpi_id,),
        ).fetchone()
    assert actual == "312.5"


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
