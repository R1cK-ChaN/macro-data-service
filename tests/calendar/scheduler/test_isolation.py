"""Scheduler tests: daily request-budget tracking (BLS cap).

Split out of the original tests/test_calendar_refresh_scheduler.py
as part of issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pytest
from storage.sqlite import SQLiteEngineStore

from ingestion.calendar.scheduler import (
    refresh_all_schedules,
    sweep_value_side,
)
from ingestion.calendar.scheduler_state import (
    ConnectorState,
    DAILY_BUDGET_CAPS,
    FAILURE_THRESHOLD,
    get_connector_state,
    is_budget_exhausted,
    mark_connector_failure,
    mark_connector_success,
    record_connector_requests,
)


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")

@dataclass
class _FakeSummary:
    """Drop-in stand-in for a per-connector ``*RunSummary`` dataclass."""

    connector: str
    dry_run: bool
    calls: int


def test_bls_is_the_capped_connector() -> None:
    """Only BLS has a declared cap today. Any future connector with a
    hard daily limit should be added explicitly; the default is
    uncapped to avoid surprise skips on sources that publish no limit."""
    assert DAILY_BUDGET_CAPS == {"bls": 490}


def test_budget_helpers_default_to_unexhausted(
    store: SQLiteEngineStore,
) -> None:
    """A fresh connector with no state row is never exhausted; the
    check short-circuits on the absent ``requests_day_utc``."""
    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.requests_today == 0
    assert state.requests_day_utc is None
    assert is_budget_exhausted(state, cap=490, today_iso="2026-04-22") is False
    # Uncapped connector: exhausted is always False even at high counts.
    inflated = ConnectorState(
        connector="bea", requests_today=100_000, requests_day_utc="2026-04-22",
    )
    assert is_budget_exhausted(inflated, cap=None, today_iso="2026-04-22") is False


def test_record_requests_accumulates_same_day(
    store: SQLiteEngineStore,
) -> None:
    """Repeated calls on the same UTC day add to the running total —
    a cron that sweeps every 5 minutes rolls its consumption up to
    the cap rather than losing earlier runs."""
    with store.get_connection() as conn:
        record_connector_requests(conn, "bls", 3, today_iso="2026-04-22")
        record_connector_requests(conn, "bls", 2, today_iso="2026-04-22")
        record_connector_requests(conn, "bls", 0, today_iso="2026-04-22")
        conn.commit()
        state = get_connector_state(conn, "bls")
    assert state.requests_today == 5
    assert state.requests_day_utc == "2026-04-22"


def test_record_requests_rolls_over_at_new_utc_day(
    store: SQLiteEngineStore,
) -> None:
    """A call with a different ``today_iso`` resets the counter to the
    new run's consumption — matches the in-memory ``BLSClient`` reset
    so both agree on the current calendar day."""
    with store.get_connection() as conn:
        record_connector_requests(conn, "bls", 480, today_iso="2026-04-22")
        conn.commit()
        exhausted_state = get_connector_state(conn, "bls")
        assert is_budget_exhausted(
            exhausted_state, cap=490, today_iso="2026-04-22",
        ) is False  # 480 < 490
        record_connector_requests(conn, "bls", 490, today_iso="2026-04-22")
        conn.commit()
        now_exhausted = get_connector_state(conn, "bls")
        assert is_budget_exhausted(
            now_exhausted, cap=490, today_iso="2026-04-22",
        ) is True

        # UTC-day flip rolls the counter over.
        record_connector_requests(conn, "bls", 7, today_iso="2026-04-23")
        conn.commit()
        fresh_day = get_connector_state(conn, "bls")
    assert fresh_day.requests_today == 7
    assert fresh_day.requests_day_utc == "2026-04-23"
    assert is_budget_exhausted(
        fresh_day, cap=490, today_iso="2026-04-23",
    ) is False


def test_record_requests_preserves_breaker_fields(
    store: SQLiteEngineStore,
) -> None:
    """Budget and breaker state are orthogonal: incrementing the daily
    counter must not wipe an in-flight consecutive-failure count or
    clear a cool-down window. A connector can be simultaneously in
    cool-down and have consumed some of today's budget."""
    now_ms = 1_800_000_000_000
    with store.get_connection() as conn:
        for _ in range(FAILURE_THRESHOLD):
            mark_connector_failure(
                conn, "bls", error="bls.gov 403", now_ms=now_ms,
            )
        conn.commit()
        before = get_connector_state(conn, "bls")
        assert before.cooling_until_ms is not None
        record_connector_requests(conn, "bls", 5, today_iso="2026-04-22")
        conn.commit()
        after = get_connector_state(conn, "bls")
    assert after.cooling_until_ms == before.cooling_until_ms
    assert after.consecutive_failures == before.consecutive_failures
    assert after.last_error == before.last_error
    assert after.requests_today == 5
    assert after.requests_day_utc == "2026-04-22"


def test_mark_success_preserves_budget_counter(
    store: SQLiteEngineStore,
) -> None:
    """A successful run resets the failure counter but must leave
    today's budget alone — otherwise a mid-day success would create a
    false sense of unlimited remaining budget and let the scheduler
    blow through the upstream cap."""
    with store.get_connection() as conn:
        record_connector_requests(conn, "bls", 400, today_iso="2026-04-22")
        conn.commit()
        mark_connector_success(conn, "bls")
        conn.commit()
        state = get_connector_state(conn, "bls")
    assert state.consecutive_failures == 0
    assert state.requests_today == 400
    assert state.requests_day_utc == "2026-04-22"


def test_mark_failure_preserves_budget_counter(
    store: SQLiteEngineStore,
) -> None:
    """Same invariant from the failure side. A 502 on a connector
    that's already consumed budget shouldn't roll the counter back —
    those upstream calls happened and a second cron should still see
    them against today's cap."""
    with store.get_connection() as conn:
        record_connector_requests(conn, "bls", 250, today_iso="2026-04-22")
        conn.commit()
        mark_connector_failure(
            conn, "bls", error="flaky", now_ms=1_800_000_000_000,
        )
        conn.commit()
        state = get_connector_state(conn, "bls")
    assert state.consecutive_failures == 1
    assert state.requests_today == 250
    assert state.requests_day_utc == "2026-04-22"


def test_driver_skips_bls_when_budget_exhausted(
    store: SQLiteEngineStore,
) -> None:
    """End-to-end: a BLS state row pre-seeded at the cap for today
    causes the value-side driver to skip invocation. The per-connector
    fn is never called and the result carries the budget-exhausted
    reason. Schedule-side has its own test below — it must ignore the
    cap because its BLS path is HTML-only."""
    import datetime as _dt

    today_iso = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")
    cap = DAILY_BUDGET_CAPS["bls"]

    with store.get_connection() as conn:
        record_connector_requests(conn, "bls", cap, today_iso=today_iso)
        conn.commit()

    calls: list[str] = []

    def _would_run(conn, dry_run):
        calls.append("bls")
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _would_run},
    )
    assert calls == []  # fn never invoked
    bls_result = summary.results[0]
    assert bls_result.ok is False
    assert "budget exhausted" in (bls_result.error or "")
    assert f"{cap}/{cap}" in (bls_result.error or "")


def test_driver_runs_bls_when_cap_not_reached(
    store: SQLiteEngineStore,
) -> None:
    """Counterpoint: a BLS row at cap-1 for today stays invokable —
    one more request is allowed within the cap window."""
    import datetime as _dt

    today_iso = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")
    cap = DAILY_BUDGET_CAPS["bls"]

    with store.get_connection() as conn:
        record_connector_requests(conn, "bls", cap - 1, today_iso=today_iso)
        conn.commit()

    @dataclass
    class _BlsSummary:
        dry_run: bool
        requests_made: int

    def _one_request(conn, dry_run):
        return _BlsSummary(dry_run=dry_run, requests_made=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _one_request},
    )
    assert summary.results[0].ok is True

    # After the run, the counter pushed the connector to exactly cap.
    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.requests_today == cap
    assert is_budget_exhausted(state, cap, today_iso=today_iso) is True


def test_driver_accumulates_requests_made_across_runs(
    store: SQLiteEngineStore,
) -> None:
    """Successive sweeps within the same UTC day roll the counter up
    — the driver reads ``summary.requests_made`` off each connector
    return and accumulates through :func:`record_connector_requests`."""
    import datetime as _dt

    today_iso = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")

    @dataclass
    class _BlsSummary:
        dry_run: bool
        requests_made: int

    def _consumes(count: int):
        def _fn(conn, dry_run):
            return _BlsSummary(dry_run=dry_run, requests_made=count)
        return _fn

    for count in (3, 7, 2):
        sweep_value_side(
            store.get_connection,
            dry_run=False,
            connectors=["bls"],
            _connector_overrides={"bls": _consumes(count)},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.requests_today == 12
    assert state.requests_day_utc == today_iso


def test_driver_ignores_summary_without_requests_made(
    store: SQLiteEngineStore,
) -> None:
    """Connectors without a ``requests_made`` field on their summary
    (BEA / ECB / Fed / NBS today) don't contribute to the counter and
    don't raise — the driver's attribute access is defensive so a
    future connector can opt in without a coordinated rollout."""
    @dataclass
    class _PlainSummary:
        connector: str
        dry_run: bool

    def _no_requests(conn, dry_run):
        return _PlainSummary(connector="bea", dry_run=dry_run)

    refresh_all_schedules(
        store.get_connection,
        dry_run=False,
        connectors=["bea"],
        _connector_overrides={"bea": _no_requests},
    )
    with store.get_connection() as conn:
        state = get_connector_state(conn, "bea")
    # Success landed — counter stayed at zero since the summary didn't
    # carry the field. ``requests_day_utc`` is None because no record
    # call fired.
    assert state.requests_today == 0
    assert state.requests_day_utc is None


def test_driver_dry_run_does_not_record_requests(
    store: SQLiteEngineStore,
) -> None:
    """Dry-run previews the plan. It must not persist budget changes
    — otherwise repeated previews would trip a cap that no real run
    ever consumed. Mirrors the circuit-breaker dry-run invariant."""
    @dataclass
    class _BlsSummary:
        dry_run: bool
        requests_made: int

    def _would_consume(conn, dry_run):
        return _BlsSummary(dry_run=dry_run, requests_made=50)

    for _ in range(10):
        refresh_all_schedules(
            store.get_connection,
            dry_run=True,  # preview
            connectors=["bls"],
            _connector_overrides={"bls": _would_consume},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.requests_today == 0
    assert state.requests_day_utc is None


def test_driver_budget_skip_is_reported_before_fn_runs(
    store: SQLiteEngineStore,
) -> None:
    """Fix — the budget skip happens before the data connection is
    opened, so a skip never touches ``cal_econ_raw`` / doesn't fire
    the fn. Prevents a wasted commit attempt on a short-circuited
    connector. Tested on the value-side driver; schedule-side doesn't
    apply the cap so that path has no skip to verify."""
    import datetime as _dt

    today_iso = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")
    cap = DAILY_BUDGET_CAPS["bls"]
    with store.get_connection() as conn:
        record_connector_requests(conn, "bls", cap, today_iso=today_iso)
        conn.commit()

    def _would_write(conn, dry_run):
        conn.execute(
            "INSERT INTO cal_econ_raw "
            "(provider, provider_event_id, snapshot_epoch_ms, "
            " content_hash, payload_json, fetched_at) "
            "VALUES ('bls', 'should-not-land', 1, 'h', '{}', '2026-01-01')",
        )
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _would_write},
    )
    with store.get_connection() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_raw "
            "WHERE provider_event_id = 'should-not-land'",
        ).fetchone()[0]
    assert count == 0


def test_bls_client_exposes_daily_query_count() -> None:
    """BLSClient.daily_query_count is the public accessor the calendar
    fetcher reads before/after ``get_series`` to populate
    ``FetchRunSummary.requests_made``."""
    from ingestion.timeseries.scrapers.bls import BLSClient
    client = BLSClient(api_key="dummy")
    assert client.daily_query_count == 0
    # Simulate the client incrementing its counter.
    client._daily_query_count = 5  # noqa: SLF001 — white-box, no other writer.
    assert client.daily_query_count == 5


def test_bls_fetcher_populates_requests_made_from_client_delta() -> None:
    """``fetch_bls_calendar`` reads ``client.daily_query_count`` before
    and after ``get_series`` and reports the delta on the summary.
    Fake client exposes the same API shape."""
    from ingestion.calendar.bls_api.fetcher import fetch_bls_calendar
    from ingestion.timeseries.scrapers.bls import BLSObservation
    from storage.sqlite import SQLiteEngineStore

    class _FakeBLS:
        api_key = "dummy"

        def __init__(self) -> None:
            self._count = 10  # emulate prior same-day activity

        @property
        def daily_query_count(self) -> int:
            return self._count

        def get_series(self, series_ids, *, start_year, end_year):
            # Emulate two underlying BLS POSTs (>50-series chunking,
            # multi-year chunking, etc.). Returns one observation for
            # CPI so the projection path isn't empty.
            self._count += 2
            return {
                "CUUR0000SA0": [
                    BLSObservation(
                        series_id="CUUR0000SA0",
                        date="2026-03-01",
                        value=310.0,
                        period="M03",
                        raw={"year": "2026", "period": "M03",
                             "value": "310.0", "periodName": "March"},
                    ),
                ],
            }

    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        store = SQLiteEngineStore(db_path=Path(tmp) / "engine.db")
        with store.get_connection() as conn:
            summary = fetch_bls_calendar(
                conn, _FakeBLS(),
                start_year=2025,
                end_year=2026,
                series_ids=["CUUR0000SA0"],
                dry_run=False,
            )
    assert summary.requests_made == 2


def test_schedule_refresh_ignores_bls_budget_cap(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex R1 finding #1: the BLS API cap is a value-side
    concern only — ``schedule_bls_calendar`` hits ``bls.gov`` HTML
    (uncapped). A budget-exhausted state row must NOT block the
    daily schedule refresh, or the forward calendar would freeze
    from noon UTC to the next UTC midnight every day the sweep hit
    its 490-req ceiling."""
    import datetime as _dt

    today_iso = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")
    cap = DAILY_BUDGET_CAPS["bls"]
    with store.get_connection() as conn:
        # Seed exhausted state (as if a value-side sweep earlier today
        # burned the BLS cap).
        record_connector_requests(conn, "bls", cap, today_iso=today_iso)
        conn.commit()

    calls: list[str] = []

    def _ran(conn, dry_run):
        calls.append("bls")
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = refresh_all_schedules(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _ran},
    )
    # Schedule-side proceeded despite the exhausted cap; the fn ran.
    assert calls == ["bls"]
    assert summary.results[0].ok is True


def test_value_side_still_honors_bls_budget_cap(
    store: SQLiteEngineStore,
) -> None:
    """Counterpoint: value-side (``sweep_value_side``) hits
    ``api.bls.gov`` and does consume the capped budget. The same
    exhausted state row must skip it."""
    import datetime as _dt

    today_iso = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")
    cap = DAILY_BUDGET_CAPS["bls"]
    with store.get_connection() as conn:
        record_connector_requests(conn, "bls", cap, today_iso=today_iso)
        conn.commit()

    calls: list[str] = []

    def _would_run(conn, dry_run):
        calls.append("bls")
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _would_run},
    )
    assert calls == []  # skipped
    assert summary.results[0].ok is False
    assert "budget exhausted" in (summary.results[0].error or "")


def test_driver_records_requests_consumed_on_exception(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex R1 finding #2: a BLS ``get_series`` that raises
    after consuming some chunks (first chunk 200 OK, second chunk
    500) must still increment the persisted counter — otherwise a
    repeated-failure loop could burn the cap while the scheduler's
    budget gate saw zero.

    Emulated via an ``exc.requests_made`` attribute the value-side
    BLS shim attaches before re-raising. The driver reads it off the
    exception and records it the same way a summary field would."""
    import datetime as _dt

    today_iso = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")

    def _partial_then_fail(conn, dry_run):
        exc = RuntimeError("BLS 500 on chunk 2")
        exc.requests_made = 3  # first chunk landed before the 500
        raise exc

    sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _partial_then_fail},
    )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.requests_today == 3
    assert state.requests_day_utc == today_iso


def test_bls_shim_attaches_consumed_requests_to_exception(
    store: SQLiteEngineStore, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: drive the real ``_bls_values`` closure with a fake
    client that increments its counter between the try-entry and the
    raise. The scheduler should record the delta into today's budget
    even though the summary was never returned."""
    import datetime as _dt

    today_iso = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d")
    os_env = {"BLS_API_KEY": "dummy"}
    monkeypatch.setattr("os.environ", {**os_env})

    class _FailingBLS:
        api_key = "dummy"

        def __init__(self) -> None:
            self._count = 0

        @property
        def daily_query_count(self) -> int:
            return self._count

        def get_series(self, *args, **kwargs):
            # Simulate the BLS client incrementing its counter for
            # the first POST before the second POST errors out.
            self._count += 2
            raise RuntimeError("HTTP 500 after first chunk")

    # Patch BLSClient so the value-side closure picks up our fake.
    import ingestion.timeseries.scrapers.bls as bls_mod
    monkeypatch.setattr(bls_mod, "BLSClient", _FailingBLS)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        start_year=2025,
        end_year=2026,
    )
    assert summary.results[0].ok is False
    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.requests_today == 2
    assert state.requests_day_utc == today_iso


def test_utc_midnight_crossing_attributes_consumption_to_new_day(
    store: SQLiteEngineStore, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fix for Codex R2 finding: a value-side sweep that starts at
    23:59 UTC and finishes past 00:00 must attribute consumption to
    the record-time day, not the start-time day. ``BLSClient`` resets
    its in-memory counter at midnight so the observable
    ``requests_made`` already reflects post-midnight requests only;
    pairing that with the NEW day keeps the persisted counter aligned
    with the client's own reset semantics.

    Without the fix, the post-midnight delta was recorded against
    yesterday, which then caused the next sweep (on the new day) to
    see stale state and treat it as fresh, and yesterday's budget
    counter ran past the cap."""
    import datetime as _real_datetime
    import ingestion.calendar.scheduler as scheduler_mod
    import ingestion.calendar.scheduler_state as state_mod

    # Frozen-clock shim: the skip check fires first (before midnight);
    # ``today_utc_iso`` is then called at record time (after midnight).
    before_midnight = _real_datetime.datetime(
        2026, 6, 15, 23, 59, 30, tzinfo=_real_datetime.timezone.utc,
    )
    after_midnight = _real_datetime.datetime(
        2026, 6, 16, 0, 0, 30, tzinfo=_real_datetime.timezone.utc,
    )
    calls = {"n": 0}

    class _FrozenDatetime(_real_datetime.datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: D401 — match stdlib signature
            calls["n"] += 1
            return before_midnight if calls["n"] <= 1 else after_midnight

        @classmethod
        def fromtimestamp(cls, *args, **kwargs):
            return _real_datetime.datetime.fromtimestamp(*args, **kwargs)

    # ``today_utc_iso`` lives in scheduler_state; scheduler.py only
    # uses ``datetime`` directly for the breaker cool-down ISO format,
    # so patching both modules pins every wall-clock read in this run.
    monkeypatch.setattr(state_mod, "datetime", _FrozenDatetime)
    monkeypatch.setattr(scheduler_mod, "datetime", _FrozenDatetime)

    @dataclass
    class _BlsSummary:
        dry_run: bool
        requests_made: int

    def _consumed(conn, dry_run):
        # Post-midnight-only counter state — pretend the client reset
        # mid-sweep and this delta reflects only the requests that
        # landed on the new day.
        return _BlsSummary(dry_run=dry_run, requests_made=2)

    sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _consumed},
    )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    # The 2 requests should be attributed to 2026-06-16 (record-time
    # day), not 2026-06-15 (start-time day). On the next day's first
    # sweep the budget starts from 2, not from a stale 489-plus-2.
    assert state.requests_day_utc == "2026-06-16"
    assert state.requests_today == 2
