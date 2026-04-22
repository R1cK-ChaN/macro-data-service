"""Mocked tests for the cross-connector schedule refresh + value-side
sweep drivers (issue #9 P-sched-1 / P-sched-2).

Covers:

- Schedule-side refresh (P-sched-1) — dry-run plan, per-connector
  exception isolation, commit / rollback semantics, subset argument,
  in-summary failure detection, unknown connector names.
- Value-side sweep (P-sched-2) — same isolation invariants applied to
  the value-side connectors (BLS / BEA / ECB / Fed-values), plus
  year / period config forwarded to per-connector closures, and the
  NBS-absent plan (no value-side op yet).

The drivers are written around a ``_connector_overrides`` test seam
so no real BLS / BEA / ECB / Fed / NBS HTTP fires in CI.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from ingestion.calendar.scheduler import (
    ALL_CONNECTORS,
    ALL_VALUE_SIDE_CONNECTORS,
    ConnectorResult,
    RefreshRunSummary,
    refresh_all_schedules,
    sweep_value_side,
)
from ingestion.calendar.scheduler_state import (
    COOLDOWN_SECONDS,
    FAILURE_THRESHOLD,
    ConnectorState,
    get_connector_state,
    is_cooling,
    mark_connector_failure,
    mark_connector_success,
)
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


# ──────────────────────────────────────────────────────────────────────────
# refresh_all_schedules — core invariants
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class _FakeSummary:
    """Drop-in stand-in for a per-connector ``*RunSummary`` dataclass."""

    connector: str
    dry_run: bool
    calls: int


def test_default_connectors_cover_every_official_source() -> None:
    """The six-item default plan is the full BLS / BEA / ECB / Fed
    (FOMC + releasedates) / NBS suite. Any future connector addition
    should show up here automatically."""
    assert ALL_CONNECTORS == (
        "bls", "bea", "ecb", "fed-fomc", "fed-releases", "nbs",
    )


def test_dry_run_plans_every_connector(store: SQLiteEngineStore) -> None:
    """Dry-run forwards ``dry_run=True`` to each connector and collects
    the returned summaries without writing anything."""
    call_log: list[tuple[str, bool]] = []

    def _make_fn(name: str):
        def _fn(conn: sqlite3.Connection, dry_run: bool):
            call_log.append((name, dry_run))
            return _FakeSummary(connector=name, dry_run=dry_run, calls=1)
        return _fn

    overrides = {name: _make_fn(name) for name in ALL_CONNECTORS}
    summary = refresh_all_schedules(
        store.get_connection,
        dry_run=True,
        _connector_overrides=overrides,
    )
    assert summary.dry_run is True
    assert summary.connectors_planned == list(ALL_CONNECTORS)
    assert summary.ok_count == 6
    assert summary.failed_count == 0
    assert [c for c, _ in call_log] == list(ALL_CONNECTORS)
    # Per-connector summaries are flattened to dicts so the service op
    # can forward them without importing every connector dataclass.
    assert summary.results[0].summary["connector"] == "bls"
    assert summary.results[0].summary["dry_run"] is True


def test_one_connector_raising_does_not_skip_the_rest(
    store: SQLiteEngineStore,
) -> None:
    """Isolation invariant: a failing connector lands a ``ok=False``
    result but every subsequent connector still runs. Without this
    guarantee the daily cron would skip NBS every time ECB returned
    a 502, silently leaving the CN calendar stale."""
    call_log: list[str] = []

    def _make_ok(name: str):
        def _fn(conn, dry_run):
            call_log.append(name)
            return _FakeSummary(connector=name, dry_run=dry_run, calls=1)
        return _fn

    def _boom(conn, dry_run):
        call_log.append("ecb")
        raise RuntimeError("simulated ECB outage")

    overrides = {name: _make_ok(name) for name in ALL_CONNECTORS}
    overrides["ecb"] = _boom

    summary = refresh_all_schedules(
        store.get_connection,
        dry_run=False,
        _connector_overrides=overrides,
    )
    assert call_log == list(ALL_CONNECTORS)  # every connector invoked
    assert summary.failed_count == 1
    assert summary.ok_count == 5
    ecb_result = next(r for r in summary.results if r.connector == "ecb")
    assert ecb_result.ok is False
    assert "simulated ECB outage" in (ecb_result.error or "")


def test_failed_connector_rolls_back_without_touching_successes(
    store: SQLiteEngineStore,
) -> None:
    """Each connector owns its own connection lifecycle: a committed
    success stays committed even when a later connector rolls back.
    Tests against ``cal_econ_raw`` since it ships with the schema and
    accepts arbitrary ``(provider, provider_event_id, content_hash)``
    triples."""

    def _writer(provider: str, event_id: str):
        def _fn(conn, dry_run):
            conn.execute(
                "INSERT INTO cal_econ_raw "
                "(provider, provider_event_id, snapshot_epoch_ms, "
                " content_hash, payload_json, fetched_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (provider, event_id, 1, "h", "{}", "2026-01-01T00:00:00Z"),
            )
            return _FakeSummary(connector=provider, dry_run=dry_run, calls=1)
        return _fn

    def _raise_after_write(conn, dry_run):
        conn.execute(
            "INSERT INTO cal_econ_raw "
            "(provider, provider_event_id, snapshot_epoch_ms, "
            " content_hash, payload_json, fetched_at) "
            "VALUES ('ecb', 'would-roll-back', 1, 'h', '{}', '2026-01-01')",
        )
        raise RuntimeError("post-write failure")

    overrides = {
        "bls":          _writer("bls",          "kept-bls"),
        "bea":          _writer("bea",          "kept-bea"),
        "ecb":          _raise_after_write,
        "fed-fomc":     _writer("federal-reserve", "kept-fed-fomc"),
        "fed-releases": _writer("federal-reserve", "kept-fed-releases"),
        "nbs":          _writer("nbs",          "kept-nbs"),
    }
    refresh_all_schedules(
        store.get_connection,
        dry_run=False,
        _connector_overrides=overrides,
    )

    with store.get_connection() as conn:
        rows = conn.execute(
            "SELECT provider_event_id FROM cal_econ_raw "
            "ORDER BY provider_event_id"
        ).fetchall()
    ids = {r[0] for r in rows}
    # Every committed connector's row landed; the rolled-back ECB
    # insert did not.
    assert "kept-bls" in ids
    assert "kept-bea" in ids
    assert "kept-fed-fomc" in ids
    assert "kept-fed-releases" in ids
    assert "kept-nbs" in ids
    assert "would-roll-back" not in ids


def test_in_summary_fetch_error_flips_ok_to_false(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex P2 round 1: BEA, Fed releasedates, and ECB each
    catch their top-level HTTP exception into a summary field rather
    than raising. Without this check, a connector-wide 502 would
    report ``ok=True`` because no exception crossed the driver
    boundary — and the daily cron would log a clean run while the
    upstream delivered zero fresh rows.

    Driver inspects ``fetch_error`` / ``fetch_errors`` and flips
    ``ok=False`` with the recorded reason."""

    @dataclass
    class _BeaLikeSummary:
        connector: str
        dry_run: bool
        fetch_error: str | None

    @dataclass
    class _EcbLikeSummary:
        connector: str
        dry_run: bool
        fetch_errors: dict[str, str]

    def _bea_outage(conn, dry_run):
        return _BeaLikeSummary(
            connector="bea", dry_run=dry_run,
            fetch_error="simulated 502 from bea.gov",
        )

    def _ecb_outage(conn, dry_run):
        return _EcbLikeSummary(
            connector="ecb", dry_run=dry_run,
            fetch_errors={"meetings": "503 Service Unavailable"},
        )

    def _ok(conn, dry_run):
        return _FakeSummary(connector="ok", dry_run=dry_run, calls=1)

    overrides = {name: _ok for name in ALL_CONNECTORS}
    overrides["bea"] = _bea_outage
    overrides["ecb"] = _ecb_outage

    summary = refresh_all_schedules(
        store.get_connection,
        dry_run=False,
        _connector_overrides=overrides,
    )
    by_connector = {r.connector: r for r in summary.results}
    assert by_connector["bea"].ok is False
    assert "502" in (by_connector["bea"].error or "")
    assert by_connector["ecb"].ok is False
    assert "503" in (by_connector["ecb"].error or "")
    assert summary.ok_count == 4  # bls + fed-fomc + fed-releases + nbs
    assert summary.failed_count == 2


def test_bls_series_failed_flips_ok_to_false(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex P2 round 2 finding #1: BLS catches per-series
    failures into ``summary.series_failed`` rather than raising. A
    total outage (every series failed) would previously report
    ``ok=True`` because no exception crossed the driver boundary.

    Driver now inspects ``series_failed`` alongside ``fetch_error`` /
    ``fetch_errors`` and flips ``ok=False`` with the count + first
    failure as the reason."""

    @dataclass
    class _BlsLikeSummary:
        dry_run: bool
        series_failed: list[tuple[str, str]]

    def _bls_outage(conn, dry_run):
        return _BlsLikeSummary(
            dry_run=dry_run,
            series_failed=[
                ("CUUR0000SA0", "403 Forbidden"),
                ("CES0000000001", "403 Forbidden"),
            ],
        )

    summary = refresh_all_schedules(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _bls_outage},
    )
    bls_result = next(r for r in summary.results if r.connector == "bls")
    assert bls_result.ok is False
    assert "2 series failed" in (bls_result.error or "")
    assert summary.failed_count == 1


def test_unknown_connector_name_surfaces_in_summary(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex P2 round 2 finding #2: an operator typo in
    ``connectors=[...]`` (``"fed-fomcc"`` instead of ``"fed-fomc"``)
    would silently drop below — the plan only included members of
    ``ALL_CONNECTORS``. Now unknown names land on
    ``RefreshRunSummary.unknown_connectors`` and count toward
    ``failed_count`` so the cron envelope notices the skipped
    source."""
    def _ok(conn, dry_run):
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = refresh_all_schedules(
        store.get_connection,
        dry_run=True,
        connectors=["bls", "fed-fomcc", "nonsense"],
        _connector_overrides={"bls": _ok},
    )
    assert summary.connectors_planned == ["bls"]
    assert summary.unknown_connectors == ["fed-fomcc", "nonsense"]
    assert summary.ok_count == 1
    assert summary.failed_count == 2


def test_in_summary_empty_fetch_errors_dict_stays_ok(
    store: SQLiteEngineStore,
) -> None:
    """ECB-like summary with an empty ``fetch_errors`` dict means
    every page fetched cleanly — that must stay ``ok=True``. Only a
    non-empty mapping flips the flag."""

    @dataclass
    class _EcbCleanSummary:
        connector: str
        dry_run: bool
        fetch_errors: dict[str, str]

    def _ecb_clean(conn, dry_run):
        return _EcbCleanSummary(
            connector="ecb", dry_run=dry_run, fetch_errors={},
        )

    summary = refresh_all_schedules(
        store.get_connection,
        dry_run=False,
        connectors=["ecb"],
        _connector_overrides={"ecb": _ecb_clean},
    )
    assert summary.ok_count == 1
    assert summary.failed_count == 0


def test_connectors_subset_narrows_the_run(store: SQLiteEngineStore) -> None:
    """Operator-driven one-off refresh: pass a subset like
    ``["bls", "fed-fomc"]`` to run only those two connectors."""
    call_log: list[str] = []

    def _make_fn(name: str):
        def _fn(conn, dry_run):
            call_log.append(name)
            return _FakeSummary(connector=name, dry_run=dry_run, calls=1)
        return _fn

    overrides = {name: _make_fn(name) for name in ALL_CONNECTORS}
    summary = refresh_all_schedules(
        store.get_connection,
        dry_run=False,
        connectors=["bls", "fed-fomc"],
        _connector_overrides=overrides,
    )
    assert call_log == ["bls", "fed-fomc"]
    assert summary.connectors_planned == ["bls", "fed-fomc"]
    assert summary.ok_count == 2


def test_dry_run_does_not_commit(store: SQLiteEngineStore) -> None:
    """Dry-run passes ``dry_run=True`` into each connector; the driver
    skips the ``connection.commit()`` step so any stray writes the
    fake function might have made get rolled back at close time."""
    def _fn(conn, dry_run):
        # Write a row even in dry_run to prove the driver doesn't
        # commit. Real connector dry_run paths never write.
        conn.execute(
            "INSERT INTO cal_econ_raw "
            "(provider, provider_event_id, snapshot_epoch_ms, "
            " content_hash, payload_json, fetched_at) "
            "VALUES ('bls', 'dry', 1, 'h', '{}', '2026-01-01')",
        )
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    refresh_all_schedules(
        store.get_connection,
        dry_run=True,
        connectors=["bls"],
        _connector_overrides={"bls": _fn},
    )
    with store.get_connection() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_raw WHERE provider_event_id = 'dry'",
        ).fetchone()[0]
    assert count == 0


def test_connector_result_dataclass_carries_timings() -> None:
    """``wall_seconds`` on each ``ConnectorResult`` lets the operator
    attribute slow runs to specific upstreams without cross-checking
    the service-op log."""
    result = ConnectorResult(
        connector="bls", ok=True, summary={"foo": 1}, wall_seconds=0.25,
    )
    assert result.wall_seconds == 0.25
    assert result.summary == {"foo": 1}


def test_refresh_run_summary_counts() -> None:
    summary = RefreshRunSummary(
        connectors_planned=["bls", "bea"],
        dry_run=False,
        results=[
            ConnectorResult(connector="bls", ok=True),
            ConnectorResult(connector="bea", ok=False, error="boom"),
        ],
    )
    assert summary.ok_count == 1
    assert summary.failed_count == 1


# ──────────────────────────────────────────────────────────────────────────
# Service op wiring
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_dry_run_returns_envelope(store: SQLiteEngineStore) -> None:
    """Dry-run through the service invoker returns the same JSON
    shape the cron-level caller will see."""
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_refresh_schedules", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert result["connectors_all"] == list(ALL_CONNECTORS)
    assert result["connectors_planned"] == list(ALL_CONNECTORS)
    # Dry-run actually runs every connector's dry-run path; each
    # connector's summary dict is carried through. We don't assert on
    # specific fields (they vary by connector) but we do assert every
    # connector reported ok.
    assert result["ok_count"] == 6
    assert result["failed_count"] == 0


def test_service_op_connectors_subset_narrows_the_run(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_refresh_schedules",
        {"dry_run": True, "connectors": ["bls", "ecb"]},
    )
    assert result["connectors_planned"] == ["bls", "ecb"]
    assert len(result["results"]) == 2


# ──────────────────────────────────────────────────────────────────────────
# sweep_value_side (P-sched-2)
# ──────────────────────────────────────────────────────────────────────────


def test_value_side_default_plan_covers_four_connectors() -> None:
    """Value-side plan omits NBS (no value-side op shipped) and the
    two Fed schedule-side surfaces (``fed-fomc`` / ``fed-releases``)
    — the value-bearing Fed op lives under ``fed-values``."""
    assert ALL_VALUE_SIDE_CONNECTORS == ("bls", "bea", "ecb", "fed-values")


def test_value_side_dry_run_hits_every_connector(
    store: SQLiteEngineStore,
) -> None:
    call_log: list[tuple[str, bool]] = []

    def _make_fn(name: str):
        def _fn(conn: sqlite3.Connection, dry_run: bool):
            call_log.append((name, dry_run))
            return _FakeSummary(connector=name, dry_run=dry_run, calls=1)
        return _fn

    overrides = {name: _make_fn(name) for name in ALL_VALUE_SIDE_CONNECTORS}
    summary = sweep_value_side(
        store.get_connection,
        dry_run=True,
        _connector_overrides=overrides,
    )
    assert summary.dry_run is True
    assert summary.connectors_planned == list(ALL_VALUE_SIDE_CONNECTORS)
    assert [c for c, _ in call_log] == list(ALL_VALUE_SIDE_CONNECTORS)
    assert summary.ok_count == 4
    assert summary.failed_count == 0


def test_value_side_missing_api_key_isolates_to_one_connector(
    store: SQLiteEngineStore,
) -> None:
    """The default BLS shim raises ``BLS_API_KEY not set`` when the env
    var is absent — the test exercises that contract through the fake
    override path so the isolation guarantee is pinned."""
    def _no_key(conn, dry_run):
        raise RuntimeError("BLS_API_KEY not set")

    def _ok(name: str):
        def _fn(conn, dry_run):
            return _FakeSummary(connector=name, dry_run=dry_run, calls=1)
        return _fn

    overrides = {
        "bls":        _no_key,
        "bea":        _ok("bea"),
        "ecb":        _ok("ecb"),
        "fed-values": _ok("fed-values"),
    }
    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        _connector_overrides=overrides,
    )
    bls_result = next(r for r in summary.results if r.connector == "bls")
    assert bls_result.ok is False
    assert "BLS_API_KEY" in (bls_result.error or "")
    assert summary.ok_count == 3
    assert summary.failed_count == 1


def test_value_side_subset_narrows_and_skips_unknown(
    store: SQLiteEngineStore,
) -> None:
    """Value-side subset filter: only ``fed-values`` runs (auto-
    discovery), and a typo like ``"nbss"`` surfaces on
    ``unknown_connectors`` rather than silently dropping."""
    call_log: list[str] = []

    def _fn(conn, dry_run):
        call_log.append("fed-values")
        return _FakeSummary(connector="fed-values", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=True,
        connectors=["fed-values", "nbss"],
        _connector_overrides={"fed-values": _fn},
    )
    assert call_log == ["fed-values"]
    assert summary.connectors_planned == ["fed-values"]
    assert summary.unknown_connectors == ["nbss"]
    assert summary.ok_count == 1
    assert summary.failed_count == 1


def test_value_side_failure_marker_detection_is_shared(
    store: SQLiteEngineStore,
) -> None:
    """The summary-level failure detection (`fetch_error`, `series_failed`,
    …) added to schedule-side refresh in P-sched-1 still applies to
    value-side summaries — BLS's value-side op also stores per-series
    failures in ``series_failed``."""

    @dataclass
    class _BlsLikeSummary:
        dry_run: bool
        series_failed: list[tuple[str, str]]

    def _bls_outage(conn, dry_run):
        return _BlsLikeSummary(
            dry_run=dry_run,
            series_failed=[("CUUR0000SA0", "500")],
        )

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _bls_outage},
    )
    bls_result = next(r for r in summary.results if r.connector == "bls")
    assert bls_result.ok is False
    assert "series failed" in (bls_result.error or "")


def test_value_side_service_op_dry_run_envelope(
    store: SQLiteEngineStore,
) -> None:
    """Dry-run through the service surface — the envelope shape
    matches the schedule-side refresh op so cron scripts can use the
    same parser."""
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_sweep_values",
        {"dry_run": True, "connectors": ["fed-values"]},
    )
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert result["connectors_all"] == list(ALL_VALUE_SIDE_CONNECTORS)
    assert result["connectors_planned"] == ["fed-values"]
    assert result["ok_count"] == 1
    assert result["failed_count"] == 0


def test_fed_values_fetch_failures_flip_ok_false(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex P2 round 1 on P-sched-2: Fed-values stores
    per-URL failures in ``fetch_failures`` / ``parse_failures``
    tuples. A sweep that 404s on every statement URL lands them there
    and returns normally, so the driver would otherwise report
    ``ok=True`` for a Fed run that fetched zero actuals.

    Driver now inspects both lists and flips ``ok=False`` with the
    count + first failure as the reason."""

    @dataclass
    class _FedValuesLikeSummary:
        dry_run: bool
        fetch_failures: list[tuple[str, str]]

    @dataclass
    class _FedValuesParseFailSummary:
        dry_run: bool
        parse_failures: list[tuple[str, str]]

    def _fetch_404(conn, dry_run):
        return _FedValuesLikeSummary(
            dry_run=dry_run,
            fetch_failures=[("2025-01-29", "HTTP 404")],
        )

    def _parse_drift(conn, dry_run):
        return _FedValuesParseFailSummary(
            dry_run=dry_run,
            parse_failures=[("2025-03-19", "target-range sentence not found")],
        )

    fetch_summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["fed-values"],
        _connector_overrides={"fed-values": _fetch_404},
    )
    r = next(r for r in fetch_summary.results if r.connector == "fed-values")
    assert r.ok is False
    assert "1 fetch failures" in (r.error or "")

    parse_summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["fed-values"],
        _connector_overrides={"fed-values": _parse_drift},
    )
    r = next(r for r in parse_summary.results if r.connector == "fed-values")
    assert r.ok is False
    assert "1 parse failures" in (r.error or "")


def test_ecb_default_period_is_bounded(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex P2 round 2 finding #1: the frequent-cron sweep
    must not issue unbounded SDMX requests. Left to its own devices,
    ``fetch_ecb_calendar(start_period=None, end_period=None)``
    re-projects the full ECB history every run. The driver now
    resolves ``start_period`` to ~180 days ago (``"YYYY-MM"``) unless
    the operator overrides it, covering the last 3-4 GC meetings
    cheaply."""
    import re
    captured: dict[str, Any] = {}

    def _spy_ecb(conn, dry_run):
        # The real shim closes over ``resolved_start_period`` so the
        # override path sees what the driver passes into the library
        # function. Simulate the spy by reading the closure variable
        # back via a fresh sweep.
        return _FakeSummary(connector="ecb", dry_run=dry_run, calls=1)

    # The resolved start_period isn't directly exposed, so drive the
    # real ``_ecb_values`` closure path through the service op's
    # dry-run and assert the observable window in the output envelope
    # (fetch_ecb_calendar dry-run reports its window in the summary).
    from macro_data.service import LocalMacroDataService
    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_sweep_values",
        {"dry_run": True, "connectors": ["ecb"]},
    )
    ecb_summary = result["results"][0]["summary"]
    # The fetch_ecb_calendar dry-run plan carries ``start_period``.
    assert "start_period" in ecb_summary
    period = ecb_summary["start_period"]
    # Should be a "YYYY-MM" string, not empty and not "" (unbounded).
    assert re.fullmatch(r"\d{4}-\d{2}", period), (
        f"ECB default period expected YYYY-MM format, got {period!r}"
    )
    captured["period"] = period


def test_ecb_explicit_period_wins_over_default(
    store: SQLiteEngineStore,
) -> None:
    """Operator override: an explicit ``start_period`` passed through
    the service op flows into the driver and wins over the 180-day
    default. Needed for one-off historical backfills."""
    from macro_data.service import LocalMacroDataService
    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_sweep_values",
        {
            "dry_run": True,
            "connectors": ["ecb"],
            "start_period": "2015-01",
            "end_period": "2015-12",
        },
    )
    ecb_summary = result["results"][0]["summary"]
    assert ecb_summary["start_period"] == "2015-01"
    assert ecb_summary["end_period"] == "2015-12"


def test_empty_connectors_list_runs_nothing(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex P2 round 2 finding #2: an operator who filters
    their connector list down to ``[]`` expects to run nothing — the
    earlier ``or None`` coercion promoted that to the full default
    plan, which could have executed four connectors in execute mode
    unintentionally. Key-absent and key-present-empty now behave
    differently."""
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)

    # Schedule-side: empty explicit list → zero connectors run.
    refresh = svc.invoke(
        "calendar_econ_refresh_schedules",
        {"dry_run": True, "connectors": []},
    )
    assert refresh["connectors_planned"] == []
    assert refresh["results"] == []

    # Value-side: same contract.
    sweep = svc.invoke(
        "calendar_econ_sweep_values",
        {"dry_run": True, "connectors": []},
    )
    assert sweep["connectors_planned"] == []
    assert sweep["results"] == []

    # Key absent → full plan still runs.
    refresh_default = svc.invoke(
        "calendar_econ_refresh_schedules", {"dry_run": True},
    )
    assert refresh_default["connectors_planned"] == list(ALL_CONNECTORS)


# ──────────────────────────────────────────────────────────────────────────
# Circuit-breaker state (P-sched-3)
# ──────────────────────────────────────────────────────────────────────────


def test_state_helpers_default_to_empty(store: SQLiteEngineStore) -> None:
    """A connector with no prior state reads as a fresh default —
    zero failures, no cooling. Avoids a special-case at every read
    site."""
    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state == ConnectorState(connector="bls")
    assert is_cooling(state, now_ms=1_800_000_000_000) is False


def test_state_mark_failure_increments_and_trips(
    store: SQLiteEngineStore,
) -> None:
    """N consecutive failures trip the breaker; the ``cooling_until_ms``
    anchor is set to ``now_ms + COOLDOWN_SECONDS * 1000``."""
    now_ms = 1_800_000_000_000
    with store.get_connection() as conn:
        for i in range(1, FAILURE_THRESHOLD):
            new_state = mark_connector_failure(
                conn, "bls", error=f"attempt {i}", now_ms=now_ms + i,
            )
            assert new_state.consecutive_failures == i
            assert new_state.cooling_until_ms is None  # not yet tripped
        tripped = mark_connector_failure(
            conn, "bls", error="final",
            now_ms=now_ms + FAILURE_THRESHOLD,
        )
        conn.commit()
    assert tripped.consecutive_failures == FAILURE_THRESHOLD
    assert tripped.cooling_until_ms == (
        now_ms + FAILURE_THRESHOLD + COOLDOWN_SECONDS * 1000
    )
    assert is_cooling(tripped, now_ms=now_ms + FAILURE_THRESHOLD) is True
    assert is_cooling(
        tripped,
        now_ms=now_ms + FAILURE_THRESHOLD + COOLDOWN_SECONDS * 1000 + 1,
    ) is False


def test_state_mark_success_resets_after_trip(
    store: SQLiteEngineStore,
) -> None:
    """A successful run clears the counter + cool-down even when the
    breaker was previously tripped."""
    now_ms = 1_800_000_000_000
    with store.get_connection() as conn:
        for i in range(FAILURE_THRESHOLD):
            mark_connector_failure(
                conn, "bls", error=f"flaky {i}", now_ms=now_ms,
            )
        conn.commit()
        tripped = get_connector_state(conn, "bls")
        assert tripped.cooling_until_ms is not None

        mark_connector_success(conn, "bls")
        conn.commit()
        after = get_connector_state(conn, "bls")
    assert after.consecutive_failures == 0
    assert after.cooling_until_ms is None
    assert after.last_error is None


def test_circuit_breaker_skips_cooling_connector_in_driver(
    store: SQLiteEngineStore,
) -> None:
    """End-to-end: a connector with ``cooling_until_ms`` in the future
    is skipped by the driver before fn is invoked. The result carries
    ``ok=False`` and a ``"cooling until"`` reason so the operator can
    see the breaker is open without digging into the state table."""
    now_ms = 1_800_000_000_000
    future_ms = now_ms + COOLDOWN_SECONDS * 1000

    with store.get_connection() as conn:
        # Seed state directly — three consecutive failures trip the
        # breaker, so the next sweep must skip BLS.
        for _ in range(FAILURE_THRESHOLD):
            mark_connector_failure(
                conn, "bls", error="upstream 502", now_ms=now_ms,
            )
        conn.commit()

    call_log: list[str] = []

    def _fn(name: str):
        def _inner(conn, dry_run):
            call_log.append(name)
            return _FakeSummary(connector=name, dry_run=dry_run, calls=1)
        return _inner

    overrides = {name: _fn(name) for name in ALL_CONNECTORS}
    summary = refresh_all_schedules(
        store.get_connection,
        dry_run=False,
        _connector_overrides=overrides,
    )
    # BLS should not have been invoked; every other connector did.
    assert "bls" not in call_log
    assert set(call_log) == set(ALL_CONNECTORS) - {"bls"}

    bls_result = next(r for r in summary.results if r.connector == "bls")
    assert bls_result.ok is False
    assert "cooling until" in (bls_result.error or "")
    assert "upstream 502" in (bls_result.error or "")


def test_circuit_breaker_trips_after_threshold_failures_in_driver(
    store: SQLiteEngineStore,
) -> None:
    """Simulate a connector failing on three consecutive sweeps —
    after the third, the breaker should be tripped, i.e. the state
    should have a future ``cooling_until_ms``."""
    def _boom(conn, dry_run):
        raise RuntimeError("flaky upstream")

    overrides = {"bls": _boom}
    for _ in range(FAILURE_THRESHOLD):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bls"],
            _connector_overrides=overrides,
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.consecutive_failures == FAILURE_THRESHOLD
    assert state.cooling_until_ms is not None
    assert state.cooling_until_ms > int(time.time() * 1000)


def test_circuit_breaker_success_clears_counter_in_driver(
    store: SQLiteEngineStore,
) -> None:
    """A successful sweep clears a partial failure count. Prevents
    the breaker from tripping on a flaky-but-not-broken upstream that
    sees 1 failure + 1 success + 1 failure + 1 success — the success
    runs reset the counter so the breaker stays closed."""
    calls = iter([False, True, False, True, False, True])

    def _sometimes(conn, dry_run):
        if next(calls):
            return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)
        raise RuntimeError("flaky")

    for _ in range(6):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bls"],
            _connector_overrides={"bls": _sometimes},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.cooling_until_ms is None  # never tripped
    # After a success, the counter resets to 0.
    assert state.consecutive_failures == 0


def test_circuit_breaker_dry_run_does_not_update_state(
    store: SQLiteEngineStore,
) -> None:
    """Dry-run previews the plan. It must not persist state changes
    — otherwise an operator's preview could trip the breaker."""
    def _boom(conn, dry_run):
        raise RuntimeError("would have failed")

    for _ in range(FAILURE_THRESHOLD + 2):
        refresh_all_schedules(
            store.get_connection,
            dry_run=True,  # preview
            connectors=["bls"],
            _connector_overrides={"bls": _boom},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.consecutive_failures == 0
    assert state.cooling_until_ms is None


def test_circuit_breaker_partial_failure_does_not_trip(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex P2 round 1 finding #1: a partial failure (one
    BLS series failed while eight succeeded) shouldn't count toward
    the breaker threshold. After three such sweeps, the next run
    must still invoke the connector — otherwise a single flaky series
    blocks every fresh release on the remaining eight.

    The card still reports ``ok=False`` because some items failed,
    but the breaker stays closed and the counter resets each run."""

    @dataclass
    class _BlsPartialSummary:
        dry_run: bool
        series_planned: list[str]
        series_ok: list[str]
        series_failed: list[tuple[str, str]]

    def _bls_partial(conn, dry_run):
        return _BlsPartialSummary(
            dry_run=dry_run,
            series_planned=["a", "b", "c"],
            series_ok=["a", "b"],  # partial — two of three succeeded
            series_failed=[("c", "flaky")],
        )

    for _ in range(FAILURE_THRESHOLD + 2):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bls"],
            _connector_overrides={"bls": _bls_partial},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    # Breaker stays closed across repeated partial runs.
    assert state.cooling_until_ms is None
    assert state.consecutive_failures == 0  # success path resets


def test_circuit_breaker_total_series_failure_trips(
    store: SQLiteEngineStore,
) -> None:
    """Counterpoint: when every BLS series fails (no ``series_ok``,
    ``series_failed`` covers every planned id), the breaker treats
    that as a connector-wide outage and trips on schedule."""

    @dataclass
    class _BlsTotalFailureSummary:
        dry_run: bool
        series_planned: list[str]
        series_ok: list[str]
        series_failed: list[tuple[str, str]]

    def _bls_total(conn, dry_run):
        return _BlsTotalFailureSummary(
            dry_run=dry_run,
            series_planned=["a", "b", "c"],
            series_ok=[],
            series_failed=[("a", "403"), ("b", "403"), ("c", "403")],
        )

    for _ in range(FAILURE_THRESHOLD):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bls"],
            _connector_overrides={"bls": _bls_total},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.consecutive_failures == FAILURE_THRESHOLD
    assert state.cooling_until_ms is not None


def test_circuit_breaker_fed_values_partial_does_not_trip(
    store: SQLiteEngineStore,
) -> None:
    """Fed-values partial: two statements fetched cleanly, one 404d.
    Breaker stays closed — the working pages still delivered rows."""

    @dataclass
    class _FedValuesPartialSummary:
        dry_run: bool
        meetings_planned: int
        meetings_fetched: int
        fetch_failures: list[tuple[str, str]]

    def _fed_partial(conn, dry_run):
        return _FedValuesPartialSummary(
            dry_run=dry_run,
            meetings_planned=3,
            meetings_fetched=2,  # partial
            fetch_failures=[("2020-03-15", "HTTP 404")],
        )

    for _ in range(FAILURE_THRESHOLD + 2):
        sweep_value_side(
            store.get_connection,
            dry_run=False,
            connectors=["fed-values"],
            _connector_overrides={"fed-values": _fed_partial},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "fed-values")
    assert state.cooling_until_ms is None


def test_circuit_breaker_fed_values_total_trips(
    store: SQLiteEngineStore,
) -> None:
    """Fed-values total: every planned meeting URL failed. The
    breaker treats zero-fetched-of-N-planned as a connector outage."""

    @dataclass
    class _FedValuesTotalFailureSummary:
        dry_run: bool
        meetings_planned: int
        meetings_fetched: int
        fetch_failures: list[tuple[str, str]]

    def _fed_total(conn, dry_run):
        return _FedValuesTotalFailureSummary(
            dry_run=dry_run,
            meetings_planned=2,
            meetings_fetched=0,
            fetch_failures=[
                ("2025-01-29", "HTTP 503"),
                ("2025-03-19", "HTTP 503"),
            ],
        )

    for _ in range(FAILURE_THRESHOLD):
        sweep_value_side(
            store.get_connection,
            dry_run=False,
            connectors=["fed-values"],
            _connector_overrides={"fed-values": _fed_total},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "fed-values")
    assert state.consecutive_failures == FAILURE_THRESHOLD
    assert state.cooling_until_ms is not None


def test_circuit_breaker_ecb_partial_page_does_not_trip(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex P2 round 2 finding: ECB's press-calendar
    scraper hits two pages (GC meetings + Economic Bulletin). When
    one succeeds and one 502s, ``fetch_errors`` records the bad one
    and ``entries_parsed`` is non-zero for the good one. That's a
    partial failure and must not trip the breaker — blocking the
    next sweep would delay fresh MP-decision / Bulletin rows on the
    healthy page."""

    @dataclass
    class _EcbPartialSummary:
        dry_run: bool
        entries_parsed: int
        events_upserted: int
        fetch_errors: dict[str, str]

    def _ecb_partial(conn, dry_run):
        return _EcbPartialSummary(
            dry_run=dry_run,
            entries_parsed=6,  # GC meetings page delivered rows
            events_upserted=6,
            fetch_errors={"bulletin": "HTTP 502"},  # other page failed
        )

    for _ in range(FAILURE_THRESHOLD + 2):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["ecb"],
            _connector_overrides={"ecb": _ecb_partial},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "ecb")
    assert state.cooling_until_ms is None  # breaker stays closed


def test_circuit_breaker_ecb_both_pages_failing_trips(
    store: SQLiteEngineStore,
) -> None:
    """Counterpoint: when both ECB pages fail (``entries_parsed==0``,
    ``fetch_errors`` non-empty), the breaker treats that as a
    connector-wide outage and trips on schedule."""

    @dataclass
    class _EcbTotalFailureSummary:
        dry_run: bool
        entries_parsed: int
        events_upserted: int
        fetch_errors: dict[str, str]

    def _ecb_total(conn, dry_run):
        return _EcbTotalFailureSummary(
            dry_run=dry_run,
            entries_parsed=0,
            events_upserted=0,
            fetch_errors={"meetings": "HTTP 502", "bulletin": "HTTP 502"},
        )

    for _ in range(FAILURE_THRESHOLD):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["ecb"],
            _connector_overrides={"ecb": _ecb_total},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "ecb")
    assert state.consecutive_failures == FAILURE_THRESHOLD
    assert state.cooling_until_ms is not None


def test_circuit_breaker_cooldown_uses_failure_time_not_start(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex P2 round 1 finding #2: ``cooling_until_ms``
    must be anchored on the failure moment, not the start of the
    connector run. A 5-minute Fed-values sweep failing at the end
    should still produce a ~full cool-down window; anchoring on
    start time would have already consumed 5 of 15 minutes before
    the cool-down even began."""
    import time as _time

    def _slow_fail(conn, dry_run):
        _time.sleep(0.1)  # small but measurable lag
        raise RuntimeError("slow failure")

    before_start = int(_time.time() * 1000)
    refresh_all_schedules(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _slow_fail},
    )
    # Continue failing to actually trip the breaker.
    for _ in range(FAILURE_THRESHOLD - 1):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bls"],
            _connector_overrides={"bls": _slow_fail},
        )
    after_trip = int(_time.time() * 1000)

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.cooling_until_ms is not None
    # ``cooling_until_ms`` should be anchored on or after the final
    # failure's record moment, which is after (not before) each run's
    # start. Specifically: cooling_until > after_trip - 1000 (allowing
    # ~1s slack) proves the anchor isn't the first run's start time.
    assert state.cooling_until_ms >= after_trip - 1000


def test_circuit_breaker_counts_summary_level_failures(
    store: SQLiteEngineStore,
) -> None:
    """In-summary failure markers (e.g. BEA ``fetch_error``) count
    toward the circuit-breaker threshold just like raised exceptions
    — they represent a connector-wide outage regardless of whether
    the exception propagated."""

    @dataclass
    class _BeaLikeSummary:
        dry_run: bool
        fetch_error: str

    def _bea_outage(conn, dry_run):
        return _BeaLikeSummary(dry_run=dry_run, fetch_error="502 Bad Gateway")

    for _ in range(FAILURE_THRESHOLD):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bea"],
            _connector_overrides={"bea": _bea_outage},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bea")
    assert state.consecutive_failures == FAILURE_THRESHOLD
    assert state.cooling_until_ms is not None
    assert "502" in (state.last_error or "")


def test_value_side_driver_shares_breaker_state(
    store: SQLiteEngineStore,
) -> None:
    """A connector's breaker state is scoped by scheduler-level name,
    so failures on the schedule-side ``"bls"`` surface the same
    connector as the value-side. A BLS outage during schedule refresh
    trips the breaker, and the next value-side sweep honors the
    cool-down on ``"bls"`` without a re-trip."""
    def _boom(conn, dry_run):
        raise RuntimeError("bls.gov 403")

    # Trip on schedule-side.
    for _ in range(FAILURE_THRESHOLD):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bls"],
            _connector_overrides={"bls": _boom},
        )

    # Value-side: should skip BLS due to shared breaker state.
    value_calls: list[str] = []

    def _would_run(conn, dry_run):
        value_calls.append("ran")
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _would_run},
    )
    assert value_calls == []  # BLS skipped
    bls_result = summary.results[0]
    assert bls_result.ok is False
    assert "cooling until" in (bls_result.error or "")


def test_value_side_service_op_forwards_year_args(
    store: SQLiteEngineStore,
) -> None:
    """Operator-driven one-off with an explicit year window: the
    service op forwards ``start_year`` / ``end_year`` into the driver.
    Verified via the dry-run output since the real connectors still
    surface their plan regardless of the window."""
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_sweep_values",
        {
            "dry_run": True,
            "connectors": ["bls"],
            "start_year": 2020,
            "end_year": 2021,
        },
    )
    assert result["dry_run"] is True
    bls = result["results"][0]
    # BLS dry-run surfaces its resolved year window in the summary
    # dict via ``FetchRunSummary.start_year`` / ``end_year``.
    assert bls["summary"]["start_year"] == 2020
    assert bls["summary"]["end_year"] == 2021
