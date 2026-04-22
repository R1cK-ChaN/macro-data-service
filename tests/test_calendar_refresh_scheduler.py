"""Mocked tests for the cross-connector schedule refresh driver
(issue #9 P-sched-1).

Covers:

- Dry-run returns the per-connector plan with no HTTP.
- Execute mode hits every connector's override in the declared order.
- Per-connector exception is isolated — one connector raising does not
  skip the subsequent connectors.
- Per-connector commit / rollback — a failing connector's writes roll
  back while prior connectors' commits stay landed.
- ``connectors=[...]`` subset argument narrows the run.
- Service op ``calendar_econ_refresh_schedules`` dry-run + execute +
  subset path exposes the same shape.

The driver is written around a ``_connector_overrides`` test seam so
no real BLS / BEA / ECB / Fed / NBS HTTP fires in CI.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest

from ingestion.calendar.scheduler import (
    ALL_CONNECTORS,
    ConnectorResult,
    RefreshRunSummary,
    refresh_all_schedules,
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
