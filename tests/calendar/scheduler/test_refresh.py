"""Scheduler tests: core invariants + service-op wiring + sweep_value_side.

Split out of the original tests/test_calendar_refresh_scheduler.py
as part of issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
import pytest
from storage.sqlite import SQLiteEngineStore

from ingestion.calendar.scheduler import (
    ALL_CONNECTORS,
    ALL_VALUE_SIDE_CONNECTORS,
    ConnectorResult,
    RefreshRunSummary,
    refresh_all_schedules,
    sweep_value_side,
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


def test_default_connectors_cover_every_official_source() -> None:
    """The default plan is the full official-source suite."""
    assert ALL_CONNECTORS == (
        "bls", "bea", "census", "ism", "umich", "conference-board",
        "nar", "ecb", "eia", "dol", "ons", "boe", "statcan", "boc",
        "abs", "rba", "mospi", "rbi", "kostat", "bok",
        "ibge", "bcb", "tuik", "tcmb",
        "eurostat", "destatis", "zew", "ifo", "gfk", "hcob", "ec-bcs", "insee", "ine", "istat",
        "fed-fomc", "fed-releases", "nbs", "stat-bureau-jp", "boj",
        "boj-tankan", "mof-jp", "cao", "cao-gdp", "meti",
        "fed-speeches", "ecb-speeches", "boe-speeches", "boj-speeches",
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
    assert summary.ok_count == len(ALL_CONNECTORS)
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
    assert summary.ok_count == len(ALL_CONNECTORS) - 1
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
        "census":       _writer("census",       "kept-census"),
        "ism":          _writer("ism",          "kept-ism"),
        "umich":        _writer("umich",        "kept-umich"),
        "conference-board": _writer("conference-board", "kept-conference-board"),
        "nar":          _writer("nar",          "kept-nar"),
        "ecb":          _raise_after_write,
        "eurostat":     _writer("eurostat",     "kept-eurostat"),
        "destatis":     _writer("destatis",     "kept-destatis"),
        "zew":          _writer("zew",          "kept-zew"),
        "ifo":          _writer("ifo",          "kept-ifo"),
        "gfk":          _writer("gfk",          "kept-gfk"),
        "hcob":         _writer("hcob",         "kept-hcob"),
        "ec-bcs":       _writer("ec-bcs",       "kept-ec-bcs"),
        "insee":        _writer("insee",        "kept-insee"),
        "ine":           _writer("ine",          "kept-ine"),
        "istat":        _writer("istat",        "kept-istat"),
        "fed-fomc":     _writer("federal-reserve", "kept-fed-fomc"),
        "fed-releases": _writer("federal-reserve", "kept-fed-releases"),
        "nbs":          _writer("nbs",          "kept-nbs"),
        "stat-bureau-jp": _writer("stat-bureau-jp", "kept-stat-bureau"),
        "boj":          _writer("boj",          "kept-boj"),
        "boj-tankan":   _writer("boj",          "kept-boj-tankan"),
        "mof-jp":       _writer("mof-jp",       "kept-mof-jp"),
        "cao":          _writer("cao",          "kept-cao"),
        "cao-gdp":      _writer("cao",          "kept-cao-gdp"),
        "meti":         _writer("meti",         "kept-meti"),
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
    assert "kept-census" in ids
    assert "kept-ism" in ids
    assert "kept-umich" in ids
    assert "kept-conference-board" in ids
    assert "kept-nar" in ids
    assert "kept-eurostat" in ids
    assert "kept-destatis" in ids
    assert "kept-zew" in ids
    assert "kept-ifo" in ids
    assert "kept-gfk" in ids
    assert "kept-hcob" in ids
    assert "kept-ec-bcs" in ids
    assert "kept-insee" in ids
    assert "kept-ine" in ids
    assert "kept-istat" in ids
    assert "kept-fed-fomc" in ids
    assert "kept-fed-releases" in ids
    assert "kept-nbs" in ids
    assert "kept-stat-bureau" in ids
    assert "kept-boj" in ids
    assert "kept-boj-tankan" in ids
    assert "kept-mof-jp" in ids
    assert "kept-cao" in ids
    assert "kept-cao-gdp" in ids
    assert "kept-meti" in ids
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
    assert summary.ok_count == len(ALL_CONNECTORS) - 2
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
    assert result["ok_count"] == len(ALL_CONNECTORS)
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


def test_value_side_default_plan_covers_connectors() -> None:
    """Value-side plan omits the two Fed schedule-side surfaces
    (``fed-fomc`` / ``fed-releases``) — the value-bearing Fed op
    lives under ``fed-values``. Issue #49 adds ``nbs-values`` for
    five China indicators; issue #50 adds ``eia`` + ``dol`` (both
    combine schedule + value); issue #51 adds ``ons`` + ``boe``
    (UK coverage; same schedule-and-value-together shape); issue
    #52 adds ``statcan`` + ``boc`` (Canada coverage); issue #53 adds
    ``abs`` + ``rba`` (Australia coverage; ABS schedule-only,
    RBA schedule-and-value-together); issue #54 adds ``mospi`` +
    ``rbi`` (India coverage; both schedule-only in P1, value-side
    PDF / press-release scrape deferred to P2); issue #55 adds
    ``kostat`` + ``bok`` (Korea coverage; both schedule-only in P1,
    value-side press-release scrape deferred to P2). Issue #56 adds
    the four schedule-only ``*-speeches`` connectors (Fed / ECB / BoE
    / BoJ) — the value-side sweep re-runs them so newly-posted
    speeches land mid-day even though they have no value to fill."""
    assert ALL_VALUE_SIDE_CONNECTORS == (
        "bls", "bea", "census", "ism", "umich", "conference-board",
        "nar", "ecb", "eia", "dol", "ons", "boe", "statcan", "boc",
        "abs", "rba", "mospi", "rbi", "kostat", "bok",
        "ibge", "bcb", "tuik", "tcmb",
        "eurostat", "destatis", "zew", "ifo", "gfk", "hcob",
        "ec-bcs", "insee", "ine", "istat",
        "fed-values", "nbs-values",
        "stat-bureau-jp-values", "boj-values", "boj-tankan-values",
        "mof-jp-values", "cao-values", "cao-gdp-values", "meti-values",
        "fed-speeches", "ecb-speeches", "boe-speeches", "boj-speeches",
    )


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
    assert summary.ok_count == len(ALL_VALUE_SIDE_CONNECTORS)
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

    overrides = {name: _ok(name) for name in ALL_VALUE_SIDE_CONNECTORS}
    overrides["bls"] = _no_key
    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        _connector_overrides=overrides,
    )
    bls_result = next(r for r in summary.results if r.connector == "bls")
    assert bls_result.ok is False
    assert "BLS_API_KEY" in (bls_result.error or "")
    assert summary.ok_count == len(ALL_VALUE_SIDE_CONNECTORS) - 1
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
