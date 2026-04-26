"""Tests for the systemd-driven calendar cron entry-points (issue #31).

Covers argument parsing, exit codes, structured log shape, and op-args
forwarding for both ``scripts/calendar_refresh_schedules.py`` and
``scripts/calendar_sweep_values.py``. The underlying service op is
faked so tests stay offline and don't touch the real engine DB.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = REPO_ROOT / "scripts"


def _load_script(name: str):
    """Import a top-level script module by absolute path.

    The scripts insert ``src`` on ``sys.path`` at import time, so we
    let them do the same (idempotent across repeated test invocations).
    """
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def refresh_module():
    return _load_script("calendar_refresh_schedules")


@pytest.fixture()
def sweep_module():
    return _load_script("calendar_sweep_values")


class _FakeService:
    """Captures the op + arguments and returns a stubbed result."""

    def __init__(self, *, result: dict[str, Any] | None = None,
                 raise_exc: Exception | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._result = result or {
            "ok_count": 5,
            "failed_count": 1,
            "unknown_connectors": [],
            "wall_seconds": 12.5,
            "results": [
                {"connector": "bls", "ok": True, "error": None},
                {"connector": "bea", "ok": False, "error": "boom"},
            ],
        }
        self._raise_exc = raise_exc

    def invoke(self, operation: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((operation, arguments))
        if self._raise_exc is not None:
            raise self._raise_exc
        return self._result


def _read_log(log_path: Path) -> list[dict]:
    return [json.loads(line) for line in log_path.read_text().splitlines() if line]


# ── refresh entry-point ─────────────────────────────────────────────


def test_refresh_default_run_logs_summary(refresh_module, tmp_path,
                                          monkeypatch) -> None:
    fake = _FakeService()
    monkeypatch.setattr(refresh_module, "_build_service", lambda _db: fake)
    log_path = tmp_path / "logs" / "calendar_refresh_schedules.log"

    rc = refresh_module.main([
        "--db-path", str(tmp_path / "engine.db"),
        "--log-path", str(log_path),
    ])

    assert rc == 0
    op, args = fake.calls[0]
    assert op == "calendar_econ_refresh_schedules"
    assert args == {"dry_run": False}

    [entry] = _read_log(log_path)
    assert entry["operation"] == "calendar_econ_refresh_schedules"
    assert entry["status"] == "ok"
    assert entry["dry_run"] is False
    assert entry["ok_count"] == 5
    assert entry["failed_count"] == 1
    assert entry["failed_connectors"] == [
        {"connector": "bea", "error": "boom"},
    ]
    assert entry["wall_seconds"] == 12.5


def test_refresh_dry_run_forwards_flag(refresh_module, tmp_path,
                                       monkeypatch) -> None:
    fake = _FakeService()
    monkeypatch.setattr(refresh_module, "_build_service", lambda _db: fake)
    log_path = tmp_path / "logs" / "x.log"

    rc = refresh_module.main([
        "--db-path", str(tmp_path / "engine.db"),
        "--log-path", str(log_path),
        "--dry-run",
    ])

    assert rc == 0
    _, args = fake.calls[0]
    assert args == {"dry_run": True}
    [entry] = _read_log(log_path)
    assert entry["dry_run"] is True


def test_refresh_connector_subset_passed_through(refresh_module, tmp_path,
                                                 monkeypatch) -> None:
    fake = _FakeService()
    monkeypatch.setattr(refresh_module, "_build_service", lambda _db: fake)

    rc = refresh_module.main([
        "--db-path", str(tmp_path / "engine.db"),
        "--log-path", str(tmp_path / "x.log"),
        "--connectors", "bls", "bea",
    ])

    assert rc == 0
    _, args = fake.calls[0]
    assert args == {"dry_run": False, "connectors": ["bls", "bea"]}


def test_refresh_exception_returns_1_and_logs_traceback(
    refresh_module, tmp_path, monkeypatch,
) -> None:
    fake = _FakeService(raise_exc=RuntimeError("kaboom"))
    monkeypatch.setattr(refresh_module, "_build_service", lambda _db: fake)
    log_path = tmp_path / "x.log"

    rc = refresh_module.main([
        "--db-path", str(tmp_path / "engine.db"),
        "--log-path", str(log_path),
    ])

    assert rc == 1
    [entry] = _read_log(log_path)
    assert entry["status"] == "error"
    assert "RuntimeError" in entry["error"]
    assert "Traceback" in entry["traceback"]


# ── sweep entry-point ───────────────────────────────────────────────


def test_sweep_default_run_logs_summary(sweep_module, tmp_path,
                                        monkeypatch) -> None:
    fake = _FakeService()
    monkeypatch.setattr(sweep_module, "_build_service", lambda _db: fake)
    log_path = tmp_path / "logs" / "calendar_sweep_values.log"

    rc = sweep_module.main([
        "--db-path", str(tmp_path / "engine.db"),
        "--log-path", str(log_path),
    ])

    assert rc == 0
    op, args = fake.calls[0]
    assert op == "calendar_econ_sweep_values"
    assert args == {"dry_run": False}

    [entry] = _read_log(log_path)
    assert entry["operation"] == "calendar_econ_sweep_values"
    assert entry["status"] == "ok"
    assert entry["ok_count"] == 5
    assert entry["failed_count"] == 1


def test_sweep_window_args_forwarded(sweep_module, tmp_path,
                                     monkeypatch) -> None:
    fake = _FakeService()
    monkeypatch.setattr(sweep_module, "_build_service", lambda _db: fake)

    rc = sweep_module.main([
        "--db-path", str(tmp_path / "engine.db"),
        "--log-path", str(tmp_path / "x.log"),
        "--start-year", "2024",
        "--end-year", "2026",
        "--start-period", "2025-01",
        "--end-period", "2026-04",
        "--connectors", "ecb", "eurostat",
    ])

    assert rc == 0
    _, args = fake.calls[0]
    assert args == {
        "dry_run": False,
        "connectors": ["ecb", "eurostat"],
        "start_year": 2024,
        "end_year": 2026,
        "start_period": "2025-01",
        "end_period": "2026-04",
    }


def test_sweep_exception_returns_1(sweep_module, tmp_path,
                                   monkeypatch) -> None:
    fake = _FakeService(raise_exc=ValueError("nope"))
    monkeypatch.setattr(sweep_module, "_build_service", lambda _db: fake)
    log_path = tmp_path / "x.log"

    rc = sweep_module.main([
        "--db-path", str(tmp_path / "engine.db"),
        "--log-path", str(log_path),
    ])

    assert rc == 1
    [entry] = _read_log(log_path)
    assert entry["status"] == "error"
    assert "ValueError" in entry["error"]
