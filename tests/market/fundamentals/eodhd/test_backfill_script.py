"""Smoke tests for ``scripts/backfill_fundamentals.py`` (issue #68 S3).

Network-free — exercises the universe-resolution helper, dry-run
flow through the service op, and ad-hoc ``--tickers`` override path.
A live-execute test would require a real EODHD key; that's covered
by ``scripts/validate_fundamentals_acquisition.py`` (operator-driven,
not unit).
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

# Load the script as a module — scripts/ is not a package.
spec = importlib.util.spec_from_file_location(
    "backfill_fundamentals",
    REPO_ROOT / "scripts" / "backfill_fundamentals.py",
)
assert spec and spec.loader
backfill_fundamentals = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backfill_fundamentals)


def test_universe_resolver_returns_17_default_tickers() -> None:
    tickers = backfill_fundamentals._resolve_universe_tickers(
        asset_classes=backfill_fundamentals.DEFAULT_ASSET_CLASSES,
    )
    assert len(tickers) == 17
    # US entries carry the ``.US`` suffix; listed global entries keep theirs.
    assert "SPY.US" in tickers
    assert "VWRL.LSE" in tickers


def test_universe_resolver_respects_asset_class_filter() -> None:
    tickers = backfill_fundamentals._resolve_universe_tickers(
        asset_classes=frozenset({"equity"}),
    )
    assert tickers
    assert {"SAP.XETRA", "0700.HK"} <= set(tickers)


def test_universe_resolver_dedupes() -> None:
    tickers = backfill_fundamentals._resolve_universe_tickers(
        asset_classes=backfill_fundamentals.DEFAULT_ASSET_CLASSES,
    )
    assert len(tickers) == len(set(tickers))


def test_main_dry_run_returns_zero_and_writes_log(tmp_path: Path, capsys) -> None:
    rc = backfill_fundamentals.main([
        "--db-path", str(tmp_path / "engine.db"),
        "--log-path", str(tmp_path / "logs" / "backfill.log"),
    ])
    assert rc == 0
    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["stopped_reason"] == "dry_run"
    assert summary["requests_spent"] == 0
    log_lines = (tmp_path / "logs" / "backfill.log").read_text().splitlines()
    assert len(log_lines) == 1
    log_payload = json.loads(log_lines[0])
    assert log_payload["operation"] == "fundamentals_fetch"
    assert log_payload["dry_run"] is True
    assert log_payload["tickers_planned"] == 17


def test_main_tickers_override_skips_universe_walk(
    tmp_path: Path, capsys
) -> None:
    rc = backfill_fundamentals.main([
        "--db-path", str(tmp_path / "engine.db"),
        "--log-path", str(tmp_path / "logs" / "backfill.log"),
        "--tickers", "AAPL.US", "MSFT.US",
    ])
    assert rc == 0
    log_lines = (tmp_path / "logs" / "backfill.log").read_text().splitlines()
    log_payload = json.loads(log_lines[0])
    assert log_payload["tickers"] == ["AAPL.US", "MSFT.US"]
    assert log_payload["tickers_planned"] == 2


def test_main_execute_with_all_parse_failures_returns_one(
    tmp_path: Path, monkeypatch
) -> None:
    """HTTP 200 + every ticker raising in the parser must fail the run.

    Without this guard ``tickers_fetched`` stays incremented (the
    fetcher bumps it before the parse step) so a green sweep would
    report success while writing zero rows (Codex review #68 S3 R2 P2).
    """
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    import httpx
    import respx
    respx_router = respx.MockRouter(assert_all_called=False)

    def _fake_parse(*_args, **_kwargs):
        raise RuntimeError("synthetic parse blow-up")

    monkeypatch.setattr(
        "ingestion.market.fundamentals.eodhd_fundamentals.fetcher.parse_payload_records",
        _fake_parse,
    )

    with respx_router:
        respx_router.get(
            url__startswith="https://eodhd.com/api/fundamentals/AAPL.US",
        ).mock(return_value=httpx.Response(200, json={"General": {"Name": "X"}}))
        rc = backfill_fundamentals.main([
            "--db-path", str(tmp_path / "engine.db"),
            "--log-path", str(tmp_path / "logs" / "backfill.log"),
            "--tickers", "AAPL.US",
            "--execute",
        ])
    assert rc == 1
    log_payload = json.loads(
        (tmp_path / "logs" / "backfill.log").read_text().splitlines()[0],
    )
    assert log_payload["status"] == "error"
    assert log_payload["tickers_fetched"] == 1
    assert log_payload["parse_errors"] == 1


def test_main_execute_with_all_tickers_failing_returns_one(
    tmp_path: Path, monkeypatch
) -> None:
    """Execute mode with zero successful fetches must fail the run.

    Without this guard, a missing API key returns ``error`` per-ticker
    but a top-level ``status: ok``; the systemd timer would silently
    report success while writing nothing (Codex review #68 S3 R1 P2).
    """
    monkeypatch.delenv("EODHD_API_KEY", raising=False)
    from env import clear_env_cache
    monkeypatch.setattr("env.DEFAULT_ENV_FILES", ())
    clear_env_cache()
    rc = backfill_fundamentals.main([
        "--db-path", str(tmp_path / "engine.db"),
        "--log-path", str(tmp_path / "logs" / "backfill.log"),
        "--tickers", "AAPL.US",
        "--execute",
    ])
    assert rc == 1
    log_payload = json.loads(
        (tmp_path / "logs" / "backfill.log").read_text().splitlines()[0],
    )
    assert log_payload["status"] == "error"
    assert log_payload["tickers_fetched"] == 0


def test_main_empty_tickers_returns_argument_error(tmp_path: Path) -> None:
    rc = backfill_fundamentals.main([
        "--db-path", str(tmp_path / "engine.db"),
        # No asset class in either universe matches "nonexistent".
        "--asset-classes", "nonexistent",
    ])
    assert rc == 2
