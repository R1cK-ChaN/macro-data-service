"""Public-read allowlist tests for ``POST /v1/ops/<op>`` (issue #104).

Pre-VPS launch the public HTTP surface must only expose read ops; admin /
write ops stay reachable through CLI / SSH / systemd. The allowlist lives
in ``macro_data.server.PUBLIC_READ_OPS`` and gates the dispatcher in
``do_POST`` — anything else returns HTTP 403 without ever reaching the
service.
"""

from __future__ import annotations

import json
import sys
import threading
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from macro_data.server import PUBLIC_READ_OPS, MacroDataRequestHandler


class _RecordingService:
    """Minimal stand-in for ``LocalMacroDataService``.

    Returns a sentinel envelope and records the op name so tests can
    assert that admin ops never reach ``invoke`` while allowed reads do.
    """

    def __init__(self) -> None:
        self.invoked: list[tuple[str, dict[str, Any]]] = []

    def invoke(self, operation: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.invoked.append((operation, arguments))
        return {"ok": True, "operation": operation}


@pytest.fixture()
def live_server():
    service = _RecordingService()
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), MacroDataRequestHandler)
    httpd.service = service  # type: ignore[attr-defined]
    httpd.api_token = ""  # type: ignore[attr-defined]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[0], httpd.server_address[1]
    try:
        yield host, port, service
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def _http_post(host: str, port: int, path: str) -> tuple[int, dict[str, Any]]:
    conn = HTTPConnection(host, port, timeout=5)
    try:
        body = json.dumps({"arguments": {}}).encode("utf-8")
        conn.request(
            "POST", path, body=body,
            headers={"Content-Type": "application/json"},
        )
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8")
    finally:
        conn.close()
    parsed = json.loads(raw) if raw else {}
    return resp.status, parsed


# ── allowlist constant ────────────────────────────────────────────────


def test_public_read_ops_matches_issue_104_contract() -> None:
    """The exact set called out in the issue body. Adding or removing
    entries is a contract change — this guards against silent drift."""
    assert PUBLIC_READ_OPS == frozenset({
        "resolve_indicator",
        "resolve_indicator_history",
        "list_items",
        "get_document",
        "get_release_schedule",
        "get_release_status",
    })


# ── allowed read ops ──────────────────────────────────────────────────


@pytest.mark.parametrize("op", sorted(PUBLIC_READ_OPS))
def test_allowed_read_op_reaches_service(live_server, op: str) -> None:
    host, port, service = live_server
    status, payload = _http_post(host, port, f"/v1/ops/{op}")
    assert status == 200, f"{op} should be reachable, got {status} {payload}"
    assert payload == {"ok": True, "operation": op}
    assert service.invoked == [(op, {})]


# ── blocked admin / write ops ─────────────────────────────────────────


@pytest.mark.parametrize("op", [
    "refresh_source",
    "refresh_all_sources",
    "run_schedule",
    "refresh_news",
    "refresh_calendar",
    "fundamentals_fetch",
    "sync_catalog_latest",
    "sync_catalog_discovery",
    "validate_concept",
    "validate_all_concepts",
    "calendar_econ_backfill",
    "fetch_country_indicators",
    "fetch_reference_rates",
    "fetch_rate_expectations",
    "fetch_live_news",
    "fetch_live_markets",
    "fetch_article",
    "backfill_document_indexes",
])
def test_blocked_admin_op_returns_403(live_server, op: str) -> None:
    """Admin / write ops the issue calls out must return 403 and must
    never reach the service. The list mixes representative writes
    (``refresh_*``, ``sync_catalog_*``, ``backfill_*``) with fetchers
    that hit external APIs and would be expensive / destructive on a
    public surface (``fetch_live_*``, ``fundamentals_fetch``)."""
    host, port, service = live_server
    status, payload = _http_post(host, port, f"/v1/ops/{op}")
    assert status == 403, f"{op} must be 403, got {status} {payload}"
    assert payload == {"error": "operation not permitted on public API"}
    assert service.invoked == [], (
        f"blocked op {op} reached the service: {service.invoked}"
    )


def test_unknown_op_returns_403_not_404(live_server) -> None:
    """Unknown ops must look identical to disallowed ops on the public
    surface — the HTTP layer does not leak which ops exist on the
    underlying service. Pre-allowlist this returned 404 (KeyError from
    ``invoke``); the allowlist now intercepts it as 403."""
    host, port, service = live_server
    status, payload = _http_post(host, port, "/v1/ops/totally_made_up_op")
    assert status == 403
    assert payload == {"error": "operation not permitted on public API"}
    assert service.invoked == []


# ── auth interaction ──────────────────────────────────────────────────


def test_unauthorized_blocks_before_allowlist_check(tmp_path: Path) -> None:
    """When an API token is configured, unauthenticated callers see 401
    regardless of whether the op is on the allowlist — auth is the
    outer gate, allowlist is the inner gate."""
    service = _RecordingService()
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), MacroDataRequestHandler)
    httpd.service = service  # type: ignore[attr-defined]
    httpd.api_token = "secret"  # type: ignore[attr-defined]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[0], httpd.server_address[1]
    try:
        # Allowed op without token → 401, not 200 and not 403.
        status, payload = _http_post(host, port, "/v1/ops/list_items")
        assert status == 401
        assert payload == {"error": "unauthorized"}
        # Blocked op without token → also 401, the auth check comes first.
        status, payload = _http_post(host, port, "/v1/ops/refresh_source")
        assert status == 401
        assert service.invoked == []
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def test_authorized_admin_op_still_returns_403(tmp_path: Path) -> None:
    """A valid bearer token does not grant access to admin ops on the
    public surface — the allowlist is operation-scoped, not auth-scoped."""
    service = _RecordingService()
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), MacroDataRequestHandler)
    httpd.service = service  # type: ignore[attr-defined]
    httpd.api_token = "secret"  # type: ignore[attr-defined]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[0], httpd.server_address[1]
    try:
        conn = HTTPConnection(host, port, timeout=5)
        try:
            conn.request(
                "POST", "/v1/ops/refresh_all_sources",
                body=b'{"arguments": {}}',
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer secret",
                },
            )
            resp = conn.getresponse()
            raw = resp.read().decode("utf-8")
        finally:
            conn.close()
        assert resp.status == 403
        assert json.loads(raw) == {"error": "operation not permitted on public API"}
        assert service.invoked == []
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)
