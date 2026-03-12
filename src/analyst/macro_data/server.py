from __future__ import annotations

import argparse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
from pathlib import Path
from typing import Any

from analyst.env import get_env_value

from .client import encode_json
from .service import LocalMacroDataService

logger = logging.getLogger(__name__)


class MacroDataRequestHandler(BaseHTTPRequestHandler):
    server_version = "AnalystMacroData/0.1"

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._write_json(HTTPStatus.OK, {"status": "ok"})
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        if not self.path.startswith("/v1/ops/"):
            self._write_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        token = self.server.api_token  # type: ignore[attr-defined]
        if token:
            auth_header = self.headers.get("Authorization", "")
            if auth_header != f"Bearer {token}":
                self._write_json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        raw_body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            self._write_json(HTTPStatus.BAD_REQUEST, {"error": "invalid json"})
            return
        operation = self.path.rsplit("/", 1)[-1]
        arguments = payload.get("arguments") if isinstance(payload, dict) else {}
        if not isinstance(arguments, dict):
            self._write_json(HTTPStatus.BAD_REQUEST, {"error": "arguments must be an object"})
            return
        service = self.server.service  # type: ignore[attr-defined]
        try:
            result = service.invoke(operation, arguments)
        except KeyError as exc:
            self._write_json(HTTPStatus.NOT_FOUND, {"error": str(exc)})
            return
        except Exception as exc:
            logger.exception("macro-data operation failed: %s", operation)
            self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
            return
        self._write_json(HTTPStatus.OK, result)

    def log_message(self, format: str, *args: Any) -> None:
        logger.info("%s - %s", self.address_string(), format % args)

    def _write_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        encoded = encode_json(payload)
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def serve(
    *,
    host: str,
    port: int,
    service: LocalMacroDataService,
    api_token: str = "",
) -> None:
    httpd = ThreadingHTTPServer((host, port), MacroDataRequestHandler)
    httpd.service = service  # type: ignore[attr-defined]
    httpd.api_token = api_token  # type: ignore[attr-defined]
    logger.info("macro-data API listening on http://%s:%s", host, port)
    httpd.serve_forever()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve macro-data API")
    parser.add_argument("--host", default=get_env_value("ANALYST_MACRO_DATA_HOST", default="127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(get_env_value("ANALYST_MACRO_DATA_PORT", default="8765") or "8765"))
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--api-token", default=get_env_value("ANALYST_MACRO_DATA_API_TOKEN", default=""))
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    from analyst.macro_data.factory import build_local_macro_data_service

    service = build_local_macro_data_service(db_path=Path(args.db_path) if args.db_path else None)
    serve(
        host=args.host,
        port=args.port,
        service=service,
        api_token=args.api_token,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
