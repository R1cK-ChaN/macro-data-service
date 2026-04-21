"""EODHD Calendar API HTTP client.

Transport-only — auth (``api_token``), ``fmt=json``, 429 retry. No parsing
or persistence. Callers pass an endpoint path plus a params dict; get back
the parsed JSON payload (shape depends on the endpoint).

EODHD calendar endpoints return JSON objects (not top-level arrays): the
row list sits under a subtype-specific key (``earnings``, ``ipos``,
``splits``, ``trends``, or the JSON:API ``data`` wrapper for dividends).
The client returns the raw decoded JSON; row-list extraction is the
caller's concern so each parser can enforce its own shape expectations.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from env import get_env_value

logger = logging.getLogger(__name__)

EODHD_BASE_URL = "https://eodhd.com"
DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE_SECONDS = 2.0


class EODHDAuthMissing(RuntimeError):
    """Raised when EODHD_API_KEY is absent. Construction stays cheap so
    dry-run paths work without credentials; only raised on an actual call."""


class EODHDThrottled(RuntimeError):
    """Raised after exhausting 429 backoff retries."""


@dataclass
class EODHDCallResult:
    """Single-call outcome.

    ``payload`` is whatever EODHD returned (dict for the calendar endpoints).
    ``rows`` is the subtype-specific row list pre-extracted by
    :meth:`EODHDAPIClient.get_rows`; raw callers can still reach the full
    payload through ``payload``.
    """

    path: str
    params: dict[str, Any]
    payload: Any
    rows: list[Any] = field(default_factory=list)
    elapsed_ms: float = 0.0


class EODHDAPIClient:
    """Thin wrapper over :mod:`httpx` for the EODHD Calendar endpoints.

    Constructor never raises on missing key — auth resolution is lazy so
    dry-run code paths can instantiate a client without credentials. A
    real request triggers :class:`EODHDAuthMissing` if the key is still
    unresolved at call time.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = EODHD_BASE_URL,
        max_retries: int = DEFAULT_MAX_RETRIES,
        timeout: float = DEFAULT_TIMEOUT,
        client: httpx.Client | None = None,
        sleeper: Any = None,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._max_retries = max_retries
        self._timeout = timeout
        self._client = client or httpx.Client(timeout=timeout)
        self._sleeper = sleeper or time.sleep
        self._requests_made: int = 0

    @property
    def requests_made(self) -> int:
        return self._requests_made

    def _resolve_api_key(self) -> str:
        if self._api_key:
            return self._api_key
        value = get_env_value("EODHD_API_KEY")
        if not value:
            raise EODHDAuthMissing(
                "EODHD_API_KEY not set — cannot call EODHD. "
                "Add EODHD_API_KEY=<token> to .env or pass api_key=..."
            )
        return value

    def _build_params(self, params: dict[str, Any]) -> dict[str, Any]:
        merged = {k: v for k, v in params.items() if v is not None}
        merged["api_token"] = self._resolve_api_key()
        merged.setdefault("fmt", "json")
        return merged

    def get(self, path: str, params: dict[str, Any] | None = None) -> EODHDCallResult:
        """Fetch ``path`` with 429 backoff retries.

        ``rows`` is left empty here; use :meth:`get_rows` when you want
        the per-subtype row list extracted.
        """
        resolved_params = self._build_params(params or {})
        backoff_schedule = [
            DEFAULT_BACKOFF_BASE_SECONDS * (2**i) for i in range(self._max_retries)
        ]
        last_exc: Exception | None = None

        for attempt, _ in enumerate([None] + backoff_schedule):
            if attempt > 0:
                self._sleeper(backoff_schedule[attempt - 1])
            start = time.monotonic()
            try:
                response = self._client.get(
                    f"{self._base_url}{path}", params=resolved_params
                )
            except httpx.HTTPError as exc:  # pragma: no cover — network flake
                last_exc = exc
                continue
            self._requests_made += 1
            elapsed_ms = (time.monotonic() - start) * 1000.0

            if response.status_code == 429:
                last_exc = EODHDThrottled(
                    f"HTTP 429 on {path}; attempt {attempt + 1}/{self._max_retries + 1}"
                )
                if attempt < self._max_retries:
                    logger.warning("EODHD 429 throttle on %s, backing off", path)
                    continue
                raise last_exc

            response.raise_for_status()
            return EODHDCallResult(
                path=path,
                params=dict(resolved_params),
                payload=response.json(),
                elapsed_ms=elapsed_ms,
            )

        raise last_exc or EODHDThrottled(f"exhausted retries on {path}")

    def get_rows(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        rows_key: str,
    ) -> EODHDCallResult:
        """Fetch and extract a known row list from the payload.

        ``rows_key`` is the subtype-specific key (``earnings`` / ``ipos``
        / ``splits`` / ``trends``) or ``data`` for the JSON:API-shaped
        dividends response. A missing or non-list value yields an empty
        ``rows`` — parsers handle empty row lists as a clean no-op.

        Entries that are themselves lists are preserved (``/calendar/trends``
        returns ``[[...]]`` for per-symbol grouping); the caller is
        responsible for one-level flattening before parsing.
        """
        result = self.get(path, params=params)
        rows: list[Any] = []
        payload = result.payload
        if isinstance(payload, dict):
            raw = payload.get(rows_key)
            if isinstance(raw, list):
                rows = [r for r in raw if isinstance(r, (dict, list))]
        result.rows = rows
        return result

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # pragma: no cover
            pass

    def __enter__(self) -> "EODHDAPIClient":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()
