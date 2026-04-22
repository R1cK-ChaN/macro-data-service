"""TradingEconomics Calendar API HTTP client.

Transport-only — rate limiting, retries, URL-length guard, auth. No parsing
or persistence. Callers pass a fully-formed path (without query string) and
get back the parsed JSON list.

Basic-plan binding constraints (confirmed by ``analyst/te_endpoint`` survey
on 2026-04-20):

- 1 req/sec; HTTP 409 on breach.
- 1,000 monthly requests.
- 1,000 rows per response; truncation drops the tail by date.
- URL length ~260 chars.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx

from env import get_env_value

logger = logging.getLogger(__name__)

TE_BASE_URL = "https://api.tradingeconomics.com"
URL_MAX_LEN = 260
DEFAULT_TIMEOUT = 30.0
DEFAULT_RATE_LIMIT_SECONDS = 1.0
DEFAULT_MAX_RETRIES = 3


class TEAuthMissing(RuntimeError):
    """Raised when TE_API_KEY is absent. Never blocks construction; only
    raised when a request is actually attempted, so dry-run paths work
    without credentials."""


class TEThrottled(RuntimeError):
    """Raised after exhausting 409 backoff retries."""


class TEURLTooLong(ValueError):
    """Raised when an assembled URL would exceed 260 chars."""


@dataclass
class TECallResult:
    """Single-call outcome. Includes both the parsed rows and the metadata
    the adaptive window logic needs (row count, truncation flag)."""

    path: str
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool  # True when row_count == 1000 → tail likely dropped
    elapsed_ms: float


class TEAPIClient:
    """Thin wrapper over :mod:`httpx` for the TE Calendar endpoints.

    The client does not touch the DB and does not decide what to fetch. It
    just turns a path into a (parsed-JSON + metadata) result, applying the
    transport guarantees.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = TE_BASE_URL,
        rate_limit_seconds: float = DEFAULT_RATE_LIMIT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        timeout: float = DEFAULT_TIMEOUT,
        client: httpx.Client | None = None,
        sleeper: Any = None,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._rate_limit_seconds = rate_limit_seconds
        self._max_retries = max_retries
        self._timeout = timeout
        self._client = client or httpx.Client(timeout=timeout)
        self._sleeper = sleeper or time.sleep
        self._last_request_at: float = 0.0
        self._requests_made: int = 0

    @property
    def requests_made(self) -> int:
        return self._requests_made

    def _resolve_api_key(self) -> str:
        if self._api_key:
            return self._api_key
        value = get_env_value("TE_API_KEY", "TRADINGECONOMICS_API_KEY")
        if not value:
            raise TEAuthMissing(
                "TE_API_KEY not set — cannot call Trading Economics. "
                "Add TE_API_KEY=<client>:<secret> to .env or pass api_key=..."
            )
        return value

    def _build_url(self, path: str) -> str:
        api_key = self._resolve_api_key()
        joined = f"{self._base_url}{path}"
        sep = "&" if "?" in joined else "?"
        full = f"{joined}{sep}c={api_key}&f=json"
        if len(full) > URL_MAX_LEN:
            raise TEURLTooLong(
                f"assembled URL is {len(full)} chars (max {URL_MAX_LEN}): {path}"
            )
        return full

    def _throttle(self) -> None:
        if self._rate_limit_seconds <= 0:
            return
        delta = time.monotonic() - self._last_request_at
        wait = self._rate_limit_seconds - delta
        if wait > 0:
            self._sleeper(wait)

    def get(self, path: str) -> TECallResult:
        """Fetch ``path`` with rate limiting + 409 retries.

        Returns a :class:`TECallResult` with row metadata callers use to
        drive adaptive window shrinking.
        """
        url = self._build_url(path)
        backoff_schedule = [2.0 * (2**i) for i in range(self._max_retries)]
        last_exc: Exception | None = None

        for attempt, _ in enumerate([None] + backoff_schedule):
            if attempt > 0:
                self._sleeper(backoff_schedule[attempt - 1])
            self._throttle()
            start = time.monotonic()
            try:
                response = self._client.get(url)
            except httpx.HTTPError as exc:  # pragma: no cover — network flake
                last_exc = exc
                continue
            finally:
                self._last_request_at = time.monotonic()
            self._requests_made += 1
            elapsed_ms = (time.monotonic() - start) * 1000.0

            if response.status_code == 409:
                last_exc = TEThrottled(
                    f"HTTP 409 on {path}; attempt {attempt + 1}/{self._max_retries + 1}"
                )
                if attempt < self._max_retries:
                    logger.warning("TE 409 throttle, backing off")
                    continue
                raise last_exc

            response.raise_for_status()
            payload: Any = response.json()
            rows: list[dict[str, Any]]
            if isinstance(payload, list):
                rows = [r for r in payload if isinstance(r, dict)]
            else:
                rows = []
            return TECallResult(
                path=path,
                rows=rows,
                row_count=len(rows),
                truncated=len(rows) >= 1000,
                elapsed_ms=elapsed_ms,
            )

        raise last_exc or TEThrottled(f"exhausted retries on {path}")

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # pragma: no cover
            pass

    def __enter__(self) -> "TEAPIClient":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()
