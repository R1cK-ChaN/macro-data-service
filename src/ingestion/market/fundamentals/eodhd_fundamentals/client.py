"""EODHD ``/api/fundamentals/{TICKER}.{EX}`` HTTP client.

Transport-only — auth (``api_token``), ``fmt=json``, 429 retry. No
parsing or persistence. Returns the parsed JSON payload (a single dict
keyed by section: ``General``, ``Highlights``, ``Valuation``,
``SharesStats``, ``Financials``, ``Earnings``, etc.).

Per issue #68 scope: deliberately *not* shared with the existing
``ingestion.calendar.eodhd_api.EODHDAPIClient`` or
``ingestion.market.scrapers._eodhd.EODHDClient`` — that's the
third-caller threshold met after this lands; the consolidation pass
into a shared transport is filed as a follow-up subtraction commit
(rule 3 in CLAUDE.md: don't abstract until a third concrete case
exists).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx

from env import get_env_value

logger = logging.getLogger(__name__)

EODHD_BASE_URL = "https://eodhd.com"
DEFAULT_TIMEOUT = 60.0  # fundamentals payloads are large; calendar default of 30s is too tight
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE_SECONDS = 2.0


class EODHDFundamentalsAuthMissing(RuntimeError):
    """Raised when EODHD_API_KEY is absent. Lazy — only on first call so
    dry-run paths can construct the client without credentials."""


class EODHDFundamentalsThrottled(RuntimeError):
    """Raised after exhausting 429 backoff retries."""


class EODHDFundamentalsNotFound(RuntimeError):
    """Raised on HTTP 404 — ticker unknown to EODHD or fundamentals
    not available on the active plan tier."""


@dataclass
class FundamentalsCallResult:
    """Single-call outcome.

    ``payload`` is the full decoded JSON (a dict for the fundamentals
    endpoint). ``payload_text`` is the verbatim response body — kept
    so the projector can hash and persist the canonical bytes that
    came over the wire instead of a re-serialised approximation.
    """

    ticker: str
    payload: dict[str, Any]
    payload_text: str
    elapsed_ms: float


class EODHDFundamentalsClient:
    """Thin wrapper for the EODHD fundamentals endpoint.

    Construction never raises on missing key — auth resolution is lazy
    so dry-run code paths can instantiate a client without credentials.
    A real request triggers :class:`EODHDFundamentalsAuthMissing` if the
    key is still unresolved at call time.
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
            raise EODHDFundamentalsAuthMissing(
                "EODHD_API_KEY not set — cannot call EODHD fundamentals. "
                "Add EODHD_API_KEY=<token> to .env or pass api_key=..."
            )
        return value

    def _build_params(self, extra: dict[str, Any] | None) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        if extra:
            merged.update({k: v for k, v in extra.items() if v is not None})
        merged["api_token"] = self._resolve_api_key()
        merged.setdefault("fmt", "json")
        return merged

    def get_fundamentals(
        self,
        ticker: str,
        *,
        sections: list[str] | None = None,
        max_retries: int | None = None,
    ) -> FundamentalsCallResult:
        """Fetch ``/api/fundamentals/{ticker}`` with 429 backoff retries.

        ``sections`` translates to EODHD's ``filter`` query param —
        passing ``["General", "Highlights"]`` reduces the response to
        just those blocks. ``None`` returns the full payload.

        ``max_retries`` overrides the constructor's retry count for
        this single call. ``FundamentalsFetcher`` clamps it down to the
        remaining per-run budget so a 429-storm on one ticker can't
        blow past ``max_requests``.
        """
        cleaned = (ticker or "").strip()
        if not cleaned:
            raise ValueError("ticker is required")
        path = f"/api/fundamentals/{cleaned}"
        params: dict[str, Any] = {}
        if sections:
            params["filter"] = ",".join(sections)
        resolved_params = self._build_params(params)

        retries = self._max_retries if max_retries is None else max(0, max_retries)
        backoff_schedule = [
            DEFAULT_BACKOFF_BASE_SECONDS * (2**i) for i in range(retries)
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
                last_exc = EODHDFundamentalsThrottled(
                    f"HTTP 429 on {path}; "
                    f"attempt {attempt + 1}/{retries + 1}"
                )
                if attempt < retries:
                    logger.warning(
                        "EODHD 429 throttle on %s, backing off", path
                    )
                    continue
                raise last_exc

            if response.status_code == 404:
                raise EODHDFundamentalsNotFound(
                    f"EODHD fundamentals 404 for {cleaned}"
                )

            response.raise_for_status()
            text = response.text
            try:
                payload = response.json()
            except ValueError as exc:
                raise RuntimeError(
                    f"EODHD returned non-JSON for {cleaned}: {exc}"
                ) from exc
            if not isinstance(payload, dict):
                raise RuntimeError(
                    f"EODHD fundamentals payload for {cleaned} is "
                    f"{type(payload).__name__}, expected object"
                )
            return FundamentalsCallResult(
                ticker=cleaned,
                payload=payload,
                payload_text=text,
                elapsed_ms=elapsed_ms,
            )

        raise last_exc or EODHDFundamentalsThrottled(
            f"exhausted retries on {path}"
        )

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # pragma: no cover
            pass

    def __enter__(self) -> "EODHDFundamentalsClient":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()
