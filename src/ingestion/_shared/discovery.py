"""Discovery helpers — ad-hoc web search + generic URL fetch.

Ported from information-layer ``news/src/discovery/``. Used during
ingestion to chase sources a newsletter or report references.

Scope trimmed vs. the upstream module: the Playwright + Wayback paywall
fallback (``paywall_fetcher``) is intentionally NOT ported here because
it pulls Playwright as a runtime dependency. When a 4xx is returned and
no caller-supplied ``paywall_fetcher`` is given, ``fetch_url`` returns
an error result and lets the caller decide what to do.

NOT agent-callable. Intended for internal ingestion code paths.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

import httpx

from env import get_env_value

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
_BRAVE_API_KEY_ENV = "BRAVE_API_KEY"

_URL_RE = re.compile(
    r"https?://[^\s<>()\"']+[^\s<>()\"'.,;:!?]",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


@dataclass
class FetchResult:
    url: str
    status: int
    text: str
    via_paywall: bool = False
    error: str | None = None


def fetch_url(
    url: str,
    *,
    timeout: float = 20.0,
    paywall_fetcher=None,
    max_chars: int = 120_000,
) -> FetchResult:
    """GET ``url`` and return the decoded body text.

    On a 4xx response and when ``paywall_fetcher`` is supplied, retry via
    the paywall path (caller provides — this module does not bundle
    Playwright).
    """
    try:
        resp = httpx.get(
            url,
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": _USER_AGENT},
        )
    except httpx.HTTPError as exc:
        return FetchResult(url=url, status=0, text="", error=str(exc))

    if resp.status_code < 400:
        return FetchResult(
            url=url, status=resp.status_code, text=resp.text[:max_chars],
        )

    if paywall_fetcher is None:
        return FetchResult(
            url=url,
            status=resp.status_code,
            text="",
            error=f"HTTP {resp.status_code}",
        )

    try:
        article = paywall_fetcher.fetch_article(url, "")
    except Exception as exc:  # defensive — paywall path is best-effort
        return FetchResult(
            url=url, status=resp.status_code, text="", error=f"paywall: {exc}",
        )

    return FetchResult(
        url=url,
        status=resp.status_code,
        text=(getattr(article, "content", "") or "")[:max_chars],
        via_paywall=True,
    )


# ---------------------------------------------------------------------------
# Search (Brave)
# ---------------------------------------------------------------------------


@dataclass
class SearchResult:
    title: str
    url: str
    snippet: str


def search(
    query: str, *, limit: int = 5, timeout: float = 10.0
) -> list[SearchResult]:
    """Return up to ``limit`` Brave Search results for ``query``.

    Returns ``[]`` when ``BRAVE_API_KEY`` is unset, the query is blank,
    or the API errors out — callers can invoke unconditionally.
    """
    if not query or not query.strip():
        return []
    # Read via the shared env helper so .env files are picked up alongside
    # process-environment exports — same path as FRED_API_KEY / LLM_API_KEY.
    api_key = get_env_value(_BRAVE_API_KEY_ENV)
    if not api_key:
        logger.debug("discovery.search: %s not set; skipping", _BRAVE_API_KEY_ENV)
        return []

    try:
        resp = httpx.get(
            _BRAVE_ENDPOINT,
            params={"q": query, "count": limit},
            headers={
                "Accept": "application/json",
                "X-Subscription-Token": api_key,
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        # ValueError covers resp.json() on non-JSON 200s (CDN, rate-limit HTML).
        logger.warning("discovery.search failed for %r: %s", query, exc)
        return []

    web = payload.get("web") or {}
    results = web.get("results") or []
    return [
        SearchResult(
            title=r.get("title") or "",
            url=r.get("url") or "",
            snippet=r.get("description") or "",
        )
        for r in results[:limit]
        if r.get("url")
    ]


# ---------------------------------------------------------------------------
# URL extraction
# ---------------------------------------------------------------------------


def extract_urls(markdown: str, *, limit: int | None = None) -> list[str]:
    """Return unique http(s) URLs from ``markdown``, first-seen order.

    Pure — no network — so parsers can call it unconditionally while
    building up a list of sources to chase with :func:`fetch_url`.
    """
    if not markdown:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for match in _URL_RE.finditer(markdown):
        url = match.group(0)
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
        if limit is not None and len(out) >= limit:
            break
    return out


__all__ = [
    "FetchResult",
    "SearchResult",
    "extract_urls",
    "fetch_url",
    "search",
]
