"""DOL Employment & Training Administration newsroom listing.

The ``/newsroom/releases/eta?lang=en`` page renders one card per
ETA release. For UI Claims releases, each card carries:

- An anchor pointing at the release detail page (``/newsroom/releases/eta/etaYYYYMMDD``).
- The release title — ``"Unemployment Insurance Weekly Claims Report"``.
- A release date adjacent to the title.

The detail URL ID embeds the release date (``etaYYYYMMDD``); the
connector decodes that rather than parsing a separate date span on
the listing card. Per-release fetches resolve to a PDF directly
(the detail URL serves ``application/pdf`` via DOL's CDN).

DOL.gov sits behind Akamai bot protection; live requests need the
browser-shaped headers below plus a cookie jar (the ``sec_cpt``
challenge is set on the first response and must be replayed).
The :func:`requests.Session` returned by :func:`session_for_dol`
carries the cookie jar; tests inject a session-equivalent fetcher
seam to avoid live HTTP in CI.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


DOL_NEWSROOM_BASE = "https://www.dol.gov"
DOL_ETA_LISTING_URL = f"{DOL_NEWSROOM_BASE}/newsroom/releases/eta?lang=en"


# Browser headers — matches a recent Mac Safari fingerprint. DOL
# returns a 403 challenge to the bare ``python-requests`` UA;
# advertising HTTP/2 hints + ``Accept-Encoding: gzip,deflate,br``
# (with ``--compressed`` decoding) gets through.
_DOL_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.1 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "navigate",
    "Connection": "keep-alive",
}


_RELEASE_HREF_RE = re.compile(
    r"/newsroom/releases/eta/eta(\d{4})(\d{2})(\d{2})(?:-(\d+))?",
    re.IGNORECASE,
)


class DOLListingParseError(ValueError):
    """Raised when the DOL ETA listing parses to zero UI Claims rows."""


@dataclass(frozen=True)
class DOLReleaseEntry:
    """One UI Weekly Claims release resolved from the ETA listing."""

    release_date: date
    title: str
    detail_url: str          # absolute URL to the press-release page (PDF)


def parse_listing_html(html: str | bytes) -> list[DOLReleaseEntry]:
    """Walk the ETA listing for ``Unemployment Insurance Weekly Claims Report``.

    The card structure varies across DOL redesigns; we anchor on the
    title text and pull the nearest matching anchor + URL-embedded
    date. Cards with the same anchor for two dates (Spanish-language
    sibling, archival reposts) deduplicate by URL.
    """
    text = (
        html.decode("utf-8", errors="replace")
        if isinstance(html, bytes) else html
    )
    soup = BeautifulSoup(text, "html.parser")

    entries: list[DOLReleaseEntry] = []
    seen_urls: set[str] = set()
    for span in soup.find_all("span"):
        title = span.get_text(" ", strip=True)
        if title != "Unemployment Insurance Weekly Claims Report":
            continue
        # The card wraps title + link; walk up to the enclosing anchor.
        anchor = span.find_parent("a")
        if anchor is None:
            continue
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        match = _RELEASE_HREF_RE.search(href)
        if not match:
            continue
        try:
            release_date = date(
                int(match.group(1)),
                int(match.group(2)),
                int(match.group(3)),
            )
        except ValueError:
            continue
        absolute = urljoin(DOL_NEWSROOM_BASE, href)
        if absolute in seen_urls:
            continue
        seen_urls.add(absolute)
        entries.append(
            DOLReleaseEntry(
                release_date=release_date,
                title=title,
                detail_url=absolute,
            )
        )

    if not entries:
        raise DOLListingParseError(
            "DOL ETA listing parsed zero UI Claims rows — DOM drift "
            "or anti-bot challenge"
        )
    # Newest first (page already orders this way, but pin it).
    entries.sort(key=lambda e: e.release_date, reverse=True)
    return entries


def session_for_dol() -> requests.Session:
    """Return a fresh session preconfigured for DOL's anti-bot stack.

    The session carries the browser headers as defaults so callers
    don't need to repeat them per request. Cookie jar is empty on
    first call; DOL's ``sec_cpt`` challenge cookie is captured on
    the first ``GET`` and replayed on subsequent requests.
    """
    s = requests.Session()
    s.headers.update(_DOL_BROWSER_HEADERS)
    return s


def fetch_listing_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the ETA newsroom listing index page."""
    s = session or session_for_dol()
    response = s.get(DOL_ETA_LISTING_URL, timeout=timeout)
    response.raise_for_status()
    return response.text


def fetch_release_pdf_bytes(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 60.0,
) -> bytes:
    """Download a DOL press-release URL — returns the raw PDF bytes.

    The ``/newsroom/releases/eta/etaYYYYMMDD`` URL serves a PDF
    directly when the request carries the right headers; the
    response Content-Type is ``application/pdf`` despite the
    HTML-style URL.
    """
    s = session or session_for_dol()
    response = s.get(url, timeout=timeout)
    response.raise_for_status()
    return response.content


__all__ = [
    "DOL_ETA_LISTING_URL",
    "DOL_NEWSROOM_BASE",
    "DOLListingParseError",
    "DOLReleaseEntry",
    "fetch_listing_html",
    "fetch_release_pdf_bytes",
    "parse_listing_html",
    "session_for_dol",
]
