"""NBS English press-release listing → per-release URL resolution.

Issue #49 — value side. The English NBS press-release index at
``https://www.stats.gov.cn/english/PressRelease/`` lists every
recent release as a row carrying:

- a release-date span (``"2026-04-13"``),
- a title span (``"Consumer Price Index in March 2026"``),
- an anchor pointing at the article (``"./202604/t20260413_1963288.html"``).

This module fetches the listing and exposes
:func:`resolve_release_url`, which finds the article URL whose
``(release_date, title)`` matches a pending schedule row.

Pagination is intentionally out of scope: the value-side fetcher
only fills rows in the burst window (``[now − 1h, now + 30min]``)
and the daily catch-up sweep, both well within the ~25–30 entries
the listing's first page carries. Backfill of older releases
remains a manual op.

The fetch path mirrors :mod:`nbs_api.scraper` (browser headers,
forced UTF-8) — stats.gov.cn is the highest-risk upstream on this
issue.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


NBS_PRESS_RELEASE_INDEX_URL = "https://www.stats.gov.cn/english/PressRelease/"

# Browser headers — same bundle as ``nbs_api.scraper``. NBS frontends
# 403 the bare ``python-requests`` UA; ``Accept-Encoding`` advertises
# only formats urllib3 decodes natively.
_NBS_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.1 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


# Date span text shape: "2026-04-13" — ISO-ish, with optional padding.
_LISTING_DATE_RE = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})")


class NBSPressListingParseError(ValueError):
    """Raised when the press-release listing DOM deviates from the expected shape.

    Loud-fail is deliberate — a silent empty parse on an HTML-fragile
    upstream would let a release-day value miss go undetected until a
    parity-tripwire alert fires.
    """


@dataclass(frozen=True)
class NBSPressListingEntry:
    """One listing row, resolved to ``(release_date, title, url)``."""

    release_date: date
    title: str
    url: str


def _normalize_title(text: str) -> str:
    """Lowercase + collapse whitespace for fragment matching."""
    return re.sub(r"\s+", " ", text).strip().lower()


def parse_press_listing_html(
    html: str | bytes,
    *,
    base_url: str = NBS_PRESS_RELEASE_INDEX_URL,
) -> list[NBSPressListingEntry]:
    """Parse the NBS press-release listing into structured entries.

    The listing renders as alternating ``<span class="cont_tit">`` /
    ``<span class="cont_tit02">`` rows in the right-hand column, each
    containing one anchor + one date. The exact class names have
    drifted across NBS redesigns, so we scan every anchor that:

    - lives inside the press-release content area,
    - carries a non-empty ``href``,
    - has a sibling text node that parses as a YYYY-MM-DD date.

    Entries whose date or title can't be parsed are skipped (logged
    at warning) rather than failing the whole sweep — the listing
    sometimes carries a "Notice" / cross-language link that doesn't
    follow the release-row shape.
    """
    text = (
        html.decode("utf-8", errors="replace")
        if isinstance(html, bytes) else html
    )
    soup = BeautifulSoup(text, "html.parser")

    entries: list[NBSPressListingEntry] = []
    seen_urls: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        # Skip self-links, anchors, mailto, etc.
        if href.startswith("#") or href.startswith("javascript:"):
            continue
        # Press-release articles live under ``./YYYYMM/tYYYYMMDD_<id>.html``.
        # Filter out unrelated nav links (the index also carries About /
        # Statistical Database / Privacy etc.).
        if not re.search(r"/?(?:\d{6}/)?t\d{8}_\d+\.html$", href):
            continue

        title = anchor.get_text(" ", strip=True)
        if not title:
            continue

        release_date = _nearest_date_for_anchor(anchor)
        if release_date is None:
            continue

        absolute_url = urljoin(base_url, href)
        if absolute_url in seen_urls:
            continue
        seen_urls.add(absolute_url)

        entries.append(
            NBSPressListingEntry(
                release_date=release_date,
                title=title,
                url=absolute_url,
            )
        )

    if not entries:
        raise NBSPressListingParseError(
            "NBS press-release listing parsed zero entries — DOM drift or "
            "interstitial response"
        )
    return entries


def _nearest_date_for_anchor(anchor) -> date | None:
    """Find the YYYY-MM-DD release date adjacent to ``anchor``.

    The listing's row shape varies across NBS redesigns but always
    pairs the anchor with a date in the same parent ``<li>`` / ``<tr>``
    / ``<span>``. We walk up to two ancestors, scan their text for the
    first ``YYYY-MM-DD`` token, and use that.
    """
    node = anchor
    for _ in range(3):
        parent = node.parent
        if parent is None:
            break
        text = parent.get_text(" ", strip=True)
        match = _LISTING_DATE_RE.search(text)
        if match:
            try:
                year = int(match.group(1))
                month = int(match.group(2))
                day = int(match.group(3))
                return date(year, month, day)
            except ValueError:
                pass
        node = parent
    return None


def resolve_release_url(
    entries: Iterable[NBSPressListingEntry],
    *,
    release_date: date,
    listing_title_fragment: str,
) -> NBSPressListingEntry | None:
    """Pick the listing entry matching ``release_date`` + title fragment.

    Returns ``None`` when no entry is found — caller treats that as a
    "not yet on the listing" miss (the connector will retry on the
    next sweep). Raises :class:`NBSPressListingParseError` only if the
    fragment matches multiple candidates on the same date — that
    indicates a registry collision, not a transient miss.
    """
    needle = listing_title_fragment.strip().lower()
    if not needle:
        raise ValueError("listing_title_fragment is empty")
    matches = [
        entry for entry in entries
        if entry.release_date == release_date
        and needle in _normalize_title(entry.title)
    ]
    if not matches:
        return None
    if len(matches) > 1:
        raise NBSPressListingParseError(
            f"NBS press listing has {len(matches)} entries matching "
            f"{release_date.isoformat()} / {listing_title_fragment!r}: "
            f"{[e.title for e in matches]}"
        )
    return matches[0]


def fetch_press_listing_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the NBS press-release listing page (UTF-8 forced)."""
    owned = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            NBS_PRESS_RELEASE_INDEX_URL,
            headers=_NBS_BROWSER_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        # NBS omits ``charset`` from ``Content-Type`` so requests falls
        # back to ISO-8859-1; force UTF-8 to keep CJK characters intact.
        response.encoding = "utf-8"
        return response.text
    finally:
        if owned:
            s.close()


def fetch_press_release_html(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET one NBS press-release article (UTF-8 forced)."""
    owned = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            url, headers=_NBS_BROWSER_HEADERS, timeout=timeout,
        )
        response.raise_for_status()
        response.encoding = "utf-8"
        return response.text
    finally:
        if owned:
            s.close()


__all__ = [
    "NBS_PRESS_RELEASE_INDEX_URL",
    "NBSPressListingEntry",
    "NBSPressListingParseError",
    "fetch_press_listing_html",
    "fetch_press_release_html",
    "parse_press_listing_html",
    "resolve_release_url",
]
