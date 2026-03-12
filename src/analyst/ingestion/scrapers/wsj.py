"""Wall Street Journal scraper – news listings and full article extraction.

Uses ``curl_cffi`` with TLS fingerprint impersonation for HTTP requests.
Authenticated cookies are loaded from ``~/.analyst/wsj_cookies.json``,
exported from a real Chrome session via ``browser_cookie3``.

Cookie setup::

    pip install browser-cookie3
    python -c "
    import browser_cookie3, json
    from pathlib import Path
    cj = browser_cookie3.chrome(domain_name='.wsj.com')
    cookies = [{'name': c.name, 'value': c.value, 'domain': c.domain,
                'path': c.path, 'expires': c.expires or -1,
                'secure': bool(c.secure), 'httpOnly': False, 'sameSite': 'Lax'}
               for c in cj]
    out = Path.home() / '.analyst' / 'wsj_cookies.json'
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(cookies, indent=2))
    print(f'Saved {len(cookies)} cookies')
    "
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup, Tag

from analyst.ingestion.http_transport import create_cf_session

from ._common import ScrapedNewsItem

logger = logging.getLogger(__name__)

BASE_URL = "https://www.wsj.com"
COOKIE_PATH = Path.home() / ".analyst" / "wsj_cookies.json"

WSJ_SECTIONS = {
    "markets": "/finance",
    "economy": "/economy",
    "business": "/business",
    "tech": "/tech",
    "politics": "/politics",
    "opinion": "/opinion",
    "world": "/world",
}

# URL path segments that indicate an article (not a section/index page).
_ARTICLE_PATH_SEGMENTS = ("/finance/", "/economy/", "/business/", "/tech/",
                          "/politics/", "/opinion/", "/world/", "/us-news/",
                          "/arts-culture/", "/real-estate/", "/sports/",
                          "/science/", "/lifestyle/")

# Boilerplate patterns filtered from article body text.
_BOILERPLATE_PATTERNS = (
    "subscribe to",
    "sign in",
    "what to read next",
    "copyright",
    "dow jones",
    "all rights reserved",
    "appeared in the",
    "write to",
    "corrections & amplifications",
    "newsletter",
    "terms of use",
    "privacy notice",
)


# ------------------------------------------------------------------
# Data class for full article content
# ------------------------------------------------------------------

@dataclass
class WSJArticle:
    """Parsed full article from the Wall Street Journal."""

    url: str
    title: str
    content: str  # body as plain text
    authors: list[str] = field(default_factory=list)
    published_at: str = ""
    section: str = ""
    keywords: list[str] = field(default_factory=list)
    image_url: str = ""
    dek: str = ""  # WSJ-specific sub-headline summary
    fetched: bool = True
    error: str | None = None


# ------------------------------------------------------------------
# Cookie / session helpers
# ------------------------------------------------------------------

def _load_cookies_into_session(session: Any) -> None:
    """Load WSJ cookies from disk into a curl_cffi session."""
    if not COOKIE_PATH.exists():
        logger.warning("No WSJ cookie file at %s — requests will be unauthenticated.", COOKIE_PATH)
        return
    try:
        cookies = json.loads(COOKIE_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        logger.warning("Failed to read WSJ cookie file; ignoring.")
        return
    now = time.time()
    for c in cookies:
        if c.get("expires", -1) != -1 and c.get("expires", 0) < now:
            continue
        session.cookies.set(
            c["name"], c["value"],
            domain=c.get("domain", ".wsj.com"),
            path=c.get("path", "/"),
        )


def _make_session() -> Any:
    """Create a curl_cffi session with WSJ cookies loaded."""
    session = create_cf_session(headers={
        "Accept": "text/html,application/xhtml+xml",
    })
    _load_cookies_into_session(session)
    return session


def _is_article_url(url: str) -> bool:
    """Check if a URL looks like a WSJ article (not a section index)."""
    # WSJ article URLs end with a hex slug, e.g. /finance/some-headline-ab12cd34
    # Section pages are just /finance, /finance/investing, etc.
    path = url.split("wsj.com")[-1].split("?")[0] if "wsj.com" in url else url.split("?")[0]
    if not any(seg in path for seg in _ARTICLE_PATH_SEGMENTS):
        return False
    # Article slugs contain a hex suffix (8+ hex chars at end of last path segment)
    last_seg = path.rstrip("/").rsplit("/", 1)[-1]
    # Check for hex suffix pattern (at least 8 hex chars)
    hex_part = last_seg.rsplit("-", 1)[-1] if "-" in last_seg else ""
    if len(hex_part) >= 8 and all(c in "0123456789abcdef" for c in hex_part):
        return True
    return False


# ------------------------------------------------------------------
# News listing client
# ------------------------------------------------------------------

class WSJNewsClient:
    """Scrapes article listings from WSJ section pages using curl_cffi."""

    def __init__(self) -> None:
        self.session = _make_session()

    def __enter__(self) -> WSJNewsClient:
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    def fetch_news(self, *, section: str = "markets") -> list[ScrapedNewsItem]:
        """Fetch article listings from a single WSJ section."""
        path = WSJ_SECTIONS.get(section, f"/{section}")
        url = f"{BASE_URL}{path}"

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        except Exception as exc:
            logger.warning("WSJ page load failed for %s: %s", section, exc)
            return []

        return self._parse_listing_html(response.text, section)

    def fetch_all_news(
        self,
        *,
        sections: list[str] | None = None,
        sleep_between: float = 1.5,
    ) -> list[ScrapedNewsItem]:
        """Fetch article listings from multiple sections with delay.

        *sections* defaults to ``["markets", "economy", "business"]``.
        """
        targets = sections or ["markets", "economy", "business"]
        all_items: list[ScrapedNewsItem] = []
        seen_urls: set[str] = set()

        for idx, section in enumerate(targets):
            try:
                items = self.fetch_news(section=section)
                for item in items:
                    if item.url not in seen_urls:
                        seen_urls.add(item.url)
                        all_items.append(item)
            except Exception as exc:
                logger.warning("WSJ listing fetch failed for %s: %s", section, exc)
            if idx < len(targets) - 1:
                time.sleep(sleep_between)

        return all_items

    # ---- internal --------------------------------------------------

    def _parse_listing_html(self, html: str, section: str) -> list[ScrapedNewsItem]:
        soup = BeautifulSoup(html, "html.parser")
        items: list[ScrapedNewsItem] = []
        seen: set[str] = set()

        # Strategy 1: JSON-LD structured data.
        ld_items = self._try_json_ld(soup, section)
        if ld_items:
            for item in ld_items:
                if item.url not in seen:
                    seen.add(item.url)
                    items.append(item)

        # Strategy 2: DOM — extract article links with headings.
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if not href:
                continue
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            # Strip tracking params for dedup.
            clean_url = full_url.split("?")[0]
            if clean_url in seen:
                continue
            if not _is_article_url(full_url):
                continue

            # Need a title — check if the link contains or is near a heading.
            title = ""
            # Check for heading inside the link.
            for tag in ("h1", "h2", "h3", "h4"):
                heading = link.find(tag)
                if heading:
                    title = heading.get_text(strip=True)
                    break
            if not title:
                title = link.get_text(strip=True)
            if not title or len(title) < 10:
                continue

            # Look at parent for more context.
            parent = link.parent
            description = ""
            image_url = ""
            published_at = ""
            if parent:
                p_el = parent.find("p")
                if p_el and p_el.get_text(strip=True) != title:
                    description = p_el.get_text(strip=True)
                time_el = parent.find("time")
                if time_el:
                    published_at = time_el.get("datetime", "") or time_el.get_text(strip=True)
                img = parent.find("img")
                if img:
                    image_url = img.get("src", "") or img.get("data-src", "")

            seen.add(clean_url)
            items.append(ScrapedNewsItem(
                source="wsj",
                title=title,
                url=clean_url,
                published_at=published_at,
                description=description,
                category=section,
                image_url=image_url,
            ))

        return items

    def _try_json_ld(self, soup: BeautifulSoup, section: str) -> list[ScrapedNewsItem]:
        """Extract articles from JSON-LD structured data."""
        items: list[ScrapedNewsItem] = []
        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(data, list):
                for entry in data:
                    item = self._ld_to_news_item(entry, section)
                    if item:
                        items.append(item)
            elif isinstance(data, dict):
                item = self._ld_to_news_item(data, section)
                if item:
                    items.append(item)
        return items

    def _ld_to_news_item(self, data: dict, section: str) -> ScrapedNewsItem | None:
        if not isinstance(data, dict):
            return None
        ld_type = data.get("@type", "")
        if "Article" not in ld_type and "NewsArticle" not in ld_type:
            return None
        title = data.get("headline", "")
        url = data.get("url", "")
        if not title or not url:
            return None
        return ScrapedNewsItem(
            source="wsj",
            title=title,
            url=url if url.startswith("http") else f"{BASE_URL}{url}",
            published_at=data.get("datePublished", ""),
            description=data.get("description", ""),
            category=data.get("articleSection", section),
            image_url=self._ld_image(data),
        )

    @staticmethod
    def _ld_image(data: dict) -> str:
        images = data.get("image", [])
        if isinstance(images, str):
            return images
        if isinstance(images, list) and images:
            first = images[0]
            if isinstance(first, str):
                return first
            if isinstance(first, dict):
                return first.get("url", "")
        if isinstance(images, dict):
            return images.get("url", "")
        return ""


# ------------------------------------------------------------------
# Full article client
# ------------------------------------------------------------------

class WSJArticleClient:
    """Fetches and parses full WSJ articles with structured metadata.

    Requires authenticated cookies at ``~/.analyst/wsj_cookies.json``.
    """

    def __init__(self) -> None:
        self.session = _make_session()

    def __enter__(self) -> WSJArticleClient:
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    def fetch_article(self, url: str) -> WSJArticle:
        """Fetch and parse a single WSJ article."""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return self._parse_article_html(response.text, url)
        except Exception as exc:
            logger.warning("WSJ article fetch failed for %s: %s", url, exc)
            return WSJArticle(
                url=url, title="", content="",
                fetched=False, error=str(exc),
            )

    def fetch_articles(
        self,
        urls: list[str],
        *,
        sleep_between: float = 1.5,
    ) -> list[WSJArticle]:
        """Fetch multiple articles with rate limiting."""
        articles: list[WSJArticle] = []
        for idx, url in enumerate(urls):
            articles.append(self.fetch_article(url))
            if idx < len(urls) - 1:
                time.sleep(sleep_between)
        return articles

    # ---- internal --------------------------------------------------

    def _parse_article_html(self, html: str, url: str) -> WSJArticle:
        soup = BeautifulSoup(html, "html.parser")

        # --- Tier 1: JSON-LD metadata ---------------------------------
        section = ""
        keywords: list[str] = []
        image_url = ""
        ld_published = ""
        ld_title = ""
        ld_authors: list[str] = []
        ld_description = ""

        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(data, dict) and "Article" in data.get("@type", ""):
                ld_title = data.get("headline", "")
                section = data.get("articleSection", "")
                keywords = data.get("keywords", [])
                if isinstance(keywords, str):
                    keywords = [keywords]
                ld_description = data.get("description", "")
                ld_published = data.get("datePublished", "")
                images = data.get("image", [])
                if isinstance(images, list) and images:
                    first = images[0]
                    image_url = first if isinstance(first, str) else first.get("url", "")
                elif isinstance(images, str):
                    image_url = images
                elif isinstance(images, dict):
                    image_url = images.get("url", "")
                authors_raw = data.get("author", [])
                if isinstance(authors_raw, dict):
                    authors_raw = [authors_raw]
                if isinstance(authors_raw, list):
                    for a in authors_raw:
                        if isinstance(a, dict):
                            name = a.get("name", "")
                        elif isinstance(a, str) and not a.startswith("http"):
                            name = a
                        else:
                            name = ""
                        if name:
                            ld_authors.append(name)
                break

        # --- Tier 2: OpenGraph meta tags ------------------------------
        og_title = self._meta(soup, "og:title")
        og_image = self._meta(soup, "og:image")
        og_published = (
            self._meta(soup, "article:published_time")
            or self._meta(soup, "article:published")
        )
        og_authors: list[str] = []
        for meta in soup.find_all("meta", {"property": "article:author"}):
            author = meta.get("content", "")
            if author and not author.startswith("http"):
                og_authors.append(author)
        og_section = self._meta(soup, "article:section")

        # --- Tier 3: DOM selectors ------------------------------------
        title = ld_title or og_title or ""
        if not title:
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(strip=True)

        # WSJ uses <meta name="author"> with clean names.
        authors = ld_authors or og_authors
        if not authors:
            meta_author = self._meta(soup, "author")
            if meta_author:
                authors = [a.strip() for a in meta_author.split(",") if a.strip()]
        if not authors:
            for a in soup.find_all("a", href=lambda h: h and "/author" in h):
                name = a.get_text(strip=True)
                if name and name not in authors:
                    authors.append(name)

        published_at = ld_published or og_published or ""
        if not published_at:
            time_el = soup.find("time")
            if time_el:
                published_at = time_el.get("datetime", "")

        if not section:
            section = og_section or ""

        if not image_url:
            image_url = og_image or ""

        # Dek (WSJ sub-headline summary).
        dek = ""
        dek_el = soup.find(class_=lambda c: c and ("dek" in c.lower()
                           if isinstance(c, str)
                           else any("dek" in x.lower() for x in c)))
        if dek_el:
            dek = dek_el.get_text(strip=True)
        if not dek:
            dek = ld_description or self._meta(soup, "og:description") or ""

        # --- Body paragraphs ------------------------------------------
        paragraphs = self._extract_body(soup)
        content = "\n\n".join(paragraphs)

        return WSJArticle(
            url=url,
            title=title,
            content=content,
            authors=authors,
            published_at=published_at,
            section=section,
            keywords=keywords,
            image_url=image_url,
            dek=dek,
            fetched=bool(content),
            error=None if content else "empty article body",
        )

    def _extract_body(self, soup: BeautifulSoup) -> list[str]:
        """Extract article body paragraphs, filtering boilerplate."""
        paragraphs: list[str] = []

        # WSJ uses CSS-module class names like "css-…-Paragraph".
        # First try to collect paragraphs with that pattern.
        for p in soup.find_all("p", class_=lambda c: c and (
            "Paragraph" in c if isinstance(c, str)
            else any("Paragraph" in x for x in c)
        )):
            text = p.get_text(strip=True)
            if text and not self._is_boilerplate(text):
                paragraphs.append(text)

        if paragraphs:
            return paragraphs

        # Fallback: body container heuristic.
        body_container = (
            soup.find("div", class_=lambda c: c and "body" in c.lower()
                       if isinstance(c, str)
                       else c and any("body" in x.lower() for x in c))
            or soup.find("article")
            or soup
        )

        for p in body_container.find_all("p"):
            text = p.get_text(strip=True)
            if text and not self._is_boilerplate(text):
                paragraphs.append(text)

        return paragraphs

    @staticmethod
    def _is_boilerplate(text: str) -> bool:
        lower = text.lower()
        if len(text) > 200:
            return False
        return any(bp in lower for bp in _BOILERPLATE_PATTERNS)

    @staticmethod
    def _meta(soup: BeautifulSoup, prop: str) -> str:
        tag = soup.find("meta", {"property": prop}) or soup.find("meta", {"name": prop})
        if tag:
            return tag.get("content", "")
        return ""
