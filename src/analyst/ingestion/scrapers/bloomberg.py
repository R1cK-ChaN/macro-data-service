"""Bloomberg scraper – news listings and full article extraction.

Uses ``curl_cffi`` with TLS fingerprint impersonation for HTTP requests.
Authenticated cookies are loaded from ``~/.analyst/bloomberg_cookies.json``,
exported from a real Chrome session via ``browser_cookie3``.

Cookie setup::

    pip install browser-cookie3
    python -c "
    import browser_cookie3, json
    from pathlib import Path
    cj = browser_cookie3.chrome(domain_name='.bloomberg.com')
    cookies = [{'name': c.name, 'value': c.value, 'domain': c.domain,
                'path': c.path, 'expires': c.expires or -1,
                'secure': bool(c.secure), 'httpOnly': False, 'sameSite': 'Lax'}
               for c in cj]
    out = Path.home() / '.analyst' / 'bloomberg_cookies.json'
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

BASE_URL = "https://www.bloomberg.com"
COOKIE_PATH = Path.home() / ".analyst" / "bloomberg_cookies.json"

BLOOMBERG_SECTIONS = {
    "markets": "/markets",
    "economics": "/economics",
    "technology": "/technology",
    "politics": "/politics",
    "wealth": "/wealth",
    "opinion": "/opinion",
    "green": "/green",
}

# Boilerplate patterns filtered from article body text.
_BOILERPLATE_PATTERNS = (
    "sign up for",
    "subscribe to",
    "read more:",
    "newsletter",
    "Bloomberg Businessweek",
    "terms of service",
    "privacy policy",
    "with assistance from",
)


# ------------------------------------------------------------------
# Exceptions (backward-compatible alias)
# ------------------------------------------------------------------

class BloombergAuthError(Exception):
    """Raised when Bloomberg authentication is missing or expired."""


# ------------------------------------------------------------------
# Data class for full article content
# ------------------------------------------------------------------

@dataclass
class BloombergArticle:
    """Parsed full article from Bloomberg."""

    url: str
    title: str
    content: str  # body as plain text
    authors: list[str] = field(default_factory=list)
    published_at: str = ""
    section: str = ""
    keywords: list[str] = field(default_factory=list)
    image_url: str = ""
    lede: str = ""  # Bloomberg-specific article summary
    fetched: bool = True
    error: str | None = None


# ------------------------------------------------------------------
# Cookie / session helpers
# ------------------------------------------------------------------

def _load_cookies_into_session(session: Any) -> None:
    """Load Bloomberg cookies from disk into a curl_cffi session."""
    if not COOKIE_PATH.exists():
        logger.warning("No Bloomberg cookie file at %s — requests will be unauthenticated.", COOKIE_PATH)
        return
    try:
        cookies = json.loads(COOKIE_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        logger.warning("Failed to read Bloomberg cookie file; ignoring.")
        return
    now = time.time()
    for c in cookies:
        if c.get("expires", -1) != -1 and c.get("expires", 0) < now:
            continue
        session.cookies.set(
            c["name"], c["value"],
            domain=c.get("domain", ".bloomberg.com"),
            path=c.get("path", "/"),
        )


def _make_session() -> Any:
    """Create a curl_cffi session with Bloomberg cookies loaded."""
    session = create_cf_session(headers={
        "Accept": "text/html,application/xhtml+xml",
    })
    _load_cookies_into_session(session)
    return session


# ------------------------------------------------------------------
# News listing client
# ------------------------------------------------------------------

class BloombergNewsClient:
    """Scrapes article listings from Bloomberg section pages using curl_cffi."""

    def __init__(self) -> None:
        self.session = _make_session()

    def __enter__(self) -> BloombergNewsClient:
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    def fetch_news(self, *, section: str = "markets") -> list[ScrapedNewsItem]:
        """Fetch article listings from a single Bloomberg section."""
        path = BLOOMBERG_SECTIONS.get(section, f"/{section}")
        url = f"{BASE_URL}{path}"

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        except Exception as exc:
            logger.warning("Bloomberg page load failed for %s: %s", section, exc)
            return []

        return self._parse_listing_html(response.text, section)

    def fetch_all_news(
        self,
        *,
        sections: list[str] | None = None,
        sleep_between: float = 1.5,
    ) -> list[ScrapedNewsItem]:
        """Fetch article listings from multiple sections with delay.

        *sections* defaults to ``["markets", "economics", "technology"]``.
        """
        targets = sections or ["markets", "economics", "technology"]
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
                logger.warning("Bloomberg listing fetch failed for %s: %s", section, exc)
            if idx < len(targets) - 1:
                time.sleep(sleep_between)

        return all_items

    # ---- internal --------------------------------------------------

    def _parse_listing_html(self, html: str, section: str) -> list[ScrapedNewsItem]:
        soup = BeautifulSoup(html, "html.parser")
        items: list[ScrapedNewsItem] = []
        seen: set[str] = set()

        # Strategy 1: __NEXT_DATA__ JSON (React hydration payload).
        next_data = self._try_next_data(soup, section)
        if next_data:
            for item in next_data:
                if item.url not in seen:
                    seen.add(item.url)
                    items.append(item)
            return items

        # Strategy 2: JSON-LD structured data.
        ld_items = self._try_json_ld(soup, section)
        if ld_items:
            for item in ld_items:
                if item.url not in seen:
                    seen.add(item.url)
                    items.append(item)
            return items

        # Strategy 3: DOM article elements.
        for article in soup.find_all("article"):
            item = self._parse_article_card(article, section)
            if item and item.url not in seen:
                seen.add(item.url)
                items.append(item)

        # Also try generic story containers.
        for card in soup.find_all(
            ["div", "section"],
            class_=lambda c: c and ("story" in c.lower() if isinstance(c, str)
                                     else any("story" in x.lower() for x in c)),
        ):
            item = self._parse_article_card(card, section)
            if item and item.url not in seen:
                seen.add(item.url)
                items.append(item)

        return items

    def _try_next_data(self, soup: BeautifulSoup, section: str) -> list[ScrapedNewsItem]:
        """Extract articles from __NEXT_DATA__ JSON if present."""
        script = soup.find("script", {"id": "__NEXT_DATA__"})
        if not script or not script.string:
            return []
        try:
            data = json.loads(script.string)
        except (json.JSONDecodeError, TypeError):
            return []

        items: list[ScrapedNewsItem] = []
        # Walk the JSON tree looking for story objects.
        self._walk_next_data(data, items, section)
        return items

    def _walk_next_data(self, obj: Any, items: list[ScrapedNewsItem], section: str) -> None:
        """Recursively walk a JSON structure extracting story-like objects."""
        if isinstance(obj, dict):
            # Heuristic: a story has a "headline" or "title" and a URL-like field.
            headline = obj.get("headline") or obj.get("title") or ""
            url = obj.get("url") or obj.get("canonical") or obj.get("href") or ""
            if headline and url and isinstance(headline, str) and isinstance(url, str):
                if not url.startswith("http"):
                    url = f"{BASE_URL}{url}"
                if "/news/" in url or "/articles/" in url or "/opinion/" in url:
                    items.append(ScrapedNewsItem(
                        source="bloomberg",
                        title=headline,
                        url=url,
                        published_at=str(obj.get("publishedAt", obj.get("published", ""))),
                        description=str(obj.get("summary", obj.get("abstract", ""))),
                        category=str(obj.get("primaryCategory", obj.get("section", section))),
                        image_url=str(obj.get("imageUrl", obj.get("image", ""))),
                    ))
            for v in obj.values():
                self._walk_next_data(v, items, section)
        elif isinstance(obj, list):
            for v in obj:
                self._walk_next_data(v, items, section)

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
        """Convert a JSON-LD entry to ScrapedNewsItem if it looks like an article."""
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
            source="bloomberg",
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

    def _parse_article_card(self, card: Tag, section: str) -> ScrapedNewsItem | None:
        """Parse a DOM element that looks like a story card."""
        try:
            # Find the primary link.
            link = card.find("a", href=True)
            if not link:
                return None
            href = link.get("href", "")
            if not href:
                return None
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            # Must look like an article URL.
            if not any(seg in full_url for seg in ("/news/", "/articles/", "/opinion/", "/features/")):
                return None

            # Title: prefer heading elements, fall back to link text.
            title = ""
            for tag in ("h1", "h2", "h3", "h4"):
                heading = card.find(tag)
                if heading:
                    title = heading.get_text(strip=True)
                    break
            if not title:
                title = link.get_text(strip=True)
            if not title:
                return None

            # Timestamp.
            published_at = ""
            time_el = card.find("time")
            if time_el:
                published_at = time_el.get("datetime", "") or time_el.get_text(strip=True)

            # Description / summary.
            description = ""
            summary_el = card.find("p")
            if summary_el:
                description = summary_el.get_text(strip=True)

            # Image.
            image_url = ""
            img = card.find("img")
            if img:
                image_url = img.get("src", "") or img.get("data-src", "")

            return ScrapedNewsItem(
                source="bloomberg",
                title=title,
                url=full_url,
                published_at=published_at,
                description=description,
                category=section,
                image_url=image_url,
            )
        except Exception:
            return None


# ------------------------------------------------------------------
# Full article client
# ------------------------------------------------------------------

class BloombergArticleClient:
    """Fetches and parses full Bloomberg articles with structured metadata.

    Requires authenticated cookies at ``~/.analyst/bloomberg_cookies.json``.
    """

    def __init__(self) -> None:
        self.session = _make_session()

    def __enter__(self) -> BloombergArticleClient:
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    def fetch_article(self, url: str) -> BloombergArticle:
        """Fetch and parse a single Bloomberg article."""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return self._parse_article_html(response.text, url)
        except Exception as exc:
            logger.warning("Bloomberg article fetch failed for %s: %s", url, exc)
            return BloombergArticle(
                url=url, title="", content="",
                fetched=False, error=str(exc),
            )

    def fetch_articles(
        self,
        urls: list[str],
        *,
        sleep_between: float = 1.5,
    ) -> list[BloombergArticle]:
        """Fetch multiple articles with rate limiting."""
        articles: list[BloombergArticle] = []
        for idx, url in enumerate(urls):
            articles.append(self.fetch_article(url))
            if idx < len(urls) - 1:
                time.sleep(sleep_between)
        return articles

    # ---- internal --------------------------------------------------

    def _parse_article_html(self, html: str, url: str) -> BloombergArticle:
        soup = BeautifulSoup(html, "html.parser")

        # --- Tier 1: JSON-LD metadata (most reliable) -----------------
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
                # Authors from JSON-LD.
                authors_raw = data.get("author", [])
                if isinstance(authors_raw, dict):
                    authors_raw = [authors_raw]
                if isinstance(authors_raw, list):
                    for a in authors_raw:
                        name = a.get("name", "") if isinstance(a, dict) else str(a)
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
            if author:
                og_authors.append(author)
        og_section = self._meta(soup, "article:section")

        # --- Tier 3: DOM selectors ------------------------------------
        # Headline.
        title = ld_title or og_title or ""
        if not title:
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(strip=True)

        # Authors.
        authors = ld_authors or og_authors
        if not authors:
            # Bloomberg author bylines.
            for a in soup.find_all("a", href=lambda h: h and "/authors/" in h):
                name = a.get_text(strip=True)
                if name and name not in authors:
                    authors.append(name)

        # Published date.
        published_at = ld_published or og_published or ""
        if not published_at:
            time_el = soup.find("time")
            if time_el:
                published_at = time_el.get("datetime", "")

        # Section.
        if not section:
            section = og_section or ""

        # Image.
        if not image_url:
            image_url = og_image or ""

        # Lede / summary.
        lede = ld_description or self._meta(soup, "og:description") or ""

        # --- Body paragraphs ------------------------------------------
        paragraphs = self._extract_body(soup)
        content = "\n\n".join(paragraphs)

        return BloombergArticle(
            url=url,
            title=title,
            content=content,
            authors=authors,
            published_at=published_at,
            section=section,
            keywords=keywords,
            image_url=image_url,
            lede=lede,
            fetched=bool(content),
            error=None if content else "empty article body",
        )

    def _extract_body(self, soup: BeautifulSoup) -> list[str]:
        """Extract article body paragraphs, filtering boilerplate."""
        paragraphs: list[str] = []

        # Bloomberg renders body in <p> tags within an article container.
        # Try to find the article body container first.
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
        """Filter common Bloomberg boilerplate lines."""
        lower = text.lower()
        if len(text) > 200:
            return False
        return any(bp in lower for bp in _BOILERPLATE_PATTERNS)

    @staticmethod
    def _meta(soup: BeautifulSoup, prop: str) -> str:
        """Get content of a <meta property=...> tag."""
        tag = soup.find("meta", {"property": prop}) or soup.find("meta", {"name": prop})
        if tag:
            return tag.get("content", "")
        return ""
