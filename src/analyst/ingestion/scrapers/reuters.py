"""Reuters scraper – news listings and full article extraction."""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any

from bs4 import BeautifulSoup, Tag

from analyst.ingestion.http_transport import create_cf_session

from ._common import ScrapedNewsItem

logger = logging.getLogger(__name__)

BASE_URL = "https://www.reuters.com"

REUTERS_SECTIONS = {
    "markets": "/markets/",
    "world": "/world/",
    "business": "/business/",
    "sustainability": "/sustainability/",
    "legal": "/legal/",
    "technology": "/technology/",
}

# CSS-module class prefixes (hash suffix varies per build).
_PARAGRAPH_CLASS_RE = re.compile(r"article-body-module__paragraph__")
_CONTENT_CLASS_RE = re.compile(r"article-body-module__content__")
_SIGN_OFF_CLASS_RE = re.compile(r"sign-off-module__")
_TRUST_BADGE_CLASS_RE = re.compile(r"article-body-module__trust-badge__")


# ------------------------------------------------------------------
# Data class for full article content
# ------------------------------------------------------------------

@dataclass
class ReutersArticle:
    """Parsed full article from Reuters."""

    url: str
    title: str
    content: str  # body as plain text
    authors: list[str] = field(default_factory=list)
    published_at: str = ""
    section: str = ""
    keywords: list[str] = field(default_factory=list)
    image_url: str = ""
    fetched: bool = True
    error: str | None = None


# ------------------------------------------------------------------
# News listing client
# ------------------------------------------------------------------

class ReutersNewsClient:
    """Scrapes article listings from Reuters section pages."""

    def __init__(self) -> None:
        self.session = create_cf_session(headers={
            "Accept": "text/html,application/xhtml+xml",
        })

    def fetch_news(self, *, section: str = "markets") -> list[ScrapedNewsItem]:
        """Fetch article listings from a single Reuters section."""
        path = REUTERS_SECTIONS.get(section, f"/{section}/")
        url = f"{BASE_URL}{path}"
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return self._parse_listing_html(response.text, section)

    def fetch_all_news(
        self,
        *,
        sections: list[str] | None = None,
        sleep_between: float = 1.5,
    ) -> list[ScrapedNewsItem]:
        """Fetch article listings from multiple sections with 1.5 s delay.

        *sections* defaults to ``["markets", "business", "world"]``.
        """
        targets = sections or ["markets", "business", "world"]
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
                logger.warning("Reuters listing fetch failed for %s: %s", section, exc)
            if idx < len(targets) - 1:
                time.sleep(sleep_between)

        return all_items

    # ---- internal --------------------------------------------------

    def _parse_listing_html(self, html: str, section: str) -> list[ScrapedNewsItem]:
        soup = BeautifulSoup(html, "html.parser")
        items: list[ScrapedNewsItem] = []
        seen: set[str] = set()

        # Unified extraction across card types.
        card_types = ["HeroCard", "BasicCard", "MediaStoryCard"]
        for card_type in card_types:
            for card in soup.find_all("div", {"data-testid": card_type}):
                item = self._parse_card(card, card_type, section)
                if item and item.url not in seen:
                    seen.add(item.url)
                    items.append(item)

        return items

    def _parse_card(
        self, card: Tag, card_type: str, section: str,
    ) -> ScrapedNewsItem | None:
        try:
            # Title & URL — different testid per card type.
            title_link: Tag | None = None
            for testid in ("Heading", "Title"):
                title_link = card.find("a", {"data-testid": testid})
                if title_link:
                    break
            if not title_link:
                return None

            title = title_link.get_text(strip=True)
            href = title_link.get("href", "")
            if not title or not href:
                return None
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"

            # Timestamp
            time_el = card.find("time")
            published_at = ""
            if time_el:
                published_at = time_el.get("datetime", "") or time_el.get_text(strip=True)

            # Category / kicker
            category = section
            kicker_el = card.find("span", {"data-testid": "Label"})
            if kicker_el:
                kicker_text = kicker_el.get_text(strip=True)
                # Strip trailing "category" label that Reuters appends.
                kicker_text = re.sub(r"category$", "", kicker_text, flags=re.I).strip()
                if kicker_text:
                    category = kicker_text
            if not kicker_el:
                cat_link = card.find("a", {"data-testid": "Link"})
                if cat_link:
                    cat_text = cat_link.get_text(strip=True)
                    cat_text = re.sub(r"category$", "", cat_text, flags=re.I).strip()
                    if cat_text:
                        category = cat_text

            # Description (only MediaStoryCard sometimes has a body snippet).
            description = ""
            body_el = card.find("a", {"data-testid": "Body"})
            if body_el:
                description = body_el.get_text(strip=True)

            # Thumbnail
            image_url = ""
            img = card.find("img")
            if img:
                image_url = img.get("src", "") or img.get("data-src", "")

            return ScrapedNewsItem(
                source="reuters",
                title=title,
                url=full_url,
                published_at=published_at,
                description=description,
                category=category,
                image_url=image_url,
                raw_json={"card_type": card_type},
            )
        except Exception:
            return None


# ------------------------------------------------------------------
# Full article client
# ------------------------------------------------------------------

class ReutersArticleClient:
    """Fetches and parses full Reuters articles with structured metadata.

    Use this instead of the generic ``ArticleFetcher`` for reuters.com
    URLs to get cleaner text and richer metadata.
    """

    def __init__(self) -> None:
        self.session = create_cf_session(headers={
            "Accept": "text/html,application/xhtml+xml",
        })

    def fetch_article(self, url: str) -> ReutersArticle:
        """Fetch and parse a single Reuters article."""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return self._parse_article_html(response.text, url)
        except Exception as exc:
            logger.warning("Reuters article fetch failed for %s: %s", url, exc)
            return ReutersArticle(
                url=url, title="", content="",
                fetched=False, error=str(exc),
            )

    def fetch_articles(
        self,
        urls: list[str],
        *,
        sleep_between: float = 1.0,
    ) -> list[ReutersArticle]:
        """Fetch multiple articles with rate limiting."""
        articles: list[ReutersArticle] = []
        for idx, url in enumerate(urls):
            articles.append(self.fetch_article(url))
            if idx < len(urls) - 1:
                time.sleep(sleep_between)
        return articles

    # ---- internal --------------------------------------------------

    def _parse_article_html(self, html: str, url: str) -> ReutersArticle:
        soup = BeautifulSoup(html, "html.parser")

        # --- JSON-LD metadata (most reliable source) -----------------
        section = ""
        keywords: list[str] = []
        image_url = ""
        ld_published = ""

        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(data, dict) and "Article" in data.get("@type", ""):
                section = data.get("articleSection", "")
                keywords = self._clean_keywords(data.get("keywords", []))
                images = data.get("image", [])
                if isinstance(images, list) and images:
                    image_url = images[0]
                elif isinstance(images, str):
                    image_url = images
                ld_published = data.get("datePublished", "")
                break

        # --- Headline ------------------------------------------------
        h1 = soup.find("h1", {"data-testid": "Heading"})
        title = h1.get_text(strip=True) if h1 else ""

        # --- Authors -------------------------------------------------
        authors: list[str] = []
        for a in soup.find_all("a", {"data-testid": "AuthorNameLink"}):
            name = a.get_text(strip=True)
            if name:
                authors.append(name)

        # --- Published date ------------------------------------------
        published_at = ld_published
        if not published_at:
            time_el = soup.find("time")
            if time_el:
                published_at = time_el.get("datetime", "")

        # --- Body paragraphs ----------------------------------------
        paragraphs = self._extract_body(soup)
        content = "\n\n".join(paragraphs)

        return ReutersArticle(
            url=url,
            title=title,
            content=content,
            authors=authors,
            published_at=published_at,
            section=section,
            keywords=keywords,
            image_url=image_url,
            fetched=bool(content),
            error=None if content else "empty article body",
        )

    def _extract_body(self, soup: BeautifulSoup) -> list[str]:
        """Extract article body paragraphs, excluding boilerplate."""
        paragraphs: list[str] = []

        # Paragraph divs use CSS-module class: article-body-module__paragraph__<hash>
        for el in soup.find_all(
            "div",
            class_=_PARAGRAPH_CLASS_RE,
        ):
            # Skip sign-off ("Reporting by …") and trust badge lines.
            if el.find(class_=_SIGN_OFF_CLASS_RE):
                # Still include sign-off text as it lists reporters.
                text = el.get_text(strip=True)
                if text:
                    paragraphs.append(text)
                continue
            if el.find(class_=_TRUST_BADGE_CLASS_RE):
                continue

            text = el.get_text(strip=True)
            if text and not self._is_boilerplate(text):
                paragraphs.append(text)

        return paragraphs

    @staticmethod
    def _is_boilerplate(text: str) -> bool:
        """Filter out common Reuters boilerplate lines."""
        lower = text.lower()
        boilerplate = (
            "sign up here",
            "opens new tab",
            "our standards:",
            "thomson reuters trust principles",
        )
        return any(b in lower for b in boilerplate) and len(text) < 120

    @staticmethod
    def _clean_keywords(raw: list[Any]) -> list[str]:
        """Keep only human-readable keywords from JSON-LD, drop internal codes."""
        cleaned: list[str] = []
        for kw in raw:
            if not isinstance(kw, str):
                continue
            # Skip Reuters internal tag codes (e.g. "COM", "CRU", "ENER").
            if kw.isupper() and len(kw) <= 6:
                continue
            # Skip coded tags like "REPI:OPEC" or "RULES:IRAN".
            if ":" in kw:
                # Keep TOPIC:* tags but clean them up.
                if kw.startswith("TOPIC:"):
                    cleaned.append(kw[6:].replace("-", " ").lower())
                continue
            cleaned.append(kw)
        return cleaned
