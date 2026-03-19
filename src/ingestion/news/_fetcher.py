"""Fetch full article content from URLs and convert to markdown."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from urllib.parse import quote, urlparse

import httpx
from readability import Document
from markdownify import markdownify

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_GNEWS_SIG_RE = re.compile(r'data-n-a-sg="([^"]+)"')
_GNEWS_TS_RE = re.compile(r'data-n-a-ts="([^"]+)"')

_BATCHEXECUTE_URL = (
    "https://news.google.com/_/DotsSplashUi/data/batchexecute"
)
_BATCHEXECUTE_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
}


@dataclass
class ArticleContent:
    content: str
    fetched: bool
    content_length: int
    error: str | None = None


class ArticleFetcher:
    """Fetch and extract readable article content from URLs."""

    def __init__(
        self,
        timeout: int = 20,
        max_content_chars: int = 15_000,
    ):
        self._timeout = timeout
        self._max_content_chars = max_content_chars
        self._client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": _USER_AGENT},
        )

    @staticmethod
    def _is_google_news_url(url: str) -> bool:
        try:
            return urlparse(url).hostname == "news.google.com"
        except Exception:
            return False

    def _resolve_google_news_url(self, url: str) -> str | None:
        """Resolve a Google News proxy URL to the real article URL."""
        try:
            path = urlparse(url).path.split("/")
            base64_str = path[-1]

            page_url = (
                f"https://news.google.com/articles/{base64_str}"
                "?hl=en-US&gl=US&ceid=US:en"
            )
            resp = self._client.get(page_url)
            if resp.status_code != 200:
                return None

            sig_m = _GNEWS_SIG_RE.search(resp.text)
            ts_m = _GNEWS_TS_RE.search(resp.text)
            if not sig_m or not ts_m:
                return None

            sig, ts = sig_m.group(1), ts_m.group(1)

            payload = [
                "Fbv4je",
                (
                    '["garturlreq",[["X","X",["X","X"],'
                    'null,null,1,1,"US:en",null,1,null,null,'
                    'null,null,null,0,1],"X","X",1,[1,1,1],'
                    f'1,1,null,0,0,null,0],"{base64_str}",{ts},"{sig}"]'
                ),
            ]
            body = f"f.req={quote(json.dumps([[payload]]))}"
            resp2 = self._client.post(
                _BATCHEXECUTE_URL,
                headers=_BATCHEXECUTE_HEADERS,
                data=body,
            )
            if resp2.status_code != 200 or "garturlres" not in resp2.text:
                return None

            parts = resp2.text.split("\n\n")
            parsed = json.loads(parts[1])[:-2]
            decoded_url = json.loads(parsed[0][2])[1]
            return decoded_url

        except Exception as exc:
            logger.debug("Google News URL resolve failed for %s: %s", url, exc)
            return None

    def fetch_article(self, url: str, rss_description: str) -> ArticleContent:
        """Fetch full article content from *url*.

        For Google News proxy URLs, resolves the real article URL first.
        Returns extracted markdown on success, or *rss_description* as
        fallback on any error.
        """
        if self._is_google_news_url(url):
            real_url = self._resolve_google_news_url(url)
            if not real_url:
                return ArticleContent(
                    content=rss_description,
                    fetched=False,
                    content_length=len(rss_description),
                    error="Google News URL resolution failed",
                )
            url = real_url

        try:
            resp = self._client.get(url)
            resp.raise_for_status()

            doc = Document(resp.text)
            html_summary = doc.summary()
            text = markdownify(html_summary).strip()

            if not text:
                return ArticleContent(
                    content=rss_description,
                    fetched=False,
                    content_length=len(rss_description),
                    error="readability produced empty output",
                )

            if len(text) > self._max_content_chars:
                text = text[: self._max_content_chars]

            return ArticleContent(
                content=text,
                fetched=True,
                content_length=len(text),
            )

        except Exception as exc:
            logger.debug("Article fetch failed for %s: %s", url, exc)
            return ArticleContent(
                content=rss_description,
                fetched=False,
                content_length=len(rss_description),
                error=str(exc),
            )

    def close(self):
        self._client.close()
