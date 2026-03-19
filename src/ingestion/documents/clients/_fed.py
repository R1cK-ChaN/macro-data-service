"""Fed communications ingestion client."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import feedparser
import requests
from bs4 import BeautifulSoup

from ingestion.series_config import FED_FEEDS, FED_SPEAKERS
from storage import CentralBankCommunicationRecord, SQLiteEngineStore

logger = logging.getLogger(__name__)


def extract_speaker(title: str) -> str:
    for speaker in FED_SPEAKERS:
        if speaker.lower() in title.lower():
            return speaker
    return ""


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


class FedIngestionClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "AnalystEngine/1.0"})

    def refresh(self, store: SQLiteEngineStore, *, fetch_full_text: bool = False) -> RefreshStats:
        communications = self.fetch_communications(fetch_full_text=fetch_full_text)
        return RefreshStats(source="fed", count=self.store_communications(store, communications))

    def fetch_communications(self, *, fetch_full_text: bool = False) -> list[CentralBankCommunicationRecord]:
        communications: list[CentralBankCommunicationRecord] = []
        for feed in FED_FEEDS.values():
            communications.extend(
                self._parse_feed(feed["url"], feed["content_type"], fetch_full_text=fetch_full_text)
            )
            time.sleep(0.5)
        return communications

    def store_communications(
        self,
        store: SQLiteEngineStore,
        communications: list[CentralBankCommunicationRecord],
    ) -> int:
        for communication in communications:
            store.upsert_central_bank_comm(communication)
        return len(communications)

    def _parse_feed(
        self,
        feed_url: str,
        content_type: str,
        *,
        fetch_full_text: bool,
    ) -> list[CentralBankCommunicationRecord]:
        communications: list[CentralBankCommunicationRecord] = []
        parsed = feedparser.parse(feed_url)
        for entry in parsed.entries:
            ts = 0
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                ts = int(datetime(*entry.published_parsed[:6], tzinfo=UTC).timestamp())
            title = entry.get("title", "")
            url = entry.get("link", "")
            summary = BeautifulSoup(entry.get("summary", ""), "html.parser").get_text(" ", strip=True)
            full_text = summary
            if fetch_full_text and url:
                full_text = self.fetch_full_text(url) or summary
            communications.append(
                CentralBankCommunicationRecord(
                    source="fed",
                    title=title,
                    url=url,
                    timestamp=ts or int(datetime.now(UTC).timestamp()),
                    content_type=self._detect_content_type(title, content_type),
                    speaker=extract_speaker(title),
                    summary=summary,
                    full_text=full_text,
                )
            )
        return communications

    def fetch_full_text(self, url: str) -> str:
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        content_div = (
            soup.find("div", {"id": "article"})
            or soup.find("div", {"class": "col-xs-12 col-sm-8 col-md-8"})
            or soup.find("div", {"class": "row"})
        )
        if content_div is None:
            return ""
        for tag in content_div.find_all(["script", "style", "nav"]):
            tag.decompose()
        text = content_div.get_text(separator="\n", strip=True)
        return re.sub(r"\n{3,}", "\n\n", text)[:50_000]

    def _detect_content_type(self, title: str, fallback: str) -> str:
        lowered = title.lower()
        if "minutes" in lowered:
            return "minutes"
        if "statement" in lowered or "fomc" in lowered:
            return "statement"
        if "beige book" in lowered:
            return "beige_book"
        if "testimony" in lowered:
            return "testimony"
        return fallback


