"""ForexFactory scraper – calendar and news."""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup

from analyst.ingestion.http_transport import create_cf_session
from analyst.storage import StoredEventRecord

from ._common import (
    OPEN_UTC_PLUS_8,
    ScrapedNewsItem,
    categorize_event,
    generate_event_id,
    parse_numeric_value,
    to_epoch,
)


class ForexFactoryCalendarClient:
    BASE_URL = "https://www.forexfactory.com/calendar"

    def __init__(self) -> None:
        self.session = create_cf_session(headers={
            "Accept": "text/html,application/xhtml+xml",
        })

    def fetch(self, *, week: str = "this") -> list[StoredEventRecord]:
        url = self.BASE_URL if week == "this" else f"{self.BASE_URL}?week={week}"
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"class": "calendar__table"})
        if table is None:
            return []
        current_date = datetime.now(OPEN_UTC_PLUS_8).strftime("%Y-%m-%d")
        events: list[StoredEventRecord] = []
        for row in table.find_all("tr", {"class": "calendar__row"}):
            try:
                date_cell = row.find("td", {"class": "calendar__date"})
                if date_cell and date_cell.text.strip():
                    current_date = date_cell.text.strip()
                time_cell = row.find("td", {"class": "calendar__time"})
                event_time = time_cell.text.strip() if time_cell else "00:00"
                currency_cell = row.find("td", {"class": "calendar__currency"})
                currency = currency_cell.text.strip() if currency_cell else "USD"
                event_cell = row.find("td", {"class": "calendar__event"})
                event_label = event_cell.text.strip() if event_cell else ""
                if not event_label:
                    continue
                impact_cell = row.find("td", {"class": "calendar__impact"})
                importance = "medium"
                if impact_cell:
                    impact_text = " ".join(impact_cell.get("class", []))
                    if "red" in impact_text:
                        importance = "high"
                    elif "yel" in impact_text:
                        importance = "low"
                country = {
                    "USD": "US",
                    "EUR": "EU",
                    "GBP": "UK",
                    "JPY": "JP",
                    "CAD": "CA",
                    "AUD": "AU",
                    "NZD": "NZ",
                    "CHF": "CH",
                    "CNY": "CN",
                    "SGD": "SG",
                }.get(currency, currency)
                timestamp = to_epoch(date_value=current_date, time_value=event_time)
                actual = self._clean_cell_text(row.find("td", {"class": "calendar__actual"}))
                forecast = self._clean_cell_text(row.find("td", {"class": "calendar__forecast"}))
                previous = self._clean_cell_text(row.find("td", {"class": "calendar__previous"}))
                events.append(
                    StoredEventRecord(
                        source="forexfactory",
                        event_id=generate_event_id(country, event_label, timestamp),
                        timestamp=timestamp,
                        country=country,
                        indicator=event_label,
                        category=categorize_event(event_label),
                        importance=importance,
                        actual=actual,
                        forecast=forecast,
                        previous=previous,
                        surprise=self._compute_surprise(actual, forecast),
                        raw_json={
                            "currency": currency,
                            "date": current_date,
                            "time": event_time,
                        },
                    )
                )
            except Exception:
                continue
        return events

    def _clean_cell_text(self, cell: Any) -> str | None:
        if cell is None:
            return None
        value = cell.text.strip()
        return value if value and value != "\xa0" else None

    def _compute_surprise(self, actual: str | None, forecast: str | None) -> float | None:
        actual_value = parse_numeric_value(actual)
        forecast_value = parse_numeric_value(forecast)
        if actual_value is None or forecast_value is None:
            return None
        return round(actual_value - forecast_value, 4)


class ForexFactoryNewsClient:
    """Scrapes news articles from ForexFactory."""

    BASE_URL = "https://www.forexfactory.com/news"

    def __init__(self) -> None:
        self.session = create_cf_session(headers={
            "Accept": "text/html,application/xhtml+xml",
        })

    def fetch_news(self, *, page: int = 1) -> list[ScrapedNewsItem]:
        """Fetch news from the ForexFactory news page (*page* >= 1)."""
        url = self.BASE_URL if page <= 1 else f"{self.BASE_URL}?page={page}"
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return self._parse_news_html(response.text)

    def fetch_all_news(self, *, max_pages: int = 3) -> list[ScrapedNewsItem]:
        """Paginate through up to *max_pages* of ForexFactory news."""
        all_items: list[ScrapedNewsItem] = []
        for p in range(1, max_pages + 1):
            items = self.fetch_news(page=p)
            all_items.extend(items)
            if not items:
                break
            if p < max_pages:
                time.sleep(1.5)
        return all_items

    def _parse_news_html(self, html: str) -> list[ScrapedNewsItem]:
        soup = BeautifulSoup(html, "html.parser")
        items: list[ScrapedNewsItem] = []
        seen: set[str] = set()

        for title_div in soup.find_all("div", {"class": "news-block__title"}):
            try:
                link = title_div.find("a")
                if not link:
                    continue
                title = link.get_text(strip=True)
                href = link.get("href", "")
                if not title or href in seen:
                    continue
                seen.add(href)

                full_url = f"https://www.forexfactory.com{href}" if href.startswith("/") else href
                parent = title_div.parent

                # Extract source and time from details block
                details_div = parent.find("div", {"class": "news-block__details"}) if parent else None
                source = ""
                time_ago = ""
                comments = 0
                if details_div:
                    source, time_ago, comments = self._parse_details(details_div.get_text("|", strip=True))

                # Extract preview text
                preview_div = parent.find("div", {"class": "news-block__preview"}) if parent else None
                preview = preview_div.get_text(strip=True) if preview_div else ""

                # Extract thumbnail
                thumbnail = ""
                if parent:
                    img = parent.find("img")
                    if img:
                        thumbnail = img.get("src", "") or img.get("data-src", "")

                # Extract impact level
                importance = ""
                if parent:
                    impact_span = parent.find("span", {"class": lambda c: c and "universal-impact" in str(c)})
                    if impact_span:
                        cls = " ".join(impact_span.get("class", []))
                        if "high" in cls:
                            importance = "high"
                        elif "medium" in cls:
                            importance = "medium"
                        elif "low" in cls:
                            importance = "low"

                items.append(ScrapedNewsItem(
                    source="forexfactory",
                    title=title,
                    url=full_url,
                    description=preview,
                    author=source,
                    importance=importance,
                    image_url=thumbnail,
                    raw_json={
                        "time_ago": time_ago,
                        "comments": comments,
                    },
                ))
            except Exception:
                continue

        return items

    @staticmethod
    def _parse_details(text: str) -> tuple[str, str, int]:
        """Parse 'From source|time ago|N comments' into (source, time_ago, comments)."""
        source = ""
        time_ago = ""
        comments = 0
        parts = [p.strip() for p in text.split("|") if p.strip()]
        for part in parts:
            if part.lower().startswith("from "):
                source = part[5:].strip()
            elif "ago" in part.lower():
                time_ago = part
            elif "comment" in part.lower():
                m = re.search(r"(\d+)", part)
                if m:
                    comments = int(m.group(1))
        return source, time_ago, comments
