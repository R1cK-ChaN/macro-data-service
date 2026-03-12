"""Investing.com scraper – calendar and news."""

from __future__ import annotations

import re
import time
from datetime import datetime, timedelta
from typing import Any

from bs4 import BeautifulSoup

from analyst.ingestion.http_transport import create_cf_session
from analyst.storage import StoredEventRecord

from ._common import (
    COUNTRY_MAP,
    OPEN_UTC_PLUS_8,
    ScrapedNewsItem,
    categorize_event,
    generate_event_id,
    parse_numeric_value,
    to_epoch,
)

INVESTING_NEWS_CATEGORIES = (
    "latest-news",
    "economy-news",
    "commodities-news",
    "cryptocurrency-news",
    "forex-news",
    "stock-market-news",
    "economic-indicators",
    "world-news",
    "most-popular-news",
)


class InvestingCalendarClient:
    BASE_URL = "https://www.investing.com/economic-calendar/Service/getCalendarFilteredData"

    def __init__(self, countries: list[str] | None = None) -> None:
        self.session = create_cf_session(headers={
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.investing.com/economic-calendar/",
        })
        target_countries = countries or ["US", "EU", "JP", "UK", "CA", "AU", "CN", "SG"]
        self.country_ids = [country_id for country_id, code in COUNTRY_MAP.items() if code in target_countries]

    def fetch(self, *, date_from: str | None = None, date_to: str | None = None) -> list[StoredEventRecord]:
        date_from = date_from or datetime.now(OPEN_UTC_PLUS_8).strftime("%Y-%m-%d")
        date_to = date_to or date_from
        # Use list-of-tuples so repeated keys (country[], importance[])
        # are encoded correctly by both requests and curl_cffi.
        payload: list[tuple[str, str]] = [
            ("dateFrom", date_from),
            ("dateTo", date_to),
            ("timeZone", "8"),
            ("timeFilter", "timeRemain"),
            ("currentTab", "custom"),
            ("limit_from", "0"),
        ]
        for cid in self.country_ids:
            payload.append(("country[]", str(cid)))
        for imp in (1, 2, 3):
            payload.append(("importance[]", str(imp)))
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = self.session.post(self.BASE_URL, data=payload, timeout=30)
                response.raise_for_status()
                html_content = response.json().get("data", "")
                if not html_content:
                    return []
                return self._parse_calendar_html(html_content, date_from)
            except Exception as exc:
                last_error = exc
                if attempt < 2:
                    time.sleep(2 ** attempt)
        raise RuntimeError(
            f"Investing calendar fetch failed after 3 attempts for {date_from} to {date_to}."
        ) from last_error

    def fetch_range(self, *, days_back: int = 1, days_forward: int = 3) -> list[StoredEventRecord]:
        today = datetime.now(OPEN_UTC_PLUS_8).date()
        all_events: list[StoredEventRecord] = []
        for offset in range(-days_back, days_forward + 1):
            day = today + timedelta(days=offset)
            day_str = day.strftime("%Y-%m-%d")
            all_events.extend(self.fetch(date_from=day_str, date_to=day_str))
            if offset < days_forward:
                time.sleep(1.5)
        return all_events

    def _parse_calendar_html(self, html: str, default_date: str) -> list[StoredEventRecord]:
        soup = BeautifulSoup(html, "html.parser")
        events: list[StoredEventRecord] = []
        for row in soup.find_all("tr", {"class": "js-event-item"}):
            try:
                flag_td = row.find("td", {"class": "flagCur"})
                country_code = ""
                currency_text = ""
                if flag_td is not None:
                    flag_span = flag_td.find("span")
                    if flag_span is not None:
                        for class_name in flag_span.get("class", []):
                            if class_name.startswith("ce-"):
                                country_code = class_name.replace("ce-", "").upper()
                                break
                    cur_text = flag_td.get_text(strip=True)
                    if cur_text:
                        currency_text = cur_text
                time_td = row.find("td", {"class": "time"})
                event_time = time_td.text.strip() if time_td else "00:00"
                event_td = row.find("td", {"class": "event"})
                indicator = event_td.text.strip() if event_td else ""
                if not indicator:
                    continue
                importance_td = row.find("td", {"class": "sentiment"})
                importance = "low"
                if importance_td:
                    bulls = importance_td.find_all("i", {"class": "grayFullBullishIcon"})
                    if len(bulls) >= 3:
                        importance = "high"
                    elif len(bulls) >= 2:
                        importance = "medium"
                actual = self._clean_cell_text(row.find("td", {"class": "act"}))
                forecast = self._clean_cell_text(row.find("td", {"class": "fore"}))
                prev_td = row.find("td", {"class": "prev"})
                revised_previous: str | None = None
                if prev_td is not None:
                    revised_span = prev_td.find("span")
                    if revised_span is not None:
                        rev_text = revised_span.get_text(strip=True)
                        if rev_text and rev_text != "\xa0":
                            revised_previous = rev_text
                        revised_span.decompose()
                previous = self._clean_cell_text(prev_td)
                timestamp = to_epoch(date_value=default_date, time_value=event_time)
                event_id = row.get("event_attr_id") or row.get("id") or generate_event_id(country_code, indicator, timestamp)
                events.append(
                    StoredEventRecord(
                        source="investing",
                        event_id=str(event_id),
                        timestamp=timestamp,
                        country=country_code or "US",
                        indicator=indicator,
                        category=categorize_event(indicator),
                        importance=importance,
                        actual=actual,
                        forecast=forecast,
                        previous=previous,
                        revised_previous=revised_previous,
                        surprise=self._compute_surprise(actual, forecast),
                        currency=currency_text,
                        raw_json={
                            "source_row_id": row.get("id", ""),
                            "indicator": indicator,
                            "country": country_code,
                            "currency": currency_text,
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


class InvestingNewsClient:
    """Scrapes news articles from Investing.com."""

    BASE_URL = "https://www.investing.com/news"

    def __init__(self) -> None:
        self.session = create_cf_session(headers={
            "Accept": "text/html,application/xhtml+xml",
            "Referer": "https://www.investing.com/",
        })

    def fetch_news(
        self,
        category: str = "latest-news",
        *,
        page: int = 1,
    ) -> list[ScrapedNewsItem]:
        """Fetch news articles for *category* (e.g. 'economy-news'), page >= 1."""
        url = f"{self.BASE_URL}/{category}" if page <= 1 else f"{self.BASE_URL}/{category}/{page}"
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return self._parse_news_html(response.text, category)

    def fetch_all_news(
        self,
        category: str = "latest-news",
        *,
        max_pages: int = 3,
    ) -> list[ScrapedNewsItem]:
        """Paginate through up to *max_pages* of a news category."""
        all_items: list[ScrapedNewsItem] = []
        for p in range(1, max_pages + 1):
            items = self.fetch_news(category, page=p)
            all_items.extend(items)
            if not items:
                break
            if p < max_pages:
                time.sleep(1.5)
        return all_items

    def _parse_news_html(self, html: str, category: str) -> list[ScrapedNewsItem]:
        soup = BeautifulSoup(html, "html.parser")
        items: list[ScrapedNewsItem] = []
        seen_urls: set[str] = set()

        # Rich format: articles with data-test="article-item"
        for art in soup.find_all("article", {"data-test": "article-item"}):
            try:
                link = art.find("a", {"data-test": "article-title-link"})
                if not link:
                    continue
                title = link.get_text(strip=True)
                href = link.get("href", "")
                if not title or href in seen_urls:
                    continue
                seen_urls.add(href)

                desc_p = art.find("p", {"data-test": "article-description"})
                description = desc_p.get_text(strip=True) if desc_p else ""

                time_el = art.find("time", {"data-test": "article-publish-date"})
                published_at = time_el.get("datetime", "") if time_el else ""

                provider = art.find("span", {"data-test": "news-provider-name"})
                author = provider.get_text(strip=True) if provider else ""

                # Extract comment count if visible
                comment_count = 0
                comment_el = art.find("span", {"data-test": "article-comments"})
                if comment_el:
                    m = re.search(r"(\d+)", comment_el.get_text())
                    if m:
                        comment_count = int(m.group(1))

                # Extract category from URL path
                url_category = self._category_from_url(href)

                raw: dict[str, object] = {}
                if comment_count:
                    raw["comments"] = comment_count

                items.append(ScrapedNewsItem(
                    source="investing",
                    title=title,
                    url=href,
                    published_at=published_at,
                    description=description,
                    author=author,
                    category=url_category or category,
                    raw_json=raw,
                ))
            except Exception:
                continue

        # Fallback: simpler article format (articleItem)
        if not items:
            for art in soup.find_all("article", {"class": "js-article-item"}):
                try:
                    link = art.find("a", {"class": "title"}) or art.find("a")
                    if not link:
                        continue
                    title = link.get_text(strip=True)
                    href = link.get("href", "")
                    if not title or href in seen_urls:
                        continue
                    seen_urls.add(href)

                    data_id = art.get("data-id", "")
                    url_category = self._category_from_url(href)

                    items.append(ScrapedNewsItem(
                        source="investing",
                        title=title,
                        url=href,
                        category=url_category or category,
                        raw_json={"data_id": data_id} if data_id else {},
                    ))
                except Exception:
                    continue

        return items

    @staticmethod
    def _category_from_url(url: str) -> str:
        m = re.search(r"/news/([^/]+)/", url)
        return m.group(1) if m else ""
