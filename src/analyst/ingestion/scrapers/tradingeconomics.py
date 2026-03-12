"""TradingEconomics scraper – calendar, news stream, indicators, and market quotes."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import Any

from bs4 import BeautifulSoup

from analyst.ingestion.http_transport import create_cf_session
from analyst.storage import StoredEventRecord

from ._common import (
    IMPORTANCE_MAP,
    OPEN_UTC_PLUS_8,
    ScrapedIndicator,
    ScrapedMarketQuote,
    ScrapedNewsItem,
    categorize_event,
    generate_event_id,
    parse_numeric_value,
    to_epoch,
)

TE_COUNTRY_MAP = {
    "united states": "US", "china": "CN", "japan": "JP", "germany": "DE",
    "united kingdom": "UK", "france": "FR", "canada": "CA", "australia": "AU",
    "new zealand": "NZ", "switzerland": "CH", "singapore": "SG", "south korea": "KR",
    "india": "IN", "brazil": "BR", "mexico": "MX", "indonesia": "ID",
    "italy": "IT", "spain": "ES", "netherlands": "NL", "turkey": "TR",
    "euro area": "EU", "european union": "EU", "hong kong": "HK",
    "saudi arabia": "SA", "south africa": "ZA", "russia": "RU",
    "sweden": "SE", "norway": "NO", "denmark": "DK", "poland": "PL",
    "taiwan": "TW", "thailand": "TH", "malaysia": "MY", "philippines": "PH",
    "vietnam": "VN", "colombia": "CO", "chile": "CL", "argentina": "AR",
    "nigeria": "NG", "egypt": "EG", "israel": "IL", "austria": "AT",
    "belgium": "BE", "ireland": "IE", "portugal": "PT", "greece": "GR",
    "finland": "FI", "czech republic": "CZ", "romania": "RO", "hungary": "HU",
}

TE_SLUG_MAP = {
    "united-states": "US", "china": "CN", "japan": "JP", "germany": "DE",
    "united-kingdom": "UK", "france": "FR", "canada": "CA", "australia": "AU",
    "new-zealand": "NZ", "switzerland": "CH", "singapore": "SG",
    "euro-area": "EU", "india": "IN", "brazil": "BR", "mexico": "MX",
    "south-korea": "KR", "russia": "RU", "italy": "IT", "spain": "ES",
}


class TradingEconomicsCalendarClient:
    BASE_URL = "https://tradingeconomics.com/calendar"

    IMPORTANCE_LEVELS = {"3": "high", "2": "medium", "1": "low"}

    def __init__(self) -> None:
        self.session = create_cf_session(headers={
            "Accept": "text/html,application/xhtml+xml",
        })

    def fetch(self) -> list[StoredEventRecord]:
        """Fetch calendar with per-event importance by making 3 requests."""
        all_events: list[StoredEventRecord] = []
        for level, importance in self.IMPORTANCE_LEVELS.items():
            events = self._fetch_importance_level(level, importance)
            all_events.extend(events)
            if level != "1":
                time.sleep(1.0)
        return all_events

    def _fetch_importance_level(
        self, level: str, importance: str,
    ) -> list[StoredEventRecord]:
        self.session.cookies.set("calendar-importance", level, domain="tradingeconomics.com")
        response = self.session.get(self.BASE_URL, timeout=30)
        response.raise_for_status()
        return self._parse_calendar_html(response.text, importance)

    def _parse_calendar_html(self, html: str, importance: str) -> list[StoredEventRecord]:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", {"id": "calendar"})
        if table is None:
            return []
        events: list[StoredEventRecord] = []
        seen_ids: set[str] = set()
        current_date = datetime.now(OPEN_UTC_PLUS_8).strftime("%Y-%m-%d")
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 9:
                # Date header row
                th = row.find("th")
                if th:
                    date_text = th.get_text(strip=True)
                    if date_text and "Actual" not in date_text:
                        parsed_date = self._parse_header_date(date_text)
                        if parsed_date:
                            current_date = parsed_date
                continue
            try:
                # Extract indicator name and period
                ind_cell = cells[4]
                event_link = ind_cell.find("a", {"class": "calendar-event"})
                if event_link:
                    indicator = event_link.get_text(strip=True)
                else:
                    span = ind_cell.find("span")
                    indicator = span.get_text(strip=True) if span else ind_cell.get_text(strip=True)
                if not indicator:
                    continue
                period_span = ind_cell.find("span", {"class": "calendar-reference"})
                period = period_span.get_text(strip=True) if period_span else ""

                # Country from data attribute or cell text
                data_country = row.get("data-country", "")
                country = TE_COUNTRY_MAP.get(data_country.lower(), cells[1].get_text(strip=True).upper())

                # Time and date
                time_cell = cells[0]
                event_time = time_cell.get_text(strip=True) or "00:00"
                date_class = time_cell.get("class", [])
                if date_class and date_class[0] not in ("", "calendar-item"):
                    current_date = date_class[0]

                # Values
                actual = self._clean_cell_text(cells[5])
                previous = self._clean_cell_text(cells[6])
                consensus = self._clean_cell_text(cells[7])
                te_forecast = self._clean_cell_text(cells[8])

                timestamp = to_epoch(date_value=current_date, time_value=event_time)
                data_id = row.get("data-id", "")
                event_id = data_id or generate_event_id(country, indicator, timestamp)

                if event_id in seen_ids:
                    continue
                seen_ids.add(event_id)

                data_category = row.get("data-category", "")
                data_symbol = row.get("data-symbol", "")

                events.append(
                    StoredEventRecord(
                        source="tradingeconomics",
                        event_id=str(event_id),
                        timestamp=timestamp,
                        country=country,
                        indicator=f"{indicator} ({period})" if period else indicator,
                        category=categorize_event(indicator),
                        importance=importance,
                        actual=actual,
                        forecast=consensus,
                        previous=previous,
                        surprise=self._compute_surprise(actual, consensus),
                        raw_json={
                            "te_forecast": te_forecast,
                            "data_category": data_category,
                            "data_symbol": data_symbol,
                            "period": period,
                            "time": event_time,
                            "data_country": data_country,
                        },
                    )
                )
            except Exception:
                continue
        return events

    def _parse_header_date(self, text: str) -> str | None:
        """Parse 'Monday March 09 2026' -> '2026-03-09'."""
        for fmt in ("%A %B %d %Y", "%a %B %d %Y", "%A %b %d %Y"):
            try:
                return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _clean_cell_text(self, cell: Any) -> str | None:
        if cell is None:
            return None
        value = cell.get_text(strip=True)
        return value if value and value != "\xa0" else None

    def _compute_surprise(self, actual: str | None, forecast: str | None) -> float | None:
        actual_value = parse_numeric_value(actual)
        forecast_value = parse_numeric_value(forecast)
        if actual_value is None or forecast_value is None:
            return None
        return round(actual_value - forecast_value, 4)


class TradingEconomicsNewsClient:
    """Fetches the news stream from TradingEconomics via its internal JSON API."""

    STREAM_URL = "https://tradingeconomics.com/ws/stream.ashx"

    def __init__(self) -> None:
        self.session = create_cf_session(headers={
            "Accept": "application/json, text/javascript, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://tradingeconomics.com/news",
        })

    def fetch_news(self, *, start: int = 0, count: int = 20) -> list[ScrapedNewsItem]:
        """Fetch *count* news items from the TE stream beginning at *start*."""
        response = self.session.get(
            self.STREAM_URL,
            params={"start": str(start), "size": str(count)},
            timeout=30,
        )
        response.raise_for_status()
        return self._parse_stream_json(response.text)

    def fetch_all_news(self, *, max_items: int = 100, batch_size: int = 20) -> list[ScrapedNewsItem]:
        """Paginate through the TE stream until *max_items* are collected."""
        all_items: list[ScrapedNewsItem] = []
        offset = 0
        while len(all_items) < max_items:
            batch = self.fetch_news(start=offset, count=batch_size)
            if not batch:
                break
            all_items.extend(batch)
            offset += len(batch)
            if len(batch) < batch_size:
                break
            if len(all_items) < max_items:
                time.sleep(1.0)
        return all_items[:max_items]

    def _parse_stream_json(self, text: str) -> list[ScrapedNewsItem]:
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return []
        if not isinstance(data, list):
            return []
        items: list[ScrapedNewsItem] = []
        for entry in data:
            try:
                title = entry.get("title", "")
                if not title:
                    continue
                url_path = entry.get("url", "")
                full_url = (
                    f"https://tradingeconomics.com{url_path}"
                    if url_path.startswith("/")
                    else url_path
                )
                importance_val = entry.get("importance", 0)
                importance = IMPORTANCE_MAP.get(importance_val, "")

                image_url = entry.get("image") or entry.get("thumbnail") or ""

                items.append(ScrapedNewsItem(
                    source="tradingeconomics",
                    title=title,
                    url=full_url,
                    published_at=entry.get("date", ""),
                    description=entry.get("description", ""),
                    author=entry.get("author", ""),
                    category=entry.get("category", ""),
                    importance=importance,
                    image_url=image_url,
                    raw_json={
                        "id": entry.get("ID"),
                        "country": entry.get("country", ""),
                        "expiration": entry.get("expiration", ""),
                        "html": entry.get("html"),
                        "type": entry.get("type"),
                    },
                ))
            except Exception:
                continue
        return items


class TradingEconomicsIndicatorsClient:
    """Scrapes the indicators overview for a country from TradingEconomics."""

    BASE_URL = "https://tradingeconomics.com"

    def __init__(self) -> None:
        self.session = create_cf_session(headers={
            "Accept": "text/html,application/xhtml+xml",
        })

    def fetch_indicators(self, country: str = "united-states") -> list[ScrapedIndicator]:
        """Fetch all indicator tables for *country* (URL slug, e.g. 'japan')."""
        url = f"{self.BASE_URL}/{country}/indicators"
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return self._parse_indicators_html(response.text, country)

    def _parse_indicators_html(self, html: str, country_slug: str) -> list[ScrapedIndicator]:
        soup = BeautifulSoup(html, "html.parser")
        country_code = TE_SLUG_MAP.get(country_slug, country_slug.upper()[:2])
        indicators: list[ScrapedIndicator] = []

        for table in soup.find_all("table", {"class": "table"}):
            native_category = self._extract_table_category(table)

            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue
                try:
                    link = row.find("a")
                    name = cells[0].get_text(strip=True)
                    if not name:
                        continue
                    last = cells[1].get_text(strip=True)
                    previous = cells[2].get_text(strip=True)
                    highest = cells[3].get_text(strip=True)
                    lowest = cells[4].get_text(strip=True)
                    unit = cells[5].get_text(strip=True) if len(cells) > 5 else ""
                    date = cells[6].get_text(strip=True) if len(cells) > 6 else ""
                    href = link.get("href", "") if link else ""
                    full_url = (
                        f"{self.BASE_URL}{href}"
                        if href.startswith("/")
                        else href
                    )

                    indicators.append(ScrapedIndicator(
                        source="tradingeconomics",
                        country=country_code,
                        name=name,
                        last=last,
                        previous=previous,
                        highest=highest,
                        lowest=lowest,
                        unit=unit,
                        date=date,
                        url=full_url,
                        category=native_category or categorize_event(name),
                    ))
                except Exception:
                    continue

        return indicators

    @staticmethod
    def _extract_table_category(table: Any) -> str:
        """Derive category from the page's native tab-pane structure or a preceding heading."""
        # Check parent/grandparent for a tab-pane id (e.g. <div id="gdp">)
        for ancestor in (table.parent, getattr(table.parent, "parent", None)):
            if ancestor is None:
                continue
            pane_id = ancestor.get("id") if hasattr(ancestor, "get") else None
            if pane_id:
                return pane_id.lower()
        # Fallback: preceding heading sibling
        prev = table.find_previous_sibling(["h2", "h3", "h4"])
        if prev:
            return prev.get_text(strip=True).lower()
        return ""


class TradingEconomicsMarketsClient:
    """Scrapes market overview tables from the TradingEconomics news/home page."""

    BASE_URL = "https://tradingeconomics.com/news"

    ASSET_CLASS_MAP = {
        "Commodity": "commodity",
        "FX": "fx",
        "Index": "index",
        "Share": "stock",
        "Bond": "bond",
        "Crypto": "crypto",
    }

    def __init__(self) -> None:
        self.session = create_cf_session(headers={
            "Accept": "text/html,application/xhtml+xml",
        })

    def fetch_markets(self) -> list[ScrapedMarketQuote]:
        """Fetch the market overview snapshot from the TE news page sidebar."""
        response = self.session.get(self.BASE_URL, timeout=30)
        response.raise_for_status()
        return self._parse_markets_html(response.text)

    def _parse_markets_html(self, html: str) -> list[ScrapedMarketQuote]:
        soup = BeautifulSoup(html, "html.parser")
        quotes: list[ScrapedMarketQuote] = []

        for table in soup.find_all("table", {"class": "table-condensed"}):
            # Determine asset class from parent tab-pane id
            pane = table.parent
            pane_id = pane.get("id", "") if pane else ""
            asset_class = self.ASSET_CLASS_MAP.get(pane_id, pane_id.lower())

            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue
                try:
                    name = cells[0].get_text(strip=True)
                    if not name:
                        continue
                    price = cells[1].get_text(strip=True)
                    change = cells[2].get_text(strip=True)
                    change_pct = cells[3].get_text(strip=True)
                    link = cells[0].find("a")
                    href = link.get("href", "") if link else ""
                    full_url = (
                        f"https://tradingeconomics.com{href}"
                        if href.startswith("/")
                        else href
                    )

                    data_symbol = row.get("data-symbol", "")
                    raw: dict[str, Any] = {}
                    data_decimals = row.get("data-decimals", "")
                    if data_decimals:
                        raw["decimals"] = data_decimals

                    quotes.append(ScrapedMarketQuote(
                        source="tradingeconomics",
                        name=name,
                        asset_class=asset_class,
                        price=price,
                        change=change,
                        change_pct=change_pct,
                        url=full_url,
                        symbol=data_symbol,
                        raw_json=raw,
                    ))
                except Exception:
                    continue

        return quotes
