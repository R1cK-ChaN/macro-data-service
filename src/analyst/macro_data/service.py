from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import logging
from typing import Any
from urllib.parse import urlparse

from analyst.contracts import format_epoch_iso

logger = logging.getLogger(__name__)

_NEWS_PRESETS: dict[str, tuple[str, ...]] = {
    "all": ("investing", "forexfactory", "tradingeconomics", "reuters", "bloomberg", "ft", "wsj"),
    "premium": ("bloomberg", "ft", "wsj", "reuters"),
    "free": ("investing", "forexfactory", "tradingeconomics"),
}
_VALID_MARKET_ASSET_CLASSES = {"index", "commodity", "fx", "bond", "stock", "crypto"}
_VALID_RATE_TYPES = {"sofr", "effr", "obfr", "all"}
_ARTICLE_DOMAIN_MAP: dict[str, str] = {
    "bloomberg.com": "bloomberg",
    "ft.com": "ft",
    "wsj.com": "wsj",
    "reuters.com": "reuters",
}


def _detect_article_domain(url: str) -> str | None:
    hostname = urlparse(url).hostname or ""
    for domain, key in _ARTICLE_DOMAIN_MAP.items():
        if hostname == domain or hostname.endswith("." + domain):
            return key
    return None


class LocalMacroDataService:
    def __init__(
        self,
        *,
        store: Any,
        ingestion: Any | None = None,
        retriever: Any | None = None,
    ) -> None:
        self._store = store
        self._ingestion = ingestion
        self._retriever = retriever

    def invoke(self, operation: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        handler = getattr(self, f"_op_{operation}", None)
        if handler is None:
            raise KeyError(f"unknown macro-data operation: {operation}")
        return handler(arguments or {})

    def _op_refresh_all_sources(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        if self._ingestion is None:
            return {"error": "refresh unavailable"}
        return dict(self._ingestion.refresh_all())

    def _op_run_schedule(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        if self._ingestion is None:
            return {"error": "schedule unavailable"}
        self._ingestion.run_schedule()
        return {"scheduled": True}

    def _op_refresh_calendar(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        if self._ingestion is None:
            return {"error": "calendar refresh unavailable"}
        return dict(self._ingestion.refresh_calendar())

    def _op_refresh_news(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "news refresh unavailable"}
        category = arguments.get("category")
        return dict(self._ingestion.refresh_news(category=category))

    def _op_get_recent_releases(self, arguments: dict[str, Any]) -> dict[str, Any]:
        events = self._store.list_recent_events(
            limit=int(arguments.get("limit", 10)),
            days=int(arguments.get("days", 7)),
            released_only=True,
            importance=arguments.get("importance"),
            country=arguments.get("country"),
            category=arguments.get("category"),
        )
        return {"events": [self._event_to_dict(event) for event in events]}

    def _op_get_latest_released_event(self, arguments: dict[str, Any]) -> dict[str, Any]:
        event = self._store.latest_released_event(
            indicator_keyword=arguments.get("indicator_keyword"),
        )
        return {"event": self._event_to_dict(event) if event is not None else None}

    def _op_get_upcoming_calendar(self, arguments: dict[str, Any]) -> dict[str, Any]:
        events = self._store.list_upcoming_events(limit=int(arguments.get("limit", 10)))
        return {"events": [self._event_to_dict(event) for event in events]}

    def _op_get_market_snapshot(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        prices = self._store.latest_market_prices()
        return {"prices": [self._price_to_dict(price) for price in prices]}

    def _op_get_recent_fed_comms(self, arguments: dict[str, Any]) -> dict[str, Any]:
        communications = self._store.list_recent_central_bank_comms(
            days=int(arguments.get("days", 14)),
            limit=int(arguments.get("limit", 5)),
        )
        return {"communications": [self._comm_to_dict(item) for item in communications]}

    def _op_get_fed_communications(self, arguments: dict[str, Any]) -> dict[str, Any]:
        speaker = (arguments.get("speaker") or "").strip() or None
        content_type = (arguments.get("content_type") or "").strip() or None
        days = min(int(arguments.get("days", 14)), 60)
        limit = min(int(arguments.get("limit", 5)), 15)
        comms = self._store.list_recent_central_bank_comms(
            source="fed",
            limit=limit,
            days=days,
            speaker=speaker,
            content_type=content_type,
        )
        return {
            "total": len(comms),
            "days": days,
            "communications": [self._comm_to_dict(item) for item in comms],
        }

    def _op_get_indicator_history(self, arguments: dict[str, Any]) -> dict[str, Any]:
        series_id = (arguments.get("series_id") or "").strip()
        if not series_id:
            return {"error": "series_id is required", "observations": []}
        limit = min(int(arguments.get("limit", 12)), 36)
        observations = self._store.get_indicator_history(series_id, limit=limit)
        items = [
            {
                "series_id": observation.series_id,
                "date": observation.date,
                "value": observation.value,
                "source": observation.source,
                "metadata": getattr(observation, "metadata", {}),
            }
            for observation in observations
        ]
        return {"series_id": series_id, "total": len(items), "observations": items}

    def _op_get_today_calendar(self, arguments: dict[str, Any]) -> dict[str, Any]:
        events = self._store.list_today_events(
            importance=arguments.get("importance"),
            country=arguments.get("country"),
            category=arguments.get("category"),
        )
        return {"events": [self._event_to_dict(event) for event in events]}

    def _op_get_indicator_trend(self, arguments: dict[str, Any]) -> dict[str, Any]:
        keyword = str(arguments["indicator_keyword"])
        limit = int(arguments.get("limit", 12))
        events = self._store.list_indicator_releases(indicator_keyword=keyword, limit=limit)
        return {
            "indicator_keyword": keyword,
            "releases": [self._event_to_dict(event) for event in events],
        }

    def _op_get_surprise_summary(self, arguments: dict[str, Any]) -> dict[str, Any]:
        days = int(arguments.get("days", 14))
        events = self._store.list_recent_events(limit=200, days=days, released_only=True)
        by_category: dict[str, list[float]] = {}
        for event in events:
            if event.surprise is not None:
                by_category.setdefault(event.category, []).append(float(event.surprise))
        summary = []
        for category, surprises in sorted(by_category.items()):
            beats = sum(1 for item in surprises if item > 0)
            misses = sum(1 for item in surprises if item < 0)
            avg = round(sum(surprises) / len(surprises), 4) if surprises else 0.0
            summary.append({
                "category": category,
                "count": len(surprises),
                "beats": beats,
                "misses": misses,
                "avg_surprise": avg,
            })
        return {"summary": summary}

    def _op_get_recent_news(self, arguments: dict[str, Any]) -> dict[str, Any]:
        articles = self._store.get_news_context(
            days=int(arguments.get("days", 3)),
            limit=int(arguments.get("limit", 15)),
            impact_level=arguments.get("impact_level"),
            feed_category=arguments.get("feed_category"),
            finance_category=arguments.get("finance_category"),
            country=arguments.get("country"),
            asset_class=arguments.get("asset_class"),
            display_timezone=arguments.get("timezone"),
        )
        return {"articles": articles}

    def _op_search_news(self, arguments: dict[str, Any]) -> dict[str, Any]:
        query = (arguments.get("query") or "").strip() or None
        days = min(int(arguments.get("days", 7)), 30)
        limit = min(int(arguments.get("limit", 10)), 25)
        articles = self._store.get_news_context(
            query=query,
            days=days,
            limit=limit,
            impact_level=(arguments.get("impact_level") or "").strip() or None,
            feed_category=(arguments.get("feed_category") or "").strip() or None,
            finance_category=(arguments.get("finance_category") or "").strip() or None,
            country=(arguments.get("country") or "").strip() or None,
            asset_class=(arguments.get("asset_class") or "").strip() or None,
            display_timezone=(arguments.get("timezone") or "").strip() or None,
        )
        return {"total": len(articles), "days": days, "articles": articles}

    def _op_search_knowledge_base(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._retriever is None:
            return {
                "error": "knowledge base unavailable",
                "evidences": [],
                "stats": {"total_candidates": 0, "fused": 0, "final_k": 0, "coverage": {}, "coverage_ok": False, "timing_ms": 0},
            }
        from analyst.rag.models import MacroMode

        query = str(arguments.get("query") or "")
        if not query.strip():
            return {"error": "query is required"}
        mode_str = str(arguments.get("mode") or "QA").upper()
        try:
            mode = MacroMode(mode_str)
        except ValueError:
            mode = MacroMode.QA
        filters: dict[str, Any] = {}
        for key in ("country", "indicator_group", "impact_level", "content_type", "source_type"):
            value = arguments.get(key)
            if value:
                filters[key] = [value] if isinstance(value, str) else value
        days = arguments.get("days")
        if days:
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
            filters["updated_after"] = cutoff.isoformat()
        limit = arguments.get("limit")
        result = self._retriever.retrieve(
            query,
            mode,
            filters=filters,
            limit=int(limit) if limit else None,
        )
        evidences = []
        for evidence in result.get("evidences", []):
            evidences.append({
                "chunk_id": evidence.chunk_id,
                "text": evidence.text,
                "source_type": evidence.source_type,
                "source_id": evidence.source_id,
                "section_path": evidence.section_path,
                "content_type": evidence.content_type,
                "country": evidence.country,
                "indicator_group": evidence.indicator_group,
                "impact_level": evidence.impact_level,
                "data_source": evidence.data_source,
                "updated_at": evidence.updated_at,
                "scores": evidence.scores,
            })
        return {
            "evidences": evidences,
            "stats": {
                "total_candidates": result.get("candidates_total", 0),
                "fused": result.get("deduped_total", 0),
                "final_k": result.get("final_k", 0),
                "coverage": result.get("coverage_counts", {}),
                "coverage_ok": result.get("coverage_ok", False),
                "timing_ms": result.get("timing_ms", 0),
            },
        }

    def _op_fetch_live_calendar(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from analyst.ingestion.scrapers import (
            ForexFactoryCalendarClient,
            InvestingCalendarClient,
            TradingEconomicsCalendarClient,
        )

        source = (arguments.get("source") or "all").lower()
        importance_filter = arguments.get("importance")
        country_filter = arguments.get("country")
        sources = ("investing", "forexfactory", "tradingeconomics") if source == "all" else (source,)
        all_events: list[Any] = []
        errors: list[str] = []
        for item in sources:
            try:
                if item == "investing":
                    all_events.extend(InvestingCalendarClient().fetch())
                elif item == "forexfactory":
                    all_events.extend(ForexFactoryCalendarClient().fetch())
                elif item == "tradingeconomics":
                    all_events.extend(TradingEconomicsCalendarClient().fetch())
            except Exception as exc:
                logger.warning("Live fetch from %s failed: %s", item, exc)
                errors.append(f"{item}: {exc}")
        for event in all_events:
            try:
                self._store.upsert_calendar_event(event)
            except Exception:
                logger.warning("Failed to persist live calendar event %s", getattr(event, "event_id", ""), exc_info=True)
        filtered = all_events
        if importance_filter:
            filtered = [event for event in filtered if event.importance == importance_filter]
        if country_filter:
            filtered = [event for event in filtered if event.country == str(country_filter).upper()]
        result: dict[str, Any] = {
            "total_fetched": len(all_events),
            "returned": len(filtered),
            "events": [self._event_to_dict(event) for event in filtered],
        }
        if errors:
            result["errors"] = errors
        return result

    def _op_fetch_live_news(self, arguments: dict[str, Any]) -> dict[str, Any]:
        raw_sources = (arguments.get("sources") or "all").lower().strip()
        section = arguments.get("section") or "markets"
        limit = min(int(arguments.get("limit", 10)), 25)
        sources = _NEWS_PRESETS.get(raw_sources)
        if sources is None:
            sources = tuple(item.strip() for item in raw_sources.split(",") if item.strip())
        all_items: list[dict[str, Any]] = []
        errors: list[str] = []
        for source in sources:
            try:
                all_items.extend(self._fetch_live_news_source(source, section=section, limit=limit))
            except Exception as exc:
                logger.warning("Live news fetch from %s failed: %s", source, exc)
                errors.append(f"{source}: {exc}")
        result: dict[str, Any] = {
            "sources_requested": list(sources),
            "total": len(all_items),
            "items": all_items,
        }
        if errors:
            result["errors"] = errors
        return result

    def _op_fetch_live_markets(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from analyst.ingestion.scrapers import TradingEconomicsMarketsClient

        asset_class = (arguments.get("asset_class") or "all").lower().strip()
        try:
            quotes = TradingEconomicsMarketsClient().fetch_markets()
        except Exception as exc:
            logger.warning("Live markets fetch failed: %s", exc)
            return {"error": str(exc), "quotes": []}
        items = [
            {
                "name": quote.name,
                "asset_class": quote.asset_class,
                "price": quote.price,
                "change": quote.change,
                "change_pct": quote.change_pct,
                "symbol": quote.symbol,
            }
            for quote in quotes
        ]
        if asset_class != "all" and asset_class in _VALID_MARKET_ASSET_CLASSES:
            items = [quote for quote in items if str(quote["asset_class"]).lower() == asset_class]
        return {"total": len(items), "asset_class_filter": asset_class, "quotes": items}

    def _op_fetch_country_indicators(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from analyst.ingestion.scrapers import TradingEconomicsIndicatorsClient

        country = (arguments.get("country") or "united-states").lower().strip()
        category_filter = (arguments.get("category") or "").lower().strip()
        limit = min(int(arguments.get("limit", 50)), 100)
        try:
            indicators = TradingEconomicsIndicatorsClient().fetch_indicators(country=country)
        except Exception as exc:
            logger.warning("Live indicators fetch failed for %s: %s", country, exc)
            return {"error": str(exc), "indicators": []}
        items = [
            {
                "name": indicator.name,
                "last": indicator.last,
                "previous": indicator.previous,
                "highest": indicator.highest,
                "lowest": indicator.lowest,
                "unit": indicator.unit,
                "date": indicator.date,
                "category": indicator.category,
            }
            for indicator in indicators
        ]
        if category_filter:
            items = [
                item for item in items
                if category_filter in str(item["category"]).lower() or category_filter in str(item["name"]).lower()
            ]
        return {"country": country, "total": len(items[:limit]), "indicators": items[:limit]}

    def _op_fetch_reference_rates(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from analyst.ingestion.scrapers import NYFedRatesClient

        rate_type = (arguments.get("rate_type") or "all").lower().strip()
        last_n = min(int(arguments.get("last_n", 3)), 10)
        if rate_type not in _VALID_RATE_TYPES:
            return {"error": f"Invalid rate_type '{rate_type}'. Use: sofr, effr, obfr, or all", "rates": []}
        try:
            client = NYFedRatesClient()
            if rate_type == "sofr":
                rates = client.fetch_sofr(last_n=last_n)
            elif rate_type == "effr":
                rates = client.fetch_effr(last_n=last_n)
            elif rate_type == "obfr":
                rates = client.fetch_obfr(last_n=last_n)
            else:
                rates = client.fetch_all_rates(last_n=last_n)
        except Exception as exc:
            logger.warning("Live rates fetch failed: %s", exc)
            return {"error": str(exc), "rates": []}
        return {
            "rate_type": rate_type,
            "total": len(rates),
            "rates": [
                {
                    "date": rate.date,
                    "type": rate.type,
                    "rate": rate.rate,
                    "percentile_1": rate.percentile_1,
                    "percentile_25": rate.percentile_25,
                    "percentile_75": rate.percentile_75,
                    "percentile_99": rate.percentile_99,
                    "volume_billions": rate.volume_billions,
                    "target_rate_from": rate.target_rate_from,
                    "target_rate_to": rate.target_rate_to,
                }
                for rate in rates
            ],
        }

    def _op_fetch_rate_expectations(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from analyst.ingestion.scrapers import RateProbabilityClient

        include_history = bool(arguments.get("include_history", False))
        try:
            result = RateProbabilityClient().fetch_probabilities()
        except Exception as exc:
            logger.warning("Rate expectations fetch failed: %s", exc)
            return {"error": str(exc)}
        output: dict[str, Any] = {
            "as_of": result.as_of,
            "current_band": result.current_band,
            "midpoint": result.midpoint,
            "effr": result.effr,
            "meetings": [
                {
                    "meeting_date": meeting.meeting_date,
                    "implied_rate": meeting.implied_rate,
                    "prob_move_pct": meeting.prob_move_pct,
                    "is_cut": meeting.is_cut,
                    "num_moves": meeting.num_moves,
                    "change_bps": meeting.change_bps,
                }
                for meeting in result.meetings
            ],
        }
        if include_history and result.snapshots:
            output["snapshots"] = {
                label: [
                    {
                        "meeting_date": meeting.meeting_date,
                        "implied_rate": meeting.implied_rate,
                        "prob_move_pct": meeting.prob_move_pct,
                        "is_cut": meeting.is_cut,
                        "num_moves": meeting.num_moves,
                        "change_bps": meeting.change_bps,
                    }
                    for meeting in meetings
                ]
                for label, meetings in result.snapshots.items()
            }
        return output

    def _op_fetch_article(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from analyst.ingestion.news_fetcher import ArticleFetcher
        from analyst.ingestion.scrapers import (
            BloombergArticleClient,
            FTArticleClient,
            ReutersArticleClient,
            WSJArticleClient,
        )

        url = str(arguments.get("url", "")).strip()
        if not url:
            return {"error": "url is required", "fetched": False}
        max_chars = min(int(arguments.get("max_chars", 6000)), 12000)
        domain_key = _detect_article_domain(url)
        try:
            if domain_key == "bloomberg":
                with BloombergArticleClient() as client:
                    article = client.fetch_article(url)
                if not article.fetched:
                    return {"error": article.error or "fetch failed", "fetched": False}
                return self._article_response(
                    source="bloomberg",
                    article=article,
                    max_chars=max_chars,
                    extra={"lede": article.lede},
                )
            if domain_key == "ft":
                with FTArticleClient() as client:
                    article = client.fetch_article(url)
                if not article.fetched:
                    return {"error": article.error or "fetch failed", "fetched": False}
                return self._article_response(
                    source="ft",
                    article=article,
                    max_chars=max_chars,
                    extra={"standfirst": article.standfirst},
                )
            if domain_key == "wsj":
                with WSJArticleClient() as client:
                    article = client.fetch_article(url)
                if not article.fetched:
                    return {"error": article.error or "fetch failed", "fetched": False}
                return self._article_response(
                    source="wsj",
                    article=article,
                    max_chars=max_chars,
                    extra={"dek": article.dek},
                )
            if domain_key == "reuters":
                article = ReutersArticleClient().fetch_article(url)
                if not article.fetched:
                    return {"error": article.error or "fetch failed", "fetched": False}
                return self._article_response(source="reuters", article=article, max_chars=max_chars, extra={})
            article = ArticleFetcher(timeout=20, max_content_chars=15_000).fetch_article(url, rss_description="")
            if not article.fetched:
                return {"error": article.error or "fetch failed", "fetched": False}
            content = article.content[:max_chars]
            return {
                "source": "generic",
                "title": getattr(article, "title", ""),
                "content": content,
                "content_length": len(content),
                "truncated": len(article.content) > max_chars,
                "fetched": True,
            }
        except Exception as exc:
            logger.warning("fetch_article failed for %s: %s", url, exc)
            return {"error": str(exc), "fetched": False}

    def _fetch_live_news_source(self, source: str, *, section: str, limit: int) -> list[dict[str, Any]]:
        from analyst.ingestion.scrapers import (
            BloombergNewsClient,
            FTNewsClient,
            ForexFactoryNewsClient,
            InvestingNewsClient,
            ReutersNewsClient,
            TradingEconomicsNewsClient,
            WSJNewsClient,
        )

        if source == "investing":
            raw = InvestingNewsClient().fetch_news(category=section)[:limit]
        elif source == "forexfactory":
            raw = ForexFactoryNewsClient().fetch_news()[:limit]
        elif source == "tradingeconomics":
            raw = TradingEconomicsNewsClient().fetch_news(count=limit)
        elif source == "reuters":
            raw = ReutersNewsClient().fetch_news(section=section)[:limit]
        elif source == "bloomberg":
            with BloombergNewsClient() as client:
                raw = client.fetch_news(section=section)[:limit]
        elif source == "ft":
            with FTNewsClient() as client:
                raw = client.fetch_news(section=section)[:limit]
        elif source == "wsj":
            with WSJNewsClient() as client:
                raw = client.fetch_news(section=section)[:limit]
        else:
            return []
        return [
            {
                "source": item.source,
                "title": item.title,
                "url": item.url,
                "published_at": item.published_at,
                "description": item.description[:200] if item.description else "",
                "category": item.category,
                "importance": item.importance,
            }
            for item in raw
        ]

    def _article_response(
        self,
        *,
        source: str,
        article: Any,
        max_chars: int,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        content = article.content[:max_chars]
        payload = {
            "source": source,
            "title": article.title,
            "authors": article.authors,
            "published_at": article.published_at,
            "keywords": article.keywords,
            "content": content,
            "content_length": len(content),
            "truncated": len(article.content) > max_chars,
            "fetched": True,
        }
        payload.update(extra)
        return payload

    def _event_to_dict(self, event: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "source": event.source,
            "event_id": event.event_id,
            "timestamp": event.timestamp,
            "datetime_utc": format_epoch_iso(event.timestamp),
            "country": event.country,
            "indicator": event.indicator,
            "category": event.category,
            "importance": event.importance,
            "actual": event.actual,
            "forecast": event.forecast,
            "previous": event.previous,
            "surprise": event.surprise,
        }
        indicator_id = getattr(event, "indicator_id", "")
        if indicator_id:
            payload["indicator_id"] = indicator_id
        return payload

    def _price_to_dict(self, price: Any) -> dict[str, Any]:
        return {
            "symbol": price.symbol,
            "name": price.name,
            "asset_class": price.asset_class,
            "price": price.price,
            "change_pct": price.change_pct,
            "timestamp": price.timestamp,
            "datetime_utc": format_epoch_iso(price.timestamp),
        }

    def _comm_to_dict(self, communication: Any) -> dict[str, Any]:
        summary = communication.summary
        if len(summary) > 800:
            summary = summary[:800] + "..."
        return {
            "title": communication.title,
            "url": communication.url,
            "timestamp": communication.timestamp,
            "published_at": format_epoch_iso(communication.timestamp),
            "speaker": communication.speaker,
            "content_type": communication.content_type,
            "summary": summary,
        }
