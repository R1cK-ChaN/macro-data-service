from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from contracts import format_epoch_iso

from .base import (
    LocalMacroDataServiceBase,
    _NEWS_PRESETS,
    _detect_article_domain,
    logger,
)


class NewsOpsMixin(LocalMacroDataServiceBase):
    def _op_refresh_news(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "news refresh unavailable"}
        category = arguments.get("category")
        return dict(self._ingestion.refresh_news(category=category))

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

    def _op_get_trends(self, arguments: dict[str, Any]) -> dict[str, Any]:
        limit = min(max(int(arguments.get("limit", 10)), 1), 20)
        hours = min(max(int(arguments.get("hours", 48)), 1), 168)
        category = (arguments.get("category") or "").strip() or None
        region = (arguments.get("region") or "").strip() or None
        topics = self._store.list_active_trends(
            limit=limit,
            hours=hours,
            category=category,
            region=region,
        )
        return {
            "timestamp": format_epoch_iso(int(datetime.now(timezone.utc).timestamp())),
            "total": len(topics),
            "topics": [self._trend_to_dict(topic) for topic in topics],
        }

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
        from rag.models import MacroMode

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

    def _op_fetch_article(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from ingestion.news_fetcher import ArticleFetcher
        from ingestion.scrapers import (
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
        from ingestion.scrapers import (
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

    def _trend_to_dict(self, trend: Any) -> dict[str, Any]:
        return {
            "topic": trend.topic,
            "summary": trend.summary,
            "keywords": list(trend.keywords),
            "category": trend.category,
            "region": trend.region,
            "popularity_score": round(float(trend.popularity_score), 2),
            "observed_at": format_epoch_iso(int(trend.observed_at)),
            "expires_at": format_epoch_iso(int(trend.expires_at)),
        }
