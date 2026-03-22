from __future__ import annotations

import concurrent.futures
import dataclasses
import hashlib
import math
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Callable, Iterable

import feedparser
import logging
import requests
import yfinance as yf
from bs4 import BeautifulSoup

from contracts import format_epoch_iso, normalize_utc_iso, to_epoch_ms
from env import get_env_value
from ingestion.news._classify import Deduplicator
from ingestion.news._extract import extract_news_metadata
from ingestion.news._config import get_feeds
from ingestion.news._fetcher import ArticleFetcher
from ingestion._shared.url_canon import canonicalize_url, content_hash
from ingestion.calendar.scrapers.forexfactory import ForexFactoryCalendarClient
from ingestion.calendar.scrapers.investing import InvestingCalendarClient
from ingestion.calendar.scrapers.tradingeconomics import TradingEconomicsCalendarClient
from ingestion.documents.scrapers.gov_report import GovReportClient, GovReportItem
from ingestion.timeseries.sdmx.providers.bis import BISClient
from ingestion.timeseries.sdmx.providers.ecb import ECBClient
from ingestion.timeseries.scrapers.eia import EIAClient
from ingestion.timeseries.sdmx.providers.eurostat import EurostatClient
from ingestion.timeseries.scrapers.fred import FredClient
from ingestion.timeseries.sdmx.providers.imf import IMFClient
from ingestion.timeseries.sdmx._errors import OECDRateLimitError
from ingestion.timeseries.sdmx.providers.oecd import OECDClient
from ingestion.trends.scrapers.reddit import RedditTrendClient, RedditTrendPost
from ingestion.trends.scrapers.weibo import WeiboTrendClient, WeiboTrendItem
from ingestion.timeseries.scrapers.worldbank import WorldBankClient, WorldBankRateLimitError
from ingestion.timeseries.scrapers.nyfed import NYFedRatesClient
from ingestion.timeseries.scrapers.rateprobability import RateProbabilityClient
from ingestion.timeseries.scrapers.treasury_fiscal import TreasuryFiscalClient

logger = logging.getLogger(__name__)
from storage import (
    CentralBankCommunicationRecord,
    DocumentBlobRecord,
    DocumentExtraRecord,
    DocumentRecord,
    IndicatorObservationRecord,
    IndicatorVintageRecord,
    MarketPriceRecord,
    NewsArticleRecord,
    ReleaseStatusRecord,
    SQLiteEngineStore,
    StoredEventRecord,
    TrendTopicRecord,
)


def _infer_publish_precision(value: str | None) -> str:
    if not value:
        return "estimated"
    if re.search(r"[T ]\d{1,2}:\d{2}", value):
        return "exact"
    return "date_only"

from ingestion.timeseries._config import (  # noqa: E402
    BEA_DATASETS,
    BEA_KNOWN_DATASETS,
    BIS_SERIES,
    BLS_SERIES,
    BLS_SURVEY_PREFIXES,
    CENSUS_CATALOG_BASELINE,
    CENSUS_DATASETS,
    CENSUS_KNOWN_DATASETS,
    ECB_SERIES,
    EIA_KNOWN_ROUTES,
    EIA_SERIES,
    EUROSTAT_SERIES,
    ILO_SERIES,
    IMF_SERIES,
    IMF_VINTAGE_SERIES,
    MACRO_SERIES,
    OECD_SERIES,
    OECDSeriesConfig,
    TREASURY_DATASETS,
    UNSD_SERIES,
    VINTAGE_SERIES,
    WORLDBANK_SERIES,
    WorldBankSeriesConfig,
    _generated_oecd_config_key,
    _generated_oecd_series_id,
    _generated_wb_config_key,
    _generated_wb_series_id,
    _slugify_oecd_token,
    _slugify_wb_token,
    render_oecd_series_configs,
    render_wb_series_configs,
)
from ingestion.documents._config import FED_FEEDS, FED_SPEAKERS  # noqa: E402
from ingestion.market._config import MACRO_WATCHLIST  # noqa: E402


def extract_speaker(title: str) -> str:
    for speaker in FED_SPEAKERS:
        if speaker.lower() in title.lower():
            return speaker
    return ""


@dataclass(frozen=True)
class RefreshStats:
    source: str
    count: int


@dataclass(frozen=True)
class IngestionRunReport:
    source: str
    stored: int
    fetched: int | None = None
    normalized: int | None = None
    validated: int | None = None
    deduplicated: int | None = None
    duration_ms: int = 0
    retries: int = 0
    error: str = ""
    validation_report: Any = None

    def to_counts(self) -> dict[str, int]:
        return {self.source: self.stored}

    def to_dict(self) -> dict[str, Any]:
        payload = dataclasses.asdict(self)
        payload["ok"] = not self.error
        if self.validation_report is not None:
            payload["validation"] = self.validation_report.to_dict()
        else:
            payload.pop("validation_report", None)
        return payload


@dataclass(frozen=True)
class IngestionSourceDefinition:
    name: str
    interval_seconds: int | None = None
    prepare: Callable[[], None] | None = None
    fetch: Callable[[], Iterable[Any]] | None = None
    normalize: Callable[[list[Any]], Iterable[Any]] | None = None
    validate: Callable[[list[Any]], Iterable[Any]] | None = None
    deduplicate: Callable[[list[Any]], Iterable[Any]] | None = None
    store: Callable[[list[Any]], int | None] | None = None
    execute: Callable[[], int] | None = None
    max_retries: int = 0
    retry_backoff_seconds: float = 0.0



# ── Ingestion clients (extracted to ingestion/clients/) ───────────
from ingestion.clients._fred import FREDIngestionClient
from ingestion.clients._eia import EIAIngestionClient
from ingestion.clients._treasury import TreasuryFiscalIngestionClient
from ingestion.clients._sdmx_clients import (
    IMFIngestionClient,
    EurostatIngestionClient,
    BISIngestionClient,
    ECBIngestionClient,
)
from ingestion.clients._oecd_client import OECDIngestionClient, _OECDRateLimiter
from ingestion.clients._ilo_unsd import ILOIngestionClient, UNSDIngestionClient
from ingestion.clients._worldbank_client import WorldBankIngestionClient, _WorldBankRateLimiter
from ingestion.clients._fed import FedIngestionClient
from ingestion.clients._market import MarketPriceClient
from ingestion.clients._trends import (
    RawNewsEntry,
    PreparedNewsRecord,
    RedditTrendSourceConfig,
    RawTrendEntry,
    PreparedTrendRecord,
    RedditTrendIngestionClient,
    WeiboTrendIngestionClient,
)
from ingestion.clients._news import NewsIngestionClient
from ingestion.clients._gov_report import GovReportIngestionClient


class IngestionOrchestrator:
    def __init__(
        self,
        store: SQLiteEngineStore,
        *,
        fred: FREDIngestionClient | None = None,
        investing: InvestingCalendarClient | None = None,
        forexfactory: ForexFactoryCalendarClient | None = None,
        tradingeconomics: TradingEconomicsCalendarClient | None = None,
        fed: FedIngestionClient | None = None,
        market: MarketPriceClient | None = None,
        news: NewsIngestionClient | None = None,
        reddit_trends: RedditTrendIngestionClient | None = None,
        weibo_trends: WeiboTrendIngestionClient | None = None,
        rate_probability: RateProbabilityClient | None = None,
        nyfed: NYFedRatesClient | None = None,
        gov_report: GovReportIngestionClient | None = None,
        eia: EIAIngestionClient | None = None,
        treasury_fiscal: TreasuryFiscalIngestionClient | None = None,
        imf: IMFIngestionClient | None = None,
        eurostat: EurostatIngestionClient | None = None,
        bis: BISIngestionClient | None = None,
        ecb: ECBIngestionClient | None = None,
        oecd: OECDIngestionClient | None = None,
        worldbank: WorldBankIngestionClient | None = None,
        validation_engine: Any | None = None,
    ) -> None:
        self.store = store
        self._validation = validation_engine
        self.fred = fred or FREDIngestionClient()
        self.investing = investing or InvestingCalendarClient()
        self.forexfactory = forexfactory or ForexFactoryCalendarClient()
        self.tradingeconomics = tradingeconomics or TradingEconomicsCalendarClient()
        self.fed = fed or FedIngestionClient()
        self.market = market or MarketPriceClient()
        self.news = news or NewsIngestionClient()
        self.reddit_trends = reddit_trends or RedditTrendIngestionClient()
        self.weibo_trends = weibo_trends or WeiboTrendIngestionClient()
        self.rate_probability = rate_probability or RateProbabilityClient()
        self.nyfed = nyfed or NYFedRatesClient()
        self.gov_report = gov_report or GovReportIngestionClient()
        self.eia = eia or EIAIngestionClient()
        self.treasury_fiscal = treasury_fiscal or TreasuryFiscalIngestionClient()
        self.imf = imf or IMFIngestionClient()
        self.eurostat = eurostat or EurostatIngestionClient()
        self.bis = bis or BISIngestionClient()
        self.ecb = ecb or ECBIngestionClient()
        self.oecd = oecd or OECDIngestionClient()
        self.worldbank = worldbank or WorldBankIngestionClient()
        self._obs_seeded = False
        self._cal_seeded = False
        self._family_lookup: dict[tuple[str, str], str] = {}
        self._sources: dict[str, IngestionSourceDefinition] = {}
        self._default_refresh_order: list[str] = []
        self._last_run_reports: dict[str, IngestionRunReport] = {}
        self._register_default_sources()

    def _ensure_obs_seed(self) -> None:
        """Seed observation sources/families once, then build lookup cache."""
        if self._obs_seeded:
            return
        self.store.seed_obs_sources_and_families()
        self.store.backfill_obs_family_ids()
        self._family_lookup = self.store.build_obs_family_lookup()
        self._obs_seeded = True

    def _ensure_calendar_indicator_seed(self) -> None:
        if self._cal_seeded:
            return
        self.store.seed_calendar_indicators()
        self._cal_seeded = True

    def _ensure_capability_manager(self) -> None:
        if hasattr(self, "_capability_manager"):
            return
        from ingestion.source_capabilities import SourceCapabilityManager
        self._capability_manager = SourceCapabilityManager(
            self.store,
            orchestrator=self,
        )

    def _resolve_calendar_indicator(self, event: StoredEventRecord) -> StoredEventRecord:
        indicator_id = self.store.resolve_calendar_alias(
            event.indicator, event.source, event.country
        )
        if indicator_id:
            return dataclasses.replace(event, indicator_id=indicator_id)
        return event

    def register_source(self, definition: IngestionSourceDefinition) -> None:
        self._sources[definition.name] = definition

    def list_sources(self) -> list[str]:
        return list(self._sources)

    def last_run_report(self, source: str) -> IngestionRunReport | None:
        return self._last_run_reports.get(source)

    def run_source(self, source: str) -> IngestionRunReport:
        definition = self._sources.get(source)
        if definition is None:
            raise KeyError(f"unknown ingestion source: {source}")
        return self._run_definition(definition)

    def _register_default_sources(self) -> None:
        definitions = [
            self._build_calendar_source(),
            self._build_fed_source(),
            self._build_market_source(),
            self._build_fred_daily_source(),
            self._build_fred_nondaily_source(),
            self._build_fred_full_source(),
            self._build_news_source(),
            self._build_reddit_trends_source(),
            self._build_weibo_trends_source(),
            self._build_rate_probability_source(),
            self._build_fred_vintages_source(),
            self._build_nyfed_rates_source(),
            self._build_gov_reports_source(),
            self._build_eia_source(),
            self._build_treasury_fiscal_source(),
            self._build_imf_source(),
            self._build_imf_vintages_source(),
            self._build_eurostat_source(),
            self._build_bis_source(),
            self._build_ecb_source(),
            self._build_oecd_source(),
            self._build_worldbank_source(),
            self._build_worldbank_catalog_source(),
            self._build_bls_source(),
        ]
        for definition in definitions:
            self.register_source(definition)
        self._default_refresh_order = [
            "calendar",
            "fed",
            "market",
            "fred_daily",
            "news",
            "reddit_trends",
            "weibo_trends",
            "rate_probability",
            "nyfed_rates",
            "gov_reports",
            "eia",
            "treasury_fiscal",
            "imf",
            "imf_vintages",
            "eurostat",
            "bis",
            "ecb",
            "oecd",
            "worldbank",
            "worldbank_catalog",
            "bls",
            "fred_nondaily",
        ]

    @staticmethod
    def _materialize_items(items: Iterable[Any] | None) -> list[Any]:
        if items is None:
            return []
        if isinstance(items, list):
            return items
        return list(items)

    def _run_stage(
        self,
        stage: Callable[[list[Any]], Iterable[Any]] | None,
        items: list[Any],
    ) -> list[Any]:
        if stage is None:
            return items
        return self._materialize_items(stage(items))

    def _run_definition(self, definition: IngestionSourceDefinition) -> IngestionRunReport:
        attempt = 0
        while True:
            started = time.perf_counter()
            try:
                if definition.prepare is not None:
                    definition.prepare()

                if definition.execute is not None:
                    stored = int(definition.execute())
                    report = IngestionRunReport(
                        source=definition.name,
                        stored=stored,
                        duration_ms=int((time.perf_counter() - started) * 1000),
                        retries=attempt,
                    )
                    report = self._run_validation(definition.name, report)
                    self._last_run_reports[definition.name] = report
                    return report

                if definition.fetch is None or definition.store is None:
                    raise ValueError(f"source {definition.name} is missing pipeline stages")

                raw_items = self._materialize_items(definition.fetch())
                normalized_items = self._run_stage(definition.normalize, raw_items)
                validated_items = self._run_stage(definition.validate, normalized_items)
                deduplicated_items = self._run_stage(definition.deduplicate, validated_items)
                stored = definition.store(deduplicated_items)
                report = IngestionRunReport(
                    source=definition.name,
                    stored=int(stored if stored is not None else len(deduplicated_items)),
                    fetched=len(raw_items),
                    normalized=len(normalized_items),
                    validated=len(validated_items),
                    deduplicated=len(deduplicated_items),
                    duration_ms=int((time.perf_counter() - started) * 1000),
                    retries=attempt,
                )
                report = self._run_validation(definition.name, report)
                self._last_run_reports[definition.name] = report
                return report
            except Exception as exc:
                if attempt < definition.max_retries:
                    attempt += 1
                    if definition.retry_backoff_seconds > 0:
                        time.sleep(definition.retry_backoff_seconds * attempt)
                    continue
                logger.error("%s ingestion failed", definition.name, exc_info=True)
                report = IngestionRunReport(
                    source=definition.name,
                    stored=0,
                    duration_ms=int((time.perf_counter() - started) * 1000),
                    retries=attempt,
                    error=str(exc),
                )
                self._last_run_reports[definition.name] = report
                return report

    def _run_validation(self, source_name: str, report: IngestionRunReport) -> IngestionRunReport:
        if self._validation is None:
            return report
        try:
            val_report = self._validation.validate_post_store(source_name, report)
            return dataclasses.replace(report, validation_report=val_report)
        except Exception:
            logger.warning("%s post-store validation failed", source_name, exc_info=True)
            return report

    @staticmethod
    def _deduplicate_by_key(items: list[Any], key_fn: Callable[[Any], Any]) -> list[Any]:
        unique: list[Any] = []
        seen: set[Any] = set()
        for item in items:
            marker = key_fn(item)
            if marker in seen:
                continue
            seen.add(marker)
            unique.append(item)
        return unique

    # ── v2 pipeline: Fetcher → Normalizer → store ──────────────────

    def _ensure_normalizer(self) -> None:
        """Lazy-initialise the shared Normalizer (requires obs families to be seeded)."""
        if hasattr(self, "_normalizer"):
            return
        self._ensure_obs_seed()
        from ingestion.normalization import Normalizer
        families = self.store.list_obs_families(active_only=False)
        self._normalizer = Normalizer(
            family_lookup=Normalizer.build_family_lookup(families),
        )

    def _raw_series_to_records(
        self, raw_series_list: list[Any],
    ) -> list[IndicatorObservationRecord]:
        """Convert list[RawSeries] → list[IndicatorObservationRecord] via Normalizer."""
        self._ensure_normalizer()
        records: list[IndicatorObservationRecord] = []
        for rs in raw_series_list:
            for row in self._normalizer.normalize(rs):
                records.append(
                    IndicatorObservationRecord(
                        series_id=row.series_id,
                        source=row.source,
                        date=row.date,
                        value=row.value,
                        metadata={
                            k: v for k, v in row.metadata.items()
                            if k not in ("name",)
                        },
                        obs_family_id=row.obs_family_id,
                    )
                )
        return records

    def _build_fetcher_source(
        self,
        name: str,
        fetcher: Any,
        *,
        interval_seconds: int | None = 86_400,
        lookback_days: int = 365,
    ) -> IngestionSourceDefinition:
        """Create an IngestionSourceDefinition from a Fetcher adapter."""
        return IngestionSourceDefinition(
            name=name,
            interval_seconds=interval_seconds,
            prepare=self._ensure_obs_seed,
            fetch=lambda: fetcher.fetch(lookback_days=lookback_days),
            normalize=self._raw_series_to_records,
            deduplicate=self._deduplicate_observations,
            store=self._store_indicator_observations,
        )

    def _build_calendar_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="calendar",
            interval_seconds=3600,
            prepare=self._ensure_calendar_indicator_seed,
            fetch=self._fetch_calendar_events,
            normalize=self._normalize_calendar_events,
            validate=self._validate_calendar_events,
            deduplicate=self._deduplicate_calendar_events,
            store=self._store_calendar_events,
        )

    def _fetch_calendar_events(self) -> list[StoredEventRecord]:
        events: list[StoredEventRecord] = []
        providers = [
            ("Investing.com", lambda: self.investing.fetch_range(days_back=1, days_forward=3)),
            ("ForexFactory", self.forexfactory.fetch),
            ("TradingEconomics", self.tradingeconomics.fetch),
        ]
        for label, fetch_fn in providers:
            try:
                events.extend(list(fetch_fn()))
            except Exception:
                logger.warning("%s calendar refresh failed", label, exc_info=True)
        return events

    def _normalize_calendar_events(self, events: list[StoredEventRecord]) -> list[StoredEventRecord]:
        return [self._resolve_calendar_indicator(event) for event in events]

    @staticmethod
    def _validate_calendar_events(events: list[StoredEventRecord]) -> list[StoredEventRecord]:
        return [
            event for event in events
            if event.event_id and event.indicator and event.country and event.timestamp > 0
        ]

    def _deduplicate_calendar_events(self, events: list[StoredEventRecord]) -> list[StoredEventRecord]:
        return self._deduplicate_by_key(events, lambda event: (event.source, event.event_id))

    def _store_calendar_events(self, events: list[StoredEventRecord]) -> int:
        for event in events:
            self.store.upsert_calendar_event(event)
        return len(events)

    def _build_fed_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="fed",
            interval_seconds=14_400,
            fetch=self.fed.fetch_communications,
            validate=self._validate_fed_communications,
            deduplicate=self._deduplicate_fed_communications,
            store=lambda items: self.fed.store_communications(self.store, items),
        )

    @staticmethod
    def _validate_fed_communications(
        communications: list[CentralBankCommunicationRecord],
    ) -> list[CentralBankCommunicationRecord]:
        return [item for item in communications if item.title.strip() and item.url.strip()]

    def _deduplicate_fed_communications(
        self,
        communications: list[CentralBankCommunicationRecord],
    ) -> list[CentralBankCommunicationRecord]:
        return self._deduplicate_by_key(
            communications,
            lambda item: (item.url, item.timestamp, item.title),
        )

    def _build_market_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="market",
            interval_seconds=1800,
            fetch=self.market.fetch_prices,
            validate=self._validate_market_prices,
            deduplicate=self._deduplicate_market_prices,
            store=lambda items: self.market.store_prices(self.store, items),
        )

    @staticmethod
    def _validate_market_prices(prices: list[MarketPriceRecord]) -> list[MarketPriceRecord]:
        return [price for price in prices if price.symbol and price.price > 0]

    def _deduplicate_market_prices(self, prices: list[MarketPriceRecord]) -> list[MarketPriceRecord]:
        return self._deduplicate_by_key(prices, lambda price: price.symbol)

    def _build_news_source(self, *, category: str | None = None) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="news",
            interval_seconds=300 if category is None else None,
            fetch=lambda: self.news.fetch_entries(category=category),
            normalize=self.news.normalize_entries,
            validate=self.news.validate_entries,
            deduplicate=lambda items: self.news.deduplicate_entries(self.store, items),
            store=lambda items: self.news.store_articles(self.store, items),
            max_retries=1,
            retry_backoff_seconds=1.0,
        )

    def _build_reddit_trends_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="reddit_trends",
            interval_seconds=3600,
            fetch=self.reddit_trends.fetch_entries,
            normalize=self.reddit_trends.normalize_entries,
            validate=self.reddit_trends.validate_entries,
            deduplicate=self.reddit_trends.deduplicate_entries,
            store=lambda items: self.reddit_trends.store_topics(self.store, items),
        )

    def _build_weibo_trends_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="weibo_trends",
            interval_seconds=3600,
            fetch=self.weibo_trends.fetch_entries,
            normalize=self.weibo_trends.normalize_entries,
            validate=self.weibo_trends.validate_entries,
            deduplicate=self.weibo_trends.deduplicate_entries,
            store=lambda items: self.weibo_trends.store_topics(self.store, items),
        )

    def _build_rate_probability_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="rate_probability",
            interval_seconds=3600,
            fetch=self._fetch_rate_probability_observations,
            deduplicate=self._deduplicate_observations,
            store=self._store_indicator_observations,
        )

    def _fetch_rate_probability_observations(self) -> list[IndicatorObservationRecord]:
        prob = self.rate_probability.fetch_probabilities()
        observations: list[IndicatorObservationRecord] = []
        for meeting in prob.meetings:
            observations.append(
                IndicatorObservationRecord(
                    series_id=f"FEDPROB_{meeting.meeting_date}",
                    source="rateprobability",
                    date=prob.as_of[:10] if len(prob.as_of) >= 10 else prob.as_of,
                    value=meeting.implied_rate,
                    metadata={
                        "prob_move_pct": meeting.prob_move_pct,
                        "is_cut": meeting.is_cut,
                        "num_moves": meeting.num_moves,
                        "change_bps": meeting.change_bps,
                        "current_band": prob.current_band,
                    },
                )
            )
        return observations

    def _build_fred_daily_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="fred_daily",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.fred.refresh_daily_series(
                self.store,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_fred_nondaily_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="fred_nondaily",
            interval_seconds=21_600,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.fred.refresh_nondaily_series(
                self.store,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_fred_full_source(self, *, lookback_days: int = 365) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="fred_full",
            interval_seconds=None,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.fred.refresh_all_series(
                self.store,
                lookback_days=lookback_days,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_fred_vintages_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="fred_vintages",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.fred.refresh_vintages(
                self.store,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_nyfed_rates_source(self) -> IngestionSourceDefinition:
        from ingestion.fetchers._nyfed import NYFedFetcher
        return self._build_fetcher_source(
            "nyfed_rates", NYFedFetcher(client=self.nyfed),
        )

    def _build_gov_reports_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="gov_reports",
            interval_seconds=21_600,
            fetch=self.gov_report.fetch_items,
            validate=self._validate_gov_report_items,
            deduplicate=self._deduplicate_gov_report_items,
            store=lambda items: self.gov_report.store_items(self.store, items),
        )

    @staticmethod
    def _validate_gov_report_items(items: list[GovReportItem]) -> list[GovReportItem]:
        return [item for item in items if item.title.strip() and item.url.strip() and item.source_id.strip()]

    def _deduplicate_gov_report_items(self, items: list[GovReportItem]) -> list[GovReportItem]:
        return self._deduplicate_by_key(items, lambda item: canonicalize_url(item.url))

    def _build_eia_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="eia",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.eia.refresh_subset(
                self.store,
                series_configs=EIA_SERIES,
                family_lookup=self._family_lookup or None,
            ).count,
            max_retries=1,
            retry_backoff_seconds=5.0,
        )

    def _build_treasury_fiscal_source(self) -> IngestionSourceDefinition:
        from ingestion.fetchers._treasury import TreasuryFetcher
        return self._build_fetcher_source(
            "treasury_fiscal", TreasuryFetcher(client=self.treasury_fiscal.client),
        )

    def _build_imf_source(self) -> IngestionSourceDefinition:
        from ingestion.fetchers._sdmx import SDMXFetcher
        return self._build_fetcher_source(
            "imf", SDMXFetcher(self.imf.client, "imf", IMF_SERIES),
        )

    def _build_imf_vintages_source(self) -> IngestionSourceDefinition:
        # Vintages use a specialised refresh path — keep the legacy client
        return IngestionSourceDefinition(
            name="imf_vintages",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.imf.refresh_vintages(
                self.store,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_eurostat_source(self) -> IngestionSourceDefinition:
        from ingestion.fetchers._eurostat import EurostatFetcher
        return self._build_fetcher_source(
            "eurostat", EurostatFetcher(client=self.eurostat.client),
        )

    def _build_bis_source(self) -> IngestionSourceDefinition:
        from ingestion.fetchers._sdmx import SDMXFetcher
        return self._build_fetcher_source(
            "bis", SDMXFetcher(self.bis.client, "bis", BIS_SERIES),
        )

    def _build_ecb_source(self) -> IngestionSourceDefinition:
        from ingestion.fetchers._sdmx import SDMXFetcher
        return self._build_fetcher_source(
            "ecb", SDMXFetcher(self.ecb.client, "ecb", ECB_SERIES),
        )

    def _build_oecd_source(self) -> IngestionSourceDefinition:
        from ingestion.fetchers._oecd import OECDFetcher
        return self._build_fetcher_source(
            "oecd", OECDFetcher(client=self.oecd.client),
        )

    def _build_worldbank_source(self) -> IngestionSourceDefinition:
        from ingestion.fetchers._worldbank import WorldBankFetcher
        return self._build_fetcher_source(
            "worldbank", WorldBankFetcher(client=self.worldbank.client),
        )

    def _build_worldbank_catalog_source(self) -> IngestionSourceDefinition:
        def _execute() -> int:
            lookup = self._family_lookup or None
            return self.worldbank.refresh_catalog_parallel(
                self.store, family_lookup=lookup,
            ).count

        return IngestionSourceDefinition(
            name="worldbank_catalog",
            interval_seconds=86_400 * 7,
            prepare=self._ensure_obs_seed,
            execute=_execute,
        )

    def _build_bls_source(self) -> IngestionSourceDefinition:
        from ingestion.fetchers._bls import BLSFetcher
        return self._build_fetcher_source("bls", BLSFetcher())

    # ── Indicator-driven ingestion ───────────────────────────────

    _SOURCE_FETCHER_MAP: dict[str, str] = {
        "fred": "fred",
        "bls": "bls",
        "eia": "eia",
        "nyfed": "nyfed_rates",
        "treasury_fiscal": "treasury_fiscal",
        "imf": "imf",
        "eurostat": "eurostat",
        "bis": "bis",
        "ecb": "ecb",
        "oecd": "oecd",
        "worldbank": "worldbank",
    }

    def _get_fetcher_for_source(self, source_id: str) -> Any:
        """Return the fetcher adapter for a concept_map source_id."""
        from ingestion.fetchers._bls import BLSFetcher
        from ingestion.fetchers._eia import EIAFetcher
        from ingestion.fetchers._fred import FredFetcher
        from ingestion.fetchers._nyfed import NYFedFetcher
        from ingestion.fetchers._oecd import OECDFetcher
        from ingestion.fetchers._sdmx import SDMXFetcher
        from ingestion.fetchers._treasury import TreasuryFetcher
        from ingestion.fetchers._worldbank import WorldBankFetcher

        fetcher_cache = getattr(self, "_fetcher_cache", None)
        if fetcher_cache is None:
            self._fetcher_cache: dict[str, Any] = {}
            fetcher_cache = self._fetcher_cache

        if source_id in fetcher_cache:
            return fetcher_cache[source_id]

        fetcher: Any
        if source_id == "fred":
            fetcher = FredFetcher(client=self.fred.client)
        elif source_id == "bls":
            fetcher = BLSFetcher()
        elif source_id == "eia":
            fetcher = EIAFetcher(client=self.eia.client)
        elif source_id == "nyfed":
            fetcher = NYFedFetcher(client=self.nyfed)
        elif source_id == "treasury_fiscal":
            fetcher = TreasuryFetcher(client=self.treasury_fiscal.client)
        elif source_id == "imf":
            fetcher = SDMXFetcher(self.imf.client, "imf", IMF_SERIES)
        elif source_id == "eurostat":
            from ingestion.fetchers._eurostat import EurostatFetcher
            fetcher = EurostatFetcher(client=self.eurostat.client)
        elif source_id == "bis":
            fetcher = SDMXFetcher(self.bis.client, "bis", BIS_SERIES)
        elif source_id == "ecb":
            fetcher = SDMXFetcher(self.ecb.client, "ecb", ECB_SERIES)
        elif source_id == "oecd":
            fetcher = OECDFetcher(client=self.oecd.client)
        elif source_id == "worldbank":
            fetcher = WorldBankFetcher(client=self.worldbank.client)
        else:
            return None
        fetcher_cache[source_id] = fetcher
        return fetcher

    def refresh_indicator(
        self,
        concept_id: str,
        *,
        lookback_days: int = 365 * 3,
    ) -> IngestionRunReport:
        """Fetch, normalize, and store all series for a single concept."""
        started = time.perf_counter()
        self._ensure_obs_seed()
        self.store.seed_concept_map()

        mappings = self.store.get_concept_series(concept_id)
        if not mappings:
            return IngestionRunReport(
                source=f"indicator:{concept_id}",
                stored=0,
                error=f"concept '{concept_id}' not found in concept_map",
            )

        all_raw: list[Any] = []
        errors: list[str] = []

        for m in mappings:
            fetcher = self._get_fetcher_for_source(m.source_id)
            if fetcher is None:
                errors.append(f"{m.source_id}: no fetcher available")
                continue
            try:
                rs = fetcher.fetch_series(m.provider_series_id, lookback_days=lookback_days)
                if rs is not None and rs.observations:
                    all_raw.append(rs)
                else:
                    errors.append(f"{m.source_id}/{m.provider_series_id}: no data returned")
            except Exception as exc:
                errors.append(f"{m.source_id}/{m.provider_series_id}: {exc}")

        if not all_raw:
            return IngestionRunReport(
                source=f"indicator:{concept_id}",
                stored=0,
                fetched=0,
                duration_ms=int((time.perf_counter() - started) * 1000),
                error="; ".join(errors) if errors else "no data from any source",
            )

        records = self._raw_series_to_records(all_raw)
        deduped = self._deduplicate_observations(records)
        stored = self._store_indicator_observations(deduped)

        report = IngestionRunReport(
            source=f"indicator:{concept_id}",
            stored=stored,
            fetched=sum(len(rs.observations) for rs in all_raw),
            normalized=len(records),
            deduplicated=len(deduped),
            duration_ms=int((time.perf_counter() - started) * 1000),
            error="; ".join(errors) if errors else "",
        )
        return report

    def _deduplicate_observations(
        self,
        observations: list[IndicatorObservationRecord],
    ) -> list[IndicatorObservationRecord]:
        return self._deduplicate_by_key(
            observations,
            lambda observation: (observation.source, observation.series_id, observation.date),
        )

    def _store_indicator_observations(self, observations: list[IndicatorObservationRecord]) -> int:
        self._run_sanity_checks(observations)
        for observation in observations:
            self.store.upsert_indicator_observation(observation)
        return len(observations)

    def _run_sanity_checks(self, observations: list[IndicatorObservationRecord]) -> None:
        """Pre-store sanity check: flag values that deviate from recent history."""
        from ingestion.validation._anomaly import check_value_sanity

        # Group by (source, series_id), check only the newest observation per series
        latest_by_series: dict[tuple[str, str], IndicatorObservationRecord] = {}
        for obs in observations:
            key = (obs.source, obs.series_id)
            if key not in latest_by_series or obs.date > latest_by_series[key].date:
                latest_by_series[key] = obs

        for (source, series_id), obs in latest_by_series.items():
            try:
                history = self.store.get_indicator_history(series_id, limit=20)
                recent_values = [h.value for h in history if h.source == source]
                if len(recent_values) < 5:
                    continue
                check_value_sanity(
                    obs.value, series_id, source, recent_values,
                )
            except Exception:
                pass  # sanity checks must never block ingestion

    def refresh_calendar(self) -> dict[str, int]:
        return self.run_source("calendar").to_counts()

    def refresh_market(self) -> dict[str, int]:
        return self.run_source("market").to_counts()

    def refresh_fed(self) -> dict[str, int]:
        return self.run_source("fed").to_counts()

    def refresh_fred_daily(self) -> dict[str, int]:
        return self.run_source("fred_daily").to_counts()

    def refresh_fred_full(self, *, lookback_days: int = 365) -> dict[str, int]:
        if lookback_days == 365:
            return self.run_source("fred_full").to_counts()
        return self._run_definition(self._build_fred_full_source(lookback_days=lookback_days)).to_counts()

    def refresh_news(self, *, category: str | None = None) -> dict[str, int]:
        if category is None:
            return self.run_source("news").to_counts()
        return self._run_definition(self._build_news_source(category=category)).to_counts()

    def refresh_rate_probability(self) -> dict[str, int]:
        return self.run_source("rate_probability").to_counts()

    def refresh_fred_vintages(self) -> dict[str, int]:
        return self.run_source("fred_vintages").to_counts()

    def refresh_eia(self) -> dict[str, int]:
        return self.run_source("eia").to_counts()

    def refresh_treasury_fiscal(self) -> dict[str, int]:
        return self.run_source("treasury_fiscal").to_counts()

    def refresh_imf(self) -> dict[str, int]:
        return self.run_source("imf").to_counts()

    def refresh_imf_vintages(self) -> dict[str, int]:
        return self.run_source("imf_vintages").to_counts()

    def refresh_eurostat(self) -> dict[str, int]:
        return self.run_source("eurostat").to_counts()

    def refresh_bis(self) -> dict[str, int]:
        return self.run_source("bis").to_counts()

    def refresh_ecb(self) -> dict[str, int]:
        return self.run_source("ecb").to_counts()

    def refresh_oecd(self) -> dict[str, int]:
        return self.run_source("oecd").to_counts()

    def refresh_worldbank(self) -> dict[str, int]:
        return self.run_source("worldbank").to_counts()

    def refresh_worldbank_catalog(self) -> dict[str, int]:
        return self.run_source("worldbank_catalog").to_counts()

    def refresh_bls(self) -> dict[str, int]:
        return self.run_source("bls").to_counts()

    def refresh_gov_reports(self) -> dict[str, int]:
        return self.run_source("gov_reports").to_counts()

    def refresh_nyfed_rates(self) -> dict[str, int]:
        return self.run_source("nyfed_rates").to_counts()

    def refresh_all(self) -> dict[str, int]:
        results: dict[str, int] = {}
        for source in self._default_refresh_order:
            results.update(self.run_source(source).to_counts())
        return results

    def list_source_capabilities(self, *, include_internal: bool = True) -> list[dict[str, Any]]:
        self._ensure_capability_manager()
        return self._capability_manager.list_capabilities(include_internal=include_internal)

    def sync_catalog_discovery(
        self,
        source_id: str,
        *,
        query: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        self._ensure_capability_manager()
        return self._capability_manager.sync_discovery(
            source_id,
            query=query,
            limit=limit,
        )

    def list_catalog_entities(
        self,
        source_id: str,
        *,
        query: str | None = None,
        limit: int = 100,
        refresh: bool = False,
    ) -> dict[str, Any]:
        self._ensure_capability_manager()
        return self._capability_manager.list_entities(
            source_id,
            query=query,
            limit=limit,
            refresh=refresh,
        )

    def get_catalog_structure(
        self,
        source_id: str,
        entity_id: str,
    ) -> dict[str, Any]:
        self._ensure_capability_manager()
        return self._capability_manager.get_structure(source_id, entity_id)

    def sync_catalog_latest(
        self,
        source_id: str,
        *,
        entity_ids: list[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        self._ensure_capability_manager()
        return self._capability_manager.sync_latest(
            source_id,
            entity_ids=entity_ids,
            limit=limit,
        )

    def get_catalog_status(self, source_id: str | None = None, *, include_internal: bool = True) -> dict[str, Any]:
        self._ensure_capability_manager()
        return self._capability_manager.get_status(source_id, include_internal=include_internal)

    def get_source_health_dashboard(self, *, include_internal: bool = False) -> dict[str, Any]:
        self._ensure_capability_manager()
        return self._capability_manager.get_customer_health(include_internal=include_internal)

    def run_schedule(self, *, poll_interval_seconds: int = 60) -> None:
        jobs = {
            name: definition
            for name, definition in self._sources.items()
            if definition.interval_seconds is not None
        }
        next_run = {name: 0.0 for name in jobs}
        self.refresh_all()
        while True:
            now = time.time()
            for job_name, job in jobs.items():
                if now >= next_run[job_name]:
                    self.run_source(job_name)
                    next_run[job_name] = now + float(job.interval_seconds or 0)
            time.sleep(poll_interval_seconds)

    # ── Release-calendar-aware scheduler ──────────────────────────────

    def run_release_schedule(
        self,
        *,
        poll_interval_seconds: int = 300,
        due_window_minutes: int = 120,
    ) -> None:
        """Release-calendar-aware scheduler loop with retry."""
        self._ensure_obs_seed()
        self.store.seed_concept_map()
        self.store.seed_release_schedules()
        self._recompute_all_next_expected()

        logger.info("release scheduler started  poll=%ds  window=%dm",
                     poll_interval_seconds, due_window_minutes)
        while True:
            # Phase 1: process newly-due concepts
            reports = self._refresh_due_concepts(window_minutes=due_window_minutes)
            for r in reports:
                status = "OK" if not r.error else "ERROR"
                logger.info("  %s  %s  stored=%d", r.source, status, r.stored)
            # Phase 2: process pending retries
            retry_reports = self._process_retries()
            for r in retry_reports:
                status = "OK" if not r.error else "ERROR"
                logger.info("  retry %s  %s  stored=%d", r.source, status, r.stored)
            time.sleep(poll_interval_seconds)

    def _recompute_all_next_expected(self) -> int:
        """Recompute and persist next_expected for all active schedules."""
        from ingestion.release_schedule import next_expected_release
        import json as _json

        schedules = self.store.list_release_schedules(is_active=True)
        updated = 0
        for s in schedules:
            rule = s.rule_json if isinstance(s.rule_json, dict) else _json.loads(s.rule_json)
            nxt = next_expected_release(s.rule_type, rule)
            if nxt is not None:
                self.store.update_release_timestamps(
                    s.concept_id, next_expected=nxt.isoformat(),
                )
                updated += 1
        logger.info("recomputed next_expected for %d/%d schedules", updated, len(schedules))
        return updated

    def _refresh_due_concepts(
        self, *, window_minutes: int = 120,
    ) -> list[IngestionRunReport]:
        """Check due concepts, fetch, verify availability, schedule retries."""
        from ingestion.release_schedule import (
            next_expected_release, expected_reference_period,
            is_data_fresh, compute_next_retry,
            STATUS_PENDING, STATUS_WAITING, STATUS_FETCHED,
            STATUS_CONFIRMED, STATUS_STALE,
        )
        from datetime import timezone as _tz
        import json as _json

        now = datetime.now(_tz.utc)
        due_before = (now + timedelta(minutes=window_minutes)).isoformat()
        due = self.store.list_release_schedules(due_before=due_before)

        reports: list[IngestionRunReport] = []
        for schedule in due:
            release_date = schedule.next_expected
            self.store.update_release_timestamps(
                schedule.concept_id, last_checked=now.isoformat(),
            )

            # Create initial status record
            exp_period = expected_reference_period(schedule.frequency, reference=now)
            self.store.upsert_release_status(ReleaseStatusRecord(
                concept_id=schedule.concept_id,
                release_date=release_date,
                status=STATUS_PENDING,
                expected_period=exp_period,
                created_at=now.isoformat(),
                updated_at=now.isoformat(),
            ))

            # Attempt fetch
            report = self._fetch_with_availability(
                schedule.concept_id, schedule.frequency,
                release_date, exp_period, attempt=0,
            )
            reports.append(report)

            # Recompute next_expected regardless of outcome
            rule = (schedule.rule_json if isinstance(schedule.rule_json, dict)
                    else _json.loads(schedule.rule_json))
            nxt = next_expected_release(schedule.rule_type, rule)
            ts_kwargs: dict[str, str] = {}
            if nxt is not None:
                ts_kwargs["next_expected"] = nxt.isoformat()
            if report.stored and report.stored > 0:
                ts_kwargs["last_released"] = now.isoformat()
            if ts_kwargs:
                self.store.update_release_timestamps(schedule.concept_id, **ts_kwargs)

        return reports

    def _fetch_with_availability(
        self,
        concept_id: str,
        frequency: str,
        release_date: str,
        expected_period: str,
        attempt: int,
    ) -> IngestionRunReport:
        """Fetch data, check freshness, set status + schedule retry if needed."""
        from ingestion.release_schedule import (
            is_data_fresh, compute_next_retry,
            STATUS_WAITING, STATUS_FETCHED, STATUS_CONFIRMED,
            STATUS_STALE, STATUS_FAILED,
        )
        from datetime import timezone as _tz

        now = datetime.now(_tz.utc)

        try:
            report = self.refresh_indicator(concept_id)
        except Exception as exc:
            report = IngestionRunReport(
                source=f"indicator:{concept_id}",
                stored=0, error=str(exc),
            )

        # Determine latest observation date from resolved data
        resolved = self.store.resolve_indicator(concept_id)
        latest_date = resolved.date if resolved else ""
        source_used = resolved.source_id if resolved else ""
        is_provisional = (resolved.priority > 1) if resolved else False

        if report.error and report.stored == 0:
            # Fetch failed entirely — schedule retry
            nxt = compute_next_retry(attempt, reference=now)
            if nxt is not None:
                self.store.update_release_status(
                    concept_id, release_date,
                    status=STATUS_WAITING,
                    attempt_count=attempt + 1,
                    next_retry=nxt.isoformat(),
                    last_attempt=now.isoformat(),
                    error=report.error,
                )
            else:
                self.store.update_release_status(
                    concept_id, release_date,
                    status=STATUS_FAILED,
                    attempt_count=attempt + 1,
                    next_retry="",
                    last_attempt=now.isoformat(),
                    error=report.error,
                )
            return report

        # Check if data is fresh enough
        fresh = is_data_fresh(latest_date, frequency, reference=now)
        if fresh:
            # Data is available — check if from primary source
            if is_provisional:
                status = STATUS_FETCHED  # got data, but from fallback source
            else:
                status = STATUS_CONFIRMED  # primary source, fresh data
            self.store.update_release_status(
                concept_id, release_date,
                status=status,
                attempt_count=attempt + 1,
                next_retry="",
                last_attempt=now.isoformat(),
                source_used=source_used,
                data_date=latest_date,
                provisional=is_provisional,
                error="",
            )
        else:
            # Data fetched but not fresh — API hasn't updated yet
            nxt = compute_next_retry(attempt, reference=now)
            if nxt is not None:
                self.store.update_release_status(
                    concept_id, release_date,
                    status=STATUS_WAITING,
                    attempt_count=attempt + 1,
                    next_retry=nxt.isoformat(),
                    last_attempt=now.isoformat(),
                    source_used=source_used,
                    data_date=latest_date,
                    error="data not fresh yet",
                )
            else:
                self.store.update_release_status(
                    concept_id, release_date,
                    status=STATUS_STALE,
                    attempt_count=attempt + 1,
                    next_retry="",
                    last_attempt=now.isoformat(),
                    source_used=source_used,
                    data_date=latest_date,
                    error=f"stale after {attempt + 1} attempts",
                )

        return report

    def _process_retries(self) -> list[IngestionRunReport]:
        """Pick up WAITING/PENDING rows whose next_retry has arrived."""
        from datetime import timezone as _tz

        now = datetime.now(_tz.utc)
        pending = self.store.list_release_statuses(
            pending_retry_before=now.isoformat(),
        )

        reports: list[IngestionRunReport] = []
        for rs in pending:
            schedule = self.store.get_release_schedule(rs.concept_id)
            freq = schedule.frequency if schedule else "monthly"
            report = self._fetch_with_availability(
                rs.concept_id, freq,
                rs.release_date, rs.expected_period,
                attempt=rs.attempt_count,
            )
            reports.append(report)
        return reports

    def get_schedule_status(self) -> list[dict[str, Any]]:
        """Return schedule status for all concepts (for CLI --show)."""
        self.store.seed_release_schedules()
        self._recompute_all_next_expected()
        schedules = self.store.list_release_schedules()
        return [
            {
                "concept_id": s.concept_id,
                "frequency": s.frequency,
                "rule_type": s.rule_type,
                "next_expected": s.next_expected,
                "confidence": s.confidence,
                "last_released": s.last_released,
                "last_checked": s.last_checked,
                "is_active": s.is_active,
                "notes": s.notes,
            }
            for s in schedules
        ]

    def get_availability_status(self) -> list[dict[str, Any]]:
        """Return per-concept availability status (for CLI --status)."""
        self.store.seed_release_schedules()
        statuses = self.store.list_all_latest_release_statuses()
        schedules_by_id = {
            s.concept_id: s
            for s in self.store.list_release_schedules()
        }
        result: list[dict[str, Any]] = []
        for rs in statuses:
            sched = schedules_by_id.get(rs.concept_id)
            result.append({
                "concept_id": rs.concept_id,
                "status": rs.status,
                "release_date": rs.release_date,
                "data_date": rs.data_date,
                "source_used": rs.source_used,
                "provisional": rs.provisional,
                "attempt_count": rs.attempt_count,
                "next_retry": rs.next_retry,
                "error": rs.error,
                "frequency": sched.frequency if sched else "",
            })
        # Also add concepts with schedules but no status yet
        seen = {rs.concept_id for rs in statuses}
        for cid, sched in schedules_by_id.items():
            if cid not in seen:
                result.append({
                    "concept_id": cid,
                    "status": "NOT_RELEASED",
                    "release_date": sched.next_expected,
                    "data_date": "",
                    "source_used": "",
                    "provisional": False,
                    "attempt_count": 0,
                    "next_retry": "",
                    "error": "",
                    "frequency": sched.frequency,
                })
        result.sort(key=lambda x: x["concept_id"])
        return result

    def get_health(
        self, *, indicator: str | None = None,
    ) -> list[dict[str, Any]]:
        """One-table health view: per-source rows with status, freshness, retries."""
        from ingestion.release_schedule import is_data_fresh
        from datetime import timezone as _tz

        self._ensure_obs_seed()
        self.store.seed_concept_map()
        self.store.seed_release_schedules()

        now = datetime.now(_tz.utc)
        schedules_by_id = {
            s.concept_id: s for s in self.store.list_release_schedules()
        }

        if indicator:
            concepts = [indicator]
        else:
            concepts = sorted(schedules_by_id.keys())

        rows: list[dict[str, Any]] = []
        for cid in concepts:
            sched = schedules_by_id.get(cid)
            freq = sched.frequency if sched else "monthly"
            mappings = self.store.get_concept_series(cid)
            rs = self.store.get_latest_release_status(cid)

            for m in mappings:
                stats = self.store.get_series_stats(m.source_id, m.provider_series_id)
                latest_date = stats.get("max_date") or ""
                fresh = is_data_fresh(latest_date, freq, reference=now) if latest_date else False

                # Derive status from release_status + freshness
                if rs and rs.status in ("CONFIRMED", "FETCHED"):
                    status = rs.status
                elif rs and rs.status in ("WAITING", "PENDING"):
                    status = rs.status
                elif rs and rs.status in ("FAILED", "STALE"):
                    status = rs.status
                elif latest_date:
                    status = "CONFIRMED" if fresh else "STALE"
                else:
                    status = "NO_DATA"

                note = m.role
                if m.priority > 1 and rs and rs.source_used == m.source_id:
                    note = "fallback"

                rows.append({
                    "indicator": cid,
                    "source": m.source_id,
                    "status": status,
                    "freshness": "fresh" if fresh else ("stale" if latest_date else "none"),
                    "retries": rs.attempt_count if rs else 0,
                    "note": note,
                    "latest_date": latest_date,
                    "obs_count": stats.get("count", 0),
                })
        return rows

    def get_alerts(
        self,
        *,
        delay_minutes: int = 30,
        mismatch_threshold_pct: float = 1.0,
    ) -> list[dict[str, str]]:
        """Run the 3 alert checks: DELAY, FAILED, MISMATCH."""
        from ingestion.release_schedule import check_alerts, Alert
        from datetime import timezone as _tz

        now = datetime.now(_tz.utc)
        self.store.seed_concept_map()
        self.store.seed_release_schedules()

        statuses = self.store.list_all_latest_release_statuses()

        # Cross-source diffs: compare primary vs secondary for multi-source concepts
        cross_diffs: dict[str, float] = {}
        seen_concepts: set[str] = set()
        for rs in statuses:
            if rs.concept_id in seen_concepts:
                continue
            seen_concepts.add(rs.concept_id)
            mappings = self.store.get_concept_series(rs.concept_id)
            if len(mappings) < 2:
                continue
            # Get latest values from primary and secondary
            vals: list[tuple[str, float]] = []
            for m in mappings[:2]:  # just compare top 2
                stats = self.store.get_series_stats(m.source_id, m.provider_series_id)
                if stats.get("latest_value") is not None:
                    vals.append((m.source_id, stats["latest_value"]))
            if len(vals) == 2 and vals[0][1] != 0:
                diff_pct = ((vals[1][1] - vals[0][1]) / abs(vals[0][1])) * 100
                cross_diffs[rs.concept_id] = diff_pct

        alerts = check_alerts(
            statuses,
            cross_source_diffs=cross_diffs,
            now=now,
            delay_minutes=delay_minutes,
            mismatch_threshold_pct=mismatch_threshold_pct,
        )
        return [
            {
                "type": a.alert_type,
                "indicator": a.concept_id,
                "source": a.source,
                "message": a.message,
            }
            for a in alerts
        ]
