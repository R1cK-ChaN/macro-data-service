from __future__ import annotations

from typing import Any

from ingestion._shared.url_canon import canonicalize_url
from ingestion.documents.scrapers.gov_report import GovReportItem
from storage import CentralBankCommunicationRecord, IndicatorObservationRecord

from . import (
    AISI_WEEKLY_STEEL_SERIES,
    EIA_SERIES,
    IngestionSourceDefinition,
    ISM_REPORT_SERIES,
    MACRO_SERIES,
    REDBOOK_SERIES,
)


def _build_fed_source(self) -> IngestionSourceDefinition:
    return IngestionSourceDefinition(
        name="fed",
        interval_seconds=14_400,
        fetch=self.fed.fetch_communications,
        validate=self._validate_fed_communications,
        deduplicate=self._deduplicate_fed_communications,
        store=lambda items: self.fed.store_communications(self.store, items),
    )


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
    as_of = prob.as_of[:10] if len(prob.as_of) >= 10 else prob.as_of
    observations: list[IndicatorObservationRecord] = []
    if as_of and prob.midpoint is not None:
        observations.append(
            IndicatorObservationRecord(
                series_id="FEDWATCH_MIDPOINT",
                source="rateprobability",
                date=as_of,
                value=float(prob.midpoint),
                metadata={
                    "current_band": prob.current_band,
                    "effr": prob.effr,
                },
            )
        )
    for meeting in prob.meetings:
        observations.append(
            IndicatorObservationRecord(
                series_id=f"FEDPROB_{meeting.meeting_date}",
                source="rateprobability",
                date=as_of,
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
    from ingestion.fetchers._fred import FredFetcher

    daily_series = {
        sid: meta for sid, meta in MACRO_SERIES.items()
        if meta["freq"] == "daily"
    }
    return self._build_fetcher_source(
        "fred_daily",
        FredFetcher(
            client=self.fred.client,
            series_config=daily_series,
            limit=5,
            raise_on_error=True,
        ),
        lookback_days=7,
    )


def _build_fred_nondaily_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._fred import FredFetcher

    nondaily = {
        sid: meta for sid, meta in MACRO_SERIES.items()
        if meta["freq"] != "daily"
    }
    return self._build_fetcher_source(
        "fred_nondaily",
        FredFetcher(
            client=self.fred.client,
            series_config=nondaily,
            limit=10,
            request_delay_seconds=1.0,
            raise_on_error=True,
        ),
        interval_seconds=21_600,
        lookback_days=120,
    )


def _build_fred_full_source(
    self, *, lookback_days: int = 365,
) -> IngestionSourceDefinition:
    from ingestion.fetchers._fred import FredFetcher

    return self._build_fetcher_source(
        "fred_full",
        FredFetcher(client=self.fred.client, limit=100, raise_on_error=True),
        interval_seconds=None,
        lookback_days=lookback_days,
    )


def _build_fred_vintages_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._vintages import FredVintageFetcher

    return self._build_vintage_fetcher_source(
        "fred_vintages",
        FredVintageFetcher(client=self.fred.client, raise_on_error=False),
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


def _validate_gov_report_items(items: list[GovReportItem]) -> list[GovReportItem]:
    return [item for item in items if item.title.strip() and item.url.strip() and item.source_id.strip()]


def _deduplicate_gov_report_items(self, items: list[GovReportItem]) -> list[GovReportItem]:
    return self._deduplicate_by_key(items, lambda item: canonicalize_url(item.url))


def _build_eia_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._eia import EIAFetcher

    fetcher = EIAFetcher(
        client=self.eia.client,
        fred_client=self.eia._fred,
        series_config=EIA_SERIES,
        history_loader=lambda series_id, limit: self.store.get_indicator_history(
            series_id, limit=limit,
        ),
        live_limit=30,
        request_delay_seconds=0.5,
    )
    return IngestionSourceDefinition(
        name="eia",
        interval_seconds=86_400,
        prepare=self._ensure_obs_seed,
        fetch=lambda: self._fetch_with_obs_raw(fetcher, lookback_days=365),
        normalize=self._raw_eia_items_to_records,
        deduplicate=self._deduplicate_eia_items,
        store=self._store_eia_items,
        max_retries=1,
        retry_backoff_seconds=5.0,
    )


def _build_treasury_fiscal_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._treasury import TreasuryFetcher

    return self._build_fetcher_source(
        "treasury_fiscal", TreasuryFetcher(client=self.treasury_fiscal.client),
    )


def _build_aisi_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._aisi import AISIFetcher

    return self._build_fetcher_source(
        "aisi",
        AISIFetcher(client=self.aisi, series_config=AISI_WEEKLY_STEEL_SERIES),
    )


def _build_ism_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._ism import ISMFetcher

    return self._build_fetcher_source(
        "ism",
        ISMFetcher(client=self.ism, series_config=ISM_REPORT_SERIES),
    )


def _build_redbook_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._redbook import RedbookFetcher

    return self._build_fetcher_source(
        "redbook",
        RedbookFetcher(client=self.redbook, series_config=REDBOOK_SERIES),
    )


def _build_bls_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._bls import BLSFetcher

    return self._build_fetcher_source("bls", BLSFetcher())


SOURCE_METHODS: dict[str, Any] = {
    "_build_fed_source": _build_fed_source,
    "_validate_fed_communications": staticmethod(_validate_fed_communications),
    "_deduplicate_fed_communications": _deduplicate_fed_communications,
    "_build_news_source": _build_news_source,
    "_build_rate_probability_source": _build_rate_probability_source,
    "_fetch_rate_probability_observations": _fetch_rate_probability_observations,
    "_build_fred_daily_source": _build_fred_daily_source,
    "_build_fred_nondaily_source": _build_fred_nondaily_source,
    "_build_fred_full_source": _build_fred_full_source,
    "_build_fred_vintages_source": _build_fred_vintages_source,
    "_build_nyfed_rates_source": _build_nyfed_rates_source,
    "_build_gov_reports_source": _build_gov_reports_source,
    "_validate_gov_report_items": staticmethod(_validate_gov_report_items),
    "_deduplicate_gov_report_items": _deduplicate_gov_report_items,
    "_build_eia_source": _build_eia_source,
    "_build_treasury_fiscal_source": _build_treasury_fiscal_source,
    "_build_aisi_source": _build_aisi_source,
    "_build_ism_source": _build_ism_source,
    "_build_redbook_source": _build_redbook_source,
    "_build_bls_source": _build_bls_source,
}
