"""Treasury Fiscal ingestion client."""

from __future__ import annotations

import logging
import time
from typing import Any

from ingestion.scrapers.treasury_fiscal import TreasuryFiscalClient
from ingestion.series_config import TREASURY_DATASETS
from storage import IndicatorObservationRecord, SQLiteEngineStore

logger = logging.getLogger(__name__)


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


class TreasuryFiscalIngestionClient:
    def __init__(self) -> None:
        self.client = TreasuryFiscalClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        fetchers = {
            "debt_outstanding": self.client.fetch_debt_outstanding,
            "dts_operating_cash": self.client.fetch_tga_balance,
            "avg_interest_rates": self.client.fetch_avg_interest_rates,
        }
        for key, fetch_fn in fetchers.items():
            cfg = TREASURY_DATASETS[key]
            try:
                observations = fetch_fn(limit=30)
                fam_id = family_lookup.get(("treasury_fiscal", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="treasury_fiscal",
                            date=obs.date,
                            value=obs.value,
                            metadata={**obs.metadata, "category": cfg["category"]},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("Treasury fiscal refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="treasury_fiscal", count=count)


