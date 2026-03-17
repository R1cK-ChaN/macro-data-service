"""EIA ingestion client."""

from __future__ import annotations

import logging
import time
from typing import Any

from ingestion.scrapers.eia import EIAClient
from ingestion.series_config import EIA_SERIES
from storage import IndicatorObservationRecord, SQLiteEngineStore

logger = logging.getLogger(__name__)


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


class EIAIngestionClient:
    def __init__(self, api_key: str | None = None) -> None:
        self.client = EIAClient(api_key=api_key)

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in EIA_SERIES.items():
            try:
                observations = self.client.get_series(
                    cfg["route"],
                    params=dict(cfg["params"]),
                    series_id=cfg["series_id"],
                    limit=30,
                )
                fam_id = family_lookup.get(("eia", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="eia",
                            date=obs.date,
                            value=obs.value,
                            metadata={"category": cfg["category"], "unit": obs.unit},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("EIA refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="eia", count=count)


