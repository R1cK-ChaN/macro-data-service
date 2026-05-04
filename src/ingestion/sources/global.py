from __future__ import annotations

from typing import Any

from . import (
    BIS_SERIES,
    IMF_SERIES,
    IngestionSourceDefinition,
)


def _build_imf_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._sdmx import SDMXFetcher

    return self._build_fetcher_source(
        "imf", SDMXFetcher(self.imf.client, "imf", IMF_SERIES),
    )


def _build_imf_vintages_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._vintages import IMFVintageFetcher

    return self._build_vintage_fetcher_source(
        "imf_vintages",
        IMFVintageFetcher(client=self.imf.client),
    )


def _build_bis_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._sdmx import SDMXFetcher

    return self._build_fetcher_source(
        "bis", SDMXFetcher(self.bis.client, "bis", BIS_SERIES),
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
    from ingestion.fetchers._worldbank import WorldBankCatalogFetcher

    return IngestionSourceDefinition(
        name="worldbank_catalog",
        interval_seconds=86_400 * 7,
        prepare=self._ensure_obs_seed,
        fetch=lambda: self._fetch_with_obs_raw(
            WorldBankCatalogFetcher(client=self.worldbank.client),
            lookback_days=365,
        ),
        normalize=self._raw_worldbank_catalog_series_to_records,
        deduplicate=self._deduplicate_observations,
        store=self._store_indicator_observations,
    )


SOURCE_METHODS: dict[str, Any] = {
    "_build_imf_source": _build_imf_source,
    "_build_imf_vintages_source": _build_imf_vintages_source,
    "_build_bis_source": _build_bis_source,
    "_build_oecd_source": _build_oecd_source,
    "_build_worldbank_source": _build_worldbank_source,
    "_build_worldbank_catalog_source": _build_worldbank_catalog_source,
}
