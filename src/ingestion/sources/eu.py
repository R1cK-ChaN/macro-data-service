from __future__ import annotations

from typing import Any

from . import (
    BUNDESBANK_SERIES,
    ECB_SERIES,
    EUROSTAT_SERIES,
    IngestionSourceDefinition,
    SENTIX_SERIES,
)


def _build_eurostat_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._eurostat import EurostatFetcher

    return self._build_fetcher_source(
        "eurostat", EurostatFetcher(client=self.eurostat.client),
    )


def _build_ecb_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._sdmx import SDMXFetcher

    return self._build_fetcher_source(
        "ecb", SDMXFetcher(self.ecb.client, "ecb", ECB_SERIES),
    )


def _build_bundesbank_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._sdmx import SDMXFetcher

    return self._build_fetcher_source(
        "bundesbank",
        SDMXFetcher(self.bundesbank.client, "bundesbank", BUNDESBANK_SERIES),
    )


def _build_sentix_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._sentix import SentixFetcher

    return self._build_fetcher_source(
        "sentix",
        SentixFetcher(client=self.sentix, series_config=SENTIX_SERIES),
    )


SOURCE_METHODS: dict[str, Any] = {
    "_build_eurostat_source": _build_eurostat_source,
    "_build_ecb_source": _build_ecb_source,
    "_build_bundesbank_source": _build_bundesbank_source,
    "_build_sentix_source": _build_sentix_source,
}
