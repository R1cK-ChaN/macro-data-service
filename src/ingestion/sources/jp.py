from __future__ import annotations

from typing import Any

from . import IngestionSourceDefinition, MOF_JGB_SERIES


def _build_mof_jp_source(self) -> IngestionSourceDefinition:
    from ingestion.fetchers._mof_jgb import MOFJGBFetcher

    return self._build_fetcher_source(
        "mof_jp", MOFJGBFetcher(client=self.mof_jp, series_config=MOF_JGB_SERIES),
    )


SOURCE_METHODS: dict[str, Any] = {
    "_build_mof_jp_source": _build_mof_jp_source,
}
