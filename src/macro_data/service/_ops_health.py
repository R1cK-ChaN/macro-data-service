from __future__ import annotations

from typing import Any

from .base import LocalMacroDataServiceBase


class OpsHealthMixin(LocalMacroDataServiceBase):
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

    def _op_refresh_source(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "refresh unavailable"}
        source = str(arguments.get("source") or "").strip()
        if not source:
            return {"error": "source is required"}
        try:
            return self._ingestion.run_source(source).to_dict()
        except KeyError:
            return {
                "error": f"unknown source: {source}",
                "available_sources": self._ingestion.list_sources(),
            }

    def _op_list_sources(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Return registered ingestion sources as ``[{name, family}, ...]``.

        Optional ``family`` filter narrows to a single family tag.
        """
        if self._ingestion is None:
            return {"error": "sources unavailable", "sources": []}
        family = (arguments.get("family") or "").strip() or None
        sources = self._ingestion.list_sources()
        if family:
            sources = [s for s in sources if s.get("family") == family]
        return {"total": len(sources), "sources": sources}

    def _op_list_source_capabilities(self, arguments: dict[str, Any]) -> dict[str, Any]:
        include_internal = bool(arguments.get("include_internal", False))
        if self._ingestion is None:
            return {"error": "capabilities unavailable", "sources": []}
        items = self._ingestion.list_source_capabilities(include_internal=include_internal)
        return {"total": len(items), "sources": items}

    def _op_get_source_health_dashboard(self, arguments: dict[str, Any]) -> dict[str, Any]:
        health = self._health or self._ingestion
        if health is None:
            return {"error": "source health unavailable", "sources": []}
        include_internal = bool(arguments.get("include_internal", False))
        return health.get_source_health_dashboard(include_internal=include_internal)

    def _op_get_health(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "ingestion unavailable"}
        indicator = (arguments.get("indicator") or "").strip() or None
        rows = self._ingestion.get_health(indicator=indicator)
        return {"total": len(rows), "rows": rows}

    def _op_get_alerts(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "ingestion unavailable"}
        alerts = self._ingestion.get_alerts(
            delay_minutes=int(arguments.get("delay_minutes", 30)),
            mismatch_threshold_pct=float(arguments.get("mismatch_threshold_pct", 1.0)),
        )
        return {"total": len(alerts), "alerts": alerts}
