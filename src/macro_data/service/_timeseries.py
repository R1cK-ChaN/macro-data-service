from __future__ import annotations

from typing import Any

from contracts import format_epoch_iso

from .base import (
    LocalMacroDataServiceBase,
    _VALID_RATE_TYPES,
    logger,
)


class TimeseriesOpsMixin(LocalMacroDataServiceBase):
    def _op_sync_catalog_discovery(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "catalog discovery unavailable"}
        source_id = str(arguments.get("source_id") or arguments.get("source") or "").strip()
        if not source_id:
            return {"error": "source_id is required"}
        query = (arguments.get("query") or "").strip() or None
        limit = arguments.get("limit")
        return self._ingestion.sync_catalog_discovery(
            source_id,
            query=query,
            limit=int(limit) if limit is not None else None,
        )

    def _op_list_catalog_entities(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "catalog listing unavailable", "entities": []}
        source_id = str(arguments.get("source_id") or arguments.get("source") or "").strip()
        if not source_id:
            return {"error": "source_id is required", "entities": []}
        query = (arguments.get("query") or "").strip() or None
        limit = int(arguments.get("limit", 100))
        refresh = bool(arguments.get("refresh", False))
        return self._ingestion.list_catalog_entities(
            source_id,
            query=query,
            limit=limit,
            refresh=refresh,
        )

    def _op_get_catalog_structure(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "catalog structure unavailable", "structure": None}
        source_id = str(arguments.get("source_id") or arguments.get("source") or "").strip()
        entity_id = str(arguments.get("entity_id") or arguments.get("entity") or "").strip()
        if not source_id:
            return {"error": "source_id is required", "structure": None}
        if not entity_id:
            return {"error": "entity_id is required", "structure": None}
        return self._ingestion.get_catalog_structure(source_id, entity_id)

    def _op_sync_catalog_latest(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "catalog sync unavailable"}
        source_id = str(arguments.get("source_id") or arguments.get("source") or "").strip()
        if not source_id:
            return {"error": "source_id is required"}
        entity_ids = arguments.get("entity_ids")
        if entity_ids is None:
            entity = (arguments.get("entity_id") or arguments.get("entity") or "").strip()
            entity_ids = [entity] if entity else None
        limit = arguments.get("limit")
        return self._ingestion.sync_catalog_latest(
            source_id,
            entity_ids=entity_ids,
            limit=int(limit) if limit is not None else None,
        )

    def _op_get_catalog_status(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "catalog status unavailable", "sources": []}
        source_id = (arguments.get("source_id") or arguments.get("source") or "").strip() or None
        include_internal = bool(arguments.get("include_internal", False))
        return self._ingestion.get_catalog_status(source_id, include_internal=include_internal)

    def _op_refresh_indicator(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "ingestion unavailable"}
        concept_id = (arguments.get("concept_id") or "").strip()
        if not concept_id:
            return {"error": "concept_id is required"}
        lookback_days = int(arguments.get("lookback_days", 365 * 3))
        report = self._ingestion.refresh_indicator(
            concept_id, lookback_days=lookback_days,
        )
        return report.to_dict()

    def _op_validate_concept(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from ingestion.validation import ValidationEngine, ValidationStore

        concept_id = (arguments.get("concept_id") or "").strip()
        if not concept_id:
            return {"error": "concept_id is required"}
        max_staleness = int(arguments.get("max_staleness_days", 90))
        tolerance = float(arguments.get("value_tolerance_pct", 1.0))
        lookback = int(arguments.get("lookback_periods", 12))

        db_path = getattr(self._store, "db_path", ".macro-data/engine.db")
        validation_store = ValidationStore(str(db_path))
        engine = ValidationEngine(validation_store)
        self._store.seed_concept_map()

        report = engine.validate_concept(
            concept_id,
            self._store,
            max_staleness_days=max_staleness,
            value_tolerance_pct=tolerance,
            lookback_periods=lookback,
        )
        return report.to_dict()

    def _op_validate_all_concepts(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from ingestion.validation import ValidationEngine, ValidationStore

        country_code = (arguments.get("country_code") or "").strip() or None
        max_staleness = int(arguments.get("max_staleness_days", 90))
        tolerance = float(arguments.get("value_tolerance_pct", 1.0))
        lookback = int(arguments.get("lookback_periods", 12))

        db_path = getattr(self._store, "db_path", ".macro-data/engine.db")
        validation_store = ValidationStore(str(db_path))
        engine = ValidationEngine(validation_store)
        self._store.seed_concept_map()

        reports = engine.validate_all_concepts(
            self._store,
            max_staleness_days=max_staleness,
            value_tolerance_pct=tolerance,
            lookback_periods=lookback,
            country_code=country_code,
        )
        return {
            "total_concepts": len(reports),
            "passed": sum(1 for r in reports if r.passed),
            "failed": sum(1 for r in reports if not r.passed),
            "reports": [r.to_dict() for r in reports],
        }

    def _op_resolve_indicator(self, arguments: dict[str, Any]) -> dict[str, Any]:
        concept_id = (arguments.get("concept_id") or "").strip()
        if not concept_id:
            return {"error": "concept_id is required"}
        self._store.seed_concept_map()
        date = (arguments.get("date") or "").strip() or None
        as_of = (arguments.get("as_of") or "").strip() or None
        obs = self._store.resolve_indicator(concept_id, date=date, as_of=as_of)
        if obs is None:
            return {"resolved": None, "concept_id": concept_id}
        from dataclasses import asdict
        return {"resolved": asdict(obs)}

    def _op_resolve_indicator_history(self, arguments: dict[str, Any]) -> dict[str, Any]:
        concept_id = (arguments.get("concept_id") or "").strip()
        if not concept_id:
            return {"error": "concept_id is required"}
        self._store.seed_concept_map()
        limit = int(arguments.get("limit", 12))
        as_of = (arguments.get("as_of") or "").strip() or None
        results = self._store.resolve_indicator_history(
            concept_id, limit=limit, as_of=as_of,
        )
        from dataclasses import asdict
        return {
            "concept_id": concept_id,
            "total": len(results),
            "observations": [asdict(r) for r in results],
        }

    def _op_get_release_schedule(self, arguments: dict[str, Any]) -> dict[str, Any]:
        concept_id = (arguments.get("concept_id") or "").strip() or None
        due_only = bool(arguments.get("due_only", False))
        limit = int(arguments.get("limit", 100))

        self._store.seed_release_schedules()

        if concept_id:
            rec = self._store.get_release_schedule(concept_id)
            if rec is None:
                return {"error": f"no schedule for {concept_id}", "schedules": []}
            from dataclasses import asdict
            return {"schedules": [asdict(rec)]}

        schedules = self._store.list_release_schedules(is_active=True)
        if due_only:
            from ingestion.release_schedule import check_due_concepts
            schedules = check_due_concepts(schedules)

        from dataclasses import asdict
        items = [asdict(s) for s in schedules[:limit]]
        return {"total": len(items), "schedules": items}

    def _op_get_release_status(self, arguments: dict[str, Any]) -> dict[str, Any]:
        concept_id = (arguments.get("concept_id") or "").strip() or None
        status_filter = (arguments.get("status") or "").strip() or None
        limit = int(arguments.get("limit", 100))

        if concept_id:
            rec = self._store.get_latest_release_status(concept_id)
            if rec is None:
                return {"error": f"no status for {concept_id}", "statuses": []}
            from dataclasses import asdict
            return {"statuses": [asdict(rec)]}

        statuses = self._store.list_release_statuses(status=status_filter)
        from dataclasses import asdict
        items = [asdict(s) for s in statuses[:limit]]
        return {"total": len(items), "statuses": items}

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

    def _op_get_indicator_ontology(self, arguments: dict[str, Any]) -> dict[str, Any]:
        self._ensure_structural_ontology()
        indicator_id = (arguments.get("indicator_id") or "").strip()
        if not indicator_id:
            return {"error": "indicator_id is required", "indicator": None}

        indicator = self._store.get_calendar_indicator(indicator_id)
        if indicator is None:
            return {"error": f"unknown indicator_id: {indicator_id}", "indicator": None}

        aliases = self._store.list_aliases_for_indicator(indicator_id)
        release_families = self._store.list_release_families_for_indicator(indicator_id)
        release_sources: dict[str, Any | None] = {}
        release_items: list[dict[str, Any]] = []
        institutions: dict[str, dict[str, Any]] = {}
        release_family_ids: list[str] = []
        produced_by_ids: list[str] = []

        obs_family = None
        obs_source = None
        if indicator.obs_family_id:
            obs_family = self._store.get_obs_family(indicator.obs_family_id)
            if obs_family is not None:
                obs_source = self._store.get_obs_source(obs_family.source_id)
                if obs_source is not None:
                    self._merge_ontology_institution(
                        institutions,
                        institution_id=obs_source.source_id,
                        name=obs_source.source_name,
                        source_type=obs_source.source_type,
                        country_code=obs_source.country_code,
                        homepage_url=obs_source.homepage_url,
                        role="series_provider",
                    )

        for release_family in release_families:
            release_family_ids.append(release_family.release_family_id)
            produced_by_ids.append(release_family.source_id)
            release_source = release_sources.get(release_family.source_id)
            if release_family.source_id not in release_sources:
                release_source = self._store.get_doc_source(release_family.source_id)
                release_sources[release_family.source_id] = release_source
            if release_source is not None:
                self._merge_ontology_institution(
                    institutions,
                    institution_id=release_source.source_id,
                    name=release_source.source_name,
                    source_type=release_source.source_type,
                    country_code=release_source.country_code,
                    homepage_url=release_source.homepage_url,
                    role="release_producer",
                )
            release_items.append(self._release_family_to_dict(release_family, release_source=release_source))

        return {
            "indicator": self._calendar_indicator_to_dict(
                indicator,
                produced_by_institution_ids=sorted(set(produced_by_ids)),
                release_family_ids=sorted(release_family_ids),
            ),
            "topic": {
                "code": indicator.topic,
                "country_code": indicator.country_code,
            },
            "aliases": [self._calendar_alias_to_dict(alias) for alias in aliases],
            "time_series": self._obs_family_to_dict(obs_family, obs_source=obs_source) if obs_family is not None else None,
            "release_families": release_items,
            "institutions": sorted(institutions.values(), key=lambda item: item["institution_id"]),
        }

    def _op_list_indicators_by_topic(self, arguments: dict[str, Any]) -> dict[str, Any]:
        self._ensure_structural_ontology()
        topic = (arguments.get("topic_code") or arguments.get("topic") or "").strip().lower()
        if not topic:
            return {"error": "topic is required", "indicators": []}
        country_code = (arguments.get("country_code") or arguments.get("country") or "").strip().upper()
        indicators = self._store.list_calendar_indicators(
            country_code=country_code or None,
            topic=topic,
        )
        items = []
        for indicator in indicators:
            release_family_ids = [
                release.release_family_id
                for release in self._store.list_release_families_for_indicator(indicator.indicator_id)
            ]
            items.append(
                self._calendar_indicator_to_dict(
                    indicator,
                    release_family_ids=sorted(release_family_ids),
                )
                | {"has_time_series": bool(indicator.obs_family_id)}
            )
        return {
            "topic": topic,
            "country_code": country_code,
            "total": len(items),
            "indicators": items,
        }

    def _op_list_release_families_for_indicator(self, arguments: dict[str, Any]) -> dict[str, Any]:
        self._ensure_structural_ontology()
        indicator_id = (arguments.get("indicator_id") or "").strip()
        if not indicator_id:
            return {"error": "indicator_id is required", "release_families": []}

        indicator = self._store.get_calendar_indicator(indicator_id)
        if indicator is None:
            return {"error": f"unknown indicator_id: {indicator_id}", "release_families": []}

        release_families = self._store.list_release_families_for_indicator(indicator_id)
        items = []
        for release_family in release_families:
            release_source = self._store.get_doc_source(release_family.source_id)
            items.append(self._release_family_to_dict(release_family, release_source=release_source))
        return {
            "indicator": self._calendar_indicator_to_dict(
                indicator,
                produced_by_institution_ids=sorted({release.source_id for release in release_families}),
                release_family_ids=sorted(release.release_family_id for release in release_families),
            ),
            "total": len(items),
            "release_families": items,
        }

    def _op_fetch_country_indicators(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from ingestion.scrapers import TradingEconomicsIndicatorsClient

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
        from ingestion.scrapers import NYFedRatesClient

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
        from ingestion.scrapers import RateProbabilityClient

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

    def _merge_ontology_institution(
        self,
        institutions: dict[str, dict[str, Any]],
        *,
        institution_id: str,
        name: str,
        source_type: str,
        country_code: str,
        homepage_url: str,
        role: str,
    ) -> None:
        if not institution_id:
            return
        record = institutions.setdefault(
            institution_id,
            {
                "institution_id": institution_id,
                "name": name,
                "source_type": source_type,
                "country_code": country_code,
                "homepage_url": homepage_url,
                "roles": [],
            },
        )
        if role not in record["roles"]:
            record["roles"].append(role)
            record["roles"].sort()

    def _calendar_indicator_to_dict(
        self,
        indicator: Any,
        *,
        produced_by_institution_ids: list[str] | None = None,
        release_family_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "indicator_id": indicator.indicator_id,
            "canonical_name": indicator.canonical_name,
            "topic": indicator.topic,
            "country_code": indicator.country_code,
            "frequency": indicator.frequency,
            "unit": indicator.unit,
            "obs_family_id": indicator.obs_family_id,
        }
        if produced_by_institution_ids is not None:
            payload["produced_by_institution_ids"] = produced_by_institution_ids
        if release_family_ids is not None:
            payload["release_family_ids"] = release_family_ids
        return payload

    def _calendar_alias_to_dict(self, alias: Any) -> dict[str, Any]:
        return {
            "alias": alias.alias_original or alias.alias_normalized,
            "normalized_alias": alias.alias_normalized,
            "source": alias.source,
            "country_code": alias.country_code,
        }

    def _obs_family_to_dict(self, obs_family: Any, *, obs_source: Any | None = None) -> dict[str, Any]:
        payload = {
            "family_id": obs_family.family_id,
            "provider_series_id": obs_family.provider_series_id,
            "canonical_name": obs_family.canonical_name,
            "source_id": obs_family.source_id,
            "country_code": obs_family.country_code,
            "topic_code": obs_family.topic_code,
            "category": obs_family.category,
            "frequency": obs_family.frequency,
            "unit": obs_family.unit,
            "seasonal_adjustment": obs_family.seasonal_adjustment,
            "has_vintages": obs_family.has_vintages,
        }
        if obs_source is not None:
            payload["source_name"] = obs_source.source_name
            payload["source_type"] = obs_source.source_type
        return payload

    def _release_family_to_dict(self, release_family: Any, *, release_source: Any | None = None) -> dict[str, Any]:
        payload = {
            "release_family_id": release_family.release_family_id,
            "release_code": release_family.release_code,
            "release_name": release_family.release_name,
            "topic_code": release_family.topic_code,
            "country_code": release_family.country_code,
            "frequency": release_family.frequency,
            "produced_by_institution_id": release_family.source_id,
        }
        if release_source is not None:
            payload["institution_name"] = release_source.source_name
            payload["institution_type"] = release_source.source_type
            payload["homepage_url"] = release_source.homepage_url
        return payload

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

    # ────────────────────────────────────────────────────────────────────
    # Fundamentals (issue #68 slice 2)
    # ────────────────────────────────────────────────────────────────────

    def _op_fundamentals_fetch(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Fetch ``/api/fundamentals/`` for one or more EODHD tickers.

        Arguments:
          tickers       — required list (or comma-string) of EODHD
                          tickers like ``["AAPL.US","MSFT.US"]``.
          sections      — optional list of section names to filter the
                          response (``General`` / ``Highlights`` /
                          ``Financials`` / …). Default keeps the
                          slice-1 set: General, Highlights, Valuation,
                          SharesStats, Financials. Pass ``[]`` to
                          request the full unfiltered payload.
          dry_run       — default True. Returns the ticker plan
                          without any HTTP call.
          max_requests  — default 20. Hard upper bound on requests
                          spent in one invocation, including retry
                          attempts inside a single ticker call.

        Returns request / row / insert counts and a stop reason. The
        per-ticker error list surfaces 404s, throttle, and parse
        errors without aborting the whole batch.
        """
        from ingestion.market.fundamentals.eodhd_fundamentals import (
            EODHDFundamentalsClient,
            FundamentalsFetcher,
        )

        raw_tickers = arguments.get("tickers")
        tickers: list[str]
        if isinstance(raw_tickers, list):
            tickers = [str(t).strip() for t in raw_tickers if str(t).strip()]
        elif isinstance(raw_tickers, str) and raw_tickers.strip():
            tickers = [t.strip() for t in raw_tickers.split(",") if t.strip()]
        else:
            tickers = []
        if not tickers:
            return {"error": "tickers is required"}

        raw_sections = arguments.get("sections")
        sections: list[str] | None
        if isinstance(raw_sections, list):
            sections = [str(s).strip() for s in raw_sections if str(s).strip()]
        elif isinstance(raw_sections, str):
            sections = [s.strip() for s in raw_sections.split(",") if s.strip()] or None
        else:
            sections = None

        dry_run = bool(arguments.get("dry_run", True))
        try:
            max_requests = max(1, int(arguments.get("max_requests") or 20))
        except (TypeError, ValueError):
            max_requests = 20

        # Partial-section fetches are dry-run only. In execute mode the
        # response anchors PIT reconstruction (``as_of`` reads re-parse
        # ``fundamentals_raw``), so a Highlights-only payload would
        # surface as ``financials=[]`` for any later ``as_of`` cutoff
        # against that snapshot — corrupting the projection history.
        if not dry_run and sections is not None:
            from ingestion.market.fundamentals.eodhd_fundamentals import (
                FundamentalsFetcher,
            )

            required = set(FundamentalsFetcher.DEFAULT_SECTIONS)
            missing = sorted(required.difference(sections))
            if missing:
                return {
                    "error": (
                        "execute-mode fetch requires the full slice-1 "
                        f"section set; missing: {missing}"
                    ),
                }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        with EODHDFundamentalsClient() as client:
            connection = get_conn()
            try:
                fetcher = FundamentalsFetcher(
                    connection=connection,
                    client=client,
                    max_requests=max_requests,
                )
                summary = fetcher.fetch(
                    tickers=tickers, sections=sections, dry_run=dry_run,
                )
                connection.commit()
            except ValueError as exc:
                connection.rollback()
                return {"error": str(exc)}
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

        return {
            "dry_run":              summary.dry_run,
            "tickers_planned":      summary.tickers_planned,
            "tickers_fetched":      summary.tickers_fetched,
            "tickers_skipped_error": summary.tickers_skipped_error,
            "tickers":              summary.tickers,
            "requests_spent":       summary.requests_spent,
            "raw_inserted":         summary.raw_inserted,
            "company_upserted":     summary.company_upserted,
            "financials_upserted":  summary.financials_upserted,
            "highlights_upserted":  summary.highlights_upserted,
            "parse_errors":         summary.parse_errors,
            "stopped_reason":       summary.stopped_reason,
            "errors":               summary.errors,
        }

    def _op_get_fundamentals(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Read-side query over ``fundamentals_*`` projections.

        Arguments:
          ticker       — required EODHD ticker like ``AAPL.US``.
          provider     — default ``eodhd``.
          as_of        — optional ISO-8601 UTC cutoff. When set:
                         * ``company`` is returned as-is (it's a
                           one-row ticker profile);
                         * ``highlights`` returns the latest snapshot
                           with ``as_of_date <=`` the cutoff;
                         * ``financials`` is reconstructed from the
                           latest ``fundamentals_raw`` row at-or-
                           before the cutoff so restated quarters
                           return the prior-version numbers visible
                           on that date.
                         Future ``as_of`` is rejected.
          statement    — optional ``IS`` / ``BS`` / ``CF`` filter on
                         the financials list.
          period       — optional ``Q`` / ``A`` filter.
          limit        — financials row cap (default 100, max 500).

        Returns:
          {"ticker": "...",
           "as_of": "..." | null,
           "company": {...} | null,
           "highlights": {...} | null,
           "financials": [ {...}, ... ]}
        """
        from datetime import datetime, timezone
        import json as _json

        from ingestion.market.fundamentals.eodhd_fundamentals import (
            parse_financials_section,
            parse_highlights_section,
        )
        from storage.queries.fundamentals import parse_as_of_to_epoch_ms

        ticker = (arguments.get("ticker") or "").strip()
        if not ticker:
            return {"error": "ticker is required"}
        provider = (arguments.get("provider") or "eodhd").strip()
        statement = (arguments.get("statement") or "").strip().upper() or None
        period = (arguments.get("period") or "").strip().upper() or None
        if statement and statement not in {"IS", "BS", "CF"}:
            return {"error": f"invalid statement: {statement!r}"}
        if period and period not in {"Q", "A"}:
            return {"error": f"invalid period: {period!r}"}
        try:
            limit = int(arguments.get("limit") or 100)
        except (TypeError, ValueError):
            limit = 100
        limit = max(1, min(500, limit))

        as_of_raw = (arguments.get("as_of") or "").strip()
        as_of_epoch_ms: int | None = None
        as_of_iso: str | None = None
        if as_of_raw:
            try:
                as_of_epoch_ms = parse_as_of_to_epoch_ms(as_of_raw)
            except ValueError:
                return {"error": f"invalid as_of: {as_of_raw!r}"}
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            if as_of_epoch_ms > now_ms:
                return {"error": "as_of must not be in the future"}
            as_of_iso = datetime.fromtimestamp(
                as_of_epoch_ms / 1000, tz=timezone.utc,
            ).isoformat()

        company = self._store.get_fundamentals_company_row(
            provider=provider, ticker=ticker,
        )

        if as_of_epoch_ms is None:
            highlights = self._store.get_fundamentals_highlights_row(
                provider=provider, ticker=ticker,
            )
            financials = self._store.list_fundamentals_financials(
                provider=provider, ticker=ticker,
                statement=statement, period_type=period, limit=limit,
            )
        else:
            # Reconstruct both highlights and financials from the same
            # raw snapshot at-or-before the cutoff. Querying the
            # ``fundamentals_highlights`` projection by ``as_of_date``
            # leaks intra-day post-cutoff values when two snapshots
            # land on the same UTC day (the grain is one row per day,
            # not per snapshot — Codex review #68 S2 round 1 P2).
            raw = self._store.get_fundamentals_raw_at(
                provider=provider, ticker=ticker,
                as_of_epoch_ms=as_of_epoch_ms,
            )
            if raw is None:
                highlights = None
                financials = []
            else:
                try:
                    payload = _json.loads(raw["payload_json"])
                except (TypeError, ValueError):
                    payload = {}
                snapshot_ms = int(raw["snapshot_epoch_ms"])
                highlights_record = parse_highlights_section(
                    payload, ticker=ticker, snapshot_epoch_ms=snapshot_ms,
                )
                if highlights_record is None:
                    highlights = None
                else:
                    highlights = {
                        "provider":             highlights_record.provider,
                        "ticker":               highlights_record.ticker,
                        "as_of_date":           highlights_record.as_of_date,
                        "market_cap":           highlights_record.market_cap,
                        "pe_ratio":             highlights_record.pe_ratio,
                        "eps_ttm":              highlights_record.eps_ttm,
                        "dividend_yield":       highlights_record.dividend_yield,
                        "book_value":           highlights_record.book_value,
                        "shares_outstanding":   highlights_record.shares_outstanding,
                        "payload_json":         highlights_record.payload_json,
                        "content_hash":         highlights_record.content_hash,
                        "observed_at_epoch_ms": highlights_record.observed_at_epoch_ms,
                    }
                rows = parse_financials_section(
                    payload, ticker=ticker, snapshot_epoch_ms=snapshot_ms,
                )
                financials = [
                    {
                        "provider":              r.provider,
                        "ticker":                r.ticker,
                        "period_end":            r.period_end,
                        "period_type":           r.period_type,
                        "statement":             r.statement,
                        "currency":              r.currency,
                        "filing_date":           r.filing_date,
                        "revenue":               r.revenue,
                        "net_income":            r.net_income,
                        "eps_basic":             r.eps_basic,
                        "total_assets":          r.total_assets,
                        "total_equity":          r.total_equity,
                        "total_liabilities":     r.total_liabilities,
                        "cash_from_ops":         r.cash_from_ops,
                        "capex":                 r.capex,
                        "payload_json":          r.payload_json,
                        "content_hash":          r.content_hash,
                        "observed_at_epoch_ms":  r.observed_at_epoch_ms,
                    }
                    for r in rows
                    if (statement is None or r.statement == statement)
                    and (period is None or r.period_type == period)
                ]
                # Match the SQL ORDER BY in list_fundamentals_financials:
                # ``period_end DESC, statement ASC, period_type ASC``.
                # Python's sort is stable, so a primary sort on the
                # ascending tiebreakers followed by a descending
                # ``period_end`` sort lands rows in the same order as
                # the latest-projection path (Codex review #68 S2 R2 P2).
                financials.sort(key=lambda r: (r["statement"], r["period_type"]))
                financials.sort(key=lambda r: r["period_end"], reverse=True)
                financials = financials[:limit]

        return {
            "ticker":     ticker,
            "provider":   provider,
            "as_of":      as_of_iso,
            "company":    company,
            "highlights": highlights,
            "financials": financials,
        }
