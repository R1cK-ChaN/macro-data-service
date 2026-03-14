from __future__ import annotations

import dataclasses
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Callable

from ._anomaly import check_anomalies, compute_series_profile
from ._catalog import CatalogExpectation, check_catalog_completeness
from ._cross_source import (
    CROSS_SOURCE_PAIRS,
    CrossSourcePair,
    check_cross_source,
)
from ._diff import check_data_diff
from ._dimensions import check_dimensions
from ._freshness import (
    FreshnessExpectation,
    check_freshness,
    check_freshness_batch,
)
from ._lineage import check_lineage
from ._revision import check_revisions, check_revisions_batch
from ._schema import check_schema
from ._series import check_series_integrity
from ._store import ValidationStore
from ._types import CheckResult, ValidationReport, ValidationSeverity
from ._volume import VolumeExpectation, check_volume, check_volume_batch

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidationConfig:
    """Per-source validation configuration."""

    source: str
    enable_schema: bool = True
    enable_catalog: bool = False
    enable_series: bool = True
    enable_anomaly: bool = False
    enable_cross_source: bool = False
    enable_data_diff: bool = False
    enable_volume: bool = True
    enable_freshness: bool = True
    enable_revisions: bool = False
    enable_lineage: bool = True
    enable_dimensions: bool = True
    anomaly_sample_size: int = 10
    fail_on_error: bool = False
    catalog_expectations: tuple[CatalogExpectation, ...] = ()
    series_expectations: dict[str, dict[str, Any]] = field(default_factory=dict)
    volume_expectation: VolumeExpectation | None = None
    freshness_expectation: FreshnessExpectation | None = None
    on_report: Callable[[ValidationReport], None] | None = None


class ValidationEngine:
    """Orchestrates all validation layers and integrates with the pipeline."""

    def __init__(
        self,
        validation_store: ValidationStore,
        configs: dict[str, ValidationConfig] | None = None,
    ) -> None:
        self._store = validation_store
        self._configs: dict[str, ValidationConfig] = configs or {}

    def get_config(self, source: str) -> ValidationConfig:
        return self._configs.get(source, ValidationConfig(source=source))

    def set_config(self, config: ValidationConfig) -> None:
        self._configs[config.source] = config

    def should_fail(self, source: str) -> bool:
        return self.get_config(source).fail_on_error

    # ── Post-fetch hook (schema validation) ──────────────────────

    def validate_post_fetch(
        self,
        source: str,
        endpoint: str,
        raw_items: list[Any],
    ) -> ValidationReport:
        """Run schema validation on raw fetched items."""
        started = time.perf_counter()
        config = self.get_config(source)
        checks: list[CheckResult] = []

        if config.enable_schema and raw_items:
            checks.extend(
                check_schema(source, endpoint, raw_items, self._store)
            )

        return self._finalize_report(source, checks, started)

    # ── Post-store hook (series, anomaly) ────────────────────────

    def validate_post_store(
        self,
        source: str,
        ingestion_report: Any,
        stored_observations: dict[str, list[Any]] | None = None,
    ) -> ValidationReport:
        """Run series integrity and anomaly checks after data is stored."""
        started = time.perf_counter()
        config = self.get_config(source)
        checks: list[CheckResult] = []

        if stored_observations and config.enable_series:
            for series_id, obs_list in stored_observations.items():
                expectations = config.series_expectations.get(series_id, {})
                checks.extend(
                    check_series_integrity(
                        source,
                        series_id,
                        obs_list,
                        **expectations,
                    )
                )

        if stored_observations and config.enable_anomaly:
            count = 0
            for series_id, obs_list in stored_observations.items():
                if count >= config.anomaly_sample_size:
                    break
                profile = compute_series_profile(series_id, source, obs_list)
                checks.extend(
                    check_anomalies(source, series_id, profile, self._store)
                )
                count += 1

        return self._finalize_report(source, checks, started)

    # ── Catalog validation (on-demand) ───────────────────────────

    def validate_catalog(self, source: str) -> ValidationReport:
        """Run catalog completeness checks for a source."""
        started = time.perf_counter()
        config = self.get_config(source)
        checks: list[CheckResult] = []

        if config.enable_catalog and config.catalog_expectations:
            checks.extend(
                check_catalog_completeness(
                    source, list(config.catalog_expectations)
                )
            )

        return self._finalize_report(source, checks, started)

    # ── Cross-source validation (on-demand) ──────────────────────

    def validate_cross_source(
        self,
        pairs: list[CrossSourcePair] | None = None,
        observation_fetcher: Callable[[str], list[dict[str, Any]]] | None = None,
    ) -> ValidationReport:
        """Run cross-source consistency checks."""
        started = time.perf_counter()
        checks: list[CheckResult] = []
        pairs = pairs or CROSS_SOURCE_PAIRS

        if observation_fetcher is None:
            return self._finalize_report("cross_source", checks, started)

        for pair in pairs:
            obs_a = observation_fetcher(pair.family_id_a)
            obs_b = observation_fetcher(pair.family_id_b)
            checks.extend(check_cross_source(pair, obs_a, obs_b))

        return self._finalize_report("cross_source", checks, started)

    # ── Data diff validation (on-demand) ─────────────────────────

    def validate_data_diff(
        self,
        source: str,
        series_id: str,
        api_observations: list[dict[str, Any]],
        db_observations: list[dict[str, Any]],
    ) -> ValidationReport:
        """Compare API data against database data for a single series."""
        started = time.perf_counter()
        checks = check_data_diff(source, series_id, api_observations, db_observations)
        return self._finalize_report(source, checks, started)

    # ── Volume validation (on-demand) ────────────────────────────

    def validate_volume(
        self,
        source_counts: dict[str, int],
        expectations: dict[str, VolumeExpectation] | None = None,
    ) -> ValidationReport:
        """Check dataset volume against expected ranges."""
        started = time.perf_counter()
        checks = check_volume_batch(
            source_counts,
            expectations=expectations,
            validation_store=self._store,
        )
        return self._finalize_report("volume", checks, started)

    # ── Freshness validation (on-demand) ─────────────────────────

    def validate_freshness(
        self,
        source_latest_dates: dict[str, str],
        expectations: dict[str, FreshnessExpectation] | None = None,
    ) -> ValidationReport:
        """Check data freshness against staleness thresholds."""
        started = time.perf_counter()
        checks = check_freshness_batch(
            source_latest_dates, expectations=expectations
        )
        return self._finalize_report("freshness", checks, started)

    # ── Revision validation (on-demand) ──────────────────────────

    def validate_revisions(
        self,
        source: str,
        series_vintages: dict[str, list[dict[str, Any]]],
    ) -> ValidationReport:
        """Check revision patterns across vintaged series."""
        started = time.perf_counter()
        checks = check_revisions_batch(
            source, series_vintages, validation_store=self._store
        )
        return self._finalize_report(source, checks, started)

    # ── Lineage validation (on-demand) ───────────────────────────

    def validate_lineage(
        self,
        source: str,
        observations: list[Any],
        *,
        require_family_id: bool = False,
    ) -> ValidationReport:
        """Check every observation has source, series_id, date."""
        started = time.perf_counter()
        checks = check_lineage(
            source, observations, require_family_id=require_family_id
        )
        return self._finalize_report(source, checks, started)

    # ── Dimension validation (on-demand) ─────────────────────────

    def validate_dimensions(
        self,
        source: str,
        families: list[Any],
    ) -> ValidationReport:
        """Check dimension values on observation families."""
        started = time.perf_counter()
        checks = check_dimensions(source, families)
        return self._finalize_report(source, checks, started)

    # ── Full validation (CLI / scheduled) ────────────────────────

    def validate_full(
        self,
        source: str,
        raw_items: list[Any] | None = None,
        endpoint: str = "default",
        stored_observations: dict[str, list[Any]] | None = None,
        ingestion_report: Any = None,
        observation_count: int | None = None,
        latest_date: str | None = None,
        vintages: dict[str, list[dict[str, Any]]] | None = None,
        families: list[Any] | None = None,
    ) -> ValidationReport:
        """Run all applicable layers for a source."""
        started = time.perf_counter()
        config = self.get_config(source)
        checks: list[CheckResult] = []

        # Layer 1: Schema
        if config.enable_schema and raw_items:
            checks.extend(
                check_schema(source, endpoint, raw_items, self._store)
            )

        # Layer 2: Catalog
        if config.enable_catalog and config.catalog_expectations:
            checks.extend(
                check_catalog_completeness(
                    source, list(config.catalog_expectations)
                )
            )

        # Layer 3: Series
        if config.enable_series and stored_observations:
            for series_id, obs_list in stored_observations.items():
                expectations = config.series_expectations.get(series_id, {})
                checks.extend(
                    check_series_integrity(
                        source, series_id, obs_list, **expectations
                    )
                )

        # Layer 4: Anomaly
        if config.enable_anomaly and stored_observations:
            count = 0
            for series_id, obs_list in stored_observations.items():
                if count >= config.anomaly_sample_size:
                    break
                profile = compute_series_profile(series_id, source, obs_list)
                checks.extend(
                    check_anomalies(source, series_id, profile, self._store)
                )
                count += 1

        # Layer 7: Volume
        if config.enable_volume and observation_count is not None:
            checks.extend(
                check_volume(
                    source,
                    observation_count,
                    expectation=config.volume_expectation,
                    validation_store=self._store,
                )
            )

        # Layer 8: Freshness
        if config.enable_freshness and latest_date is not None:
            checks.extend(
                check_freshness(
                    source,
                    latest_date,
                    expectation=config.freshness_expectation,
                )
            )

        # Layer 9: Revisions
        if config.enable_revisions and vintages:
            checks.extend(
                check_revisions_batch(source, vintages, validation_store=self._store)
            )

        # Layer 10: Lineage
        if config.enable_lineage and stored_observations:
            all_obs = [
                obs
                for obs_list in stored_observations.values()
                for obs in obs_list
            ]
            if all_obs:
                checks.extend(check_lineage(source, all_obs))

        # Layer 11: Dimensions
        if config.enable_dimensions and families:
            checks.extend(check_dimensions(source, families))

        return self._finalize_report(source, checks, started)

    # ── Report finalization ──────────────────────────────────────

    def _finalize_report(
        self,
        source: str,
        checks: list[CheckResult],
        started: float,
    ) -> ValidationReport:
        duration_ms = int((time.perf_counter() - started) * 1000)
        report = ValidationReport(
            source=source,
            run_id=uuid.uuid4().hex[:12],
            timestamp=datetime.now(UTC).isoformat(),
            checks=tuple(checks),
            duration_ms=duration_ms,
        )

        # Persist report and individual check history
        try:
            self._store.save_report(report.to_dict())
            self._store.save_check_results(
                [dataclasses.asdict(c) for c in checks]
            )
        except Exception:
            logger.warning("Failed to persist validation report", exc_info=True)

        # Notify callback
        config = self._configs.get(source)
        if config and config.on_report:
            try:
                config.on_report(report)
            except Exception:
                logger.warning("Validation on_report callback failed", exc_info=True)

        # Log failures
        if not report.passed:
            logger.warning(
                "%s validation FAILED: %d errors, %d warnings",
                source,
                report.error_count,
                report.warning_count,
            )

        return report
