"""Multi-layer data validation framework for macro data ingestion."""

from ._anomaly import check_anomalies, compute_series_profile
from ._catalog import CatalogExpectation, check_catalog_completeness
from ._cross_source import CROSS_SOURCE_PAIRS, CrossSourcePair, check_cross_source
from ._diff import check_data_diff
from ._dimensions import (
    VALID_COUNTRY_CODES,
    VALID_FREQUENCIES,
    VALID_SEASONAL_ADJUSTMENTS,
    VALID_SOURCE_IDS,
    VALID_UNITS,
    check_dimensions,
)
from ._engine import ValidationConfig, ValidationEngine
from ._freshness import (
    FreshnessExpectation,
    check_freshness,
    check_freshness_batch,
)
from ._lineage import check_lineage
from ._revision import (
    RevisionSummary,
    check_revisions,
    check_revisions_batch,
    compute_revision_summary,
)
from ._schema import check_schema
from ._series import check_series_batch, check_series_integrity
from ._store import ValidationStore
from ._types import (
    CheckResult,
    SchemaFingerprint,
    SeriesProfile,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)
from ._volume import (
    VolumeExpectation,
    check_volume,
    check_volume_batch,
)

__all__ = [
    # Engine
    "ValidationEngine",
    "ValidationConfig",
    "ValidationStore",
    # Types
    "CheckResult",
    "ValidationReport",
    "ValidationLayer",
    "ValidationSeverity",
    "SchemaFingerprint",
    "SeriesProfile",
    # Layer 1: Schema
    "check_schema",
    # Layer 2: Catalog
    "CatalogExpectation",
    "check_catalog_completeness",
    # Layer 3: Series
    "check_series_integrity",
    "check_series_batch",
    # Layer 4: Anomaly
    "check_anomalies",
    "compute_series_profile",
    # Layer 5: Cross-source
    "CrossSourcePair",
    "CROSS_SOURCE_PAIRS",
    "check_cross_source",
    # Layer 6: Data diff
    "check_data_diff",
    # Layer 7: Volume
    "VolumeExpectation",
    "check_volume",
    "check_volume_batch",
    # Layer 8: Freshness
    "FreshnessExpectation",
    "check_freshness",
    "check_freshness_batch",
    # Layer 9: Revisions
    "RevisionSummary",
    "compute_revision_summary",
    "check_revisions",
    "check_revisions_batch",
    # Layer 10: Lineage
    "check_lineage",
    # Layer 11: Dimensions
    "check_dimensions",
    "VALID_FREQUENCIES",
    "VALID_UNITS",
    "VALID_SEASONAL_ADJUSTMENTS",
    "VALID_COUNTRY_CODES",
    "VALID_SOURCE_IDS",
]
