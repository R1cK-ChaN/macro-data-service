"""Unified frozen dataclasses for all SDMX providers."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SDMXObservation:
    """A single observation from any SDMX provider."""

    series_id: str
    date: str
    value: float
    dataflow: str = ""
    dataset: str = ""  # Eurostat JSON-stat


@dataclass(frozen=True)
class SDMXDataflow:
    """Represents a dataset in an SDMX catalog."""

    id: str
    agency_id: str
    version: str
    name: str = ""
    description: str = ""
    structure_id: str = ""
    structure_version: str = ""


@dataclass(frozen=True)
class SDMXDimension:
    """A single dimension in a data structure definition."""

    id: str
    position: int
    name: str = ""
    code_count: int = 0
    is_time: bool = False
    codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class SDMXDataStructure:
    """Full DSD with dimensions for a dataflow."""

    id: str
    version: str
    name: str = ""
    dimensions: tuple[SDMXDimension, ...] = ()
    dataflow_id: str = ""
    dataflow_version: str = ""


@dataclass(frozen=True)
class SDMXStructureSummary:
    """Compact summary for catalog inspection."""

    dataflow_id: str
    version: str
    name: str = ""
    structure_id: str = ""
    time_dimension_id: str = ""
    series_dimensions: tuple[str, ...] = ()
    code_counts: dict[str, int] = field(default_factory=dict)
    estimated_series: int = 0


@dataclass(frozen=True)
class SDMXSizeEstimate:
    """Observation count estimate for a dataflow."""

    dataflow_id: str
    version: str
    total_series: int = 0
    time_periods: int = 0
    estimated_observations: int = 0
