"""ClickHouse storage backend for the market-data lane (issue #118).

Bilingual storage: SQLite still owns the macro line (calendar / indicators
/ documents / news); ClickHouse owns the market line. The two stores never
JOIN inside the database — cross-domain reads (``list_items`` family
fan-out) stay a service-level operation per issue #118 P3.
"""

from __future__ import annotations

from .schema import (
    CLICKHOUSE_DATABASE,
    apply_clickhouse_schema,
    clickhouse_database_from_env,
)

__all__ = [
    "CLICKHOUSE_DATABASE",
    "apply_clickhouse_schema",
    "clickhouse_database_from_env",
]
