"""Centralised series configuration — facade re-exporting from per-data-class configs.

All symbols remain importable from ``ingestion.series_config`` for backward
compatibility.  Canonical definitions now live in per-data-class ``_config.py``
modules.
"""

from ingestion.timeseries._config import *  # noqa: F401,F403
from ingestion.documents._config import *  # noqa: F401,F403
from ingestion.market._config import *  # noqa: F401,F403
