"""World Bank ingestion client — facade delegating to timeseries/clients/."""

from ingestion.timeseries.clients._worldbank_client import *  # noqa: F401,F403
from ingestion.timeseries.clients._worldbank_client import _WorldBankRateLimiter  # noqa: F401
