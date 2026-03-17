"""Ingestion clients — extracted from sources.py for modularity."""

from ingestion.clients._fred import FREDIngestionClient
from ingestion.clients._eia import EIAIngestionClient
from ingestion.clients._treasury import TreasuryFiscalIngestionClient
from ingestion.clients._sdmx_clients import (
    IMFIngestionClient,
    EurostatIngestionClient,
    BISIngestionClient,
    ECBIngestionClient,
)
from ingestion.clients._oecd_client import OECDIngestionClient
from ingestion.clients._ilo_unsd import ILOIngestionClient, UNSDIngestionClient
from ingestion.clients._worldbank_client import WorldBankIngestionClient
from ingestion.clients._fed import FedIngestionClient
from ingestion.clients._market import MarketPriceClient
from ingestion.clients._trends import (
    RedditTrendIngestionClient,
    WeiboTrendIngestionClient,
)
from ingestion.clients._news import NewsIngestionClient
from ingestion.clients._gov_report import GovReportIngestionClient

__all__ = [
    "BISIngestionClient",
    "ECBIngestionClient",
    "EIAIngestionClient",
    "EurostatIngestionClient",
    "FREDIngestionClient",
    "FedIngestionClient",
    "GovReportIngestionClient",
    "ILOIngestionClient",
    "IMFIngestionClient",
    "MarketPriceClient",
    "NewsIngestionClient",
    "OECDIngestionClient",
    "RedditTrendIngestionClient",
    "TreasuryFiscalIngestionClient",
    "UNSDIngestionClient",
    "WeiboTrendIngestionClient",
    "WorldBankIngestionClient",
]
