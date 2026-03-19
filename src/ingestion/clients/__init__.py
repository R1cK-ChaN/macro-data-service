"""Ingestion clients — extracted from sources.py for modularity."""

from ingestion.timeseries.clients._fred import FREDIngestionClient
from ingestion.timeseries.clients._eia import EIAIngestionClient
from ingestion.timeseries.clients._treasury import TreasuryFiscalIngestionClient
from ingestion.timeseries.clients._sdmx_clients import (
    IMFIngestionClient,
    EurostatIngestionClient,
    BISIngestionClient,
    ECBIngestionClient,
)
from ingestion.timeseries.clients._oecd_client import OECDIngestionClient
from ingestion.timeseries.clients._ilo_unsd import ILOIngestionClient, UNSDIngestionClient
from ingestion.timeseries.clients._worldbank_client import WorldBankIngestionClient
from ingestion.documents.clients._fed import FedIngestionClient
from ingestion.market.clients._market import MarketPriceClient
from ingestion.trends.clients._trends import (
    RedditTrendIngestionClient,
    WeiboTrendIngestionClient,
)
from ingestion.news.client import NewsIngestionClient
from ingestion.documents.clients._gov_report import GovReportIngestionClient

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
