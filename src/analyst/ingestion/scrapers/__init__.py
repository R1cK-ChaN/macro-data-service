"""Site scrapers – calendar, news, indicators, and market data."""

from ._common import ScrapedIndicator, ScrapedMarketQuote, ScrapedNewsItem
from .bis import BISClient, BISObservation
from .ecb import ECBClient, ECBObservation
from .eia import EIAClient, EIAObservation
from .eurostat import EurostatClient, EurostatObservation
from .forexfactory import ForexFactoryCalendarClient, ForexFactoryNewsClient
from .fred import FredClient, FredObservation, FredVintageObservation
from .imf import IMFClient, IMFObservation, IMFVintageObservation
from .investing import InvestingCalendarClient, InvestingNewsClient
from .oecd import (
    OECDAPIError,
    OECDClient,
    OECDCode,
    OECDDataStructure,
    OECDDataflow,
    OECDDimension,
    OECDObservation,
    OECDRateLimitError,
    OECDResponseFormatError,
    OECDSeries,
    OECDStructureSummary,
)
from .bloomberg import BloombergArticle, BloombergArticleClient, BloombergNewsClient
from .ft import FTArticle, FTArticleClient, FTNewsClient
from .reuters import ReutersArticle, ReutersArticleClient, ReutersNewsClient
from .treasury_fiscal import TreasuryFiscalClient, TreasuryFiscalObservation
from .worldbank import WorldBankClient, WorldBankObservation
from .wsj import WSJArticle, WSJArticleClient, WSJNewsClient
from .gov_report import GovReportClient, GovReportItem
from .nyfed import NYFedRate, NYFedRatesClient
from .rateprobability import (
    FedMeetingProbability,
    FedRateProbability,
    RateProbabilityClient,
)
from .tradingeconomics import (
    TradingEconomicsCalendarClient,
    TradingEconomicsIndicatorsClient,
    TradingEconomicsMarketsClient,
    TradingEconomicsNewsClient,
)

__all__ = [
    # Data classes
    "BISObservation",
    "BloombergArticle",
    "ECBObservation",
    "EIAObservation",
    "EurostatObservation",
    "FTArticle",
    "FedMeetingProbability",
    "FedRateProbability",
    "FredObservation",
    "FredVintageObservation",
    "GovReportClient",
    "GovReportItem",
    "IMFObservation",
    "IMFVintageObservation",
    "NYFedRate",
    "OECDAPIError",
    "OECDCode",
    "OECDDataStructure",
    "OECDDataflow",
    "OECDDimension",
    "OECDObservation",
    "OECDRateLimitError",
    "OECDResponseFormatError",
    "OECDSeries",
    "OECDStructureSummary",
    "ReutersArticle",
    "ScrapedIndicator",
    "ScrapedMarketQuote",
    "ScrapedNewsItem",
    "TreasuryFiscalObservation",
    "WSJArticle",
    "WorldBankObservation",
    # Calendar
    "ForexFactoryCalendarClient",
    "InvestingCalendarClient",
    "TradingEconomicsCalendarClient",
    # News
    "BloombergNewsClient",
    "FTNewsClient",
    "ForexFactoryNewsClient",
    "InvestingNewsClient",
    "ReutersNewsClient",
    "TradingEconomicsNewsClient",
    "WSJNewsClient",
    # Articles
    "BloombergArticleClient",
    "FTArticleClient",
    "ReutersArticleClient",
    "WSJArticleClient",
    # Structured Data APIs
    "BISClient",
    "ECBClient",
    "EIAClient",
    "EurostatClient",
    "FredClient",
    "IMFClient",
    "OECDClient",
    "TreasuryFiscalClient",
    "WorldBankClient",
    # Indicators & Markets
    "TradingEconomicsIndicatorsClient",
    "TradingEconomicsMarketsClient",
    # Rate Probabilities & Reference Rates
    "NYFedRatesClient",
    "RateProbabilityClient",
]
