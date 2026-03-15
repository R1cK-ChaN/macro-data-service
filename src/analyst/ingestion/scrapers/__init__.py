"""Site scrapers – calendar, news, indicators, and market data."""

from ._common import ScrapedIndicator, ScrapedMarketQuote, ScrapedNewsItem
from .bis import BISClient, BISObservation
from .ecb import (
    ECBAPIError,
    ECBClient,
    ECBDataStructure,
    ECBDataflow,
    ECBDimension,
    ECBObservation,
    ECBRateLimitError,
    ECBSizeEstimate,
    ECBStructureSummary,
)
from .eia import EIAClient, EIAObservation
from .eurostat import (
    EurostatAPIError,
    EurostatClient,
    EurostatDataStructure,
    EurostatDataflow,
    EurostatDimension,
    EurostatObservation,
    EurostatRateLimitError,
    EurostatSizeEstimate,
    EurostatStructureSummary,
)
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
from .unsd import (
    UNSDAPIError,
    UNSDClient,
    UNSDDataStructure,
    UNSDDataflow,
    UNSDDimension,
    UNSDObservation,
    UNSDRateLimitError,
    UNSDSizeEstimate,
    UNSDStructureSummary,
)
from .bloomberg import BloombergArticle, BloombergArticleClient, BloombergNewsClient
from .ft import FTArticle, FTArticleClient, FTNewsClient
from .reuters import ReutersArticle, ReutersArticleClient, ReutersNewsClient
from .treasury_fiscal import TreasuryFiscalClient, TreasuryFiscalObservation
from .worldbank import (
    WorldBankAPIError,
    WorldBankClient,
    WorldBankCountry,
    WorldBankIndicatorInfo,
    WorldBankObservation,
    WorldBankRateLimitError,
    WorldBankResponseFormatError,
    WorldBankSource,
    WorldBankTopic,
)
from .wsj import WSJArticle, WSJArticleClient, WSJNewsClient
from .gov_report import GovReportClient, GovReportItem
from .nyfed import NYFedRate, NYFedRatesClient
from .rateprobability import (
    FedMeetingProbability,
    FedRateProbability,
    RateProbabilityClient,
)
from .reddit import RedditTrendClient, RedditTrendPost
from .tradingeconomics import (
    TradingEconomicsCalendarClient,
    TradingEconomicsIndicatorsClient,
    TradingEconomicsMarketsClient,
    TradingEconomicsNewsClient,
)
from .weibo import WeiboTrendClient, WeiboTrendItem

__all__ = [
    # Data classes
    "BISObservation",
    "BloombergArticle",
    "ECBAPIError",
    "ECBDataStructure",
    "ECBDataflow",
    "ECBDimension",
    "ECBObservation",
    "ECBRateLimitError",
    "ECBSizeEstimate",
    "ECBStructureSummary",
    "EIAObservation",
    "EurostatAPIError",
    "EurostatDataStructure",
    "EurostatDataflow",
    "EurostatDimension",
    "EurostatObservation",
    "EurostatRateLimitError",
    "EurostatSizeEstimate",
    "EurostatStructureSummary",
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
    "RedditTrendPost",
    "ScrapedIndicator",
    "ScrapedMarketQuote",
    "ScrapedNewsItem",
    "TreasuryFiscalObservation",
    "UNSDAPIError",
    "UNSDDataStructure",
    "UNSDDataflow",
    "UNSDDimension",
    "UNSDObservation",
    "UNSDRateLimitError",
    "UNSDSizeEstimate",
    "UNSDStructureSummary",
    "WSJArticle",
    "WorldBankAPIError",
    "WorldBankCountry",
    "WorldBankIndicatorInfo",
    "WorldBankObservation",
    "WorldBankRateLimitError",
    "WorldBankResponseFormatError",
    "WorldBankSource",
    "WorldBankTopic",
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
    "RedditTrendClient",
    "WeiboTrendClient",
    "WeiboTrendItem",
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
    "UNSDClient",
    "WorldBankClient",
    # Indicators & Markets
    "TradingEconomicsIndicatorsClient",
    "TradingEconomicsMarketsClient",
    # Rate Probabilities & Reference Rates
    "NYFedRatesClient",
    "RateProbabilityClient",
]
