"""Storage record dataclasses, grouped by domain.

Re-exported by storage.sqlite for backwards compatibility — every record
name remains importable from there. New consumers may import directly from
the per-domain submodule (``from storage.models.calendar import ...``)
for tighter dependency surfaces.
"""

from __future__ import annotations

from .calendar import (
    StoredEventRecord,
    CalendarIndicatorRecord,
    CalendarIndicatorAliasRecord,
    CalendarEventVintageRecord,
)
from .market import (
    MarketPriceRecord,
    MarketInstrumentRecord,
    MarketSymbolHistoryRecord,
    MarketPriceBarRecord,
    MarketCorpActionsRawRecord,
    MarketPriceBarsRawRecord,
)
from .fundamentals import (
    FundamentalsCompanyRecord,
    FundamentalsEstimatesRecord,
    FundamentalsFinancialsRecord,
    FundamentalsHighlightsRecord,
    FundamentalsRawRecord,
)
from .indicator import (
    CentralBankCommunicationRecord,
    IndicatorObservationRecord,
    IndicatorVintageRecord,
    ObsRawRecord,
    ObsSourceRecord,
    ObsFamilyRecord,
    ObsFamilyDocumentRecord,
    ConceptMapRecord,
    ResolvedObservation,
    ReleaseScheduleRecord,
    ReleaseStatusRecord,
)
from .news import (
    NewsArticleRecord,
    TrendTopicRecord,
)
from .sentiment import (
    XPostRecord,
)
from .documents import (
    DocSourceRecord,
    DocReleaseFamilyRecord,
    DocumentRecord,
    DocumentBlobRecord,
    DocumentExtraRecord,
)

__all__ = [
    "CalendarEventVintageRecord",
    "CalendarIndicatorAliasRecord",
    "CalendarIndicatorRecord",
    "CentralBankCommunicationRecord",
    "ConceptMapRecord",
    "DocReleaseFamilyRecord",
    "DocSourceRecord",
    "DocumentBlobRecord",
    "DocumentExtraRecord",
    "DocumentRecord",
    "FundamentalsCompanyRecord",
    "FundamentalsEstimatesRecord",
    "FundamentalsFinancialsRecord",
    "FundamentalsHighlightsRecord",
    "FundamentalsRawRecord",
    "IndicatorObservationRecord",
    "IndicatorVintageRecord",
    "MarketCorpActionsRawRecord",
    "MarketInstrumentRecord",
    "MarketPriceBarRecord",
    "MarketPriceBarsRawRecord",
    "MarketPriceRecord",
    "MarketSymbolHistoryRecord",
    "NewsArticleRecord",
    "ObsFamilyDocumentRecord",
    "ObsFamilyRecord",
    "ObsRawRecord",
    "ObsSourceRecord",
    "ReleaseScheduleRecord",
    "ReleaseStatusRecord",
    "ResolvedObservation",
    "StoredEventRecord",
    "TrendTopicRecord",
    "XPostRecord",
]
