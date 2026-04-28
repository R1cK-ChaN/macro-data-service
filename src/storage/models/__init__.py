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
from .analytical import (
    RegimeSnapshotRecord,
    GeneratedNoteRecord,
    AnalyticalObservationRecord,
    ResearchArtifactRecord,
)
from .trading import (
    TradeSignalRecord,
    DecisionLogRecord,
    PositionStateRecord,
    PerformanceRecord,
    TradingArtifactRecord,
)
from .messaging import (
    ClientProfileRecord,
    ConversationMessageRecord,
    DeliveryQueueRecord,
    GroupProfileRecord,
    GroupMemberRecord,
    GroupMessageRecord,
)
from .documents import (
    DocSourceRecord,
    DocReleaseFamilyRecord,
    DocumentRecord,
    DocumentBlobRecord,
    DocumentExtraRecord,
)

__all__ = [
    "AnalyticalObservationRecord",
    "CalendarEventVintageRecord",
    "CalendarIndicatorAliasRecord",
    "CalendarIndicatorRecord",
    "CentralBankCommunicationRecord",
    "ClientProfileRecord",
    "ConceptMapRecord",
    "ConversationMessageRecord",
    "DecisionLogRecord",
    "DeliveryQueueRecord",
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
    "GeneratedNoteRecord",
    "GroupMemberRecord",
    "GroupMessageRecord",
    "GroupProfileRecord",
    "IndicatorObservationRecord",
    "IndicatorVintageRecord",
    "MarketCorpActionsRawRecord",
    "MarketInstrumentRecord",
    "MarketPriceBarRecord",
    "MarketPriceRecord",
    "MarketSymbolHistoryRecord",
    "NewsArticleRecord",
    "ObsFamilyDocumentRecord",
    "ObsFamilyRecord",
    "ObsRawRecord",
    "ObsSourceRecord",
    "PerformanceRecord",
    "PositionStateRecord",
    "RegimeSnapshotRecord",
    "ReleaseScheduleRecord",
    "ReleaseStatusRecord",
    "ResearchArtifactRecord",
    "ResolvedObservation",
    "StoredEventRecord",
    "TradeSignalRecord",
    "TradingArtifactRecord",
    "TrendTopicRecord",
]
