"""SQLite-backed engine store for the local macro-data service.

After issue #71 Tier 2.1B the file is reduced to:

* connection-management + schema-bootstrap base methods on
  ``SQLiteEngineStore`` (``__init__`` / ``get_connection`` /
  ``_connection`` / ``init_schema``);
* the 8 per-domain query mixins composed into ``SQLiteEngineStore`` via
  multiple inheritance, each owning the SQL for its tables — see
  ``storage.queries.{calendar,documents,indicator,market,news,trading,
  messaging,analytical}``;
* backwards-compatibility re-exports of the 40 record dataclasses from
  ``storage.models`` and the calendar-vintage helper from
  ``storage.queries.calendar``, so external ``from storage.sqlite import
  XRecord`` / ``…import append_calendar_event_vintage_if_changed_with_conn``
  consumers keep working.

DDL lives in ``storage.schema``; the seed-data dictionaries that power
``seed_obs_sources_and_families`` / ``seed_calendar_indicators`` /
``seed_release_schedules`` live in their respective queries modules.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from storage.models import (
    AnalyticalObservationRecord,
    CalendarEventVintageRecord,
    CalendarIndicatorAliasRecord,
    CalendarIndicatorRecord,
    CentralBankCommunicationRecord,
    ClientProfileRecord,
    ConceptMapRecord,
    ConversationMessageRecord,
    DecisionLogRecord,
    DeliveryQueueRecord,
    DocReleaseFamilyRecord,
    DocSourceRecord,
    DocumentBlobRecord,
    DocumentExtraRecord,
    DocumentRecord,
    FundamentalsCompanyRecord,
    FundamentalsEstimatesRecord,
    FundamentalsFinancialsRecord,
    FundamentalsHighlightsRecord,
    FundamentalsRawRecord,
    GeneratedNoteRecord,
    GroupMemberRecord,
    GroupMessageRecord,
    GroupProfileRecord,
    IndicatorObservationRecord,
    IndicatorVintageRecord,
    MarketCorpActionsRawRecord,
    MarketInstrumentRecord,
    MarketPriceBarRecord,
    MarketPriceBarsRawRecord,
    MarketPriceRecord,
    MarketSymbolHistoryRecord,
    NewsArticleRecord,
    ObsFamilyDocumentRecord,
    ObsFamilyRecord,
    ObsRawRecord,
    ObsSourceRecord,
    PerformanceRecord,
    PositionStateRecord,
    RegimeSnapshotRecord,
    ReleaseScheduleRecord,
    ReleaseStatusRecord,
    ResearchArtifactRecord,
    ResolvedObservation,
    StoredEventRecord,
    TradeSignalRecord,
    TradingArtifactRecord,
    TrendTopicRecord,
    XPostRecord,
)
from storage.queries.analytical import _AnalyticalQueriesMixin
from storage.queries.calendar import (
    _CalendarQueriesMixin,
    append_calendar_event_vintage_if_changed_with_conn,
)
from storage.queries.documents import _DocumentsQueriesMixin
from storage.queries.fundamentals import _FundamentalsQueriesMixin
from storage.queries.indicator import _IndicatorQueriesMixin
from storage.queries.market import _MarketQueriesMixin
from storage.queries.messaging import _MessagingQueriesMixin
from storage.queries.news import _NewsQueriesMixin
from storage.queries.sentiment import _SentimentQueriesMixin
from storage.queries.trading import _TradingQueriesMixin
from storage.schema import apply_schema


def default_engine_db_path(root: Path | None = None) -> Path:
    base = root or Path.cwd()
    return base / ".macro-data" / "engine.db"


class SQLiteEngineStore(
    _AnalyticalQueriesMixin,
    _CalendarQueriesMixin,
    _DocumentsQueriesMixin,
    _FundamentalsQueriesMixin,
    _IndicatorQueriesMixin,
    _MarketQueriesMixin,
    _MessagingQueriesMixin,
    _NewsQueriesMixin,
    _SentimentQueriesMixin,
    _TradingQueriesMixin,
):
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or default_engine_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

    def get_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    @contextmanager
    def _connection(self, *, commit: bool) -> Iterator[sqlite3.Connection]:
        connection = self.get_connection()
        try:
            yield connection
            if commit:
                connection.commit()
        except Exception:
            if commit:
                connection.rollback()
            raise
        finally:
            connection.close()

    def init_schema(self) -> None:
        with self._connection(commit=True) as connection:
            apply_schema(connection)
