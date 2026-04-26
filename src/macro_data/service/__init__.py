from __future__ import annotations

from ._calendar import CalendarOpsMixin
from ._documents import DocumentsOpsMixin
from ._market import MarketOpsMixin
from ._news import NewsOpsMixin
from ._ops_health import OpsHealthMixin
from ._timeseries import TimeseriesOpsMixin
from .base import LocalMacroDataServiceBase

__all__ = ["LocalMacroDataService"]


class LocalMacroDataService(
    CalendarOpsMixin,
    TimeseriesOpsMixin,
    DocumentsOpsMixin,
    NewsOpsMixin,
    MarketOpsMixin,
    OpsHealthMixin,
    LocalMacroDataServiceBase,
):
    pass
