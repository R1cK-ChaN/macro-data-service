"""Market data ingestion package.

Per issue #118 the SQLite-backed yfinance snapshot lane
(``MarketPriceClient``) was retired and the market store moved to
ClickHouse — see ``storage.clickhouse``. The remaining provider clients
(Tiingo / EODHD / macro-market / identity-repair) are dormant pending
the follow-up backfill issue rewires their writers against
``ClickHouseMarketStore``.
"""
