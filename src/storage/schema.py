"""SQLite schema DDL for the local macro-data engine database.

Extracted from ``storage.sqlite.SQLiteEngineStore.init_schema`` in
issue #71 Tier 2.1B-1. ``apply_schema`` runs every CREATE TABLE / CREATE
INDEX / additive ALTER on a caller-supplied connection and is invoked once
per ``SQLiteEngineStore`` instance from inside its commit-bracketed
``init_schema`` wrapper. The transaction boundary stays on the EngineStore
side; this module is pure DDL.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


def _ensure_table_columns(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    columns: dict[str, str],
) -> None:
    existing = {
        row["name"]
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for column_name, column_def in columns.items():
        if column_name in existing:
            continue
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


def apply_schema(connection: sqlite3.Connection) -> None:
    """Create / migrate every table & index this engine relies on.

    Idempotent — every CREATE uses ``IF NOT EXISTS`` and every additive
    column migration goes through ``_ensure_table_columns``. Caller owns
    the transaction (``SQLiteEngineStore.init_schema`` wraps this in a
    commit-bracketed ``_connection`` context).
    """
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS calendar_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            event_id TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            country TEXT NOT NULL,
            indicator TEXT NOT NULL,
            category TEXT NOT NULL,
            importance TEXT NOT NULL,
            actual TEXT,
            forecast TEXT,
            previous TEXT,
            revised_previous TEXT,
            surprise REAL,
            unit TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            UNIQUE(source, event_id)
        )
        """
    )
    try:
        connection.execute("ALTER TABLE calendar_events ADD COLUMN currency TEXT NOT NULL DEFAULT ''")
    except sqlite3.OperationalError:
        pass  # column already exists
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            change_pct REAL,
            timestamp INTEGER NOT NULL,
            scraped_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_instruments (
            instrument_id TEXT PRIMARY KEY,
            primary_ticker TEXT NOT NULL,
            name TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            market TEXT NOT NULL,
            exchange_code TEXT NOT NULL DEFAULT '',
            currency TEXT NOT NULL DEFAULT 'USD',
            isin TEXT NOT NULL DEFAULT '',
            openfigi TEXT NOT NULL DEFAULT '',
            composite_figi TEXT NOT NULL DEFAULT '',
            share_class_figi TEXT NOT NULL DEFAULT '',
            cusip TEXT NOT NULL DEFAULT '',
            lei TEXT NOT NULL DEFAULT '',
            primary_provider TEXT NOT NULL DEFAULT 'tiingo',
            provider_symbols_json TEXT NOT NULL DEFAULT '{}',
            history_status TEXT NOT NULL DEFAULT 'provider_continuous',
            description_for_agent TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_market_instruments_ticker ON market_instruments(primary_ticker)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_symbol_history (
            segment_id TEXT PRIMARY KEY,
            instrument_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            provider_name TEXT NOT NULL,
            exchange_code TEXT NOT NULL DEFAULT '',
            isin TEXT NOT NULL DEFAULT '',
            figi TEXT NOT NULL DEFAULT '',
            valid_from TEXT NOT NULL,
            valid_to TEXT NOT NULL DEFAULT '',
            event_type TEXT NOT NULL DEFAULT 'listing_start',
            mapping_confidence TEXT NOT NULL DEFAULT 'provider_native',
            source_name TEXT NOT NULL DEFAULT '',
            raw_json TEXT NOT NULL DEFAULT '{}',
            collected_at TEXT NOT NULL,
            FOREIGN KEY(instrument_id) REFERENCES market_instruments(instrument_id)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_symbol_history_instrument ON market_symbol_history(instrument_id)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_symbol_history_ticker ON market_symbol_history(ticker)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_price_bars (
            instrument_id TEXT NOT NULL,
            source_segment_id TEXT NOT NULL DEFAULT '',
            date TEXT NOT NULL,
            bar_interval TEXT NOT NULL DEFAULT '1d',
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            adjusted_open REAL,
            adjusted_high REAL,
            adjusted_low REAL,
            adjusted_close REAL,
            adjusted_volume REAL,
            dividend_cash REAL NOT NULL DEFAULT 0,
            split_factor REAL NOT NULL DEFAULT 1,
            source_name TEXT NOT NULL,
            source_symbol TEXT NOT NULL,
            has_break_detected INTEGER NOT NULL DEFAULT 0,
            has_pre2018_delisted INTEGER NOT NULL DEFAULT 0,
            has_missing_corp_acts INTEGER NOT NULL DEFAULT 0,
            has_mapping_review_needed INTEGER NOT NULL DEFAULT 0,
            quality_flags_json TEXT NOT NULL DEFAULT '{}',
            collected_at TEXT NOT NULL,
            PRIMARY KEY (instrument_id, date, bar_interval, source_name, source_symbol)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_price_bars_instrument_date ON market_price_bars(instrument_id, date)"
    )
    # ── Market corporate-actions audit lane (issue #67 slice 2) ──────────
    # Structurally mirrors cal_corp_raw — same content-hash + raw-payload
    # + snapshot pattern — but kept as an independent table because the
    # two domains evolve independently: cal_corp_raw is event-shaped (one
    # row per discovered calendar event); market_corp_actions_raw is
    # ticker-shaped (one row per ticker × action × event_date). Same
    # shape, different consumers, deliberately separate code per
    # CLAUDE.md rule 3 (only two callers — wait for a third before
    # collapsing).
    #
    # Why this lane exists at all (audit-layer rationale):
    #   * Every value of market_price_bars.dividend_cash / .split_factor
    #     is reproducible from market_corp_actions_raw — so projection
    #     can be re-run after a bug fix without re-fetching from EODHD
    #     (zero quota cost).
    #   * Restatement detection — a revised dividend amount inserts a
    #     new row (different content_hash) instead of overwriting; the
    #     full revision chain stays queryable.
    #   * Schema-drift insurance — payload_json keeps the raw EODHD row
    #     verbatim so even renamed/added fields stay parseable.
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_corp_actions_raw (
            provider           TEXT NOT NULL,
            ticker             TEXT NOT NULL,
            action_type        TEXT NOT NULL
                CHECK (action_type IN ('dividend','split')),
            event_date         TEXT NOT NULL,
            snapshot_epoch_ms  INTEGER NOT NULL,
            content_hash       TEXT NOT NULL,
            payload_json       TEXT NOT NULL,
            fetched_at         TEXT NOT NULL,
            PRIMARY KEY (provider, ticker, action_type, event_date, content_hash)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_market_corp_actions_raw_latest "
        "ON market_corp_actions_raw(provider, ticker, action_type, event_date, snapshot_epoch_ms DESC)"
    )
    # ── Market price-bars audit lane (issue #69 slice 2) ─────────────────
    # Mirrors cal_corp_raw / market_corp_actions_raw — same content-hash +
    # raw-payload + snapshot pattern. One row per HTTP response (per
    # provider × ticker × bar window): the ``payload_json`` holds the full
    # EODHD ``/api/eod`` or Tiingo ``/tiingo/daily/{t}/prices`` body.
    # ``content_hash`` is sha256 over the canonicalized bar array (sorted
    # by date, volatile envelope fields dropped) so re-fetching unchanged
    # data dedupes.
    #
    # Audit floor — historical bars already in market_price_bars cannot be
    # replayed into raw because providers only return their current-best
    # version. This table captures from "first write after #69 ships".
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_price_bars_raw (
            provider            TEXT NOT NULL,
            ticker              TEXT NOT NULL,
            snapshot_epoch_ms   INTEGER NOT NULL,
            content_hash        TEXT NOT NULL,
            payload_json        TEXT NOT NULL,
            fetched_at          TEXT NOT NULL,
            request_params_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (provider, ticker, content_hash)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_market_price_bars_raw_latest "
        "ON market_price_bars_raw(provider, ticker, snapshot_epoch_ms DESC)"
    )
    # ── EODHD fundamentals (issue #68 slice 1) ───────────────────────────
    # One raw audit lane + four typed projections. Same content-hash +
    # observed-at PIT discipline as cal_corp_*: raw is append-only on
    # (provider, ticker, content_hash); projections only update when
    # the incoming observed_at_epoch_ms is at least as recent as the
    # stored value, so a late-arriving older snapshot cannot overwrite
    # a newer view. Restated past quarters land as a new raw row;
    # ``as_of`` PIT queries reconstruct historical projections by
    # scanning raw at-or-before a target epoch.
    #
    # Per-section split:
    #   _company    — General block; one row per ticker (sector / FY end).
    #   _financials — Financials.{IS,BS,CF} × {Q,A}; PK by period_end.
    #   _highlights — Highlights + Valuation + SharesStats merged; ~daily.
    #   _estimates  — schema-only in slice 1; population deferred.
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_raw (
            provider           TEXT NOT NULL,
            ticker             TEXT NOT NULL,
            snapshot_epoch_ms  INTEGER NOT NULL,
            content_hash       TEXT NOT NULL,
            payload_json       TEXT NOT NULL,
            fetched_at         TEXT NOT NULL,
            PRIMARY KEY (provider, ticker, content_hash)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_fundamentals_raw_latest "
        "ON fundamentals_raw(provider, ticker, snapshot_epoch_ms DESC)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_company (
            provider             TEXT NOT NULL,
            ticker               TEXT NOT NULL,
            name                 TEXT NOT NULL DEFAULT '',
            asset_type           TEXT NOT NULL DEFAULT '',
            sector               TEXT NOT NULL DEFAULT '',
            industry             TEXT NOT NULL DEFAULT '',
            fiscal_year_end      TEXT NOT NULL DEFAULT '',
            listing_exchange     TEXT NOT NULL DEFAULT '',
            currency_code        TEXT NOT NULL DEFAULT '',
            country_iso          TEXT NOT NULL DEFAULT '',
            isin                 TEXT NOT NULL DEFAULT '',
            cusip                TEXT NOT NULL DEFAULT '',
            payload_json         TEXT NOT NULL DEFAULT '{}',
            content_hash         TEXT NOT NULL,
            observed_at_epoch_ms INTEGER NOT NULL,
            created_at           TEXT NOT NULL,
            updated_at           TEXT NOT NULL,
            PRIMARY KEY (provider, ticker)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_financials (
            provider             TEXT NOT NULL,
            ticker               TEXT NOT NULL,
            period_end           TEXT NOT NULL,
            period_type          TEXT NOT NULL
                CHECK (period_type IN ('Q','A')),
            statement            TEXT NOT NULL
                CHECK (statement IN ('IS','BS','CF')),
            currency             TEXT NOT NULL DEFAULT '',
            filing_date          TEXT NOT NULL DEFAULT '',
            revenue              REAL,
            net_income           REAL,
            eps_basic            REAL,
            total_assets         REAL,
            total_equity         REAL,
            total_liabilities    REAL,
            cash_from_ops        REAL,
            capex                REAL,
            payload_json         TEXT NOT NULL DEFAULT '{}',
            content_hash         TEXT NOT NULL,
            observed_at_epoch_ms INTEGER NOT NULL,
            created_at           TEXT NOT NULL,
            updated_at           TEXT NOT NULL,
            PRIMARY KEY (provider, ticker, period_end, period_type, statement)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_fundamentals_financials_ticker_period "
        "ON fundamentals_financials(ticker, period_type, period_end DESC)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_highlights (
            provider             TEXT NOT NULL,
            ticker               TEXT NOT NULL,
            as_of_date           TEXT NOT NULL,
            market_cap           REAL,
            pe_ratio             REAL,
            eps_ttm              REAL,
            dividend_yield       REAL,
            book_value           REAL,
            shares_outstanding   REAL,
            payload_json         TEXT NOT NULL DEFAULT '{}',
            content_hash         TEXT NOT NULL,
            observed_at_epoch_ms INTEGER NOT NULL,
            created_at           TEXT NOT NULL,
            updated_at           TEXT NOT NULL,
            PRIMARY KEY (provider, ticker, as_of_date)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_estimates (
            provider             TEXT NOT NULL,
            ticker               TEXT NOT NULL,
            period_end           TEXT NOT NULL,
            period_type          TEXT NOT NULL
                CHECK (period_type IN ('Q','A')),
            metric               TEXT NOT NULL,
            value                REAL,
            analyst_count        INTEGER,
            payload_json         TEXT NOT NULL DEFAULT '{}',
            content_hash         TEXT NOT NULL,
            observed_at_epoch_ms INTEGER NOT NULL,
            created_at           TEXT NOT NULL,
            updated_at           TEXT NOT NULL,
            PRIMARY KEY (provider, ticker, period_end, period_type, metric)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS central_bank_comms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            timestamp INTEGER NOT NULL,
            content_type TEXT NOT NULL,
            speaker TEXT NOT NULL,
            summary TEXT NOT NULL,
            full_text TEXT NOT NULL,
            scraped_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id TEXT NOT NULL,
            source TEXT NOT NULL,
            date TEXT NOT NULL,
            value REAL NOT NULL,
            metadata_json TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            UNIQUE(series_id, source, date)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS indicator_vintages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id TEXT NOT NULL,
            source TEXT NOT NULL,
            observation_date TEXT NOT NULL,
            vintage_date TEXT NOT NULL,
            value REAL NOT NULL,
            metadata_json TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            UNIQUE(series_id, source, observation_date, vintage_date)
        )
        """
    )
    # ── Macro time-series audit lane (issue #69 slice 1) ─────────────────
    # Mirrors cal_econ_raw — same content-hash + raw-payload + snapshot
    # pattern, ticker-shaped at the source/series_id grain. One row per
    # HTTP response (per source × series_id × snapshot): ``payload_json``
    # holds the FRED / BLS / SDMX response verbatim, ``content_hash`` is
    # sha256 over the canonicalized observations (sorted by date with
    # query-time echo fields dropped) so re-fetching unchanged data
    # dedupes via the INSERT OR IGNORE PK.
    #
    # ``indicator_vintages`` already stores typed values + vintage history
    # but discards the upstream byte stream — this table closes that gap:
    # parser bug or schema change → fix code, re-project from raw, zero
    # FRED/BLS quota consumed. Audit floor is "first write after #69".
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS obs_raw (
            source              TEXT NOT NULL,
            series_id           TEXT NOT NULL,
            snapshot_epoch_ms   INTEGER NOT NULL,
            content_hash        TEXT NOT NULL,
            payload_json        TEXT NOT NULL,
            fetched_at          TEXT NOT NULL,
            request_params_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (source, series_id, content_hash)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_raw_latest "
        "ON obs_raw(source, series_id, snapshot_epoch_ms DESC)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS calendar_event_vintages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            provider TEXT NOT NULL,
            vintage_date TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            actual TEXT,
            forecast TEXT,
            previous TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            scraped_at TEXT NOT NULL,
            source_url TEXT NOT NULL DEFAULT '',
            evidence_archive_url TEXT,
            evidence_last_attempt_at TEXT,
            UNIQUE(event_id, provider, vintage_date)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_calendar_event_vintages_lookup "
        "ON calendar_event_vintages(event_id, provider, observed_at)"
    )
    # Backfill the issue-#36 columns onto pre-existing engines.
    _ensure_table_columns(
        connection,
        table_name="calendar_event_vintages",
        columns={
            "source_url": "TEXT NOT NULL DEFAULT ''",
            "evidence_archive_url": "TEXT",
            # Stamp on every Wayback submission attempt so failed
            # rows rotate to the back of the retry queue and a
            # block of unarchivable URLs cannot stall the head.
            "evidence_last_attempt_at": "TEXT",
        },
    )
    # Partial index drives the retry-tail scan over rows whose
    # archive submission has not yet succeeded.
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_evidence_pending "
        "ON calendar_event_vintages(observed_at) "
        "WHERE evidence_archive_url IS NULL"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS news_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url_hash TEXT NOT NULL UNIQUE,
            source_feed TEXT NOT NULL,
            feed_category TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            description TEXT NOT NULL,
            content_markdown TEXT NOT NULL,
            impact_level TEXT NOT NULL,
            finance_category TEXT NOT NULL,
            confidence REAL NOT NULL,
            content_fetched INTEGER NOT NULL DEFAULT 0,
            scraped_at TEXT NOT NULL
        )
        """
    )
    # -- news_articles new columns for LLM extraction -----------
    _news_new_cols = [
        ("institution", "TEXT NOT NULL DEFAULT ''"),
        ("country", "TEXT NOT NULL DEFAULT ''"),
        ("market", "TEXT NOT NULL DEFAULT ''"),
        ("asset_class", "TEXT NOT NULL DEFAULT ''"),
        ("sector", "TEXT NOT NULL DEFAULT ''"),
        ("document_type", "TEXT NOT NULL DEFAULT ''"),
        ("event_type", "TEXT NOT NULL DEFAULT ''"),
        ("subject", "TEXT NOT NULL DEFAULT ''"),
        ("subject_id", "TEXT NOT NULL DEFAULT ''"),
        ("data_period", "TEXT NOT NULL DEFAULT ''"),
        ("contains_commentary", "INTEGER NOT NULL DEFAULT 0"),
        ("language", "TEXT NOT NULL DEFAULT 'en'"),
        ("authors", "TEXT NOT NULL DEFAULT ''"),
        ("extraction_provider", "TEXT NOT NULL DEFAULT 'keyword'"),
    ]
    for col_name, col_def in _news_new_cols:
        try:
            connection.execute(f"ALTER TABLE news_articles ADD COLUMN {col_name} {col_def}")
        except sqlite3.OperationalError:
            pass
    # -- FTS5 full-text search for news articles ----------------
    # Guarded: SQLite builds without FTS5 skip this block;
    # search_news() falls back to LIKE queries.
    try:
        connection.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS news_fts USING fts5(
                title, description, subject,
                content='news_articles',
                content_rowid='id'
            )
            """
        )
        for trigger_name in ("news_fts_ai", "news_fts_ad", "news_fts_au"):
            connection.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
        connection.execute(
            """
            CREATE TRIGGER news_fts_ai AFTER INSERT ON news_articles BEGIN
                INSERT INTO news_fts(rowid, title, description, subject)
                VALUES (new.id, new.title, new.description, new.subject);
            END
            """
        )
        connection.execute(
            """
            CREATE TRIGGER news_fts_ad AFTER DELETE ON news_articles BEGIN
                INSERT INTO news_fts(news_fts, rowid, title, description, subject)
                VALUES ('delete', old.id, old.title, old.description, old.subject);
            END
            """
        )
        connection.execute(
            """
            CREATE TRIGGER news_fts_au AFTER UPDATE ON news_articles BEGIN
                INSERT INTO news_fts(news_fts, rowid, title, description, subject)
                VALUES ('delete', old.id, old.title, old.description, old.subject);
                INSERT INTO news_fts(rowid, title, description, subject)
                VALUES (new.id, new.title, new.description, new.subject);
            END
            """
        )
        connection.execute("INSERT INTO news_fts(news_fts) VALUES('rebuild')")
    except sqlite3.OperationalError:
        pass  # FTS5 not available; search_news() will use LIKE fallback
    # -- Article fingerprint table for multi-layer dedup ----------
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS article_fingerprint (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url_hash TEXT NOT NULL,
            title_hash TEXT NOT NULL,
            canonical_url TEXT NOT NULL,
            raw_url TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            source_feed TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_fp_url ON article_fingerprint(url_hash)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_fp_title ON article_fingerprint(title_hash)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS trend_topics (
            trend_id TEXT PRIMARY KEY,
            provider TEXT NOT NULL,
            provider_topic_id TEXT NOT NULL,
            title_raw TEXT NOT NULL,
            topic TEXT NOT NULL,
            summary TEXT NOT NULL,
            keywords_json TEXT NOT NULL DEFAULT '[]',
            category TEXT NOT NULL,
            region TEXT NOT NULL,
            popularity_score REAL NOT NULL,
            provider_rank INTEGER NOT NULL DEFAULT 0,
            engagement_score REAL NOT NULL DEFAULT 0,
            comment_count INTEGER NOT NULL DEFAULT 0,
            observed_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            raw_json TEXT NOT NULL DEFAULT '{}',
            normalized_topic_hash TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            UNIQUE(provider, provider_topic_id)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_trend_topics_active "
        "ON trend_topics(expires_at, observed_at)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_trend_topics_scope "
        "ON trend_topics(category, region)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_trend_topics_popularity "
        "ON trend_topics(popularity_score DESC, observed_at DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_trend_topics_normalized "
        "ON trend_topics(normalized_topic_hash)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            regime_json TEXT NOT NULL,
            trigger_event TEXT NOT NULL,
            summary TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS generated_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            note_type TEXT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT NOT NULL,
            body_markdown TEXT NOT NULL,
            regime_json TEXT,
            metadata_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS analytical_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_type TEXT NOT NULL,
            summary TEXT NOT NULL,
            detail TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            source_id INTEGER NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS research_artifacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artifact_type TEXT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT NOT NULL,
            content_markdown TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            source_id INTEGER NOT NULL,
            tags_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_type TEXT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT NOT NULL,
            rationale_markdown TEXT NOT NULL,
            signal_json TEXT NOT NULL,
            confidence REAL NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            decision_type TEXT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT NOT NULL,
            rationale_markdown TEXT NOT NULL,
            research_artifact_id INTEGER,
            signal_id INTEGER,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (research_artifact_id) REFERENCES research_artifacts(id),
            FOREIGN KEY (signal_id) REFERENCES trade_signals(id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS position_state (
            symbol TEXT PRIMARY KEY,
            exposure REAL NOT NULL,
            direction TEXT NOT NULL,
            thesis TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS performance_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_name TEXT NOT NULL,
            metric_value REAL NOT NULL,
            period_label TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS trading_artifacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artifact_type TEXT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT NOT NULL,
            rationale_markdown TEXT NOT NULL,
            research_artifact_id INTEGER NOT NULL,
            decision_log_id INTEGER,
            signal_json TEXT NOT NULL,
            confidence REAL NOT NULL,
            tags_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (research_artifact_id) REFERENCES research_artifacts(id),
            FOREIGN KEY (decision_log_id) REFERENCES decision_log(id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS client_profiles (
            client_id TEXT PRIMARY KEY,
            preferred_language TEXT NOT NULL DEFAULT '',
            watchlist_topics_json TEXT NOT NULL DEFAULT '[]',
            response_style TEXT NOT NULL DEFAULT '',
            risk_appetite TEXT NOT NULL DEFAULT '',
            investment_horizon TEXT NOT NULL DEFAULT '',
            institution_type TEXT NOT NULL DEFAULT '',
            risk_preference TEXT NOT NULL DEFAULT '',
            asset_focus_json TEXT NOT NULL DEFAULT '[]',
            market_focus_json TEXT NOT NULL DEFAULT '[]',
            expertise_level TEXT NOT NULL DEFAULT '',
            activity TEXT NOT NULL DEFAULT '',
            current_mood TEXT NOT NULL DEFAULT '',
            confidence TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT '',
            last_active_at TEXT NOT NULL DEFAULT '',
            total_interactions INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
        """
    )
    _ensure_table_columns(
        connection,
        table_name="client_profiles",
        columns={
            "institution_type": "TEXT NOT NULL DEFAULT ''",
            "risk_preference": "TEXT NOT NULL DEFAULT ''",
            "asset_focus_json": "TEXT NOT NULL DEFAULT '[]'",
            "market_focus_json": "TEXT NOT NULL DEFAULT '[]'",
            "expertise_level": "TEXT NOT NULL DEFAULT ''",
            "activity": "TEXT NOT NULL DEFAULT ''",
            "current_mood": "TEXT NOT NULL DEFAULT ''",
            "emotional_trend": "TEXT NOT NULL DEFAULT ''",
            "stress_level": "TEXT NOT NULL DEFAULT ''",
            "confidence": "TEXT NOT NULL DEFAULT ''",
            "notes": "TEXT NOT NULL DEFAULT ''",
            "personal_facts_json": "TEXT NOT NULL DEFAULT '[]'",
        },
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS conversation_threads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            last_active_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            UNIQUE(client_id, channel, thread_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS conversation_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (client_id, channel, thread_id)
                REFERENCES conversation_threads(client_id, channel, thread_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS delivery_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_artifact_id INTEGER,
            content_rendered TEXT NOT NULL,
            status TEXT NOT NULL,
            delivered_at TEXT,
            client_reaction TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_analytical_observations_created ON analytical_observations(id DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_research_artifacts_type_created ON research_artifacts(artifact_type, id DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_trading_artifacts_research_created ON trading_artifacts(research_artifact_id, id DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_decision_log_research_created ON decision_log(research_artifact_id, id DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_performance_records_metric_created ON performance_records(metric_name, id DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_conversation_messages_thread_created ON conversation_messages(client_id, channel, thread_id, id DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_delivery_queue_client_created ON delivery_queue(client_id, channel, thread_id, id DESC)"
    )
    # -- Portfolio volatility management tables ---------------------
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id TEXT NOT NULL DEFAULT 'default',
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            weight REAL NOT NULL,
            notional REAL NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(portfolio_id, symbol)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_vol_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id TEXT NOT NULL DEFAULT 'default',
            snapshot_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_vol_snapshots_portfolio ON portfolio_vol_snapshots(portfolio_id, id DESC)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id TEXT NOT NULL DEFAULT 'default',
            alert_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            message TEXT NOT NULL,
            acknowledged INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS subagent_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            parent_agent TEXT NOT NULL,
            task_type TEXT NOT NULL,
            objective TEXT NOT NULL,
            scope_tags_json TEXT NOT NULL,
            status TEXT NOT NULL,
            summary TEXT NOT NULL DEFAULT '',
            elapsed_seconds REAL NOT NULL DEFAULT 0.0,
            created_at TEXT NOT NULL
        )
        """
    )
    # -- Three-layer memory: group tables --------------------------------
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS group_profiles (
            group_id TEXT PRIMARY KEY,
            group_name TEXT NOT NULL DEFAULT '',
            group_topic TEXT NOT NULL DEFAULT '',
            group_notes TEXT NOT NULL DEFAULT '',
            member_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS group_members (
            group_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            display_name TEXT NOT NULL DEFAULT '',
            role_in_group TEXT NOT NULL DEFAULT '',
            personality_notes TEXT NOT NULL DEFAULT '',
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            message_count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (group_id, user_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS group_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT NOT NULL,
            thread_id TEXT NOT NULL DEFAULT 'main',
            user_id TEXT NOT NULL,
            display_name TEXT NOT NULL DEFAULT '',
            content TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_group_messages_group_thread "
        "ON group_messages(group_id, thread_id, id DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_group_members_group "
        "ON group_members(group_id, last_seen_at DESC)"
    )
    # -- Document storage: 5-table normalized schema --------------------
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS doc_source (
            source_id TEXT PRIMARY KEY,
            source_code TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_type TEXT NOT NULL
                CHECK (source_type IN (
                    'government_agency', 'central_bank', 'intl_org',
                    'statistics_bureau', 'news_agency'
                )),
            country_code TEXT NOT NULL CHECK (length(country_code) = 2),
            default_language_code TEXT CHECK (length(default_language_code) IN (2, 5)),
            homepage_url TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS doc_release_family (
            release_family_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            release_code TEXT NOT NULL,
            release_name TEXT NOT NULL,
            topic_code TEXT NOT NULL,
            country_code TEXT NOT NULL CHECK (length(country_code) = 2),
            frequency TEXT,
            default_language_code TEXT CHECK (default_language_code IS NULL OR length(default_language_code) IN (2, 5)),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (source_id) REFERENCES doc_source(source_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS document (
            document_id TEXT PRIMARY KEY,
            release_family_id TEXT,
            source_id TEXT NOT NULL,
            canonical_url TEXT NOT NULL,
            title TEXT NOT NULL,
            subtitle TEXT NOT NULL DEFAULT '',
            document_type TEXT NOT NULL
                CHECK (document_type IN (
                    'release', 'bulletin', 'speech', 'methodology',
                    'revision_notice', 'minutes', 'statement',
                    'press_release', 'report', 'outlook'
                )),
            mime_type TEXT NOT NULL DEFAULT 'text/html',
            language_code TEXT NOT NULL CHECK (length(language_code) IN (2, 5)),
            country_code TEXT NOT NULL CHECK (length(country_code) = 2),
            topic_code TEXT NOT NULL,
            published_date TEXT NOT NULL,
            published_at TEXT,
            published_precision TEXT NOT NULL DEFAULT 'date_only',
            published_epoch_ms INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'published'
                CHECK (status IN ('published', 'revised', 'superseded', 'withdrawn')),
            version_no INTEGER NOT NULL DEFAULT 1,
            parent_document_id TEXT,
            hash_sha256 TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            created_epoch_ms INTEGER NOT NULL DEFAULT 0,
            updated_epoch_ms INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (release_family_id) REFERENCES doc_release_family(release_family_id),
            FOREIGN KEY (source_id) REFERENCES doc_source(source_id),
            FOREIGN KEY (parent_document_id) REFERENCES document(document_id)
        )
        """
    )
    _ensure_table_columns(
        connection,
        table_name="document",
        columns={
            "published_precision": "TEXT NOT NULL DEFAULT 'date_only'",
            "published_epoch_ms": "INTEGER NOT NULL DEFAULT 0",
            "created_epoch_ms": "INTEGER NOT NULL DEFAULT 0",
            "updated_epoch_ms": "INTEGER NOT NULL DEFAULT 0",
            # ── 17-field LLM-extraction fields (information-layer) ──
            # Added for issue #3: port doc_parser / gov_report / news
            # pipelines onto the unified document table. All default
            # blank/zero so existing rows and non-LLM-extracted
            # sources stay valid.
            "institution": "TEXT NOT NULL DEFAULT ''",
            "authors": "TEXT NOT NULL DEFAULT ''",
            "data_period": "TEXT NOT NULL DEFAULT ''",
            "market": "TEXT NOT NULL DEFAULT ''",
            "asset_class": "TEXT NOT NULL DEFAULT ''",
            "sector": "TEXT NOT NULL DEFAULT ''",
            "event_type": "TEXT NOT NULL DEFAULT ''",
            "impact_level": "TEXT NOT NULL DEFAULT ''",
            "contains_commentary": "INTEGER NOT NULL DEFAULT 0",
            "confidence": "REAL NOT NULL DEFAULT 0",
            # Free-text subject string produced by the LLM before it is
            # resolved to a canonical subject_id. Stored for audit;
            # queries go through item_subjects / subject_aliases.
            "subject_freetext": "TEXT NOT NULL DEFAULT ''",
        },
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS document_blob (
            document_blob_id TEXT PRIMARY KEY,
            document_id TEXT NOT NULL,
            blob_role TEXT NOT NULL
                CHECK (blob_role IN (
                    'raw_pdf', 'raw_html', 'clean_html',
                    'plain_text', 'markdown'
                )),
            storage_path TEXT,
            content_text TEXT,
            content_bytes BLOB,
            byte_size INTEGER,
            encoding TEXT,
            parser_name TEXT,
            parser_version TEXT,
            extracted_at TEXT,
            FOREIGN KEY (document_id) REFERENCES document(document_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS document_extra (
            document_id TEXT PRIMARY KEY,
            extra_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY (document_id) REFERENCES document(document_id)
        )
        """
    )
    # -- RAG sync watermarks ----------------------------------------
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS rag_sync_watermarks (
            source_type TEXT PRIMARY KEY,
            last_synced_id INTEGER NOT NULL DEFAULT 0,
            last_synced_at TEXT NOT NULL
        )
        """
    )
    # -- Document storage indexes ----------------------------------------
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_document_url "
        "ON document(canonical_url)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_document_source_date "
        "ON document(source_id, published_date)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_document_release_date "
        "ON document(release_family_id, published_date)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_document_country_topic_date "
        "ON document(country_code, topic_code, published_date)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_document_published_epoch "
        "ON document(published_epoch_ms)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_document_status "
        "ON document(status)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_blob_document_role "
        "ON document_blob(document_id, blob_role)"
    )
    # -- Filter indexes for the 17-field extension --------------------
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_document_impact_level "
        "ON document(impact_level)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_document_asset_class "
        "ON document(asset_class)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_document_event_type "
        "ON document(event_type)"
    )
    # -- FTS5 over document title + body ------------------------------
    # Contentless (no content= link) — body lives in document_blob,
    # so upsert_document_fts() writes the denormalized title+body
    # row whenever a document or its markdown blob changes. Guarded
    # against SQLite builds without FTS5; search_documents() falls
    # back to LIKE if the virtual table is absent.
    try:
        connection.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
                document_id UNINDEXED,
                title,
                body,
                tokenize = 'porter unicode61'
            )
            """
        )
    except sqlite3.OperationalError:
        pass  # FTS5 unavailable; document search falls back to LIKE
    # -- Observation family: 3-table hierarchy --------------------------
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS obs_source (
            source_id TEXT PRIMARY KEY,
            source_code TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_type TEXT NOT NULL
                CHECK (source_type IN (
                    'data_aggregator', 'government_agency', 'central_bank',
                    'exchange', 'market_data'
                )),
            country_code TEXT NOT NULL CHECK (length(country_code) = 2),
            homepage_url TEXT,
            api_base_url TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS obs_family (
            family_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            provider_series_id TEXT NOT NULL,
            canonical_name TEXT NOT NULL,
            short_name TEXT NOT NULL DEFAULT '',
            unit TEXT NOT NULL DEFAULT '',
            frequency TEXT NOT NULL DEFAULT 'irregular'
                CHECK (frequency IN (
                    'daily','weekly','monthly','quarterly','annual','irregular'
                )),
            seasonal_adjustment TEXT NOT NULL DEFAULT 'none'
                CHECK (seasonal_adjustment IN ('sa','nsa','saar','none')),
            country_code TEXT NOT NULL CHECK (length(country_code) = 2),
            topic_code TEXT NOT NULL DEFAULT '',
            category TEXT NOT NULL DEFAULT '',
            is_active INTEGER NOT NULL DEFAULT 1,
            has_vintages INTEGER NOT NULL DEFAULT 0,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (source_id) REFERENCES obs_source(source_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS obs_family_document (
            family_id TEXT NOT NULL,
            release_family_id TEXT NOT NULL,
            relationship TEXT NOT NULL DEFAULT 'produced_by'
                CHECK (relationship IN (
                    'produced_by','derived_from','related_to'
                )),
            created_at TEXT NOT NULL,
            PRIMARY KEY (family_id, release_family_id),
            FOREIGN KEY (family_id) REFERENCES obs_family(family_id),
            FOREIGN KEY (release_family_id) REFERENCES doc_release_family(release_family_id)
        )
        """
    )
    # ALTER TABLE migrations for obs_family_id
    try:
        connection.execute(
            "ALTER TABLE indicators ADD COLUMN obs_family_id TEXT DEFAULT NULL"
        )
    except sqlite3.OperationalError:
        pass  # column already exists
    try:
        connection.execute(
            "ALTER TABLE indicator_vintages ADD COLUMN obs_family_id TEXT DEFAULT NULL"
        )
    except sqlite3.OperationalError:
        pass  # column already exists
    # -- Observation family indexes --------------------------------------
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_family_source "
        "ON obs_family(source_id)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_family_country_topic "
        "ON obs_family(country_code, topic_code)"
    )
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_obs_family_provider_series "
        "ON obs_family(source_id, provider_series_id)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_indicators_family_date "
        "ON indicators(obs_family_id, date)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_vintages_family_date "
        "ON indicator_vintages(obs_family_id, observation_date)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_family_doc_release "
        "ON obs_family_document(release_family_id)"
    )

    # ── Cross-source concept map ───────────────────────────
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS concept_map (
            concept_id         TEXT NOT NULL,
            source_id          TEXT NOT NULL,
            provider_series_id TEXT NOT NULL,
            obs_family_id      TEXT NOT NULL DEFAULT '',
            role               TEXT NOT NULL DEFAULT 'primary'
                CHECK (role IN ('primary','secondary','cross_check')),
            notes              TEXT NOT NULL DEFAULT '',
            created_at         TEXT NOT NULL,
            PRIMARY KEY (concept_id, source_id, provider_series_id)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_concept_map_concept "
        "ON concept_map(concept_id)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_concept_map_series "
        "ON concept_map(source_id, provider_series_id)"
    )
    try:
        connection.execute("ALTER TABLE concept_map ADD COLUMN priority INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # column already exists

    # ── Release schedule ──────────────────────────────────────
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS release_schedule (
            concept_id        TEXT PRIMARY KEY,
            rule_type         TEXT NOT NULL,
            rule_json         TEXT NOT NULL DEFAULT '{}',
            frequency         TEXT NOT NULL DEFAULT 'monthly',
            release_time_utc  TEXT NOT NULL DEFAULT '',
            timezone          TEXT NOT NULL DEFAULT '',
            source_authority  TEXT NOT NULL DEFAULT 'manual',
            confidence        TEXT NOT NULL DEFAULT 'pattern'
                CHECK (confidence IN ('exact','pattern','approximate')),
            next_expected     TEXT NOT NULL DEFAULT '',
            last_released     TEXT NOT NULL DEFAULT '',
            last_checked      TEXT NOT NULL DEFAULT '',
            is_active         INTEGER NOT NULL DEFAULT 1,
            notes             TEXT NOT NULL DEFAULT '',
            created_at        TEXT NOT NULL,
            updated_at        TEXT NOT NULL
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_release_schedule_next "
        "ON release_schedule(next_expected) WHERE is_active = 1"
    )

    # ── Release status (availability tracking) ────────────────
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS release_status (
            concept_id      TEXT NOT NULL,
            release_date    TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'PENDING'
                CHECK (status IN (
                    'PENDING','WAITING','FETCHED','CONFIRMED','STALE','FAILED'
                )),
            attempt_count   INTEGER NOT NULL DEFAULT 0,
            next_retry      TEXT NOT NULL DEFAULT '',
            last_attempt    TEXT NOT NULL DEFAULT '',
            source_used     TEXT NOT NULL DEFAULT '',
            data_date       TEXT NOT NULL DEFAULT '',
            expected_period TEXT NOT NULL DEFAULT '',
            provisional     INTEGER NOT NULL DEFAULT 0,
            error           TEXT NOT NULL DEFAULT '',
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            PRIMARY KEY (concept_id, release_date)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_release_status_retry "
        "ON release_status(next_retry) "
        "WHERE status IN ('PENDING','WAITING')"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_release_status_concept "
        "ON release_status(concept_id, status)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS source_capability (
            source_id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            source_type TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            supports_discovery INTEGER NOT NULL DEFAULT 0,
            supports_structure INTEGER NOT NULL DEFAULT 0,
            supports_latest_sync INTEGER NOT NULL DEFAULT 0,
            supports_backfill INTEGER NOT NULL DEFAULT 0,
            is_default_scheduled INTEGER NOT NULL DEFAULT 0,
            description TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS catalog_entity (
            source_id TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            display_name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            is_active INTEGER NOT NULL DEFAULT 1,
            discovered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (source_id, entity_id)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_catalog_entity_source "
        "ON catalog_entity(source_id, entity_type, display_name)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS catalog_sync_checkpoint (
            source_id TEXT NOT NULL,
            job_type TEXT NOT NULL,
            cursor TEXT NOT NULL DEFAULT '',
            entities_total INTEGER NOT NULL DEFAULT 0,
            entities_synced INTEGER NOT NULL DEFAULT 0,
            observations_synced INTEGER NOT NULL DEFAULT 0,
            last_success_at TEXT NOT NULL DEFAULT '',
            last_error TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL,
            PRIMARY KEY (source_id, job_type)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS catalog_sync_run (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL,
            entities_total INTEGER NOT NULL DEFAULT 0,
            entities_synced INTEGER NOT NULL DEFAULT 0,
            observations_synced INTEGER NOT NULL DEFAULT 0,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL DEFAULT '',
            duration_ms INTEGER NOT NULL DEFAULT 0,
            error TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_catalog_sync_run_source "
        "ON catalog_sync_run(source_id, job_type, started_at DESC)"
    )

    # ── Calendar indicator normalization tables ───────────────
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS calendar_indicator (
            indicator_id   TEXT PRIMARY KEY,
            canonical_name TEXT NOT NULL,
            topic          TEXT NOT NULL DEFAULT '',
            country_code   TEXT NOT NULL,
            frequency      TEXT NOT NULL DEFAULT 'monthly',
            unit           TEXT NOT NULL DEFAULT '',
            obs_family_id  TEXT DEFAULT NULL,
            is_active      INTEGER NOT NULL DEFAULT 1,
            created_at     TEXT NOT NULL,
            updated_at     TEXT NOT NULL
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_indicator_country_topic "
        "ON calendar_indicator(country_code, topic)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS calendar_indicator_alias (
            alias_normalized TEXT NOT NULL,
            indicator_id     TEXT NOT NULL,
            source           TEXT NOT NULL,
            country_code     TEXT NOT NULL,
            alias_original   TEXT NOT NULL DEFAULT '',
            created_at       TEXT NOT NULL,
            PRIMARY KEY (alias_normalized, source, country_code)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_alias_indicator "
        "ON calendar_indicator_alias(indicator_id)"
    )
    _ensure_table_columns(
        connection,
        table_name="calendar_events",
        columns={"indicator_id": "TEXT DEFAULT NULL"},
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_calendar_events_indicator_id "
        "ON calendar_events(indicator_id)"
    )

    # ── Unified calendar (issue #8) ────────────────────────────
    # Two physical lanes sharing a revision pattern:
    #   economic  — macro releases (TE now, BLS/ECB/Fed/NBS later)
    #   corporate — earnings/IPOs/splits/dividends (EODHD now)
    # Downstream reads the v_calendar_item VIEW for a unified shape.
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cal_provider (
            provider_id   TEXT NOT NULL,
            provider_type TEXT NOT NULL
                CHECK (provider_type IN (
                    'data_aggregator','government_agency','central_bank',
                    'exchange','market_data'
                )),
            domain        TEXT NOT NULL
                CHECK (domain IN ('economic','corporate')),
            precedence    INTEGER NOT NULL DEFAULT 10,
            is_active     INTEGER NOT NULL DEFAULT 1,
            created_at    TEXT NOT NULL,
            updated_at    TEXT NOT NULL,
            PRIMARY KEY (provider_id, domain)
        )
        """
    )
    # Per-connector circuit-breaker state for the calendar
    # scheduler (issue #9 P-sched-3). Keyed by scheduler-level
    # connector name (``"bls"`` / ``"bea"`` / ``"ecb"`` /
    # ``"fed-fomc"`` / ``"fed-releases"`` / ``"fed-values"`` /
    # ``"nbs"``) rather than provider-id, because the scheduler
    # distinguishes Fed's three surfaces while ``cal_provider``
    # carries a single ``federal-reserve`` row.
    #
    # ``requests_today`` + ``requests_day_utc`` (added in
    # P-sched-3-budget) persist a per-connector daily request
    # counter across cron-invocation processes so the scheduler
    # can skip a connector once its upstream cap is exhausted.
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS calendar_connector_state (
            connector             TEXT NOT NULL PRIMARY KEY,
            consecutive_failures  INTEGER NOT NULL DEFAULT 0,
            last_error            TEXT,
            last_failure_at_ms    INTEGER,
            cooling_until_ms      INTEGER,
            requests_today        INTEGER NOT NULL DEFAULT 0,
            requests_day_utc      TEXT,
            updated_at            TEXT NOT NULL
        )
        """
    )
    _ensure_table_columns(
        connection,
        table_name="calendar_connector_state",
        columns={
            "requests_today":   "INTEGER NOT NULL DEFAULT 0",
            "requests_day_utc": "TEXT",
        },
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cal_econ_raw (
            provider           TEXT NOT NULL,
            provider_event_id  TEXT NOT NULL,
            snapshot_epoch_ms  INTEGER NOT NULL,
            content_hash       TEXT NOT NULL,
            payload_json       TEXT NOT NULL,
            fetched_at         TEXT NOT NULL,
            PRIMARY KEY (provider, provider_event_id, content_hash)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_econ_raw_latest "
        "ON cal_econ_raw(provider, provider_event_id, snapshot_epoch_ms DESC)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cal_econ_event (
            provider               TEXT NOT NULL,
            provider_event_id      TEXT NOT NULL,
            event_time_utc         TEXT NOT NULL,
            event_time_precision   TEXT NOT NULL DEFAULT 'datetime'
                CHECK (event_time_precision IN ('datetime','date','approximate')),
            reference_date         TEXT,
            reference_label        TEXT NOT NULL DEFAULT '',
            country_code           TEXT NOT NULL,
            indicator_id           TEXT,
            category               TEXT NOT NULL DEFAULT '',
            title                  TEXT NOT NULL,
            importance             TEXT
                CHECK (importance IS NULL OR importance IN ('low','medium','high')),
            currency               TEXT NOT NULL DEFAULT '',
            unit                   TEXT NOT NULL DEFAULT '',
            actual                 TEXT,
            previous               TEXT,
            revised                TEXT,
            forecast               TEXT,
            consensus_forecast     TEXT,
            ticker                 TEXT NOT NULL DEFAULT '',
            source                 TEXT NOT NULL DEFAULT '',
            source_url             TEXT NOT NULL DEFAULT '',
            content_hash           TEXT NOT NULL,
            last_update_epoch_ms   INTEGER,
            observed_at_epoch_ms   INTEGER NOT NULL,
            created_at             TEXT NOT NULL,
            updated_at             TEXT NOT NULL,
            PRIMARY KEY (provider, provider_event_id),
            FOREIGN KEY (indicator_id) REFERENCES calendar_indicator(indicator_id)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_econ_event_country_time "
        "ON cal_econ_event(country_code, event_time_utc)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_econ_event_indicator_time "
        "ON cal_econ_event(indicator_id, event_time_utc)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_econ_event_time "
        "ON cal_econ_event(event_time_utc)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_econ_event_datetime "
        "ON cal_econ_event(datetime(event_time_utc))"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_econ_event_datetime_provider "
        "ON cal_econ_event(datetime(event_time_utc), provider_event_id)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_econ_event_datetime_country "
        "ON cal_econ_event(country_code, datetime(event_time_utc))"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_econ_event_datetime_indicator "
        "ON cal_econ_event(indicator_id, datetime(event_time_utc))"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_econ_event_date "
        "ON cal_econ_event(date(event_time_utc))"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_econ_event_date_provider "
        "ON cal_econ_event(date(event_time_utc), provider_event_id)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_econ_event_date_country "
        "ON cal_econ_event(country_code, date(event_time_utc))"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_econ_event_date_indicator "
        "ON cal_econ_event(indicator_id, date(event_time_utc))"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cal_econ_drops (
            provider           TEXT NOT NULL,
            provider_event_id  TEXT NOT NULL,
            first_dropped_at   TEXT NOT NULL,
            last_seen_at       TEXT NOT NULL DEFAULT '',
            reason             TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (provider, provider_event_id)
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cal_corp_raw (
            provider           TEXT NOT NULL,
            provider_event_id  TEXT NOT NULL,
            snapshot_epoch_ms  INTEGER NOT NULL,
            content_hash       TEXT NOT NULL,
            payload_json       TEXT NOT NULL,
            fetched_at         TEXT NOT NULL,
            PRIMARY KEY (provider, provider_event_id, content_hash)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_corp_raw_latest "
        "ON cal_corp_raw(provider, provider_event_id, snapshot_epoch_ms DESC)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cal_corp_event (
            provider               TEXT NOT NULL,
            provider_event_id      TEXT NOT NULL,
            event_subtype          TEXT NOT NULL
                CHECK (event_subtype IN (
                    'earnings','ipo','split','dividend','earnings_trend'
                )),
            event_time_utc         TEXT NOT NULL,
            event_time_precision   TEXT NOT NULL DEFAULT 'date'
                CHECK (event_time_precision IN ('datetime','date','approximate')),
            ticker                 TEXT NOT NULL,
            exchange               TEXT NOT NULL DEFAULT '',
            currency               TEXT NOT NULL DEFAULT '',
            currency_reporting     TEXT NOT NULL DEFAULT '',
            title                  TEXT NOT NULL DEFAULT '',
            reference_date         TEXT,
            source_url             TEXT NOT NULL DEFAULT '',
            content_hash           TEXT NOT NULL,
            payload_json           TEXT NOT NULL DEFAULT '{}',
            observed_at_epoch_ms   INTEGER NOT NULL,
            created_at             TEXT NOT NULL,
            updated_at             TEXT NOT NULL,
            PRIMARY KEY (provider, provider_event_id)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_corp_event_ticker_time "
        "ON cal_corp_event(ticker, event_time_utc)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_cal_corp_event_subtype_time "
        "ON cal_corp_event(event_subtype, event_time_utc)"
    )

    # Unified read view — UNION ALL over both lanes, projected into
    # the CalendarItem contract shape. Storage stays split; consumers
    # see one target.
    connection.execute("DROP VIEW IF EXISTS v_calendar_item")
    connection.execute(
        """
        CREATE VIEW v_calendar_item AS
        SELECT
            provider || ':' || provider_event_id AS event_id,
            'economic'                           AS domain,
            'release'                            AS subtype,
            provider                             AS provider,
            provider_event_id                    AS provider_event_id,
            event_time_utc                       AS event_time_utc,
            event_time_precision                 AS event_time_precision,
            title                                AS title,
            country_code                         AS country,
            NULL                                 AS ticker,
            NULL                                 AS exchange,
            currency                             AS currency,
            importance                           AS importance,
            indicator_id                         AS indicator_id,
            reference_date                       AS reference_date,
            actual                               AS actual,
            previous                             AS previous,
            forecast                             AS forecast,
            consensus_forecast                   AS consensus_forecast,
            source_url                           AS source_url,
            last_update_epoch_ms                 AS last_update_epoch_ms,
            observed_at_epoch_ms                 AS observed_at_epoch_ms,
            NULL                                 AS payload_json
        FROM cal_econ_event
        UNION ALL
        SELECT
            provider || ':' || provider_event_id AS event_id,
            'corporate'                          AS domain,
            event_subtype                        AS subtype,
            provider                             AS provider,
            provider_event_id                    AS provider_event_id,
            event_time_utc                       AS event_time_utc,
            event_time_precision                 AS event_time_precision,
            title                                AS title,
            NULL                                 AS country,
            ticker                               AS ticker,
            exchange                             AS exchange,
            currency                             AS currency,
            NULL                                 AS importance,
            NULL                                 AS indicator_id,
            reference_date                       AS reference_date,
            NULL                                 AS actual,
            NULL                                 AS previous,
            NULL                                 AS forecast,
            NULL                                 AS consensus_forecast,
            source_url                           AS source_url,
            NULL                                 AS last_update_epoch_ms,
            observed_at_epoch_ms                 AS observed_at_epoch_ms,
            payload_json                         AS payload_json
        FROM cal_corp_event
        """
    )

    # Backfill cursor — per (provider, phase) resumability for the
    # economic-lane API fetcher. `phase` lets us drive the recent /
    # mid / early sweeps independently so a mid-phase budget breach
    # doesn't reset the others.
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cal_backfill_cursor (
            provider         TEXT NOT NULL,
            phase            TEXT NOT NULL,
            cursor_date      TEXT NOT NULL,
            window_end_date  TEXT NOT NULL,
            rows_ingested    INTEGER NOT NULL DEFAULT 0,
            requests_spent   INTEGER NOT NULL DEFAULT 0,
            last_run_at      TEXT NOT NULL DEFAULT '',
            is_complete      INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (provider, phase)
        )
        """
    )

    # Corporate-lane equivalent of cal_backfill_cursor. The corp lane has
    # five subtypes whose density and budget cost differ wildly (per-symbol
    # dividend detail vs. one-shot global splits), so the cursor PK adds
    # `subtype` — a budget breach on `dividend` mustn't reset progress on
    # `split`. Phase split mirrors the econ lane.
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cal_corp_backfill_cursor (
            provider         TEXT NOT NULL,
            subtype          TEXT NOT NULL,
            phase            TEXT NOT NULL,
            cursor_date      TEXT NOT NULL,
            window_end_date  TEXT NOT NULL,
            rows_ingested    INTEGER NOT NULL DEFAULT 0,
            requests_spent   INTEGER NOT NULL DEFAULT 0,
            last_run_at      TEXT NOT NULL DEFAULT '',
            is_complete      INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (provider, subtype, phase)
        )
        """
    )

    # Seed provider dim. INSERT OR IGNORE = idempotent on repeated
    # init_schema calls. Official-tier providers (precedence=100)
    # rank above the TE aggregator (precedence=10); the current
    # v_calendar_item VIEW is a plain UNION ALL and does not yet
    # apply precedence — the parity harness (issue #9 P6) is the
    # first caller that resolves conflicts on this column.
    _now_iso = datetime.now(timezone.utc).isoformat()
    connection.executemany(
        """
        INSERT OR IGNORE INTO cal_provider (
            provider_id, provider_type, domain, precedence,
            is_active, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 1, ?, ?)
        """,
        [
            ("tradingeconomics", "data_aggregator",   "economic",  10,  _now_iso, _now_iso),
            ("eodhd",            "data_aggregator",   "corporate", 10,  _now_iso, _now_iso),
            ("bls",              "government_agency", "economic",  100, _now_iso, _now_iso),
            ("bea",              "government_agency", "economic",  100, _now_iso, _now_iso),
            ("census",           "government_agency", "economic",  100, _now_iso, _now_iso),
            ("ism",              "market_data",       "economic",  100, _now_iso, _now_iso),
            ("umich",            "market_data",       "economic",  100, _now_iso, _now_iso),
            ("conference-board",  "market_data",       "economic",  100, _now_iso, _now_iso),
            ("nar",              "market_data",       "economic",  100, _now_iso, _now_iso),
            ("federal-reserve",  "central_bank",      "economic",  100, _now_iso, _now_iso),
            ("ecb",              "central_bank",      "economic",  100, _now_iso, _now_iso),
            ("eurostat",         "government_agency", "economic",  100, _now_iso, _now_iso),
            ("destatis",         "government_agency", "economic",  100, _now_iso, _now_iso),
            ("zew",              "market_data",       "economic",  100, _now_iso, _now_iso),
            ("ifo",              "market_data",       "economic",  100, _now_iso, _now_iso),
            ("gfk",              "market_data",       "economic",  100, _now_iso, _now_iso),
            ("hcob",             "market_data",       "economic",  100, _now_iso, _now_iso),
            ("ec-bcs",           "government_agency", "economic",  100, _now_iso, _now_iso),
            ("insee",            "government_agency", "economic",  100, _now_iso, _now_iso),
            ("ine",              "government_agency", "economic",  100, _now_iso, _now_iso),
            ("istat",            "government_agency", "economic",  100, _now_iso, _now_iso),
            ("nbs",              "government_agency", "economic",  100, _now_iso, _now_iso),
            ("boj",              "central_bank",      "economic",  100, _now_iso, _now_iso),
            ("mof-jp",           "government_agency", "economic",  100, _now_iso, _now_iso),
            ("cao",              "government_agency", "economic",  100, _now_iso, _now_iso),
            ("meti",             "government_agency", "economic",  100, _now_iso, _now_iso),
            ("stat-bureau-jp",   "government_agency", "economic",  100, _now_iso, _now_iso),
            # Issue #50 — DOL UI Weekly Claims + EIA weekly stocks.
            ("dol",              "government_agency", "economic",  100, _now_iso, _now_iso),
            ("eia",              "government_agency", "economic",  100, _now_iso, _now_iso),
            # Issue #51 — UK coverage: ONS releases + Bank of England.
            ("ons",              "government_agency", "economic",  100, _now_iso, _now_iso),
            ("boe",              "central_bank",      "economic",  100, _now_iso, _now_iso),
            # Issue #52 — Canada coverage: Statistics Canada + Bank of Canada.
            ("statcan",          "government_agency", "economic",  100, _now_iso, _now_iso),
            ("boc",              "central_bank",      "economic",  100, _now_iso, _now_iso),
            # Issue #53 — Australia coverage: ABS + Reserve Bank of Australia.
            ("abs",              "government_agency", "economic",  100, _now_iso, _now_iso),
            ("rba",              "central_bank",      "economic",  100, _now_iso, _now_iso),
            # Issue #54 — India coverage: MoSPI + Reserve Bank of India.
            ("mospi",            "government_agency", "economic",  100, _now_iso, _now_iso),
            ("rbi",              "central_bank",      "economic",  100, _now_iso, _now_iso),
            # Issue #55 — Korea coverage: Statistics Korea + Bank of Korea.
            ("kostat",           "government_agency", "economic",  100, _now_iso, _now_iso),
            ("bok",              "central_bank",      "economic",  100, _now_iso, _now_iso),
            # Issue #84 — Brazil coverage: IBGE + Banco Central do Brasil.
            ("ibge",             "government_agency", "economic",  100, _now_iso, _now_iso),
            ("bcb",              "central_bank",      "economic",  100, _now_iso, _now_iso),
            # Issue #86 — Turkey coverage: TÜİK + Türkiye Cumhuriyet Merkez Bankası.
            ("tuik",             "government_agency", "economic",  100, _now_iso, _now_iso),
            ("tcmb",             "central_bank",      "economic",  100, _now_iso, _now_iso),
            # Issue #88 — Mexico coverage: INEGI + Banco de México.
            ("inegi",            "government_agency", "economic",  100, _now_iso, _now_iso),
            ("banxico",          "central_bank",      "economic",  100, _now_iso, _now_iso),
            # Issue #90 — South Africa coverage: Stats SA + SARB.
            ("statssa",          "government_agency", "economic",  100, _now_iso, _now_iso),
            ("sarb",             "central_bank",      "economic",  100, _now_iso, _now_iso),
            # Issue #92 — Indonesia coverage: Bank Indonesia (BPS deferred to P2).
            ("bank-indonesia",   "central_bank",      "economic",  100, _now_iso, _now_iso),
            # Issue #56 — central-bank speeches calendar (Fed / ECB / BoE / BoJ).
            # Each speech connector writes its own provider id; the parity
            # harness and provider-metadata lookups need a row per id even
            # though the events are schedule-only (no parity whitelist).
            ("fed-speeches",     "central_bank",      "economic",  100, _now_iso, _now_iso),
            ("ecb-speeches",     "central_bank",      "economic",  100, _now_iso, _now_iso),
            ("boe-speeches",     "central_bank",      "economic",  100, _now_iso, _now_iso),
            ("boj-speeches",     "central_bank",      "economic",  100, _now_iso, _now_iso),
        ],
    )

    # ── Observation enrichment sidecar ─────────────────────────
    # Stores derived labels / computed tags alongside an observation
    # family + date without polluting the indicators schema. Used
    # today for VIX regime classification (key='regime'); future
    # enrichments (drawdown buckets, surprise z-scores, etc.) land
    # under their own `key` values with the same (family, date) PK.
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS obs_enrichment (
            obs_family_id TEXT NOT NULL,
            date          TEXT NOT NULL,
            key           TEXT NOT NULL,
            value         TEXT NOT NULL,
            created_at    TEXT NOT NULL,
            PRIMARY KEY (obs_family_id, date, key)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_enrichment_family_key "
        "ON obs_enrichment(obs_family_id, key)"
    )
    # ── Unified subject vocabulary (issue #2) ──────────────────
    # subject_id is the canonical cross-source identifier (e.g.
    # 'econ.cpi', 'rate.us.sofr'). Aliases map source-native keys
    # (FRED series, calendar indicator strings, title regex, ...)
    # back to a subject. item_subjects tags documents at ingest;
    # calendar_events and observations are resolved at query time
    # via subject_alias lookups.
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS subjects (
            subject_id   TEXT PRIMARY KEY,
            display_name TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS subject_aliases (
            subject_id  TEXT NOT NULL,
            alias_type  TEXT NOT NULL,
            alias_value TEXT NOT NULL,
            PRIMARY KEY (subject_id, alias_type, alias_value)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_subject_aliases_lookup "
        "ON subject_aliases(alias_type, alias_value)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS item_subjects (
            item_sha   TEXT NOT NULL,
            subject_id TEXT NOT NULL,
            confidence REAL NOT NULL,
            PRIMARY KEY (item_sha, subject_id)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_item_subjects_subject "
        "ON item_subjects(subject_id)"
    )
