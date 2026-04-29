"""Schema tests for the X (Twitter) sentiment lane (issue #76 P0).

Covers the five new tables (x_tracked_accounts, x_keyword_pool, x_posts,
x_post_keywords, x_post_event_links), the cal_econ_event.event_type
extension, the keyword-pool seed, and idempotency of repeated
``init_schema`` calls. The tracked-account seed is a Python constant
(resolved by the P1 client into user_ids) so it's exercised at the
constants level, not via DB assertions.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from storage.schema import X_TRACKED_ACCOUNT_SEEDS
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _tables(store: SQLiteEngineStore) -> set[str]:
    with store._connection(commit=False) as c:
        return {
            r[0]
            for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }


def _columns(store: SQLiteEngineStore, table: str) -> dict[str, str]:
    with store._connection(commit=False) as c:
        return {
            row["name"]: row["type"]
            for row in c.execute(f"PRAGMA table_info({table})").fetchall()
        }


def _indexes(store: SQLiteEngineStore, table: str) -> set[str]:
    with store._connection(commit=False) as c:
        return {
            r[1]
            for r in c.execute(f"PRAGMA index_list({table})").fetchall()
        }


def test_creates_five_x_tables(store: SQLiteEngineStore) -> None:
    expected = {
        "x_tracked_accounts",
        "x_keyword_pool",
        "x_posts",
        "x_post_keywords",
        "x_post_event_links",
    }
    assert expected <= _tables(store)


def test_x_tracked_accounts_columns(store: SQLiteEngineStore) -> None:
    cols = _columns(store, "x_tracked_accounts")
    assert {
        "user_id",
        "handle",
        "category",
        "priority",
        "since_id",
        "last_fetched_at",
        "is_active",
        "created_at",
        "updated_at",
    } <= set(cols)


def test_x_tracked_accounts_pk_is_handle(store: SQLiteEngineStore) -> None:
    """Handle is the seed-time identifier; user_id is filled by P1.
    PK on handle keeps init_schema idempotent before any API call."""
    with store._connection(commit=False) as c:
        pk_cols = [
            row["name"]
            for row in c.execute("PRAGMA table_info(x_tracked_accounts)").fetchall()
            if row["pk"] > 0
        ]
    assert pk_cols == ["handle"]


def test_tracked_accounts_seeded(store: SQLiteEngineStore) -> None:
    """P0 deliverable per issue body: seed Fed/ECB/BoE/BoJ + ~20 economists."""
    with store._connection(commit=False) as c:
        rows = c.execute(
            "SELECT handle, category, priority, user_id "
            "FROM x_tracked_accounts"
        ).fetchall()
    assert len(rows) >= 20
    handles = {r["handle"] for r in rows}
    assert {"federalreserve", "ecb", "bankofengland", "bankofjapan"} <= handles
    by_cat: dict[str, int] = {}
    for r in rows:
        by_cat[r["category"]] = by_cat.get(r["category"], 0) + 1
    assert by_cat.get("central_bank", 0) >= 4
    assert by_cat.get("economist", 0) >= 8
    # user_id starts empty — P1 client resolves via X API on bootstrap.
    assert all(r["user_id"] == "" for r in rows)


def test_tracked_accounts_seed_idempotent(tmp_path: Path) -> None:
    """Re-running init_schema must not duplicate rows or wipe operator
    edits to user_id / priority."""
    db = tmp_path / "engine.db"
    s1 = SQLiteEngineStore(db_path=db)
    with s1._connection(commit=True) as c:
        c.execute(
            "UPDATE x_tracked_accounts SET user_id = '23381256', priority = 999 "
            "WHERE handle = 'federalreserve'"
        )
        first_count = c.execute(
            "SELECT COUNT(*) FROM x_tracked_accounts"
        ).fetchone()[0]

    s2 = SQLiteEngineStore(db_path=db)
    with s2._connection(commit=False) as c:
        second_count = c.execute(
            "SELECT COUNT(*) FROM x_tracked_accounts"
        ).fetchone()[0]
        row = c.execute(
            "SELECT user_id, priority FROM x_tracked_accounts "
            "WHERE handle = 'federalreserve'"
        ).fetchone()

    assert first_count == second_count
    assert row["user_id"] == "23381256"
    assert row["priority"] == 999


def test_x_posts_columns(store: SQLiteEngineStore) -> None:
    cols = _columns(store, "x_posts")
    assert {
        "post_id",
        "author_id",
        "author_handle",
        "text",
        "created_at",
        "lang",
        "retweet_count",
        "like_count",
        "reply_count",
        "quote_count",
        "query_context",
        "fetched_at",
        "is_available",
        "availability_checked_at",
    } <= set(cols)


def test_x_post_event_links_composite_pk(store: SQLiteEngineStore) -> None:
    """Composite (post_id, cal_provider, cal_provider_event_id) PK matches
    the cal_econ_event PK shape — the issue body's `event_id` shorthand
    expanded to fit the real calendar table."""
    cols = _columns(store, "x_post_event_links")
    assert "cal_provider" in cols
    assert "cal_provider_event_id" in cols
    assert "link_type" in cols


def test_cal_econ_event_event_type_added(store: SQLiteEngineStore) -> None:
    cols = _columns(store, "cal_econ_event")
    assert "event_type" in cols, (
        "issue #76 P0 — event_type column required for source='x_derived' "
        "social_breakout rows"
    )


def test_keyword_pool_seeded(store: SQLiteEngineStore) -> None:
    with store._connection(commit=False) as c:
        rows = c.execute(
            "SELECT keyword, category, priority FROM x_keyword_pool"
        ).fetchall()
    assert len(rows) >= 40, "expect ~50 seed keywords across 4 categories"
    by_category: dict[str, int] = {}
    for r in rows:
        by_category[r["category"]] = by_category.get(r["category"], 0) + 1
    assert {"macro", "ticker", "geopolitical", "tech"} <= set(by_category)
    assert by_category["macro"] >= 15


def test_keyword_pool_seed_idempotent(tmp_path: Path) -> None:
    """Re-running init_schema must not duplicate seed rows or wipe
    operator-tuned priority / since_id values."""
    db = tmp_path / "engine.db"
    s1 = SQLiteEngineStore(db_path=db)
    with s1._connection(commit=True) as c:
        c.execute(
            "UPDATE x_keyword_pool SET priority = 999, since_id = 'abc' "
            "WHERE keyword = 'fed'"
        )
        first_count = c.execute(
            "SELECT COUNT(*) FROM x_keyword_pool"
        ).fetchone()[0]

    s2 = SQLiteEngineStore(db_path=db)
    with s2._connection(commit=False) as c:
        second_count = c.execute(
            "SELECT COUNT(*) FROM x_keyword_pool"
        ).fetchone()[0]
        row = c.execute(
            "SELECT priority, since_id FROM x_keyword_pool WHERE keyword='fed'"
        ).fetchone()

    assert first_count == second_count
    assert row["priority"] == 999
    assert row["since_id"] == "abc"


def test_tracked_account_seed_constant_shape() -> None:
    """The P1 client reads X_TRACKED_ACCOUNT_SEEDS at bootstrap and
    resolves handles to user_ids. Validates shape + minimum coverage
    so the constant doesn't silently drift below the issue's target
    (Fed + ECB + BoE + BoJ + ~20 economists/researchers)."""
    handles = {h for (h, _, _) in X_TRACKED_ACCOUNT_SEEDS}
    assert {"federalreserve", "ecb", "bankofengland", "bankofjapan"} <= handles
    by_category: dict[str, int] = {}
    for _, cat, _ in X_TRACKED_ACCOUNT_SEEDS:
        by_category[cat] = by_category.get(cat, 0) + 1
    assert by_category.get("central_bank", 0) >= 4
    assert by_category.get("economist", 0) >= 8
    for handle, category, priority in X_TRACKED_ACCOUNT_SEEDS:
        assert category in {"central_bank", "economist", "buyside", "sellside"}
        assert 0 <= priority <= 100
        assert handle == handle.lower(), f"{handle!r} should be lower-case"


def test_x_keyword_pool_check_rejects_unknown_category(
    store: SQLiteEngineStore,
) -> None:
    import sqlite3

    with store._connection(commit=True) as c:
        with pytest.raises(sqlite3.IntegrityError):
            c.execute(
                "INSERT INTO x_keyword_pool ("
                "  keyword, category, priority, created_at, updated_at"
                ") VALUES ('bogus', 'invalid', 50, '2026-01-01', '2026-01-01')"
            )


def test_x_post_event_links_check_rejects_unknown_link_type(
    store: SQLiteEngineStore,
) -> None:
    import sqlite3

    with store._connection(commit=True) as c:
        c.execute(
            "INSERT INTO x_posts (post_id, author_id, created_at, fetched_at) "
            "VALUES ('p1', 'u1', '2026-01-01', '2026-01-01')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            c.execute(
                "INSERT INTO x_post_event_links ("
                "  post_id, cal_provider, cal_provider_event_id, link_type, created_at"
                ") VALUES ('p1', 'bls', 'CPIAUCSL', 'totally_made_up', '2026-01-01')"
            )


def test_v_calendar_item_surfaces_event_type(store: SQLiteEngineStore) -> None:
    """A row with event_type='social_breakout' (the P3 injector's
    target) must surface as ``subtype='social_breakout'`` through
    v_calendar_item; existing rows with event_type='' fall back to
    ``subtype='release'`` so the view stays compatible with current
    callers (list_calendar_items / GET /v1/calendar)."""
    with store._connection(commit=True) as c:
        c.execute(
            """
            INSERT INTO cal_econ_event (
                provider, provider_event_id, event_time_utc,
                country_code, title, source, content_hash,
                observed_at_epoch_ms, created_at, updated_at,
                event_type
            ) VALUES (
                'x_derived', 'breakout-2026-01-01',
                '2026-01-01T12:00:00Z', '', 'social breakout',
                'x_derived', 'h1', 1000, '2026-01-01', '2026-01-01',
                'social_breakout'
            )
            """
        )
        c.execute(
            """
            INSERT INTO cal_econ_event (
                provider, provider_event_id, event_time_utc,
                country_code, title, source, content_hash,
                observed_at_epoch_ms, created_at, updated_at
            ) VALUES (
                'bls', 'CPI-2026-01', '2026-01-15T13:30:00Z',
                'US', 'CPI release', 'bls', 'h2',
                1100, '2026-01-01', '2026-01-01'
            )
            """
        )
        rows = c.execute(
            "SELECT provider, subtype FROM v_calendar_item "
            "WHERE provider IN ('x_derived', 'bls') ORDER BY provider"
        ).fetchall()
    by_provider = {r["provider"]: r["subtype"] for r in rows}
    assert by_provider["x_derived"] == "social_breakout"
    assert by_provider["bls"] == "release"
