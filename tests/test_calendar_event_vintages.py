"""Unit tests for the calendar_event_vintages PIT layer (issue #21 P3).

Covers the core invariants:

- no-future-leak: ``calendar_actual_as_of(event_id, provider, t)`` never
  returns a vintage whose ``observed_at`` exceeds ``t``;
- predecessor compare on append: out-of-order writes between existing
  rows record genuine intermediate states rather than collapsing into
  the latest;
- tie-breaking: when two vintages share an ``observed_at``, ``as_of``
  returns the more recently appended row deterministically;
- revision chains: a multi-step revision history reads correctly at
  every cutoff.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from storage import CalendarEventVintageRecord, SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    s = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    s.init_schema()
    return s


def _v(t: str, a: str, *, f: str = "F", p: str = "P", event_id: str = "e1",
       provider: str = "tradingeconomics") -> CalendarEventVintageRecord:
    return CalendarEventVintageRecord(
        event_id=event_id, provider=provider,
        vintage_date=t, observed_at=t,
        actual=a, forecast=f, previous=p,
    )


class TestAppendOnChange:

    def test_first_write_appends(self, store: SQLiteEngineStore) -> None:
        assert store.append_calendar_event_vintage_if_changed(
            _v("2024-01-01T00:00:00Z", "100"),
        ) is True

    def test_unchanged_triple_no_ops(self, store: SQLiteEngineStore) -> None:
        store.append_calendar_event_vintage_if_changed(_v("2024-01-01T00:00:00Z", "100"))
        # Same triple at a later observed_at — should not append.
        assert store.append_calendar_event_vintage_if_changed(
            _v("2024-01-02T00:00:00Z", "100"),
        ) is False

    def test_changed_actual_appends(self, store: SQLiteEngineStore) -> None:
        store.append_calendar_event_vintage_if_changed(_v("2024-01-01T00:00:00Z", "100"))
        assert store.append_calendar_event_vintage_if_changed(
            _v("2024-01-02T00:00:00Z", "110"),
        ) is True

    def test_out_of_order_intermediate_recorded(self, store: SQLiteEngineStore) -> None:
        """Writing t1 after t3 (where t1 differs from t3's predecessor) must
        record t1, not collapse into t3 because they share an actual.
        """
        store.append_calendar_event_vintage_if_changed(_v("2024-01-01T00:00:00Z", "A"))
        store.append_calendar_event_vintage_if_changed(_v("2024-01-03T00:00:00Z", "B"))
        # Now a late-arriving t2=B between t1=A and t3=B. Predecessor (t1=A)
        # differs from candidate (B), so it must append.
        assert store.append_calendar_event_vintage_if_changed(
            _v("2024-01-02T00:00:00Z", "B"),
        ) is True

    def test_idempotent_same_vintage_date(self, store: SQLiteEngineStore) -> None:
        """Re-running the seed with the same vintage_date is a no-op even if
        the candidate's triple differs (UNIQUE backstop).
        """
        store.append_calendar_event_vintage_if_changed(_v("2024-01-01T00:00:00Z", "A"))
        # Different actual at same vintage_date — UNIQUE collision, IGNORE.
        assert store.append_calendar_event_vintage_if_changed(
            _v("2024-01-01T00:00:00Z", "ZZZ"),
        ) is False


class TestActualAsOf:

    def test_no_vintage_returns_none(self, store: SQLiteEngineStore) -> None:
        assert store.calendar_actual_as_of("missing", "te", "2024-01-01T00:00:00Z") is None

    def test_no_future_leak(self, store: SQLiteEngineStore) -> None:
        """Querying before any vintage exists must return None, not the
        first future vintage.
        """
        store.append_calendar_event_vintage_if_changed(_v("2024-06-01T00:00:00Z", "200"))
        assert store.calendar_actual_as_of(
            "e1", "tradingeconomics", "2024-05-31T23:59:59Z",
        ) is None

    def test_returns_predecessor(self, store: SQLiteEngineStore) -> None:
        store.append_calendar_event_vintage_if_changed(_v("2024-01-01T00:00:00Z", "100"))
        store.append_calendar_event_vintage_if_changed(_v("2024-02-01T00:00:00Z", "120"))
        # Cutoff between the two vintages — should return the earlier one.
        result = store.calendar_actual_as_of(
            "e1", "tradingeconomics", "2024-01-15T00:00:00Z",
        )
        assert result is not None and result.actual == "100"

    def test_returns_exact_match(self, store: SQLiteEngineStore) -> None:
        store.append_calendar_event_vintage_if_changed(_v("2024-01-01T00:00:00Z", "100"))
        result = store.calendar_actual_as_of(
            "e1", "tradingeconomics", "2024-01-01T00:00:00Z",
        )
        assert result is not None and result.actual == "100"

    def test_tie_breaks_to_latest_id(self, store: SQLiteEngineStore) -> None:
        """When two vintages share observed_at, return the one with the
        higher id (most recently inserted at that moment).
        """
        # Use distinct vintage_date to bypass UNIQUE; same observed_at.
        early = CalendarEventVintageRecord(
            event_id="e1", provider="tradingeconomics",
            vintage_date="2024-01-01T00:00:00Z",
            observed_at="2024-01-01T00:00:00Z",
            actual="100", forecast="F", previous="P",
        )
        same_obs_later_insert = CalendarEventVintageRecord(
            event_id="e1", provider="tradingeconomics",
            vintage_date="2024-01-01T00:00:00.001Z",
            observed_at="2024-01-01T00:00:00Z",
            actual="105", forecast="F", previous="P",
        )
        store.append_calendar_event_vintage_if_changed(early)
        store.append_calendar_event_vintage_if_changed(same_obs_later_insert)
        result = store.calendar_actual_as_of(
            "e1", "tradingeconomics", "2024-01-01T00:00:00Z",
        )
        assert result is not None and result.actual == "105"

    def test_revision_chain(self, store: SQLiteEngineStore) -> None:
        """Multi-step revision: read at each cutoff returns the right value."""
        store.append_calendar_event_vintage_if_changed(_v("2024-01-01T00:00:00Z", "first"))
        store.append_calendar_event_vintage_if_changed(_v("2024-01-15T00:00:00Z", "second"))
        store.append_calendar_event_vintage_if_changed(_v("2024-02-01T00:00:00Z", "third"))
        store.append_calendar_event_vintage_if_changed(_v("2024-03-01T00:00:00Z", "fourth"))

        cases = [
            ("2024-01-01T00:00:00Z", "first"),
            ("2024-01-10T00:00:00Z", "first"),
            ("2024-01-15T12:00:00Z", "second"),
            ("2024-02-15T00:00:00Z", "third"),
            ("2030-01-01T00:00:00Z", "fourth"),
        ]
        for as_of, expected in cases:
            r = store.calendar_actual_as_of("e1", "tradingeconomics", as_of)
            assert r is not None and r.actual == expected, f"as_of={as_of}"

    def test_fractional_seconds_no_future_leak(self, store: SQLiteEngineStore) -> None:
        """Vintage at 2024-01-01T00:00:00.500Z must NOT be returned for a
        cutoff at 2024-01-01T00:00:00Z. Lexicographic compare on TEXT
        observed_at would falsely include it because '.' < 'Z'; SQL must
        use julianday() to normalize.
        """
        future = CalendarEventVintageRecord(
            event_id="e1", provider="tradingeconomics",
            vintage_date="2024-01-01T00:00:00.500Z",
            observed_at="2024-01-01T00:00:00.500Z",
            actual="future-leak", forecast="F", previous="P",
        )
        store.append_calendar_event_vintage_if_changed(future)
        # Cutoff is whole-second, before the .500Z vintage.
        result = store.calendar_actual_as_of(
            "e1", "tradingeconomics", "2024-01-01T00:00:00Z",
        )
        assert result is None, f"future leak: returned {result!r}"

    def test_provider_isolation(self, store: SQLiteEngineStore) -> None:
        """Same event_id, two providers — queries scope to one provider."""
        store.append_calendar_event_vintage_if_changed(
            _v("2024-01-01T00:00:00Z", "te-val", provider="tradingeconomics"),
        )
        store.append_calendar_event_vintage_if_changed(
            _v("2024-01-01T00:00:00Z", "ec-val", provider="ec-bcs"),
        )
        te = store.calendar_actual_as_of("e1", "tradingeconomics", "2024-06-01T00:00:00Z")
        ec = store.calendar_actual_as_of("e1", "ec-bcs", "2024-06-01T00:00:00Z")
        assert te is not None and te.actual == "te-val"
        assert ec is not None and ec.actual == "ec-val"


class TestVintageHistory:

    def test_empty_history(self, store: SQLiteEngineStore) -> None:
        assert store.calendar_vintage_history("missing", "te") == []

    def test_history_ordered_ascending(self, store: SQLiteEngineStore) -> None:
        # Insert out of chronological order to exercise ordering.
        store.append_calendar_event_vintage_if_changed(_v("2024-03-01T00:00:00Z", "C"))
        store.append_calendar_event_vintage_if_changed(_v("2024-01-01T00:00:00Z", "A"))
        store.append_calendar_event_vintage_if_changed(_v("2024-02-01T00:00:00Z", "B"))
        history = store.calendar_vintage_history("e1", "tradingeconomics")
        assert [v.actual for v in history] == ["A", "B", "C"]
