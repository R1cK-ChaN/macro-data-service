"""Tests for the Wayback evidence-archive submitter (issue #36).

Covers the contract laid out in the issue body:

- Every PIT vintage row is a candidate evidence anchor; the retry-tail
  scan picks up rows with NULL ``evidence_archive_url`` and non-empty
  ``source_url`` and submits each one to Wayback's Save Page Now API.
- Two vintage rows that carry the *same* ``source_url`` produce two
  independent SPN submissions — one PIT observation, one snapshot.
- A submitter failure leaves the row's ``evidence_archive_url`` NULL
  so the next sweep retries it.
- Rows with empty ``source_url`` are skipped (derived rows have no
  per-release URL to archive).
- Setting ``MACRO_DATA_WAYBACK_DISABLED`` makes the tail a no-op.

Tests inject a stub submitter; the real network path
(:func:`submit_save_request`) is exercised only at the unit level.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.evidence_archive import (
    DISABLED_ENV_VAR,
    archive_pending,
    submit_save_request,
)
from storage import CalendarEventVintageRecord, SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    s = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    s.init_schema()
    return s


def _seed(
    store: SQLiteEngineStore,
    *,
    event_id: str,
    observed_at: str,
    actual: str,
    source_url: str,
    provider: str = "bls",
) -> None:
    store.append_calendar_event_vintage_if_changed(
        CalendarEventVintageRecord(
            event_id=event_id, provider=provider,
            vintage_date=observed_at, observed_at=observed_at,
            actual=actual, forecast=None, previous=None,
            source_url=source_url,
        ),
    )


class TestArchivePending:

    def test_submits_pending_row_and_updates_snapshot(
        self, store: SQLiteEngineStore,
    ) -> None:
        _seed(
            store, event_id="cpi-2024-03",
            observed_at="2024-04-10T12:30:00Z", actual="3.5",
            source_url="https://www.bls.gov/news.release/cpi.htm",
        )
        calls: list[str] = []

        def stub(url: str) -> str | None:
            calls.append(url)
            return f"https://web.archive.org/web/20240410123000/{url}"

        with store._connection(commit=True) as conn:  # type: ignore[attr-defined]
            counters = archive_pending(conn, submitter=stub)

        assert calls == ["https://www.bls.gov/news.release/cpi.htm"]
        assert counters == {"scanned": 1, "archived": 1, "failed": 0}
        history = store.calendar_vintage_history("cpi-2024-03", "bls")
        assert history[0].evidence_archive_url == (
            "https://web.archive.org/web/20240410123000/"
            "https://www.bls.gov/news.release/cpi.htm"
        )

    def test_skips_rows_with_empty_source_url(
        self, store: SQLiteEngineStore,
    ) -> None:
        _seed(
            store, event_id="derived-1",
            observed_at="2024-04-10T12:30:00Z", actual="100",
            source_url="",
        )
        calls: list[str] = []

        with store._connection(commit=True) as conn:  # type: ignore[attr-defined]
            counters = archive_pending(conn, submitter=lambda u: (calls.append(u), "x")[1])

        assert calls == []
        assert counters == {"scanned": 0, "archived": 0, "failed": 0}

    def test_failure_leaves_archive_url_null(
        self, store: SQLiteEngineStore,
    ) -> None:
        _seed(
            store, event_id="cpi-2024-03",
            observed_at="2024-04-10T12:30:00Z", actual="3.5",
            source_url="https://www.bls.gov/news.release/cpi.htm",
        )

        with store._connection(commit=True) as conn:  # type: ignore[attr-defined]
            counters = archive_pending(conn, submitter=lambda _u: None)

        assert counters == {"scanned": 1, "archived": 0, "failed": 1}
        history = store.calendar_vintage_history("cpi-2024-03", "bls")
        assert history[0].evidence_archive_url is None

    def test_identical_source_url_submits_twice(
        self, store: SQLiteEngineStore,
    ) -> None:
        # Two vintages on the same provider_event_id sharing one URL.
        # Each PIT observation must produce its own SPN submission so
        # the snapshots carry distinct ``web.archive.org/web/<ts>/``
        # timestamps.
        _seed(
            store, event_id="cpi-2024-03",
            observed_at="2024-04-10T12:30:00Z", actual="3.5",
            source_url="https://www.bls.gov/news.release/cpi.htm",
        )
        # Append a second vintage with a different actual so the
        # if-changed guard appends rather than no-ops; same URL.
        _seed(
            store, event_id="cpi-2024-03",
            observed_at="2024-05-10T12:30:00Z", actual="3.7",
            source_url="https://www.bls.gov/news.release/cpi.htm",
        )

        calls: list[str] = []

        def stub(url: str) -> str | None:
            calls.append(url)
            return f"https://web.archive.org/web/{len(calls):020d}/{url}"

        with store._connection(commit=True) as conn:  # type: ignore[attr-defined]
            counters = archive_pending(conn, submitter=stub)

        assert calls == [
            "https://www.bls.gov/news.release/cpi.htm",
            "https://www.bls.gov/news.release/cpi.htm",
        ]
        assert counters == {"scanned": 2, "archived": 2, "failed": 0}
        history = store.calendar_vintage_history("cpi-2024-03", "bls")
        # Distinct snapshots — same URL, different timestamps.
        assert history[0].evidence_archive_url != history[1].evidence_archive_url
        assert history[0].evidence_archive_url is not None
        assert history[1].evidence_archive_url is not None

    def test_respects_limit(self, store: SQLiteEngineStore) -> None:
        for idx in range(5):
            _seed(
                store, event_id=f"e-{idx}",
                observed_at=f"2024-04-{10 + idx:02d}T00:00:00Z",
                actual=str(idx),
                source_url=f"https://example.test/{idx}",
            )

        with store._connection(commit=True) as conn:  # type: ignore[attr-defined]
            counters = archive_pending(
                conn, limit=2, submitter=lambda u: f"https://web.archive.org/web/x/{u}",
            )

        assert counters == {"scanned": 2, "archived": 2, "failed": 0}

    def test_disabled_env_var_short_circuits(
        self, store: SQLiteEngineStore, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _seed(
            store, event_id="e1",
            observed_at="2024-04-10T00:00:00Z", actual="1",
            source_url="https://example.test/1",
        )
        monkeypatch.setenv(DISABLED_ENV_VAR, "1")
        calls: list[str] = []

        with store._connection(commit=True) as conn:  # type: ignore[attr-defined]
            counters = archive_pending(conn, submitter=lambda u: (calls.append(u), "x")[1])

        assert calls == []
        assert counters == {"scanned": 0, "archived": 0, "failed": 0}

    @pytest.mark.parametrize("flag", ["0", "false", "no", ""])
    def test_disabled_flag_only_matches_literal_one(
        self, store: SQLiteEngineStore, monkeypatch: pytest.MonkeyPatch, flag: str,
    ) -> None:
        """Codex review P3: ``MACRO_DATA_WAYBACK_DISABLED=0`` must NOT
        suppress the tail. Truthy-string semantics of ``os.environ.get``
        would silently disable archival on any non-empty value."""
        _seed(
            store, event_id="e1",
            observed_at="2024-04-10T00:00:00Z", actual="1",
            source_url="https://example.test/1",
        )
        monkeypatch.setenv(DISABLED_ENV_VAR, flag)

        with store._connection(commit=True) as conn:  # type: ignore[attr-defined]
            counters = archive_pending(
                conn, submitter=lambda u: f"https://web.archive.org/web/x/{u}",
            )
        assert counters["archived"] == 1

    def test_failures_rotate_to_back_of_queue(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Codex review P2: a block of unarchivable URLs at the head of
        the queue must not stall the tail forever. Failed rows stamp
        ``evidence_last_attempt_at`` and sort behind never-tried rows
        on the next sweep."""
        # Two rows that always fail upstream + two that succeed. The
        # failures have older observed_at so the naive scan would always
        # surface them first.
        for idx, url in enumerate(["bad-1", "bad-2"], start=1):
            _seed(
                store, event_id=f"bad-{idx}",
                observed_at=f"2024-01-{idx:02d}T00:00:00Z",
                actual=str(idx),
                source_url=f"https://example.test/{url}",
            )
        for idx, url in enumerate(["ok-1", "ok-2"], start=1):
            _seed(
                store, event_id=f"ok-{idx}",
                observed_at=f"2024-04-{idx:02d}T00:00:00Z",
                actual=str(idx),
                source_url=f"https://example.test/{url}",
            )

        def stub(url: str) -> str | None:
            if "bad" in url:
                return None
            return f"https://web.archive.org/web/x/{url}"

        # First pass with a tight limit picks up the two failing rows.
        with store._connection(commit=True) as conn:  # type: ignore[attr-defined]
            first = archive_pending(conn, limit=2, submitter=stub)
        assert first == {"scanned": 2, "archived": 0, "failed": 2}

        # Second pass — the failing rows are now stamped, so the
        # never-tried "ok" rows surface first and get archived.
        with store._connection(commit=True) as conn:  # type: ignore[attr-defined]
            second = archive_pending(conn, limit=2, submitter=stub)
        assert second == {"scanned": 2, "archived": 2, "failed": 0}

        ok_history = store.calendar_vintage_history("ok-1", "bls")
        assert ok_history[0].evidence_archive_url is not None

    def test_writer_lock_released_during_network(
        self, store: SQLiteEngineStore, tmp_path,
    ) -> None:
        """Codex review P2: a parallel writer must not see SQLITE_BUSY
        while the archive tail is mid-network. The fix collects all
        submissions before any UPDATE runs, so the writer lock is held
        only during the (fast) batch-write phase."""
        import sqlite3 as _sqlite3
        # Three rows so the submitter is invoked multiple times.
        for idx in range(3):
            _seed(
                store, event_id=f"e-{idx}",
                observed_at=f"2024-04-{10 + idx:02d}T00:00:00Z",
                actual=str(idx),
                source_url=f"https://example.test/{idx}",
            )

        # Track whether a SECOND connection successfully wrote during
        # the submitter calls — proving the tail isn't holding the
        # writer lock across network.
        side_writes: list[bool] = []

        def stub(url: str) -> str | None:
            other = _sqlite3.connect(str(store.db_path), timeout=0.5)
            try:
                other.execute(
                    "CREATE TABLE IF NOT EXISTS lock_probe (n INTEGER)"
                )
                other.execute("INSERT INTO lock_probe (n) VALUES (1)")
                other.commit()
                side_writes.append(True)
            except _sqlite3.OperationalError:
                side_writes.append(False)
            finally:
                other.close()
            return f"https://web.archive.org/web/x/{url}"

        with store._connection(commit=True) as conn:  # type: ignore[attr-defined]
            counters = archive_pending(conn, submitter=stub)

        assert counters == {"scanned": 3, "archived": 3, "failed": 0}
        assert side_writes == [True, True, True], (
            f"writer lock leaked during network calls: {side_writes}"
        )

    def test_already_archived_rows_are_not_resubmitted(
        self, store: SQLiteEngineStore,
    ) -> None:
        _seed(
            store, event_id="e1",
            observed_at="2024-04-10T00:00:00Z", actual="1",
            source_url="https://example.test/1",
        )

        # First sweep archives.
        with store._connection(commit=True) as conn:  # type: ignore[attr-defined]
            archive_pending(conn, submitter=lambda u: f"https://web.archive.org/web/x/{u}")

        # Second sweep should find nothing pending.
        calls: list[str] = []
        with store._connection(commit=True) as conn:  # type: ignore[attr-defined]
            counters = archive_pending(conn, submitter=lambda u: (calls.append(u), "x")[1])

        assert calls == []
        assert counters == {"scanned": 0, "archived": 0, "failed": 0}


class TestSweepTailIntegration:

    def test_dry_run_skips_archive_pending(
        self, store: SQLiteEngineStore, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Dry-run wrote nothing, so the tail must short-circuit."""
        from macro_data.service import LocalMacroDataService
        from ingestion.calendar import evidence_archive

        calls: list[int] = []

        def spy(*_args, **_kwargs) -> dict[str, int]:
            calls.append(1)
            return {"scanned": 0, "archived": 0, "failed": 0}

        monkeypatch.setattr(evidence_archive, "archive_pending", spy)
        svc = LocalMacroDataService(store=store)
        result = svc.invoke(
            "calendar_econ_sweep_values",
            {"dry_run": True, "connectors": ["fed-values"]},
        )
        assert calls == []
        assert result["evidence_archive"] == {
            "scanned": 0, "archived": 0, "failed": 0,
        }

    def test_execute_mode_runs_archive_pending(
        self, store: SQLiteEngineStore, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Execute-mode sweep runs the tail and surfaces its counters."""
        from macro_data.service import LocalMacroDataService
        from ingestion.calendar import evidence_archive

        captured: list[dict[str, int]] = []

        def spy(connection, *, limit=64, submitter=None) -> dict[str, int]:
            counters = {"scanned": 3, "archived": 2, "failed": 1}
            captured.append(counters)
            return counters

        monkeypatch.setattr(evidence_archive, "archive_pending", spy)
        svc = LocalMacroDataService(store=store)
        result = svc.invoke(
            "calendar_econ_sweep_values",
            {"dry_run": False, "connectors": []},
        )
        assert captured == [{"scanned": 3, "archived": 2, "failed": 1}]
        assert result["evidence_archive"] == {
            "scanned": 3, "archived": 2, "failed": 1,
        }


class TestSubmitSaveRequest:

    def test_empty_url_returns_none_without_network(self) -> None:
        assert submit_save_request("") is None

    def test_http_error_returns_none(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import urllib.error
        import urllib.request

        def boom(*_args, **_kwargs):
            raise urllib.error.URLError("network down")

        monkeypatch.setattr(urllib.request, "urlopen", boom)
        assert submit_save_request("https://example.test/x") is None

    def test_resolves_content_location_header(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import urllib.request

        class _FakeResp:
            headers = {"Content-Location": "/web/20240410123000/https://example.test/x"}

            def geturl(self) -> str:
                return "https://web.archive.org/web/20240410123000/https://example.test/x"

            def __enter__(self) -> "_FakeResp":
                return self

            def __exit__(self, *_exc) -> None:  # pragma: no cover — context plumbing
                return None

        monkeypatch.setattr(urllib.request, "urlopen", lambda *_a, **_k: _FakeResp())
        archived = submit_save_request("https://example.test/x")
        assert archived == (
            "https://web.archive.org/web/20240410123000/https://example.test/x"
        )
