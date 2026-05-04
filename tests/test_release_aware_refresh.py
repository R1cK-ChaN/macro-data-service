from __future__ import annotations

import datetime as dt
import importlib.util
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "release_aware_refresh.py"
sys.path.insert(0, str(REPO_ROOT / "src"))

from storage import SQLiteEngineStore
from storage.sqlite import ReleaseScheduleRecord


def _load_script():
    spec = importlib.util.spec_from_file_location("release_aware_refresh", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["release_aware_refresh"] = module
    spec.loader.exec_module(module)
    return module


class FakeStore:
    def __init__(self, schedules: list[ReleaseScheduleRecord]) -> None:
        self.schedules = schedules
        self.updates: list[tuple[str, dict[str, str]]] = []

    def list_release_schedules(self, *, is_active: bool | None = None):
        if is_active is None:
            return list(self.schedules)
        return [s for s in self.schedules if s.is_active is is_active]

    def update_release_timestamps(self, concept_id: str, **kwargs: str) -> None:
        self.updates.append((concept_id, kwargs))


class FakeService:
    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.result = result or {"ok": True}

    def invoke(self, operation: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((operation, arguments))
        return self.result


def _schedule(
    concept_id: str,
    *,
    next_expected: str = "2026-05-12T00:00:00+00:00",
    last_released: str = "",
) -> ReleaseScheduleRecord:
    return ReleaseScheduleRecord(
        concept_id=concept_id,
        rule_type="day_of_month",
        rule_json={"day": 12, "tolerance_days": 3},
        frequency="monthly",
        release_time_utc="12:30",
        timezone="America/New_York",
        source_authority="manual",
        confidence="pattern",
        next_expected=next_expected,
        last_released=last_released,
        last_checked="",
        is_active=True,
        notes="test",
        created_at="2026-05-01T00:00:00+00:00",
        updated_at="2026-05-01T00:00:00+00:00",
    )


def test_due_release_triggers_incremental_fetch_and_advances_schedule() -> None:
    script = _load_script()
    store = FakeStore([_schedule("CPI_US")])
    service = FakeService()
    now = dt.datetime(2026, 5, 12, 12, 31, tzinfo=dt.timezone.utc)

    result = script.run_release_aware_refresh(
        store=store,
        service=service,
        now=now,
        dry_run=False,
        lag_seconds=30,
        window_seconds=90,
    )

    assert result["triggered_count"] == 1
    assert service.calls == [
        (
            "calendar_econ_fetch_bls",
            {
                "series_ids": ["CUUR0000SA0", "CUUR0000SA0L1E"],
                "dry_run": False,
            },
        ),
    ]
    concept_id, update = store.updates[0]
    assert concept_id == "CPI_US"
    assert update["last_released"] == "2026-05-12T12:30:00+00:00"
    assert update["next_expected"] == "2026-06-12T00:00:00+00:00"


def test_same_release_group_dedupes_to_one_fetch() -> None:
    script = _load_script()
    store = FakeStore([_schedule("CPI_US"), _schedule("CORE_CPI_US")])
    service = FakeService()
    now = dt.datetime(2026, 5, 12, 12, 31, tzinfo=dt.timezone.utc)

    result = script.run_release_aware_refresh(
        store=store,
        service=service,
        now=now,
        dry_run=False,
        lag_seconds=30,
        window_seconds=90,
    )

    assert result["due_count"] == 2
    assert result["triggered_count"] == 1
    assert len(service.calls) == 1
    assert sorted(concept_id for concept_id, _ in store.updates) == [
        "CORE_CPI_US",
        "CPI_US",
    ]


def test_last_released_blocks_repeat_trigger() -> None:
    script = _load_script()
    store = FakeStore([
        _schedule(
            "CPI_US",
            last_released="2026-05-12T12:30:00+00:00",
        ),
    ])
    service = FakeService()
    now = dt.datetime(2026, 5, 12, 12, 31, tzinfo=dt.timezone.utc)

    result = script.run_release_aware_refresh(
        store=store,
        service=service,
        now=now,
        dry_run=False,
        lag_seconds=30,
        window_seconds=90,
    )

    assert result["triggered_count"] == 0
    assert service.calls == []
    assert store.updates == []


def test_outside_release_window_waits_for_fixed_cron() -> None:
    script = _load_script()
    store = FakeStore([_schedule("CPI_US")])
    service = FakeService()
    now = dt.datetime(2026, 5, 12, 12, 29, tzinfo=dt.timezone.utc)

    result = script.run_release_aware_refresh(
        store=store,
        service=service,
        now=now,
        dry_run=False,
        lag_seconds=30,
        window_seconds=90,
    )

    assert result["triggered_count"] == 0
    assert service.calls == []
    assert store.updates == []


def test_fetch_failure_keeps_release_open_for_retry() -> None:
    script = _load_script()
    store = FakeStore([_schedule("CPI_US")])
    service = FakeService({"fetch_failures": [{"series_id": "CUUR0000SA0"}]})
    now = dt.datetime(2026, 5, 12, 12, 31, tzinfo=dt.timezone.utc)

    result = script.run_release_aware_refresh(
        store=store,
        service=service,
        now=now,
        dry_run=False,
        lag_seconds=30,
        window_seconds=90,
    )

    assert result["triggered_count"] == 1
    assert result["failed_count"] == 1
    assert store.updates == [
        ("CPI_US", {"last_checked": "2026-05-12T12:31:00+00:00"}),
    ]


def test_execute_seeds_empty_release_schedule_table(tmp_path: Path) -> None:
    script = _load_script()
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    service = FakeService()

    result = script.run_release_aware_refresh(
        store=store,
        service=service,
        now=dt.datetime(2026, 5, 1, 0, 0, tzinfo=dt.timezone.utc),
        dry_run=False,
        lag_seconds=30,
        window_seconds=90,
    )

    cpi_schedule = store.get_release_schedule("CPI_US")
    assert cpi_schedule is not None
    assert cpi_schedule.next_expected
    assert result["schedules_seen"] > 0
    assert result["initialized_count"] > 0
    assert service.calls == []


def test_clean_stale_response_keeps_release_open_for_retry(tmp_path: Path) -> None:
    script = _load_script()
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    store.upsert_release_schedule(_schedule("CPI_US"))
    service = FakeService({
        "series_planned": ["CUUR0000SA0", "CUUR0000SA0L1E"],
        "series_ok": ["CUUR0000SA0", "CUUR0000SA0L1E"],
        "observations_seen": 24,
        "events_upserted": 0,
    })
    now = dt.datetime(2026, 5, 12, 12, 31, tzinfo=dt.timezone.utc)

    result = script.run_release_aware_refresh(
        store=store,
        service=service,
        now=now,
        dry_run=False,
        lag_seconds=30,
        window_seconds=90,
    )

    cpi_schedule = store.get_release_schedule("CPI_US")
    assert cpi_schedule is not None
    assert result["triggered_count"] == 1
    assert result["failed_count"] == 1
    assert cpi_schedule.last_checked == "2026-05-12T12:31:00+00:00"
    assert cpi_schedule.last_released == ""
    assert cpi_schedule.next_expected == "2026-05-12T00:00:00+00:00"


def test_seeded_daily_policy_rate_us_waits_for_fixed_cron() -> None:
    script = _load_script()
    store = FakeStore([
        _schedule(
            "POLICY_RATE_US",
            next_expected="2026-05-12T00:00:00+00:00",
        ),
    ])
    service = FakeService()
    now = dt.datetime(2026, 5, 12, 0, 1, tzinfo=dt.timezone.utc)

    result = script.run_release_aware_refresh(
        store=store,
        service=service,
        now=now,
        dry_run=False,
        lag_seconds=30,
        window_seconds=90,
    )

    assert script.GROUP_BY_CONCEPT.get("POLICY_RATE_US") is None
    assert result["triggered_count"] == 0
    assert service.calls == []
    assert store.updates == []


def test_calendar_event_rules_cover_central_bank_meetings() -> None:
    script = _load_script()

    rules = {
        (rule.group_key, rule.provider, rule.title_prefix)
        for rule in script.CALENDAR_EVENT_RULES
    }

    assert rules == {
        ("fed-fomc", "federal-reserve", "FOMC Rate Decision"),
        ("ecb-policy", "ecb", "ECB Monetary Policy Decision"),
        ("boe-policy", "boe", "BoE Interest Rate Decision"),
        ("boj-policy", "boj", "BoJ Interest Rate Decision"),
    }


def test_fomc_calendar_event_triggers_statement_fetch(tmp_path: Path) -> None:
    script = _load_script()
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    now_text = "2026-05-06T18:00:00+00:00"
    with store.get_connection() as conn:
        conn.execute(
            """
            INSERT INTO cal_econ_event (
                provider, provider_event_id, event_time_utc,
                event_time_precision, reference_date, reference_label,
                country_code, indicator_id, category, title, importance,
                currency, unit, actual, previous, revised, forecast,
                consensus_forecast, ticker, source, source_url,
                content_hash, last_update_epoch_ms, observed_at_epoch_ms,
                created_at, updated_at
            ) VALUES (
                ?, ?, ?, 'datetime', ?, ?, 'US', NULL,
                'Monetary Policy', 'FOMC Rate Decision', 'high',
                'USD', 'percent', NULL, NULL, NULL, NULL, NULL, '',
                'Federal Reserve', '', 'hash-fed-fomc-2026-05-06',
                NULL, ?, ?, ?
            )
            """,
            (
                "federal-reserve",
                "fed-fomc-2026-05-06",
                now_text,
                "2026-05-06",
                "2026-05-06",
                1_778_088_000_000,
                now_text,
                now_text,
            ),
        )
    service = FakeService()

    result = script.run_release_aware_refresh(
        store=store,
        service=service,
        now=dt.datetime(2026, 5, 6, 18, 1, tzinfo=dt.timezone.utc),
        dry_run=False,
        lag_seconds=30,
        window_seconds=90,
    )

    assert result["triggered_count"] == 1
    assert service.calls == [
        (
            "calendar_econ_fetch_fed_values",
            {"closing_dates": ["2026-05-06"], "dry_run": False},
        ),
    ]
    assert result["results"][0]["events"] == ["fed-fomc-2026-05-06"]
