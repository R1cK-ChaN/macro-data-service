#!/usr/bin/env python3
"""Release-calendar-aware value refresh entry-point — issue #130.

Scans ``release_schedule`` once, finds watched concepts whose
``next_expected`` release has crossed the publication buffer, and invokes
the matching incremental calendar value fetch. The watcher is designed for
a one-minute systemd timer; each successful hit advances the schedule row
so repeated timer firings stay idempotent.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from ingestion.release_schedule import (  # noqa: E402
    expected_reference_period,
    next_expected_release,
)
from macro_data.service import LocalMacroDataService  # noqa: E402
from storage import SQLiteEngineStore, default_engine_db_path  # noqa: E402

LOG_FILENAME = "release_aware_refresh.log"
OPERATION = "release_aware_refresh"

logger = logging.getLogger("release_aware_refresh")


@dataclass(frozen=True)
class ReleaseAwareGroup:
    key: str
    source: str
    concepts: tuple[str, ...]
    operation: str
    arguments: Mapping[str, Any] = field(default_factory=dict)
    date_argument: str = ""
    fresh_titles: tuple[str, ...] = ()


@dataclass(frozen=True)
class CalendarEventRule:
    group_key: str
    provider: str
    title_prefix: str


# Explicit release-aware decision table. Concepts absent from this table
# continue on the fixed cron/baseline sweep path.
RELEASE_AWARE_GROUPS: tuple[ReleaseAwareGroup, ...] = (
    ReleaseAwareGroup(
        key="bls-cpi",
        source="bls",
        concepts=("CPI_US", "CORE_CPI_US"),
        operation="calendar_econ_fetch_bls",
        arguments={"series_ids": ("CUUR0000SA0", "CUUR0000SA0L1E")},
        fresh_titles=(
            "Consumer Price Index",
            "Consumer Price Index \u2014 All Items Less Food and Energy",
        ),
    ),
    ReleaseAwareGroup(
        key="bls-nfp",
        source="bls",
        concepts=(
            "NFP_US",
            "UNEMP_US",
            "AVG_HOURLY_EARN_US",
            "AVG_WEEKLY_HOURS_US",
        ),
        operation="calendar_econ_fetch_bls",
        arguments={
            "series_ids": (
                "CES0000000001",
                "LNS14000000",
                "CES0500000003",
                "CES0500000002",
            ),
        },
        fresh_titles=(
            "Nonfarm Payrolls",
            "Unemployment Rate",
            "Average Hourly Earnings \u2014 Total Private",
            "Average Weekly Hours \u2014 Total Private",
        ),
    ),
    ReleaseAwareGroup(
        key="bea-pce",
        source="bea",
        concepts=("CORE_PCE_US",),
        operation="calendar_econ_fetch_bea",
        arguments={"series_ids": ("BEA_NIPA_T20804_1",)},
        fresh_titles=("PCE Price Index",),
    ),
    ReleaseAwareGroup(
        key="ism-pmi",
        source="ism",
        concepts=("ISM_MFG_PMI_US", "ISM_MFG_PMI_MOM_US"),
        operation="calendar_econ_fetch_ism",
        arguments={"series_ids": ("ISM_MANUFACTURING_PMI",)},
        fresh_titles=("ISM Manufacturing PMI",),
    ),
    ReleaseAwareGroup(
        key="fed-fomc",
        source="federal-reserve",
        concepts=("FOMC_RATE_US",),
        operation="calendar_econ_fetch_fed_values",
        date_argument="closing_dates",
    ),
    ReleaseAwareGroup(
        key="ecb-policy",
        source="ecb",
        concepts=("ECB_POLICY_RATE_EU",),
        operation="calendar_econ_fetch_ecb",
        arguments={
            "limit": 10,
            "series_ids": (
                "FM.B.U2.EUR.4F.KR.MRR_FR.LEV",
                "FM.B.U2.EUR.4F.KR.DFR.LEV",
                "FM.B.U2.EUR.4F.KR.MLFR.LEV",
            ),
        },
    ),
    ReleaseAwareGroup(
        key="boe-policy",
        source="boe",
        concepts=("BOE_POLICY_RATE_GB",),
        operation="calendar_econ_sweep_values",
        arguments={"connectors": ("boe",)},
    ),
    ReleaseAwareGroup(
        key="boj-policy",
        source="boj",
        concepts=("BOJ_POLICY_RATE_JP",),
        operation="calendar_econ_fetch_boj_values",
        date_argument="closing_dates",
    ),
)

GROUP_BY_KEY: dict[str, ReleaseAwareGroup] = {
    group.key: group for group in RELEASE_AWARE_GROUPS
}

GROUP_BY_CONCEPT: dict[str, ReleaseAwareGroup] = {
    concept: group
    for group in RELEASE_AWARE_GROUPS
    for concept in group.concepts
}

CALENDAR_EVENT_RULES: tuple[CalendarEventRule, ...] = (
    CalendarEventRule(
        group_key="fed-fomc",
        provider="federal-reserve",
        title_prefix="FOMC Rate Decision",
    ),
    CalendarEventRule(
        group_key="ecb-policy",
        provider="ecb",
        title_prefix="ECB Monetary Policy Decision",
    ),
    CalendarEventRule(
        group_key="boe-policy",
        provider="boe",
        title_prefix="BoE Interest Rate Decision",
    ),
    CalendarEventRule(
        group_key="boj-policy",
        provider="boj",
        title_prefix="BoJ Interest Rate Decision",
    ),
)


@dataclass(frozen=True)
class CalendarEventHit:
    provider: str
    provider_event_id: str
    event_time_utc: str
    reference_date: str
    title: str


@dataclass(frozen=True)
class DueRelease:
    schedule: Any | None
    group: ReleaseAwareGroup
    expected_at: dt.datetime
    trigger_at: dt.datetime
    window_end: dt.datetime
    event: CalendarEventHit | None = None
    argument_date: dt.date | None = None


def _append_log(log_path: Path, payload: dict[str, Any]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


def _build_service(db_path: Path) -> LocalMacroDataService:
    return LocalMacroDataService(store=SQLiteEngineStore(db_path=db_path))


def _parse_iso_utc(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _parse_clock(value: Any) -> dt.time | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.removesuffix("Z").strip()
    for fmt in ("%H:%M:%S", "%H:%M", "%I:%M %p", "%I %p"):
        try:
            return dt.datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def _zoneinfo(name: str | None) -> dt.tzinfo:
    if not name:
        return dt.timezone.utc
    try:
        return ZoneInfo(str(name))
    except ZoneInfoNotFoundError:
        return dt.timezone.utc


def expected_release_at(schedule: Any) -> dt.datetime | None:
    base = _parse_iso_utc(getattr(schedule, "next_expected", ""))
    if base is None:
        return None

    rule_json = getattr(schedule, "rule_json", {}) or {}
    if isinstance(rule_json, str):
        try:
            rule_json = json.loads(rule_json)
        except json.JSONDecodeError:
            rule_json = {}

    rule_clock = _parse_clock(rule_json.get("time") or rule_json.get("release_time"))
    if rule_clock is not None:
        tz_name = rule_json.get("timezone") or getattr(schedule, "timezone", "")
        local = dt.datetime.combine(base.date(), rule_clock, tzinfo=_zoneinfo(tz_name))
        return local.astimezone(dt.timezone.utc)

    if base.timetz().replace(tzinfo=None) != dt.time(0, 0):
        return base

    release_clock = _parse_clock(getattr(schedule, "release_time_utc", ""))
    if release_clock is not None:
        return dt.datetime.combine(
            base.date(), release_clock, tzinfo=dt.timezone.utc,
        )

    return base


def _already_released(schedule: Any, expected_at: dt.datetime) -> bool:
    last_released = _parse_iso_utc(getattr(schedule, "last_released", ""))
    return last_released is not None and last_released >= expected_at


def _next_expected_iso(schedule: Any, *, reference: dt.datetime) -> str:
    rule_json = getattr(schedule, "rule_json", {}) or {}
    if isinstance(rule_json, str):
        try:
            rule_json = json.loads(rule_json)
        except json.JSONDecodeError:
            rule_json = {}
    nxt = next_expected_release(
        str(getattr(schedule, "rule_type", "")),
        rule_json,
        reference=reference,
    )
    return nxt.isoformat() if nxt is not None else ""


def find_due_releases(
    schedules: Iterable[Any],
    *,
    now: dt.datetime,
    lag_seconds: int,
    window_seconds: int,
) -> tuple[list[DueRelease], list[Any], list[Any]]:
    due: list[DueRelease] = []
    stale: list[Any] = []
    needs_next: list[Any] = []
    for schedule in schedules:
        group = GROUP_BY_CONCEPT.get(str(getattr(schedule, "concept_id", "")))
        if group is None:
            continue
        expected_at = expected_release_at(schedule)
        if expected_at is None:
            needs_next.append(schedule)
            continue
        trigger_at = expected_at + dt.timedelta(seconds=lag_seconds)
        window_end = trigger_at + dt.timedelta(seconds=window_seconds)
        if _already_released(schedule, expected_at):
            if now >= window_end:
                stale.append(schedule)
            continue
        if trigger_at <= now < window_end:
            due.append(DueRelease(
                schedule=schedule,
                group=group,
                expected_at=expected_at,
                trigger_at=trigger_at,
                window_end=window_end,
                argument_date=expected_at.date(),
            ))
        elif now >= window_end:
            stale.append(schedule)
    due.sort(key=lambda d: (d.trigger_at, d.group.key, d.schedule.concept_id))
    return due, stale, needs_next


def find_due_calendar_events(
    store: Any,
    *,
    now: dt.datetime,
    lag_seconds: int,
    window_seconds: int,
) -> list[DueRelease]:
    get_conn = getattr(store, "get_connection", None)
    if not callable(get_conn):
        return []

    providers = tuple({rule.provider for rule in CALENDAR_EVENT_RULES})
    if not providers:
        return []

    window_start = now - dt.timedelta(seconds=lag_seconds + window_seconds)
    window_end = now - dt.timedelta(seconds=lag_seconds)
    placeholders = ",".join("?" for _ in providers)
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT provider, provider_event_id, event_time_utc,
                   COALESCE(reference_date, '') AS reference_date,
                   title
            FROM cal_econ_event
            WHERE actual IS NULL
              AND event_time_utc != ''
              AND event_time_utc >= ?
              AND event_time_utc <= ?
              AND provider IN ({placeholders})
            ORDER BY event_time_utc, provider, provider_event_id
            """,
            (window_start.isoformat(), window_end.isoformat(), *providers),
        ).fetchall()
    finally:
        conn.close()

    due: list[DueRelease] = []
    for row in rows:
        title = str(row["title"] or "")
        provider = str(row["provider"] or "")
        rule = next(
            (
                r for r in CALENDAR_EVENT_RULES
                if r.provider == provider and title.startswith(r.title_prefix)
            ),
            None,
        )
        if rule is None:
            continue
        group = GROUP_BY_KEY[rule.group_key]
        expected_at = _parse_iso_utc(str(row["event_time_utc"] or ""))
        if expected_at is None:
            continue
        trigger_at = expected_at + dt.timedelta(seconds=lag_seconds)
        hit_window_end = trigger_at + dt.timedelta(seconds=window_seconds)
        if not (trigger_at <= now < hit_window_end):
            continue
        reference_date = str(row["reference_date"] or "")
        try:
            arg_date = dt.date.fromisoformat(reference_date) if reference_date else expected_at.date()
        except ValueError:
            arg_date = expected_at.date()
        event = CalendarEventHit(
            provider=provider,
            provider_event_id=str(row["provider_event_id"] or ""),
            event_time_utc=str(row["event_time_utc"] or ""),
            reference_date=reference_date,
            title=title,
        )
        due.append(DueRelease(
            schedule=None,
            group=group,
            expected_at=expected_at,
            trigger_at=trigger_at,
            window_end=hit_window_end,
            event=event,
            argument_date=arg_date,
        ))
    return due


def _jsonable_arguments(arguments: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in arguments.items():
        if isinstance(value, tuple):
            out[key] = list(value)
        else:
            out[key] = value
    return out


def _build_arguments(
    group: ReleaseAwareGroup,
    *,
    argument_date: dt.date,
    dry_run: bool,
) -> dict[str, Any]:
    args = _jsonable_arguments(group.arguments)
    if group.date_argument:
        args[group.date_argument] = [argument_date.isoformat()]
    args["dry_run"] = dry_run
    return args


def _service_result_ok(result: Mapping[str, Any]) -> bool:
    if result.get("error"):
        return False
    if result.get("fetch_error"):
        return False
    for key in (
        "fetch_failures",
        "parse_failures",
        "fetch_errors",
        "series_failed",
        "series_empty",
        "series_unknown",
    ):
        if result.get(key):
            return False
    try:
        if int(result.get("failed_count") or 0) > 0:
            return False
    except (TypeError, ValueError):
        return False
    return True


def _seed_and_initialize_schedules(
    store: Any,
    *,
    now: dt.datetime,
    dry_run: bool,
) -> int:
    if dry_run:
        return 0

    seed = getattr(store, "seed_release_schedules", None)
    if callable(seed):
        seed()

    list_schedules = getattr(store, "list_release_schedules", None)
    update = getattr(store, "update_release_timestamps", None)
    if not callable(list_schedules) or not callable(update):
        return 0

    initialized = 0
    for schedule in list_schedules(is_active=True):
        if str(getattr(schedule, "next_expected", "") or ""):
            continue
        next_expected = _next_expected_iso(schedule, reference=now)
        if not next_expected:
            continue
        update(schedule.concept_id, next_expected=next_expected)
        initialized += 1
    return initialized


def _fresh_actuals_available(
    store: Any,
    group: ReleaseAwareGroup,
    schedules: Iterable[Any],
    *,
    expected_at: dt.datetime,
) -> bool:
    schedule_list = [s for s in schedules if s is not None]
    if not schedule_list or not group.fresh_titles:
        return True

    get_conn = getattr(store, "get_connection", None)
    if not callable(get_conn):
        return True

    thresholds = [
        expected_reference_period(
            str(getattr(schedule, "frequency", "")),
            reference=expected_at,
        )
        for schedule in schedule_list
    ]
    threshold = min(thresholds)
    placeholders = ",".join("?" for _ in group.fresh_titles)
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT title
            FROM cal_econ_event
            WHERE provider = ?
              AND actual IS NOT NULL
              AND COALESCE(reference_date, '') >= ?
              AND title IN ({placeholders})
            """,
            (group.source, threshold, *group.fresh_titles),
        ).fetchall()
    finally:
        conn.close()

    found = {str(row["title"] or "") for row in rows}
    return set(group.fresh_titles).issubset(found)


def _release_sort_id(release: DueRelease) -> str:
    if release.event is not None:
        return release.event.provider_event_id
    if release.schedule is not None:
        return str(getattr(release.schedule, "concept_id", ""))
    return ""


def _mark_schedules(
    store: Any,
    schedules: Iterable[Any],
    *,
    now: dt.datetime,
    last_released: str = "",
    reference_for_next: dt.datetime,
) -> int:
    updated = 0
    for schedule in schedules:
        next_expected = _next_expected_iso(schedule, reference=reference_for_next)
        kwargs: dict[str, str] = {"last_checked": now.isoformat()}
        if last_released:
            kwargs["last_released"] = last_released
        if next_expected:
            kwargs["next_expected"] = next_expected
        store.update_release_timestamps(schedule.concept_id, **kwargs)
        updated += 1
    return updated


def run_release_aware_refresh(
    *,
    store: Any,
    service: Any,
    now: dt.datetime,
    dry_run: bool,
    lag_seconds: int = 30,
    window_seconds: int = 90,
) -> dict[str, Any]:
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    now = now.astimezone(dt.timezone.utc)

    preinitialized = _seed_and_initialize_schedules(
        store,
        now=now,
        dry_run=dry_run,
    )
    schedules = list(store.list_release_schedules(is_active=True))
    due, stale, needs_next = find_due_releases(
        schedules,
        now=now,
        lag_seconds=lag_seconds,
        window_seconds=window_seconds,
    )
    due.extend(find_due_calendar_events(
        store,
        now=now,
        lag_seconds=lag_seconds,
        window_seconds=window_seconds,
    ))
    due.sort(key=lambda d: (d.trigger_at, d.group.key, _release_sort_id(d)))

    results: list[dict[str, Any]] = []
    triggered_keys: set[str] = set()
    updated_rows = preinitialized

    for schedule in needs_next:
        if dry_run:
            continue
        next_expected = _next_expected_iso(schedule, reference=now)
        if next_expected:
            store.update_release_timestamps(
                schedule.concept_id,
                next_expected=next_expected,
                last_checked=now.isoformat(),
            )
            updated_rows += 1

    for schedule in stale:
        if dry_run:
            continue
        updated_rows += _mark_schedules(
            store,
            [schedule],
            now=now,
            reference_for_next=now,
        )

    for release in due:
        if release.group.key in triggered_keys:
            continue
        same_group = [r for r in due if r.group.key == release.group.key]
        triggered_keys.add(release.group.key)
        args = _build_arguments(
            release.group,
            argument_date=release.argument_date or release.expected_at.date(),
            dry_run=dry_run,
        )
        result = service.invoke(release.group.operation, args)
        ok = _service_result_ok(result)
        schedule_group = [r.schedule for r in same_group if r.schedule is not None]
        if ok:
            ok = _fresh_actuals_available(
                store,
                release.group,
                schedule_group,
                expected_at=release.expected_at,
            )
        if ok and not dry_run:
            updated_rows += _mark_schedules(
                store,
                schedule_group,
                now=now,
                last_released=release.expected_at.isoformat(),
                reference_for_next=release.expected_at + dt.timedelta(seconds=1),
            )
        elif not dry_run:
            for r in same_group:
                if r.schedule is None:
                    continue
                store.update_release_timestamps(
                    r.schedule.concept_id,
                    last_checked=now.isoformat(),
                )
                updated_rows += 1
        results.append({
            "group": release.group.key,
            "source": release.group.source,
            "operation": release.group.operation,
            "arguments": args,
            "concepts": [
                r.schedule.concept_id for r in same_group
                if r.schedule is not None
            ],
            "events": [
                r.event.provider_event_id for r in same_group
                if r.event is not None
            ],
            "expected_at": release.expected_at.isoformat(),
            "trigger_at": release.trigger_at.isoformat(),
            "window_end": release.window_end.isoformat(),
            "ok": ok,
            "result": result,
        })

    return {
        "operation": OPERATION,
        "checked_at": now.isoformat(),
        "dry_run": dry_run,
        "watched_concepts": len(GROUP_BY_CONCEPT),
        "schedules_seen": len(schedules),
        "due_count": len(due),
        "stale_count": len(stale),
        "initialized_count": preinitialized + len(needs_next),
        "triggered_count": len(results),
        "failed_count": sum(1 for r in results if not r["ok"]),
        "rows_updated": updated_rows,
        "results": results,
    }


def _parse_now(value: str | None) -> dt.datetime:
    if not value:
        return dt.datetime.now(dt.timezone.utc)
    parsed = _parse_iso_utc(value)
    if parsed is None:
        raise ValueError(f"invalid --now value: {value!r}")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path", type=Path, default=None,
        help="engine.db override (default: .macro-data/engine.db).",
    )
    parser.add_argument(
        "--log-path", type=Path, default=None,
        help="Log file path override.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Plan matching releases and call fetchers in dry-run mode.",
    )
    parser.add_argument(
        "--lag-seconds", type=int, default=30,
        help="Seconds after expected publish time before firing.",
    )
    parser.add_argument(
        "--window-seconds", type=int, default=90,
        help="Seconds after the lag point to accept a release hit.",
    )
    parser.add_argument(
        "--now", default=None,
        help="UTC ISO timestamp override for tests and manual replays.",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Log to stderr at DEBUG.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    engine_db = args.db_path or default_engine_db_path()
    log_path = args.log_path or (engine_db.parent / "logs" / LOG_FILENAME)
    summary: dict[str, Any] = {
        "operation": OPERATION,
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "dry_run": args.dry_run,
    }

    try:
        now = _parse_now(args.now)
        store = SQLiteEngineStore(db_path=engine_db)
        service = _build_service(engine_db)
        result = run_release_aware_refresh(
            store=store,
            service=service,
            now=now,
            dry_run=args.dry_run,
            lag_seconds=max(0, args.lag_seconds),
            window_seconds=max(1, args.window_seconds),
        )
        summary.update(result)
        summary["status"] = "ok" if result["failed_count"] == 0 else "failed"
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)
        return 0 if result["failed_count"] == 0 else 1

    except Exception as exc:
        summary["status"] = "error"
        summary["error"] = repr(exc)
        summary["traceback"] = traceback.format_exc()
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)
        logger.exception("release-aware refresh entry-point crashed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
