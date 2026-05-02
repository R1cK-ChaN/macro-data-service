"""Release-calendar resolvers and availability checks — pure date-math, no DB dependency."""
from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Sequence
from zoneinfo import ZoneInfo

# Re-use the dataclass from storage when available; if imported standalone
# we only need the fields listed in the protocol below.
try:
    from storage.sqlite import ReleaseScheduleRecord
except Exception:  # pragma: no cover
    ReleaseScheduleRecord = Any  # type: ignore[assignment,misc]


# ── Retry / availability constants ─────────────────────────────────────

# Exponential-ish backoff: 1m, 5m, 15m, 1h, 4h (in seconds)
RETRY_BACKOFF_SECONDS: list[int] = [60, 300, 900, 3600, 14400]

MAX_RETRIES: int = len(RETRY_BACKOFF_SECONDS)

# Status values for release_status
STATUS_PENDING = "PENDING"
STATUS_WAITING = "WAITING"
STATUS_FETCHED = "FETCHED"
STATUS_CONFIRMED = "CONFIRMED"
STATUS_STALE = "STALE"
STATUS_FAILED = "FAILED"

ALL_STATUSES = (STATUS_PENDING, STATUS_WAITING, STATUS_FETCHED,
                STATUS_CONFIRMED, STATUS_STALE, STATUS_FAILED)


# ── Helpers ────────────────────────────────────────────────────────────

def _skip_weekend(dt: datetime) -> datetime:
    """Shift Sat→Mon, Sun→Mon."""
    wd = dt.weekday()  # 0=Mon … 6=Sun
    if wd == 5:
        return dt + timedelta(days=2)
    if wd == 6:
        return dt + timedelta(days=1)
    return dt


def _month_add(dt: datetime, months: int) -> datetime:
    """Add *months* calendar months, clamping day to month length."""
    m = dt.month - 1 + months
    year = dt.year + m // 12
    month = m % 12 + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)


def _observed_fed_holidays(year: int) -> set[date]:
    """US Federal Reserve holidays observed on weekdays."""

    def _nth_weekday(month: int, weekday: int, ordinal: int) -> date:
        count = 0
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            candidate = date(year, month, day)
            if candidate.weekday() == weekday:
                count += 1
                if count == ordinal:
                    return candidate
        raise ValueError("weekday ordinal outside month")

    def _last_weekday(month: int, weekday: int) -> date:
        for day in range(calendar.monthrange(year, month)[1], 0, -1):
            candidate = date(year, month, day)
            if candidate.weekday() == weekday:
                return candidate
        raise ValueError("weekday outside month")

    def _observed(month: int, day: int) -> date | None:
        holiday = date(year, month, day)
        if holiday.weekday() == 6:
            return holiday + timedelta(days=1)
        if holiday.weekday() == 5:
            return None
        return holiday

    holidays = {
        _nth_weekday(1, 0, 3),   # Martin Luther King Jr. Day
        _nth_weekday(2, 0, 3),   # Washington's Birthday
        _last_weekday(5, 0),     # Memorial Day
        _nth_weekday(9, 0, 1),   # Labor Day
        _nth_weekday(10, 0, 2),  # Columbus Day
        _nth_weekday(11, 3, 4),  # Thanksgiving Day
    }
    for fixed in (
        _observed(1, 1),
        _observed(6, 19),
        _observed(7, 4),
        _observed(11, 11),
        _observed(12, 25),
    ):
        if fixed is not None:
            holidays.add(fixed)
    return holidays


def _nth_weekday_date(year: int, month: int, weekday: int, ordinal: int) -> date:
    count = 0
    for day in range(1, calendar.monthrange(year, month)[1] + 1):
        candidate = date(year, month, day)
        if candidate.weekday() == weekday:
            count += 1
            if count == ordinal:
                return candidate
    raise ValueError("weekday ordinal outside month")


def _japan_equinox_day(year: int, *, autumn: bool) -> int:
    if autumn:
        return int(23.2488 + 0.242194 * (year - 1980) - ((year - 1980) // 4))
    return int(20.8431 + 0.242194 * (year - 1980) - ((year - 1980) // 4))


def _japan_public_holidays(year: int) -> set[date]:
    holidays = {
        date(year, 1, 1),   # New Year's Day
        _nth_weekday_date(year, 1, 0, 2),   # Coming of Age Day
        date(year, 2, 11),  # National Foundation Day
        date(year, 2, 23),  # Emperor's Birthday
        date(year, 3, _japan_equinox_day(year, autumn=False)),
        date(year, 4, 29),  # Showa Day
        date(year, 5, 3),   # Constitution Memorial Day
        date(year, 5, 4),   # Greenery Day
        date(year, 5, 5),   # Children's Day
        _nth_weekday_date(year, 7, 0, 3),   # Marine Day
        date(year, 8, 11),  # Mountain Day
        _nth_weekday_date(year, 9, 0, 3),   # Respect for the Aged Day
        date(year, 9, _japan_equinox_day(year, autumn=True)),
        _nth_weekday_date(year, 10, 0, 2),  # Sports Day
        date(year, 11, 3),  # Culture Day
        date(year, 11, 23), # Labor Thanksgiving Day
    }

    observed = set(holidays)
    for holiday in sorted(holidays):
        if holiday.weekday() != 6:
            continue
        substitute = holiday + timedelta(days=1)
        while substitute in observed:
            substitute += timedelta(days=1)
        observed.add(substitute)

    day = date(year, 1, 2)
    last = date(year, 12, 30)
    while day <= last:
        if (
            day.weekday() < 5
            and day not in observed
            and day - timedelta(days=1) in observed
            and day + timedelta(days=1) in observed
        ):
            observed.add(day)
        day += timedelta(days=1)

    return observed


def _is_business_day(day: date, calendar_name: str) -> bool:
    if day.weekday() >= 5:
        return False
    if calendar_name in {"japan", "jp", "japan_public", "jp_public"}:
        return day not in _japan_public_holidays(day.year)
    return True


def _apply_rule_time(candidate: datetime, rule: dict[str, Any]) -> datetime:
    time_text = str(rule.get("time", "") or rule.get("release_time", "")).strip()
    if not time_text:
        return candidate
    parts = time_text.split(":", 1)
    hour = int(parts[0])
    minute = int(parts[1]) if len(parts) > 1 else 0
    tz_name = str(rule.get("timezone", "UTC") or "UTC")
    local_dt = datetime(
        candidate.year,
        candidate.month,
        candidate.day,
        hour,
        minute,
        tzinfo=ZoneInfo(tz_name),
    )
    return local_dt.astimezone(timezone.utc)


# ── Individual resolvers ──────────────────────────────────────────────

def _next_day_of_month(rule: dict[str, Any], ref: datetime) -> datetime | None:
    day = int(rule.get("day", 1))
    tol = int(rule.get("tolerance_days", 0))
    _ = tol  # tolerance used by scheduler, not resolver

    # Can we still hit this month?
    max_day = calendar.monthrange(ref.year, ref.month)[1]
    target_day = min(day, max_day)
    candidate = ref.replace(day=target_day, hour=0, minute=0, second=0, microsecond=0)
    if candidate > ref:
        return _skip_weekend(candidate)
    # Next month
    nxt = _month_add(ref, 1)
    max_day = calendar.monthrange(nxt.year, nxt.month)[1]
    target_day = min(day, max_day)
    candidate = nxt.replace(day=target_day, hour=0, minute=0, second=0, microsecond=0)
    return _skip_weekend(candidate)


def _next_weekday_of_month(rule: dict[str, Any], ref: datetime) -> datetime | None:
    weekday = int(rule.get("weekday", 4))  # 0=Mon … 6=Sun
    ordinal = int(rule.get("ordinal", 1))  # 1st, 2nd, …

    def _find(year: int, month: int) -> datetime | None:
        cal = calendar.monthcalendar(year, month)
        count = 0
        for week in cal:
            if week[weekday] != 0:
                count += 1
                if count == ordinal:
                    return datetime(year, month, week[weekday],
                                    tzinfo=timezone.utc)
        return None

    candidate = _find(ref.year, ref.month)
    if candidate is not None and candidate > ref:
        return candidate  # weekday targets are never weekend-shifted

    # Advance to next month
    nxt = _month_add(ref, 1)
    return _find(nxt.year, nxt.month)


def _next_business_day_of_month(rule: dict[str, Any], ref: datetime) -> datetime | None:
    ordinal = int(rule.get("ordinal", 1))

    def _find(year: int, month: int) -> datetime | None:
        count = 0
        holiday_calendar = str(rule.get("calendar", "")).lower()
        holidays = (
            _observed_fed_holidays(year)
            if holiday_calendar in {"us_federal", "us_fed", "federal_reserve"}
            else set()
        )
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            candidate = datetime(year, month, day, tzinfo=timezone.utc)
            if candidate.weekday() >= 5 or candidate.date() in holidays:
                continue
            count += 1
            if count == ordinal:
                return _apply_rule_time(candidate, rule)
        return None

    candidate = _find(ref.year, ref.month)
    if candidate is not None and candidate > ref:
        return candidate
    nxt = _month_add(ref, 1)
    return _find(nxt.year, nxt.month)


def _next_quarter_lag(rule: dict[str, Any], ref: datetime) -> datetime | None:
    lag_days = int(rule.get("lag_days", 30))
    quarter_ends = [
        datetime(ref.year, 3, 31, tzinfo=timezone.utc),
        datetime(ref.year, 6, 30, tzinfo=timezone.utc),
        datetime(ref.year, 9, 30, tzinfo=timezone.utc),
        datetime(ref.year, 12, 31, tzinfo=timezone.utc),
        datetime(ref.year + 1, 3, 31, tzinfo=timezone.utc),
        datetime(ref.year + 1, 6, 30, tzinfo=timezone.utc),
    ]
    for qe in quarter_ends:
        release = qe + timedelta(days=lag_days)
        release = _skip_weekend(release)
        if release > ref:
            return release
    return None


def _next_daily(rule: dict[str, Any], ref: datetime) -> datetime | None:
    """Next business day."""
    time_text = str(rule.get("time", "") or rule.get("release_time", "")).strip()
    if time_text:
        tz_name = str(rule.get("timezone", "UTC") or "UTC")
        tz = ZoneInfo(tz_name)
        ref_utc = ref if ref.tzinfo is not None else ref.replace(tzinfo=timezone.utc)
        local_ref = ref_utc.astimezone(tz)
        candidate_date = local_ref.date()
        calendar_name = str(rule.get("calendar", "") or "").lower()
        for _ in range(8):
            local_candidate = datetime(
                candidate_date.year,
                candidate_date.month,
                candidate_date.day,
                tzinfo=tz,
            )
            if _is_business_day(candidate_date, calendar_name):
                release = _apply_rule_time(local_candidate, rule)
                if release > ref_utc:
                    return release
            candidate_date += timedelta(days=1)
        return None

    candidate = ref + timedelta(days=1)
    candidate = candidate.replace(hour=0, minute=0, second=0, microsecond=0)
    return _skip_weekend(candidate)


def _next_weekly(rule: dict[str, Any], ref: datetime) -> datetime | None:
    target_weekday = int(rule.get("weekday", 3))  # 0=Mon
    days_ahead = target_weekday - ref.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    candidate = ref + timedelta(days=days_ahead)
    return candidate.replace(hour=0, minute=0, second=0, microsecond=0)


def _next_fixed_dates(rule: dict[str, Any], ref: datetime) -> datetime | None:
    dates = rule.get("dates", [])
    for d in sorted(dates):
        dt = datetime.fromisoformat(d)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if dt > ref:
            return dt
    return None


def _next_approximate_window(rule: dict[str, Any], ref: datetime) -> datetime | None:
    month_offset = int(rule.get("month_offset", 3))
    window_days = int(rule.get("window_days", 15))

    # For quarterly/annual data: find the next reference-period end whose
    # release window midpoint is still in the future.
    # We check quarterly boundaries going backward from ref.
    quarter_ends = []
    for y in (ref.year - 1, ref.year, ref.year + 1):
        for qe_args in [(3, 31), (6, 30), (9, 30), (12, 31)]:
            quarter_ends.append(datetime(y, qe_args[0], qe_args[1], tzinfo=timezone.utc))
    quarter_ends.sort()

    for qe in quarter_ends:
        release_center = _month_add(qe, month_offset)
        if release_center > ref:
            return release_center
    return None


def _next_monthly_lag(rule: dict[str, Any], ref: datetime) -> datetime | None:
    lag_months = int(rule.get("lag_months", 1))
    day = int(rule.get("day", 15))
    tol = int(rule.get("tolerance_days", 5))
    _ = tol

    # The "reference month" is the data period. The release comes lag_months later.
    # Walk backwards from ref to find the first reference month whose release is still future.
    for offset in range(0, 24):
        ref_month = _month_add(ref, -offset)
        release_month = _month_add(ref_month, lag_months)
        max_day = calendar.monthrange(release_month.year, release_month.month)[1]
        target_day = min(day, max_day)
        candidate = release_month.replace(day=target_day, hour=0, minute=0, second=0, microsecond=0)
        candidate = _skip_weekend(candidate)
        if candidate > ref:
            best = candidate
        else:
            break
    # Return the earliest future candidate we found
    # (the loop walks backwards, so we want the *last* hit before it went past)
    # Re-do forward scan:
    for offset in range(0, 24):
        ref_month = _month_add(ref, -offset)
        release_month = _month_add(ref_month, lag_months)
        max_day = calendar.monthrange(release_month.year, release_month.month)[1]
        target_day = min(day, max_day)
        candidate = release_month.replace(day=target_day, hour=0, minute=0, second=0, microsecond=0)
        candidate = _skip_weekend(candidate)
        if candidate > ref:
            return candidate
    return None


# ── Dispatcher ────────────────────────────────────────────────────────

_RESOLVERS: dict[str, Callable[..., datetime | None]] = {
    "day_of_month": _next_day_of_month,
    "weekday_of_month": _next_weekday_of_month,
    "business_day_of_month": _next_business_day_of_month,
    "quarter_lag": _next_quarter_lag,
    "daily": _next_daily,
    "weekly": _next_weekly,
    "fixed_dates": _next_fixed_dates,
    "approximate_window": _next_approximate_window,
    "monthly_lag": _next_monthly_lag,
}


# ── Public API ────────────────────────────────────────────────────────

def next_expected_release(
    rule_type: str,
    rule_json: dict[str, Any],
    *,
    reference: datetime | None = None,
) -> datetime | None:
    """Dispatch to rule-type-specific resolver. Returns None if unresolvable."""
    ref = reference or datetime.now(timezone.utc)
    resolver = _RESOLVERS.get(rule_type)
    if resolver is None:
        return None
    return resolver(rule_json, ref)


def is_due(
    next_expected: str,
    *,
    now: datetime | None = None,
    window_minutes: int = 120,
) -> bool:
    """True if next_expected is within window_minutes of now (or past)."""
    if not next_expected:
        return False
    _now = now or datetime.now(timezone.utc)
    try:
        nxt = datetime.fromisoformat(next_expected)
    except (ValueError, TypeError):
        return False
    if nxt.tzinfo is None:
        nxt = nxt.replace(tzinfo=timezone.utc)
    return nxt <= _now + timedelta(minutes=window_minutes)


def check_due_concepts(
    schedules: Sequence[Any],
    *,
    now: datetime | None = None,
    window_minutes: int = 120,
) -> list[Any]:
    """Filter to schedules whose next_expected is due."""
    return [
        s for s in schedules
        if is_due(s.next_expected, now=now, window_minutes=window_minutes)
    ]


# ── Availability / freshness logic ────────────────────────────────────

def expected_reference_period(
    frequency: str,
    *,
    reference: datetime | None = None,
) -> str:
    """Return the minimum observation date we'd expect *after* a release.

    For example, after a monthly CPI release in March 2026, we expect at least
    one observation dated 2026-02 (the reference month for the just-released data).
    """
    ref = reference or datetime.now(timezone.utc)
    if frequency == "daily":
        # Expect yesterday (or last business day)
        d = ref - timedelta(days=1)
        while d.weekday() >= 5:  # skip weekends backwards
            d -= timedelta(days=1)
        return d.strftime("%Y-%m-%d")
    if frequency == "weekly":
        # Expect data within the last 7 days
        d = ref - timedelta(days=7)
        return d.strftime("%Y-%m-%d")
    if frequency == "monthly":
        # Expect the previous month (at least)
        prev = _month_add(ref, -1)
        return prev.strftime("%Y-%m-01")
    if frequency == "quarterly":
        # Expect the previous quarter's start
        q_month = ((ref.month - 1) // 3) * 3 + 1  # current quarter start
        # Previous quarter start
        prev_q = _month_add(datetime(ref.year, q_month, 1, tzinfo=timezone.utc), -3)
        return prev_q.strftime("%Y-%m-01")
    if frequency == "annual":
        return f"{ref.year - 1}-01-01"
    # Fallback: any data within last 90 days
    return (ref - timedelta(days=90)).strftime("%Y-%m-%d")


def is_data_fresh(
    latest_obs_date: str | None,
    frequency: str,
    *,
    reference: datetime | None = None,
) -> bool:
    """Check whether the latest observation is fresh enough for the given frequency.

    Returns True if ``latest_obs_date >= expected_reference_period(frequency)``.
    """
    if not latest_obs_date:
        return False
    threshold = expected_reference_period(frequency, reference=reference)
    return latest_obs_date >= threshold


def compute_next_retry(
    attempt_count: int,
    *,
    reference: datetime | None = None,
) -> datetime | None:
    """Return the next retry time based on attempt count, or None if exhausted."""
    if attempt_count >= MAX_RETRIES:
        return None
    ref = reference or datetime.now(timezone.utc)
    delay = RETRY_BACKOFF_SECONDS[min(attempt_count, len(RETRY_BACKOFF_SECONDS) - 1)]
    return ref + timedelta(seconds=delay)


# ── Alert types ───────────────────────────────────────────────────────

ALERT_DELAY = "DELAY"
ALERT_MISMATCH = "MISMATCH"
ALERT_FAILED = "FAILED"


@dataclass(frozen=True)
class Alert:
    alert_type: str     # DELAY, MISMATCH, FAILED
    concept_id: str
    source: str
    message: str


def check_alerts(
    statuses: Sequence[Any],
    *,
    schedules_by_id: dict[str, Any] | None = None,
    cross_source_diffs: dict[str, float] | None = None,
    now: datetime | None = None,
    delay_minutes: int = 30,
    mismatch_threshold_pct: float = 1.0,
) -> list[Alert]:
    """Scan release statuses and emit alerts. Pure function, no DB.

    Three alert types:
    1. DELAY  — data not confirmed after expected_release + delay_minutes
    2. FAILED — retries exhausted
    3. MISMATCH — cross-source value difference exceeds threshold
    """
    _now = now or datetime.now(timezone.utc)
    alerts: list[Alert] = []

    for rs in statuses:
        cid = rs.concept_id if hasattr(rs, "concept_id") else rs.get("concept_id", "")
        status = rs.status if hasattr(rs, "status") else rs.get("status", "")
        release_date = rs.release_date if hasattr(rs, "release_date") else rs.get("release_date", "")
        attempt_count = rs.attempt_count if hasattr(rs, "attempt_count") else rs.get("attempt_count", 0)
        source_used = rs.source_used if hasattr(rs, "source_used") else rs.get("source_used", "")
        error = rs.error if hasattr(rs, "error") else rs.get("error", "")

        # Alert 1: DELAY — not confirmed and past release + delay
        if status not in (STATUS_CONFIRMED, STATUS_FETCHED) and release_date:
            try:
                rd = datetime.fromisoformat(release_date)
                if rd.tzinfo is None:
                    rd = rd.replace(tzinfo=timezone.utc)
                if _now > rd + timedelta(minutes=delay_minutes):
                    alerts.append(Alert(
                        alert_type=ALERT_DELAY,
                        concept_id=cid,
                        source=source_used or "unknown",
                        message=f"{cid} delayed (status={status})",
                    ))
            except (ValueError, TypeError):
                pass

        # Alert 2: FAILED — retries exhausted
        if status == STATUS_FAILED:
            alerts.append(Alert(
                alert_type=ALERT_FAILED,
                concept_id=cid,
                source=source_used or "unknown",
                message=f"{cid} fetch failed ({attempt_count} retries): {error}",
            ))

    # Alert 3: MISMATCH — cross-source divergence
    if cross_source_diffs:
        for cid, diff_pct in cross_source_diffs.items():
            if abs(diff_pct) > mismatch_threshold_pct:
                alerts.append(Alert(
                    alert_type=ALERT_MISMATCH,
                    concept_id=cid,
                    source="cross_source",
                    message=f"{cid} mismatch ({diff_pct:+.2f}%)",
                ))

    return alerts
