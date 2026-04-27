"""BEA calendar validation — planner, runner, diff helpers.

One probe per entry in the BEA calendar ``INDICATOR_REGISTRY``; each
probe drives :class:`BEAClient.get_data` against a single
``(dataset, table, frequency)`` coordinate over a two-year window.
"""

from __future__ import annotations

import re
import time as _time
from collections import Counter
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any

from ingestion.calendar.bea_api import (
    INDICATOR_REGISTRY as BEA_INDICATOR_REGISTRY,
    parse_observation as parse_bea_observation,
)
from ingestion.timeseries.scrapers.bea import BEAClient

from scripts.validate._shared import Probe, ProbeResult, RowDiff


# Fields each BEAObservation carries in its ``raw`` dict — keys the parser
# hashes on (``_HASH_FIELDS = (DataValue, LineDescription, NoteRef)``) plus
# the identity fields (``TimePeriod``, ``LineNumber``). Anything observed
# outside this set surfaces as UNKNOWN_OBSERVED — the prompt to decide
# whether a new upstream column should be read or explicitly ignored.
BEA_OBS_EXPECTED_FIELDS: frozenset[str] = frozenset({
    "TimePeriod", "DataValue", "LineNumber", "LineDescription", "NoteRef",
})


@dataclass
class BEAProbe:
    """One BEA live-validation probe — a single series + year window.

    Mirrors :class:`BLSProbe`. BEA's query surface differs from BLS
    (``GET /api/data`` with ``DatasetName`` / ``TableName`` / ``Frequency``
    / ``Year`` rather than a POST body), so this dataclass carries the
    call coordinates the runner needs; the generic :class:`Probe` shape
    still carries the report metadata on the :class:`ProbeResult`.
    """

    name: str
    series_id: str
    indicator: str          # canonical token (``"GDP"``, ``"PERSONAL_INCOME"``)
    dataset: str            # BEA dataset name (``"NIPA"``, ``"ITA"``)
    table: str              # BEA table code (``"T10101"``, ``"T20600"``)
    line_number: str        # BEA line within the table (``"1"``)
    frequency: str          # BEA frequency code (``"Q"``, ``"M"``, ``"A"``)
    description: str
    start_year: int
    end_year: int


def plan_bea_probes() -> list[BEAProbe]:
    """One probe per entry in the BEA calendar ``INDICATOR_REGISTRY``.

    Each probe hits one ``(dataset, table, frequency)`` coordinate with
    a two-year window — current year plus prior — which guarantees
    coverage across the monthly + quarterly cadences the whitelist
    mixes. The runner filters the returned rows down to the probe's
    ``line_number`` after the response lands (BEA returns every line
    of the requested table in a single call).

    Total BEA API usage per live run is one request per indicator
    (currently 2 — GDP + Personal Income), well under the 1000-per-day
    free-tier budget. The registry includes entries with
    ``api_fetch=False`` (GDP, schedule-only) — the probe still exercises
    their API shape so an operator can verify the parser's expectations
    before enabling an API lane.
    """
    now_year = datetime.now(timezone.utc).year
    probes: list[BEAProbe] = []
    for series_id, spec in BEA_INDICATOR_REGISTRY.items():
        token = _bea_probe_token(spec.indicator)
        probes.append(
            BEAProbe(
                name=f"{token}_two_year_window",
                series_id=series_id,
                indicator=spec.indicator,
                dataset=spec.dataset,
                table=spec.table,
                line_number=spec.line_number,
                frequency=spec.frequency,
                description=spec.title,
                start_year=now_year - 1,
                end_year=now_year,
            )
        )
    return probes


def _bea_probe_token(indicator_label: str) -> str:
    """Slugify an indicator label into a stable probe-name token."""
    token = indicator_label.strip().lower()
    token = re.sub(r"[^a-z0-9]+", "_", token).strip("_")
    return token or "indicator"


def _diff_bea_observation(raw_row: dict[str, Any]) -> RowDiff:
    """Field diff for one BEA observation row (the ``raw`` dict).

    Parser reads ``TimePeriod`` (→ reference date) and ``DataValue``
    (→ numeric); hashes on ``DataValue`` + ``LineDescription`` +
    ``NoteRef`` (methodology / release-stage flags). ``LineNumber`` is
    the identity coordinate on the line within the table. Any other
    field upstream returns — ``SeriesCode``, ``CL_UNIT``,
    ``UNIT_MULT``, ``METRIC_NAME`` are documented possibilities on some
    BEA datasets — surfaces as UNKNOWN_OBSERVED so the operator decides
    whether to read or ignore it.
    """
    observed = set(raw_row.keys())
    diff = RowDiff(
        observed_fields=sorted(observed),
        read_by_parser=sorted(observed & BEA_OBS_EXPECTED_FIELDS),
        ignored_by_parser=sorted(
            (BEA_OBS_EXPECTED_FIELDS & observed) - BEA_OBS_EXPECTED_FIELDS
        ),  # empty by construction — symmetry with BLS shape
        unknown_observed=sorted(observed - BEA_OBS_EXPECTED_FIELDS),
        missing_expected=sorted(BEA_OBS_EXPECTED_FIELDS - observed),
    )

    time_period = raw_row.get("TimePeriod")
    if time_period is None or str(time_period).strip() == "":
        diff.type_warnings.append(
            "TimePeriod missing — client would synthesize an empty reference date"
        )
    data_value = raw_row.get("DataValue")
    if data_value is None:
        diff.type_warnings.append("DataValue missing — parser would write null actual")
    elif not isinstance(data_value, str):
        # BEA documents DataValue as a string ("21,542.5" / "(NA)" /
        # "(D)") so the client's ``_parse_data_value`` can run its
        # suppressed-value detection. A non-string would bypass the
        # suppressed-value check and land as ``None``.
        diff.type_warnings.append(
            f"DataValue is {type(data_value).__name__}={data_value!r} — "
            f"parser expects str"
        )
    line_number = raw_row.get("LineNumber")
    if line_number is None:
        diff.type_warnings.append(
            "LineNumber missing — line filter + series-id synthesis both break"
        )
    return diff


def _try_parse_bea(obs) -> tuple[bool, str]:
    """Dry-run the calendar-side BEA projector on one observation."""
    spec = BEA_INDICATOR_REGISTRY.get(obs.series_id)
    if spec is None:
        return False, f"no BEA calendar spec for series_id={obs.series_id!r}"
    try:
        raw, event = parse_bea_observation(
            obs, snapshot_epoch_ms=1_700_000_000_000, spec=spec,
        )
        return True, (
            f"ok indicator={spec.indicator} event_id={raw.provider_event_id[:10]}…"
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run_bea_probe(client: BEAClient, probe: BEAProbe) -> ProbeResult:
    """Execute one BEA probe and return a populated :class:`ProbeResult`.

    Runs via ``BEAClient.get_data`` — reuses the production transport so
    throttling + error handling behave identically to a recurring fetch.
    BEA returns every line for the requested table; the runner filters
    down to the probe's ``line_number`` after the call lands. An empty
    filtered set lands as ``ok`` with ``row_count=0`` and a note that
    records the full-table row count — the signal that the coordinate
    drifted upstream vs the registry.
    """
    generic = Probe(
        name=probe.name,
        path=(
            f"GET https://apps.bea.gov/api/data?DatasetName={probe.dataset}"
            f"&TableName={probe.table}"
        ),
        description=probe.description,
        expected_shape="list[BEAObservation] after client parse",
        expected_fields=BEA_OBS_EXPECTED_FIELDS,
    )
    result = ProbeResult(probe=generic, status="skipped")
    year_param = ",".join(
        str(y) for y in range(probe.start_year, probe.end_year + 1)
    )
    result.request_path = (
        f"{generic.path}"
        f"&Frequency={probe.frequency}&Year={year_param} "
        f"(filter LineNumber={probe.line_number})"
    )

    if not client.api_key:
        result.status = "auth_missing"
        result.notes.append("BEA_API_KEY not set — probe skipped")
        return result

    t0 = _time.monotonic()
    try:
        observations = client.get_data(
            probe.dataset,
            TableName=probe.table,
            Frequency=probe.frequency,
            Year=year_param,
        )
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000

    # BEA returns every line of the table; filter down to the probe's
    # line before shape-diffing so the sample row reflects the indicator
    # we actually care about rather than an arbitrary neighbour line.
    filtered = [o for o in observations if o.line_number == probe.line_number]

    result.status = "ok"
    result.row_count = len(filtered)
    if not filtered:
        result.notes.append(
            f"BEA returned zero observations for LineNumber={probe.line_number} "
            f"(table payload had {len(observations)} rows across other lines) "
            f"— coordinate may have drifted upstream"
        )
        return result

    # BEA ``obs.date`` is end-of-period ISO (quarterly/annual) or first-
    # of-month (monthly) — lexical sort descending puts the newest
    # observation first for both.
    filtered = sorted(filtered, key=lambda o: o.date, reverse=True)
    sample = filtered[0]
    result.sample_row = sample.raw or {
        "series_id": sample.series_id,
        "date": sample.date,
        "value": sample.value,
        "line_number": sample.line_number,
    }
    if sample.raw:
        result.field_diff = _diff_bea_observation(sample.raw)

    # TimePeriod tally — surfaces mixed-frequency responses (e.g. an
    # annual row landing inside a quarterly ``Frequency=Q`` query) that
    # would otherwise silently inflate the event stream.
    period_counter: Counter = Counter()
    for obs in filtered:
        period_counter[repr(obs.raw.get("TimePeriod") if obs.raw else None)] += 1
    result.enum_counters = {"TimePeriod": period_counter}

    sample_n = min(10, len(filtered))
    result.parse_attempts = sample_n
    for obs in filtered[:sample_n]:
        ok, msg = _try_parse_bea(obs)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


__all__ = [
    "BEAClient",
    "BEAProbe",
    "BEA_INDICATOR_REGISTRY",
    "BEA_OBS_EXPECTED_FIELDS",
    "plan_bea_probes",
    "run_bea_probe",
]
