"""ECB SDMX calendar validation — planner, runner, diff helpers.

One probe per entry in the ECB calendar ``INDICATOR_REGISTRY``; each
probe pulls ~two years of business-daily observations and reports
both the raw count and the collapsed rate-change count so an
operator can eyeball whether the fetcher would project the expected
signal.
"""

from __future__ import annotations

import time as _time
from collections import Counter
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any

from ingestion.calendar.ecb_api import (
    INDICATOR_REGISTRY as ECB_INDICATOR_REGISTRY,
    parse_observation as parse_ecb_observation,
)
from ingestion.calendar.ecb_api.fetcher import (
    _collapse_to_rate_changes as _ecb_collapse_to_rate_changes,
)
from ingestion.timeseries.sdmx._types import SDMXObservation
from ingestion.timeseries.sdmx.providers.ecb import ECBClient

from scripts.validate._shared import Probe, ProbeResult, RowDiff


# SDMX observations arrive as a typed dataclass (:class:`SDMXObservation`)
# rather than a raw upstream dict — the SDMX parser consumes the JSON
# envelope at the boundary. "Field diff" therefore reports the parser-
# facing attributes rather than upstream keys; upstream schema drift
# surfaces as empty-results or type warnings, not UNKNOWN_OBSERVED.
ECB_OBS_EXPECTED_FIELDS: frozenset[str] = frozenset({
    "series_id", "date", "value", "dataflow",
})


@dataclass
class ECBProbe:
    """One ECB SDMX live-validation probe — a single series + date window.

    Mirrors :class:`BLSProbe` / :class:`BEAProbe`. ECB's data path is
    ``GET /service/data/{dataflow}/{key}`` — no auth, `jsondata` format,
    ``startPeriod`` / ``endPeriod`` / ``lastNObservations`` query params.
    """

    name: str
    series_id: str
    dataflow_id: str
    series_key: str
    indicator: str
    description: str
    start_period: str     # ISO date
    end_period: str       # ISO date


def plan_ecb_probes() -> list[ECBProbe]:
    """One probe per entry in the ECB calendar ``INDICATOR_REGISTRY``.

    Each probe pulls roughly two years of business-daily observations
    for the series — enough to cover multiple Governing Council
    decisions in any realistic recent window. The runner reports both
    the raw count (~500/year of business days) and the collapsed rate-
    change count (~3–6 per window) so an operator can eyeball whether
    the fetcher would project the expected signal from the firehose.

    ECB requires no auth, so there's no ``api_key`` guard to bypass.
    Three probes per run — under the endpoint's unspecified but
    generous rate limit (we throttle via the SDMX client's retry
    shape regardless).
    """
    today = datetime.now(timezone.utc).date()
    # Calendar-day rollback rather than calendar-year so a leap year
    # doesn't change the window size; 730 days fully covers two GC
    # cycles (~8 scheduled decisions per year).
    two_years_ago = today - timedelta(days=730)
    probes: list[ECBProbe] = []
    for series_id, spec in ECB_INDICATOR_REGISTRY.items():
        probes.append(
            ECBProbe(
                name=f"{spec.indicator.lower()}_two_year_window",
                series_id=series_id,
                dataflow_id=spec.dataflow_id,
                series_key=spec.series_key,
                indicator=spec.indicator,
                description=spec.title,
                start_period=two_years_ago.isoformat(),
                end_period=today.isoformat(),
            )
        )
    return probes


def _diff_ecb_observation(obs: SDMXObservation) -> RowDiff:
    """Field diff for one SDMX observation.

    "Observed" means the attributes present + non-null after the SDMX
    parser has run — ECB's JSON envelope is already consumed at that
    boundary. Missing-field / type warnings surface parser mis-
    mappings rather than upstream schema drift; a fully empty
    response from ECB (series key retired upstream) is caught at the
    runner level, not here.
    """
    present = {
        "series_id": obs.series_id,
        "date":      obs.date,
        "value":     obs.value,
        "dataflow":  obs.dataflow,
    }
    observed = {k for k, v in present.items() if v not in (None, "")}
    diff = RowDiff(
        observed_fields=sorted(observed),
        read_by_parser=sorted(observed & ECB_OBS_EXPECTED_FIELDS),
        ignored_by_parser=[],   # no ambient upstream fields — dataclass is closed
        unknown_observed=[],    # likewise
        missing_expected=sorted(ECB_OBS_EXPECTED_FIELDS - observed),
    )
    if obs.value is None:
        diff.type_warnings.append(
            "value=None — rate-level series should always carry a numeric"
        )
    elif not isinstance(obs.value, (int, float)):
        diff.type_warnings.append(
            f"value is {type(obs.value).__name__}={obs.value!r} — "
            f"SDMX parser expects numeric"
        )
    if not obs.date:
        diff.type_warnings.append("date empty — parser would drop this row")
    return diff


def _try_parse_ecb(obs: SDMXObservation) -> tuple[bool, str]:
    """Dry-run the calendar-side ECB projector on one observation."""
    spec = ECB_INDICATOR_REGISTRY.get(obs.series_id)
    if spec is None:
        return False, f"no ECB calendar spec for series_id={obs.series_id!r}"
    try:
        raw, event = parse_ecb_observation(
            obs, snapshot_epoch_ms=1_700_000_000_000, spec=spec,
        )
        return True, (
            f"ok indicator={spec.indicator} event_id={raw.provider_event_id[:10]}…"
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run_ecb_probe(client: ECBClient, probe: ECBProbe) -> ProbeResult:
    """Execute one ECB probe and return a populated :class:`ProbeResult`.

    Runs via ``ECBClient.get_data`` — reuses the production SDMX
    transport so the probe exercises exactly the path a recurring
    fetch would. An empty response lands as ``ok`` with ``row_count=0``
    plus a note that the series key may have been retired upstream
    (the signal we need for P6 parity not to silently drift).
    """
    generic = Probe(
        name=probe.name,
        path=(
            f"GET https://data-api.ecb.europa.eu/service/data/"
            f"{probe.dataflow_id}/{probe.series_key}"
        ),
        description=probe.description,
        expected_shape="list[SDMXObservation] after client parse",
        expected_fields=ECB_OBS_EXPECTED_FIELDS,
    )
    result = ProbeResult(probe=generic, status="skipped")
    result.request_path = (
        f"{generic.path}?format=jsondata"
        f"&startPeriod={probe.start_period}"
        f"&endPeriod={probe.end_period}"
    )

    t0 = _time.monotonic()
    try:
        observations = client.get_data(
            probe.dataflow_id,
            probe.series_key,
            series_id=probe.series_id,
            start_period=probe.start_period,
            end_period=probe.end_period,
            limit=0,  # no lastNObservations cap
        )
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000

    result.status = "ok"
    result.row_count = len(observations)
    if not observations:
        result.notes.append(
            "ECB returned zero observations for this series + window "
            "— series key may have been retired upstream"
        )
        return result

    # FM-lane publishes the same level every business day until the
    # Governing Council moves it. Expose both counts so the report
    # makes the ~500-per-2-years business-daily noise vs the ~8-per-
    # 2-years policy-move signal explicit.
    changes = _ecb_collapse_to_rate_changes(observations, prior_value=None)
    result.notes.append(
        f"raw observations: {len(observations)} | rate changes after "
        f"collapse: {len(changes)}"
    )

    # Newest-first via lexical date sort (ISO YYYY-MM-DD).
    sorted_obs = sorted(observations, key=lambda o: o.date, reverse=True)
    sample = sorted_obs[0]
    result.sample_row = {
        "series_id": sample.series_id,
        "date":      sample.date,
        "value":     sample.value,
        "dataflow":  sample.dataflow,
    }
    result.field_diff = _diff_ecb_observation(sample)

    # Unique rate-level tally — makes the policy-move cadence visible
    # inside the window, and flags oddities (sub-zero prints, unusual
    # precision) the parser should be ready for.
    value_counter: Counter = Counter()
    for obs in observations:
        value_counter[repr(obs.value)] += 1
    result.enum_counters = {"value": value_counter}

    # Dry-parse the collapsed-changes subset rather than the business-
    # daily firehose — the fetcher projects only these rows, so
    # they're the set whose parse failure would matter.
    target = changes if changes else sorted_obs
    sample_n = min(10, len(target))
    result.parse_attempts = sample_n
    for obs in target[:sample_n]:
        ok, msg = _try_parse_ecb(obs)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


__all__ = [
    "ECBClient",
    "ECBProbe",
    "ECB_INDICATOR_REGISTRY",
    "ECB_OBS_EXPECTED_FIELDS",
    "plan_ecb_probes",
    "run_ecb_probe",
]
