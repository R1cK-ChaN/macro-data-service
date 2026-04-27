"""BLS calendar validation — planner, runner, diff helpers.

One probe per entry in the BLS calendar ``INDICATOR_REGISTRY``; each
probe drives :class:`BLSClient.get_series_single` over a two-year
window (current year + prior).
"""

from __future__ import annotations

import re
import time as _time
from collections import Counter
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any

from ingestion.calendar.bls_api import (
    INDICATOR_REGISTRY as BLS_INDICATOR_REGISTRY,
    parse_observation as parse_bls_observation,
)
from ingestion.timeseries.scrapers.bls import BLSClient

from scripts.validate._shared import Probe, ProbeResult, RowDiff


# Field set each BLSObservation carries in its ``raw`` dict — this is what
# the BLS API returns per observation row inside ``Results.series[].data[]``.
# The parser reads ``year`` + ``period`` to compute ``date``, ``value`` for
# the numeric, and keeps the entire dict in ``BLSObservation.raw`` so
# footnote-only revisions register a new content hash on the calendar side.
BLS_OBS_EXPECTED_FIELDS: frozenset[str] = frozenset({
    "year", "period", "periodName", "value", "footnotes", "latest",
})

# Structural assertions on a parsed BLSObservation:
#   - period matches M01..M13 (monthly) / Q01..Q05 (quarterly) / A01 / S01..S02.
#   - value is numeric (the client's _parse_observation already coerces).
#   - date is ISO YYYY-MM-DD.
_BLS_PERIOD_RE = re.compile(r"^(M0[1-9]|M1[0-3]|Q0[1-5]|A01|S0[1-2])$")


@dataclass
class BLSProbe:
    """One BLS live-validation probe — a single series + year window.

    Separate dataclass from :class:`Probe` because BLS probes drive
    ``BLSClient.get_series_single`` rather than a raw-path HTTP call;
    the generic Probe shape (``path``, ``params``, ``rows_key``,
    ``subtype``) doesn't fit cleanly.
    """

    name: str
    series_id: str
    indicator: str          # canonical token (``"CPI"``, ``"NFP"``)
    description: str
    start_year: int
    end_year: int


def plan_bls_probes() -> list[BLSProbe]:
    """One probe per entry in the BLS calendar INDICATOR_REGISTRY.

    P1c expanded the whitelist from 2 anchors (CPI + NFP) to 11
    indicators covering the full BLS headline set. Each probe hits
    one series with a two-year window — the current year plus the
    prior year — which guarantees coverage across the monthly /
    quarterly release cadences the whitelist mixes. Total BLS API
    usage per live run is one request per series (~11), well under
    the 500-requests-per-day budget.
    """
    now_year = datetime.now(timezone.utc).year
    probes: list[BLSProbe] = []
    for series_id, spec in BLS_INDICATOR_REGISTRY.items():
        token = _bls_probe_token(spec.indicator)
        probes.append(
            BLSProbe(
                name=f"{token}_two_year_window",
                series_id=series_id,
                indicator=token.upper(),
                description=spec.title,
                start_year=now_year - 1,
                end_year=now_year,
            )
        )
    return probes


def _bls_probe_token(indicator_label: str) -> str:
    """Slugify an indicator label into a stable probe-name token."""
    token = indicator_label.strip().lower()
    token = re.sub(r"[^a-z0-9]+", "_", token).strip("_")
    return token or "indicator"


def _diff_bls_observation(raw_row: dict[str, Any]) -> RowDiff:
    """Field diff for one BLS observation row (the ``raw`` dict)."""
    observed = set(raw_row.keys())
    diff = RowDiff(
        observed_fields=sorted(observed),
        read_by_parser=sorted(observed & BLS_OBS_EXPECTED_FIELDS),
        ignored_by_parser=sorted(
            (BLS_OBS_EXPECTED_FIELDS & observed) - BLS_OBS_EXPECTED_FIELDS
        ),  # empty by construction — kept for shape symmetry
        unknown_observed=sorted(observed - BLS_OBS_EXPECTED_FIELDS),
        missing_expected=sorted(BLS_OBS_EXPECTED_FIELDS - observed),
    )

    period = raw_row.get("period", "")
    if period and not _BLS_PERIOD_RE.match(str(period)):
        diff.type_warnings.append(
            f"period={period!r} doesn't match M01-M13 / Q01-Q05 / A01 / "
            f"S01-S02 shape"
        )
    value = raw_row.get("value")
    if value is None:
        diff.type_warnings.append("value missing — parser would drop this row")
    elif not isinstance(value, str):
        diff.type_warnings.append(
            f"value is {type(value).__name__}={value!r} — parser expects str"
        )
    return diff


def _try_parse_bls(obs) -> tuple[bool, str]:
    """Dry-run the calendar-side BLS projector on one observation."""
    spec = BLS_INDICATOR_REGISTRY.get(obs.series_id)
    if spec is None:
        return False, f"no BLS calendar spec for series_id={obs.series_id!r}"
    try:
        raw, event = parse_bls_observation(
            obs, snapshot_epoch_ms=1_700_000_000_000, spec=spec,
        )
        return True, (
            f"ok indicator={spec.indicator} event_id={raw.provider_event_id[:10]}…"
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run_bls_probe(client: BLSClient, probe: BLSProbe) -> ProbeResult:
    """Execute one BLS probe and return a populated :class:`ProbeResult`.

    Runs via ``BLSClient.get_series_single`` — reuses the production
    transport layer so the probe exercises exactly the path a recurring
    fetch would. Rate-limit / auth errors surface as ``http_error``;
    empty responses surface as ``ok`` with ``row_count=0`` so the
    report still prints the probe card.
    """
    # BLS uses a POST body rather than a GET path; the audit trail
    # needs to describe both the wire shape (endpoint) and the
    # body-level identity (series id + year window) so a reader can
    # reproduce the call from the report alone.
    generic = Probe(
        name=probe.name,
        path="POST https://api.bls.gov/publicAPI/v2/timeseries/data/",
        description=probe.description,
        expected_shape="list[BLSObservation] after client parse",
        expected_fields=BLS_OBS_EXPECTED_FIELDS,
    )
    result = ProbeResult(probe=generic, status="skipped")
    result.request_path = (
        f"{generic.path} "
        f"seriesid=[{probe.series_id}] "
        f"startyear={probe.start_year} endyear={probe.end_year}"
    )

    if not client.api_key:
        result.status = "auth_missing"
        result.notes.append("BLS_API_KEY not set — probe skipped")
        return result

    t0 = _time.monotonic()
    try:
        observations = client.get_series_single(
            probe.series_id,
            start_year=probe.start_year,
            end_year=probe.end_year,
        )
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000

    result.status = "ok"
    result.row_count = len(observations)
    if not observations:
        result.notes.append("BLS returned zero observations for this window")
        return result

    # Sort newest-first so the sample row + diff reflect the most recent
    # release — that's the one whose shape we care about for schedule
    # adherence audits.
    observations = sorted(observations, key=lambda o: o.date, reverse=True)
    sample = observations[0]
    result.sample_row = sample.raw or {
        "series_id": sample.series_id,
        "date": sample.date,
        "value": sample.value,
        "period": sample.period,
    }
    if sample.raw:
        result.field_diff = _diff_bls_observation(sample.raw)

    # period / periodName tallies — surfaces mixed-frequency responses
    # (monthly + annual average rows) when the client passes
    # annual_average=False but BLS attaches A01 anyway.
    period_counter: Counter = Counter()
    for obs in observations:
        period_counter[repr(obs.period)] += 1
    result.enum_counters = {"period": period_counter}

    sample_n = min(10, len(observations))
    result.parse_attempts = sample_n
    for obs in observations[:sample_n]:
        ok, msg = _try_parse_bls(obs)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


__all__ = [
    "BLSClient",
    "BLSProbe",
    "BLS_INDICATOR_REGISTRY",
    "BLS_OBS_EXPECTED_FIELDS",
    "plan_bls_probes",
    "run_bls_probe",
]
