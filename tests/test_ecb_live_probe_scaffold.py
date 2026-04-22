"""Tests for the ECB live-probe scaffold (issue #9 P3b).

No real HTTP. Exercises the probe planner, observation field-diff,
dry-parse path, and the runner's http-error / empty / happy branches
through a fake :class:`ECBClient`.

Parallel to :mod:`tests.test_bls_live_probe_scaffold` and
:mod:`tests.test_bea_live_probe_scaffold`. ECB Data Portal is
unauthenticated so there's no ``auth_missing`` branch to exercise.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from ingestion.timeseries.sdmx._types import SDMXObservation


REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_validator_module():
    module_name = "validate_calendar_acquisition"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(
        module_name,
        REPO_ROOT / "scripts" / "validate_calendar_acquisition.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def validator():
    return _load_validator_module()


class _FakeECBClient:
    """Duck-typed stand-in for :class:`ECBClient`.

    ``get_data`` returns whatever the test seeded keyed on
    ``series_id``. ``raise_with`` forces an exception to exercise the
    http-error branch.
    """

    def __init__(
        self,
        *,
        observations: dict[str, list[SDMXObservation]] | None = None,
        raise_with: Exception | None = None,
    ):
        self._observations = observations or {}
        self._raise = raise_with
        self.calls: list[dict] = []

    def get_data(
        self,
        dataflow_id,
        key=".",
        *,
        series_id="",
        start_period=None,
        end_period=None,
        limit=0,
        **kwargs,
    ):
        self.calls.append({
            "dataflow_id":  dataflow_id,
            "key":          key,
            "series_id":    series_id,
            "start_period": start_period,
            "end_period":   end_period,
            "limit":        limit,
        })
        if self._raise is not None:
            raise self._raise
        return list(self._observations.get(series_id, []))


def _dfr_obs(value: float, date_str: str) -> SDMXObservation:
    return SDMXObservation(
        series_id="FM.B.U2.EUR.4F.KR.DFR.LEV",
        date=date_str,
        value=value,
        dataflow="FM",
    )


def _mro_obs(value: float, date_str: str) -> SDMXObservation:
    return SDMXObservation(
        series_id="FM.B.U2.EUR.4F.KR.MRR_FR.LEV",
        date=date_str,
        value=value,
        dataflow="FM",
    )


# ──────────────────────────────────────────────────────────────────────────
# plan_ecb_probes
# ──────────────────────────────────────────────────────────────────────────


def test_planner_ships_one_probe_per_registry_entry(validator) -> None:
    from ingestion.calendar.ecb_api import INDICATOR_REGISTRY

    probes = validator.plan_ecb_probes()
    assert {p.series_id for p in probes} == set(INDICATOR_REGISTRY.keys())
    assert len(probes) == len(INDICATOR_REGISTRY)


def test_planner_covers_mro_dfr_mlf(validator) -> None:
    probes = {p.indicator: p for p in validator.plan_ecb_probes()}
    assert {"ECB_MRO", "ECB_DFR", "ECB_MLF"} <= set(probes.keys())


def test_planner_window_spans_two_years(validator) -> None:
    """Window rolls back 730 calendar days — covers two GC cycles."""
    from datetime import date

    probes = validator.plan_ecb_probes()
    for probe in probes:
        start = date.fromisoformat(probe.start_period)
        end = date.fromisoformat(probe.end_period)
        assert (end - start).days == 730


def test_planner_names_are_unique(validator) -> None:
    probes = validator.plan_ecb_probes()
    names = [p.name for p in probes]
    assert len(names) == len(set(names))


def test_planner_carries_coordinates_for_each_spec(validator) -> None:
    from ingestion.calendar.ecb_api import INDICATOR_REGISTRY

    probes = {p.series_id: p for p in validator.plan_ecb_probes()}
    for sid, spec in INDICATOR_REGISTRY.items():
        p = probes[sid]
        assert p.dataflow_id == spec.dataflow_id
        assert p.series_key == spec.series_key
        assert p.indicator == spec.indicator


# ──────────────────────────────────────────────────────────────────────────
# _diff_ecb_observation
# ──────────────────────────────────────────────────────────────────────────


def test_diff_happy_path(validator) -> None:
    diff = validator._diff_ecb_observation(_dfr_obs(4.0, "2026-03-01"))
    assert diff.missing_expected == []
    assert diff.unknown_observed == []
    assert diff.type_warnings == []


def test_diff_flags_none_value(validator) -> None:
    obs = SDMXObservation(
        series_id="FM.B.U2.EUR.4F.KR.DFR.LEV",
        date="2026-03-01",
        value=None,  # type: ignore[arg-type]
        dataflow="FM",
    )
    diff = validator._diff_ecb_observation(obs)
    assert "value" in diff.missing_expected
    assert any("value=None" in w for w in diff.type_warnings)


def test_diff_flags_empty_date(validator) -> None:
    obs = SDMXObservation(
        series_id="FM.B.U2.EUR.4F.KR.DFR.LEV",
        date="",
        value=4.0,
        dataflow="FM",
    )
    diff = validator._diff_ecb_observation(obs)
    assert "date" in diff.missing_expected
    assert any("date empty" in w for w in diff.type_warnings)


def test_diff_flags_non_numeric_value(validator) -> None:
    obs = SDMXObservation(
        series_id="FM.B.U2.EUR.4F.KR.DFR.LEV",
        date="2026-03-01",
        value="4.0",  # type: ignore[arg-type]
        dataflow="FM",
    )
    diff = validator._diff_ecb_observation(obs)
    assert any("str" in w for w in diff.type_warnings)


# ──────────────────────────────────────────────────────────────────────────
# _try_parse_ecb
# ──────────────────────────────────────────────────────────────────────────


def test_try_parse_projects_real_observation(validator) -> None:
    ok, msg = validator._try_parse_ecb(_dfr_obs(4.0, "2026-03-01"))
    assert ok is True
    assert "indicator=ECB_DFR" in msg


def test_try_parse_rejects_off_whitelist_series(validator) -> None:
    off = SDMXObservation(
        series_id="FM.B.U2.EUR.4F.KR.BOGUS.LEV",
        date="2026-03-01",
        value=1.0,
        dataflow="FM",
    )
    ok, msg = validator._try_parse_ecb(off)
    assert ok is False
    assert "FM.B.U2.EUR.4F.KR.BOGUS.LEV" in msg


# ──────────────────────────────────────────────────────────────────────────
# run_ecb_probe
# ──────────────────────────────────────────────────────────────────────────


def test_runner_reports_http_error(validator) -> None:
    probe = validator.plan_ecb_probes()[0]
    client = _FakeECBClient(raise_with=RuntimeError("boom"))
    result = validator.run_ecb_probe(client, probe)
    assert result.status == "http_error"
    assert any("RuntimeError" in n for n in result.notes)


def test_runner_handles_empty_response(validator) -> None:
    """Empty response for a registered series → the signal the series
    key was retired upstream. Probe lands ``ok`` with row_count=0
    and a loud note."""
    probe = next(
        p for p in validator.plan_ecb_probes()
        if p.series_id == "FM.B.U2.EUR.4F.KR.DFR.LEV"
    )
    client = _FakeECBClient(observations={probe.series_id: []})
    result = validator.run_ecb_probe(client, probe)
    assert result.status == "ok"
    assert result.row_count == 0
    assert any("retired upstream" in n for n in result.notes)


def test_runner_populates_field_diff_and_parse_samples(validator) -> None:
    """Business-daily firehose: 4 observations, 1 rate change. The
    probe reports both counts and dry-parses the collapsed subset."""
    probe = next(
        p for p in validator.plan_ecb_probes()
        if p.series_id == "FM.B.U2.EUR.4F.KR.DFR.LEV"
    )
    obs = [
        _dfr_obs(4.0, "2026-03-01"),
        _dfr_obs(4.0, "2026-03-02"),
        _dfr_obs(4.0, "2026-03-03"),
        _dfr_obs(3.75, "2026-03-04"),  # policy move
    ]
    client = _FakeECBClient(observations={probe.series_id: obs})
    result = validator.run_ecb_probe(client, probe)
    assert result.status == "ok"
    assert result.row_count == 4
    # Sample is newest-first.
    assert result.sample_row["date"] == "2026-03-04"
    assert result.sample_row["value"] == pytest.approx(3.75)
    assert result.field_diff is not None
    assert result.field_diff.missing_expected == []
    # Collapsed count note surfaces.
    assert any("rate changes after collapse" in n for n in result.notes)
    # Value counter tallies unique levels (prior_value=None means the
    # first obs counts as a change → 2 collapsed rows → 2 dry-parses).
    assert "value" in result.enum_counters
    assert result.parse_attempts == 2
    assert result.parse_successes == 2


def test_runner_records_call_with_window_and_dataflow(validator) -> None:
    probe = next(
        p for p in validator.plan_ecb_probes()
        if p.series_id == "FM.B.U2.EUR.4F.KR.MRR_FR.LEV"
    )
    client = _FakeECBClient(observations={probe.series_id: [_mro_obs(4.25, "2026-03-01")]})
    validator.run_ecb_probe(client, probe)
    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["dataflow_id"] == probe.dataflow_id
    assert call["key"] == probe.series_key
    assert call["series_id"] == probe.series_id
    assert call["start_period"] == probe.start_period
    assert call["end_period"] == probe.end_period
    assert call["limit"] == 0


# ──────────────────────────────────────────────────────────────────────────
# CLI dispatch — ECB now wired, fed/nbs still stub
# ──────────────────────────────────────────────────────────────────────────


def test_ecb_no_longer_unwired(validator) -> None:
    assert "ecb" not in (
        validator._OFFICIAL_PROVIDERS
        - validator._OFFICIAL_PROVIDERS_WITH_PROBES
    )


def test_no_providers_remain_unwired(validator) -> None:
    """P5c closed out the last stub — every official provider now has
    a live probe scaffold."""
    unwired = (
        validator._OFFICIAL_PROVIDERS
        - validator._OFFICIAL_PROVIDERS_WITH_PROBES
    )
    assert unwired == frozenset()


def test_ecb_dry_run_prints_plan(
    validator, capsys: pytest.CaptureFixture,
) -> None:
    rc = validator.main(["--provider", "ecb"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "DRY RUN (ecb)" in out
    assert "FM.B.U2.EUR.4F.KR.DFR.LEV" in out
    assert "FM.B.U2.EUR.4F.KR.MRR_FR.LEV" in out
    assert "FM.B.U2.EUR.4F.KR.MLFR.LEV" in out
