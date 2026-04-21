"""Tests for the BEA live-probe scaffold (issue #9 P2b).

No real HTTP. Exercises the probe planner, observation field-diff,
dry-parse path, and the runner's auth-missing / empty-match / http-
error / happy-path branches through a fake :class:`BEAClient`.

Mirrors :mod:`tests.test_bls_live_probe_scaffold` — the two probe lanes
share shape deliberately so review + maintenance stays cheap. The
validator is loaded via :mod:`importlib.util` because it ships as a
script, not a package member.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from ingestion.timeseries.scrapers.bea import BEAObservation


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


class _FakeBEAClient:
    """Duck-typed stand-in for :class:`BEAClient`.

    ``api_key`` drives the auth-missing branch; ``get_data`` returns
    whatever the test seeded keyed on ``(dataset, TableName, Frequency)``
    or the catch-all ``"*"`` bucket. ``raise_with`` forces an exception
    to exercise the http-error path.
    """

    def __init__(
        self,
        *,
        api_key: str = "test-key",
        observations: dict | None = None,
        raise_with: Exception | None = None,
    ):
        self.api_key = api_key
        self._observations = observations or {}
        self._raise = raise_with
        self.calls: list[tuple[str, dict]] = []

    def get_data(self, dataset_name: str, **params) -> list[BEAObservation]:
        self.calls.append((dataset_name, dict(params)))
        if self._raise is not None:
            raise self._raise
        key = (
            dataset_name,
            params.get("TableName", ""),
            params.get("Frequency", ""),
        )
        if key in self._observations:
            return list(self._observations[key])
        return list(self._observations.get("*", []))


def _gdp_obs(
    *, time_period: str = "2026Q1", value: str = "2.4",
    line_number: str = "1", note_ref: str = "T10101",
) -> BEAObservation:
    """Build a BEA observation the fixture tests already validated
    against the real API row shape (see ``tests/test_bea_calendar_api_scaffold.py``)."""
    # End-of-quarter ISO — matches ``BEAClient._normalize_time_period``.
    year = time_period[:4]
    q = int(time_period[-1])
    quarter_ends = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
    iso = f"{year}-{quarter_ends[q]}"
    return BEAObservation(
        series_id="BEA_NIPA_T10101_1",
        date=iso,
        value=float(value),
        table_name="T10101",
        line_number=line_number,
        line_description="Gross domestic product",
        raw={
            "TimePeriod":      time_period,
            "DataValue":       value,
            "LineNumber":      line_number,
            "LineDescription": "Gross domestic product",
            "NoteRef":         note_ref,
        },
    )


def _neighbour_line_obs(line_number: str = "2") -> BEAObservation:
    """Same table, different line — the runner must filter this out."""
    return BEAObservation(
        series_id=f"BEA_NIPA_T10101_{line_number}",
        date="2026-03-31",
        value=1.5,
        table_name="T10101",
        line_number=line_number,
        line_description="Personal consumption expenditures",
        raw={
            "TimePeriod":      "2026Q1",
            "DataValue":       "1.5",
            "LineNumber":      line_number,
            "LineDescription": "Personal consumption expenditures",
            "NoteRef":         "T10101",
        },
    )


# ──────────────────────────────────────────────────────────────────────────
# plan_bea_probes
# ──────────────────────────────────────────────────────────────────────────


def test_planner_ships_one_probe_per_registry_entry(validator) -> None:
    from ingestion.calendar.bea_api import INDICATOR_REGISTRY

    probes = validator.plan_bea_probes()
    assert {p.series_id for p in probes} == set(INDICATOR_REGISTRY.keys())
    assert len(probes) == len(INDICATOR_REGISTRY)


def test_planner_covers_gdp_and_personal_income_anchors(validator) -> None:
    probes = validator.plan_bea_probes()
    by_sid = {p.series_id: p for p in probes}
    assert "BEA_NIPA_T10101_1" in by_sid
    assert "BEA_NIPA_T20600_1" in by_sid
    assert by_sid["BEA_NIPA_T10101_1"].name == "gdp_two_year_window"
    assert by_sid["BEA_NIPA_T20600_1"].name == "personal_income_two_year_window"


def test_planner_window_spans_two_years(validator) -> None:
    probes = validator.plan_bea_probes()
    for probe in probes:
        assert probe.end_year - probe.start_year == 1


def test_planner_names_are_unique(validator) -> None:
    probes = validator.plan_bea_probes()
    names = [p.name for p in probes]
    assert len(names) == len(set(names))


def test_planner_carries_coordinates_for_each_spec(validator) -> None:
    """The runner reads ``dataset`` / ``table`` / ``line_number`` / ``frequency``
    off the probe; drift between spec and probe would silently mis-route."""
    from ingestion.calendar.bea_api import INDICATOR_REGISTRY

    probes = {p.series_id: p for p in validator.plan_bea_probes()}
    for sid, spec in INDICATOR_REGISTRY.items():
        p = probes[sid]
        assert p.dataset == spec.dataset
        assert p.table == spec.table
        assert p.line_number == spec.line_number
        assert p.frequency == spec.frequency


# ──────────────────────────────────────────────────────────────────────────
# _diff_bea_observation
# ──────────────────────────────────────────────────────────────────────────


def test_diff_happy_path(validator) -> None:
    diff = validator._diff_bea_observation(_gdp_obs().raw)
    assert diff.missing_expected == []
    assert diff.unknown_observed == []
    assert diff.type_warnings == []


def test_diff_flags_unknown_field(validator) -> None:
    raw = {**_gdp_obs().raw, "CL_UNIT": "pct_chg"}
    diff = validator._diff_bea_observation(raw)
    assert "CL_UNIT" in diff.unknown_observed


def test_diff_flags_missing_time_period(validator) -> None:
    raw = {**_gdp_obs().raw}
    raw.pop("TimePeriod")
    diff = validator._diff_bea_observation(raw)
    assert "TimePeriod" in diff.missing_expected
    assert any("TimePeriod" in w for w in diff.type_warnings)


def test_diff_flags_empty_time_period(validator) -> None:
    raw = {**_gdp_obs().raw, "TimePeriod": ""}
    diff = validator._diff_bea_observation(raw)
    # Empty string is present but semantically missing — parser would
    # synthesize an empty reference date.
    assert any("TimePeriod" in w for w in diff.type_warnings)


def test_diff_flags_missing_data_value(validator) -> None:
    raw = {**_gdp_obs().raw}
    raw.pop("DataValue")
    diff = validator._diff_bea_observation(raw)
    assert "DataValue" in diff.missing_expected
    assert any("DataValue missing" in w for w in diff.type_warnings)


def test_diff_flags_non_string_data_value(validator) -> None:
    raw = {**_gdp_obs().raw, "DataValue": 2.4}
    diff = validator._diff_bea_observation(raw)
    assert any("DataValue" in w and "float" in w for w in diff.type_warnings)


def test_diff_flags_missing_line_number(validator) -> None:
    raw = {**_gdp_obs().raw}
    raw.pop("LineNumber")
    diff = validator._diff_bea_observation(raw)
    assert "LineNumber" in diff.missing_expected
    assert any("LineNumber" in w for w in diff.type_warnings)


# ──────────────────────────────────────────────────────────────────────────
# _try_parse_bea
# ──────────────────────────────────────────────────────────────────────────


def test_try_parse_projects_real_observation(validator) -> None:
    ok, msg = validator._try_parse_bea(_gdp_obs())
    assert ok is True
    assert "indicator=GDP" in msg


def test_try_parse_rejects_off_whitelist_series(validator) -> None:
    off = BEAObservation(
        series_id="BEA_NIPA_BOGUS_TABLE_99",
        date="2026-03-31",
        value=1.0,
        table_name="BOGUS",
        line_number="99",
        line_description="",
        raw={},
    )
    ok, msg = validator._try_parse_bea(off)
    assert ok is False
    assert "BEA_NIPA_BOGUS_TABLE_99" in msg


# ──────────────────────────────────────────────────────────────────────────
# run_bea_probe
# ──────────────────────────────────────────────────────────────────────────


def test_runner_skips_when_api_key_missing(validator) -> None:
    probe = validator.plan_bea_probes()[0]
    client = _FakeBEAClient(api_key="")
    result = validator.run_bea_probe(client, probe)
    assert result.status == "auth_missing"
    assert client.calls == []


def test_runner_reports_http_error(validator) -> None:
    probe = validator.plan_bea_probes()[0]
    client = _FakeBEAClient(raise_with=RuntimeError("boom"))
    result = validator.run_bea_probe(client, probe)
    assert result.status == "http_error"
    assert any("RuntimeError" in n for n in result.notes)


def test_runner_handles_zero_matching_lines(validator) -> None:
    """BEA returned rows for other lines but none for the probe's line —
    an upstream coordinate drift we want loudly surfaced in the report."""
    probe = next(
        p for p in validator.plan_bea_probes()
        if p.series_id == "BEA_NIPA_T10101_1"
    )
    client = _FakeBEAClient(observations={"*": [_neighbour_line_obs("2")]})
    result = validator.run_bea_probe(client, probe)
    assert result.status == "ok"
    assert result.row_count == 0
    assert any("LineNumber=1" in n for n in result.notes)


def test_runner_populates_field_diff_and_parse_samples(validator) -> None:
    probe = next(
        p for p in validator.plan_bea_probes()
        if p.series_id == "BEA_NIPA_T10101_1"
    )
    rows = [
        _gdp_obs(time_period="2026Q1", value="2.4"),
        _gdp_obs(time_period="2025Q4", value="2.1"),
        _gdp_obs(time_period="2025Q3", value="3.0"),
        _neighbour_line_obs("2"),  # filtered out
    ]
    client = _FakeBEAClient(observations={"*": rows})
    result = validator.run_bea_probe(client, probe)
    assert result.status == "ok"
    # Three GDP rows survive the line filter; the PCE neighbour is dropped.
    assert result.row_count == 3
    # Sample is newest-first.
    assert result.sample_row["TimePeriod"] == "2026Q1"
    assert result.field_diff is not None
    assert result.field_diff.missing_expected == []
    assert result.parse_attempts == 3
    assert result.parse_successes == 3
    assert "TimePeriod" in result.enum_counters
    assert sum(result.enum_counters["TimePeriod"].values()) == 3


def test_runner_records_call_with_year_window(validator) -> None:
    probe = next(
        p for p in validator.plan_bea_probes()
        if p.series_id == "BEA_NIPA_T10101_1"
    )
    client = _FakeBEAClient(observations={"*": [_gdp_obs()]})
    validator.run_bea_probe(client, probe)
    assert len(client.calls) == 1
    dataset, params = client.calls[0]
    assert dataset == probe.dataset
    assert params["TableName"] == probe.table
    assert params["Frequency"] == probe.frequency
    # Year is the comma-rendered inclusive window.
    expected_years = ",".join(
        str(y) for y in range(probe.start_year, probe.end_year + 1)
    )
    assert params["Year"] == expected_years


# ──────────────────────────────────────────────────────────────────────────
# CLI dispatch — BEA now wired, fed/ecb/nbs still stub
# ──────────────────────────────────────────────────────────────────────────


def test_bea_no_longer_unwired(validator) -> None:
    assert "bea" not in (
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


def test_bea_dry_run_prints_plan(
    validator, capsys: pytest.CaptureFixture,
) -> None:
    rc = validator.main(["--provider", "bea"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "DRY RUN (bea)" in out
    assert "BEA_NIPA_T10101_1" not in out  # summary uses coordinate, not series-id
    assert "T10101" in out
    assert "T20600" in out
