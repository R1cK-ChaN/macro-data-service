"""Tests for the BLS live-probe scaffold (issue #9 P1 probe body).

No real HTTP. Exercises the probe planner, observation field-diff,
dry-parse path, and the runner's auth-missing / empty-response /
happy-path branches through a fake :class:`BLSClient`.

The validator script itself is invoked as a module (``sys.path`` +
``import scripts.validate_calendar_acquisition``) — we don't re-run
``main()`` end-to-end because the probe-result coverage is already
specific enough here.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from ingestion.timeseries.scrapers.bls import BLSObservation


REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_validator_module():
    """Load ``scripts/validate_calendar_acquisition.py`` as a module.

    The validator ships as a script, not a package member. Using
    ``importlib.util.spec_from_file_location`` keeps the test
    independent of where the script is on disk.
    """
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


class _FakeBLSClient:
    """Duck-typed stand-in for :class:`BLSClient`.

    ``api_key`` drives the auth-missing branch; ``get_series_single``
    returns whatever the test seeded for a series id. ``raise_with``
    lets a test force an exception to exercise the http-error path.
    """

    def __init__(
        self,
        *,
        api_key: str = "test-key",
        observations: dict[str, list[BLSObservation]] | None = None,
        raise_with: Exception | None = None,
    ):
        self.api_key = api_key
        self._observations = observations or {}
        self._raise = raise_with
        self.calls: list[tuple[str, int, int]] = []

    def get_series_single(
        self, series_id: str, *, start_year: int, end_year: int,
    ) -> list[BLSObservation]:
        self.calls.append((series_id, start_year, end_year))
        if self._raise is not None:
            raise self._raise
        return list(self._observations.get(series_id, []))


def _cpi_obs(date: str = "2026-03-01", value: float = 301.2) -> BLSObservation:
    year, month, _ = date.split("-")
    return BLSObservation(
        series_id="CUUR0000SA0",
        date=date,
        value=value,
        period=f"M{month}",
        raw={
            "year":       year,
            "period":     f"M{month}",
            "periodName": {
                "01": "January", "02": "February", "03": "March",
                "04": "April", "05": "May", "06": "June",
                "07": "July", "08": "August", "09": "September",
                "10": "October", "11": "November", "12": "December",
            }[month],
            "value":      str(value),
            "footnotes":  [{"code": "", "text": ""}],
            "latest":     "true",
        },
    )


# ──────────────────────────────────────────────────────────────────────────
# plan_bls_probes
# ──────────────────────────────────────────────────────────────────────────


def test_planner_ships_one_probe_per_registry_entry(validator) -> None:
    """P1c: probe plan mirrors ``INDICATOR_REGISTRY`` one-to-one."""
    from ingestion.calendar.bls_api import INDICATOR_REGISTRY

    probes = validator.plan_bls_probes()
    assert {p.series_id for p in probes} == set(INDICATOR_REGISTRY.keys())
    assert len(probes) == len(INDICATOR_REGISTRY)


def test_planner_covers_cpi_and_nfp_anchors(validator) -> None:
    """The original P1 anchors must stay in the plan after P1c grows it."""
    probes = validator.plan_bls_probes()
    by_id = {p.series_id: p for p in probes}
    assert "CUUR0000SA0" in by_id
    assert "CES0000000001" in by_id
    assert by_id["CUUR0000SA0"].name == "cpi_two_year_window"
    assert by_id["CES0000000001"].name == "nfp_two_year_window"


def test_planner_window_spans_two_years(validator) -> None:
    probes = validator.plan_bls_probes()
    for probe in probes:
        assert probe.end_year - probe.start_year == 1


def test_planner_names_are_unique(validator) -> None:
    """Probe names become file-system paths for per-probe sections in the
    validation report; duplicates would clobber each other."""
    probes = validator.plan_bls_probes()
    names = [p.name for p in probes]
    assert len(names) == len(set(names))


# ──────────────────────────────────────────────────────────────────────────
# _diff_bls_observation
# ──────────────────────────────────────────────────────────────────────────


def test_diff_happy_path(validator) -> None:
    raw = _cpi_obs().raw
    diff = validator._diff_bls_observation(raw)
    assert diff.missing_expected == []
    assert diff.unknown_observed == []
    assert diff.type_warnings == []


def test_diff_flags_unknown_field(validator) -> None:
    raw = {**_cpi_obs().raw, "new_field": "surprise"}
    diff = validator._diff_bls_observation(raw)
    assert "new_field" in diff.unknown_observed


def test_diff_flags_missing_value(validator) -> None:
    raw = {**_cpi_obs().raw}
    raw.pop("value")
    diff = validator._diff_bls_observation(raw)
    assert "value" in diff.missing_expected
    assert any("value missing" in w for w in diff.type_warnings)


def test_diff_flags_malformed_period(validator) -> None:
    raw = {**_cpi_obs().raw, "period": "X99"}
    diff = validator._diff_bls_observation(raw)
    assert any("period" in w for w in diff.type_warnings)


# ──────────────────────────────────────────────────────────────────────────
# _try_parse_bls
# ──────────────────────────────────────────────────────────────────────────


def test_try_parse_projects_real_observation(validator) -> None:
    ok, msg = validator._try_parse_bls(_cpi_obs())
    assert ok is True
    assert "indicator=CPI" in msg


def test_try_parse_rejects_off_whitelist_series(validator) -> None:
    obs = BLSObservation(
        series_id="BOGUS_SERIES",
        date="2026-03-01",
        value=1.0,
        period="M03",
        raw={"year": "2026", "period": "M03", "value": "1.0"},
    )
    ok, msg = validator._try_parse_bls(obs)
    assert ok is False
    assert "BOGUS_SERIES" in msg


# ──────────────────────────────────────────────────────────────────────────
# run_bls_probe
# ──────────────────────────────────────────────────────────────────────────


def test_runner_skips_when_api_key_missing(validator) -> None:
    probe = validator.plan_bls_probes()[0]
    client = _FakeBLSClient(api_key="")
    result = validator.run_bls_probe(client, probe)
    assert result.status == "auth_missing"
    assert client.calls == []


def test_runner_reports_http_error(validator) -> None:
    probe = validator.plan_bls_probes()[0]
    client = _FakeBLSClient(raise_with=RuntimeError("boom"))
    result = validator.run_bls_probe(client, probe)
    assert result.status == "http_error"
    assert any("RuntimeError" in n for n in result.notes)


def test_runner_handles_empty_response(validator) -> None:
    probe = validator.plan_bls_probes()[0]
    client = _FakeBLSClient(observations={probe.series_id: []})
    result = validator.run_bls_probe(client, probe)
    assert result.status == "ok"
    assert result.row_count == 0
    assert any("zero observations" in n for n in result.notes)


def test_runner_populates_field_diff_and_parse_samples(validator) -> None:
    probe = validator.plan_bls_probes()[0]
    obs = [
        _cpi_obs(date="2026-03-01", value=301.2),
        _cpi_obs(date="2026-02-01", value=300.5),
        _cpi_obs(date="2026-01-01", value=299.8),
    ]
    client = _FakeBLSClient(observations={probe.series_id: obs})
    result = validator.run_bls_probe(client, probe)
    assert result.status == "ok"
    assert result.row_count == 3
    # Sample is newest-first.
    assert result.sample_row["year"] == "2026"
    assert result.sample_row["period"] == "M03"
    assert result.field_diff is not None
    assert result.parse_attempts == 3
    assert result.parse_successes == 3
    # Period enum tally captures three distinct monthly codes.
    assert "period" in result.enum_counters
    tallies = result.enum_counters["period"]
    assert sum(tallies.values()) == 3


def test_runner_records_call_with_year_window(validator) -> None:
    probe = validator.plan_bls_probes()[0]
    client = _FakeBLSClient(observations={probe.series_id: [_cpi_obs()]})
    validator.run_bls_probe(client, probe)
    assert client.calls == [
        (probe.series_id, probe.start_year, probe.end_year),
    ]


# ──────────────────────────────────────────────────────────────────────────
# CLI dispatch — stub path for still-unwired providers
# ──────────────────────────────────────────────────────────────────────────


def test_still_unwired_provider_returns_stub(
    validator, capsys: pytest.CaptureFixture,
) -> None:
    """NBS still prints the stub message; BLS / BEA / ECB / Fed must
    *not* be on that list any more (live probes shipped in
    P1b / P2b / P3b / P4b)."""
    assert "bls" not in (
        validator._OFFICIAL_PROVIDERS
        - validator._OFFICIAL_PROVIDERS_WITH_PROBES
    )
    rc = validator.main(["--provider", "nbs"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "scaffold registered" in out


def test_bls_dry_run_prints_plan(
    validator, capsys: pytest.CaptureFixture,
) -> None:
    rc = validator.main(["--provider", "bls"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "DRY RUN (bls)" in out
    assert "CUUR0000SA0" in out
    assert "CES0000000001" in out
