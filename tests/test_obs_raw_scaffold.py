"""Scaffold tests for the macro time-series ``obs_raw`` audit lane (issue #69 slice 1).

Mirrors the calendar ``cal_econ_raw`` test surface — content-hash
stability across query-time echo variation, revision detection, INSERT
OR IGNORE idempotency, and a re-projection check that re-running parser
+ Normalizer against a stored ``obs_raw`` payload yields the same
indicator rows the live ingest would have produced (no HTTP).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion.timeseries.canonicalize import (
    bls_content_hash,
    canonicalize_bls_payload,
    canonicalize_fred_payload,
    canonicalize_sdmx_payload,
    content_hash_for_source,
    fred_content_hash,
    sdmx_content_hash,
)
from ingestion.timeseries.scrapers.fred import FredObservation, _parse_fred_observations
from ingestion.types import RawObservation, RawSeries
from storage import ObsRawRecord, SQLiteEngineStore


# ── Canonicalization: stable across query-time echo variation ────────────


class TestFredCanonicalization:
    def test_drops_realtime_top_level(self) -> None:
        a = {"realtime_start": "2025-01-01", "realtime_end": "2025-01-01", "observations": []}
        b = {"realtime_start": "2025-06-15", "realtime_end": "9999-12-31", "observations": []}
        assert fred_content_hash(a) == fred_content_hash(b)

    def test_drops_query_envelope_echoes(self) -> None:
        """Sliding ``observation_start`` (lookback_days each refresh)
        must not change the hash. Codex review caught: if envelope
        keys leak into the hash, INSERT OR IGNORE never dedupes."""
        a = {
            "observation_start": "2024-01-01",
            "observation_end": "2025-01-01",
            "limit": 100, "offset": 0, "order_by": "observation_date",
            "sort_order": "desc", "units": "lin", "output_type": 1,
            "count": 1, "file_type": "json",
            "observations": [{"date": "2024-01-01", "value": "100.0"}],
        }
        b = {
            "observation_start": "2024-06-15",  # sliding window
            "observation_end": "2025-06-15",
            "limit": 50, "offset": 5, "order_by": "asc",
            "sort_order": "asc", "units": "chg", "output_type": 2,
            "count": 1, "file_type": "json",
            "observations": [{"date": "2024-01-01", "value": "100.0"}],
        }
        assert fred_content_hash(a) == fred_content_hash(b)

    def test_drops_realtime_per_observation(self) -> None:
        a = {"observations": [
            {"date": "2024-01", "value": "300.0", "realtime_start": "2024-01"},
        ]}
        b = {"observations": [
            {"date": "2024-01", "value": "300.0", "realtime_start": "2025-06-01"},
        ]}
        assert fred_content_hash(a) == fred_content_hash(b)

    def test_sorts_observations_by_date(self) -> None:
        a = {"observations": [
            {"date": "2024-01", "value": "300.0"},
            {"date": "2024-02", "value": "302.5"},
        ]}
        b = {"observations": [
            {"date": "2024-02", "value": "302.5"},
            {"date": "2024-01", "value": "300.0"},
        ]}
        assert fred_content_hash(a) == fred_content_hash(b)

    def test_value_revision_changes_hash(self) -> None:
        a = {"observations": [{"date": "2024-01", "value": "300.0"}]}
        b = {"observations": [{"date": "2024-01", "value": "301.5"}]}
        assert fred_content_hash(a) != fred_content_hash(b)

    def test_new_observation_changes_hash(self) -> None:
        a = {"observations": [{"date": "2024-01", "value": "300.0"}]}
        b = {"observations": [
            {"date": "2024-01", "value": "300.0"},
            {"date": "2024-02", "value": "301.0"},
        ]}
        assert fred_content_hash(a) != fred_content_hash(b)


class TestBlsCanonicalization:
    def _series(self, obs: list[dict]) -> dict:
        return {
            "status": "REQUEST_SUCCEEDED",
            "responseTime": 100,
            "message": [],
            "Results": {"series": [{"seriesID": "CUUR0000SA0", "data": obs}]},
        }

    def test_drops_envelope(self) -> None:
        a = self._series([{"year": "2024", "period": "M01", "value": "300.0"}])
        b = self._series([{"year": "2024", "period": "M01", "value": "300.0"}])
        b["responseTime"] = 999
        b["message"] = ["throttle warning"]
        assert bls_content_hash(a) == bls_content_hash(b)

    def test_drops_calculations(self) -> None:
        a = self._series([{"year": "2024", "period": "M01", "value": "300.0"}])
        b = self._series([{"year": "2024", "period": "M01", "value": "300.0"}])
        b["Results"]["series"][0]["calculations"] = {"net_changes": {"1": 0.5}}
        assert bls_content_hash(a) == bls_content_hash(b)

    def test_sorts_data_by_year_period(self) -> None:
        a = self._series([
            {"year": "2024", "period": "M02", "value": "302.5"},
            {"year": "2024", "period": "M01", "value": "300.0"},
        ])
        b = self._series([
            {"year": "2024", "period": "M01", "value": "300.0"},
            {"year": "2024", "period": "M02", "value": "302.5"},
        ])
        assert bls_content_hash(a) == bls_content_hash(b)

    def test_value_revision_changes_hash(self) -> None:
        a = self._series([{"year": "2024", "period": "M01", "value": "300.0"}])
        b = self._series([{"year": "2024", "period": "M01", "value": "301.5"}])
        assert bls_content_hash(a) != bls_content_hash(b)


class TestSdmxCanonicalization:
    def test_drops_header_envelope(self) -> None:
        a = {"header": {"prepared": "2025-01-01T10:00:00Z", "id": "abc"},
             "data": {"dataSets": [{"series": {}}]}}
        b = {"header": {"prepared": "2025-09-01T11:00:00Z", "id": "xyz"},
             "data": {"dataSets": [{"series": {}}]}}
        assert sdmx_content_hash(a) == sdmx_content_hash(b)

    def test_sorts_observations_by_index(self) -> None:
        a = {"data": {"dataSets": [{"series": {"0:0:0": {"observations": {
            "0": [100.0], "1": [101.0],
        }}}}]}}
        b = {"data": {"dataSets": [{"series": {"0:0:0": {"observations": {
            "1": [101.0], "0": [100.0],
        }}}}]}}
        assert sdmx_content_hash(a) == sdmx_content_hash(b)

    def test_value_revision_changes_hash(self) -> None:
        a = {"data": {"dataSets": [{"series": {"0:0:0": {"observations": {
            "0": [100.0],
        }}}}]}}
        b = {"data": {"dataSets": [{"series": {"0:0:0": {"observations": {
            "0": [105.0],
        }}}}]}}
        assert sdmx_content_hash(a) != sdmx_content_hash(b)


class TestSourceDispatch:
    def test_known_sources_dispatch(self) -> None:
        assert content_hash_for_source("fred", {"observations": []}) is not None
        assert content_hash_for_source("bls", {"Results": {"series": []}}) is not None
        assert content_hash_for_source("imf", {"data": {"dataSets": []}}) is not None
        assert content_hash_for_source("ecb", {"data": {"dataSets": []}}) is not None
        assert content_hash_for_source("eurostat", {"data": {"dataSets": []}}) is not None

    def test_unknown_source_returns_none(self) -> None:
        # BIS uses CSV — explicitly excluded so caller can branch and skip.
        assert content_hash_for_source("bis", {}) is None
        assert content_hash_for_source("brand-new-source", {}) is None


# ── Storage idempotency: INSERT OR IGNORE on (source, series_id, content_hash)


class TestObsRawIdempotency:
    @pytest.fixture
    def store(self, tmp_path: Path) -> SQLiteEngineStore:
        return SQLiteEngineStore(tmp_path / "engine.db")

    def _record(self, *, content_hash: str, snapshot_ms: int = 1700000000000) -> ObsRawRecord:
        return ObsRawRecord(
            source="fred",
            series_id="GDP",
            snapshot_epoch_ms=snapshot_ms,
            content_hash=content_hash,
            payload_json='{"observations":[]}',
            fetched_at="2025-01-01T00:00:00Z",
        )

    def test_first_insert_writes_one_row(self, store: SQLiteEngineStore) -> None:
        assert store.insert_obs_raw([self._record(content_hash="h1")]) == 1

    def test_duplicate_hash_inserts_zero(self, store: SQLiteEngineStore) -> None:
        rec = self._record(content_hash="h1")
        store.insert_obs_raw([rec])
        assert store.insert_obs_raw([rec]) == 0

    def test_revised_hash_inserts_one(self, store: SQLiteEngineStore) -> None:
        store.insert_obs_raw([self._record(content_hash="h1")])
        assert store.insert_obs_raw([
            self._record(content_hash="h2", snapshot_ms=1700000001000),
        ]) == 1

    def test_latest_returns_newest_snapshot(self, store: SQLiteEngineStore) -> None:
        store.insert_obs_raw([
            self._record(content_hash="h1", snapshot_ms=1700000000000),
            self._record(content_hash="h2", snapshot_ms=1700000005000),
        ])
        latest = store.latest_obs_raw_for_series("fred", "GDP")
        assert latest is not None
        assert latest.content_hash == "h2"


# ── Re-projection: parser replay against a stored payload


class TestReProjectionFromObsRaw:
    """Issue #69 acceptance: re-running the projection logic against
    ``obs_raw`` (no upstream call) yields byte-identical typed rows."""

    def test_fred_payload_round_trips_through_parser(self, tmp_path: Path) -> None:
        store = SQLiteEngineStore(tmp_path / "engine.db")
        payload = {
            "realtime_start": "2025-01-01",
            "realtime_end": "2025-01-01",
            "observations": [
                {"date": "2024-01-01", "value": "300.0"},
                {"date": "2024-02-01", "value": "302.5"},
                # FRED uses "." for missing — parser must drop these on replay too.
                {"date": "2024-03-01", "value": "."},
            ],
        }
        rec = ObsRawRecord(
            source="fred", series_id="CPIAUCSL",
            snapshot_epoch_ms=1700000000000,
            content_hash=fred_content_hash(payload),
            payload_json=json.dumps(payload, sort_keys=True),
            fetched_at="2025-01-01T00:00:00Z",
        )
        store.insert_obs_raw([rec])

        latest = store.latest_obs_raw_for_series("fred", "CPIAUCSL")
        assert latest is not None
        replayed = _parse_fred_observations(
            json.loads(latest.payload_json), series_id="CPIAUCSL",
        )
        assert replayed == [
            FredObservation(series_id="CPIAUCSL", date="2024-01-01", value=300.0),
            FredObservation(series_id="CPIAUCSL", date="2024-02-01", value=302.5),
        ]
