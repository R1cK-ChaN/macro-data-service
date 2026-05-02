"""Unit tests for fetcher adapters — mock underlying clients, verify RawSeries output."""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import MagicMock

import pytest

from ingestion.types import RawObservation, RawSeries


# ── Helpers ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _FakeObs:
    series_id: str = "TEST"
    date: str = "2024-01-01"
    value: float = 100.0


@dataclass(frozen=True)
class _FakeBLSObs:
    series_id: str = "TEST"
    date: str = "2024-01-01"
    value: float = 100.0
    period: str = "M01"


@dataclass(frozen=True)
class _FakeEIAObs:
    series_id: str = "TEST"
    date: str = "2024-01-01"
    value: float = 100.0
    unit: str = "usd"


@dataclass(frozen=True)
class _FakeNYFedRate:
    date: str = "2024-01-01"
    type: str = "SOFR"
    rate: float = 5.33
    percentile_1: float | None = None
    percentile_25: float | None = None
    percentile_75: float | None = None
    percentile_99: float | None = None
    volume_billions: float | None = None
    target_rate_from: float | None = None
    target_rate_to: float | None = None


@dataclass(frozen=True)
class _FakeNYFedGSCPI:
    date: str = "2024-01-31"
    value: float = 0.5


@dataclass(frozen=True)
class _FakeSDMXObs:
    series_id: str = "TEST"
    date: str = "2024-01-01"
    value: float = 100.0
    dataflow: str = "DF"
    dataset: str = ""


@dataclass(frozen=True)
class _FakeOECDObs:
    series_id: str = "TEST"
    date: str = "2024-01-01"
    value: float = 100.0
    dataflow: str = "DF"
    dataset: str = ""
    agency_id: str = "OECD"
    series_key: str = "USA.M"
    raw_series_key: str = "USA.M"
    dimensions: dict = field(default_factory=dict)


@dataclass(frozen=True)
class _FakeTreasuryObs:
    series_id: str = "TEST"
    date: str = "2024-01-01"
    value: float = 100.0
    metadata: dict = field(default_factory=dict)


@dataclass(frozen=True)
class _FakeWBObs:
    series_id: str = "WB_TEST"
    date: str = "2024-01-01"
    value: float = 100.0
    indicator: str = "NY.GDP"
    country_code: str = "USA"
    country_name: str = "United States"


# ── Tests ─────────────────────────────────────────────────────────────


class TestFredFetcher:
    def test_fetch_returns_raw_series(self):
        from ingestion.fetchers._fred import FredFetcher

        mock_client = MagicMock()
        mock_client.get_series_with_raw.return_value = (
            [
                _FakeObs(series_id="CPIAUCSL", date="2024-01-01", value=312.3),
                _FakeObs(series_id="CPIAUCSL", date="2024-02-01", value=313.1),
            ],
            {"observations": [
                {"date": "2024-01-01", "value": "312.3"},
                {"date": "2024-02-01", "value": "313.1"},
            ]},
            {"series_id": "CPIAUCSL"},
        )
        fetcher = FredFetcher(
            client=mock_client,
            series_config={"CPIAUCSL": {"name": "CPI", "category": "inflation", "freq": "monthly"}},
        )
        results = fetcher.fetch(lookback_days=30)
        assert len(results) == 1
        rs = results[0]
        assert isinstance(rs, RawSeries)
        assert rs.source == "fred"
        assert rs.series_id == "CPIAUCSL"
        assert len(rs.observations) == 2
        assert rs.observations[0].date == "2024-01-01"
        assert rs.observations[0].value == 312.3
        # Issue #69: raw payload + content_hash threaded through
        assert rs.raw_payload is not None
        assert rs.content_hash is not None

    def test_fetch_series_returns_single(self):
        from ingestion.fetchers._fred import FredFetcher

        mock_client = MagicMock()
        mock_client.get_series_with_raw.return_value = (
            [_FakeObs(series_id="UNRATE", value=3.7)],
            {"observations": [{"date": "2024-01-01", "value": "3.7"}]},
            {"series_id": "UNRATE"},
        )
        fetcher = FredFetcher(
            client=mock_client,
            series_config={"UNRATE": {"name": "Unemployment", "category": "employment", "freq": "monthly"}},
        )
        rs = fetcher.fetch_series("UNRATE", lookback_days=30)
        assert rs is not None
        assert rs.series_id == "UNRATE"

    def test_fetch_series_unknown_returns_none(self):
        from ingestion.fetchers._fred import FredFetcher

        fetcher = FredFetcher(client=MagicMock(), series_config={})
        assert fetcher.fetch_series("UNKNOWN") is None


class TestBLSFetcher:
    def test_fetch_preserves_period(self):
        from ingestion.fetchers._bls import BLSFetcher

        mock_client = MagicMock()
        mock_client.get_series_single_with_raw.return_value = (
            [_FakeBLSObs(series_id="CUUR0000SA0", date="2024-01-01", value=310.5, period="M01")],
            {"Results": {"series": [{"seriesID": "CUUR0000SA0", "data": [
                {"year": "2024", "period": "M01", "value": "310.5"},
            ]}]}},
            {"seriesid": ["CUUR0000SA0"]},
        )
        fetcher = BLSFetcher(
            client=mock_client,
            series_config={"cpi": {"series_id": "CUUR0000SA0", "name": "CPI", "category": "inflation", "survey": "CU", "freq": "monthly"}},
        )
        results = fetcher.fetch(lookback_days=365)
        assert len(results) == 1
        assert results[0].observations[0].provider_metadata["period"] == "M01"
        assert results[0].content_hash is not None


class TestEIAFetcher:
    def test_fetch_preserves_unit(self):
        from ingestion.fetchers._eia import EIAFetcher

        mock_client = MagicMock()
        mock_client.get_series.return_value = [
            _FakeEIAObs(series_id="EIA_BRENT", value=82.5, unit="usd_per_barrel"),
        ]
        fetcher = EIAFetcher(
            client=mock_client,
            series_config={"brent": {"route": "petroleum/pri/spt/data", "params": {}, "series_id": "EIA_BRENT", "category": "energy"}},
        )
        results = fetcher.fetch()
        assert len(results) == 1
        assert results[0].observations[0].provider_metadata["unit"] == "usd_per_barrel"


class TestTreasuryFetcher:
    def test_fetch_all_datasets(self):
        from ingestion.fetchers._treasury import TreasuryFetcher

        mock_client = MagicMock()
        mock_client.fetch_debt_outstanding.return_value = [_FakeTreasuryObs(value=34_000_000)]
        mock_client.fetch_tga_balance.return_value = [_FakeTreasuryObs(value=500_000)]
        mock_client.fetch_avg_interest_rates.return_value = [_FakeTreasuryObs(value=3.2)]
        fetcher = TreasuryFetcher(client=mock_client)
        results = fetcher.fetch()
        assert len(results) == 3


class TestNYFedFetcher:
    def test_fetch_all_rates(self):
        from ingestion.fetchers._nyfed import NYFedFetcher

        mock_client = MagicMock()
        mock_client.fetch_sofr.return_value = [_FakeNYFedRate(type="SOFR", rate=5.33)]
        mock_client.fetch_effr.return_value = [_FakeNYFedRate(type="EFFR", rate=5.33)]
        mock_client.fetch_obfr.return_value = [_FakeNYFedRate(type="OBFR", rate=5.32)]
        mock_client.fetch_gscpi.return_value = [_FakeNYFedGSCPI()]
        fetcher = NYFedFetcher(client=mock_client)
        results = fetcher.fetch()
        assert len(results) == 4
        assert all(r.source == "nyfed" for r in results)
        assert {r.series_id for r in results} == {
            "NYFED_SOFR", "NYFED_EFFR", "NYFED_OBFR", "NYFED_GSCPI",
        }


class TestSDMXFetcher:
    def test_fetch_converts_observations(self):
        from ingestion.fetchers._sdmx import SDMXFetcher

        mock_client = MagicMock()
        mock_client.get_data_with_raw.return_value = (
            [_FakeSDMXObs(series_id="IMF_CN_CPI", date="2024-01-01", value=102.5, dataflow="CPI")],
            {"data": {"dataSets": [{"series": {"0:0:0": {"observations": {"0": [102.5]}}}}]}},
            {"format": "jsondata", "dataflow_id": "CPI", "key": "CHN"},
        )
        fetcher = SDMXFetcher(
            client=mock_client,
            source_name="imf",
            series_config={"cn_cpi": {"dataflow": "CPI", "version": "5.0", "key": "CHN", "series_id": "IMF_CN_CPI", "category": "inflation"}},
        )
        results = fetcher.fetch()
        assert len(results) == 1
        assert results[0].source == "imf"
        assert results[0].observations[0].provider_metadata["dataflow"] == "CPI"
        assert results[0].content_hash is not None


class TestOECDFetcher:
    def test_fetch_with_filters(self):
        from ingestion.fetchers._oecd import OECDFetcher
        from ingestion.series_config import OECDSeriesConfig

        mock_client = MagicMock()
        mock_client.fetch_data.return_value = [
            _FakeOECDObs(series_id="OECD_CLI_US", value=100.5),
        ]
        cfg = OECDSeriesConfig(
            dataflow="DSD_STES@DF_CLI", series_id="OECD_CLI_US", category="leading",
            filters={"REF_AREA": "USA"},
        )
        fetcher = OECDFetcher(
            client=mock_client,
            series_config={"cli_us": cfg},
        )
        results = fetcher.fetch()
        assert len(results) == 1
        assert results[0].observations[0].provider_metadata["agency_id"] == "OECD"


class TestWorldBankFetcher:
    def test_fetch_preserves_country(self):
        from ingestion.fetchers._worldbank import WorldBankFetcher
        from ingestion.series_config import WorldBankSeriesConfig

        mock_client = MagicMock()
        mock_client.get_indicator.return_value = [
            _FakeWBObs(series_id="WB_GDP_PCAP_US", value=65000, indicator="NY.GDP.PCAP.PP.CD", country_code="USA"),
        ]
        cfg = WorldBankSeriesConfig(indicator="NY.GDP.PCAP.PP.CD", country="USA", series_id="WB_GDP_PCAP_US", category="development")
        fetcher = WorldBankFetcher(
            client=mock_client,
            series_config={"gdp_pcap_us": cfg},
        )
        results = fetcher.fetch()
        assert len(results) == 1
        assert results[0].observations[0].provider_metadata["country_code"] == "USA"
