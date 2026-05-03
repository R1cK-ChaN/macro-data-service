"""Bronze → silver round-trip replay tests for issue #116 P3.

For each source wired in P1, the test:

1. Constructs a synthetic raw payload representative of the upstream shape.
2. Runs the live fetcher path with a fake client returning that payload —
   captures ``RawSeries.raw_payload`` (= the bronze row that ``_capture_obs_raw``
   would write) plus ``RawSeries.observations`` (= the silver-projection input).
3. Re-parses ``raw_payload`` through the module-level ``_parse_*`` helper
   added in P2 (or the natural _with_raw refactor in P1 for EIA + Treasury).
4. Asserts the replay parser output matches the live-path silver projection
   per-(date, value).

This proves the architecture-doc promise — *"fix parser, replay raw, zero
quota cost"* — actually works for the 6 newly-wired sources.

The 9 sources already wired before #116 (BLS, IMF, ECB, Bundesbank, MOF JP,
AISI, ISM, Redbook, sentix) have round-trip coverage in their existing
``test_obs_raw_scaffold.py`` (FRED + BLS) and ``test_us_workbook_*sources.py``
(MOF JP, AISI, ISM, Redbook, sentix, Bundesbank) suites.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))


def _round_trip(payload: dict) -> dict:
    """Simulate obs_raw storage + replay: JSON serialize + deserialize."""
    return json.loads(json.dumps(payload, sort_keys=True, ensure_ascii=False))


# ── FRED vintages ─────────────────────────────────────────────────────────


def test_fred_vintages_bronze_replay_matches_silver():
    from ingestion.fetchers._vintages import FredVintageFetcher
    from ingestion.timeseries.scrapers.fred import (
        FredVintageObservation, _parse_fred_vintage_observations,
    )

    payload = {
        "observations": [
            {
                "date": "2024-01-01",
                "realtime_start": "2024-02-01",
                "realtime_end": "2024-03-31",
                "value": "100.0",
            },
            {
                "date": "2024-01-01",
                "realtime_start": "2024-04-01",
                "realtime_end": "9999-12-31",
                "value": "101.5",
            },
        ],
    }
    expected = _parse_fred_vintage_observations(payload, series_id="GDP")

    fake_client = MagicMock()
    fake_client.get_vintages_with_raw.return_value = (
        expected,
        payload,
        {"series_id": "GDP", "observation_start": "2023-01-01"},
    )
    fetcher = FredVintageFetcher(
        client=fake_client,
        series_ids=("GDP",),
        series_config={"GDP": {"name": "GDP"}},
        request_delay_seconds=0,
    )
    [rs] = fetcher.fetch()
    silver = [(o.date, o.vintage_date, o.value) for o in rs.vintages]

    replayed = _parse_fred_vintage_observations(
        _round_trip(rs.raw_payload), series_id="GDP",
    )
    replay = [(o.date, o.vintage_date, o.value) for o in replayed]

    assert silver == replay
    assert silver == [
        ("2024-01-01", "2024-02-01", 100.0),
        ("2024-01-01", "2024-04-01", 101.5),
    ]
    assert rs.source == "fred_vintages"
    assert rs.storage_source == "fred"
    assert rs.content_hash is not None
    assert isinstance(replayed[0], FredVintageObservation)


# ── EIA ───────────────────────────────────────────────────────────────────


def test_eia_bronze_replay_matches_silver():
    from ingestion.fetchers._eia import EIAFetcher
    from ingestion.timeseries.scrapers.eia import (
        EIAObservation, _parse_eia_observations,
    )

    payload = {
        "request": {"command": "/v2/petroleum/pri/spt/data"},
        "apiVersion": "2.1.5",
        "response": {
            "total": "2",
            "data": [
                {"period": "2024-01-15", "value": 82.5, "units": "usd_per_barrel"},
                {"period": "2024-01-16", "value": 83.0, "units": "usd_per_barrel"},
            ],
        },
    }
    expected = _parse_eia_observations(
        payload, series_id="EIA_BRENT", value_col="value",
    )

    fake_client = MagicMock()
    fake_client.get_series_with_raw.return_value = (
        expected, payload, {"route": "petroleum/pri/spt/data"},
    )
    fetcher = EIAFetcher(
        client=fake_client,
        series_config={
            "brent": {
                "route": "petroleum/pri/spt/data", "params": {},
                "series_id": "EIA_BRENT", "category": "energy",
            },
        },
    )
    [rs] = fetcher.fetch()
    silver = [(o.date, o.value) for o in rs.observations]

    replayed = _parse_eia_observations(
        _round_trip(rs.raw_payload), series_id="EIA_BRENT", value_col="value",
    )
    replay = [(o.date, o.value) for o in replayed]

    assert silver == replay == [("2024-01-15", 82.5), ("2024-01-16", 83.0)]
    assert isinstance(replayed[0], EIAObservation)


# ── Treasury Fiscal ───────────────────────────────────────────────────────


def test_treasury_fiscal_bronze_replay_matches_silver():
    from ingestion.fetchers._treasury import TreasuryFetcher
    from ingestion.timeseries.scrapers.treasury_fiscal import (
        TreasuryFiscalObservation, _parse_treasury_debt,
    )

    payload = {
        "data": [
            {"record_date": "2026-04-30", "tot_pub_debt_out_amt": "34123456789.00",
             "debt_held_public_amt": "27000000000.00", "intragov_hold_amt": "7123456789.00"},
            {"record_date": "2026-04-29", "tot_pub_debt_out_amt": "34122000000.00",
             "debt_held_public_amt": "27000000000.00", "intragov_hold_amt": "7122000000.00"},
        ],
        "meta": {"count": 2},
        "links": {"self": "..."},
    }
    expected = _parse_treasury_debt(payload)

    fake_client = MagicMock()
    fake_client.fetch_debt_outstanding_with_raw.return_value = (
        expected, payload, {"endpoint": "v2/accounting/od/debt_to_penny"},
    )
    # Other configs return empty so only debt_outstanding contributes.
    fake_client.fetch_tga_balance_with_raw.return_value = ([], {}, {})
    fake_client.fetch_avg_interest_rates_with_raw.return_value = ([], {}, {})
    fetcher = TreasuryFetcher(client=fake_client)
    results = fetcher.fetch()
    [debt_rs] = [rs for rs in results if rs.series_id == "TREAS_DEBT_TOTAL"]
    silver = [(o.date, o.value) for o in debt_rs.observations]

    replayed = _parse_treasury_debt(_round_trip(debt_rs.raw_payload))
    replay = [(o.date, o.value) for o in replayed]

    assert silver == replay
    assert silver == [
        ("2026-04-30", 34123456789.00),
        ("2026-04-29", 34122000000.00),
    ]
    assert isinstance(replayed[0], TreasuryFiscalObservation)


# ── NY Fed ────────────────────────────────────────────────────────────────


def test_nyfed_rates_bronze_replay_matches_silver():
    from ingestion.fetchers._nyfed import NYFedFetcher
    from ingestion.timeseries.scrapers.nyfed import (
        NYFedRate, _parse_nyfed_rates_payload,
    )

    payload = {
        "refRates": [
            {"effectiveDate": "2026-04-30", "percentRate": 5.33, "volumeInBillions": 2_500.5,
             "percentPercentile1": 5.20, "percentPercentile99": 5.45},
            {"effectiveDate": "2026-04-29", "percentRate": 5.32, "volumeInBillions": 2_490.0,
             "percentPercentile1": 5.19, "percentPercentile99": 5.44},
        ],
    }
    expected = _parse_nyfed_rates_payload(payload, rate_type="SOFR")

    fake_client = MagicMock()
    fake_client.fetch_sofr_with_raw.return_value = (
        expected, payload, {"url": "sofr", "rate_type": "SOFR"},
    )
    fake_client.fetch_effr_with_raw.return_value = ([], {}, {})
    fake_client.fetch_obfr_with_raw.return_value = ([], {}, {})
    fake_client.fetch_gscpi_with_raw.return_value = ([], {}, {})
    fetcher = NYFedFetcher(client=fake_client)
    results = fetcher.fetch()
    [sofr_rs] = [rs for rs in results if rs.series_id == "NYFED_SOFR"]
    silver = [(o.date, o.value) for o in sofr_rs.observations]

    # Stored payload includes the synthetic series_id tag the fetcher injects;
    # the rates parser ignores it (only reads refRates).
    replayed = _parse_nyfed_rates_payload(
        _round_trip(sofr_rs.raw_payload), rate_type="SOFR",
    )
    replay = [(o.date, o.rate) for o in replayed]

    assert silver == replay == [("2026-04-30", 5.33), ("2026-04-29", 5.32)]
    assert isinstance(replayed[0], NYFedRate)


def test_nyfed_gscpi_bronze_replay_subsumes_silver():
    """GSCPI bronze stores the full workbook; silver is a sliced tail.

    Unlike the other 5 sources where bronze == silver, GSCPI's fetcher
    downloads the full XLSX (the API has no row limit) and slices
    client-side via ``last_n``. Bronze persists the full history so a
    parser fix can recover months of data; silver only carries the
    requested tail. The replay assertion is therefore *superset*, not
    equality: replay returns the full workbook, silver is its tail.
    """
    from ingestion.fetchers._nyfed import NYFedFetcher
    from ingestion.timeseries.scrapers.nyfed import (
        NYFedGSCPI, _parse_nyfed_gscpi_payload,
    )

    workbook_rows = [
        NYFedGSCPI(date="2025-11-30", value=0.41),
        NYFedGSCPI(date="2025-12-31", value=0.47),
        NYFedGSCPI(date="2026-01-31", value=0.50),
        NYFedGSCPI(date="2026-02-28", value=0.5433),
        NYFedGSCPI(date="2026-03-31", value=0.6770),
    ]
    sliced_silver = workbook_rows[-2:]
    payload = {
        "series_id": "NYFED_GSCPI",
        "observations": [
            {"date": r.date, "value": r.value} for r in workbook_rows
        ],
    }

    fake_client = MagicMock()
    fake_client.fetch_sofr_with_raw.return_value = ([], {}, {})
    fake_client.fetch_effr_with_raw.return_value = ([], {}, {})
    fake_client.fetch_obfr_with_raw.return_value = ([], {}, {})
    fake_client.fetch_gscpi_with_raw.return_value = (
        sliced_silver, payload, {"url": "gscpi.xlsx", "last_n": "2"},
    )
    fetcher = NYFedFetcher(client=fake_client)
    [gscpi_rs] = [
        rs for rs in fetcher.fetch() if rs.series_id == "NYFED_GSCPI"
    ]
    silver = [(o.date, o.value) for o in gscpi_rs.observations]
    assert silver == [("2026-02-28", 0.5433), ("2026-03-31", 0.6770)]

    replayed = _parse_nyfed_gscpi_payload(_round_trip(gscpi_rs.raw_payload))
    replay = [(o.date, o.value) for o in replayed]

    # Replay reconstructs the full workbook, silver is its tail.
    assert len(replay) == 5
    assert silver == replay[-len(silver):]


# ── Eurostat (JSON-stat) ──────────────────────────────────────────────────


def test_eurostat_jsonstat_bronze_replay_matches_silver():
    from ingestion.fetchers._eurostat import EurostatFetcher
    from ingestion.timeseries.sdmx._types import SDMXObservation
    from ingestion.timeseries.sdmx.providers.eurostat import (
        _parse_eurostat_jsonstat,
    )

    payload = {
        "dimension": {
            "time": {"category": {"index": {
                "2024-Q1": 0, "2024-Q2": 1, "2024-Q3": 2,
            }}},
        },
        "value": {"0": 100.5, "1": 101.2, "2": 102.0},
        "updated": "2025-01-15",
    }

    expected = _parse_eurostat_jsonstat(
        payload, dataset_code="namq_10_gdp", series_id="ESTAT_GDP_EU",
        limit=30,
    )

    fake_client = MagicMock()
    fake_client.get_dataset_with_raw.return_value = (
        expected, payload, {"dataset": "namq_10_gdp"},
    )
    fetcher = EurostatFetcher(
        client=fake_client,
        series_config={
            "gdp": {
                "dataset": "namq_10_gdp", "params": {},
                "series_id": "ESTAT_GDP_EU", "category": "national_accounts",
            },
        },
    )
    [rs] = fetcher.fetch()
    silver = [(o.date, o.value) for o in rs.observations]

    # The fetcher tags raw_payload with series_id; the parser doesn't read
    # it — series_id is passed as a kwarg.
    replayed = _parse_eurostat_jsonstat(
        _round_trip(rs.raw_payload), dataset_code="namq_10_gdp",
        series_id="ESTAT_GDP_EU", limit=30,
    )
    replay = [(o.date, o.value) for o in replayed]

    assert silver == replay
    # Dashed-quarter normalization to YYYY-MM-DD; sorted desc.
    assert silver == [
        ("2024-07-01", 102.0),
        ("2024-04-01", 101.2),
        ("2024-01-01", 100.5),
    ]
    assert isinstance(replayed[0], SDMXObservation)


# ── OECD ──────────────────────────────────────────────────────────────────


def test_oecd_bronze_replay_matches_silver():
    from ingestion.fetchers._oecd import OECDFetcher
    from ingestion.series_config import OECDSeriesConfig
    from ingestion.timeseries.sdmx.providers.oecd import (
        OECDObservation, _parse_oecd_observations,
    )

    payload = {
        "data": {
            "dataSets": [{"series": {"0:0:0": {"observations": {
                "0": [100.5], "1": [101.2],
            }}}}],
            "structures": [{
                "dimensions": {
                    "series": [
                        {"id": "REF_AREA", "values": [{"id": "USA"}]},
                        {"id": "FREQ", "values": [{"id": "M"}]},
                        {"id": "MEASURE", "values": [{"id": "BCI"}]},
                    ],
                    "observation": [
                        {"id": "TIME_PERIOD", "values": [
                            {"id": "2024-01"}, {"id": "2024-02"},
                        ]},
                    ],
                },
            }],
        },
    }
    expected = _parse_oecd_observations(
        payload, series_id="OECD_BCI_US", dataflow="DSD_STES@DF_BCI",
        agency_id="OECD", limit=100,
    )

    fake_client = MagicMock()
    fake_client.fetch_data_with_raw.return_value = (
        expected, payload,
        {"dataflow_id": "DSD_STES@DF_BCI", "agency_id": "OECD", "key": "USA.M.BCI"},
    )
    cfg = OECDSeriesConfig(
        dataflow="DSD_STES@DF_BCI", series_id="OECD_BCI_US", category="leading",
        filters={"REF_AREA": "USA"},
    )
    fetcher = OECDFetcher(client=fake_client, series_config={"bci_us": cfg})
    [rs] = fetcher.fetch()
    silver = [(o.date, o.value) for o in rs.observations]

    replayed = _parse_oecd_observations(
        _round_trip(rs.raw_payload), series_id="OECD_BCI_US",
        dataflow="DSD_STES@DF_BCI", agency_id="OECD", limit=100,
    )
    replay = [(o.date, o.value) for o in replayed]

    assert silver == replay
    assert len(silver) == 2
    assert isinstance(replayed[0], OECDObservation)


# ── World Bank ────────────────────────────────────────────────────────────


def test_worldbank_bronze_replay_matches_silver():
    from ingestion.fetchers._worldbank import WorldBankFetcher
    from ingestion.series_config import WorldBankSeriesConfig
    from ingestion.timeseries.scrapers.worldbank import (
        WorldBankObservation, _parse_worldbank_indicator_payload,
    )

    payload = {
        "response": [
            {"page": 1, "pages": 1, "per_page": 50, "total": 2},
            [
                {"date": "2023", "value": 65000.0,
                 "country": {"id": "US", "value": "United States"},
                 "indicator": {"id": "NY.GDP.PCAP.PP.CD", "value": "GDP per capita"}},
                {"date": "2022", "value": 63000.0,
                 "country": {"id": "US", "value": "United States"},
                 "indicator": {"id": "NY.GDP.PCAP.PP.CD", "value": "GDP per capita"}},
            ],
        ],
    }
    expected = _parse_worldbank_indicator_payload(
        payload, series_id="WB_GDP_PCAP_US", indicator="NY.GDP.PCAP.PP.CD",
    )

    fake_client = MagicMock()
    fake_client.get_indicator_with_raw.return_value = (
        expected, payload,
        {"url": "worldbank/USA/NY.GDP.PCAP.PP.CD", "per_page": "50"},
    )
    cfg = WorldBankSeriesConfig(
        indicator="NY.GDP.PCAP.PP.CD", country="USA",
        series_id="WB_GDP_PCAP_US", category="development",
    )
    fetcher = WorldBankFetcher(
        client=fake_client, series_config={"gdp_pcap_us": cfg},
    )
    [rs] = fetcher.fetch()
    silver = [(o.date, o.value) for o in rs.observations]

    replayed = _parse_worldbank_indicator_payload(
        _round_trip(rs.raw_payload), series_id="WB_GDP_PCAP_US",
        indicator="NY.GDP.PCAP.PP.CD",
    )
    replay = [(o.date, o.value) for o in replayed]

    assert silver == replay
    assert silver == [("2023-01-01", 65000.0), ("2022-01-01", 63000.0)]
    assert isinstance(replayed[0], WorldBankObservation)
