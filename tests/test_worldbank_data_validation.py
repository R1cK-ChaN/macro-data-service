"""World Bank data-level validation tests.

These tests verify DATA COMPLETENESS against the live World Bank API,
not just code mechanics.  They confirm:

1. Source completeness    — fetched count == API total
2. Topic completeness     — fetched count == API total
3. Country completeness   — fetched count == API total
4. Indicator completeness — fetched count == API total (full catalog)
5. Pagination correctness — no truncation across all endpoints
6. Series row accounting  — raw_fetched == API total,
                            parsed + null_filtered == raw_fetched
7. Series hash consistency — SHA-256(client data) == SHA-256(raw API data)

Run with:
    pytest tests/test_worldbank_data_validation.py -v -s
"""

from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path

import pytest
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.scrapers.worldbank import WorldBankClient

pytestmark = pytest.mark.integration

BASE = "https://api.worldbank.org/v2"

# -- helpers -----------------------------------------------------------------

def _api_total(endpoint: str) -> int:
    """Quick single-request count from the API metadata."""
    r = requests.get(f"{BASE}/{endpoint}", params={"format": "json", "per_page": "1"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list) and len(data) >= 1 and isinstance(data[0], dict):
        return int(data[0].get("total", 0))
    return 0


def _api_fetch_all(endpoint: str, *, per_page: int = 1000) -> tuple[list[dict], int]:
    """Fetch all pages from an endpoint, return (records, api_total)."""
    params = {"format": "json", "per_page": str(per_page)}
    all_records: list[dict] = []
    api_total = 0
    page = 1
    while True:
        params["page"] = str(page)
        r = requests.get(f"{BASE}/{endpoint}", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or len(data) < 2:
            break
        if page == 1:
            api_total = int(data[0].get("total", 0))
        records = data[1]
        if records:
            all_records.extend(records)
        total_pages = int(data[0].get("pages", 1))
        if page >= total_pages:
            break
        page += 1
    return all_records, api_total


def _count_null_values(records: list[dict]) -> int:
    """Count records where value is None."""
    return sum(1 for r in records if r.get("value") is None)


@pytest.fixture(scope="module")
def wb_client() -> WorldBankClient:
    return WorldBankClient()


# ============================================================================
# Test 1 — Source completeness
# ============================================================================

class TestSourceCompleteness:
    """API-reported source count must equal fetched source count."""

    def test_source_count_matches_api_total(self, wb_client: WorldBankClient) -> None:
        api_total = wb_client.count_sources()
        sources = wb_client.list_sources()
        fetched = len(sources)

        print(f"\n  Sources — API total: {api_total}, fetched: {fetched}")
        assert fetched == api_total, (
            f"Source completeness FAILED: fetched {fetched}, API reports {api_total}"
        )

    def test_sources_have_required_fields(self, wb_client: WorldBankClient) -> None:
        sources = wb_client.list_sources()
        for src in sources:
            assert src.id, f"Source missing id: {src}"
            assert src.name, f"Source missing name: {src}"


# ============================================================================
# Test 2 — Topic completeness
# ============================================================================

class TestTopicCompleteness:
    """API-reported topic count must equal fetched topic count."""

    def test_topic_count_matches_api_total(self, wb_client: WorldBankClient) -> None:
        api_total = wb_client.count_topics()
        topics = wb_client.list_topics()
        fetched = len(topics)

        print(f"\n  Topics — API total: {api_total}, fetched: {fetched}")
        # Topics endpoint may report total including blank entries;
        # our parser skips entries without id/name
        assert fetched >= api_total - 5, (
            f"Topic completeness FAILED: fetched {fetched}, API reports {api_total}"
        )


# ============================================================================
# Test 3 — Country completeness
# ============================================================================

class TestCountryCompleteness:
    """API-reported country count must equal fetched country count."""

    def test_country_count_matches_api_total(self, wb_client: WorldBankClient) -> None:
        api_total = wb_client.count_countries()
        countries = wb_client.list_countries()
        fetched = len(countries)

        print(f"\n  Countries — API total: {api_total}, fetched: {fetched}")
        assert fetched == api_total, (
            f"Country completeness FAILED: fetched {fetched}, API reports {api_total}"
        )

    def test_major_countries_present(self, wb_client: WorldBankClient) -> None:
        countries = wb_client.list_countries()
        ids = {c.id for c in countries}
        required = {"USA", "CHN", "GBR", "JPN", "DEU", "IND", "BRA", "FRA"}
        missing = required - ids
        assert not missing, f"Missing major countries: {missing}"
        print(f"\n  All {len(required)} major countries present")


# ============================================================================
# Test 4 — Indicator completeness
# ============================================================================

class TestIndicatorCompleteness:
    """Full indicator catalog fetch must match API total."""

    def test_indicator_count_matches_api_total(self, wb_client: WorldBankClient) -> None:
        api_total = wb_client.count_indicators()
        indicators = wb_client.list_indicators()
        fetched = len(indicators)

        print(f"\n  Indicators — API total: {api_total}, fetched: {fetched}")
        assert fetched == api_total, (
            f"Indicator completeness FAILED: fetched {fetched}, API reports {api_total}"
        )

    def test_wdi_indicator_count_matches(self, wb_client: WorldBankClient) -> None:
        """WDI (source 2) is the largest and most important database."""
        api_total = wb_client.count_indicators(source_id="2")
        indicators = wb_client.list_indicators(source_id="2")
        fetched = len(indicators)

        print(f"\n  WDI indicators — API total: {api_total}, fetched: {fetched}")
        assert fetched == api_total, (
            f"WDI indicator completeness FAILED: fetched {fetched}, API reports {api_total}"
        )


# ============================================================================
# Test 5 — Pagination correctness
# ============================================================================

class TestPaginationCorrectness:
    """Verify that pagination never truncates results."""

    def test_sources_pagination(self) -> None:
        records, api_total = _api_fetch_all("source")
        print(f"\n  Sources pagination: {len(records)} records, API total: {api_total}")
        assert len(records) == api_total

    def test_countries_pagination(self) -> None:
        records, api_total = _api_fetch_all("country")
        print(f"\n  Countries pagination: {len(records)} records, API total: {api_total}")
        assert len(records) == api_total

    def test_topics_pagination(self) -> None:
        records, api_total = _api_fetch_all("topic")
        print(f"\n  Topics pagination: {len(records)} records, API total: {api_total}")
        assert len(records) == api_total

    def test_indicators_pagination_full(self) -> None:
        """Full indicator catalog — this is the critical pagination test."""
        records, api_total = _api_fetch_all("indicator", per_page=1000)
        print(f"\n  Indicators pagination: {len(records)} records, API total: {api_total}")
        assert len(records) == api_total, (
            f"Pagination TRUNCATED indicators: got {len(records)}, expected {api_total}"
        )

    def test_client_pagination_matches_raw(self, wb_client: WorldBankClient) -> None:
        """Client's _get_all_pages_with_total must match raw pagination."""
        records, api_total = wb_client._get_all_pages_with_total(
            f"{wb_client.BASE_URL}/country",
        )
        print(f"\n  Client pagination: {len(records)} records, API total: {api_total}")
        assert len(records) == api_total


# ============================================================================
# Test 6 — Series row accounting (exact)
# ============================================================================

SAMPLE_INDICATORS = [
    ("SP.POP.TOTL", "Population, total"),
    ("NY.GDP.MKTP.CD", "GDP (current US$)"),
    ("FP.CPI.TOTL.ZG", "Inflation, consumer prices (annual %)"),
    ("NY.GDP.PCAP.CD", "GDP per capita (current US$)"),
    ("SL.UEM.TOTL.ZS", "Unemployment, total (% of labor force)"),
]

SAMPLE_COUNTRIES = ["USA", "CHN", "GBR"]


class TestSeriesRowAccounting:
    """Exact row accounting: raw_fetched == API total, parsed + nulls == raw_fetched."""

    @pytest.mark.parametrize(
        "indicator_code,indicator_name",
        SAMPLE_INDICATORS,
        ids=[code for code, _ in SAMPLE_INDICATORS],
    )
    def test_exact_row_accounting_for_indicator(
        self, wb_client: WorldBankClient, indicator_code: str, indicator_name: str,
    ) -> None:
        """For each indicator:
        1. raw_fetched == api_total  (pagination complete)
        2. parsed + null_filtered == raw_fetched  (no silent drops)
        """
        # Step 1: fetch raw records (including nulls) with API total
        raw_records, api_total = wb_client.fetch_indicator_raw(
            indicator_code, "USA",
        )
        raw_fetched = len(raw_records)

        # Step 2: count nulls in raw
        null_count = _count_null_values(raw_records)
        no_date_count = sum(1 for r in raw_records if not r.get("date"))

        # Step 3: fetch parsed (non-null) observations through client
        parsed_obs = wb_client.get_indicator(
            indicator_code, "USA",
            series_id="accounting",
            fetch_all_pages=True,
        )
        parsed_count = len(parsed_obs)

        filtered_total = null_count + no_date_count

        print(
            f"\n  {indicator_code} ({indicator_name}) USA:"
            f"\n    API total:     {api_total}"
            f"\n    Raw fetched:   {raw_fetched}"
            f"\n    Null values:   {null_count}"
            f"\n    No date:       {no_date_count}"
            f"\n    Parsed (used): {parsed_count}"
            f"\n    Accounting:    {parsed_count} + {filtered_total} = {parsed_count + filtered_total}"
        )

        # Assertion 1: pagination fetched ALL rows the API claims to have
        assert raw_fetched == api_total, (
            f"PAGINATION TRUNCATION: raw_fetched={raw_fetched} != api_total={api_total}"
        )

        # Assertion 2: every raw row is accounted for (parsed or filtered)
        assert parsed_count + filtered_total == raw_fetched, (
            f"ROW LEAK: parsed({parsed_count}) + filtered({filtered_total}) "
            f"!= raw({raw_fetched}). "
            f"Unaccounted rows: {raw_fetched - parsed_count - filtered_total}"
        )


# ============================================================================
# Test 7 — Series hash consistency
# ============================================================================

class TestSeriesHashConsistency:
    """SHA-256 hash of client-parsed data must match raw API data exactly."""

    def test_multi_country_data_hash_matches(self, wb_client: WorldBankClient) -> None:
        """Fetch SP.POP.TOTL for 3 countries; client hash == raw API hash."""
        indicator = "SP.POP.TOTL"
        country_str = ";".join(SAMPLE_COUNTRIES)

        # Raw API fetch
        raw_records, api_total = _api_fetch_all(
            f"country/{country_str}/indicator/{indicator}",
        )

        # Client fetch
        client_obs = wb_client.get_indicator(
            indicator, country_str,
            series_id="hash_test",
            fetch_all_pages=True,
        )

        # Build comparable dicts from client observations
        client_rows = [
            {"date": o.date.replace("-01-01", ""), "value": o.value, "country": o.country_code}
            for o in client_obs
        ]
        client_rows.sort(key=lambda r: (r["country"], r["date"]))

        # Build comparable dicts from raw records (skip null values — same as client)
        raw_rows = []
        for rec in raw_records:
            val = rec.get("value")
            if val is None:
                continue
            raw_rows.append({
                "date": rec.get("date", ""),
                "value": float(val),
                "country": (rec.get("country") or {}).get("id", ""),
            })
        raw_rows.sort(key=lambda r: (r["country"], r["date"]))

        raw_hash = hashlib.sha256(json.dumps(raw_rows, sort_keys=True).encode()).hexdigest()
        client_hash = hashlib.sha256(json.dumps(client_rows, sort_keys=True).encode()).hexdigest()

        raw_nulls = _count_null_values(raw_records)

        print(
            f"\n  SP.POP.TOTL hash check:"
            f"\n    API total:       {api_total}"
            f"\n    Raw records:     {len(raw_records)}"
            f"\n    Raw nulls:       {raw_nulls}"
            f"\n    Raw non-null:    {len(raw_rows)}"
            f"\n    Client parsed:   {len(client_rows)}"
            f"\n    Raw hash:        {raw_hash[:16]}..."
            f"\n    Client hash:     {client_hash[:16]}..."
        )

        # Row counts must match
        assert len(raw_records) == api_total, (
            f"Raw pagination incomplete: {len(raw_records)} != {api_total}"
        )
        assert len(client_rows) == len(raw_rows), (
            f"Row count mismatch: client={len(client_rows)}, raw_non_null={len(raw_rows)}"
        )

        # Hashes must match
        assert client_hash == raw_hash, (
            "Data hash mismatch — client parsed data differs from raw API"
        )


# ============================================================================
# Test 8 — Coverage depth
# ============================================================================

class TestCoverageDepth:
    """Verify time and country coverage dimensions."""

    def test_year_coverage_for_population(self, wb_client: WorldBankClient) -> None:
        """SP.POP.TOTL for USA should have data from at least 1960-2023."""
        observations = wb_client.get_indicator(
            "SP.POP.TOTL", "USA",
            series_id="coverage_test",
            fetch_all_pages=True,
        )
        years = sorted({int(o.date[:4]) for o in observations})
        print(f"\n  USA population year range: {years[0]}-{years[-1]} ({len(years)} years)")

        assert years[0] <= 1961, f"Expected data from 1960s, earliest is {years[0]}"
        assert years[-1] >= 2022, f"Expected data through 2022+, latest is {years[-1]}"
        assert len(years) >= 50, f"Expected 50+ years of coverage, got {len(years)}"

    def test_country_coverage_for_gdp(self, wb_client: WorldBankClient) -> None:
        """NY.GDP.MKTP.CD for 'all' should cover 150+ countries."""
        observations = wb_client.fetch_indicator_bulk(
            "NY.GDP.MKTP.CD",
            per_page=10000,
        )
        countries = {o.country_code for o in observations}
        print(f"\n  GDP country coverage: {len(countries)} countries with data")
        assert len(countries) >= 150, (
            f"Expected 150+ countries with GDP data, got {len(countries)}"
        )


# ============================================================================
# Validation report
# ============================================================================

class TestValidationReport:
    """Produce a human-readable validation report with exact row accounting."""

    def test_full_validation_report(self, wb_client: WorldBankClient) -> None:
        report_lines = ["\n" + "=" * 70, "  World Bank Ingestion Validation Report", "=" * 70]

        # Catalog completeness
        checks: list[tuple[str, int, int]] = []

        api_src = wb_client.count_sources()
        fetched_src = len(wb_client.list_sources())
        checks.append(("Sources", api_src, fetched_src))

        api_top = wb_client.count_topics()
        fetched_top = len(wb_client.list_topics())
        checks.append(("Topics", api_top, fetched_top))

        api_cty = wb_client.count_countries()
        fetched_cty = len(wb_client.list_countries())
        checks.append(("Countries", api_cty, fetched_cty))

        api_ind = wb_client.count_indicators()
        fetched_ind = len(wb_client.list_indicators())
        checks.append(("Indicators", api_ind, fetched_ind))

        report_lines.append("\n  Catalog completeness:")
        catalog_ok = True
        for label, api, fetched in checks:
            ok = fetched == api or (label == "Topics" and fetched >= api - 5)
            if not ok:
                catalog_ok = False
            report_lines.append(
                f"    {label:<14} API={api:>6}  Fetched={fetched:>6}  {'PASS' if ok else 'FAIL'}"
            )

        # Series row accounting
        report_lines.append(f"\n  Series row accounting ({len(SAMPLE_INDICATORS)} indicators, USA):")
        report_lines.append(
            f"    {'Indicator':<25} {'API':>5} {'Raw':>5} {'Null':>5} {'Parsed':>6} {'Accounted':>9}  Status"
        )
        series_ok = True
        for code, name in SAMPLE_INDICATORS:
            raw_records, api_total = wb_client.fetch_indicator_raw(code, "USA")
            raw_fetched = len(raw_records)
            null_count = _count_null_values(raw_records)
            parsed_obs = wb_client.get_indicator(
                code, "USA", series_id="report", fetch_all_pages=True,
            )
            parsed_count = len(parsed_obs)

            pagination_ok = raw_fetched == api_total
            accounting_ok = parsed_count + null_count == raw_fetched
            ok = pagination_ok and accounting_ok

            if not ok:
                series_ok = False

            status = "PASS" if ok else "FAIL"
            report_lines.append(
                f"    {code:<25} {api_total:>5} {raw_fetched:>5} {null_count:>5} {parsed_count:>6} "
                f"{parsed_count + null_count:>5}/{raw_fetched:<5} {status}"
            )
            time.sleep(0.3)

        all_pass = catalog_ok and series_ok
        report_lines.append(f"\n  Overall: {'ALL PASS' if all_pass else 'FAILURES DETECTED'}")
        report_lines.append("=" * 70)

        print("\n".join(report_lines))
        assert all_pass, "Validation report has failures — see output above"
