from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from analyst.macro_data.service import LocalMacroDataService
from analyst.storage import MarketPriceRecord, SQLiteEngineStore


class MacroOntologyServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.store = SQLiteEngineStore(db_path=Path(self.temp_dir.name) / "engine.db")
        self.service = LocalMacroDataService(store=self.store)

    def test_get_indicator_ontology_returns_structural_links_only(self) -> None:
        payload = self.service.invoke("get_indicator_ontology", {"indicator_id": "us.inflation.cpi_yoy"})

        indicator = payload["indicator"]
        self.assertEqual(indicator["indicator_id"], "us.inflation.cpi_yoy")
        self.assertEqual(indicator["obs_family_id"], "us.inflation.cpi_all")
        self.assertEqual(indicator["produced_by_institution_ids"], ["us.bls"])
        self.assertIn("us.bls.cpi", indicator["release_family_ids"])
        self.assertEqual(payload["topic"]["code"], "inflation")

        aliases = {(item["alias"], item["source"]) for item in payload["aliases"]}
        self.assertIn(("CPI y/y", "investing"), aliases)

        time_series = payload["time_series"]
        self.assertIsNotNone(time_series)
        self.assertEqual(time_series["family_id"], "us.inflation.cpi_all")
        self.assertEqual(time_series["source_id"], "fred")
        self.assertEqual(time_series["source_type"], "data_aggregator")

        release_families = {item["release_family_id"]: item for item in payload["release_families"]}
        self.assertIn("us.bls.cpi", release_families)
        self.assertEqual(release_families["us.bls.cpi"]["produced_by_institution_id"], "us.bls")

        institutions = {item["institution_id"]: item for item in payload["institutions"]}
        self.assertEqual(institutions["fred"]["roles"], ["series_provider"])
        self.assertEqual(institutions["us.bls"]["roles"], ["release_producer"])

        for forbidden_key in ("price", "symbol", "asset_class", "change_pct", "market_prices"):
            self.assertFalse(self._contains_key(payload, forbidden_key))

    def test_list_indicators_by_topic_returns_normalized_indicator_records(self) -> None:
        payload = self.service.invoke(
            "list_indicators_by_topic",
            {"topic": "inflation", "country_code": "US"},
        )

        self.assertEqual(payload["topic"], "inflation")
        self.assertEqual(payload["country_code"], "US")
        self.assertGreater(payload["total"], 0)

        indicators = {item["indicator_id"]: item for item in payload["indicators"]}
        self.assertIn("us.inflation.cpi_yoy", indicators)
        self.assertTrue(indicators["us.inflation.cpi_yoy"]["has_time_series"])
        self.assertIn("us.bls.cpi", indicators["us.inflation.cpi_yoy"]["release_family_ids"])

    def test_market_snapshot_remains_separate_from_release_families(self) -> None:
        self.store.insert_market_price(
            MarketPriceRecord(
                symbol="SPX",
                asset_class="index",
                price=5100.25,
                change_pct=0.8,
                timestamp=1_741_736_800,
                name="S&P 500",
            )
        )

        market_payload = self.service.invoke("get_market_snapshot")
        self.assertEqual(market_payload["prices"][0]["symbol"], "SPX")

        ontology_payload = self.service.invoke(
            "list_release_families_for_indicator",
            {"indicator_id": "us.inflation.cpi_yoy"},
        )
        self.assertEqual(ontology_payload["indicator"]["produced_by_institution_ids"], ["us.bls"])
        self.assertEqual(ontology_payload["release_families"][0]["release_family_id"], "us.bls.cpi")
        self.assertFalse(self._contains_key(ontology_payload, "price"))

    def _contains_key(self, value: object, key: str) -> bool:
        if isinstance(value, dict):
            if key in value:
                return True
            return any(self._contains_key(item, key) for item in value.values())
        if isinstance(value, list):
            return any(self._contains_key(item, key) for item in value)
        return False


if __name__ == "__main__":
    unittest.main()
