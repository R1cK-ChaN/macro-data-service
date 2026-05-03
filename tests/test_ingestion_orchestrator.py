from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion import IngestionOrchestrator, IngestionSourceDefinition
from ingestion.sources import SOURCE_FAMILIES


class IngestionOrchestratorTest(unittest.TestCase):
    def test_run_source_executes_pipeline_stages_in_order(self) -> None:
        orchestrator = IngestionOrchestrator(
            store=Mock(),
            fred=Mock(),
            investing=Mock(),
            forexfactory=Mock(),
            tradingeconomics=Mock(),
            fed=Mock(),

            news=Mock(),
            reddit_trends=Mock(),
            weibo_trends=Mock(),
            rate_probability=Mock(),
            nyfed=Mock(),
            gov_report=Mock(),
            eia=Mock(),
            treasury_fiscal=Mock(),
            imf=Mock(),
            eurostat=Mock(),
            bis=Mock(),
            ecb=Mock(),
            oecd=Mock(),
            worldbank=Mock(),
        )

        calls: list[object] = []

        def fetch() -> list[str]:
            calls.append("fetch")
            return ["a", "a", "b", ""]

        def normalize(items: list[str]) -> list[str]:
            calls.append(("normalize", list(items)))
            return [item.upper() for item in items]

        def validate(items: list[str]) -> list[str]:
            calls.append(("validate", list(items)))
            return [item for item in items if item]

        def deduplicate(items: list[str]) -> list[str]:
            calls.append(("deduplicate", list(items)))
            return list(dict.fromkeys(items))

        def store(items: list[str]) -> int:
            calls.append(("store", list(items)))
            return len(items)

        orchestrator.register_source(
            IngestionSourceDefinition(
                name="test_source",
                interval_seconds=30,
                fetch=fetch,
                normalize=normalize,
                validate=validate,
                deduplicate=deduplicate,
                store=store,
            )
        )

        report = orchestrator.run_source("test_source")

        self.assertEqual(
            calls,
            [
                "fetch",
                ("normalize", ["a", "a", "b", ""]),
                ("validate", ["A", "A", "B", ""]),
                ("deduplicate", ["A", "A", "B"]),
                ("store", ["A", "B"]),
            ],
        )
        self.assertEqual(report.source, "test_source")
        self.assertEqual(report.fetched, 4)
        self.assertEqual(report.normalized, 4)
        self.assertEqual(report.validated, 3)
        self.assertEqual(report.deduplicated, 2)
        self.assertEqual(report.stored, 2)
        self.assertTrue(report.to_dict()["ok"])

    def test_reddit_trends_source_runs_pipeline(self) -> None:
        fake_reddit_trends = Mock()
        fake_reddit_trends.fetch_entries.return_value = ["raw-a", "raw-b", "raw-c"]
        fake_reddit_trends.normalize_entries.return_value = ["norm-a", "norm-b"]
        fake_reddit_trends.validate_entries.return_value = ["valid-a"]
        fake_reddit_trends.deduplicate_entries.return_value = ["dedup-a"]
        fake_reddit_trends.store_topics.return_value = 1

        orchestrator = IngestionOrchestrator(
            store=Mock(),
            fred=Mock(),
            investing=Mock(),
            forexfactory=Mock(),
            tradingeconomics=Mock(),
            fed=Mock(),

            news=Mock(),
            reddit_trends=fake_reddit_trends,
            weibo_trends=Mock(),
            rate_probability=Mock(),
            nyfed=Mock(),
            gov_report=Mock(),
            eia=Mock(),
            treasury_fiscal=Mock(),
            imf=Mock(),
            eurostat=Mock(),
            bis=Mock(),
            ecb=Mock(),
            oecd=Mock(),
            worldbank=Mock(),
        )

        report = orchestrator.run_source("reddit_trends")

        fake_reddit_trends.fetch_entries.assert_called_once_with()
        fake_reddit_trends.normalize_entries.assert_called_once_with(["raw-a", "raw-b", "raw-c"])
        fake_reddit_trends.validate_entries.assert_called_once_with(["norm-a", "norm-b"])
        fake_reddit_trends.deduplicate_entries.assert_called_once_with(["valid-a"])
        fake_reddit_trends.store_topics.assert_called_once_with(orchestrator.store, ["dedup-a"])
        self.assertEqual(report.source, "reddit_trends")
        self.assertEqual(report.fetched, 3)
        self.assertEqual(report.normalized, 2)
        self.assertEqual(report.validated, 1)
        self.assertEqual(report.deduplicated, 1)
        self.assertEqual(report.stored, 1)

    def test_weibo_trends_source_runs_pipeline(self) -> None:
        fake_weibo_trends = Mock()
        fake_weibo_trends.fetch_entries.return_value = ["raw-a", "raw-b"]
        fake_weibo_trends.normalize_entries.return_value = ["norm-a", "norm-b"]
        fake_weibo_trends.validate_entries.return_value = ["valid-a", "valid-b"]
        fake_weibo_trends.deduplicate_entries.return_value = ["dedup-a"]
        fake_weibo_trends.store_topics.return_value = 1

        orchestrator = IngestionOrchestrator(
            store=Mock(),
            fred=Mock(),
            investing=Mock(),
            forexfactory=Mock(),
            tradingeconomics=Mock(),
            fed=Mock(),

            news=Mock(),
            reddit_trends=Mock(),
            weibo_trends=fake_weibo_trends,
            rate_probability=Mock(),
            nyfed=Mock(),
            gov_report=Mock(),
            eia=Mock(),
            treasury_fiscal=Mock(),
            imf=Mock(),
            eurostat=Mock(),
            bis=Mock(),
            ecb=Mock(),
            oecd=Mock(),
            worldbank=Mock(),
        )

        report = orchestrator.run_source("weibo_trends")

        fake_weibo_trends.fetch_entries.assert_called_once_with()
        fake_weibo_trends.normalize_entries.assert_called_once_with(["raw-a", "raw-b"])
        fake_weibo_trends.validate_entries.assert_called_once_with(["norm-a", "norm-b"])
        fake_weibo_trends.deduplicate_entries.assert_called_once_with(["valid-a", "valid-b"])
        fake_weibo_trends.store_topics.assert_called_once_with(orchestrator.store, ["dedup-a"])
        self.assertEqual(report.source, "weibo_trends")
        self.assertEqual(report.fetched, 2)
        self.assertEqual(report.normalized, 2)
        self.assertEqual(report.validated, 2)
        self.assertEqual(report.deduplicated, 1)
        self.assertEqual(report.stored, 1)


class SourceFamilyTaggingTest(unittest.TestCase):
    """Issue #5 Slice 1 — every default source carries a family tag, and the
    tag flows through list_sources() and IngestionRunReport.to_dict()."""

    def _build_orchestrator(self) -> IngestionOrchestrator:
        return IngestionOrchestrator(
            store=Mock(),
            fred=Mock(), investing=Mock(), forexfactory=Mock(),
            tradingeconomics=Mock(), fed=Mock(),
            news=Mock(), reddit_trends=Mock(), weibo_trends=Mock(),
            rate_probability=Mock(), nyfed=Mock(), gov_report=Mock(),
            eia=Mock(), treasury_fiscal=Mock(), imf=Mock(),
            eurostat=Mock(), bis=Mock(), ecb=Mock(), oecd=Mock(),
            worldbank=Mock(),
        )

    def test_every_registered_default_source_has_non_empty_family(self) -> None:
        orch = self._build_orchestrator()
        offenders = [s for s in orch.list_sources() if not s["family"]]
        self.assertEqual(offenders, [], f"sources without family: {offenders}")

    def test_list_sources_returns_name_family_dicts(self) -> None:
        orch = self._build_orchestrator()
        rows = orch.list_sources()
        self.assertTrue(all(set(r.keys()) == {"name", "family"} for r in rows))
        by_name = {r["name"]: r["family"] for r in rows}
        # Spot-check one entry per family bucket. Issue #118 P4
        # retired the SQLite market lane and unregistered the
        # ``tiingo_market`` / ``eodhd_market`` / ``macro_market`` /
        # ``identity_repair`` sources from the orchestrator default
        # roster — the follow-up backfill issue re-registers them
        # against ``ClickHouseMarketStore``.
        self.assertEqual(by_name["fred_daily"], "economic_data")
        self.assertEqual(by_name["gov_reports"], "release_report")
        self.assertEqual(by_name["fed"], "release_report")
        self.assertEqual(by_name["news"], "news")
        self.assertEqual(by_name["calendar"], "calendar")
        self.assertEqual(by_name["reddit_trends"], "trend")
        self.assertEqual(by_name["rate_probability"], "signal")

    def test_calendar_source_is_retired_noop(self) -> None:
        orch = self._build_orchestrator()
        report = orch.run_source("calendar")
        self.assertEqual(report.source, "calendar")
        self.assertEqual(report.stored, 0)
        self.assertIsNone(report.fetched)
        self.assertNotIn("calendar", orch._default_refresh_order)
        orch.investing.fetch_range.assert_not_called()
        orch.forexfactory.fetch.assert_not_called()
        orch.tradingeconomics.fetch.assert_not_called()

    def test_registered_definition_picks_up_family_from_registry(self) -> None:
        orch = self._build_orchestrator()
        orch.register_source(
            IngestionSourceDefinition(name="fred_daily", fetch=lambda: [], store=lambda _: 0)
        )
        self.assertEqual(orch._sources["fred_daily"].family, "economic_data")

    def test_custom_source_without_family_stays_empty(self) -> None:
        orch = self._build_orchestrator()
        orch.register_source(
            IngestionSourceDefinition(name="adhoc", fetch=lambda: [], store=lambda _: 0)
        )
        self.assertEqual(orch._sources["adhoc"].family, "")

    def test_run_report_to_dict_includes_family(self) -> None:
        orch = self._build_orchestrator()
        orch.register_source(
            IngestionSourceDefinition(
                name="fred_daily",
                execute=lambda: 7,
            )
        )
        report = orch.run_source("fred_daily")
        payload = report.to_dict()
        self.assertEqual(payload["family"], "economic_data")
        self.assertEqual(payload["source"], "fred_daily")
        self.assertEqual(payload["stored"], 7)

    def test_source_families_registry_covers_all_defaults(self) -> None:
        orch = self._build_orchestrator()
        registered = {s["name"] for s in orch.list_sources()}
        missing = registered - set(SOURCE_FAMILIES)
        self.assertEqual(missing, set(), f"registry missing entries: {missing}")


if __name__ == "__main__":
    unittest.main()
