from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from analyst.ingestion import IngestionOrchestrator, IngestionSourceDefinition


class IngestionOrchestratorTest(unittest.TestCase):
    def test_run_source_executes_pipeline_stages_in_order(self) -> None:
        orchestrator = IngestionOrchestrator(
            store=Mock(),
            fred=Mock(),
            investing=Mock(),
            forexfactory=Mock(),
            tradingeconomics=Mock(),
            fed=Mock(),
            market=Mock(),
            news=Mock(),
            reddit_trends=Mock(),
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
            market=Mock(),
            news=Mock(),
            reddit_trends=fake_reddit_trends,
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


if __name__ == "__main__":
    unittest.main()
