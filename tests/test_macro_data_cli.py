from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.sdmx.providers.oecd import OECDDataflow, OECDStructureSummary
from ingestion.sources import OECDSeriesConfig
from macro_data.cli import main
from storage.sqlite import default_engine_db_path


class MacroDataCLITest(unittest.TestCase):
    def test_oecd_dataflows_command_prints_matches(self) -> None:
        output = io.StringIO()
        fake_ingestion = Mock()
        fake_ingestion.list_catalog_dataflows.return_value = [
            OECDDataflow(
                id="DSD_STES@DF_CLI",
                agency_id="OECD.SDD.STES",
                version="4.1",
                name="Composite leading indicators",
            )
        ]
        with patch("macro_data.cli.OECDIngestionClient", return_value=fake_ingestion):
            with redirect_stdout(output):
                rc = main(["oecd-dataflows", "--limit", "1"])

        self.assertEqual(rc, 0)
        self.assertIn("OECD.SDD.STES", output.getvalue())
        self.assertIn("DSD_STES@DF_CLI", output.getvalue())

    def test_oecd_structure_command_prints_json_summary(self) -> None:
        output = io.StringIO()
        fake_ingestion = Mock()
        fake_ingestion.get_structure_summary.return_value = OECDStructureSummary(
            dataflow_id="DSD_STES@DF_CLI",
            agency_id="OECD.SDD.STES",
            version="4.1",
            name="Composite leading indicators",
            structure_id="DSD_STES",
            time_dimension_id="TIME_PERIOD",
            series_dimensions=("REF_AREA", "FREQ", "MEASURE"),
            code_counts={"REF_AREA": 2},
            defaults={"FREQ": "M"},
        )
        with patch("macro_data.cli.OECDIngestionClient", return_value=fake_ingestion):
            with redirect_stdout(output):
                rc = main(["oecd-structure", "--dataflow", "DSD_STES@DF_CLI"])

        self.assertEqual(rc, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["dataflow_id"], "DSD_STES@DF_CLI")
        self.assertEqual(payload["time_dimension_id"], "TIME_PERIOD")

    def test_oecd_generate_configs_command_prints_python_snippet(self) -> None:
        output = io.StringIO()
        fake_ingestion = Mock()
        fake_ingestion.generate_catalog_series_configs.return_value = {
            "auto_cli": OECDSeriesConfig(
                dataflow="DSD_STES@DF_CLI",
                series_id="OECD_AUTO_DSD_STES_DF_CLI_ABCDEF123456",
                category="catalog",
                agency_id="OECD.SDD.STES",
                version="4.1",
                filters={"REF_AREA": "USA", "FREQ": "M"},
            )
        }
        with patch("macro_data.cli.OECDIngestionClient", return_value=fake_ingestion):
            with redirect_stdout(output):
                rc = main(["oecd-generate-configs", "--dataflow-limit", "1", "--series-per-dataflow", "1"])

        self.assertEqual(rc, 0)
        self.assertIn("generated_oecd_series = {", output.getvalue())
        self.assertIn('"auto_cli": OECDSeriesConfig(', output.getvalue())

    def test_oecd_refresh_catalog_command_prints_counts(self) -> None:
        output = io.StringIO()
        fake_ingestion = Mock()
        fake_ingestion.refresh_catalog.return_value = Mock(source="oecd_catalog", count=12)
        fake_store = Mock()
        with patch("macro_data.cli.OECDIngestionClient", return_value=fake_ingestion):
            with patch("storage.SQLiteEngineStore", return_value=fake_store):
                with redirect_stdout(output):
                    rc = main(["oecd-refresh-catalog", "--dataflow-limit", "1", "--sleep-seconds", "0"])

        self.assertEqual(rc, 0)
        self.assertIn("oecd_catalog", output.getvalue())

    def test_refresh_source_command_prints_pipeline_report(self) -> None:
        output = io.StringIO()
        fake_service = Mock()
        fake_service.invoke.return_value = {
            "source": "news",
            "stored": 4,
            "fetched": 10,
            "normalized": 8,
            "validated": 6,
            "deduplicated": 4,
            "duration_ms": 123,
            "retries": 0,
            "error": "",
            "ok": True,
        }
        with patch("macro_data.factory.build_local_macro_data_service", return_value=fake_service):
            with redirect_stdout(output):
                rc = main(["refresh-source", "--source", "news"])

        self.assertEqual(rc, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["source"], "news")
        self.assertEqual(payload["stored"], 4)
        fake_service.invoke.assert_called_once_with("refresh_source", {"source": "news"})

    def test_refresh_source_command_supports_reddit_trends(self) -> None:
        output = io.StringIO()
        fake_service = Mock()
        fake_service.invoke.return_value = {
            "source": "reddit_trends",
            "stored": 3,
            "fetched": 12,
            "normalized": 12,
            "validated": 9,
            "deduplicated": 3,
            "duration_ms": 88,
            "retries": 0,
            "error": "",
            "ok": True,
        }
        with patch("macro_data.factory.build_local_macro_data_service", return_value=fake_service):
            with redirect_stdout(output):
                rc = main(["refresh-source", "--source", "reddit_trends"])

        self.assertEqual(rc, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["source"], "reddit_trends")
        self.assertEqual(payload["stored"], 3)
        fake_service.invoke.assert_called_once_with("refresh_source", {"source": "reddit_trends"})

    def test_refresh_source_command_supports_weibo_trends(self) -> None:
        output = io.StringIO()
        fake_service = Mock()
        fake_service.invoke.return_value = {
            "source": "weibo_trends",
            "stored": 5,
            "fetched": 20,
            "normalized": 20,
            "validated": 18,
            "deduplicated": 5,
            "duration_ms": 91,
            "retries": 0,
            "error": "",
            "ok": True,
        }
        with patch("macro_data.factory.build_local_macro_data_service", return_value=fake_service):
            with redirect_stdout(output):
                rc = main(["refresh-source", "--source", "weibo_trends"])

        self.assertEqual(rc, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["source"], "weibo_trends")
        self.assertEqual(payload["stored"], 5)
        fake_service.invoke.assert_called_once_with("refresh_source", {"source": "weibo_trends"})

    def test_sources_capabilities_command_invokes_service(self) -> None:
        output = io.StringIO()
        fake_service = Mock()
        fake_service.invoke.return_value = {
            "total": 1,
            "sources": [{"source_id": "oecd", "supports_discovery": True}],
        }
        with patch("macro_data.factory.build_local_macro_data_service", return_value=fake_service):
            with redirect_stdout(output):
                rc = main(["sources-capabilities"])

        self.assertEqual(rc, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["total"], 1)
        fake_service.invoke.assert_called_once_with("list_source_capabilities", {"include_internal": False})

    def test_source_health_command_invokes_service(self) -> None:
        output = io.StringIO()
        fake_service = Mock()
        fake_service.invoke.return_value = {
            "status": "healthy",
            "summary": {"healthy": 3, "degraded": 0, "other": 0, "visible_sources": 3},
            "sources": [],
        }
        with patch("macro_data.factory.build_local_macro_data_service", return_value=fake_service):
            with redirect_stdout(output):
                rc = main(["source-health"])

        self.assertEqual(rc, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["status"], "healthy")
        fake_service.invoke.assert_called_once_with(
            "get_source_health_dashboard",
            {"include_internal": False},
        )

    def test_catalog_list_command_invokes_service(self) -> None:
        output = io.StringIO()
        fake_service = Mock()
        fake_service.invoke.return_value = {
            "source_id": "oecd",
            "total": 1,
            "entities": [{"entity_id": "DF_CLI"}],
        }
        with patch("macro_data.factory.build_local_macro_data_service", return_value=fake_service):
            with redirect_stdout(output):
                rc = main(["catalog-list", "--source", "oecd", "--query", "cli", "--limit", "5", "--refresh"])

        self.assertEqual(rc, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["source_id"], "oecd")
        fake_service.invoke.assert_called_once_with(
            "list_catalog_entities",
            {"source_id": "oecd", "query": "cli", "limit": 5, "refresh": True},
        )

    def test_catalog_sync_latest_command_invokes_service(self) -> None:
        output = io.StringIO()
        fake_service = Mock()
        fake_service.invoke.return_value = {
            "source_id": "worldbank",
            "status": "success",
            "observations_synced": 12,
        }
        with patch("macro_data.factory.build_local_macro_data_service", return_value=fake_service):
            with redirect_stdout(output):
                rc = main(["catalog-sync-latest", "--source", "worldbank", "--entity", "SP.POP.TOTL", "--limit", "3"])

        self.assertEqual(rc, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["status"], "success")
        fake_service.invoke.assert_called_once_with(
            "sync_catalog_latest",
            {"source_id": "worldbank", "entity_ids": ["SP.POP.TOTL"], "limit": 3},
        )

    def test_default_engine_db_path_is_service_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = default_engine_db_path(Path(temp_dir))
        self.assertEqual(db_path.name, "engine.db")
        self.assertEqual(db_path.parent.name, ".macro-data")

    def test_validate_no_source_delegates_to_concept_validation(self) -> None:
        """Issue #102 P2: bare ``validate`` must run concept reports rather
        than the legacy zero-check ``validate_full('all')`` path."""

        from ingestion.validation._types import (
            CheckResult,
            ValidationLayer,
            ValidationReport,
            ValidationSeverity,
        )

        failing_check = CheckResult(
            check_name="concept_source_coverage",
            layer=ValidationLayer.CONCEPT,
            passed=False,
            severity=ValidationSeverity.ERROR,
            message="FEDWATCH_US: 0/1 sources have data",
            source="FEDWATCH_US",
            timestamp="2026-05-01T00:00:00+00:00",
        )
        report = ValidationReport(
            source="concept:FEDWATCH_US",
            run_id="abc",
            timestamp="2026-05-01T00:00:00+00:00",
            checks=(failing_check,),
        )

        engine = Mock()
        engine.validate_all_concepts.return_value = [report]

        store = Mock()
        store.seed_concept_map.return_value = None

        output = io.StringIO()
        with patch("ingestion.validation.ValidationEngine", return_value=engine), \
             patch("ingestion.validation.ValidationStore"), \
             patch("storage.SQLiteEngineStore", return_value=store):
            with redirect_stdout(output):
                rc = main(["validate"])

        self.assertEqual(rc, 1)
        engine.validate_all_concepts.assert_called_once()
        engine.validate_full.assert_not_called()
        text = output.getvalue()
        self.assertIn("FEDWATCH_US", text)

    def test_validate_no_source_passes_when_all_concepts_pass(self) -> None:
        from ingestion.validation._types import ValidationReport

        passing = ValidationReport(
            source="concept:CPI_US",
            run_id="r1",
            timestamp="2026-05-01T00:00:00+00:00",
            checks=(),
        )

        engine = Mock()
        engine.validate_all_concepts.return_value = [passing]

        store = Mock()

        with patch("ingestion.validation.ValidationEngine", return_value=engine), \
             patch("ingestion.validation.ValidationStore"), \
             patch("storage.SQLiteEngineStore", return_value=store):
            with redirect_stdout(io.StringIO()):
                rc = main(["validate"])

        self.assertEqual(rc, 0)

    def test_validate_with_source_zero_checks_returns_error(self) -> None:
        """Issue #102 P2: ``validate --source X`` that produces zero checks
        must exit non-zero, not silently report PASS."""

        from ingestion.validation._types import ValidationReport

        empty_report = ValidationReport(
            source="fred",
            run_id="r1",
            timestamp="2026-05-01T00:00:00+00:00",
            checks=(),
        )

        engine = Mock()
        engine.validate_full.return_value = empty_report

        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        with patch("ingestion.validation.ValidationEngine", return_value=engine), \
             patch("ingestion.validation.ValidationStore"):
            with redirect_stdout(stdout_buf), \
                 patch("sys.stderr", stderr_buf):
                rc = main(["validate", "--source", "fred"])

        self.assertEqual(rc, 1)
        # Human-readable failure message goes to stderr, not stdout.
        self.assertIn("0 checks", stderr_buf.getvalue())

    def test_validate_with_source_zero_checks_json_keeps_payload_parseable(self) -> None:
        """Issue #102 P2: ``--json`` output must remain valid JSON even
        when zero checks ran. The failure signal goes into the payload
        as ``ok=false`` + ``error`` rather than as a trailing text line."""

        from ingestion.validation._types import ValidationReport

        empty_report = ValidationReport(
            source="fred",
            run_id="r1",
            timestamp="2026-05-01T00:00:00+00:00",
            checks=(),
        )

        engine = Mock()
        engine.validate_full.return_value = empty_report

        stdout_buf = io.StringIO()
        with patch("ingestion.validation.ValidationEngine", return_value=engine), \
             patch("ingestion.validation.ValidationStore"):
            with redirect_stdout(stdout_buf):
                rc = main(["validate", "--source", "fred", "--json"])

        self.assertEqual(rc, 1)
        payload = json.loads(stdout_buf.getvalue())
        self.assertFalse(payload["ok"])
        self.assertIn("0 checks", payload["error"])


if __name__ == "__main__":
    unittest.main()
