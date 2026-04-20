"""Tests for the unified document query ops: list_items / get_document /
list_subjects (Step 8 of the information-layer merge)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.notes.ingest import ingest_notes
from macro_data.service import LocalMacroDataService
from storage.sqlite import DocumentRecord, SQLiteEngineStore


_NOTE_CPI = """---
title: "Sticky services inflation"
date: "2026-04-15"
subject_id: econ.cpi
author: ewan
---

# Body

March CPI rose 0.5% MoM. Core services remain elevated.
"""

_NOTE_VIX = """---
title: "VIX regime thoughts"
date: "2026-04-16"
subject_id: vol.vix
author: ewan
---

Volatility is picking up as the VIX prints at 22, still firmly in the
elevated regime but approaching the stressed threshold.
"""


@pytest.fixture()
def service(tmp_path: Path) -> LocalMacroDataService:
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    notes_dir = tmp_path / "notes"
    notes_dir.mkdir()
    (notes_dir / "cpi.md").write_text(_NOTE_CPI, encoding="utf-8")
    (notes_dir / "vix.md").write_text(_NOTE_VIX, encoding="utf-8")
    ingest_notes(notes_dir, store=store)
    return LocalMacroDataService(store=store)


# ── invoke dispatch ─────────────────────────────────────────────────────


def test_unknown_operation_raises_key_error(service: LocalMacroDataService) -> None:
    with pytest.raises(KeyError):
        service.invoke("does_not_exist", {})


# ── list_subjects ───────────────────────────────────────────────────────


def test_list_subjects_returns_seeded_vocabulary(
    service: LocalMacroDataService,
) -> None:
    resp = service.invoke("list_subjects", {})
    subjects = {s["subject_id"] for s in resp["subjects"]}
    # A handful from the yaml seed — enough to prove sync ran.
    assert {"econ.cpi", "rate.us.sofr", "vol.vix"} <= subjects
    for row in resp["subjects"]:
        assert row["display_name"]  # non-empty


# ── list_items ──────────────────────────────────────────────────────────


def test_list_items_by_subject(service: LocalMacroDataService) -> None:
    resp = service.invoke("list_items", {"subject": "econ.cpi"})
    assert resp["total"] == 1
    item = resp["items"][0]
    assert item["title"] == "Sticky services inflation"
    assert item["source_id"] == "notes"


def test_list_items_by_query(service: LocalMacroDataService) -> None:
    resp = service.invoke("list_items", {"q": "VIX"})
    # FTS5 finds the VIX note body; note subject 'vol.vix' also tagged.
    titles = [i["title"] for i in resp["items"]]
    assert "VIX regime thoughts" in titles


def test_list_items_intersects_subject_and_query(
    service: LocalMacroDataService,
) -> None:
    # Subject matches both notes if both had the subject — only CPI has
    # econ.cpi; the query "services" matches only CPI body. Intersection:
    # CPI.
    resp = service.invoke(
        "list_items", {"subject": "econ.cpi", "q": "services"},
    )
    assert resp["total"] == 1
    assert resp["items"][0]["subject_freetext"] == "econ.cpi"


def test_list_items_empty_args_returns_recent(
    service: LocalMacroDataService,
) -> None:
    resp = service.invoke("list_items", {"limit": 10})
    # Both notes come back, most-recent first.
    assert resp["total"] == 2
    assert resp["items"][0]["published_date"] >= resp["items"][1]["published_date"]


def test_list_items_clamps_limit_to_cap(service: LocalMacroDataService) -> None:
    # limit=99999 should clamp to 500 (cap). Nothing to assert on counts
    # since the fixture has 2 items, but the call should not error.
    resp = service.invoke("list_items", {"limit": 99999})
    assert resp["total"] <= 500


def test_list_items_invalid_limit_falls_back_to_default(
    service: LocalMacroDataService,
) -> None:
    resp = service.invoke("list_items", {"limit": "abc"})
    # Doesn't raise; falls back to the 50 default
    assert resp["total"] >= 1


def test_list_items_respects_document_type_filter(
    service: LocalMacroDataService,
) -> None:
    resp = service.invoke(
        "list_items", {"document_type": "report"},
    )
    assert resp["total"] == 2  # both notes are document_type='report'

    resp = service.invoke(
        "list_items", {"document_type": "minutes"},
    )
    assert resp["total"] == 0


def test_list_items_respects_country_code_filter(
    service: LocalMacroDataService,
) -> None:
    # Notes default to country_code='XX' unless frontmatter overrides.
    resp = service.invoke("list_items", {"country_code": "XX"})
    assert resp["total"] == 2
    resp = service.invoke("list_items", {"country_code": "US"})
    assert resp["total"] == 0


def test_list_items_min_confidence_excludes_low_confidence(
    service: LocalMacroDataService,
) -> None:
    # Note tags land at confidence 1.0, so min_confidence=1.0 still keeps
    # them; 1.1 excludes everything.
    resp = service.invoke(
        "list_items", {"subject": "econ.cpi", "min_confidence": 1.0},
    )
    assert resp["total"] == 1
    resp = service.invoke(
        "list_items", {"subject": "econ.cpi", "min_confidence": 1.1},
    )
    assert resp["total"] == 0


@pytest.mark.parametrize("bad_conf", ["abc", "", [], {"k": "v"}, object()])
def test_list_items_malformed_min_confidence_uses_default(
    service: LocalMacroDataService, bad_conf,
) -> None:
    """Regression for codex P2: malformed min_confidence must fall back
    to 0.0 instead of raising ValueError into the HTTP handler as a 500."""
    resp = service.invoke(
        "list_items", {"subject": "econ.cpi", "min_confidence": bad_conf},
    )
    assert "error" not in resp
    assert resp["total"] == 1


def test_list_items_combined_applies_filters_in_sql(
    tmp_path: Path,
) -> None:
    """Regression for codex P2: when both subject and q are given, the
    limit must bound the FINAL result set — not two separate candidate
    windows that could hide matches beyond their caps. Seed many econ.cpi
    notes, then a single one whose body hits a rare term, and confirm the
    combined query finds it even if recent subject-tagged notes exceed a
    window-based cap."""
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    notes_dir = tmp_path / "in"
    notes_dir.mkdir()
    # Generate many CPI notes — more than any internal limit*4 window
    # would ever hold by accident.
    for i in range(60):
        (notes_dir / f"cpi_{i:03d}.md").write_text(
            "---\n"
            f'title: "CPI note {i}"\n'
            f"date: 2026-01-{(i % 28) + 1:02d}\n"
            "subject_id: econ.cpi\n"
            "---\n\n"
            "Routine monthly CPI commentary without the target term.\n",
            encoding="utf-8",
        )
    # One note carries the rare term in the body.
    (notes_dir / "needle.md").write_text(
        "---\n"
        'title: "Special CPI reading"\n'
        "date: 2025-06-15\n"  # older so it's deep in the subject-order window
        "subject_id: econ.cpi\n"
        "---\n\n"
        "This note mentions unobtainium explicitly.\n",
        encoding="utf-8",
    )
    ingest_notes(notes_dir, store=store)
    svc = LocalMacroDataService(store=store)

    resp = svc.invoke(
        "list_items",
        {"subject": "econ.cpi", "q": "unobtainium", "limit": 5},
    )
    assert resp["total"] == 1
    assert resp["items"][0]["title"] == "Special CPI reading"


# ── get_document ────────────────────────────────────────────────────────


def test_get_document_by_id_returns_body_and_subjects(
    service: LocalMacroDataService,
) -> None:
    ids = [i["document_id"] for i in
           service.invoke("list_items", {"subject": "econ.cpi"})["items"]]
    assert ids
    resp = service.invoke("get_document", {"document_id": ids[0]})
    assert resp["document"]["title"] == "Sticky services inflation"
    assert "March CPI" in resp["body"]
    assert {"subject_id": "econ.cpi", "confidence": 1.0} in resp["subjects"]


def test_get_document_by_hash_sha256(service: LocalMacroDataService) -> None:
    items = service.invoke("list_items", {"subject": "vol.vix"})["items"]
    sha = items[0]["hash_sha256"]
    resp = service.invoke("get_document", {"hash_sha256": sha})
    assert resp["document"]["title"] == "VIX regime thoughts"
    assert "elevated" in resp["body"]


def test_get_document_missing_id_returns_error(
    service: LocalMacroDataService,
) -> None:
    resp = service.invoke("get_document", {})
    assert "error" in resp


def test_get_document_not_found_returns_null(
    service: LocalMacroDataService,
) -> None:
    resp = service.invoke("get_document", {"document_id": "nonexistent_id"})
    assert resp["document"] is None


# ── list_sources (issue #5 Slice 1) ─────────────────────────────────────


class _StubIngestion:
    def __init__(self, rows: list[dict[str, str]]) -> None:
        self._rows = rows

    def list_sources(self) -> list[dict[str, str]]:
        return list(self._rows)


def test_list_sources_returns_name_family_rows(tmp_path: Path) -> None:
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    ingestion = _StubIngestion([
        {"name": "fred_daily", "family": "economic_data"},
        {"name": "tiingo_market", "family": "market_price"},
        {"name": "news", "family": "news"},
    ])
    svc = LocalMacroDataService(store=store, ingestion=ingestion)
    resp = svc.invoke("list_sources", {})
    assert resp["total"] == 3
    assert resp["sources"][0] == {"name": "fred_daily", "family": "economic_data"}


def test_list_sources_filters_by_family(tmp_path: Path) -> None:
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    ingestion = _StubIngestion([
        {"name": "fred_daily", "family": "economic_data"},
        {"name": "tiingo_market", "family": "market_price"},
        {"name": "news", "family": "news"},
    ])
    svc = LocalMacroDataService(store=store, ingestion=ingestion)
    resp = svc.invoke("list_sources", {"family": "market_price"})
    assert resp["total"] == 1
    assert resp["sources"] == [{"name": "tiingo_market", "family": "market_price"}]


def test_list_sources_without_ingestion_returns_error(tmp_path: Path) -> None:
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    svc = LocalMacroDataService(store=store, ingestion=None)
    resp = svc.invoke("list_sources", {})
    assert resp == {"error": "sources unavailable", "sources": []}
