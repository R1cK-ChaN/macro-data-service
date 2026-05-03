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
from storage.sqlite import (
    ConceptMapRecord,
    DocSourceRecord,
    DocumentRecord,
    IndicatorObservationRecord,
    SQLiteEngineStore,
)
from storage.subjects import sync_from_yaml


def _make_doc_source(store: SQLiteEngineStore, source_id: str) -> None:
    """Test helper — seed the minimum doc_source row needed for the
    foreign key on ``document.source_id``."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    store.upsert_doc_source(
        DocSourceRecord(
            source_id=source_id,
            source_code=source_id.upper().replace(".", "_"),
            source_name=source_id,
            source_type=(
                "government_agency" if "." in source_id else "news_agency"
            ),
            country_code="US", default_language_code="en",
            homepage_url="https://example.com",
            is_active=True, created_at=now, updated_at=now,
        )
    )


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


# ── list_items cross-type (issue #5 Slice 2) ────────────────────────────


@pytest.fixture()
def cross_type_service(tmp_path: Path) -> LocalMacroDataService:
    """Seed subjects.yaml + concept_map + an indicator row so the
    ``subject_aliases → concept_map → indicators`` chain has live rows
    to find. Pre-#118 this fixture also seeded a macro market
    instrument so the ``subject_aliases → market_instruments`` JOIN
    branch had something to return; that branch retired with
    ``list_subject_market_bars`` when the market lane moved to
    ClickHouse — `list_items` is intentionally macro-line-only."""
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    sync_from_yaml(store)
    store.seed_concept_map()

    # Also ingest one CPI-tagged note so the documents branch has
    # something to return alongside the indicator rows.
    notes_dir = tmp_path / "notes"
    notes_dir.mkdir()
    (notes_dir / "cpi.md").write_text(_NOTE_CPI, encoding="utf-8")
    ingest_notes(notes_dir, store=store)

    store.upsert_indicator_observation(
        IndicatorObservationRecord(
            series_id="CPIAUCSL", source="fred",
            date="2026-03-31", value=310.12,
        )
    )
    return LocalMacroDataService(store=store)


def test_list_items_returns_indicator_rows_tagged_economic_data(
    cross_type_service: LocalMacroDataService,
) -> None:
    resp = cross_type_service.invoke("list_items", {"subject": "econ.cpi"})
    indicators = [
        i for i in resp["items"]
        if i.get("kind") == "indicator"
    ]
    assert indicators, "expected at least one indicator row for econ.cpi"
    row = indicators[0]
    assert row["family"] == "economic_data"
    assert row["source"] == "fred"
    assert row["series_id"] == "CPIAUCSL"
    assert row["concept_id"] == "CPI_US"
    assert row["value"] == 310.12


def test_list_items_family_filter_narrows_to_economic_data(
    cross_type_service: LocalMacroDataService,
) -> None:
    resp = cross_type_service.invoke(
        "list_items", {"subject": "econ.cpi", "family": "economic_data"},
    )
    assert resp["items"], "family filter should not empty the result"
    for item in resp["items"]:
        assert item["family"] == "economic_data"
        assert item["kind"] == "indicator"


def test_list_items_document_rows_carry_family_tag(
    cross_type_service: LocalMacroDataService,
) -> None:
    resp = cross_type_service.invoke("list_items", {"subject": "econ.cpi"})
    docs = [i for i in resp["items"] if i.get("kind") == "document"]
    assert docs, "cpi note should appear in the envelope"
    # The ingested note carries source_id='notes' — surfaced as family='note'.
    assert docs[0]["family"] == "note"


def test_list_items_with_query_suppresses_indicator_branch(
    cross_type_service: LocalMacroDataService,
) -> None:
    """When the caller provides a free-text query, only the documents
    branch runs — indicators carry no FTS-addressable text, so
    surfacing them would silently ignore the query. The market-bar
    branch retired in issue #118 P4 (no cross-store JOIN)."""
    resp = cross_type_service.invoke(
        "list_items", {"subject": "econ.cpi", "q": "inflation"},
    )
    kinds = {i.get("kind") for i in resp["items"]}
    assert "indicator" not in kinds
    assert "market_bar" not in kinds


def test_list_items_lazy_seeds_subject_vocabulary(tmp_path: Path) -> None:
    """A DB populated only through the time-series path carries no
    subject_aliases / concept_map rows. list_items must seed them on
    demand rather than silently returning empty."""
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    store.upsert_indicator_observation(
        IndicatorObservationRecord(
            series_id="CPIAUCSL", source="fred",
            date="2026-03-31", value=310.12,
        )
    )
    svc = LocalMacroDataService(store=store)
    resp = svc.invoke(
        "list_items", {"subject": "econ.cpi", "family": "economic_data"},
    )
    indicators = [i for i in resp["items"] if i.get("kind") == "indicator"]
    assert indicators, "lazy seeding should surface the indicator row"
    assert indicators[0]["series_id"] == "CPIAUCSL"


def test_list_items_dotted_source_id_tagged_release_report(tmp_path: Path) -> None:
    """GovReport docs are stored with source ids like ``us.bls`` —
    ``_document_family`` treats any dotted id as a release_report so the
    family filter catches them."""
    from datetime import datetime, timezone
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    sync_from_yaml(store)
    _make_doc_source(store, "us.bls")
    now = datetime.now(timezone.utc).isoformat()
    doc = DocumentRecord(
        document_id="doc-bls-1",
        release_family_id="",
        source_id="us.bls",
        canonical_url="https://www.bls.gov/releases/cpi-20260315",
        title="CPI March 2026",
        subtitle="", document_type="release", mime_type="text/html",
        language_code="en", country_code="US", topic_code="inflation",
        published_date="2026-03-15", published_at="2026-03-15T12:30:00Z",
        published_epoch_ms=1_742_042_200_000,
        published_precision="day", status="published",
        version_no=1, parent_document_id="",
        hash_sha256="sha-bls-cpi-0315", created_at=now, updated_at=now,
    )
    store.upsert_document(doc)
    store.set_document_subjects(doc.document_id, {"econ.cpi": 1.0})
    svc = LocalMacroDataService(store=store)
    resp = svc.invoke(
        "list_items", {"subject": "econ.cpi", "family": "release_report"},
    )
    assert resp["items"], "dotted source_id should match release_report filter"
    assert all(i["family"] == "release_report" for i in resp["items"])
    assert resp["items"][0]["source_id"] == "us.bls"


def test_list_items_family_filter_sees_through_recency_crowding(tmp_path: Path) -> None:
    """If many recent news docs crowd out older notes within the raw
    fetch window, the family filter must still surface the note — the
    service widens the internal fetch when a family filter is set."""
    from datetime import datetime, timezone
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    sync_from_yaml(store)
    _make_doc_source(store, "news")
    _make_doc_source(store, "notes")
    now_iso = datetime.now(timezone.utc).isoformat()

    def _make_doc(doc_id: str, source: str, doc_type: str, ts_ms: int) -> DocumentRecord:
        return DocumentRecord(
            document_id=doc_id, release_family_id="", source_id=source,
            canonical_url=f"https://example.com/{doc_id}",
            title=f"Title {doc_id}", subtitle="", document_type=doc_type,
            mime_type="text/html", language_code="en", country_code="US",
            topic_code="inflation", published_date="2026-04-01",
            published_at="2026-04-01T00:00:00Z",
            published_epoch_ms=ts_ms, published_precision="day",
            status="published", version_no=1, parent_document_id="",
            hash_sha256=doc_id, created_at=now_iso, updated_at=now_iso,
        )

    # 15 recent news items, then one older note. With a limit of 5 the
    # un-widened fetch would only see news rows.
    for i in range(15):
        doc = _make_doc(f"news-{i:02d}", "news", "report",
                        1_745_000_000_000 + i)
        store.upsert_document(doc)
        store.set_document_subjects(doc.document_id, {"econ.cpi": 1.0})
    older = _make_doc("note-01", "notes", "report", 1_700_000_000_000)
    store.upsert_document(older)
    store.set_document_subjects(older.document_id, {"econ.cpi": 1.0})

    svc = LocalMacroDataService(store=store)
    resp = svc.invoke(
        "list_items",
        {"subject": "econ.cpi", "family": "note", "limit": 5},
    )
    assert resp["items"], "SQL family filter should surface the older note"
    assert resp["items"][0]["document_id"] == "note-01"
    assert resp["items"][0]["family"] == "note"


def test_list_items_indicator_reached_via_direct_alias(tmp_path: Path) -> None:
    """Not every subject alias has a matching concept_map entry —
    ``commodity.gold`` aliases ``fred_series: GOLDAMGBD228NLBM`` but
    the default concept_map doesn't carry it. The indicator branch
    must still surface that observation via the direct alias join."""
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    sync_from_yaml(store)
    store.seed_concept_map()
    store.upsert_indicator_observation(
        IndicatorObservationRecord(
            series_id="GOLDAMGBD228NLBM", source="fred",
            date="2026-04-01", value=2345.5,
        )
    )
    svc = LocalMacroDataService(store=store)
    resp = svc.invoke(
        "list_items", {"subject": "commodity.gold", "family": "economic_data"},
    )
    indicators = [i for i in resp["items"] if i.get("kind") == "indicator"]
    assert indicators, "direct alias path should surface gold indicator"
    assert indicators[0]["series_id"] == "GOLDAMGBD228NLBM"
    assert indicators[0]["source"] == "fred"
    assert indicators[0]["concept_id"] == ""  # no concept_map mapping


def test_list_items_family_filter_without_subject_is_enforced_in_sql(
    tmp_path: Path,
) -> None:
    """``family='note'`` without a subject/query hits the recency feed
    branch. The filter must apply in SQL so newer news rows don't bury
    older notes within the returned window."""
    from datetime import datetime, timezone
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    sync_from_yaml(store)
    _make_doc_source(store, "news")
    _make_doc_source(store, "notes")
    now_iso = datetime.now(timezone.utc).isoformat()
    for i in range(8):
        store.upsert_document(
            DocumentRecord(
                document_id=f"news-{i:02d}", release_family_id="",
                source_id="news",
                canonical_url=f"https://example.com/news-{i}",
                title=f"News {i}", subtitle="", document_type="report",
                mime_type="text/html", language_code="en", country_code="US",
                topic_code="", published_date="2026-04-01",
                published_at="2026-04-01T00:00:00Z",
                published_epoch_ms=1_745_000_000_000 + i,
                published_precision="day", status="published",
                version_no=1, parent_document_id="",
                hash_sha256=f"news-{i}", created_at=now_iso, updated_at=now_iso,
            )
        )
    store.upsert_document(
        DocumentRecord(
            document_id="note-01", release_family_id="", source_id="notes",
            canonical_url="https://example.com/note-01",
            title="Older Note", subtitle="", document_type="report",
            mime_type="text/html", language_code="en", country_code="US",
            topic_code="", published_date="2025-12-01",
            published_at="2025-12-01T00:00:00Z",
            published_epoch_ms=1_700_000_000_000,
            published_precision="day", status="published",
            version_no=1, parent_document_id="",
            hash_sha256="note-01", created_at=now_iso, updated_at=now_iso,
        )
    )
    svc = LocalMacroDataService(store=store)
    resp = svc.invoke("list_items", {"family": "note", "limit": 5})
    assert resp["items"], "family filter must survive the no-subject branch"
    for item in resp["items"]:
        assert item["family"] == "note"


def test_list_items_trend_family_does_not_leak_documents(
    tmp_path: Path,
) -> None:
    """``trend`` rows live in ``trend_topics`` — list_items does not
    project them yet, so a trend-family request must not return
    documents tagged under a different family."""
    from datetime import datetime, timezone
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    sync_from_yaml(store)
    _make_doc_source(store, "news")
    now_iso = datetime.now(timezone.utc).isoformat()
    store.upsert_document(
        DocumentRecord(
            document_id="news-1", release_family_id="", source_id="news",
            canonical_url="https://example.com/news-1",
            title="Headline", subtitle="", document_type="report",
            mime_type="text/html", language_code="en", country_code="US",
            topic_code="", published_date="2026-04-01",
            published_at="2026-04-01T00:00:00Z",
            published_epoch_ms=1_745_000_000_000,
            published_precision="day", status="published",
            version_no=1, parent_document_id="",
            hash_sha256="news-1", created_at=now_iso, updated_at=now_iso,
        )
    )
    svc = LocalMacroDataService(store=store)
    resp = svc.invoke("list_items", {"family": "trend"})
    assert resp["items"] == []


def test_list_items_indicator_fans_out_cross_source_via_concept_id(
    tmp_path: Path,
) -> None:
    """``UNEMP_US`` concept maps FRED ``UNRATE`` + BLS ``LNS14000000``.
    A subject aliasing only ``UNRATE`` must still surface the BLS
    observation — the concept bridge pivots through concept_id rather
    than stopping at the matched concept_map row."""
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    sync_from_yaml(store)
    store.seed_concept_map()
    store.upsert_indicator_observation(
        IndicatorObservationRecord(
            series_id="UNRATE", source="fred",
            date="2026-03-31", value=3.9,
        )
    )
    store.upsert_indicator_observation(
        IndicatorObservationRecord(
            series_id="LNS14000000", source="bls",
            date="2026-03-31", value=3.9,
        )
    )
    svc = LocalMacroDataService(store=store)
    resp = svc.invoke(
        "list_items",
        {"subject": "econ.unemployment", "family": "economic_data"},
    )
    indicators = [i for i in resp["items"] if i.get("kind") == "indicator"]
    sources = {i["source"] for i in indicators}
    assert {"fred", "bls"} <= sources, (
        f"cross-source fan-out should carry both providers, got {sources}"
    )


def test_list_items_family_recency_with_document_type_filter(tmp_path: Path) -> None:
    """Recency feed called with ``family='news'`` + ``document_type='report'``
    must bind each filter to the right column. A parameter-order mix-up
    would pass ``report`` to ``source_id`` and return an empty result."""
    from datetime import datetime, timezone
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    sync_from_yaml(store)
    _make_doc_source(store, "news")
    now_iso = datetime.now(timezone.utc).isoformat()
    store.upsert_document(
        DocumentRecord(
            document_id="news-report-1", release_family_id="", source_id="news",
            canonical_url="https://example.com/report-1",
            title="Analyst Report", subtitle="", document_type="report",
            mime_type="text/html", language_code="en", country_code="US",
            topic_code="", published_date="2026-04-01",
            published_at="2026-04-01T00:00:00Z",
            published_epoch_ms=1_745_000_000_000,
            published_precision="day", status="published",
            version_no=1, parent_document_id="",
            hash_sha256="news-report-1", created_at=now_iso, updated_at=now_iso,
        )
    )
    svc = LocalMacroDataService(store=store)
    resp = svc.invoke(
        "list_items",
        {"family": "news", "document_type": "report", "limit": 5},
    )
    assert resp["items"], "family + document_type combo should return rows"
    assert resp["items"][0]["family"] == "news"
    assert resp["items"][0]["document_type"] == "report"


def test_list_items_direct_only_series_survives_concept_path_cap(
    tmp_path: Path,
) -> None:
    """``rate.us.fed_funds`` has ``DFF`` in concept_map and ``FEDFUNDS``
    on the direct alias path. If the concept path saturates ``limit``
    with DFF observations the direct-only FEDFUNDS series must still
    appear — the merge happens before the cap."""
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    sync_from_yaml(store)
    store.seed_concept_map()
    # Saturate the concept path with recent DFF rows.
    for day in range(3, 25):
        store.upsert_indicator_observation(
            IndicatorObservationRecord(
                series_id="DFF", source="fred",
                date=f"2026-04-{day:02d}", value=5.25,
            )
        )
    # One older direct-only observation.
    store.upsert_indicator_observation(
        IndicatorObservationRecord(
            series_id="FEDFUNDS", source="fred",
            date="2026-04-02", value=5.25,
        )
    )
    svc = LocalMacroDataService(store=store)
    resp = svc.invoke(
        "list_items",
        {"subject": "rate.us.fed_funds", "family": "economic_data", "limit": 10},
    )
    series = {i["series_id"] for i in resp["items"] if i.get("kind") == "indicator"}
    assert "FEDFUNDS" in series, (
        f"direct-only series must survive concept-path saturation, got {sorted(series)}"
    )


def test_list_items_concept_bridge_respects_alias_source(tmp_path: Path) -> None:
    """When a provider_series_id collides across sources in concept_map
    (same id under two ``source_id`` values), a ``fred_series`` alias
    must only bridge through the fred row — otherwise the fan-out
    through cm_out leaks unrelated observations."""
    from datetime import datetime, timezone
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    sync_from_yaml(store)
    store.seed_concept_map()
    # Stage a colliding concept_map entry under a non-fred source that
    # reuses the CPIAUCSL series id, and store an indicator row there.
    now = datetime.now(timezone.utc).isoformat()
    with store._connection(commit=True) as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO concept_map
              (concept_id, source_id, provider_series_id,
               obs_family_id, priority, role, notes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("UNRELATED_CPI", "imf", "CPIAUCSL", "",
             1, "primary", "collision", now),
        )
    store.upsert_indicator_observation(
        IndicatorObservationRecord(
            series_id="CPIAUCSL", source="imf",
            date="2026-03-31", value=999.0,
        )
    )
    store.upsert_indicator_observation(
        IndicatorObservationRecord(
            series_id="CPIAUCSL", source="fred",
            date="2026-03-31", value=310.12,
        )
    )
    svc = LocalMacroDataService(store=store)
    resp = svc.invoke(
        "list_items", {"subject": "econ.cpi", "family": "economic_data"},
    )
    concept_ids = {
        i.get("concept_id")
        for i in resp["items"]
        if i.get("kind") == "indicator"
    }
    assert "UNRELATED_CPI" not in concept_ids, (
        "alias_type=fred_series must not bridge through a non-fred "
        f"concept_map row; got concepts {concept_ids}"
    )


def test_list_items_without_family_filter_shows_all_branches(
    cross_type_service: LocalMacroDataService,
) -> None:
    """With ``family`` omitted, documents and indicators both surface
    — dropping the later branch to fit a global ``limit`` would
    re-introduce the crowding bug. Each branch keeps its own cap.
    The market-bar branch retired in issue #118 P4."""
    resp = cross_type_service.invoke(
        "list_items", {"subject": "econ.cpi", "limit": 5},
    )
    kinds = {i.get("kind") for i in resp["items"]}
    assert "document" in kinds
    assert "indicator" in kinds
