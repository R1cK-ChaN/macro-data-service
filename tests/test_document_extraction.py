"""Tests for the LLM 17-field extractor + gov_report ingestion wiring."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Match the standalone-runnable layout used by other tests in this suite.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.documents._extraction import (
    EXTRACTION_FIELDS,
    ExtractionFields,
    LLMDocumentExtractor,
    NullDocumentExtractor,
    _coerce_fields,
    _parse_json_response,
    make_extractor_from_env,
)


# ── Coercion / JSON parsing ──────────────────────────────────────────────


def test_parse_json_response_strips_markdown_fence() -> None:
    assert _parse_json_response('```json\n{"a": 1}\n```') == {"a": 1}
    assert _parse_json_response("{\"a\": 1}") == {"a": 1}
    assert _parse_json_response("") == {}


def test_coerce_fields_empty() -> None:
    f = _coerce_fields({})
    assert f == ExtractionFields()


def test_coerce_fields_tolerates_types() -> None:
    f = _coerce_fields({
        "institution": "US BLS",
        "impact_level": "HIGH",           # case-insensitive
        "confidence": "0.85",              # string number
        "contains_commentary": "yes",      # string truthy
        "subject": "CPI",                  # -> subject_freetext
    })
    assert f.institution == "US BLS"
    assert f.impact_level == "high"
    assert f.confidence == 0.85
    assert f.contains_commentary is True
    assert f.subject_freetext == "CPI"


def test_coerce_fields_clamps_confidence_and_rejects_bad_impact() -> None:
    f = _coerce_fields({"confidence": 2.5, "impact_level": "bogus"})
    assert f.confidence == 1.0
    assert f.impact_level == ""  # rejected, not preserved


def test_coerce_fields_null_safe() -> None:
    # LLM sometimes returns null/None for unknown fields.
    f = _coerce_fields({"institution": None, "confidence": None,
                        "contains_commentary": None})
    assert f.institution == ""
    assert f.confidence == 0.0
    assert f.contains_commentary is False


# ── Factory ──────────────────────────────────────────────────────────────


def test_make_extractor_returns_null_without_key() -> None:
    ex = make_extractor_from_env(env={})
    assert isinstance(ex, NullDocumentExtractor)
    assert ex.extract(title="x", markdown="y") == ExtractionFields()


def test_make_extractor_empty_env_is_isolated_from_os_environ(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Passing an explicit empty dict must NOT fall through to os.environ —
    otherwise tests on machines that happen to export OPENAI_API_KEY build
    an LLM extractor and hit the network."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-host-env")
    ex = make_extractor_from_env(env={})
    assert isinstance(ex, NullDocumentExtractor)


def test_make_extractor_returns_llm_with_key() -> None:
    # Avoid OpenAI SDK constructor side-effects by stubbing it.
    with patch("openai.OpenAI") as mock_openai:
        ex = make_extractor_from_env(env={
            "DOCUMENT_EXTRACT_API_KEY": "sk-test",
            "DOCUMENT_EXTRACT_MODEL": "my-model",
        })
        assert isinstance(ex, LLMDocumentExtractor)
        assert mock_openai.called
        kwargs = mock_openai.call_args.kwargs
        assert kwargs["api_key"] == "sk-test"


# ── LLMDocumentExtractor.extract ─────────────────────────────────────────


class _FakeMessage:
    def __init__(self, content: str) -> None:
        self.content = content


class _FakeChoice:
    def __init__(self, content: str) -> None:
        self.message = _FakeMessage(content)


class _FakeResponse:
    def __init__(self, content: str) -> None:
        self.choices = [_FakeChoice(content)]


class _FakeCompletions:
    def __init__(self, content: str) -> None:
        self._content = content
        self.call_kwargs = None

    def create(self, **kwargs):
        self.call_kwargs = kwargs
        return _FakeResponse(self._content)


class _FakeOpenAIClient:
    def __init__(self, content: str) -> None:
        self.chat = type("Chat", (), {})()
        self.chat.completions = _FakeCompletions(content)


def _build_extractor(reply: str) -> tuple[LLMDocumentExtractor, _FakeOpenAIClient]:
    fake = _FakeOpenAIClient(reply)
    with patch("openai.OpenAI", return_value=fake):
        ex = LLMDocumentExtractor(api_key="sk-x", model="m")
    return ex, fake


def test_extractor_empty_markdown_short_circuits() -> None:
    ex, fake = _build_extractor("{}")
    fields = ex.extract(title="t", markdown="   ")
    assert fields == ExtractionFields()
    assert fake.chat.completions.call_kwargs is None  # not called


def test_extractor_happy_path_maps_all_fields() -> None:
    reply = """{
      "title": "CPI rose 0.5% in March",
      "institution": "US Bureau of Labor Statistics",
      "authors": "BLS Commissioner",
      "publish_date": "2026-04-10",
      "data_period": "2026-03",
      "country": "US",
      "market": "US Treasuries",
      "asset_class": "Macro",
      "sector": "Inflation",
      "document_type": "Economic Release",
      "event_type": "Economic Release",
      "subject": "CPI",
      "language": "en",
      "contains_commentary": true,
      "impact_level": "high",
      "confidence": 0.85
    }"""
    ex, fake = _build_extractor(reply)
    f = ex.extract(title="CPI rose 0.5%", markdown="Consumer prices rose ...")
    assert f.institution == "US Bureau of Labor Statistics"
    assert f.impact_level == "high"
    assert f.confidence == 0.85
    assert f.contains_commentary is True
    assert f.asset_class == "Macro"
    assert f.sector == "Inflation"
    assert f.subject_freetext == "CPI"
    # prompt was built with today's date + all 17 field descriptors
    sent = fake.chat.completions.call_kwargs
    assert sent["model"] == "m"
    assert any(m["role"] == "system" for m in sent["messages"])


def test_extractor_api_error_returns_empty_fields() -> None:
    """Extraction is best-effort — any failure must never bubble out."""
    class Broken:
        class chat:
            class completions:
                @staticmethod
                def create(**_):
                    raise RuntimeError("upstream 500")

    with patch("openai.OpenAI", return_value=Broken):
        ex = LLMDocumentExtractor(api_key="sk", model="m")
    fields = ex.extract(title="t", markdown="body text")
    assert fields == ExtractionFields()


def test_extractor_bad_json_returns_empty_fields() -> None:
    ex, _ = _build_extractor("not json at all")
    assert ex.extract(title="t", markdown="body text") == ExtractionFields()


@pytest.mark.parametrize("reply", ["[]", "null", '"just a string"', "42"])
def test_extractor_non_object_json_returns_empty_fields(reply: str) -> None:
    """The LLM occasionally returns valid JSON of the wrong shape
    (top-level list, null, scalar). That must not raise through the
    ingestion loop — return empty fields instead."""
    ex, _ = _build_extractor(reply)
    assert ex.extract(title="t", markdown="body text") == ExtractionFields()


# ── EXTRACTION_FIELDS schema guarantee ───────────────────────────────────


def test_extraction_field_set_matches_structured_columns() -> None:
    """Drift guard — the extractor's prompt must still cover every
    structured column that ingestion promises to populate."""
    keys = {f["key"] for f in EXTRACTION_FIELDS}
    expected_structured = {
        "institution", "authors", "data_period", "market", "asset_class",
        "sector", "event_type", "impact_level", "contains_commentary",
        "confidence", "subject",
    }
    missing = expected_structured - keys
    assert not missing, f"extractor prompt missing columns: {missing}"


# ── gov_report ingestion wiring ──────────────────────────────────────────


def _gov_item(**overrides):
    from ingestion.scrapers.gov_report import GovReportItem
    # content_markdown must clear the 100-char threshold in _gov_report.py
    # (below which the client falls back to item.description); tests should
    # always exercise the full "content present" path.
    base = dict(
        source="gov_bls",
        source_id="us_bls_cpi",
        title="CPI rose 0.5% in March",
        url="https://bls.gov/cpi/2026-04.htm",
        published_at="2026-04-10T08:30:00Z",
        published_precision="exact",
        data_category="inflation",
        institution="US Bureau of Labor Statistics",
        description="",
        importance="high",
        country="US",
        language="en",
        content_markdown=(
            "Consumer prices rose 0.5 percent in March after a flat February. "
            "The all items index was up 3.1 percent over the last twelve "
            "months before seasonal adjustment. Core inflation ex-food and "
            "energy advanced 0.3 percent month over month."
        ),
        raw_json={},
    )
    base.update(overrides)
    return GovReportItem(**base)


@pytest.fixture()
def store(tmp_path: Path):
    from storage.sqlite import SQLiteEngineStore
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def test_gov_report_writes_structured_columns_from_scraper_metadata(store) -> None:
    """Without any LLM configured, scraper-supplied institution +
    importance must still land in the new structured columns."""
    from ingestion.documents.clients._gov_report import (
        GovReportIngestionClient,
    )
    client = GovReportIngestionClient(extractor=NullDocumentExtractor())
    client.store_items(store, [_gov_item()])

    docs = store.list_documents(document_type="release", limit=5)
    assert len(docs) == 1
    doc = docs[0]
    assert doc.institution == "US Bureau of Labor Statistics"
    assert doc.impact_level == "high"
    # Fields the LLM would have populated stay empty in the null path
    assert doc.asset_class == ""
    assert doc.sector == ""


def test_gov_report_merges_llm_extraction_on_blank_fields(store) -> None:
    from ingestion.documents.clients._gov_report import (
        GovReportIngestionClient,
    )

    class FakeExtractor:
        def extract(self, *, title, markdown):
            return ExtractionFields(
                institution="LLM-said-inst",  # scraper value wins
                authors="BLS Commissioner",
                data_period="2026-03",
                market="US Treasuries",
                asset_class="Macro",
                sector="Inflation",
                event_type="Economic Release",
                impact_level="critical",       # scraper "high" wins
                contains_commentary=True,
                confidence=0.9,
                subject_freetext="CPI",
            )

    client = GovReportIngestionClient(extractor=FakeExtractor())
    client.store_items(store, [_gov_item()])

    doc = store.list_documents(document_type="release", limit=5)[0]
    # Scraper metadata is authoritative for the fields it covers
    assert doc.institution == "US Bureau of Labor Statistics"
    assert doc.impact_level == "high"
    # LLM fills the rest
    assert doc.authors == "BLS Commissioner"
    assert doc.data_period == "2026-03"
    assert doc.asset_class == "Macro"
    assert doc.sector == "Inflation"
    assert doc.event_type == "Economic Release"
    assert doc.contains_commentary is True
    assert doc.confidence == 0.9
    assert doc.subject_freetext == "CPI"


def test_gov_report_indexes_into_documents_fts(store) -> None:
    from ingestion.documents.clients._gov_report import (
        GovReportIngestionClient,
    )
    client = GovReportIngestionClient(extractor=NullDocumentExtractor())
    client.store_items(store, [_gov_item()])

    hits = store.search_documents("CPI")
    assert len(hits) == 1
    assert hits[0].title.startswith("CPI rose 0.5%")
    hits = store.search_documents("consumer prices")
    assert len(hits) == 1


def test_gov_report_tags_subjects_via_title_regex(store) -> None:
    from ingestion.documents.clients._gov_report import (
        GovReportIngestionClient,
    )
    client = GovReportIngestionClient(extractor=NullDocumentExtractor())
    client.store_items(store, [_gov_item()])

    doc = store.list_documents(document_type="release", limit=5)[0]
    tags = dict(store.list_document_subjects(doc.document_id))
    # Title "CPI rose 0.5% in March" matches econ.cpi via \bCPI\b regex
    assert "econ.cpi" in tags
    assert tags["econ.cpi"] == 0.8
