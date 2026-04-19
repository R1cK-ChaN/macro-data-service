"""Tests for Step 4: news ingestion mirrors articles into the unified
document surface (document + document_blob + documents_fts + item_subjects)
and for the ported discovery helpers."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion._shared.discovery import (
    FetchResult,
    SearchResult,
    extract_urls,
    fetch_url,
    search,
)
from ingestion.news.client import NewsIngestionClient
from ingestion.news._extract import NewsExtraction
from ingestion.news._types import PreparedNewsRecord
from storage.sqlite import SQLiteEngineStore


# ── Discovery helpers ────────────────────────────────────────────────────


def test_extract_urls_dedups_and_preserves_order() -> None:
    md = (
        "First https://a.com/p1 then "
        "https://b.com/path?x=1. "
        "Duplicate https://a.com/p1 should collapse."
    )
    assert extract_urls(md) == [
        "https://a.com/p1",
        "https://b.com/path?x=1",
    ]


def test_extract_urls_limit() -> None:
    md = "one https://a.com two https://b.com three https://c.com"
    assert extract_urls(md, limit=2) == ["https://a.com", "https://b.com"]


def test_extract_urls_empty() -> None:
    assert extract_urls("") == []
    assert extract_urls(None) == []  # type: ignore[arg-type]


def test_search_returns_empty_without_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("BRAVE_API_KEY", raising=False)
    # Neutralize any .env file resolution too — otherwise a local .env
    # with BRAVE_API_KEY defined would make this test hit the network.
    import env
    monkeypatch.setattr(env, "DEFAULT_ENV_FILES", ())
    env.clear_env_cache()
    assert search("federal reserve") == []


def test_search_reads_key_from_env_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Regression for the codex P2: search must honor repo .env files the
    same way FRED_API_KEY / LLM_API_KEY do."""
    env_file = tmp_path / ".env"
    env_file.write_text("BRAVE_API_KEY=from-env-file\n")
    monkeypatch.delenv("BRAVE_API_KEY", raising=False)
    import env
    monkeypatch.setattr(env, "DEFAULT_ENV_FILES", (env_file,))
    env.clear_env_cache()

    captured: dict[str, object] = {}

    class FakeResp:
        status_code = 200
        def raise_for_status(self) -> None: return None
        def json(self):
            return {"web": {"results": [
                {"title": "t", "url": "https://x.com/", "description": "s"},
            ]}}

    def fake_get(url, params=None, headers=None, timeout=None):
        captured["headers"] = headers
        return FakeResp()

    monkeypatch.setattr("httpx.get", fake_get)
    results = search("x")
    assert len(results) == 1
    assert captured["headers"]["X-Subscription-Token"] == "from-env-file"


def test_search_empty_query_short_circuits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BRAVE_API_KEY", "sk-fake")  # would be used if queried
    assert search("") == []
    assert search("   ") == []


def test_fetch_url_without_paywall_returns_error_on_4xx(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeResp:
        status_code = 403
        text = ""

    def fake_get(*a, **k):
        return FakeResp()

    monkeypatch.setattr("httpx.get", fake_get)
    result = fetch_url("https://example.com/paywalled")
    assert isinstance(result, FetchResult)
    assert result.status == 403
    assert result.text == ""
    assert "HTTP 403" in (result.error or "")


def test_fetch_url_returns_text_on_2xx(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeResp:
        status_code = 200
        text = "<html>hello world</html>"

    monkeypatch.setattr("httpx.get", lambda *a, **k: FakeResp())
    result = fetch_url("https://example.com/")
    assert result.status == 200
    assert "hello world" in result.text


# ── News → document mirroring ────────────────────────────────────────────


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _prepared(**overrides) -> PreparedNewsRecord:
    base = dict(
        source_feed="Reuters Business",
        feed_category="business",
        description="Fed chair comments on inflation outlook.",
        timestamp=1_712_764_200,  # 2024-04-10 ~14:30Z
        canonical_url="https://reuters.com/x/fed-chair-inflation",
        raw_url="https://reuters.com/x/fed-chair-inflation?src=rss",
        raw_title="Fed chair warns CPI trajectory unstable",
        url_hash="a" * 64,
        title_hash="b" * 64,
    )
    base.update(overrides)
    return PreparedNewsRecord(**base)


def _extraction(**overrides) -> NewsExtraction:
    base = dict(
        title="Fed chair warns CPI trajectory unstable",
        institution="Reuters",
        authors="Jane Reporter",
        publish_date="2024-04-10",
        data_period="2024-04",
        country="US",
        market="US Treasuries",
        asset_class="Macro",
        sector="Inflation",
        document_type="News Article",
        event_type="Market Commentary",
        subject="CPI",
        subject_id="",
        language="en",
        contains_commentary=True,
        impact_level="high",
        finance_category="inflation",
        confidence=0.8,
        extraction_provider="keyword",
    )
    base.update(overrides)
    return NewsExtraction(**base)


def test_mirror_creates_document_with_structured_columns(store) -> None:
    client = NewsIngestionClient()
    # Build the subject tagger the same way store_articles does.
    client._ensure_news_doc_source(store)
    from storage.subjects import SubjectTagger, sync_from_yaml
    sync_from_yaml(store)
    with store._connection(commit=False) as c:
        tagger = SubjectTagger(c)
    client._mirror_as_document(
        store=store,
        tagger=tagger,
        entry=_prepared(),
        extraction=_extraction(),
        article_content=(
            "The chair emphasized that headline CPI readings have been "
            "running hotter than expected for three consecutive months, "
            "and markets now price higher terminal rates as a result."
        ),
    )
    docs = store.list_documents(source_id="news", limit=5)
    assert len(docs) == 1
    doc = docs[0]
    assert doc.source_id == "news"
    assert doc.document_type == "report"  # news maps to 'report'
    assert doc.title.startswith("Fed chair warns")
    assert doc.subtitle == "Reuters Business"  # feed name carried here
    assert doc.institution == "Reuters"
    assert doc.asset_class == "Macro"
    assert doc.impact_level == "high"
    assert doc.contains_commentary is True
    assert doc.confidence == 0.8
    assert doc.subject_freetext == "CPI"
    assert doc.event_type == "Market Commentary"


def test_mirror_indexes_into_documents_fts(store) -> None:
    client = NewsIngestionClient()
    client._ensure_news_doc_source(store)
    from storage.subjects import SubjectTagger, sync_from_yaml
    sync_from_yaml(store)
    with store._connection(commit=False) as c:
        tagger = SubjectTagger(c)
    client._mirror_as_document(
        store=store,
        tagger=tagger,
        entry=_prepared(),
        extraction=_extraction(),
        article_content="The chair emphasized headline CPI trajectory.",
    )
    hits = store.search_documents("CPI")
    assert [h.source_id for h in hits] == ["news"]
    hits = store.search_documents("trajectory")
    assert len(hits) == 1


def test_mirror_tags_subjects_via_title_regex(store) -> None:
    client = NewsIngestionClient()
    client._ensure_news_doc_source(store)
    from storage.subjects import SubjectTagger, sync_from_yaml
    sync_from_yaml(store)
    with store._connection(commit=False) as c:
        tagger = SubjectTagger(c)
    client._mirror_as_document(
        store=store,
        tagger=tagger,
        entry=_prepared(),
        extraction=_extraction(),
        article_content="body",
    )
    doc = store.list_documents(source_id="news", limit=1)[0]
    tags = dict(store.list_document_subjects(doc.document_id))
    # "Fed chair warns CPI trajectory unstable" — matches econ.cpi (\bCPI\b)
    # and rate.us.fed_funds (\bfed(eral reserve)?\s+rate\b? -- no, just \bFOMC\b
    # / \bfed funds\b). Let's just assert CPI and check the tagger didn't
    # over-match.
    assert "econ.cpi" in tags
    assert tags["econ.cpi"] == 0.8


@pytest.mark.parametrize(
    "extractor_country, expected",
    [
        ("", "XX"),
        ("global", "XX"),
        ("Global", "XX"),
        ("World", "XX"),
        ("US", "US"),
        ("us", "US"),
        ("USA", "US"),
        ("united states", "US"),
        ("China", "CN"),
        ("JAPAN", "JP"),
        ("Euro Area", "EU"),
        ("UK", "GB"),
        ("Unknown land", "XX"),
    ],
)
def test_mirror_normalizes_country_code(
    store, extractor_country: str, expected: str,
) -> None:
    """Regression for codex P2: blank / free-text country values must not
    silently default to 'US' — that poisons downstream country filters
    with global and non-US news."""
    client = NewsIngestionClient()
    client._ensure_news_doc_source(store)
    from storage.subjects import SubjectTagger, sync_from_yaml
    sync_from_yaml(store)
    with store._connection(commit=False) as c:
        tagger = SubjectTagger(c)
    # Unique URL per parametrization so each case creates a distinct doc.
    prepared = _prepared(
        canonical_url=f"https://example.com/{expected}/{extractor_country}",
        url_hash=f"{hash((extractor_country, expected)) & 0xFFFFFFFFFFFFFFFF:016x}" * 4,
    )
    client._mirror_as_document(
        store=store, tagger=tagger, entry=prepared,
        extraction=_extraction(country=extractor_country),
        article_content="body",
    )
    doc = store.get_document(prepared.url_hash[:16])
    assert doc is not None
    assert doc.country_code == expected


def test_mirror_skips_if_document_already_exists(store) -> None:
    """When a gov-report ingest wrote this URL first, news ingestion
    should leave it alone so it isn't overwritten with news metadata."""
    client = NewsIngestionClient()
    client._ensure_news_doc_source(store)
    from storage.subjects import SubjectTagger, sync_from_yaml
    sync_from_yaml(store)
    with store._connection(commit=False) as c:
        tagger = SubjectTagger(c)

    prepared = _prepared()
    # First write — as a news document.
    client._mirror_as_document(
        store=store, tagger=tagger, entry=prepared,
        extraction=_extraction(impact_level="high"),
        article_content="original body",
    )
    # Second write with different extraction — should be a no-op.
    client._mirror_as_document(
        store=store, tagger=tagger, entry=prepared,
        extraction=_extraction(impact_level="critical",
                               institution="SomethingElse"),
        article_content="updated body that should not land",
    )
    doc = store.list_documents(source_id="news", limit=5)[0]
    assert doc.impact_level == "high"
    assert doc.institution == "Reuters"


def test_ensure_news_doc_source_is_idempotent(store) -> None:
    client = NewsIngestionClient()
    client._ensure_news_doc_source(store)
    client._ensure_news_doc_source(store)
    with store._connection(commit=False) as c:
        cnt = c.execute(
            "SELECT COUNT(*) FROM doc_source WHERE source_id = 'news'"
        ).fetchone()[0]
    assert cnt == 1
