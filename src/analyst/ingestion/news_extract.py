"""LLM-based metadata extraction for news articles."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date

import httpx

from analyst.env import get_env_value
from analyst.ingestion.news_classify import classify

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Field definitions (matching information repo's doc_parser)
# ---------------------------------------------------------------------------

EXTRACTION_FIELDS = [
    {"key": "title", "description": "Document title or report title"},
    {"key": "institution", "description": "Publishing institution (e.g., Goldman Sachs, BLS, Federal Reserve, CNBC, Reuters)"},
    {"key": "authors", "description": "Author names, analysts or spokespersons"},
    {"key": "publish_date", "description": "Publication date of the document"},
    {"key": "data_period", "description": "Data reference period if applicable (e.g., 2025-01, Q4 2024), distinct from publish_date"},
    {"key": "country", "description": "Country or region (e.g., US, CN, EU, Global)"},
    {"key": "market", "description": "Financial market dimension (e.g., US Treasuries, S&P 500, Global Markets)"},
    {"key": "asset_class", "description": "High-level asset class (e.g., Fixed Income, Equity, FX, Commodity, Macro, Policy)"},
    {"key": "sector", "description": "Specific sector or topic (e.g., Inflation, Labor Market, Interest Rate, Technology)"},
    {"key": "document_type", "description": "Type of document (e.g., Research Report, News Article, Press Release, Policy Statement)"},
    {"key": "event_type", "description": "Event classification (e.g., Economic Release, Policy Statement, Market Move, Corporate Earnings)"},
    {"key": "subject", "description": "Core subject or topic (e.g., CPI, Federal Funds Rate, Apple Inc.)"},
    {"key": "subject_id", "description": "Identifier for the subject if available (e.g., AAPL, CPIAUCSL)"},
    {"key": "language", "description": "Document language (e.g., en, zh)"},
    {"key": "contains_commentary", "description": "Whether document contains qualitative commentary (true or false)"},
    {"key": "impact_level", "description": "Financial market impact: 'critical' (crashes, crises), 'high' (rate decisions, CPI, NFP), 'medium' (inflation, yields, earnings), 'low' (housing, regulation), 'info' (no impact)"},
    {"key": "confidence", "description": "Confidence in impact_level, 0.0-1.0 (0.9 critical, 0.8 high, 0.7 medium, 0.6 low, 0.3 info)"},
]

_SYSTEM_PROMPT_TEMPLATE = """\
You are a financial document metadata extractor. The documents may be \
broker research reports, government statistical releases, central bank \
statements, press conference transcripts, news articles, or other \
financial/economic publications. Extract the following fields from the \
document text. Return ONLY valid JSON with these keys:

{field_descriptions}

Today's date is {today}. For publish_date, extract the date exactly as it \
appears in the document text — do not substitute a different year based on \
assumptions.

data_period refers to the period the data covers, not the publication date \
(e.g., a CPI report published 2025-02-12 may cover data_period "2025-01"). \
Normalize to these formats: monthly "YYYY-MM", quarterly "YYYY-QN", \
annual "YYYY". Do not use spelled-out month names or other variations.

For contains_commentary, return true only if the document contains at \
least one full paragraph of qualitative analysis, interpretation, or \
opinion from analysts or officials. A document that is purely numerical \
tables, or that contains only a single sentence of boilerplate summary, \
should be false.

For language, use the primary language of the document body: "en" or "zh". \
If the document has substantial content in both languages, use "en,zh".

For impact_level, assess from a macro-finance trading perspective how \
significant this document's content is for financial markets. Use the same \
scale: "critical" for systemic events (bank failures, crashes, currency \
crises), "high" for major scheduled releases and policy decisions (rate \
decisions, CPI, NFP, tariffs), "medium" for notable market-moving topics \
(inflation data, yield moves, earnings, commodities), "low" for background \
context (housing, regulation, geopolitics), "info" for minimal market impact.

For confidence, reflect how certain you are about the impact_level. Use 0.9 \
for clear critical events, 0.8 for high, 0.7 for medium, 0.6 for low, 0.3 \
for info. Adjust within ±0.1 based on how clearly the content matches.

document_type describes the form of the document (e.g., "Research Report", \
"Meeting Minutes"). event_type describes the event that triggered the \
document (e.g., "Economic Release", "Press Conference"). The two may \
coincide (e.g., both "Policy Statement") — this is expected, not an error.

For any field you cannot determine, use null.\
"""

# ---------------------------------------------------------------------------
# Fallback mapping tables (from information repo's export.py)
# ---------------------------------------------------------------------------

_FINANCE_CAT_TO_ASSET_CLASS = {
    "monetary_policy": "Macro",
    "inflation": "Macro",
    "employment": "Macro",
    "rates": "Fixed Income",
    "fx": "FX",
    "commodities": "Commodity",
    "crypto": "Crypto",
    "earnings": "Equity",
    "ipo": "Equity",
    "trade": "Macro",
    "regulation": "Policy",
    "geopolitical_risk": "Macro",
    "general": "Multi-Asset",
}

_FEED_CAT_TO_MARKET = {
    "markets": "Global Markets",
    "forex": "FX",
    "bonds": "US Treasuries",
    "commodities": "Commodities",
    "crypto": "Crypto",
    "centralbanks": "Global Markets",
    "economic": "Macro",
    "ipo": "US Equity",
    "derivatives": "Derivatives",
    "fintech": "Fintech",
    "regulation": "Regulatory",
    "institutional": "Institutional",
    "analysis": "Global Markets",
    "thinktanks": "Geopolitics",
    "government": "Policy",
}

_FINANCE_CAT_TO_EVENT_TYPE = {
    "monetary_policy": "Policy Statement",
    "inflation": "Economic Release",
    "employment": "Economic Release",
    "rates": "Market Move",
    "fx": "Market Move",
    "commodities": "Market Move",
    "crypto": "Market Move",
    "earnings": "Corporate Earnings",
    "ipo": "Corporate Action",
    "trade": "Policy Statement",
    "regulation": "Regulatory Action",
    "geopolitical_risk": "Geopolitical Event",
    "general": "News Article",
}

_LLM_CONTEXT_CHARS = 4000
_LLM_MAX_TOKENS = 1024
_LLM_TEMPERATURE = 0.0


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class NewsExtraction:
    """Result of news metadata extraction (LLM or keyword fallback)."""
    title: str
    institution: str
    authors: str
    publish_date: str
    data_period: str
    country: str
    market: str
    asset_class: str
    sector: str
    document_type: str
    event_type: str
    subject: str
    subject_id: str
    language: str
    contains_commentary: bool
    impact_level: str
    finance_category: str
    confidence: float
    extraction_provider: str  # "llm" or "keyword"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_system_prompt() -> str:
    field_lines = "\n".join(
        f'- "{f["key"]}": {f["description"]}' for f in EXTRACTION_FIELDS
    )
    return _SYSTEM_PROMPT_TEMPLATE.format(
        field_descriptions=field_lines,
        today=date.today().isoformat(),
    )


def _compose_extraction_markdown(
    title: str,
    content: str,
    source_feed: str,
    published_at: str,
    feed_category: str,
) -> str:
    lines = [
        f"# {title}",
        "",
        f"Source: {source_feed}",
        f"Published: {published_at}",
        f"Category: {feed_category}",
        "",
        "---",
        "",
        content[:_LLM_CONTEXT_CHARS],
    ]
    return "\n".join(lines)


def _parse_json_response(text: str) -> dict:
    """Extract a JSON object from the LLM response text."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        lines = [line for line in lines[1:] if not line.strip().startswith("```")]
        cleaned = "\n".join(lines)
    return json.loads(cleaned)


def _keyword_fallback(
    title: str,
    description: str,
    source_feed: str,
    feed_category: str,
    published_at: str,
) -> NewsExtraction:
    """Build extraction result from keyword classifier + mapping tables."""
    cls = classify(title, description)
    return NewsExtraction(
        title=title,
        institution=source_feed,
        authors="",
        publish_date=published_at or "",
        data_period="",
        country="",
        market=_FEED_CAT_TO_MARKET.get(feed_category, ""),
        asset_class=_FINANCE_CAT_TO_ASSET_CLASS.get(cls.finance_category, ""),
        sector=cls.finance_category,
        document_type="News Article",
        event_type=_FINANCE_CAT_TO_EVENT_TYPE.get(cls.finance_category, "News Article"),
        subject=title,
        subject_id="",
        language="en",
        contains_commentary=False,
        impact_level=cls.impact_level,
        finance_category=cls.finance_category,
        confidence=cls.confidence,
        extraction_provider="keyword",
    )


# ---------------------------------------------------------------------------
# Main extraction function
# ---------------------------------------------------------------------------

def extract_news_metadata(
    *,
    title: str,
    description: str,
    content_markdown: str,
    source_feed: str,
    feed_category: str,
    published_at: str,
) -> NewsExtraction:
    """Extract structured metadata from a news article.

    Uses LLM when API key is available, falls back to keyword classification.
    """
    api_key = get_env_value("LLM_API_KEY", "OPENROUTER_API_KEY")
    if not api_key:
        return _keyword_fallback(title, description, source_feed, feed_category, published_at)

    base_url = get_env_value(
        "LLM_BASE_URL", "OPENROUTER_BASE_URL",
        default="https://openrouter.ai/api/v1",
    )
    model = get_env_value("ANALYST_NEWS_EXTRACT_MODEL", default="openai/gpt-4o-mini")

    markdown = _compose_extraction_markdown(
        title=title,
        content=content_markdown or description,
        source_feed=source_feed,
        published_at=published_at,
        feed_category=feed_category,
    )

    try:
        client = httpx.Client(
            timeout=httpx.Timeout(60.0, connect=15.0),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        try:
            resp = client.post(
                f"{base_url.rstrip('/')}/chat/completions",
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": _build_system_prompt()},
                        {"role": "user", "content": markdown},
                    ],
                    "max_tokens": _LLM_MAX_TOKENS,
                    "temperature": _LLM_TEMPERATURE,
                },
            )
            resp.raise_for_status()
            content_text = resp.json()["choices"][0]["message"]["content"]
            fields = _parse_json_response(content_text)
        finally:
            client.close()

        def _f(key: str, fallback: str = "") -> str:
            v = fields.get(key)
            return str(v) if v else fallback

        confidence_raw = fields.get("confidence", 0.3)
        try:
            confidence = float(confidence_raw)
        except (TypeError, ValueError):
            confidence = 0.3

        contains_commentary_raw = fields.get("contains_commentary", False)
        if isinstance(contains_commentary_raw, str):
            contains_commentary = contains_commentary_raw.lower() in ("true", "1", "yes")
        else:
            contains_commentary = bool(contains_commentary_raw)

        sector = _f("sector", "general")
        cls = classify(title, description)
        finance_category = cls.finance_category
        market = _f("market", _FEED_CAT_TO_MARKET.get(feed_category, ""))
        asset_class = _f("asset_class", _FINANCE_CAT_TO_ASSET_CLASS.get(finance_category, ""))
        event_type = _f("event_type", _FINANCE_CAT_TO_EVENT_TYPE.get(finance_category, "News Article"))

        return NewsExtraction(
            title=_f("title", title),
            institution=_f("institution", source_feed),
            authors=_f("authors"),
            publish_date=_f("publish_date", published_at or ""),
            data_period=_f("data_period"),
            country=_f("country"),
            market=market,
            asset_class=asset_class,
            sector=sector,
            document_type=_f("document_type", "News Article"),
            event_type=event_type,
            subject=_f("subject", title),
            subject_id=_f("subject_id"),
            language=_f("language", "en"),
            contains_commentary=contains_commentary,
            impact_level=_f("impact_level", "info"),
            finance_category=finance_category,
            confidence=confidence,
            extraction_provider="llm",
        )

    except Exception as exc:
        logger.warning("LLM extraction failed, falling back to keyword: %s", exc)
        return _keyword_fallback(title, description, source_feed, feed_category, published_at)
