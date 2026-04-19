"""LLM-based 17-field entity extractor for documents.

Ported from information-layer ``doc_parser/extraction.py``. Given a
document title + markdown body, it calls an OpenAI-compatible chat
completions endpoint and returns the structured fields the
information-layer introduced (issue #3 + the 17-field schema).

Designed to be **optional** — when no API key is configured,
:func:`make_extractor_from_env` returns a :class:`NullDocumentExtractor`
that yields empty fields, so ingestion pipelines continue to work
with zero configuration. Scraper-provided metadata
(``institution``, ``importance``, ``category``) is written directly by
callers without going through the LLM.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Field definitions + prompt
# ---------------------------------------------------------------------------


EXTRACTION_FIELDS: list[dict[str, str]] = [
    {"key": "title",          "description": "Document title or report title"},
    {"key": "institution",    "description": "Publishing institution (e.g., Goldman Sachs, BLS, Federal Reserve, 国家统计局)"},
    {"key": "authors",        "description": "Author names, analysts or spokespersons"},
    {"key": "publish_date",   "description": "Publication date of the document"},
    {"key": "data_period",    "description": "Data reference period if applicable (e.g., 2025-01, Q4 2024), distinct from publish_date"},
    {"key": "country",        "description": "Country or region the document pertains to (e.g., US, CN, EU, Global)"},
    {"key": "market",         "description": "Financial market dimension (e.g., A股, US Treasuries, S&P 500)"},
    {"key": "asset_class",    "description": "High-level asset class (e.g., Fixed Income, Equity, FX, Commodity, Real Estate, Multi-Asset, Macro, Policy)"},
    {"key": "sector",         "description": "Specific sector or topic (e.g., Inflation, Labor Market, Healthcare, Technology, Gold, Interest Rate)"},
    {"key": "document_type",  "description": "Type of document (e.g., Research Report, Market Commentary, Official Press Release, Policy Statement, Meeting Minutes, Policy Report, Press Conference Transcript, Survey Report, News Article, Government Announcement)"},
    {"key": "event_type",     "description": "Event classification if applicable (e.g., Economic Release, Policy Statement, Press Conference, Survey, News Article)"},
    {"key": "subject",        "description": "Core subject or topic (e.g., CPI, Apple Inc., Federal Funds Rate, LPR)"},
    {"key": "language",       "description": "Document language (en or zh)"},
    {"key": "contains_commentary", "description": "true if the document contains a full paragraph of qualitative analysis from officials or analysts; false if purely numerical or a single boilerplate sentence"},
    {"key": "impact_level",   "description": "Market impact level: 'critical' (bank failure, crash, currency crisis), 'high' (rate decision, CPI, NFP, tariff), 'medium' (inflation, yield moves, earnings, commodities), 'low' (housing, regulation, geopolitics), or 'info' (no significant impact)"},
    {"key": "confidence",     "description": "Confidence in the impact_level classification, 0.0–1.0 (0.9 critical / 0.8 high / 0.7 medium / 0.6 low / 0.3 info; ±0.1 for clarity of fit)"},
]


_SYSTEM_PROMPT_TEMPLATE = """\
You are a financial document metadata extractor. The documents may be \
broker research reports, government statistical releases, central bank \
statements, press conference transcripts, news articles, or other \
financial/economic publications. Extract the following fields from the \
document text. Return ONLY valid JSON with these keys:

{field_descriptions}

The source text may contain OCR-style character errors. In Chinese text, \
visually similar characters are often swapped (e.g., 周↔風, 辩↔牌, 宗↔资, \
期↔朋). Use financial domain knowledge to correct likely mistakes — prefer \
well-known financial terms and proper nouns over unlikely combinations.

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
significant this document's content is for financial markets. document_type \
describes the form of the document; event_type describes the event that \
triggered it. They may coincide — that is expected.

For any field you cannot determine, use null.\
"""


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtractionFields:
    """The 11 structured columns we added to ``document`` in Step 2,
    plus ``subject_freetext`` for the pre-resolution subject string."""

    institution: str = ""
    authors: str = ""
    data_period: str = ""
    market: str = ""
    asset_class: str = ""
    sector: str = ""
    event_type: str = ""
    impact_level: str = ""
    contains_commentary: bool = False
    confidence: float = 0.0
    subject_freetext: str = ""


# ---------------------------------------------------------------------------
# Interface + implementations
# ---------------------------------------------------------------------------


class DocumentExtractor(Protocol):
    def extract(self, *, title: str, markdown: str) -> ExtractionFields:
        ...


class NullDocumentExtractor:
    """Default extractor: returns empty fields. Used when no LLM is
    configured. Ingestion fills the structured columns from scraper
    metadata only."""

    def extract(self, *, title: str, markdown: str) -> ExtractionFields:
        del title, markdown
        return ExtractionFields()


class LLMDocumentExtractor:
    """Calls an OpenAI-compatible chat.completions endpoint and parses the
    JSON reply into :class:`ExtractionFields`. Any parse failure or API
    error is logged and an empty result returned — the caller's flow
    must never break because of extraction."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "gpt-4o-mini",
        base_url: str | None = None,
        context_chars: int = 8000,
        max_tokens: int = 1024,
        temperature: float = 0.0,
        timeout_s: float = 120.0,
    ) -> None:
        from openai import OpenAI  # lazy import so tests can mock
        self._model = model
        self._context_chars = context_chars
        self._max_tokens = max_tokens
        self._temperature = temperature
        self._client = OpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout_s,
        )

    def extract(self, *, title: str, markdown: str) -> ExtractionFields:
        body = (markdown or "").strip()
        if not body:
            return ExtractionFields()
        field_lines = "\n".join(
            f'- "{f["key"]}": {f["description"]}' for f in EXTRACTION_FIELDS
        )
        system = _SYSTEM_PROMPT_TEMPLATE.format(
            field_descriptions=field_lines,
            today=date.today().isoformat(),
        )
        user = (
            f"Title: {title}\n\n{body[: self._context_chars]}"
            if title
            else body[: self._context_chars]
        )
        try:
            resp = self._client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                max_tokens=self._max_tokens,
                temperature=self._temperature,
            )
            content = resp.choices[0].message.content or "{}"
            data = _parse_json_response(content)
            # Guard against wrong-shaped replies ([], null, "string"): valid
            # JSON but the wrong type. _coerce_fields expects a dict; any
            # other top-level type becomes an empty result.
            if not isinstance(data, dict):
                return ExtractionFields()
            return _coerce_fields(data)
        except Exception:  # noqa: BLE001 — extraction is best-effort
            logger.warning("LLM extraction failed for title=%r", title, exc_info=True)
            return ExtractionFields()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def make_extractor_from_env(env: dict[str, str] | None = None) -> DocumentExtractor:
    """Return an :class:`LLMDocumentExtractor` if a key is available in the
    environment, otherwise a :class:`NullDocumentExtractor`.

    Recognized env vars (first wins):
      * ``DOCUMENT_EXTRACT_API_KEY`` / ``OPENAI_API_KEY`` — required
      * ``DOCUMENT_EXTRACT_MODEL``   — default ``gpt-4o-mini``
      * ``DOCUMENT_EXTRACT_BASE_URL`` — optional (OpenRouter, Together, etc.)
      * ``DOCUMENT_EXTRACT_CONTEXT_CHARS`` — default ``8000``
    """
    # Use `is None` so callers that pass an explicit empty mapping (common
    # in tests) stay isolated from host env vars like OPENAI_API_KEY.
    if env is None:
        env = os.environ  # type: ignore[assignment]
    api_key = env.get("DOCUMENT_EXTRACT_API_KEY") or env.get("OPENAI_API_KEY")
    if not api_key:
        return NullDocumentExtractor()
    return LLMDocumentExtractor(
        api_key=api_key,
        model=env.get("DOCUMENT_EXTRACT_MODEL") or "gpt-4o-mini",
        base_url=env.get("DOCUMENT_EXTRACT_BASE_URL") or None,
        context_chars=int(env.get("DOCUMENT_EXTRACT_CONTEXT_CHARS") or 8000),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_json_response(text: str) -> dict[str, Any]:
    """Parse a JSON object from an LLM reply, tolerating ```json fences."""
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        lines = [l for l in lines[1:] if not l.strip().startswith("```")]
        cleaned = "\n".join(lines)
    return json.loads(cleaned) if cleaned else {}


_VALID_IMPACT = {"critical", "high", "medium", "low", "info"}


def _coerce_fields(data: dict[str, Any]) -> ExtractionFields:
    """Coerce an LLM JSON dict into :class:`ExtractionFields`, tolerating
    None / missing keys / type drift."""

    def _s(key: str) -> str:
        v = data.get(key)
        return str(v).strip() if v not in (None, "") else ""

    impact = _s("impact_level").lower()
    if impact and impact not in _VALID_IMPACT:
        impact = ""

    try:
        confidence = float(data.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    commentary = data.get("contains_commentary")
    if isinstance(commentary, str):
        commentary = commentary.strip().lower() in ("true", "yes", "1")
    else:
        commentary = bool(commentary)

    return ExtractionFields(
        institution=_s("institution"),
        authors=_s("authors"),
        data_period=_s("data_period"),
        market=_s("market"),
        asset_class=_s("asset_class"),
        sector=_s("sector"),
        event_type=_s("event_type"),
        impact_level=impact,
        contains_commentary=commentary,
        confidence=confidence,
        subject_freetext=_s("subject"),
    )
