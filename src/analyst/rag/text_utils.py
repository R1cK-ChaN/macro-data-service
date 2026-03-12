"""Text utilities — tokenizer, language detection, content hashing.

Vendored from rag-service ``app/utils/text.py`` (verbatim).
"""

from __future__ import annotations

import hashlib
import re

TOKENIZER_VERSION = "tokenize_v2_stopstem_cjk_bigram"

_ASCII_TOKEN_RE = re.compile(r"[a-z0-9_]+")
_CJK_CHAR_RE = re.compile(r"[\u4e00-\u9fff]")
_CJK_RUN_RE = re.compile(r"[\u4e00-\u9fff]{2,}")

EN_STOPWORDS = frozenset(
    {
        "a", "an", "and", "are", "as", "at", "be", "been", "being", "by",
        "did", "do", "does", "for", "from", "had", "has", "have", "he",
        "her", "here", "hers", "him", "his", "i", "if", "in", "into", "is",
        "it", "its", "me", "my", "of", "on", "or", "our", "ours", "she",
        "so", "that", "the", "their", "theirs", "them", "there", "these",
        "they", "this", "those", "to", "was", "we", "were", "what", "when",
        "where", "which", "who", "with", "you", "your", "yours",
    }
)


def _trim_double_consonant(token: str) -> str:
    if len(token) < 3:
        return token
    if token[-1] != token[-2]:
        return token
    if token[-1] in {"a", "e", "i", "o", "u"}:
        return token
    return token[:-1]


def _stem_en(token: str) -> str:
    if len(token) <= 3 or not token.isalpha():
        return token
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith("ing") and len(token) > 5:
        return _trim_double_consonant(token[:-3])
    if token.endswith("ed") and len(token) > 4:
        return _trim_double_consonant(token[:-2])
    if token.endswith("s") and len(token) > 3 and not token.endswith(("ss", "us", "is")):
        return token[:-1]
    return token


def detect_language(text: str) -> str:
    if _CJK_CHAR_RE.search(text):
        return "zh"
    return "en"


def tokenize(text: str) -> list[str]:
    text = text.lower()
    tokens: list[str] = []
    for raw in _ASCII_TOKEN_RE.findall(text):
        if raw in EN_STOPWORDS:
            continue
        stemmed = _stem_en(raw)
        if stemmed and stemmed not in EN_STOPWORDS:
            tokens.append(stemmed)
    cjk_chars = _CJK_CHAR_RE.findall(text)
    tokens.extend(cjk_chars)
    for seq in _CJK_RUN_RE.findall(text):
        tokens.extend(seq[i : i + 2] for i in range(len(seq) - 1))
    return tokens


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
