"""Selector-versioning helpers for fragile HTML scrapers."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from bs4 import BeautifulSoup, Tag

T = TypeVar("T")

_FINGERPRINT_ATTRS = ("id", "data-testid", "data-test", "role", "itemprop", "aria-label")
_DOM_FINGERPRINT_CACHE: dict[str, str] = {}


@dataclass(frozen=True)
class SelectorField:
    selectors: tuple[str, ...]
    attr: str | None = None


@dataclass(frozen=True)
class SelectorVersion:
    name: str
    item_selectors: tuple[str, ...]
    fields: dict[str, SelectorField]
    min_items: int = 1
    min_confidence: float = 0.6
    success_items: int | None = None


@dataclass(frozen=True)
class SelectorBuildContext:
    version: SelectorVersion
    fingerprint: str
    matched_item_selector: str


@dataclass(frozen=True)
class SelectorAttempt(Generic[T]):
    version: str
    items: tuple[T, ...]
    candidate_count: int
    matched_item_selector: str = ""
    field_hits: dict[str, int] = field(default_factory=dict)

    @property
    def items_extracted(self) -> int:
        return len(self.items)

    @property
    def confidence(self) -> float:
        if self.candidate_count <= 0:
            return 0.0
        return self.items_extracted / self.candidate_count


@dataclass(frozen=True)
class SelectorRunResult(Generic[T]):
    items: tuple[T, ...]
    version: str | None
    confidence: float
    fingerprint: str
    fingerprint_changed: bool
    fallback_used: bool
    attempts: tuple[SelectorAttempt[T], ...]


def selector_field(*selectors: str, attr: str | None = None) -> SelectorField:
    normalized = tuple(selector.strip() for selector in selectors if selector and selector.strip())
    if not normalized:
        raise ValueError("selector_field requires at least one selector")
    return SelectorField(selectors=normalized, attr=attr)


def reset_dom_fingerprint_cache() -> None:
    """Clear in-process DOM fingerprints. Intended for tests."""
    _DOM_FINGERPRINT_CACHE.clear()


def dom_structure_fingerprint(html: str, *, max_nodes: int = 1500) -> str:
    soup = BeautifulSoup(html, "html.parser")
    nodes: list[str] = []

    def walk(node: Tag, depth: int) -> None:
        if len(nodes) >= max_nodes:
            return
        attrs: list[str] = []
        for attr_name in _FINGERPRINT_ATTRS:
            raw_value = node.get(attr_name)
            if raw_value in (None, ""):
                continue
            if isinstance(raw_value, list):
                value = " ".join(str(part).strip() for part in raw_value if str(part).strip())
            else:
                value = str(raw_value).strip()
            if value:
                attrs.append(f"{attr_name}={value}")
        nodes.append(f"{depth}:{node.name}[{'|'.join(attrs)}]")
        for child in node.children:
            if isinstance(child, Tag):
                walk(child, depth + 1)
                if len(nodes) >= max_nodes:
                    return

    root = soup.body
    if root is not None:
        walk(root, 0)
    else:
        for child in soup.children:
            if isinstance(child, Tag):
                walk(child, 0)
                if len(nodes) >= max_nodes:
                    break

    payload = "\n".join(nodes).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def extract_with_selector_versions(
    html: str,
    *,
    source: str,
    context: str,
    versions: Sequence[SelectorVersion],
    build_item: Callable[[dict[str, str], Tag, SelectorBuildContext], T | None],
    logger: logging.Logger | None = None,
) -> SelectorRunResult[T]:
    soup = BeautifulSoup(html, "html.parser")
    fingerprint = dom_structure_fingerprint(html)
    cache_key = f"{source}:{context}"
    previous_fingerprint = _DOM_FINGERPRINT_CACHE.get(cache_key)
    fingerprint_changed = previous_fingerprint is not None and previous_fingerprint != fingerprint
    _DOM_FINGERPRINT_CACHE[cache_key] = fingerprint

    attempts: list[SelectorAttempt[T]] = []
    best_attempt: SelectorAttempt[T] | None = None

    for index, version in enumerate(versions):
        candidates, matched_item_selector = _select_candidates(soup, version.item_selectors)
        build_context = SelectorBuildContext(
            version=version,
            fingerprint=fingerprint,
            matched_item_selector=matched_item_selector or "",
        )

        items: list[T] = []
        field_hits: dict[str, int] = {}
        for candidate in candidates:
            raw_fields: dict[str, str] = {}
            for field_name, field in version.fields.items():
                match, _ = _select_first(candidate, field.selectors)
                value = _extract_field_value(match, field.attr)
                raw_fields[field_name] = value
                if value:
                    field_hits[field_name] = field_hits.get(field_name, 0) + 1
            item = build_item(raw_fields, candidate, build_context)
            if item is not None:
                items.append(item)

        attempt = SelectorAttempt(
            version=version.name,
            items=tuple(items),
            candidate_count=len(candidates),
            matched_item_selector=matched_item_selector or "",
            field_hits=field_hits,
        )
        attempts.append(attempt)
        if _is_better_attempt(attempt, best_attempt):
            best_attempt = attempt

        enough_items = attempt.items_extracted >= version.min_items
        confidence_ok = attempt.confidence >= version.min_confidence
        count_ok = version.success_items is not None and attempt.items_extracted >= version.success_items
        if enough_items and (confidence_ok or count_ok):
            if fingerprint_changed and logger is not None:
                logger.warning(
                    "DOM fingerprint changed for %s (%s): %s -> %s",
                    source,
                    context,
                    previous_fingerprint,
                    fingerprint,
                )
            if index > 0 and logger is not None:
                logger.warning(
                    "Selector fallback engaged for %s (%s): version=%s items=%d confidence=%.2f fingerprint=%s",
                    source,
                    context,
                    version.name,
                    attempt.items_extracted,
                    attempt.confidence,
                    fingerprint,
                )
            return SelectorRunResult(
                items=attempt.items,
                version=version.name,
                confidence=attempt.confidence,
                fingerprint=fingerprint,
                fingerprint_changed=fingerprint_changed,
                fallback_used=index > 0,
                attempts=tuple(attempts),
            )

    if logger is not None:
        if fingerprint_changed:
            logger.warning(
                "DOM fingerprint changed for %s (%s): %s -> %s",
                source,
                context,
                previous_fingerprint,
                fingerprint,
            )
        logger.warning(
            "Low-confidence selector extraction for %s (%s): fingerprint=%s attempts=[%s]",
            source,
            context,
            fingerprint,
            _summarize_attempts(attempts),
        )

    if best_attempt is None:
        best_attempt = SelectorAttempt(version="", items=(), candidate_count=0)

    return SelectorRunResult(
        items=best_attempt.items,
        version=best_attempt.version or None,
        confidence=best_attempt.confidence,
        fingerprint=fingerprint,
        fingerprint_changed=fingerprint_changed,
        fallback_used=bool(attempts and attempts[0].version != best_attempt.version),
        attempts=tuple(attempts),
    )


def _select_candidates(root: BeautifulSoup | Tag, selectors: Sequence[str]) -> tuple[list[Tag], str | None]:
    for selector in selectors:
        matches = root.select(selector)
        if matches:
            return list(matches), selector
    return [], None


def _select_first(root: BeautifulSoup | Tag, selectors: Sequence[str]) -> tuple[Tag | None, str | None]:
    for selector in selectors:
        match = root.select_one(selector)
        if match is not None:
            return match, selector
    return None, None


def _extract_field_value(match: Tag | None, attr: str | None) -> str:
    if match is None:
        return ""
    if attr:
        raw_value = match.get(attr, "")
        if isinstance(raw_value, list):
            value = " ".join(str(part).strip() for part in raw_value if str(part).strip())
        else:
            value = str(raw_value).strip()
        return value
    return match.get_text(" ", strip=True)


def _is_better_attempt(candidate: SelectorAttempt[T], current: SelectorAttempt[T] | None) -> bool:
    if current is None:
        return True
    if candidate.items_extracted != current.items_extracted:
        return candidate.items_extracted > current.items_extracted
    if candidate.confidence != current.confidence:
        return candidate.confidence > current.confidence
    return candidate.candidate_count < current.candidate_count


def _summarize_attempts(attempts: Sequence[SelectorAttempt[object]]) -> str:
    if not attempts:
        return "none"
    return ", ".join(
        f"{attempt.version}={attempt.items_extracted}/{attempt.candidate_count}@{attempt.confidence:.2f}"
        for attempt in attempts
    )
