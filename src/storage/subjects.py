"""Subject vocabulary: YAML loader + runtime tagger.

The canonical subject list lives in ``config/subjects.yaml`` at the repo
root. It is synced into the ``subjects`` + ``subject_aliases`` tables
through :func:`sync_from_yaml`. Ingestion code tags text and structured
identifiers with :class:`SubjectTagger`, built from the DB.

See issue #2 for the vocabulary model. v1 is lean:
  * structured alias match → confidence 1.0
  * title_regex match → confidence 0.8
No disambiguation rules, no body scoring, no LLM fallback — add only when
measured false positives justify them.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable

import yaml


_DEFAULT_YAML = (
    Path(__file__).resolve().parents[2] / "config" / "subjects.yaml"
)

STRUCTURED_CONFIDENCE = 1.0
TITLE_CONFIDENCE = 0.8


def load_subjects_yaml(path: str | Path | None = None) -> list[dict[str, Any]]:
    """Parse ``config/subjects.yaml`` and return the ``subjects:`` list."""
    src = Path(path) if path else _DEFAULT_YAML
    with src.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    subjects = data.get("subjects") or []
    if not isinstance(subjects, list):
        raise ValueError(f"{src}: top-level 'subjects' must be a list")
    return subjects


def sync_from_yaml(store, path: str | Path | None = None) -> int:
    """Load the YAML and push it through ``store.sync_subjects``.

    Returns the number of subjects synced.
    """
    subjects = load_subjects_yaml(path)
    store.sync_subjects(subjects)
    return len(subjects)


class SubjectTagger:
    """Compiled view over the subject-alias tables.

    Build once per process — the regex list is small but compiling per call
    wastes work. Rebuild by constructing a new instance after a yaml re-sync.
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._conn = connection
        self._title_patterns: list[tuple[str, re.Pattern[str]]] = []
        self._exact_index: dict[tuple[str, str], str] = {}
        self._load()

    def _load(self) -> None:
        rows = self._conn.execute(
            "SELECT subject_id, alias_type, alias_value FROM subject_aliases"
        ).fetchall()
        for sid, atype, aval in rows:
            if atype == "title_regex":
                self._title_patterns.append(
                    (sid, re.compile(aval, re.IGNORECASE))
                )
            else:
                # Case-insensitive exact match for all non-regex alias types
                # (fred_series, ny_fed_series, fedwatch_series,
                # calendar_indicator, te_indicator, news_category, ...).
                self._exact_index[(atype, aval.lower())] = sid

    # ---- public API -------------------------------------------------------

    def tag_text(self, title: str | None) -> list[tuple[str, float]]:
        """Return ``(subject_id, 0.8)`` for every title_regex hit.

        Dedup on subject_id; no confidence scoring beyond the flat 0.8.
        """
        if not title:
            return []
        hits: set[str] = set()
        for sid, pat in self._title_patterns:
            if pat.search(title):
                hits.add(sid)
        return [(sid, TITLE_CONFIDENCE) for sid in sorted(hits)]

    def tag_alias(
        self, alias_type: str, values: str | Iterable[str] | None
    ) -> list[tuple[str, float]]:
        """Resolve one or more alias values of a given type to subjects.

        Returns ``(subject_id, 1.0)`` per matched subject, deduped.
        """
        if not values:
            return []
        if isinstance(values, str):
            values = [values]
        hits: set[str] = set()
        for v in values:
            if not v:
                continue
            mapped = self._exact_index.get((alias_type, v.lower()))
            if mapped:
                hits.add(mapped)
        return [(sid, STRUCTURED_CONFIDENCE) for sid in sorted(hits)]

    def tag_structured(
        self,
        *,
        fred_series: str | Iterable[str] | None = None,
        ny_fed_series: str | Iterable[str] | None = None,
        fedwatch_series: str | Iterable[str] | None = None,
        calendar_indicator: str | None = None,
        te_indicator: str | None = None,
    ) -> list[tuple[str, float]]:
        """Resolve any structured alias (confidence 1.0), deduped."""
        hits: dict[str, float] = {}
        pairs = (
            ("fred_series", fred_series),
            ("ny_fed_series", ny_fed_series),
            ("fedwatch_series", fedwatch_series),
            ("calendar_indicator", calendar_indicator),
            ("te_indicator", te_indicator),
        )
        for atype, val in pairs:
            for sid, conf in self.tag_alias(atype, val):
                hits[sid] = conf
        return sorted(hits.items())
