"""Research-notes ingestion module.

Ported from the information-layer ``notes`` package (issue #3 item 6).
Reads YAML-frontmatter markdown files and writes them into the unified
document surface alongside news + gov reports. The markdown export step
that used to target Milvus (``6_information_layer/notes/``) is dropped
in the merge — consumers query the DB directly.
"""

from ingestion.notes.ingest import (
    NoteFrontmatter,
    ingest_notes,
    parse_note,
)

__all__ = ["NoteFrontmatter", "ingest_notes", "parse_note"]
