"""Per-domain SQL query helpers for SQLiteEngineStore.

Extracted from storage.sqlite in issue #71 Tier 2.1B-2. Each domain module
exposes a private mixin class (``_XQueriesMixin``) consumed by
``SQLiteEngineStore`` via multiple inheritance, mirroring the
``macro_data.service`` mixin layout shipped in issue #58 Tier 1.1.

Layout follows ``storage.models``: one module per domain.
"""
