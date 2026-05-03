# Local Macro Data Service

`LocalMacroDataService` is the in-process facade every downstream surface
uses (HTTP server, CLI, tests, scripts). It owns no business logic — every
public method routes through `invoke(operation, params)`, which dispatches
to the matching `_op_<operation>` method via `getattr`.

The class is composed from one base + six domain mixins. Tier 1.1 of
issue #58 split a 5,566-line `service.py` into this package; method
bodies are byte-equivalent to the pre-split version and the public
import path (`from macro_data.service import LocalMacroDataService`) is
unchanged.

## Layout

```
service/
  __init__.py    — composes LocalMacroDataService from the mixins below
  base.py        — module-level helpers + LocalMacroDataServiceBase
                   (__init__, invoke, _ensure_* lazy-init helpers)
  _calendar.py   — 68 ops covering econ + corp calendars, schedules,
                   sweeps, parity, surprise, vintages, drops, fed-comm
  _timeseries.py — 23 ops for indicators, catalog, concepts,
                   release-schedule, release-status
  _documents.py  — 4 ops: list_items, get_document, subjects, backfill
  _news.py       — 5 ops: recent, search, live, article fetch
  _market.py     — 2 ops: snapshot, live_markets
  _ops_health.py — 8 ops: refresh_all, run_schedule, source listing,
                   health, alerts
```

Every mixin inherits from `LocalMacroDataServiceBase`, so each one has
direct access to `self._store`, `self.invoke`, and the `_ensure_*` lazy
slots. Mixins share no overlapping method names, so MRO order in
`__init__.py` is cosmetic — `getattr(self, f"_op_{operation}")` finds
the one matching method regardless.

## Where to add a new op

1. Identify the domain. The choice maps directly to the file:
   - Touches `cal_econ_*` / `cal_corp_*` / `cal_provider*` / parity /
     surprise / vintages / fed_comm → `_calendar.py`.
   - Touches `obs_*` / `concept_*` / `release_schedule*` /
     `indicator_*` → `_timeseries.py`.
   - Touches `document` / `doc_*` / subjects / backfill → `_documents.py`.
   - Touches `news_*` / article fetch → `_news.py`.
   - Touches `market_prices` / live market quotes → `_market.py`.
   - Cross-cutting refresh / scheduler / health / alerts →
     `_ops_health.py`.

2. Add the method as `def _op_<operation>(self, arguments: dict[str,
   Any]) -> dict[str, Any]:` inside the chosen mixin's class. The
   positional dict matches what `LocalMacroDataServiceBase.invoke`
   passes (`handler(arguments or {})`); a `**kwargs` signature would
   raise `TypeError`. Use the surrounding ops as template — they all
   return `dict[str, Any]`, surface errors as `{"error": str}`, and
   delegate state access through `self._store`.

3. The dispatch in `LocalMacroDataServiceBase.invoke` picks up the
   method automatically — no registry edit required. The method name
   minus `_op_` is the operation key downstream callers pass to
   `invoke(...)`.

4. Run the focused test for the touched domain (e.g. `pytest
   tests/test_service_calendar*.py`) plus the full unit suite
   (`pytest -m "not integration"`).

## Cross-cutting ops

A few ops legitimately touch multiple domains (e.g. `_op_list_items`
accepts a `kind` parameter that fans out to documents / indicators /
market bars). The convention is to put the op in the mixin matching its
**primary** branch and resolve the secondary branches via inherited
helpers (`self._store`, `self._event_to_dict`, etc.). `_op_list_items`
lives in `_documents.py` for that reason.

## Initialization

`LocalMacroDataServiceBase.__init__` requires a keyword-only `store`
argument; `ingestion` is an optional keyword arg that defaults to
`None`. There is no built-in default-store branch.

For most callers the right entry point is the factory in
`src/macro_data/factory.py`:

```python
from macro_data.factory import build_local_macro_data_service
service = build_local_macro_data_service(db_path=Path("..."))
```

`build_local_macro_data_service` constructs a `SQLiteEngineStore` and
an `IngestionOrchestrator` bound to that store, then wires them into a
`LocalMacroDataService`. (Issue #113 P4 retired the in-tree RAG
sidecar; downstream RAG services consume `get_document` / `list_items`
over HTTP.)

Mixins must not override `__init__`. They read `self._store`,
optionally `self._ingestion`, and call the two lazy seeders defined on
the base — `_ensure_structural_ontology` and
`_ensure_subject_vocabulary` — when an op needs the ontology or
subject vocabulary tables seeded.
