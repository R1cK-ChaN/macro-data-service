from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import logging
from typing import Any
from urllib.parse import urlparse

from contracts import format_epoch_iso

logger = logging.getLogger(__name__)

_NEWS_PRESETS: dict[str, tuple[str, ...]] = {
    "all": ("investing", "forexfactory", "tradingeconomics", "reuters", "bloomberg", "ft", "wsj"),
    "premium": ("bloomberg", "ft", "wsj", "reuters"),
    "free": ("investing", "forexfactory", "tradingeconomics"),
}

# Maps a DocumentRecord.source_id to its ingestion family tag. Exact-match
# sources are listed here; `GovReportIngestionClient` writes documents with
# derived source ids like ``us.bls`` / ``cn.stats`` (country.agency), so
# ``_document_family`` also treats any dotted source id as a gov report
# and tags it ``release_report``.
_DOCUMENT_SOURCE_FAMILY: dict[str, str] = {
    "news": "news",
    "notes": "note",
    "calendar": "calendar",
}


def _document_family(doc: Any) -> str:
    source_id = getattr(doc, "source_id", "") or ""
    fam = _DOCUMENT_SOURCE_FAMILY.get(source_id)
    if fam:
        return fam
    # Gov report source ids are derived as "country.agency" — treat any
    # dotted id as a gov release so the family filter actually matches.
    if "." in source_id:
        return "release_report"
    return ""


# Families whose rows live in the document table (as opposed to
# indicators / market_price_bars). When the caller's family filter is one
# of these, the indicator + market-bar branches are skipped; otherwise
# only those branches run and documents are skipped. ``trend`` is stored
# in ``trend_topics`` — list_items does not yet project it, so it is
# omitted here on purpose (returning documents under a `trend` filter
# would mislabel the rows).
_DOCUMENT_FAMILIES: frozenset[str] = frozenset(
    {"news", "note", "calendar", "release_report"}
)
_VALID_MARKET_ASSET_CLASSES = {"index", "commodity", "fx", "bond", "stock", "crypto"}
_VALID_RATE_TYPES = {"sofr", "effr", "obfr", "all"}
_ARTICLE_DOMAIN_MAP: dict[str, str] = {
    "bloomberg.com": "bloomberg",
    "ft.com": "ft",
    "wsj.com": "wsj",
    "reuters.com": "reuters",
}


def _detect_article_domain(url: str) -> str | None:
    hostname = urlparse(url).hostname or ""
    for domain, key in _ARTICLE_DOMAIN_MAP.items():
        if hostname == domain or hostname.endswith("." + domain):
            return key
    return None


class LocalMacroDataService:
    def __init__(
        self,
        *,
        store: Any,
        ingestion: Any | None = None,
        retriever: Any | None = None,
    ) -> None:
        self._store = store
        self._ingestion = ingestion
        self._retriever = retriever
        self._ontology_seeded = False
        self._subject_vocabulary_seeded = False

    def invoke(self, operation: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        handler = getattr(self, f"_op_{operation}", None)
        if handler is None:
            raise KeyError(f"unknown macro-data operation: {operation}")
        return handler(arguments or {})

    def _ensure_structural_ontology(self) -> None:
        if self._ontology_seeded:
            return
        seed = getattr(self._store, "seed_structural_ontology", None)
        if callable(seed):
            seed()
        self._ontology_seeded = True

    def _ensure_subject_vocabulary(self) -> None:
        """Seed subject_aliases + concept_map once per process.

        The cross-type ``list_items`` branches (indicators, market bars)
        read ``subject_aliases`` and ``concept_map``. On a DB populated
        only through time-series refresh paths neither table is filled,
        so we load the yaml vocabulary + default concept map on demand
        — both helpers are idempotent so repeat calls are cheap."""
        if self._subject_vocabulary_seeded:
            return
        try:
            from storage.subjects import sync_from_yaml
            sync_from_yaml(self._store)
        except (AttributeError, TypeError, FileNotFoundError):
            pass
        seed_cm = getattr(self._store, "seed_concept_map", None)
        if callable(seed_cm):
            try:
                seed_cm()
            except Exception:
                logger.warning("seed_concept_map failed", exc_info=True)
        self._subject_vocabulary_seeded = True

    def _op_refresh_all_sources(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        if self._ingestion is None:
            return {"error": "refresh unavailable"}
        return dict(self._ingestion.refresh_all())

    def _op_run_schedule(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        if self._ingestion is None:
            return {"error": "schedule unavailable"}
        self._ingestion.run_schedule()
        return {"scheduled": True}

    def _op_refresh_calendar(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        if self._ingestion is None:
            return {"error": "calendar refresh unavailable"}
        return dict(self._ingestion.refresh_calendar())

    # ── Economic calendar API backfill (issue #8 slice 2) ──────────────

    def _op_calendar_econ_backfill(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Plan or run a TE Calendar API backfill window.

        Arguments:
          from           — ISO date ``YYYY-MM-DD``. Default 2023-01-01.
          to             — ISO date. Default today (UTC).
          phases         — optional list of phase names
                           (``p1_recent`` / ``p2_mid`` / ``p3_early``);
                           omit to cover whichever phases the date range
                           overlaps.
          dry_run        — default True. Returns the plan + cursor state
                           without issuing any HTTP request.
          max_requests   — default 50. Caps how many windows a single
                           invocation will fetch.

        Dry-run returns ``{windows_planned, windows: [...], cursor_state}``
        with no HTTP traffic. Execute mode returns the same plus
        ``{requests_spent, rows_raw_inserted, events_upserted,
           truncated_windows, stopped_reason}``.
        """
        from datetime import date as _date, datetime as _dt, timezone as _tz

        from ingestion.calendar.te_api import BackfillRunner, TEAPIClient, plan_windows

        dry_run = bool(arguments.get("dry_run", True))
        try:
            max_requests = max(1, int(arguments.get("max_requests") or 50))
        except (TypeError, ValueError):
            max_requests = 50

        default_start = _date(2023, 1, 1)
        default_end = _dt.now(_tz.utc).date()
        try:
            start = _date.fromisoformat(str(arguments.get("from") or default_start.isoformat()))
        except ValueError:
            start = default_start
        try:
            end = _date.fromisoformat(str(arguments.get("to") or default_end.isoformat()))
        except ValueError:
            end = default_end
        raw_phases = arguments.get("phases")
        phases = None
        if isinstance(raw_phases, list) and raw_phases:
            phases = [str(p) for p in raw_phases]

        windows = plan_windows(start=start, end=end, phases=phases)
        base_payload: dict[str, Any] = {
            "dry_run": dry_run,
            "from": start.isoformat(),
            "to": end.isoformat(),
            "phases_planned": sorted({w.phase for w in windows}),
            "windows_planned": len(windows),
            "windows": [
                {"phase": w.phase, "start": w.start.isoformat(), "end": w.end.isoformat()}
                for w in windows[:10]  # preview only; full list on-demand via a later op
            ],
        }

        if dry_run:
            base_payload["stopped_reason"] = "dry_run"
            return base_payload

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        with TEAPIClient() as client:
            connection = get_conn()
            try:
                runner = BackfillRunner(
                    connection=connection,
                    client=client,
                    max_requests=max_requests,
                )
                summary = runner.run(
                    start=start, end=end, phases=phases, dry_run=False,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
        base_payload.update({
            "requests_spent":    summary.requests_spent,
            "rows_raw_inserted": summary.rows_raw_inserted,
            "events_upserted":   summary.events_upserted,
            "windows_fetched":   summary.windows_fetched,
            "truncated_windows": summary.truncated_windows,
            "budget_halt":       summary.budget_halt,
            "stopped_reason":    summary.stopped_reason,
            "cursor_state":      summary.cursor_state,
        })
        return base_payload

    # ── Corporate calendar API fetch (issue #8 slice 3) ─────────────────

    def _op_calendar_corp_fetch(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Fetch one EODHD corporate calendar subtype into ``cal_corp_*``.

        Arguments:
          subtype         — one of ``earnings``, ``ipo``, ``split``,
                            ``dividend``, ``earnings_trend``.
          from            — ISO date ``YYYY-MM-DD``. Default today (UTC).
          to              — ISO date. Default today + 7 days.
          symbols         — optional list / comma string of EODHD tickers
                            (e.g. ``["AAPL.US", "MSFT.US"]``). Required
                            for ``earnings_trend`` which has no date
                            scoping upstream.
          dry_run         — default True. Returns the window plan without
                            issuing any HTTP request.
          max_requests    — default 20. Caps windows fetched per call.
          window_days     — default 7. Window slice width in days.

        Execute mode returns request / row / insert counts and the stop
        reason.
        """
        from datetime import date as _date, datetime as _dt, timezone as _tz

        from ingestion.calendar.eodhd_api import CorpCalendarFetcher, EODHDAPIClient

        subtype = (arguments.get("subtype") or "").strip()
        if not subtype:
            return {"error": "subtype is required"}

        dry_run = bool(arguments.get("dry_run", True))
        try:
            max_requests = max(1, int(arguments.get("max_requests") or 20))
        except (TypeError, ValueError):
            max_requests = 20
        try:
            window_days = max(1, int(arguments.get("window_days") or 7))
        except (TypeError, ValueError):
            window_days = 7

        today = _dt.now(_tz.utc).date()
        default_end = today + timedelta(days=7)
        try:
            start = _date.fromisoformat(str(arguments.get("from") or today.isoformat()))
        except ValueError:
            start = today
        try:
            end = _date.fromisoformat(str(arguments.get("to") or default_end.isoformat()))
        except ValueError:
            end = default_end

        raw_symbols = arguments.get("symbols")
        symbols: list[str]
        if isinstance(raw_symbols, list):
            symbols = [str(s).strip() for s in raw_symbols if str(s).strip()]
        elif isinstance(raw_symbols, str) and raw_symbols.strip():
            symbols = [s.strip() for s in raw_symbols.split(",") if s.strip()]
        else:
            symbols = []

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        with EODHDAPIClient() as client:
            connection = get_conn()
            try:
                fetcher = CorpCalendarFetcher(
                    connection=connection,
                    client=client,
                    max_requests=max_requests,
                    window_days=window_days,
                )
                summary = fetcher.fetch(
                    subtype=subtype,
                    start=start,
                    end=end,
                    symbols=symbols or None,
                    dry_run=dry_run,
                )
                connection.commit()
            except ValueError as exc:
                connection.rollback()
                return {"error": str(exc)}
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

        return {
            "subtype":           summary.subtype,
            "dry_run":           summary.dry_run,
            "from":              start.isoformat(),
            "to":                end.isoformat(),
            "windows_planned":   summary.windows_planned,
            "windows":           summary.windows,
            "requests_spent":    summary.requests_spent,
            "rows_parsed":       summary.rows_parsed,
            "rows_raw_inserted": summary.rows_raw_inserted,
            "events_upserted":   summary.events_upserted,
            "parse_errors":      summary.parse_errors,
            "stopped_reason":    summary.stopped_reason,
        }

    def _op_calendar_corp_fetch_dividend_details(
        self, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        """Enrich discovered dividends via ``/api/div/{TICKER}.{EXCHANGE}``.

        The ``/calendar/dividends`` discovery feed returns only
        ``(symbol, ex_date)``; this op fetches the per-ticker feed that
        carries ``value``, ``currency``, and declaration / record /
        payment dates. Rows upsert the same ``cal_corp_event`` row the
        discovery parser created (identity is shared).

        Arguments:
          symbols      — list or comma string of EODHD codes
                         (``AAPL.US``, ``MSFT.US``). Required.
          from         — ISO date. Optional; when set, narrows the
                         per-ticker response to that window.
          to           — ISO date. Optional; pairs with ``from``.
          dry_run      — default True. Returns the symbol plan without
                         issuing any HTTP request.
          max_requests — default 20. Caps symbols fetched per call
                         (1 request per symbol).
        """
        from datetime import date as _date

        from ingestion.calendar.eodhd_api import EODHDAPIClient, fetch_dividend_details

        dry_run = bool(arguments.get("dry_run", True))
        try:
            max_requests = max(1, int(arguments.get("max_requests") or 20))
        except (TypeError, ValueError):
            max_requests = 20

        raw_symbols = arguments.get("symbols")
        symbols: list[str]
        if isinstance(raw_symbols, list):
            symbols = [str(s).strip() for s in raw_symbols if str(s).strip()]
        elif isinstance(raw_symbols, str) and raw_symbols.strip():
            symbols = [s.strip() for s in raw_symbols.split(",") if s.strip()]
        else:
            symbols = []
        if not symbols:
            return {"error": "symbols is required"}

        # Missing field → None (unbounded — caller wants full history).
        # Malformed field → return an error instead of silently dropping
        # the bound, which would otherwise turn a typo into an unbounded
        # /api/div/{symbol} sweep burning API budget on every symbol.
        def _parse_iso(v: Any) -> tuple[_date | None, str | None]:
            if v is None or v == "":
                return None, None
            try:
                return _date.fromisoformat(str(v)), None
            except ValueError:
                return None, str(v)

        start, bad_start = _parse_iso(arguments.get("from"))
        end, bad_end = _parse_iso(arguments.get("to"))
        if bad_start is not None:
            return {"error": f"invalid 'from' date: {bad_start!r} (expected YYYY-MM-DD)"}
        if bad_end is not None:
            return {"error": f"invalid 'to' date: {bad_end!r} (expected YYYY-MM-DD)"}

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        with EODHDAPIClient() as client:
            connection = get_conn()
            try:
                summary = fetch_dividend_details(
                    connection=connection,
                    client=client,
                    symbols=symbols,
                    start=start,
                    end=end,
                    max_requests=max_requests,
                    dry_run=dry_run,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

        return {
            "subtype":           summary.subtype,
            "dry_run":           summary.dry_run,
            "from":              start.isoformat() if start else None,
            "to":                end.isoformat() if end else None,
            "symbols_planned":   summary.windows_planned,
            "symbols":           summary.windows,
            "requests_spent":    summary.requests_spent,
            "rows_parsed":       summary.rows_parsed,
            "rows_raw_inserted": summary.rows_raw_inserted,
            "events_upserted":   summary.events_upserted,
            "parse_errors":      summary.parse_errors,
            "stopped_reason":    summary.stopped_reason,
        }

    def _op_calendar_econ_sync_updates(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Run the ``/calendar/updates`` → ``/calendarid`` reconciliation loop.

        Arguments:
          dry_run         — default True. Returns the plan shape without
                            issuing any HTTP request.
          batch_id_count  — default 30. Upper bound on ids per rehydrate
                            batch; URL-length guard still applies.

        Execute mode returns counts for updates fetched, ids rehydrated,
        batches issued, raw rows inserted, events upserted, and drops
        recorded.
        """
        from ingestion.calendar.te_api import TEAPIClient, UpdatesReconciler

        dry_run = bool(arguments.get("dry_run", True))
        try:
            batch_id_count = max(1, int(arguments.get("batch_id_count") or 30))
        except (TypeError, ValueError):
            batch_id_count = 30

        if dry_run:
            return {
                "dry_run": True,
                "batch_id_count": batch_id_count,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        with TEAPIClient() as client:
            connection = get_conn()
            try:
                reconciler = UpdatesReconciler(
                    connection=connection,
                    client=client,
                    batch_id_count=batch_id_count,
                )
                summary = reconciler.sync(dry_run=False)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
        return {
            "dry_run":             False,
            "updates_fetched":     summary.updates_fetched,
            "ids_to_rehydrate":    summary.ids_to_rehydrate,
            "batches_planned":     summary.batches_planned,
            "batches_fetched":     summary.batches_fetched,
            "rows_raw_inserted":   summary.rows_raw_inserted,
            "events_upserted":     summary.events_upserted,
            "drops_recorded":      summary.drops_recorded,
            "requests_spent":      summary.requests_spent,
            "updates_truncated":   summary.updates_truncated,
            "batch_preview":       summary.batch_preview,
            "stopped_reason":      summary.stopped_reason,
        }

    # ── Economic calendar — BLS connector (issue #9 P1) ─────────────────

    def _op_calendar_econ_fetch_bls(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch BLS Public Data API observations into the calendar.

        Arguments:
          start_year     — int. Default current year − 1.
          end_year       — int. Default current year.
          series_ids     — optional list of BLS series ids. Omit for the
                           full P1 whitelist (CPI, NFP).
          dry_run        — default True. No HTTP, no DB writes.

        Dry-run returns the plan only. Execute mode returns counts for
        observations seen, raw rows inserted, events upserted, plus
        per-series success / empty / unknown lists.
        """
        from datetime import datetime as _dt, timezone as _tz

        from ingestion.calendar.bls_api import fetch_bls_calendar
        from ingestion.timeseries.scrapers.bls import BLSClient

        dry_run = bool(arguments.get("dry_run", True))
        now_year = _dt.now(_tz.utc).year
        try:
            start_year = int(arguments.get("start_year") or (now_year - 1))
        except (TypeError, ValueError):
            start_year = now_year - 1
        try:
            end_year = int(arguments.get("end_year") or now_year)
        except (TypeError, ValueError):
            end_year = now_year
        if end_year < start_year:
            start_year, end_year = end_year, start_year

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            # Run the same resolver the execute path uses so dry-run
            # mirrors what would actually be fetched — callers must see
            # unknown ids surfaced here, not after they've triggered HTTP.
            from ingestion.calendar.bls_api.fetcher import _resolve_series
            planned, unknown = _resolve_series(series_ids)
            return {
                "dry_run":        True,
                "start_year":     start_year,
                "end_year":       end_year,
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        client = BLSClient()
        if not client.api_key:
            return {"error": "BLS_API_KEY not set"}

        connection = get_conn()
        try:
            summary = fetch_bls_calendar(
                connection, client,
                start_year=start_year, end_year=end_year,
                series_ids=series_ids, dry_run=False,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "start_year":         summary.start_year,
            "end_year":           summary.end_year,
            "series_planned":     summary.series_planned,
            "series_ok":          summary.series_ok,
            "series_empty":       summary.series_empty,
            "series_unknown":     summary.series_unknown,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "staged_skipped":     summary.staged_skipped,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    # ── Economic calendar — BLS release schedule (issue #9 P1a) ────────

    def _op_calendar_econ_schedule_bls(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape BLS release-schedule pages into ``cal_econ_event``.

        Arguments:
          series_ids     — optional list of BLS series ids. Omit for
                           the full whitelist (CPI, NFP).
          dry_run        — default True. No HTTP, no DB writes.

        Schedule rows land with ``actual=NULL`` and
        ``event_time_precision='datetime'``. The API-side
        ``calendar_econ_fetch_bls`` op later merges value-bearing
        rows onto the same ``provider_event_id`` without clobbering
        the scheduled datetime (see ``project_events`` cross-source
        merge rule).
        """
        from ingestion.calendar.bls_api import (
            SCHEDULE_URL_SLUG,
            schedule_bls_calendar,
        )

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.bls_api.fetcher import (
                _resolve_schedule_series,
            )
            planned, unknown = _resolve_schedule_series(series_ids)
            return {
                "dry_run":        True,
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_bls_calendar(
                connection, series_ids=series_ids, dry_run=False,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "series_planned":     summary.series_planned,
            "series_ok":          summary.series_ok,
            "series_unknown":     summary.series_unknown,
            "series_failed":      summary.series_failed,
            "entries_parsed":     summary.entries_parsed,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    # ── Economic calendar — BEA connector (issue #9 P2) ─────────────────

    def _op_calendar_econ_fetch_bea(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch BEA REST API observations into the calendar.

        Arguments:
          start_year     — int. Default current year − 1.
          end_year       — int. Default current year.
          series_ids     — optional list of BEA series ids. Omit for the
                           full P2 whitelist (Real GDP, PCE).
          dry_run        — default True. No HTTP, no DB writes.

        Dry-run returns the plan only. Execute mode returns counts for
        observations seen, raw rows inserted, events upserted, plus
        per-series success / empty / unknown lists.
        """
        from datetime import datetime as _dt, timezone as _tz

        from ingestion.calendar.bea_api import fetch_bea_calendar
        from ingestion.timeseries.scrapers.bea import BEAClient

        dry_run = bool(arguments.get("dry_run", True))
        now_year = _dt.now(_tz.utc).year
        try:
            start_year = int(arguments.get("start_year") or (now_year - 1))
        except (TypeError, ValueError):
            start_year = now_year - 1
        try:
            end_year = int(arguments.get("end_year") or now_year)
        except (TypeError, ValueError):
            end_year = now_year
        if end_year < start_year:
            start_year, end_year = end_year, start_year

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            # Run the same resolver the execute path uses so dry-run
            # mirrors what would actually be fetched — callers must see
            # unknown ids surfaced here, not after they've triggered HTTP.
            from ingestion.calendar.bea_api.fetcher import _resolve_series
            planned, unknown = _resolve_series(series_ids)
            return {
                "dry_run":        True,
                "start_year":     start_year,
                "end_year":       end_year,
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        client = BEAClient()
        if not client.api_key:
            return {"error": "BEA_API_KEY not set"}

        connection = get_conn()
        try:
            summary = fetch_bea_calendar(
                connection, client,
                start_year=start_year, end_year=end_year,
                series_ids=series_ids, dry_run=False,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "start_year":         summary.start_year,
            "end_year":           summary.end_year,
            "series_planned":     summary.series_planned,
            "series_ok":          summary.series_ok,
            "series_empty":       summary.series_empty,
            "series_unknown":     summary.series_unknown,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_bea(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape BEA's release calendar into ``cal_econ_event``.

        Arguments:
          dry_run — default True. No HTTP, no DB writes.

        Schedule rows land with ``actual=NULL`` and
        ``event_time_precision='datetime'``. Personal Income rows
        merge with the API-side ``calendar_econ_fetch_bea`` output
        on the shared ``provider_event_id``. GDP rows stay
        schedule-only — their ``release_stage`` folds into the id so
        the three staged releases of a quarter surface as distinct
        calendar events, which the bare-date API observation does
        not merge into (see ``api_fetch=False`` on the GDP spec).
        """
        from ingestion.calendar.bea_api import schedule_bea_calendar

        dry_run = bool(arguments.get("dry_run", True))

        if dry_run:
            return {
                "dry_run":        True,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_bea_calendar(connection, dry_run=False)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "series_ok":          summary.series_ok,
            "series_empty":       summary.series_empty,
            "entries_parsed":     summary.entries_parsed,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "row_issues":         summary.row_issues,
            "fetch_error":        summary.fetch_error,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    # ── Economic calendar — Census connector (issue #13 P1) ─────────────

    def _op_calendar_econ_fetch_census(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch Census EITS observations into the calendar."""
        from datetime import datetime as _dt, timezone as _tz

        from ingestion.calendar.census_api import (
            CensusEITSClient,
            fetch_census_calendar,
        )

        dry_run = bool(arguments.get("dry_run", True))
        now_year = _dt.now(_tz.utc).year
        try:
            start_year = int(arguments.get("start_year") or (now_year - 1))
        except (TypeError, ValueError):
            start_year = now_year - 1
        try:
            end_year = int(arguments.get("end_year") or now_year)
        except (TypeError, ValueError):
            end_year = now_year
        if end_year < start_year:
            start_year, end_year = end_year, start_year

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.census_api.fetcher import _resolve_series
            planned, unknown = _resolve_series(series_ids)
            return {
                "dry_run":        True,
                "start_year":     start_year,
                "end_year":       end_year,
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        client = CensusEITSClient()
        connection = get_conn()
        try:
            summary = fetch_census_calendar(
                connection,
                client,
                start_year=start_year,
                end_year=end_year,
                series_ids=series_ids,
                dry_run=False,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "start_year":         summary.start_year,
            "end_year":           summary.end_year,
            "series_planned":     summary.series_planned,
            "series_ok":          summary.series_ok,
            "series_empty":       summary.series_empty,
            "series_unknown":     summary.series_unknown,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "requests_made":      summary.requests_made,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_census(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape the Census economic-indicator release calendar."""
        from ingestion.calendar.census_api import schedule_census_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.census_api.fetcher import _resolve_series
            planned, unknown = _resolve_series(series_ids)
            return {
                "dry_run":        True,
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_census_calendar(
                connection,
                series_ids=series_ids,
                dry_run=False,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "series_planned":     summary.series_planned,
            "series_ok":          summary.series_ok,
            "series_empty":       summary.series_empty,
            "series_unknown":     summary.series_unknown,
            "entries_parsed":     summary.entries_parsed,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "row_issues":         summary.row_issues,
            "fetch_error":        summary.fetch_error,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    # ── Economic calendar — ISM connector (issue #13 P2) ────────────────

    def _op_calendar_econ_fetch_ism(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch the current ISM Manufacturing PMI value."""
        from ingestion.calendar.ism_api import fetch_ism_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.ism_api.fetcher import _resolve_series
            planned, unknown = _resolve_series(series_ids)
            return {
                "dry_run":        True,
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_ism_calendar(
                connection,
                series_ids=series_ids,
                dry_run=False,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "series_planned":     summary.series_planned,
            "series_ok":          summary.series_ok,
            "series_empty":       summary.series_empty,
            "series_unknown":     summary.series_unknown,
            "report_url":         summary.report_url,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_error":        summary.fetch_error,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_ism(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape the ISM Manufacturing PMI release calendar."""
        from ingestion.calendar.ism_api import schedule_ism_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.ism_api.fetcher import _resolve_series
            planned, unknown = _resolve_series(series_ids)
            return {
                "dry_run":        True,
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_ism_calendar(
                connection,
                series_ids=series_ids,
                dry_run=False,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "series_planned":     summary.series_planned,
            "series_ok":          summary.series_ok,
            "series_empty":       summary.series_empty,
            "series_unknown":     summary.series_unknown,
            "entries_parsed":     summary.entries_parsed,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "row_issues":         summary.row_issues,
            "fetch_error":        summary.fetch_error,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    # -- Economic calendar - U Michigan connector (issue #13 P3) ------

    def _op_calendar_econ_fetch_umich(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch the current U Michigan Consumer Sentiment value."""
        from ingestion.calendar.umich_api import fetch_umich_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.umich_api.fetcher import _resolve_series
            planned, unknown = _resolve_series(series_ids)
            return {
                "dry_run":        True,
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_umich_calendar(
                connection,
                series_ids=series_ids,
                dry_run=False,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "series_planned":     summary.series_planned,
            "series_ok":          summary.series_ok,
            "series_empty":       summary.series_empty,
            "series_unknown":     summary.series_unknown,
            "release_stage":      summary.release_stage,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_error":        summary.fetch_error,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_umich(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape the U Michigan Consumer Sentiment release calendar."""
        from ingestion.calendar.umich_api import schedule_umich_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]
        try:
            year = int(arguments["year"]) if arguments.get("year") else None
        except (TypeError, ValueError):
            year = None

        if dry_run:
            from ingestion.calendar.umich_api.fetcher import _resolve_series
            planned, unknown = _resolve_series(series_ids)
            return {
                "dry_run":        True,
                "series_planned": planned,
                "series_unknown": unknown,
                "year":           year,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_umich_calendar(
                connection,
                series_ids=series_ids,
                dry_run=False,
                year=year,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "series_planned":     summary.series_planned,
            "series_ok":          summary.series_ok,
            "series_empty":       summary.series_empty,
            "series_unknown":     summary.series_unknown,
            "year":               summary.year,
            "source_url":         summary.source_url,
            "entries_parsed":     summary.entries_parsed,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_error":        summary.fetch_error,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    # ── Economic calendar — ECB connector (issue #9 P3) ─────────────────

    def _op_calendar_econ_fetch_ecb(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch ECB Data Portal SDMX observations into the calendar.

        Arguments:
          start_period   — optional ISO date (``"YYYY-MM-DD"``) lower
                           bound. Omit to fetch the full available history.
          end_period     — optional ISO date upper bound.
          series_ids     — optional list of ECB SDMX series ids. Omit
                           for the full P3 whitelist (MRO / DFR / MLF).
          dry_run        — default True. No HTTP, no DB writes.
          limit          — optional ``lastNObservations`` cap. ``0``
                           means uncapped (default).

        Dry-run returns the plan only. Execute mode returns counts for
        observations seen, raw rows inserted, events upserted, plus
        per-series success / empty / unknown lists.
        """
        from ingestion.calendar.ecb_api import fetch_ecb_calendar
        from ingestion.timeseries.sdmx.providers.ecb import ECBClient

        dry_run = bool(arguments.get("dry_run", True))
        start_period = arguments.get("start_period")
        end_period = arguments.get("end_period")
        start_period = str(start_period) if start_period else None
        end_period = str(end_period) if end_period else None

        try:
            limit = int(arguments.get("limit") or 0)
        except (TypeError, ValueError):
            limit = 0
        if limit < 0:
            limit = 0

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            # Dry-run mirrors execute-path resolution so unknown ids
            # surface here rather than after HTTP is triggered.
            from ingestion.calendar.ecb_api.fetcher import _resolve_series
            planned, unknown = _resolve_series(series_ids)
            return {
                "dry_run":        True,
                "start_period":   start_period or "",
                "end_period":     end_period or "",
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        client = ECBClient()
        connection = get_conn()
        try:
            summary = fetch_ecb_calendar(
                connection, client,
                start_period=start_period, end_period=end_period,
                series_ids=series_ids, dry_run=False, limit=limit,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":                   False,
            "start_period":              summary.start_period,
            "end_period":                summary.end_period,
            "series_planned":            summary.series_planned,
            "series_ok":                 summary.series_ok,
            "series_empty":              summary.series_empty,
            "series_unknown":            summary.series_unknown,
            "observations_seen":         summary.observations_seen,
            "rows_raw_inserted":         summary.rows_raw_inserted,
            "events_upserted":           summary.events_upserted,
            "decision_anchor_fallbacks": summary.decision_anchor_fallbacks,
            "wall_seconds":              round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_ecb(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape ECB press-calendar pages into ``cal_econ_event``.

        Arguments:
          dry_run — default True. No HTTP, no DB writes.

        Scrapes two ECB calendar surfaces in one run:
        Governing Council monetary-policy meetings and Economic
        Bulletin publications. Each lands as a schedule-only
        indicator (``ECB Monetary Policy Decision`` / ``ECB
        Economic Bulletin``) — distinct from the SDMX rate-level
        rows because the SDMX lane carries the rate's effective
        date, not the decision date. Schedule rows land with
        ``actual=NULL`` / ``event_time_precision='datetime'``.

        ``fetch_errors`` on the return dict surfaces per-page
        upstream failures — one page failing doesn't stop the
        other from landing.
        """
        from ingestion.calendar.ecb_api import schedule_ecb_calendar

        dry_run = bool(arguments.get("dry_run", True))

        if dry_run:
            return {
                "dry_run":        True,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_ecb_calendar(connection, dry_run=False)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":              False,
            "mp_decision_entries":  summary.mp_decision_entries,
            "bulletin_entries":     summary.bulletin_entries,
            "entries_parsed":       summary.entries_parsed,
            "rows_raw_inserted":    summary.rows_raw_inserted,
            "events_upserted":      summary.events_upserted,
            "row_issues":           summary.row_issues,
            "fetch_errors":         summary.fetch_errors,
            "wall_seconds":         round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_fed(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape FOMC meeting calendar into the calendar schema.

        Arguments:
          dry_run        — default True. No HTTP, no DB writes.

        Dry-run returns the indicator plan only. Execute mode fetches
        ``federalreserve.gov/monetarypolicy/fomccalendars.htm`` once,
        parses each ``<div class="row fomc-meeting">`` into a
        FOMC_RATE event, and upserts via the shared merge-rule
        projector. Returns counts for meetings parsed, raw rows
        inserted, and events upserted.
        """
        from ingestion.calendar.fed_api import (
            INDICATOR_REGISTRY,
            fetch_fed_calendar,
        )

        dry_run = bool(arguments.get("dry_run", True))

        if dry_run:
            return {
                "dry_run":            True,
                "indicators_planned": ["FOMC_RATE"],
                "stopped_reason":     "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_fed_calendar(connection, dry_run=False)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "indicators_planned": summary.indicators_planned,
            "meetings_parsed":    summary.meetings_parsed,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_fed_releases(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Consume ``/json/calendar.json`` into the calendar schema.

        Arguments:
          dry_run — default True. No HTTP, no DB writes.

        Complements ``calendar_econ_fetch_fed`` (the FOMC meeting
        scrape) with the Fed's news-release schedule — Beige Book,
        H.4.1, H.8. SEP events are filtered out (they ride as a
        boolean on the FOMC event). Scheduled speeches and testimony
        are out of scope.
        """
        from ingestion.calendar.fed_api import fetch_fed_releasedates

        dry_run = bool(arguments.get("dry_run", True))

        if dry_run:
            return {
                "dry_run":            True,
                "indicators_planned": [
                    "BEIGE_BOOK", "FED_H41", "FED_H8",
                ],
                "stopped_reason":     "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_fed_releasedates(connection, dry_run=False)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":              False,
            "indicators_planned":   summary.indicators_planned,
            "entries_parsed":       summary.entries_parsed,
            "entries_by_indicator": summary.entries_by_indicator,
            "rows_raw_inserted":    summary.rows_raw_inserted,
            "events_upserted":      summary.events_upserted,
            "row_issues":           summary.row_issues,
            "fetch_error":          summary.fetch_error,
            "wall_seconds":         round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_fed_values(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape FOMC statement pages and fill ``actual`` on existing rows.

        Arguments:
          dry_run       — default True. No HTTP, no DB writes.
          closing_dates — optional list of ISO closing dates
                          (``["2025-01-29"]``). When omitted, the op
                          auto-discovers past FOMC meetings from
                          ``cal_econ_event`` whose ``actual`` is still
                          NULL.

        The ``provider_event_id`` written by this op matches the one
        from :func:`calendar_econ_fetch_fed` exactly (same closing-date
        ISO anchor), so the target-range value upserts onto the
        existing schedule row via the shared projector's merge CASE.
        """
        from datetime import date
        from ingestion.calendar.fed_api import fetch_fed_statement_values

        dry_run = bool(arguments.get("dry_run", True))
        raw_closings = arguments.get("closing_dates") or []
        closing_dates: list[date] | None
        if raw_closings:
            closing_dates = [date.fromisoformat(str(d)) for d in raw_closings]
        else:
            closing_dates = None

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_fed_statement_values(
                connection,
                dry_run=dry_run,
                closing_dates=closing_dates,
            )
            if not dry_run:
                connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            summary.dry_run,
            "indicators_planned": summary.indicators_planned,
            "meetings_planned":   summary.meetings_planned,
            "meetings_fetched":   summary.meetings_fetched,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_failures":     summary.fetch_failures,
            "parse_failures":     summary.parse_failures,
            "stopped_reason":     "dry_run" if dry_run else None,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    # ── Cross-connector recurring refresh (issue #9 P-sched-1 / P-sched-2) ──

    def _op_calendar_econ_sweep_values(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Invoke every value-side connector to fill ``actual`` on recent rows.

        Frequent-cron candidate — paired with
        :func:`calendar_econ_refresh_schedules` for the full P-sched
        coverage. The schedule-side refresh runs once daily (cheap,
        forward-looking); this sweep runs every few minutes so a
        just-published release crosses into ``cal_econ_event`` with
        ``actual`` populated within minutes of publication.

        Arguments:
          dry_run      — default True. No HTTP, no DB writes.
          connectors   — optional list subset of
                         ``["bls", "bea", "census", "ism", "umich", "ecb", "fed-values"]``.
          start_year   — optional int; default ``current_year − 1``.
                         Applied to BLS / BEA / Census.
          end_year     — optional int; default current year. BLS / BEA / Census.
          start_period — optional SDMX period string (ECB only).
          end_period   — optional SDMX period string (ECB only).

        NBS is not in the plan — the yearly calendar scraper is
        schedule-only; per-release value scraping for NBS indicators
        is a future slice.
        """
        from ingestion.calendar.scheduler import (
            ALL_VALUE_SIDE_CONNECTORS,
            sweep_value_side,
        )

        dry_run = bool(arguments.get("dry_run", True))
        raw_connectors = arguments.get("connectors")
        connectors: list[str] | None
        if raw_connectors is None:
            # Key absent → fall through to the full default plan.
            connectors = None
        elif isinstance(raw_connectors, list):
            # Key present — preserve the list shape so an explicit
            # empty ``[]`` means "run nothing" rather than being
            # silently promoted to the full plan.
            connectors = [str(c) for c in raw_connectors]
        else:
            connectors = None

        def _opt_int(key: str) -> int | None:
            raw = arguments.get(key)
            if raw is None or raw == "":
                return None
            try:
                return int(raw)
            except (TypeError, ValueError):
                return None

        def _opt_str(key: str) -> str | None:
            raw = arguments.get(key)
            if raw is None:
                return None
            text = str(raw).strip()
            return text or None

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        summary = sweep_value_side(
            get_conn,
            dry_run=dry_run,
            start_year=_opt_int("start_year"),
            end_year=_opt_int("end_year"),
            start_period=_opt_str("start_period"),
            end_period=_opt_str("end_period"),
            connectors=connectors,
        )

        return {
            "dry_run":             summary.dry_run,
            "connectors_planned":  summary.connectors_planned,
            "connectors_all":      list(ALL_VALUE_SIDE_CONNECTORS),
            "unknown_connectors":  summary.unknown_connectors,
            "ok_count":            summary.ok_count,
            "failed_count":        summary.failed_count,
            "results": [
                {
                    "connector":    r.connector,
                    "ok":           r.ok,
                    "error":        r.error,
                    "summary":      r.summary,
                    "wall_seconds": r.wall_seconds,
                }
                for r in summary.results
            ],
            "stopped_reason":      "dry_run" if dry_run else None,
            "wall_seconds":        round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_refresh_schedules(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Invoke every official-source connector's schedule scrape.

        Daily-cron candidate for the recurring-fetch scheduler
        (:mod:`ingestion.calendar.scheduler`). Iterates BLS + BEA +
        Census + ISM + U Michigan + ECB + Fed FOMC + Fed releasedates + NBS in order, isolating
        per-connector exceptions so one upstream outage (ECB 502, NBS
        timeout, …) doesn't roll back the rest. Each connector gets
        its own connection / commit / rollback lifecycle.

        Arguments:
          dry_run    — default True. No HTTP, no DB writes.
          connectors — optional list subset of
                       ``["bls","bea","census","ism","umich","ecb","fed-fomc","fed-releases","nbs"]``;
                       omit to run the full roster.

        This slice ships only the schedule-side aggregator. The
        value-side sweep (triggered after each expected release
        crosses its scheduled time), budget guards / circuit breakers,
        and health-telemetry wiring are follow-up slices under the
        P-sched umbrella.
        """
        from ingestion.calendar.scheduler import (
            ALL_CONNECTORS,
            refresh_all_schedules,
        )

        dry_run = bool(arguments.get("dry_run", True))
        raw_connectors = arguments.get("connectors")
        connectors: list[str] | None
        if raw_connectors is None:
            # Key absent → fall through to the full default plan.
            connectors = None
        elif isinstance(raw_connectors, list):
            # Key present — preserve the list shape so an explicit
            # empty ``[]`` means "run nothing" rather than being
            # silently promoted to the full plan.
            connectors = [str(c) for c in raw_connectors]
        else:
            connectors = None

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        summary = refresh_all_schedules(
            get_conn,
            dry_run=dry_run,
            connectors=connectors,
        )

        return {
            "dry_run":             summary.dry_run,
            "connectors_planned":  summary.connectors_planned,
            "connectors_all":      list(ALL_CONNECTORS),
            "unknown_connectors":  summary.unknown_connectors,
            "ok_count":            summary.ok_count,
            "failed_count":        summary.failed_count,
            "results": [
                {
                    "connector":    r.connector,
                    "ok":           r.ok,
                    "error":        r.error,
                    "summary":      r.summary,
                    "wall_seconds": r.wall_seconds,
                }
                for r in summary.results
            ],
            "stopped_reason":      "dry_run" if dry_run else None,
            "wall_seconds":        round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_parity(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Compare TE and official-source rows in ``cal_econ_event``.

        Shipped with issue #9 P6. Buckets rows by
        ``(country, canonicalize_indicator(title), reference_date)``
        inside the caller's ``(from_date, to_date)`` window and
        reports per-indicator match coverage against the TE provider.
        TE-only gaps are actionable (scheduler missed a release);
        official-only rows are TE blind spots (documented but not a
        regression — see the issue #9 P6 NBS caveat).

        Arguments:
          from_date   — required ISO date / datetime, inclusive lower
                        bound on ``event_time_utc``.
          to_date     — required ISO date / datetime, inclusive upper.
          indicators  — optional list of canonical tokens
                        (``["CPI", "NFP"]``). Omit to cover everything
                        in-window that canonicalizes to a non-empty
                        token.
          write_report — optional bool. When True the markdown report
                        is also written to
                        ``docs/validation/calendar_parity_<YYYY-MM-DD>.md``.
                        Default False — the op returns the report
                        string so the caller can decide where to
                        persist.

        Returns a JSON-serializable envelope carrying totals,
        per-indicator breakdown, the TE-only / official-only lists
        (truncated-neutral — full lists, no cap at the service
        boundary), and the rendered markdown report.
        """
        from ingestion.calendar.parity import (
            OFFICIAL_PROVIDERS,
            TE_PROVIDER,
            calendar_econ_parity,
            format_parity_report,
        )

        from_date = str(arguments.get("from_date") or "").strip()
        to_date = str(arguments.get("to_date") or "").strip()
        if not from_date or not to_date:
            return {"error": "from_date and to_date are required"}

        raw_indicators = arguments.get("indicators")
        indicators: list[str] | None
        if raw_indicators is None:
            indicators = None
        elif isinstance(raw_indicators, list):
            indicators = [str(ind) for ind in raw_indicators]
        else:
            indicators = None

        write_report = bool(arguments.get("write_report", False))

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        # Use ``contextlib.closing`` so the connection is released on
        # the usual service-op cadence — Connection.__exit__ commits
        # but does not close, and this op runs on operator demand, so
        # leaving handles open across invocations would compound.
        from contextlib import closing
        with closing(get_conn()) as connection:
            summary = calendar_econ_parity(
                connection,
                from_date=from_date,
                to_date=to_date,
                indicators=indicators,
            )

        report_markdown = format_parity_report(summary)
        report_path: str | None = None
        if write_report:
            from pathlib import Path
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            docs_dir = Path("docs/validation")
            docs_dir.mkdir(parents=True, exist_ok=True)
            target = docs_dir / f"calendar_parity_{today}.md"
            target.write_text(report_markdown, encoding="utf-8")
            report_path = str(target)

        return {
            "from_date":            summary.from_date,
            "to_date":              summary.to_date,
            "te_provider":          TE_PROVIDER,
            "official_providers":   list(OFFICIAL_PROVIDERS),
            "total_events":         summary.total_events,
            "matched":              summary.matched,
            "te_only_count":        summary.te_only_count,
            "official_only_count":  summary.official_only_count,
            "match_percentage":     summary.match_percentage,
            "indicators": [
                {
                    "country":              ind.country_code,
                    "canonical_indicator":  ind.canonical_indicator,
                    "total_events":         ind.total_events,
                    "matched":              ind.matched,
                    "te_only":              ind.te_only,
                    "official_only":        ind.official_only,
                    "match_percentage":     ind.match_percentage,
                }
                for ind in summary.indicators
            ],
            "te_only_events": [
                self._parity_event_dict(e) for e in summary.te_only_events
            ],
            "official_only_events": [
                self._parity_event_dict(e) for e in summary.official_only_events
            ],
            "report_markdown":  report_markdown,
            "report_path":      report_path,
        }

    @staticmethod
    def _parity_event_dict(event: Any) -> dict[str, Any]:
        return {
            "provider":             event.provider,
            "provider_event_id":    event.provider_event_id,
            "country_code":         event.country_code,
            "canonical_indicator":  event.canonical_indicator,
            "reference_date":       event.reference_date,
            "title":                event.title,
            "event_time_utc":       event.event_time_utc,
        }

    def _op_list_calendar_items(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """List calendar items from the unified ``v_calendar_item`` view.

        Shipped with issue #9 P7 so downstream consumers can query the
        calendar without importing from ``src/`` or reading
        ``engine.db`` directly.

        Arguments:
          domain       — optional ``'economic'`` / ``'corporate'``.
          country      — optional ISO-3166 alpha-2 (economic rows only).
          ticker       — optional symbol (corporate rows only).
          subtype      — optional ``'release'`` / ``'dividend'`` / … .
          provider     — optional ``cal_provider.provider_id``.
          page_offset  — default 0. Rows to skip before the first result.
          page_limit   — default 100; capped at 500.

        Returns a JSON:API-shaped envelope:

            {"data": [...CalendarItem dicts...],
             "meta": {"count": total, "offset": X, "limit": Y},
             "links": {"next": {"page_offset": …, "page_limit": …} or null}}

        ``links.next`` carries the cursor for the next page when more
        rows match the filter; ``null`` otherwise. Cursor keys are
        the same names this op reads (``page_offset`` / ``page_limit``)
        so a client can spread the cursor back in as arguments and
        the next call Just Works; they're also usable as query-string
        pairs on ``GET /v1/calendar`` after the conventional
        ``page[offset]`` → ``page_offset`` mapping the HTTP handler
        performs.
        """
        domain = (arguments.get("domain") or "").strip() or None
        country = (arguments.get("country") or "").strip() or None
        ticker = (arguments.get("ticker") or "").strip() or None
        subtype = (arguments.get("subtype") or "").strip() or None
        provider = (arguments.get("provider") or "").strip() or None
        try:
            offset = int(arguments.get("page_offset") or 0)
        except (TypeError, ValueError):
            offset = 0
        try:
            limit = int(arguments.get("page_limit") or 100)
        except (TypeError, ValueError):
            limit = 100
        offset = max(0, offset)
        limit = max(1, min(500, limit))

        lister = getattr(self._store, "list_calendar_items", None)
        if not callable(lister):
            return {"error": "store does not expose list_calendar_items"}

        items, total = lister(
            domain=domain,
            country=country,
            ticker=ticker,
            subtype=subtype,
            provider=provider,
            offset=offset,
            limit=limit,
        )
        next_cursor: dict[str, int] | None = None
        if offset + len(items) < total:
            next_cursor = {
                "page_offset": offset + len(items),
                "page_limit":  limit,
            }
        return {
            "data":  items,
            "meta":  {"count": total, "offset": offset, "limit": limit},
            "links": {"next": next_cursor},
        }

    def _op_calendar_econ_fetch_nbs(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape an NBS yearly-calendar article into the calendar schema.

        Arguments:
          calendar_url   — optional. Full URL of an NBS yearly-calendar
                           article. Required unless ``auto_discover``
                           is ``true``.
          auto_discover  — default False. When ``true`` and
                           ``calendar_url`` is omitted, the fetcher
                           resolves the article URL for ``year`` (or
                           the current UTC year) by scraping the NBS
                           release-calendar index page. Explicit
                           opt-in keeps accidental network calls out
                           of caller loops that forgot to pass the
                           URL.
          year           — optional integer year override; the parser
                           defaults to reading it from the article title.
                           Used for index-page lookup when
                           ``auto_discover`` is ``true``.
          dry_run        — default True. No HTTP, no DB writes.

        Dry-run returns the indicator plan. Execute mode hits the URL
        (or discovers it when ``auto_discover=true``), parses the
        schedule table, and lands each scheduled release as a
        ``cal_econ_event`` row with ``actual=NULL`` /
        ``event_time_precision='datetime'``.
        """
        from ingestion.calendar.nbs_api import (
            INDICATOR_REGISTRY,
            fetch_nbs_calendar,
        )

        dry_run = bool(arguments.get("dry_run", True))
        calendar_url = arguments.get("calendar_url") or None
        auto_discover = bool(arguments.get("auto_discover", False))
        year_raw = arguments.get("year")
        try:
            year = int(year_raw) if year_raw is not None else None
        except (TypeError, ValueError):
            year = None

        if dry_run:
            return {
                "dry_run":            True,
                "indicators_planned": list(INDICATOR_REGISTRY.keys()),
                "calendar_url":       calendar_url or "",
                "auto_discover":      auto_discover,
                "year":               year,
                "stopped_reason":     "dry_run",
            }

        if not calendar_url and not auto_discover:
            return {
                "error": (
                    "calendar_url is required unless auto_discover=true"
                ),
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_nbs_calendar(
                connection,
                calendar_url=calendar_url,
                year=year,
                dry_run=False,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":              False,
            "indicators_planned":   summary.indicators_planned,
            "calendar_url":         summary.calendar_url,
            "year":                 summary.year,
            "url_auto_discovered":  summary.url_auto_discovered,
            "entries_parsed":       summary.entries_parsed,
            "rows_raw_inserted":    summary.rows_raw_inserted,
            "events_upserted":      summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    # ── Unified document queries (issue #2 / #3) ────────────────────────

    def _op_list_items(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Merged feed across the unified surface: documents + indicator
        observations + market-price bars, all filterable by ``subject``.

        Arguments:
          subject         — subject_id to filter on (e.g. "econ.cpi")
          q               — free-text query (FTS5 over title + body;
                            applies to documents only)
          family          — optional family filter (e.g. "economic_data",
                            "market_price", "news"). When unset, rows
                            from every family are returned.
          min_confidence  — default 0.0; filters item_subjects rows
          limit           — default 50, capped at 500
          document_type   — optional exact match (e.g. "report")
          country_code    — optional 2-letter ISO filter

        Each item carries a ``family`` and ``kind`` tag so callers can
        dispatch without a second lookup. Documents keep the existing
        summary shape; indicator / market-bar rows carry only the
        columns relevant to their type.
        """
        subject = (arguments.get("subject") or "").strip() or None
        query = (arguments.get("q") or "").strip() or None
        family_filter = (arguments.get("family") or "").strip() or None
        document_type = (arguments.get("document_type") or "").strip() or None
        country_code = (arguments.get("country_code") or "").strip() or None
        try:
            limit = int(arguments.get("limit") or 50)
        except (TypeError, ValueError):
            limit = 50
        limit = max(1, min(limit, 500))
        raw_conf = arguments.get("min_confidence")
        try:
            min_conf = float(raw_conf) if raw_conf is not None else 0.0
        except (TypeError, ValueError):
            min_conf = 0.0

        # Subject-driven branches need the yaml vocabulary + default
        # concept_map loaded. Both helpers are idempotent.
        if subject:
            self._ensure_subject_vocabulary()

        items: list[dict[str, Any]] = []

        # Documents branch. Non-document filters (document_type /
        # country_code) only make sense here — indicator + market-bar
        # rows carry no such metadata, so including them when the
        # caller asked for `document_type="report"` would violate the
        # filter contract.
        want_documents = family_filter is None or family_filter in _DOCUMENT_FAMILIES
        if want_documents:
            # The family predicate runs in SQL now (see
            # SQLiteEngineStore._family_predicate) so the server-side
            # limit bounds the matching document rows directly — no
            # widen-then-post-filter sleight of hand.
            candidates = self._store.list_items_combined(
                subject_id=subject,
                query=query,
                limit=limit,
                min_confidence=min_conf,
                document_type=document_type,
                country_code=country_code,
                family=family_filter if family_filter in _DOCUMENT_FAMILIES else None,
            )
            for doc in candidates:
                summary = self._document_summary(doc)
                summary["family"] = _document_family(doc)
                summary["kind"] = "document"
                items.append(summary)

        # Indicator + market-bar branches only run when the caller is
        # asking by subject. Without a subject the join chain
        # (subject_aliases → concept_map / market_instruments) can't
        # produce meaningful rows, and widening the branches to "all
        # indicators" would explode the result set.
        if subject and not query and not document_type and not country_code:
            if family_filter in (None, "economic_data"):
                items.extend(
                    self._store.list_subject_indicators(subject, limit=limit)
                )
            if family_filter in (None, "market_price"):
                items.extend(
                    self._store.list_subject_market_bars(subject, limit=limit)
                )

        # When a family filter is set, only one branch ran and its own
        # SQL LIMIT has already capped the result to `limit`. Without a
        # filter, every branch returns up to `limit` rows so the
        # envelope can carry up to ``3 * limit`` — intentional, since
        # the callers asked for cross-type visibility and dropping the
        # later branches to fit a global `limit` would re-introduce the
        # crowding bug Codex flagged.
        if family_filter:
            items = items[:limit]
        return {"total": len(items), "items": items}

    def _op_get_document(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Fetch one document (metadata + markdown body + subject tags).

        Accepts either ``document_id`` (internal TEXT id) or
        ``hash_sha256`` (content hash stored on the row).
        """
        document_id = (arguments.get("document_id") or "").strip()
        sha = (arguments.get("hash_sha256") or "").strip()
        if not document_id and not sha:
            return {"error": "document_id or hash_sha256 is required"}
        doc = (
            self._store.get_document(document_id)
            if document_id
            else self._store.get_document_by_sha(sha)
        )
        if doc is None:
            return {"document": None}
        body = self._store.get_document_body(doc.document_id)
        subjects = self._store.list_document_subjects(doc.document_id)
        return {
            "document": self._document_summary(doc),
            "body": body,
            "subjects": [{"subject_id": s, "confidence": c} for s, c in subjects],
        }

    def _op_list_subjects(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """List the subject vocabulary. Seeds the yaml on first call so
        the response is always complete on a fresh DB."""
        del arguments
        try:
            from storage.subjects import sync_from_yaml
            sync_from_yaml(self._store)
        except (AttributeError, TypeError, FileNotFoundError):
            pass  # best-effort; yaml may be unavailable in test environments
        return {"subjects": self._store.list_subjects()}

    def _op_backfill_document_indexes(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Run the one-shot FTS + subject-tag backfills.

        Needed on DBs that accumulated ``document`` rows before Step 2
        added ``documents_fts`` and Step 3 started calling
        ``set_document_subjects`` at ingest. Both helpers are idempotent,
        so calling this on a fresh DB does no work.
        """
        del arguments
        # Ensure the vocabulary is seeded before tagging runs — otherwise
        # the tagger sees an empty alias list and nothing gets tagged.
        try:
            from storage.subjects import sync_from_yaml
            sync_from_yaml(self._store)
        except (AttributeError, TypeError, FileNotFoundError):
            pass
        fts_written = self._store.backfill_documents_fts()
        subjects_tagged = self._store.backfill_document_subjects()
        return {
            "fts_rows_written": fts_written,
            "documents_subject_tagged": subjects_tagged,
        }

    @staticmethod
    def _document_summary(doc: Any) -> dict[str, Any]:
        """Shape a DocumentRecord for API responses — omit internal-only
        fields (epoch_ms duplicates, release_family_id) to keep the
        payload lean."""
        return {
            "document_id": doc.document_id,
            "hash_sha256": doc.hash_sha256,
            "title": doc.title,
            "subtitle": doc.subtitle,
            "source_id": doc.source_id,
            "document_type": doc.document_type,
            "country_code": doc.country_code,
            "language_code": doc.language_code,
            "topic_code": doc.topic_code,
            "published_date": doc.published_date,
            "published_at": doc.published_at,
            "institution": doc.institution,
            "authors": doc.authors,
            "data_period": doc.data_period,
            "market": doc.market,
            "asset_class": doc.asset_class,
            "sector": doc.sector,
            "event_type": doc.event_type,
            "impact_level": doc.impact_level,
            "contains_commentary": doc.contains_commentary,
            "confidence": doc.confidence,
            "subject_freetext": doc.subject_freetext,
            "canonical_url": doc.canonical_url,
        }

    def _op_refresh_news(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "news refresh unavailable"}
        category = arguments.get("category")
        return dict(self._ingestion.refresh_news(category=category))

    def _op_refresh_source(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "refresh unavailable"}
        source = str(arguments.get("source") or "").strip()
        if not source:
            return {"error": "source is required"}
        try:
            return self._ingestion.run_source(source).to_dict()
        except KeyError:
            return {
                "error": f"unknown source: {source}",
                "available_sources": self._ingestion.list_sources(),
            }

    def _op_list_sources(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Return registered ingestion sources as ``[{name, family}, ...]``.

        Optional ``family`` filter narrows to a single family tag.
        """
        if self._ingestion is None:
            return {"error": "sources unavailable", "sources": []}
        family = (arguments.get("family") or "").strip() or None
        sources = self._ingestion.list_sources()
        if family:
            sources = [s for s in sources if s.get("family") == family]
        return {"total": len(sources), "sources": sources}

    def _op_list_source_capabilities(self, arguments: dict[str, Any]) -> dict[str, Any]:
        include_internal = bool(arguments.get("include_internal", False))
        if self._ingestion is None:
            return {"error": "capabilities unavailable", "sources": []}
        items = self._ingestion.list_source_capabilities(include_internal=include_internal)
        return {"total": len(items), "sources": items}

    def _op_get_source_health_dashboard(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "source health unavailable", "sources": []}
        include_internal = bool(arguments.get("include_internal", False))
        return self._ingestion.get_source_health_dashboard(include_internal=include_internal)

    def _op_sync_catalog_discovery(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "catalog discovery unavailable"}
        source_id = str(arguments.get("source_id") or arguments.get("source") or "").strip()
        if not source_id:
            return {"error": "source_id is required"}
        query = (arguments.get("query") or "").strip() or None
        limit = arguments.get("limit")
        return self._ingestion.sync_catalog_discovery(
            source_id,
            query=query,
            limit=int(limit) if limit is not None else None,
        )

    def _op_list_catalog_entities(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "catalog listing unavailable", "entities": []}
        source_id = str(arguments.get("source_id") or arguments.get("source") or "").strip()
        if not source_id:
            return {"error": "source_id is required", "entities": []}
        query = (arguments.get("query") or "").strip() or None
        limit = int(arguments.get("limit", 100))
        refresh = bool(arguments.get("refresh", False))
        return self._ingestion.list_catalog_entities(
            source_id,
            query=query,
            limit=limit,
            refresh=refresh,
        )

    def _op_get_catalog_structure(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "catalog structure unavailable", "structure": None}
        source_id = str(arguments.get("source_id") or arguments.get("source") or "").strip()
        entity_id = str(arguments.get("entity_id") or arguments.get("entity") or "").strip()
        if not source_id:
            return {"error": "source_id is required", "structure": None}
        if not entity_id:
            return {"error": "entity_id is required", "structure": None}
        return self._ingestion.get_catalog_structure(source_id, entity_id)

    def _op_sync_catalog_latest(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "catalog sync unavailable"}
        source_id = str(arguments.get("source_id") or arguments.get("source") or "").strip()
        if not source_id:
            return {"error": "source_id is required"}
        entity_ids = arguments.get("entity_ids")
        if entity_ids is None:
            entity = (arguments.get("entity_id") or arguments.get("entity") or "").strip()
            entity_ids = [entity] if entity else None
        limit = arguments.get("limit")
        return self._ingestion.sync_catalog_latest(
            source_id,
            entity_ids=entity_ids,
            limit=int(limit) if limit is not None else None,
        )

    def _op_get_catalog_status(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "catalog status unavailable", "sources": []}
        source_id = (arguments.get("source_id") or arguments.get("source") or "").strip() or None
        include_internal = bool(arguments.get("include_internal", False))
        return self._ingestion.get_catalog_status(source_id, include_internal=include_internal)

    def _op_refresh_indicator(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "ingestion unavailable"}
        concept_id = (arguments.get("concept_id") or "").strip()
        if not concept_id:
            return {"error": "concept_id is required"}
        lookback_days = int(arguments.get("lookback_days", 365 * 3))
        report = self._ingestion.refresh_indicator(
            concept_id, lookback_days=lookback_days,
        )
        return report.to_dict()

    def _op_validate_concept(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from ingestion.validation import ValidationEngine, ValidationStore

        concept_id = (arguments.get("concept_id") or "").strip()
        if not concept_id:
            return {"error": "concept_id is required"}
        max_staleness = int(arguments.get("max_staleness_days", 90))
        tolerance = float(arguments.get("value_tolerance_pct", 1.0))
        lookback = int(arguments.get("lookback_periods", 12))

        db_path = getattr(self._store, "db_path", ".macro-data/engine.db")
        validation_store = ValidationStore(str(db_path))
        engine = ValidationEngine(validation_store)
        self._store.seed_concept_map()

        report = engine.validate_concept(
            concept_id,
            self._store,
            max_staleness_days=max_staleness,
            value_tolerance_pct=tolerance,
            lookback_periods=lookback,
        )
        return report.to_dict()

    def _op_validate_all_concepts(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from ingestion.validation import ValidationEngine, ValidationStore

        country_code = (arguments.get("country_code") or "").strip() or None
        max_staleness = int(arguments.get("max_staleness_days", 90))
        tolerance = float(arguments.get("value_tolerance_pct", 1.0))
        lookback = int(arguments.get("lookback_periods", 12))

        db_path = getattr(self._store, "db_path", ".macro-data/engine.db")
        validation_store = ValidationStore(str(db_path))
        engine = ValidationEngine(validation_store)
        self._store.seed_concept_map()

        reports = engine.validate_all_concepts(
            self._store,
            max_staleness_days=max_staleness,
            value_tolerance_pct=tolerance,
            lookback_periods=lookback,
            country_code=country_code,
        )
        return {
            "total_concepts": len(reports),
            "passed": sum(1 for r in reports if r.passed),
            "failed": sum(1 for r in reports if not r.passed),
            "reports": [r.to_dict() for r in reports],
        }

    def _op_resolve_indicator(self, arguments: dict[str, Any]) -> dict[str, Any]:
        concept_id = (arguments.get("concept_id") or "").strip()
        if not concept_id:
            return {"error": "concept_id is required"}
        self._store.seed_concept_map()
        date = (arguments.get("date") or "").strip() or None
        obs = self._store.resolve_indicator(concept_id, date=date)
        if obs is None:
            return {"resolved": None, "concept_id": concept_id}
        from dataclasses import asdict
        return {"resolved": asdict(obs)}

    def _op_resolve_indicator_history(self, arguments: dict[str, Any]) -> dict[str, Any]:
        concept_id = (arguments.get("concept_id") or "").strip()
        if not concept_id:
            return {"error": "concept_id is required"}
        self._store.seed_concept_map()
        limit = int(arguments.get("limit", 12))
        results = self._store.resolve_indicator_history(concept_id, limit=limit)
        from dataclasses import asdict
        return {
            "concept_id": concept_id,
            "total": len(results),
            "observations": [asdict(r) for r in results],
        }

    def _op_get_release_schedule(self, arguments: dict[str, Any]) -> dict[str, Any]:
        concept_id = (arguments.get("concept_id") or "").strip() or None
        due_only = bool(arguments.get("due_only", False))
        limit = int(arguments.get("limit", 100))

        self._store.seed_release_schedules()

        if concept_id:
            rec = self._store.get_release_schedule(concept_id)
            if rec is None:
                return {"error": f"no schedule for {concept_id}", "schedules": []}
            from dataclasses import asdict
            return {"schedules": [asdict(rec)]}

        schedules = self._store.list_release_schedules(is_active=True)
        if due_only:
            from ingestion.release_schedule import check_due_concepts
            schedules = check_due_concepts(schedules)

        from dataclasses import asdict
        items = [asdict(s) for s in schedules[:limit]]
        return {"total": len(items), "schedules": items}

    def _op_get_release_status(self, arguments: dict[str, Any]) -> dict[str, Any]:
        concept_id = (arguments.get("concept_id") or "").strip() or None
        status_filter = (arguments.get("status") or "").strip() or None
        limit = int(arguments.get("limit", 100))

        if concept_id:
            rec = self._store.get_latest_release_status(concept_id)
            if rec is None:
                return {"error": f"no status for {concept_id}", "statuses": []}
            from dataclasses import asdict
            return {"statuses": [asdict(rec)]}

        statuses = self._store.list_release_statuses(status=status_filter)
        from dataclasses import asdict
        items = [asdict(s) for s in statuses[:limit]]
        return {"total": len(items), "statuses": items}

    def _op_get_health(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "ingestion unavailable"}
        indicator = (arguments.get("indicator") or "").strip() or None
        rows = self._ingestion.get_health(indicator=indicator)
        return {"total": len(rows), "rows": rows}

    def _op_get_alerts(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._ingestion is None:
            return {"error": "ingestion unavailable"}
        alerts = self._ingestion.get_alerts(
            delay_minutes=int(arguments.get("delay_minutes", 30)),
            mismatch_threshold_pct=float(arguments.get("mismatch_threshold_pct", 1.0)),
        )
        return {"total": len(alerts), "alerts": alerts}

    def _op_get_recent_releases(self, arguments: dict[str, Any]) -> dict[str, Any]:
        events = self._store.list_recent_events(
            limit=int(arguments.get("limit", 10)),
            days=int(arguments.get("days", 7)),
            released_only=True,
            importance=arguments.get("importance"),
            country=arguments.get("country"),
            category=arguments.get("category"),
        )
        return {"events": [self._event_to_dict(event) for event in events]}

    def _op_get_latest_released_event(self, arguments: dict[str, Any]) -> dict[str, Any]:
        event = self._store.latest_released_event(
            indicator_keyword=arguments.get("indicator_keyword"),
        )
        return {"event": self._event_to_dict(event) if event is not None else None}

    def _op_get_upcoming_calendar(self, arguments: dict[str, Any]) -> dict[str, Any]:
        events = self._store.list_upcoming_events(limit=int(arguments.get("limit", 10)))
        return {"events": [self._event_to_dict(event) for event in events]}

    def _op_get_market_snapshot(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        prices = self._store.latest_market_prices()
        return {"prices": [self._price_to_dict(price) for price in prices]}

    def _op_get_recent_fed_comms(self, arguments: dict[str, Any]) -> dict[str, Any]:
        communications = self._store.list_recent_central_bank_comms(
            days=int(arguments.get("days", 14)),
            limit=int(arguments.get("limit", 5)),
        )
        return {"communications": [self._comm_to_dict(item) for item in communications]}

    def _op_get_fed_communications(self, arguments: dict[str, Any]) -> dict[str, Any]:
        speaker = (arguments.get("speaker") or "").strip() or None
        content_type = (arguments.get("content_type") or "").strip() or None
        days = min(int(arguments.get("days", 14)), 60)
        limit = min(int(arguments.get("limit", 5)), 15)
        comms = self._store.list_recent_central_bank_comms(
            source="fed",
            limit=limit,
            days=days,
            speaker=speaker,
            content_type=content_type,
        )
        return {
            "total": len(comms),
            "days": days,
            "communications": [self._comm_to_dict(item) for item in comms],
        }

    def _op_get_indicator_history(self, arguments: dict[str, Any]) -> dict[str, Any]:
        series_id = (arguments.get("series_id") or "").strip()
        if not series_id:
            return {"error": "series_id is required", "observations": []}
        limit = min(int(arguments.get("limit", 12)), 36)
        observations = self._store.get_indicator_history(series_id, limit=limit)
        items = [
            {
                "series_id": observation.series_id,
                "date": observation.date,
                "value": observation.value,
                "source": observation.source,
                "metadata": getattr(observation, "metadata", {}),
            }
            for observation in observations
        ]
        return {"series_id": series_id, "total": len(items), "observations": items}

    def _op_get_indicator_ontology(self, arguments: dict[str, Any]) -> dict[str, Any]:
        self._ensure_structural_ontology()
        indicator_id = (arguments.get("indicator_id") or "").strip()
        if not indicator_id:
            return {"error": "indicator_id is required", "indicator": None}

        indicator = self._store.get_calendar_indicator(indicator_id)
        if indicator is None:
            return {"error": f"unknown indicator_id: {indicator_id}", "indicator": None}

        aliases = self._store.list_aliases_for_indicator(indicator_id)
        release_families = self._store.list_release_families_for_indicator(indicator_id)
        release_sources: dict[str, Any | None] = {}
        release_items: list[dict[str, Any]] = []
        institutions: dict[str, dict[str, Any]] = {}
        release_family_ids: list[str] = []
        produced_by_ids: list[str] = []

        obs_family = None
        obs_source = None
        if indicator.obs_family_id:
            obs_family = self._store.get_obs_family(indicator.obs_family_id)
            if obs_family is not None:
                obs_source = self._store.get_obs_source(obs_family.source_id)
                if obs_source is not None:
                    self._merge_ontology_institution(
                        institutions,
                        institution_id=obs_source.source_id,
                        name=obs_source.source_name,
                        source_type=obs_source.source_type,
                        country_code=obs_source.country_code,
                        homepage_url=obs_source.homepage_url,
                        role="series_provider",
                    )

        for release_family in release_families:
            release_family_ids.append(release_family.release_family_id)
            produced_by_ids.append(release_family.source_id)
            release_source = release_sources.get(release_family.source_id)
            if release_family.source_id not in release_sources:
                release_source = self._store.get_doc_source(release_family.source_id)
                release_sources[release_family.source_id] = release_source
            if release_source is not None:
                self._merge_ontology_institution(
                    institutions,
                    institution_id=release_source.source_id,
                    name=release_source.source_name,
                    source_type=release_source.source_type,
                    country_code=release_source.country_code,
                    homepage_url=release_source.homepage_url,
                    role="release_producer",
                )
            release_items.append(self._release_family_to_dict(release_family, release_source=release_source))

        return {
            "indicator": self._calendar_indicator_to_dict(
                indicator,
                produced_by_institution_ids=sorted(set(produced_by_ids)),
                release_family_ids=sorted(release_family_ids),
            ),
            "topic": {
                "code": indicator.topic,
                "country_code": indicator.country_code,
            },
            "aliases": [self._calendar_alias_to_dict(alias) for alias in aliases],
            "time_series": self._obs_family_to_dict(obs_family, obs_source=obs_source) if obs_family is not None else None,
            "release_families": release_items,
            "institutions": sorted(institutions.values(), key=lambda item: item["institution_id"]),
        }

    def _op_list_indicators_by_topic(self, arguments: dict[str, Any]) -> dict[str, Any]:
        self._ensure_structural_ontology()
        topic = (arguments.get("topic_code") or arguments.get("topic") or "").strip().lower()
        if not topic:
            return {"error": "topic is required", "indicators": []}
        country_code = (arguments.get("country_code") or arguments.get("country") or "").strip().upper()
        indicators = self._store.list_calendar_indicators(
            country_code=country_code or None,
            topic=topic,
        )
        items = []
        for indicator in indicators:
            release_family_ids = [
                release.release_family_id
                for release in self._store.list_release_families_for_indicator(indicator.indicator_id)
            ]
            items.append(
                self._calendar_indicator_to_dict(
                    indicator,
                    release_family_ids=sorted(release_family_ids),
                )
                | {"has_time_series": bool(indicator.obs_family_id)}
            )
        return {
            "topic": topic,
            "country_code": country_code,
            "total": len(items),
            "indicators": items,
        }

    def _op_list_release_families_for_indicator(self, arguments: dict[str, Any]) -> dict[str, Any]:
        self._ensure_structural_ontology()
        indicator_id = (arguments.get("indicator_id") or "").strip()
        if not indicator_id:
            return {"error": "indicator_id is required", "release_families": []}

        indicator = self._store.get_calendar_indicator(indicator_id)
        if indicator is None:
            return {"error": f"unknown indicator_id: {indicator_id}", "release_families": []}

        release_families = self._store.list_release_families_for_indicator(indicator_id)
        items = []
        for release_family in release_families:
            release_source = self._store.get_doc_source(release_family.source_id)
            items.append(self._release_family_to_dict(release_family, release_source=release_source))
        return {
            "indicator": self._calendar_indicator_to_dict(
                indicator,
                produced_by_institution_ids=sorted({release.source_id for release in release_families}),
                release_family_ids=sorted(release.release_family_id for release in release_families),
            ),
            "total": len(items),
            "release_families": items,
        }

    def _op_get_today_calendar(self, arguments: dict[str, Any]) -> dict[str, Any]:
        events = self._store.list_today_events(
            importance=arguments.get("importance"),
            country=arguments.get("country"),
            category=arguments.get("category"),
        )
        return {"events": [self._event_to_dict(event) for event in events]}

    def _op_get_indicator_trend(self, arguments: dict[str, Any]) -> dict[str, Any]:
        keyword = str(arguments["indicator_keyword"])
        limit = int(arguments.get("limit", 12))
        events = self._store.list_indicator_releases(indicator_keyword=keyword, limit=limit)
        return {
            "indicator_keyword": keyword,
            "releases": [self._event_to_dict(event) for event in events],
        }

    def _op_get_surprise_summary(self, arguments: dict[str, Any]) -> dict[str, Any]:
        days = int(arguments.get("days", 14))
        events = self._store.list_recent_events(limit=200, days=days, released_only=True)
        by_category: dict[str, list[float]] = {}
        for event in events:
            if event.surprise is not None:
                by_category.setdefault(event.category, []).append(float(event.surprise))
        summary = []
        for category, surprises in sorted(by_category.items()):
            beats = sum(1 for item in surprises if item > 0)
            misses = sum(1 for item in surprises if item < 0)
            avg = round(sum(surprises) / len(surprises), 4) if surprises else 0.0
            summary.append({
                "category": category,
                "count": len(surprises),
                "beats": beats,
                "misses": misses,
                "avg_surprise": avg,
            })
        return {"summary": summary}

    def _op_get_recent_news(self, arguments: dict[str, Any]) -> dict[str, Any]:
        articles = self._store.get_news_context(
            days=int(arguments.get("days", 3)),
            limit=int(arguments.get("limit", 15)),
            impact_level=arguments.get("impact_level"),
            feed_category=arguments.get("feed_category"),
            finance_category=arguments.get("finance_category"),
            country=arguments.get("country"),
            asset_class=arguments.get("asset_class"),
            display_timezone=arguments.get("timezone"),
        )
        return {"articles": articles}

    def _op_get_trends(self, arguments: dict[str, Any]) -> dict[str, Any]:
        limit = min(max(int(arguments.get("limit", 10)), 1), 20)
        hours = min(max(int(arguments.get("hours", 48)), 1), 168)
        category = (arguments.get("category") or "").strip() or None
        region = (arguments.get("region") or "").strip() or None
        topics = self._store.list_active_trends(
            limit=limit,
            hours=hours,
            category=category,
            region=region,
        )
        return {
            "timestamp": format_epoch_iso(int(datetime.now(timezone.utc).timestamp())),
            "total": len(topics),
            "topics": [self._trend_to_dict(topic) for topic in topics],
        }

    def _op_search_news(self, arguments: dict[str, Any]) -> dict[str, Any]:
        query = (arguments.get("query") or "").strip() or None
        days = min(int(arguments.get("days", 7)), 30)
        limit = min(int(arguments.get("limit", 10)), 25)
        articles = self._store.get_news_context(
            query=query,
            days=days,
            limit=limit,
            impact_level=(arguments.get("impact_level") or "").strip() or None,
            feed_category=(arguments.get("feed_category") or "").strip() or None,
            finance_category=(arguments.get("finance_category") or "").strip() or None,
            country=(arguments.get("country") or "").strip() or None,
            asset_class=(arguments.get("asset_class") or "").strip() or None,
            display_timezone=(arguments.get("timezone") or "").strip() or None,
        )
        return {"total": len(articles), "days": days, "articles": articles}

    def _op_search_knowledge_base(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self._retriever is None:
            return {
                "error": "knowledge base unavailable",
                "evidences": [],
                "stats": {"total_candidates": 0, "fused": 0, "final_k": 0, "coverage": {}, "coverage_ok": False, "timing_ms": 0},
            }
        from rag.models import MacroMode

        query = str(arguments.get("query") or "")
        if not query.strip():
            return {"error": "query is required"}
        mode_str = str(arguments.get("mode") or "QA").upper()
        try:
            mode = MacroMode(mode_str)
        except ValueError:
            mode = MacroMode.QA
        filters: dict[str, Any] = {}
        for key in ("country", "indicator_group", "impact_level", "content_type", "source_type"):
            value = arguments.get(key)
            if value:
                filters[key] = [value] if isinstance(value, str) else value
        days = arguments.get("days")
        if days:
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
            filters["updated_after"] = cutoff.isoformat()
        limit = arguments.get("limit")
        result = self._retriever.retrieve(
            query,
            mode,
            filters=filters,
            limit=int(limit) if limit else None,
        )
        evidences = []
        for evidence in result.get("evidences", []):
            evidences.append({
                "chunk_id": evidence.chunk_id,
                "text": evidence.text,
                "source_type": evidence.source_type,
                "source_id": evidence.source_id,
                "section_path": evidence.section_path,
                "content_type": evidence.content_type,
                "country": evidence.country,
                "indicator_group": evidence.indicator_group,
                "impact_level": evidence.impact_level,
                "data_source": evidence.data_source,
                "updated_at": evidence.updated_at,
                "scores": evidence.scores,
            })
        return {
            "evidences": evidences,
            "stats": {
                "total_candidates": result.get("candidates_total", 0),
                "fused": result.get("deduped_total", 0),
                "final_k": result.get("final_k", 0),
                "coverage": result.get("coverage_counts", {}),
                "coverage_ok": result.get("coverage_ok", False),
                "timing_ms": result.get("timing_ms", 0),
            },
        }

    def _op_fetch_live_calendar(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from ingestion.scrapers import (
            ForexFactoryCalendarClient,
            InvestingCalendarClient,
            TradingEconomicsCalendarClient,
        )

        source = (arguments.get("source") or "all").lower()
        importance_filter = arguments.get("importance")
        country_filter = arguments.get("country")
        sources = ("investing", "forexfactory", "tradingeconomics") if source == "all" else (source,)
        all_events: list[Any] = []
        errors: list[str] = []
        for item in sources:
            try:
                if item == "investing":
                    all_events.extend(InvestingCalendarClient().fetch())
                elif item == "forexfactory":
                    all_events.extend(ForexFactoryCalendarClient().fetch())
                elif item == "tradingeconomics":
                    all_events.extend(TradingEconomicsCalendarClient().fetch())
            except Exception as exc:
                logger.warning("Live fetch from %s failed: %s", item, exc)
                errors.append(f"{item}: {exc}")
        for event in all_events:
            try:
                self._store.upsert_calendar_event(event)
            except Exception:
                logger.warning("Failed to persist live calendar event %s", getattr(event, "event_id", ""), exc_info=True)
        filtered = all_events
        if importance_filter:
            filtered = [event for event in filtered if event.importance == importance_filter]
        if country_filter:
            filtered = [event for event in filtered if event.country == str(country_filter).upper()]
        result: dict[str, Any] = {
            "total_fetched": len(all_events),
            "returned": len(filtered),
            "events": [self._event_to_dict(event) for event in filtered],
        }
        if errors:
            result["errors"] = errors
        return result

    def _op_fetch_live_news(self, arguments: dict[str, Any]) -> dict[str, Any]:
        raw_sources = (arguments.get("sources") or "all").lower().strip()
        section = arguments.get("section") or "markets"
        limit = min(int(arguments.get("limit", 10)), 25)
        sources = _NEWS_PRESETS.get(raw_sources)
        if sources is None:
            sources = tuple(item.strip() for item in raw_sources.split(",") if item.strip())
        all_items: list[dict[str, Any]] = []
        errors: list[str] = []
        for source in sources:
            try:
                all_items.extend(self._fetch_live_news_source(source, section=section, limit=limit))
            except Exception as exc:
                logger.warning("Live news fetch from %s failed: %s", source, exc)
                errors.append(f"{source}: {exc}")
        result: dict[str, Any] = {
            "sources_requested": list(sources),
            "total": len(all_items),
            "items": all_items,
        }
        if errors:
            result["errors"] = errors
        return result

    def _op_fetch_live_markets(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from ingestion.scrapers import TradingEconomicsMarketsClient

        asset_class = (arguments.get("asset_class") or "all").lower().strip()
        try:
            quotes = TradingEconomicsMarketsClient().fetch_markets()
        except Exception as exc:
            logger.warning("Live markets fetch failed: %s", exc)
            return {"error": str(exc), "quotes": []}
        items = [
            {
                "name": quote.name,
                "asset_class": quote.asset_class,
                "price": quote.price,
                "change": quote.change,
                "change_pct": quote.change_pct,
                "symbol": quote.symbol,
            }
            for quote in quotes
        ]
        if asset_class != "all" and asset_class in _VALID_MARKET_ASSET_CLASSES:
            items = [quote for quote in items if str(quote["asset_class"]).lower() == asset_class]
        return {"total": len(items), "asset_class_filter": asset_class, "quotes": items}

    def _op_fetch_country_indicators(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from ingestion.scrapers import TradingEconomicsIndicatorsClient

        country = (arguments.get("country") or "united-states").lower().strip()
        category_filter = (arguments.get("category") or "").lower().strip()
        limit = min(int(arguments.get("limit", 50)), 100)
        try:
            indicators = TradingEconomicsIndicatorsClient().fetch_indicators(country=country)
        except Exception as exc:
            logger.warning("Live indicators fetch failed for %s: %s", country, exc)
            return {"error": str(exc), "indicators": []}
        items = [
            {
                "name": indicator.name,
                "last": indicator.last,
                "previous": indicator.previous,
                "highest": indicator.highest,
                "lowest": indicator.lowest,
                "unit": indicator.unit,
                "date": indicator.date,
                "category": indicator.category,
            }
            for indicator in indicators
        ]
        if category_filter:
            items = [
                item for item in items
                if category_filter in str(item["category"]).lower() or category_filter in str(item["name"]).lower()
            ]
        return {"country": country, "total": len(items[:limit]), "indicators": items[:limit]}

    def _op_fetch_reference_rates(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from ingestion.scrapers import NYFedRatesClient

        rate_type = (arguments.get("rate_type") or "all").lower().strip()
        last_n = min(int(arguments.get("last_n", 3)), 10)
        if rate_type not in _VALID_RATE_TYPES:
            return {"error": f"Invalid rate_type '{rate_type}'. Use: sofr, effr, obfr, or all", "rates": []}
        try:
            client = NYFedRatesClient()
            if rate_type == "sofr":
                rates = client.fetch_sofr(last_n=last_n)
            elif rate_type == "effr":
                rates = client.fetch_effr(last_n=last_n)
            elif rate_type == "obfr":
                rates = client.fetch_obfr(last_n=last_n)
            else:
                rates = client.fetch_all_rates(last_n=last_n)
        except Exception as exc:
            logger.warning("Live rates fetch failed: %s", exc)
            return {"error": str(exc), "rates": []}
        return {
            "rate_type": rate_type,
            "total": len(rates),
            "rates": [
                {
                    "date": rate.date,
                    "type": rate.type,
                    "rate": rate.rate,
                    "percentile_1": rate.percentile_1,
                    "percentile_25": rate.percentile_25,
                    "percentile_75": rate.percentile_75,
                    "percentile_99": rate.percentile_99,
                    "volume_billions": rate.volume_billions,
                    "target_rate_from": rate.target_rate_from,
                    "target_rate_to": rate.target_rate_to,
                }
                for rate in rates
            ],
        }

    def _op_fetch_rate_expectations(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from ingestion.scrapers import RateProbabilityClient

        include_history = bool(arguments.get("include_history", False))
        try:
            result = RateProbabilityClient().fetch_probabilities()
        except Exception as exc:
            logger.warning("Rate expectations fetch failed: %s", exc)
            return {"error": str(exc)}
        output: dict[str, Any] = {
            "as_of": result.as_of,
            "current_band": result.current_band,
            "midpoint": result.midpoint,
            "effr": result.effr,
            "meetings": [
                {
                    "meeting_date": meeting.meeting_date,
                    "implied_rate": meeting.implied_rate,
                    "prob_move_pct": meeting.prob_move_pct,
                    "is_cut": meeting.is_cut,
                    "num_moves": meeting.num_moves,
                    "change_bps": meeting.change_bps,
                }
                for meeting in result.meetings
            ],
        }
        if include_history and result.snapshots:
            output["snapshots"] = {
                label: [
                    {
                        "meeting_date": meeting.meeting_date,
                        "implied_rate": meeting.implied_rate,
                        "prob_move_pct": meeting.prob_move_pct,
                        "is_cut": meeting.is_cut,
                        "num_moves": meeting.num_moves,
                        "change_bps": meeting.change_bps,
                    }
                    for meeting in meetings
                ]
                for label, meetings in result.snapshots.items()
            }
        return output

    def _op_fetch_article(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from ingestion.news_fetcher import ArticleFetcher
        from ingestion.scrapers import (
            BloombergArticleClient,
            FTArticleClient,
            ReutersArticleClient,
            WSJArticleClient,
        )

        url = str(arguments.get("url", "")).strip()
        if not url:
            return {"error": "url is required", "fetched": False}
        max_chars = min(int(arguments.get("max_chars", 6000)), 12000)
        domain_key = _detect_article_domain(url)
        try:
            if domain_key == "bloomberg":
                with BloombergArticleClient() as client:
                    article = client.fetch_article(url)
                if not article.fetched:
                    return {"error": article.error or "fetch failed", "fetched": False}
                return self._article_response(
                    source="bloomberg",
                    article=article,
                    max_chars=max_chars,
                    extra={"lede": article.lede},
                )
            if domain_key == "ft":
                with FTArticleClient() as client:
                    article = client.fetch_article(url)
                if not article.fetched:
                    return {"error": article.error or "fetch failed", "fetched": False}
                return self._article_response(
                    source="ft",
                    article=article,
                    max_chars=max_chars,
                    extra={"standfirst": article.standfirst},
                )
            if domain_key == "wsj":
                with WSJArticleClient() as client:
                    article = client.fetch_article(url)
                if not article.fetched:
                    return {"error": article.error or "fetch failed", "fetched": False}
                return self._article_response(
                    source="wsj",
                    article=article,
                    max_chars=max_chars,
                    extra={"dek": article.dek},
                )
            if domain_key == "reuters":
                article = ReutersArticleClient().fetch_article(url)
                if not article.fetched:
                    return {"error": article.error or "fetch failed", "fetched": False}
                return self._article_response(source="reuters", article=article, max_chars=max_chars, extra={})
            article = ArticleFetcher(timeout=20, max_content_chars=15_000).fetch_article(url, rss_description="")
            if not article.fetched:
                return {"error": article.error or "fetch failed", "fetched": False}
            content = article.content[:max_chars]
            return {
                "source": "generic",
                "title": getattr(article, "title", ""),
                "content": content,
                "content_length": len(content),
                "truncated": len(article.content) > max_chars,
                "fetched": True,
            }
        except Exception as exc:
            logger.warning("fetch_article failed for %s: %s", url, exc)
            return {"error": str(exc), "fetched": False}

    def _fetch_live_news_source(self, source: str, *, section: str, limit: int) -> list[dict[str, Any]]:
        from ingestion.scrapers import (
            BloombergNewsClient,
            FTNewsClient,
            ForexFactoryNewsClient,
            InvestingNewsClient,
            ReutersNewsClient,
            TradingEconomicsNewsClient,
            WSJNewsClient,
        )

        if source == "investing":
            raw = InvestingNewsClient().fetch_news(category=section)[:limit]
        elif source == "forexfactory":
            raw = ForexFactoryNewsClient().fetch_news()[:limit]
        elif source == "tradingeconomics":
            raw = TradingEconomicsNewsClient().fetch_news(count=limit)
        elif source == "reuters":
            raw = ReutersNewsClient().fetch_news(section=section)[:limit]
        elif source == "bloomberg":
            with BloombergNewsClient() as client:
                raw = client.fetch_news(section=section)[:limit]
        elif source == "ft":
            with FTNewsClient() as client:
                raw = client.fetch_news(section=section)[:limit]
        elif source == "wsj":
            with WSJNewsClient() as client:
                raw = client.fetch_news(section=section)[:limit]
        else:
            return []
        return [
            {
                "source": item.source,
                "title": item.title,
                "url": item.url,
                "published_at": item.published_at,
                "description": item.description[:200] if item.description else "",
                "category": item.category,
                "importance": item.importance,
            }
            for item in raw
        ]

    def _article_response(
        self,
        *,
        source: str,
        article: Any,
        max_chars: int,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        content = article.content[:max_chars]
        payload = {
            "source": source,
            "title": article.title,
            "authors": article.authors,
            "published_at": article.published_at,
            "keywords": article.keywords,
            "content": content,
            "content_length": len(content),
            "truncated": len(article.content) > max_chars,
            "fetched": True,
        }
        payload.update(extra)
        return payload

    def _merge_ontology_institution(
        self,
        institutions: dict[str, dict[str, Any]],
        *,
        institution_id: str,
        name: str,
        source_type: str,
        country_code: str,
        homepage_url: str,
        role: str,
    ) -> None:
        if not institution_id:
            return
        record = institutions.setdefault(
            institution_id,
            {
                "institution_id": institution_id,
                "name": name,
                "source_type": source_type,
                "country_code": country_code,
                "homepage_url": homepage_url,
                "roles": [],
            },
        )
        if role not in record["roles"]:
            record["roles"].append(role)
            record["roles"].sort()

    def _calendar_indicator_to_dict(
        self,
        indicator: Any,
        *,
        produced_by_institution_ids: list[str] | None = None,
        release_family_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "indicator_id": indicator.indicator_id,
            "canonical_name": indicator.canonical_name,
            "topic": indicator.topic,
            "country_code": indicator.country_code,
            "frequency": indicator.frequency,
            "unit": indicator.unit,
            "obs_family_id": indicator.obs_family_id,
        }
        if produced_by_institution_ids is not None:
            payload["produced_by_institution_ids"] = produced_by_institution_ids
        if release_family_ids is not None:
            payload["release_family_ids"] = release_family_ids
        return payload

    def _calendar_alias_to_dict(self, alias: Any) -> dict[str, Any]:
        return {
            "alias": alias.alias_original or alias.alias_normalized,
            "normalized_alias": alias.alias_normalized,
            "source": alias.source,
            "country_code": alias.country_code,
        }

    def _obs_family_to_dict(self, obs_family: Any, *, obs_source: Any | None = None) -> dict[str, Any]:
        payload = {
            "family_id": obs_family.family_id,
            "provider_series_id": obs_family.provider_series_id,
            "canonical_name": obs_family.canonical_name,
            "source_id": obs_family.source_id,
            "country_code": obs_family.country_code,
            "topic_code": obs_family.topic_code,
            "category": obs_family.category,
            "frequency": obs_family.frequency,
            "unit": obs_family.unit,
            "seasonal_adjustment": obs_family.seasonal_adjustment,
            "has_vintages": obs_family.has_vintages,
        }
        if obs_source is not None:
            payload["source_name"] = obs_source.source_name
            payload["source_type"] = obs_source.source_type
        return payload

    def _release_family_to_dict(self, release_family: Any, *, release_source: Any | None = None) -> dict[str, Any]:
        payload = {
            "release_family_id": release_family.release_family_id,
            "release_code": release_family.release_code,
            "release_name": release_family.release_name,
            "topic_code": release_family.topic_code,
            "country_code": release_family.country_code,
            "frequency": release_family.frequency,
            "produced_by_institution_id": release_family.source_id,
        }
        if release_source is not None:
            payload["institution_name"] = release_source.source_name
            payload["institution_type"] = release_source.source_type
            payload["homepage_url"] = release_source.homepage_url
        return payload

    def _event_to_dict(self, event: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "source": event.source,
            "event_id": event.event_id,
            "timestamp": event.timestamp,
            "datetime_utc": format_epoch_iso(event.timestamp),
            "country": event.country,
            "indicator": event.indicator,
            "category": event.category,
            "importance": event.importance,
            "actual": event.actual,
            "forecast": event.forecast,
            "previous": event.previous,
            "surprise": event.surprise,
        }
        indicator_id = getattr(event, "indicator_id", "")
        if indicator_id:
            payload["indicator_id"] = indicator_id
        return payload

    def _price_to_dict(self, price: Any) -> dict[str, Any]:
        return {
            "symbol": price.symbol,
            "name": price.name,
            "asset_class": price.asset_class,
            "price": price.price,
            "change_pct": price.change_pct,
            "timestamp": price.timestamp,
            "datetime_utc": format_epoch_iso(price.timestamp),
        }

    def _comm_to_dict(self, communication: Any) -> dict[str, Any]:
        summary = communication.summary
        if len(summary) > 800:
            summary = summary[:800] + "..."
        return {
            "title": communication.title,
            "url": communication.url,
            "timestamp": communication.timestamp,
            "published_at": format_epoch_iso(communication.timestamp),
            "speaker": communication.speaker,
            "content_type": communication.content_type,
            "summary": summary,
        }

    def _trend_to_dict(self, trend: Any) -> dict[str, Any]:
        return {
            "topic": trend.topic,
            "summary": trend.summary,
            "keywords": list(trend.keywords),
            "category": trend.category,
            "region": trend.region,
            "popularity_score": round(float(trend.popularity_score), 2),
            "observed_at": format_epoch_iso(int(trend.observed_at)),
            "expires_at": format_epoch_iso(int(trend.expires_at)),
        }
