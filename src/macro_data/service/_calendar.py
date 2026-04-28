from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Any

from contracts import format_epoch_iso

from .base import LocalMacroDataServiceBase


def _collect_dividend_tickers(
    connection: sqlite3.Connection,
    *,
    start: date,
    end: date,
    provider: str = "eodhd",
) -> list[str]:
    """Ordered ``TICKER.EXCHANGE`` codes for dividends in the window.

    Matches the dividend-detail endpoint's ``/api/div/{TICKER}.{EXCHANGE}``
    routing — discovery rows that wrote ``ticker``/``exchange`` are the
    only rows the enrichment pass can act on.

    Order is **unenriched-first** (``reference_date IS NULL`` — the
    discovery feed leaves ``reference_date`` empty; the enrichment
    writeback fills it), then by ``observed_at_epoch_ms`` so the oldest
    enriched rows are next in line for restatement re-checks. Without
    this rotation, a budgeted daily sweep would consume the same
    alphabetic prefix every run, leaving later tickers permanently
    unenriched.
    """
    rows = connection.execute(
        """
        SELECT ticker || CASE WHEN exchange != ''
                              THEN '.' || exchange
                              ELSE '' END AS code,
               (reference_date IS NULL OR reference_date = '') AS unenriched,
               MIN(observed_at_epoch_ms) AS first_seen
        FROM cal_corp_event
        WHERE provider = ?
          AND event_subtype = 'dividend'
          AND substr(event_time_utc, 1, 10) >= ?
          AND substr(event_time_utc, 1, 10) <= ?
          AND ticker != ''
        GROUP BY code
        ORDER BY unenriched DESC, first_seen ASC, code ASC
        """,
        (provider, start.isoformat(), end.isoformat()),
    ).fetchall()
    seen: set[str] = set()
    out: list[str] = []
    for row in rows:
        code = row[0]
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


class CalendarOpsMixin(LocalMacroDataServiceBase):
    def _op_refresh_calendar(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        if self._ingestion is None:
            return {"error": "calendar refresh unavailable"}
        return dict(self._ingestion.refresh_calendar())

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

    def _op_calendar_corp_backfill(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Resumable historical backfill of one corp calendar subtype.

        Mirror of ``_op_calendar_corp_fetch`` for long sweeps: each
        invocation is bounded by ``max_requests`` and persists progress
        to ``cal_corp_backfill_cursor`` so a budget breach or 429 storm
        does not start the next run from scratch.

        Arguments:
          subtype                  — ``earnings`` / ``ipo`` / ``split`` /
                                     ``dividend``. ``earnings_trend`` is
                                     forward-only and rejected here.
          phases                   — optional list of ``recent`` / ``mid``
                                     / ``early``; omit to cover all three.
          from / to                — optional ISO bounds clipped to the
                                     phase spans (default: phase floors
                                     → today).
          symbols                  — optional ticker filter for subtypes
                                     that accept it (earnings, split,
                                     dividend).
          dry_run                  — default True. Returns the plan and
                                     cursor state without HTTP traffic.
          max_requests             — default 50. Caps EODHD calls per
                                     invocation.
          window_days              — default 7. Window slice width.
          enrich_dividend_details  — default True. When ``subtype`` is
                                     ``dividend``, runs the per-ticker
                                     /api/div detail enrichment after
                                     the discovery sweep using leftover
                                     budget.
        """
        from datetime import date as _date

        from ingestion.calendar.eodhd_api import (
            CorpBackfillRunner,
            EODHDAPIClient,
            SUBTYPES_BACKFILLABLE,
        )

        subtype = (arguments.get("subtype") or "").strip()
        if not subtype:
            return {"error": "subtype is required"}
        if subtype not in SUBTYPES_BACKFILLABLE:
            return {
                "error": f"subtype {subtype!r} is not backfillable; "
                         f"expected one of {list(SUBTYPES_BACKFILLABLE)} "
                         f"(earnings_trend has no historical floor)"
            }

        dry_run = bool(arguments.get("dry_run", True))
        try:
            max_requests = max(1, int(arguments.get("max_requests") or 50))
        except (TypeError, ValueError):
            max_requests = 50
        try:
            window_days = max(1, int(arguments.get("window_days") or 7))
        except (TypeError, ValueError):
            window_days = 7
        enrich_dividend_details = bool(
            arguments.get("enrich_dividend_details", True)
        )

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

        raw_phases = arguments.get("phases")
        phases: list[str] | None = None
        if isinstance(raw_phases, list) and raw_phases:
            phases = [str(p).strip() for p in raw_phases if str(p).strip()]
        elif isinstance(raw_phases, str) and raw_phases.strip():
            phases = [p.strip() for p in raw_phases.split(",") if p.strip()]

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
                runner = CorpBackfillRunner(
                    connection=connection,
                    client=client,
                    max_requests=max_requests,
                    window_days=window_days,
                )
                summary = runner.run(
                    subtype=subtype,
                    start=start,
                    end=end,
                    phases=phases,
                    symbols=symbols or None,
                    dry_run=dry_run,
                    enrich_dividend_details=enrich_dividend_details,
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
            "phases_planned":    summary.phases_planned,
            "windows_planned":   summary.windows_planned,
            "requests_spent":    summary.requests_spent,
            "rows_parsed":       summary.rows_parsed,
            "rows_raw_inserted": summary.rows_raw_inserted,
            "events_upserted":   summary.events_upserted,
            "parse_errors":      summary.parse_errors,
            "windows_fetched":   summary.windows_fetched,
            "dividend_detail_symbols": summary.dividend_detail_symbols,
            "stopped_reason":    summary.stopped_reason,
            "cursor_state":      summary.cursor_state,
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

    def _op_calendar_corp_forward_sweep(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Daily forward-window sweep of the corp calendar lane — issue #63.

        Runs the four discovery subtypes (``earnings``, ``ipo``, ``split``,
        ``dividend``) over a rolling ``[today − lookback_days,
        today + lookforward_days]`` window, then enriches the dividend
        rows discovered this run via ``/api/div/{ticker}``. Each subtype
        is isolated: an EODHD 5xx, parse failure, or budget halt on one
        subtype must not abort the rest. ``earnings_trend`` is symbol-
        scoped and depends on a watchlist source-of-truth; deferred.

        Arguments:
          lookback_days       — default 7. Backward window catches
                                restatements + late-arriving actuals.
          lookforward_days    — default 90. Forward window catches
                                schedule announcements.
          max_requests        — default 30. Per-subtype hard cap.
          window_days         — default 7. Window slice width.
          dry_run             — default True. Returns the plan shape
                                without issuing any HTTP request.
          subtypes            — optional subset (default all four +
                                ``dividend_details``). ``dividend_details``
                                always runs after ``dividend`` even if
                                listed earlier; reordering is silent.
        """
        from datetime import date as _date, datetime as _dt, timezone as _tz

        from ingestion.calendar.eodhd_api import (
            CorpCalendarFetcher,
            EODHDAPIClient,
            fetch_dividend_details,
        )

        dry_run = bool(arguments.get("dry_run", True))
        try:
            lookback_days = max(0, int(arguments.get("lookback_days") or 7))
        except (TypeError, ValueError):
            lookback_days = 7
        try:
            lookforward_days = max(0, int(arguments.get("lookforward_days") or 90))
        except (TypeError, ValueError):
            lookforward_days = 90
        try:
            max_requests = max(1, int(arguments.get("max_requests") or 30))
        except (TypeError, ValueError):
            max_requests = 30
        try:
            window_days = max(1, int(arguments.get("window_days") or 7))
        except (TypeError, ValueError):
            window_days = 7

        all_subtypes = ("earnings", "ipo", "split", "dividend", "dividend_details")
        raw_subtypes = arguments.get("subtypes")
        if isinstance(raw_subtypes, list) and raw_subtypes:
            requested = [str(s).strip() for s in raw_subtypes if str(s).strip()]
        elif isinstance(raw_subtypes, str) and raw_subtypes.strip():
            requested = [s.strip() for s in raw_subtypes.split(",") if s.strip()]
        else:
            requested = list(all_subtypes)
        unknown = [s for s in requested if s not in all_subtypes]
        if unknown:
            return {"error": f"unknown subtype(s): {unknown!r}; expected subset of {list(all_subtypes)}"}
        # Force discovery → enrichment ordering: dividend_details depends on
        # tickers that the dividend discovery sweep just wrote into
        # cal_corp_event. Caller-supplied ordering is otherwise honored.
        if "dividend" in requested and "dividend_details" in requested:
            requested = [s for s in requested if s != "dividend_details"] + ["dividend_details"]

        today = _dt.now(_tz.utc).date()
        start = today - timedelta(days=lookback_days)
        end = today + timedelta(days=lookforward_days)

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        results: list[dict[str, Any]] = []
        with EODHDAPIClient() as client:
            for subtype in requested:
                started = _dt.now(_tz.utc).isoformat()
                connection = get_conn()
                try:
                    if subtype == "dividend_details":
                        # Order is significant — unenriched-first rotation
                        # so a budgeted run does not loop on the same
                        # alphabetic prefix every day. Don't sort here.
                        symbols = _collect_dividend_tickers(
                            connection, start=start, end=end,
                        )
                        if not symbols:
                            results.append({
                                "subtype":         "dividend_details",
                                "started_at":      started,
                                "finished_at":     _dt.now(_tz.utc).isoformat(),
                                "ok":              True,
                                "dry_run":         dry_run,
                                "from":            start.isoformat(),
                                "to":              end.isoformat(),
                                "symbols_planned": 0,
                                "requests_spent":  0,
                                "rows_raw_inserted": 0,
                                "events_upserted": 0,
                                "stopped_reason":  "no_dividend_tickers_in_window",
                            })
                            continue
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
                        results.append({
                            "subtype":           "dividend_details",
                            "started_at":        started,
                            "finished_at":       _dt.now(_tz.utc).isoformat(),
                            "ok":                True,
                            "dry_run":           summary.dry_run,
                            "from":              start.isoformat(),
                            "to":                end.isoformat(),
                            "symbols_planned":   summary.windows_planned,
                            "requests_spent":    summary.requests_spent,
                            "rows_parsed":       summary.rows_parsed,
                            "rows_raw_inserted": summary.rows_raw_inserted,
                            "events_upserted":   summary.events_upserted,
                            "parse_errors":      summary.parse_errors,
                            "stopped_reason":    summary.stopped_reason,
                        })
                        continue

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
                        dry_run=dry_run,
                    )
                    connection.commit()
                    results.append({
                        "subtype":           summary.subtype,
                        "started_at":        started,
                        "finished_at":       _dt.now(_tz.utc).isoformat(),
                        "ok":                True,
                        "dry_run":           summary.dry_run,
                        "from":              start.isoformat(),
                        "to":                end.isoformat(),
                        "windows_planned":   summary.windows_planned,
                        "requests_spent":    summary.requests_spent,
                        "rows_parsed":       summary.rows_parsed,
                        "rows_raw_inserted": summary.rows_raw_inserted,
                        "events_upserted":   summary.events_upserted,
                        "parse_errors":      summary.parse_errors,
                        "stopped_reason":    summary.stopped_reason,
                    })
                except Exception as exc:
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                    results.append({
                        "subtype":     subtype,
                        "started_at":  started,
                        "finished_at": _dt.now(_tz.utc).isoformat(),
                        "ok":          False,
                        "dry_run":     dry_run,
                        "error":       repr(exc),
                    })
                finally:
                    try:
                        connection.close()
                    except Exception:
                        pass

        return {
            "operation":         "calendar_corp_forward_sweep",
            "dry_run":           dry_run,
            "from":              start.isoformat(),
            "to":                end.isoformat(),
            "lookback_days":     lookback_days,
            "lookforward_days":  lookforward_days,
            "max_requests":      max_requests,
            "subtypes":          requested,
            "ok_count":          sum(1 for r in results if r.get("ok")),
            "failed_count":      sum(1 for r in results if not r.get("ok")),
            "events_upserted":   sum(int(r.get("events_upserted") or 0) for r in results),
            "requests_spent":    sum(int(r.get("requests_spent") or 0) for r in results),
            "results":           results,
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

    def _op_calendar_econ_fetch_dol(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Sweep DOL UI Weekly Claims releases (issue #50).

        Walks the ETA newsroom listing for recent ``Unemployment
        Insurance Weekly Claims Report`` rows, downloads each
        release's PDF, and writes Initial Claims + Continuing
        Claims into ``cal_econ_event``. Schedule and value land in
        one pass — DOL doesn't publish a forward calendar.
        """
        from ingestion.calendar.dol_api import fetch_dol_calendar

        dry_run = bool(arguments.get("dry_run", True))

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_dol_calendar(connection, dry_run=dry_run)
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
            "indicators_ok":      summary.indicators_ok,
            "indicators_empty":   summary.indicators_empty,
            "listing_entries":    summary.listing_entries,
            "releases_fetched":   summary.releases_fetched,
            "releases_failed":    summary.releases_failed,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_error":        summary.fetch_error,
            "stopped_reason":     "dry_run" if dry_run else None,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_eia(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Pull EIA weekly stocks via the v2 JSON API (issue #50)."""
        from ingestion.calendar.eia_api import fetch_eia_calendar
        from ingestion.timeseries.scrapers.eia import EIAClient

        dry_run = bool(arguments.get("dry_run", True))
        raw_indicators = arguments.get("indicators")
        indicators: list[str] | None = None
        if isinstance(raw_indicators, list) and raw_indicators:
            indicators = [str(s) for s in raw_indicators]

        client = EIAClient()
        if not dry_run and not client.api_key:
            return {"error": "EIA_API_KEY not set"}

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_eia_calendar(
                connection, client,
                indicators=indicators,
                dry_run=dry_run,
            )
            if not dry_run:
                connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":             summary.dry_run,
            "indicators_planned":  summary.indicators_planned,
            "indicators_unknown":  summary.indicators_unknown,
            "indicators_ok":       summary.indicators_ok,
            "indicators_empty":    summary.indicators_empty,
            "series_failed":       summary.series_failed,
            "start":               summary.start,
            "end":                 summary.end,
            "observations_seen":   summary.observations_seen,
            "rows_raw_inserted":   summary.rows_raw_inserted,
            "events_upserted":     summary.events_upserted,
            "stopped_reason":      "dry_run" if dry_run else None,
            "wall_seconds":        round(summary.wall_seconds, 3),
        }

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

    def _op_calendar_econ_fetch_conference_board(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch current Conference Board indicator values."""
        from ingestion.calendar.conference_board_api import (
            fetch_conference_board_calendar,
        )

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.conference_board_api.fetcher import (
                _resolve_series,
            )
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
            summary = fetch_conference_board_calendar(
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
            "series_failed":      summary.series_failed,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_error":        summary.fetch_error,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_conference_board(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape the Conference Board economic-indicator calendar."""
        from ingestion.calendar.conference_board_api import (
            schedule_conference_board_calendar,
        )

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        def _opt_int(key: str) -> int | None:
            raw = arguments.get(key)
            if raw is None or raw == "":
                return None
            try:
                return int(raw)
            except (TypeError, ValueError):
                return None

        from_epoch_ms = _opt_int("from_epoch_ms")
        to_epoch_ms = _opt_int("to_epoch_ms")

        if dry_run:
            from ingestion.calendar.conference_board_api.fetcher import (
                _default_schedule_window,
                _resolve_series,
            )
            planned, unknown = _resolve_series(series_ids)
            default_from, default_to = _default_schedule_window()
            return {
                "dry_run":        True,
                "series_planned": planned,
                "series_unknown": unknown,
                "from_epoch_ms":  from_epoch_ms or default_from,
                "to_epoch_ms":    to_epoch_ms or default_to,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_conference_board_calendar(
                connection,
                series_ids=series_ids,
                dry_run=False,
                from_epoch_ms=from_epoch_ms,
                to_epoch_ms=to_epoch_ms,
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
            "from_epoch_ms":      summary.from_epoch_ms,
            "to_epoch_ms":        summary.to_epoch_ms,
            "entries_parsed":     summary.entries_parsed,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "row_issues":         summary.row_issues,
            "fetch_error":        summary.fetch_error,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_nar(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch current NAR housing indicator values."""
        from ingestion.calendar.nar_api import fetch_nar_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.nar_api.fetcher import _resolve_series
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
            summary = fetch_nar_calendar(
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
            "series_failed":      summary.series_failed,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_error":        summary.fetch_error,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_nar(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape the NAR statistical news release schedule."""
        from ingestion.calendar.nar_api import schedule_nar_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.nar_api.fetcher import _resolve_series
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
            summary = schedule_nar_calendar(
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

    def _op_calendar_econ_fetch_eurostat(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch Eurostat JSON-stat observations into the calendar."""
        from ingestion.calendar.eurostat_api import fetch_eurostat_calendar
        from ingestion.timeseries.sdmx.providers.eurostat import EurostatClient

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
            from ingestion.calendar.eurostat_api.fetcher import _resolve_series
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

        client = EurostatClient()
        connection = get_conn()
        try:
            summary = fetch_eurostat_calendar(
                connection,
                client,
                start_period=start_period,
                end_period=end_period,
                series_ids=series_ids,
                dry_run=False,
                limit=limit,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "start_period":       summary.start_period,
            "end_period":         summary.end_period,
            "series_planned":     summary.series_planned,
            "series_ok":          summary.series_ok,
            "series_empty":       summary.series_empty,
            "series_unknown":     summary.series_unknown,
            "series_failed":      summary.series_failed,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_eurostat(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch Eurostat release-calendar rows into ``cal_econ_event``."""
        from ingestion.calendar.eurostat_api import schedule_eurostat_calendar

        dry_run = bool(arguments.get("dry_run", True))
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")
        start_date = str(start_date) if start_date else None
        end_date = str(end_date) if end_date else None

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.eurostat_api.fetcher import _resolve_series
            from ingestion.calendar.eurostat_api.schedule import (
                default_schedule_window,
            )
            planned, unknown = _resolve_series(series_ids)
            default_start, default_end = default_schedule_window()
            return {
                "dry_run":        True,
                "start_date":     start_date or default_start.isoformat(),
                "end_date":       end_date or default_end.isoformat(),
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_eurostat_calendar(
                connection,
                start_date=start_date,
                end_date=end_date,
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
            "start_date":         summary.start_date,
            "end_date":           summary.end_date,
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

    def _op_calendar_econ_fetch_zew(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch due ZEW press-release values into the calendar."""
        from ingestion.calendar.zew_api import fetch_zew_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list):
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.zew_api.fetcher import _resolve_series
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
            summary = fetch_zew_calendar(
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
            "series_failed":      summary.series_failed,
            "pending_releases":   summary.pending_releases,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_zew(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch ZEW release-date rows into ``cal_econ_event``."""
        from ingestion.calendar.zew_api import schedule_zew_calendar

        dry_run = bool(arguments.get("dry_run", True))
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")
        start_date = str(start_date) if start_date else None
        end_date = str(end_date) if end_date else None

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list):
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.zew_api.fetcher import _resolve_series
            from ingestion.calendar.zew_api.schedule import default_schedule_window
            planned, unknown = _resolve_series(series_ids)
            default_start, default_end = default_schedule_window()
            return {
                "dry_run":        True,
                "start_date":     start_date or default_start.isoformat(),
                "end_date":       end_date or default_end.isoformat(),
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_zew_calendar(
                connection,
                start_date=start_date,
                end_date=end_date,
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
            "start_date":         summary.start_date,
            "end_date":           summary.end_date,
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

    def _op_calendar_econ_schedule_hcob(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch HCOB Germany PMI release-date rows into ``cal_econ_event``.

        Schedule-side only. The value-side counterpart is
        ``calendar_econ_fetch_hcob`` (issue #23) which sweeps the
        public press-release listing for due rows and projects the
        headline indices onto the schedule rows.
        """
        from ingestion.calendar.hcob_api import schedule_hcob_calendar

        dry_run = bool(arguments.get("dry_run", True))
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")
        start_date = str(start_date) if start_date else None
        end_date = str(end_date) if end_date else None

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list):
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.hcob_api.fetcher import _resolve_series
            from ingestion.calendar.hcob_api.schedule import default_schedule_window
            planned, unknown = _resolve_series(series_ids)
            default_start, default_end = default_schedule_window()
            return {
                "dry_run":        True,
                "start_date":     start_date or default_start.isoformat(),
                "end_date":       end_date or default_end.isoformat(),
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_hcob_calendar(
                connection,
                start_date=start_date,
                end_date=end_date,
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
            "start_date":         summary.start_date,
            "end_date":           summary.end_date,
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

    def _op_calendar_econ_fetch_hcob(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch due HCOB / S&P Global press-release values (issue #23).

        Auto-discovers ``actual IS NULL`` HCOB rows already staged by
        ``calendar_econ_schedule_hcob``, resolves each release's PDF
        URL on the public press-release listing, downloads the PDF,
        extracts the headline index, and upserts via the shared
        ``provider_event_id``.
        """
        from ingestion.calendar.hcob_api import fetch_hcob_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list):
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.hcob_api.fetcher import _resolve_series
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
            summary = fetch_hcob_calendar(
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
            "series_failed":      summary.series_failed,
            "pending_releases":   summary.pending_releases,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_gfk(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch due GfK / NIM Consumer Climate press-release values."""
        from ingestion.calendar.gfk_api import fetch_gfk_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list):
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.gfk_api.fetcher import _resolve_series
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
            summary = fetch_gfk_calendar(
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
            "series_failed":      summary.series_failed,
            "pending_releases":   summary.pending_releases,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_gfk(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch GfK / NIM release-date rows into ``cal_econ_event``."""
        from ingestion.calendar.gfk_api import schedule_gfk_calendar

        dry_run = bool(arguments.get("dry_run", True))
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")
        start_date = str(start_date) if start_date else None
        end_date = str(end_date) if end_date else None

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list):
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.gfk_api.fetcher import _resolve_series
            from ingestion.calendar.gfk_api.schedule import default_schedule_window
            planned, unknown = _resolve_series(series_ids)
            default_start, default_end = default_schedule_window()
            return {
                "dry_run":        True,
                "start_date":     start_date or default_start.isoformat(),
                "end_date":       end_date or default_end.isoformat(),
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_gfk_calendar(
                connection,
                start_date=start_date,
                end_date=end_date,
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
            "start_date":         summary.start_date,
            "end_date":           summary.end_date,
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

    def _op_calendar_econ_fetch_ifo(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch due Ifo press-release values into the calendar."""
        from ingestion.calendar.ifo_api import fetch_ifo_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list):
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.ifo_api.fetcher import _resolve_series
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
            summary = fetch_ifo_calendar(
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
            "series_failed":      summary.series_failed,
            "pending_releases":   summary.pending_releases,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_ifo(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch Ifo release-date rows into ``cal_econ_event``."""
        from ingestion.calendar.ifo_api import schedule_ifo_calendar

        dry_run = bool(arguments.get("dry_run", True))
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")
        start_date = str(start_date) if start_date else None
        end_date = str(end_date) if end_date else None

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list):
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.ifo_api.fetcher import _resolve_series
            from ingestion.calendar.ifo_api.schedule import default_schedule_window
            planned, unknown = _resolve_series(series_ids)
            default_start, default_end = default_schedule_window()
            return {
                "dry_run":        True,
                "start_date":     start_date or default_start.isoformat(),
                "end_date":       end_date or default_end.isoformat(),
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_ifo_calendar(
                connection,
                start_date=start_date,
                end_date=end_date,
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
            "start_date":         summary.start_date,
            "end_date":           summary.end_date,
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

    def _op_calendar_econ_fetch_ec_bcs(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch due EC BCS press-release values into the calendar."""
        from ingestion.calendar.ec_bcs_api import fetch_ec_bcs_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list):
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.ec_bcs_api.fetcher import _resolve_series
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
            summary = fetch_ec_bcs_calendar(
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
            "series_failed":      summary.series_failed,
            "pending_releases":   summary.pending_releases,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_ec_bcs(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch EC BCS release-date rows into ``cal_econ_event``."""
        from ingestion.calendar.ec_bcs_api import schedule_ec_bcs_calendar

        dry_run = bool(arguments.get("dry_run", True))
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")
        start_date = str(start_date) if start_date else None
        end_date = str(end_date) if end_date else None

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list):
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.ec_bcs_api.fetcher import _resolve_series
            from ingestion.calendar.ec_bcs_api.schedule import default_schedule_window
            planned, unknown = _resolve_series(series_ids)
            default_start, default_end = default_schedule_window()
            return {
                "dry_run":        True,
                "start_date":     start_date or default_start.isoformat(),
                "end_date":       end_date or default_end.isoformat(),
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_ec_bcs_calendar(
                connection,
                start_date=start_date,
                end_date=end_date,
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
            "start_date":         summary.start_date,
            "end_date":           summary.end_date,
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

    def _op_calendar_econ_fetch_destatis(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch Destatis GENESIS observations into the calendar."""
        from ingestion.calendar.destatis_api import (
            DestatisGenesisClient,
            fetch_destatis_calendar,
        )

        dry_run = bool(arguments.get("dry_run", True))

        def _opt_int(key: str) -> int | None:
            raw = arguments.get(key)
            if raw is None or raw == "":
                return None
            try:
                return int(raw)
            except (TypeError, ValueError):
                return None

        start_year = _opt_int("start_year")
        end_year = _opt_int("end_year")
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.destatis_api.fetcher import _resolve_series
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

        username = arguments.get("username")
        password = arguments.get("password")
        client = DestatisGenesisClient.from_env()
        if username:
            client.username = str(username)
        if password:
            client.password = str(password)

        connection = get_conn()
        try:
            summary = fetch_destatis_calendar(
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
            "series_failed":      summary.series_failed,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_destatis(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch Destatis release-table rows into ``cal_econ_event``."""
        from ingestion.calendar.destatis_api import schedule_destatis_calendar

        dry_run = bool(arguments.get("dry_run", True))
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")
        start_date = str(start_date) if start_date else None
        end_date = str(end_date) if end_date else None

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.destatis_api.fetcher import _resolve_series
            from ingestion.calendar.destatis_api.schedule import (
                default_schedule_window,
            )
            planned, unknown = _resolve_series(series_ids)
            default_start, default_end = default_schedule_window()
            return {
                "dry_run":        True,
                "start_date":     start_date or default_start.isoformat(),
                "end_date":       end_date or default_end.isoformat(),
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_destatis_calendar(
                connection,
                start_date=start_date,
                end_date=end_date,
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
            "start_date":         summary.start_date,
            "end_date":           summary.end_date,
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

    def _op_calendar_econ_fetch_insee(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch due INSEE press-release values into the calendar."""
        from ingestion.calendar.insee_api import fetch_insee_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list):
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.insee_api.fetcher import _resolve_series
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
            summary = fetch_insee_calendar(
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
            "series_failed":      summary.series_failed,
            "pending_releases":   summary.pending_releases,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_insee(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch INSEE agenda rows into ``cal_econ_event``."""
        from ingestion.calendar.insee_api import schedule_insee_calendar

        dry_run = bool(arguments.get("dry_run", True))
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")
        start_date = str(start_date) if start_date else None
        end_date = str(end_date) if end_date else None

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list):
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.insee_api.fetcher import _resolve_series
            from ingestion.calendar.insee_api.schedule import default_schedule_window
            planned, unknown = _resolve_series(series_ids)
            default_start, default_end = default_schedule_window()
            return {
                "dry_run":        True,
                "start_date":     start_date or default_start.isoformat(),
                "end_date":       end_date or default_end.isoformat(),
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_insee_calendar(
                connection,
                start_date=start_date,
                end_date=end_date,
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
            "start_date":         summary.start_date,
            "end_date":           summary.end_date,
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

    def _op_calendar_econ_fetch_ine(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch due INE advance press-release values into the calendar."""
        from ingestion.calendar.ine_api import fetch_ine_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.ine_api.fetcher import _resolve_series
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
            summary = fetch_ine_calendar(
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
            "series_failed":      summary.series_failed,
            "pending_releases":   summary.pending_releases,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_ine(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch INE publication-calendar rows into ``cal_econ_event``."""
        from ingestion.calendar.ine_api import schedule_ine_calendar

        dry_run = bool(arguments.get("dry_run", True))
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")
        start_date = str(start_date) if start_date else None
        end_date = str(end_date) if end_date else None

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.ine_api.fetcher import _resolve_series
            from ingestion.calendar.ine_api.schedule import default_schedule_window
            planned, unknown = _resolve_series(series_ids)
            default_start, default_end = default_schedule_window()
            return {
                "dry_run":        True,
                "start_date":     start_date or default_start.isoformat(),
                "end_date":       end_date or default_end.isoformat(),
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_ine_calendar(
                connection,
                start_date=start_date,
                end_date=end_date,
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
            "start_date":         summary.start_date,
            "end_date":           summary.end_date,
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

    def _op_calendar_econ_fetch_istat(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch due ISTAT press-release values into the calendar."""
        from ingestion.calendar.istat_api import fetch_istat_calendar

        dry_run = bool(arguments.get("dry_run", True))
        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.istat_api.fetcher import _resolve_series
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
            summary = fetch_istat_calendar(
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
            "series_failed":      summary.series_failed,
            "pending_releases":   summary.pending_releases,
            "observations_seen":  summary.observations_seen,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_schedule_istat(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch ISTAT press-calendar rows into ``cal_econ_event``."""
        from ingestion.calendar.istat_api import schedule_istat_calendar

        dry_run = bool(arguments.get("dry_run", True))
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")
        start_date = str(start_date) if start_date else None
        end_date = str(end_date) if end_date else None

        raw_series = arguments.get("series_ids")
        series_ids: list[str] | None = None
        if isinstance(raw_series, list) and raw_series:
            series_ids = [str(s) for s in raw_series]

        if dry_run:
            from ingestion.calendar.istat_api.fetcher import _resolve_series
            from ingestion.calendar.istat_api.schedule import default_schedule_window
            planned, unknown = _resolve_series(series_ids)
            default_start, default_end = default_schedule_window()
            return {
                "dry_run":        True,
                "start_date":     start_date or default_start.isoformat(),
                "end_date":       end_date or default_end.isoformat(),
                "series_planned": planned,
                "series_unknown": unknown,
                "stopped_reason": "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = schedule_istat_calendar(
                connection,
                start_date=start_date,
                end_date=end_date,
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
            "start_date":         summary.start_date,
            "end_date":           summary.end_date,
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

    def _op_calendar_econ_fetch_boj(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape the BoJ Monetary Policy Meeting calendar.

        Arguments:
          dry_run — default True. No HTTP, no DB writes.

        Dry-run returns the indicator plan only. Execute mode fetches
        ``boj.or.jp/en/mopo/mpmsche_minu/`` once, parses each MPM row
        into a ``BOJ_RATE`` event, and upserts via the shared merge-
        rule projector.
        """
        from ingestion.calendar.boj_api import fetch_boj_calendar

        dry_run = bool(arguments.get("dry_run", True))

        if dry_run:
            return {
                "dry_run":            True,
                "indicators_planned": ["BOJ_RATE"],
                "stopped_reason":     "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_boj_calendar(connection, dry_run=False)
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

    def _op_calendar_econ_fetch_boj_values(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape BoJ statement pages and fill ``actual`` on existing rows.

        Arguments:
          dry_run       — default True. No HTTP, no DB writes.
          closing_dates — optional list of ISO closing dates
                          (``["2025-03-19"]``). When omitted, the op
                          auto-discovers past MPM rows from
                          ``cal_econ_event`` whose ``actual`` is still
                          NULL.

        The ``provider_event_id`` written by this op matches the one
        from :func:`calendar_econ_fetch_boj` exactly (same closing-date
        ISO anchor), so the policy-rate value upserts onto the
        existing schedule row via the shared projector's merge CASE.
        """
        from datetime import date
        from ingestion.calendar.boj_api import fetch_boj_statement_values

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
            summary = fetch_boj_statement_values(
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

    def _op_calendar_econ_fetch_boj_tankan(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape the BoJ Tankan yoshi-index page.

        Arguments:
          dry_run — default True. No HTTP, no DB writes.

        Dry-run returns the indicator plan only. Execute mode fetches
        ``boj.or.jp/en/statistics/tk/yoshi/index.htm`` once, parses
        each release row into two ``(raw, event)`` tuples (Large
        Manufacturers + Large Non-Manufacturers DI), and upserts via
        :func:`project_schedule_events` so the value-side writer can
        fill ``actual`` in a later pass without losing rows.
        """
        from ingestion.calendar.boj_tankan_api import (
            ALL_INDICATORS,
            fetch_boj_tankan_calendar,
        )

        dry_run = bool(arguments.get("dry_run", True))

        if dry_run:
            return {
                "dry_run":            True,
                "indicators_planned": list(ALL_INDICATORS),
                "stopped_reason":     "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_boj_tankan_calendar(connection, dry_run=False)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "indicators_planned": summary.indicators_planned,
            "releases_parsed":    summary.releases_parsed,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_boj_tankan_values(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape Tankan outline pages and fill ``actual`` on existing rows.

        Arguments:
          dry_run          — default True. No HTTP, no DB writes.
          reference_dates  — optional list of ISO reference dates
                             (``["2026-03-01"]``). When omitted, the op
                             auto-discovers past Tankan rows from
                             ``cal_econ_event`` whose ``actual`` is
                             still NULL.

        The ``provider_event_id`` written by this op matches the one
        from :func:`calendar_econ_fetch_boj_tankan` exactly (same
        ``(indicator, reference_date)`` anchor), so the DI upserts
        onto the existing schedule row through the shared projector's
        merge CASE.
        """
        from datetime import date
        from ingestion.calendar.boj_tankan_api import (
            ALL_INDICATORS,
            fetch_boj_tankan_outlines,
        )

        dry_run = bool(arguments.get("dry_run", True))
        raw_refs = arguments.get("reference_dates") or []
        reference_dates: list[date] | None
        if raw_refs:
            reference_dates = [date.fromisoformat(str(d)) for d in raw_refs]
        else:
            reference_dates = None

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_boj_tankan_outlines(
                connection,
                dry_run=dry_run,
                reference_dates=reference_dates,
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
            "releases_planned":   summary.releases_planned,
            "releases_fetched":   summary.releases_fetched,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_failures":     summary.fetch_failures,
            "parse_failures":     summary.parse_failures,
            "stopped_reason":     "dry_run" if dry_run else None,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_mof(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape the MoF trade-statistics release calendar.

        Arguments:
          dry_run — default True. No HTTP, no DB writes.

        Dry-run returns the indicator plan only. Execute mode fetches
        ``customs.go.jp/toukei/calendar/calend_e.htm`` once, parses
        each Monthly Data release row, emits a ``(raw, event)``
        tuple per month, and upserts via
        :func:`project_schedule_events` so the value-side writer can
        fill ``actual`` in a later pass without losing rows.
        """
        from ingestion.calendar.mof_api import (
            ALL_INDICATORS,
            fetch_mof_calendar,
        )

        dry_run = bool(arguments.get("dry_run", True))

        if dry_run:
            return {
                "dry_run":            True,
                "indicators_planned": list(ALL_INDICATORS),
                "stopped_reason":     "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_mof_calendar(connection, dry_run=False)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "indicators_planned": summary.indicators_planned,
            "releases_parsed":    summary.releases_parsed,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_mof_values(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape MoF trade-report XMLs and fill ``actual`` on existing rows.

        Arguments:
          dry_run          — default True. No HTTP, no DB writes.
          reference_dates  — optional list of ISO reference dates
                             (``["2026-03-01"]``). When omitted, the op
                             auto-discovers past Balance-of-Trade rows
                             whose ``actual`` is still NULL.

        ``provider_event_id`` matches the schedule-side write exactly
        so the DI upserts onto the existing schedule row via the
        shared projector's merge CASE.
        """
        from datetime import date
        from ingestion.calendar.mof_api import (
            ALL_INDICATORS,
            fetch_mof_trade_values,
        )

        dry_run = bool(arguments.get("dry_run", True))
        raw_refs = arguments.get("reference_dates") or []
        reference_dates: list[date] | None
        if raw_refs:
            reference_dates = [date.fromisoformat(str(d)) for d in raw_refs]
        else:
            reference_dates = None

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_mof_trade_values(
                connection,
                dry_run=dry_run,
                reference_dates=reference_dates,
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
            "releases_planned":   summary.releases_planned,
            "releases_fetched":   summary.releases_fetched,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_failures":     summary.fetch_failures,
            "parse_failures":     summary.parse_failures,
            "stopped_reason":     "dry_run" if dry_run else None,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_cao(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape the Cabinet Office / ESRI release-schedule page.

        Arguments:
          dry_run — default True. No HTTP, no DB writes.

        Dry-run returns the indicator plan only. Execute mode fetches
        ``esri.cao.go.jp/en/stat/stat-schedule-e.html`` once, parses
        the Consumer Confidence column into one ``(raw, event)`` tuple
        per future release, and upserts via
        :func:`project_schedule_events` so the value-side writer can
        fill ``actual`` in a later pass without losing rows.
        """
        from ingestion.calendar.cao_api import (
            ALL_INDICATORS,
            fetch_cao_calendar,
        )

        dry_run = bool(arguments.get("dry_run", True))

        if dry_run:
            return {
                "dry_run":            True,
                "indicators_planned": list(ALL_INDICATORS),
                "stopped_reason":     "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_cao_calendar(connection, dry_run=False)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "indicators_planned": summary.indicators_planned,
            "releases_parsed":    summary.releases_parsed,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_cao_values(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape the Consumer Confidence landing page and fill ``actual``.

        Arguments:
          dry_run — default True. No HTTP, no DB writes.

        One GET per invocation — CAO overwrites ``shouhi-e.html`` on
        each release, so the op always reads the release currently
        on-screen. ``provider_event_id`` matches the schedule-side
        write exactly so the SA-series headline upserts onto the
        pending schedule row via the shared projector's merge CASE.
        """
        from ingestion.calendar.cao_api import (
            ALL_INDICATORS,
            fetch_cao_consumer_confidence_values,
        )

        dry_run = bool(arguments.get("dry_run", True))

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_cao_consumer_confidence_values(
                connection, dry_run=dry_run,
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
            "releases_planned":   summary.releases_planned,
            "releases_fetched":   summary.releases_fetched,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "overdue_references": summary.overdue_references,
            "fetch_failures":     summary.fetch_failures,
            "parse_failures":     summary.parse_failures,
            "stopped_reason":     "dry_run" if dry_run else None,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_cao_gdp(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape Cabinet Office / ESRI GDP archive pages.

        Arguments:
          dry_run       — default True. No HTTP, no DB writes.
          archive_years — optional list of integer archive years.

        Execute mode fetches the ESRI SNA archive index, picks the
        latest archive years, parses first and second preliminary GDP
        releases, and upserts staged schedule rows under provider
        ``cao``.
        """
        from ingestion.calendar.cao_gdp_api import (
            ALL_INDICATORS,
            fetch_cao_gdp_calendar,
        )

        dry_run = bool(arguments.get("dry_run", True))
        raw_years = arguments.get("archive_years") or []
        archive_years = [int(y) for y in raw_years] if raw_years else None

        if dry_run:
            return {
                "dry_run":            True,
                "indicators_planned": list(ALL_INDICATORS),
                "archive_years_planned": archive_years or [],
                "stopped_reason":     "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_cao_gdp_calendar(
                connection,
                dry_run=False,
                archive_years=archive_years,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "indicators_planned": summary.indicators_planned,
            "archive_years_planned": summary.archive_years_planned,
            "archive_pages_fetched": summary.archive_pages_fetched,
            "releases_parsed":    summary.releases_parsed,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_cao_gdp_values(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape CAO GDP report CSVs and fill ``actual``.

        Arguments:
          dry_run          — default True. No HTTP, no DB writes.
          reference_dates  — optional list of ISO quarter-end dates
                             (``["2025-09-30"]``). When omitted, the
                             op auto-discovers past staged GDP rows
                             whose ``actual`` is still NULL.
        """
        from datetime import date
        from ingestion.calendar.cao_gdp_api import (
            ALL_INDICATORS,
            fetch_cao_gdp_values,
        )

        dry_run = bool(arguments.get("dry_run", True))
        raw_refs = arguments.get("reference_dates") or []
        reference_dates: list[date] | None
        if raw_refs:
            reference_dates = [date.fromisoformat(str(d)) for d in raw_refs]
        else:
            reference_dates = None

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_cao_gdp_values(
                connection,
                dry_run=dry_run,
                reference_dates=reference_dates,
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
            "releases_planned":   summary.releases_planned,
            "releases_fetched":   summary.releases_fetched,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_failures":     summary.fetch_failures,
            "parse_failures":     summary.parse_failures,
            "stopped_reason":     "dry_run" if dry_run else None,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_stat_bureau(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape Statistics Bureau schedule surfaces for CPI and LFS.

        Arguments:
          dry_run — default True. No HTTP, no DB writes.
        """
        from ingestion.calendar.stat_bureau_api import (
            ALL_INDICATORS,
            fetch_stat_bureau_calendar,
        )

        dry_run = bool(arguments.get("dry_run", True))

        if dry_run:
            return {
                "dry_run":            True,
                "indicators_planned": list(ALL_INDICATORS),
                "stopped_reason":     "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_stat_bureau_calendar(connection, dry_run=False)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "indicators_planned": summary.indicators_planned,
            "releases_parsed":    summary.releases_parsed,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_failures":     summary.fetch_failures,
            "parse_failures":     summary.parse_failures,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_stat_bureau_values(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch e-Stat scalar values and fill ``actual``.

        Arguments:
          dry_run          — default True. No HTTP, no DB writes.
          reference_dates  — optional list of ISO reference dates
                             (``["2026-03-01"]``). When omitted, the
                             op auto-discovers past Statistics Bureau
                             rows whose ``actual`` is still NULL.
          app_id           — optional e-Stat application id. When
                             omitted, ``ESTAT_APP_ID`` is read from
                             the environment.
        """
        from datetime import date
        from ingestion.calendar.stat_bureau_api import (
            ALL_INDICATORS,
            fetch_stat_bureau_values,
        )

        dry_run = bool(arguments.get("dry_run", True))
        raw_refs = arguments.get("reference_dates") or []
        reference_dates: list[date] | None
        if raw_refs:
            reference_dates = [date.fromisoformat(str(d)) for d in raw_refs]
        else:
            reference_dates = None

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        app_id_raw = arguments.get("app_id")
        app_id = str(app_id_raw).strip() if app_id_raw else None

        connection = get_conn()
        try:
            summary = fetch_stat_bureau_values(
                connection,
                dry_run=dry_run,
                reference_dates=reference_dates,
                app_id=app_id,
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
            "releases_planned":   summary.releases_planned,
            "releases_fetched":   summary.releases_fetched,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_failures":     summary.fetch_failures,
            "parse_failures":     summary.parse_failures,
            "stopped_reason":     "dry_run" if dry_run else None,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_meti(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape METI schedule surfaces for IIP and Retail Sales.

        Arguments:
          dry_run — default True. No HTTP, no DB writes.
        """
        from ingestion.calendar.meti_api import (
            ALL_INDICATORS,
            fetch_meti_calendar,
        )

        dry_run = bool(arguments.get("dry_run", True))

        if dry_run:
            return {
                "dry_run":            True,
                "indicators_planned": list(ALL_INDICATORS),
                "stopped_reason":     "dry_run",
            }

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_meti_calendar(connection, dry_run=False)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":            False,
            "indicators_planned": summary.indicators_planned,
            "releases_parsed":    summary.releases_parsed,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_failures":     summary.fetch_failures,
            "parse_failures":     summary.parse_failures,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

    def _op_calendar_econ_fetch_meti_values(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Scrape METI value reports and fill ``actual``.

        Arguments:
          dry_run          — default True. No HTTP, no DB writes.
          reference_dates  — optional list of ISO reference dates
                             (``["2026-02-01"]``). When omitted, the
                             op auto-discovers past METI rows whose
                             ``actual`` is still NULL.
        """
        from datetime import date
        from ingestion.calendar.meti_api import (
            ALL_INDICATORS,
            fetch_meti_values,
        )

        dry_run = bool(arguments.get("dry_run", True))
        raw_refs = arguments.get("reference_dates") or []
        reference_dates: list[date] | None
        if raw_refs:
            reference_dates = [date.fromisoformat(str(d)) for d in raw_refs]
        else:
            reference_dates = None

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_meti_values(
                connection,
                dry_run=dry_run,
                reference_dates=reference_dates,
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
            "releases_planned":   summary.releases_planned,
            "releases_fetched":   summary.releases_fetched,
            "rows_raw_inserted":  summary.rows_raw_inserted,
            "events_upserted":    summary.events_upserted,
            "fetch_failures":     summary.fetch_failures,
            "parse_failures":     summary.parse_failures,
            "stale_references":   summary.stale_references,
            "stopped_reason":     "dry_run" if dry_run else None,
            "wall_seconds":       round(summary.wall_seconds, 3),
        }

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
                         ``["bls", "bea", "census", "ism", "umich",
                         "conference-board", "nar", "ecb", "eia", "dol",
                         "eurostat", "destatis", "zew", "ifo", "gfk", "hcob",
                         "ec-bcs", "insee", "ine", "istat",
                         "fed-values", "nbs-values",
                         "stat-bureau-jp-values", "boj-values",
                         "boj-tankan-values", "mof-jp-values", "cao-values",
                         "cao-gdp-values", "meti-values"]``.
          start_year   — optional int; default ``current_year − 1``.
                         Applied to BLS / BEA / Census / Destatis.
          end_year     — optional int; default current year. BLS / BEA / Census / Destatis.
          start_period — optional SDMX period string (ECB / Eurostat).
          end_period   — optional SDMX period string (ECB / Eurostat).

        NBS contributes schedule-side rows for every indicator; the
        ``nbs-values`` connector (issue #49) fills ``actual`` for CPI
        / PPI / Industrial Production / Fixed Asset Investment /
        Retail Sales from the English press-release listing. PMI /
        GDP remain schedule-only.
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

        # Tail-of-sweep evidence archival (issue #36). Skipped on dry-run
        # because nothing new could have been written. Failures are
        # swallowed so an archive-side outage never fails the sweep run.
        evidence_summary: dict[str, int] = {"scanned": 0, "archived": 0, "failed": 0}
        if not dry_run:
            from ingestion.calendar.evidence_archive import archive_pending
            try:
                evidence_conn = get_conn()
                try:
                    evidence_summary = archive_pending(evidence_conn)
                    evidence_conn.commit()
                finally:
                    evidence_conn.close()
            except Exception as exc:  # pragma: no cover — defensive
                evidence_summary = {
                    "scanned": 0, "archived": 0, "failed": 0,
                    "error": repr(exc),  # type: ignore[dict-item]
                }

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
            "evidence_archive":    evidence_summary,
        }

    def _op_calendar_econ_refresh_schedules(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Invoke every official-source connector's schedule scrape.

        Daily-cron candidate for the recurring-fetch scheduler
        (:mod:`ingestion.calendar.scheduler`). Iterates BLS + BEA +
        Census + ISM + U Michigan + Conference Board + NAR + ECB + Eurostat + Destatis + ZEW + Ifo + GfK + HCOB +
        INSEE + INE + ISTAT +
        Fed FOMC + Fed releasedates + NBS + BoJ + BoJ Tankan +
        Statistics Bureau JP + MoF JP + CAO + CAO GDP + METI in order, isolating
        per-connector exceptions so one upstream outage rolls back
        only that connector. Each connector gets its own connection /
        commit / rollback lifecycle.

        Arguments:
          dry_run    — default True. No HTTP, no DB writes.
          connectors — optional list subset of
                       ``["bls","bea","census","ism","umich",
                       "conference-board","nar","ecb","eurostat","destatis","zew","ifo","gfk","hcob","ec-bcs","insee","ine","istat","fed-fomc",
                       "fed-releases","nbs","stat-bureau-jp","boj","boj-tankan",
                       "mof-jp","cao","cao-gdp","meti"]``;
                       omit to run the full roster.
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
          as_of        — optional ISO-8601 UTC cutoff for point-in-time
                         queries (issue #65). Each economic row in the
                         page is reconciled against
                         ``calendar_event_vintages`` so values reflect
                         what was on the wire at ``as_of``. Corporate
                         rows surface a ``meta.as_of_corp_unsupported``
                         flag until C2 ships. Future ``as_of`` is
                         rejected (``error`` in the envelope) so
                         callers cannot mask look-ahead bias as PIT.
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
        as_of_raw = (arguments.get("as_of") or "").strip()
        as_of: str | None = None
        if as_of_raw:
            try:
                parsed_as_of = datetime.fromisoformat(
                    as_of_raw.replace("Z", "+00:00"),
                )
            except ValueError:
                return {"error": f"invalid as_of: {as_of_raw!r}"}
            if parsed_as_of.tzinfo is None:
                parsed_as_of = parsed_as_of.replace(tzinfo=timezone.utc)
            else:
                parsed_as_of = parsed_as_of.astimezone(timezone.utc)
            if parsed_as_of > datetime.now(timezone.utc):
                return {"error": "as_of must not be in the future"}
            as_of = parsed_as_of.isoformat()
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
            as_of=as_of,
            offset=offset,
            limit=limit,
        )
        next_cursor: dict[str, int] | None = None
        if offset + len(items) < total:
            next_cursor = {
                "page_offset": offset + len(items),
                "page_limit":  limit,
            }
        meta: dict[str, Any] = {
            "count":  total,
            "offset": offset,
            "limit":  limit,
        }
        # Surface the C2 gap explicitly when ``as_of`` is set and any
        # corporate row landed on this page. Downstream PIT consumers
        # need to know they're seeing latest-snapshot values for those
        # rows so they don't silently bake look-ahead into a backtest.
        if as_of and any(item.get("domain") == "corporate" for item in items):
            meta["as_of_corp_unsupported"] = True
        return {
            "data":  items,
            "meta":  meta,
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

    def _op_calendar_econ_fetch_nbs_values(
        self, arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Sweep the NBS English press-release listing for due actuals (issue #49).

        Auto-discovers ``actual IS NULL`` rows already staged by
        ``calendar_econ_fetch_nbs`` whose ``event_time_utc`` has
        passed, resolves each release's article URL on the public
        press-release listing, downloads the article, parses the
        headline value (CPI / PPI / Industrial Production / Fixed
        Asset Investment / Retail Sales), and upserts via the shared
        ``provider_event_id``.

        Arguments:
          dry_run — default True. No HTTP, no DB writes.

        Dry-run returns the value-side indicator plan; execute mode
        runs the sweep.
        """
        from ingestion.calendar.nbs_api import fetch_nbs_values

        dry_run = bool(arguments.get("dry_run", True))

        get_conn = getattr(self._store, "get_connection", None)
        if not callable(get_conn):
            return {"error": "store does not expose get_connection"}

        connection = get_conn()
        try:
            summary = fetch_nbs_values(connection, dry_run=dry_run)
            if not dry_run:
                connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "dry_run":             summary.dry_run,
            "indicators_planned":  summary.indicators_planned,
            "pending_releases":    summary.pending_releases,
            "listing_misses":      summary.listing_misses,
            "observations_seen":   summary.observations_seen,
            "rows_raw_inserted":   summary.rows_raw_inserted,
            "events_upserted":     summary.events_upserted,
            "series_ok":           summary.series_ok,
            "series_empty":        summary.series_empty,
            "series_failed":       summary.series_failed,
            "stopped_reason":      "dry_run" if dry_run else None,
            "wall_seconds":        round(summary.wall_seconds, 3),
        }

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

    def _op_get_today_calendar(self, arguments: dict[str, Any]) -> dict[str, Any]:
        events = self._store.list_today_events(
            importance=arguments.get("importance"),
            country=arguments.get("country"),
            category=arguments.get("category"),
        )
        return {"events": [self._event_to_dict(event) for event in events]}

    def _op_fetch_live_calendar(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        return {
            "retired": True,
            "total_fetched": 0,
            "returned": 0,
            "events": [],
            "replacement": {
                "read": "GET /v1/calendar or service op list_calendar_items",
                "schedule_refresh": "calendar_econ_refresh_schedules",
                "value_sweep": "calendar_econ_sweep_values",
            },
        }

    def _event_to_dict(self, event: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "source": event.source,
            "event_id": (
                f"{event.source}:{event.event_id}"
                if ":" not in str(event.event_id)
                else event.event_id
            ),
            "provider_event_id": event.event_id,
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
        event_time_utc = getattr(event, "event_time_utc", "")
        if event_time_utc:
            payload["event_time_utc"] = event_time_utc
            payload["event_time_precision"] = getattr(
                event,
                "event_time_precision",
                "datetime",
            )
        return payload
