"""EODHD global market-data provider.

Mirrors ``TiingoMarketDataProvider`` but targets EODHD's ``/api/eod`` for
global (non-US) equities, ETFs, indices, FX, crypto, and spot metals.
Reuses the market-layer schema introduced in issue #1 P0 and shares the
quality-check helpers defined alongside the Tiingo provider.

Corporate actions (issue #67 slice 2): the EOD endpoint does not ship
dividend/split metadata, so each bar lands with ``div_cash=0`` /
``split_factor=1`` and ``has_missing_corp_acts=True``. The
:meth:`EODHDMarketDataProvider.refresh_corp_actions` method calls the
per-ticker ``/api/div`` and ``/api/splits`` endpoints, lands raw rows in
the ``market_corp_actions_raw`` audit table, then projects the latest
snapshot per ``(action_type, event_date)`` into the matching
``market_price_bars`` row's ``dividend_cash`` / ``split_factor`` and
clears the missing-CA flag.

Two-layer write — mandatory, not optional. The audit lane is symmetric to
``cal_corp_raw`` on the calendar side: every ``market_price_bars`` corp-
action value is reproducible from raw, so a projection bug can be fixed
and re-run without re-fetching from EODHD (zero quota cost). Restated
dividends keep prior versions as separate raw rows (different
``content_hash``), preserving the revision chain.

NOT redundant with the calendar lane (``cal_corp_*``):

* Calendar lane indexes corp-action rows **by date window** with a 2015
  floor (per EODHD docs); per-ticker history reaches back to instrument
  inception.
* Calendar lane writes event-shaped rows for "what events this week";
  this lane writes bar-shaped values for "compute total return / adjusted
  close".
* A 2020-08-31 AAPL 4:1 split exists in both lanes — same EODHD source
  data, two different storage shapes for two different consumers. Do
  NOT deduplicate across tables; same key is impossible (events vs
  bars are different grains).
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from ingestion.market._bars_canonicalize import bars_content_hash
from ingestion.market._eodhd_universe import (
    EODHD_GLOBAL_UNIVERSE,
    EODHDUniverseEntry,
)
from ingestion.market.clients._tiingo import (
    DEFAULT_BREAK_THRESHOLD,
    PRE2018_CUTOFF,
    check_adjustment_applied,
    check_ohlc_sanity,
    detect_history_breaks,
)
from ingestion.market.scrapers._eodhd import (
    EODHDAPIError,
    EODHDClient,
    EODHDDailyBar,
    EODHDDividend,
    EODHDNotFoundError,
    EODHDSplit,
)
from storage import (
    MarketCorpActionsRawRecord,
    MarketInstrumentRecord,
    MarketPriceBarRecord,
    MarketPriceBarsRawRecord,
    MarketSymbolHistoryRecord,
    SQLiteEngineStore,
)

logger = logging.getLogger(__name__)


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


# Asset classes that can carry corporate actions (dividends/splits) and
# can be "delisted". FX, crypto, and spot metals are continuous-tape
# instruments — there are no splits, no dividends, and the "pre-2018
# delisted" probe doesn't apply. Bars for these asset classes must NOT
# carry has_missing_corp_acts / has_pre2018_delisted flags, otherwise
# downstream agents will surface bogus quality warnings on every FX /
# crypto / spot-metal bar.
#
# bond_etf / commodity_etf are kept in the bearing set: bond ETFs make
# regular distributions, and commodity ETFs do split. Indices are kept
# even though indices themselves don't pay dividends because EODHD
# returns adjusted_close = close for indices and the missing-CA flag
# remains the right signal for "we don't have an underlying-basket
# total-return view yet".
_CORP_ACTION_BEARING_ASSET_CLASSES = frozenset({
    "equity", "equity_etf", "bond_etf", "commodity_etf", "index",
})


class EODHDMarketDataProvider:
    """High-level EODHD provider for global (non-US) coverage."""

    source_name = "eodhd"

    def __init__(
        self,
        client: EODHDClient | None = None,
        *,
        universe: tuple[EODHDUniverseEntry, ...] = EODHD_GLOBAL_UNIVERSE,
        break_threshold: float = DEFAULT_BREAK_THRESHOLD,
        request_sleep: float = 0.25,
    ) -> None:
        self.client = client or EODHDClient()
        self.universe = universe
        self.break_threshold = break_threshold
        self.request_sleep = request_sleep

    # -- seeding ------------------------------------------------------------

    def seed_universe(self, store: SQLiteEngineStore) -> int:
        for entry in self.universe:
            self._seed_single_entry(store, entry)
        return len(self.universe)

    @staticmethod
    def _seed_single_entry(
        store: SQLiteEngineStore, entry: EODHDUniverseEntry
    ) -> None:
        existing = store.get_market_instrument(entry.instrument_id)
        history_status = (
            existing.history_status if existing is not None else "provider_continuous"
        )
        store.upsert_market_instrument(
            MarketInstrumentRecord(
                instrument_id=entry.instrument_id,
                primary_ticker=entry.primary_ticker,
                name=entry.name,
                asset_class=entry.asset_class,
                market=entry.market,
                exchange_code=entry.exchange_code,
                currency=entry.currency,
                isin=entry.isin,
                composite_figi=entry.composite_figi,
                share_class_figi=entry.share_class_figi,
                primary_provider="eodhd",
                provider_symbols_json={"eodhd": entry.eodhd_ticker},
                history_status=history_status,
                description_for_agent=entry.description_for_agent,
            )
        )
        store.upsert_market_symbol_segment(
            MarketSymbolHistoryRecord(
                segment_id=f"{entry.instrument_id}:eodhd:{entry.eodhd_ticker}",
                instrument_id=entry.instrument_id,
                ticker=entry.eodhd_ticker,
                provider_name="eodhd",
                exchange_code=entry.exchange_code,
                isin=entry.isin,
                figi=entry.composite_figi,
                valid_from="1900-01-01",
                valid_to="",
                event_type="listing_start",
                mapping_confidence="provider_native",
                source_name="eodhd.eod.meta",
                raw_json={"seed": True},
            )
        )

    # -- provider API ------------------------------------------------------

    def get_daily_bars(
        self,
        ticker: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[EODHDDailyBar]:
        return self.client.get_daily_bars(ticker, start_date=start_date, end_date=end_date)

    def refresh_market_history(
        self,
        store: SQLiteEngineStore,
        symbol: str,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> RefreshStats:
        """Fetch EODHD bars and persist into ``market_price_bars``.

        ``symbol`` accepts either the full EODHD ticker (``N225.INDX``) or a
        bare primary ticker (``N225``) when it unambiguously resolves
        through the seeded universe.

        After upserting bars, this method calls
        :meth:`_reproject_corp_actions` to re-apply previously-projected
        ``dividend_cash`` / ``split_factor`` values (which the
        INSERT OR REPLACE bar upsert would otherwise wipe).

        Trade-off — ``has_missing_corp_acts`` policy:
            ``refresh_market_history`` does NOT clear the missing-CA
            flag across the bar window. Only event-date bars get
            cleared via the projection's targeted UPDATE. Bars between
            events that an earlier ``refresh_corp_actions`` had
            cleared will be re-flagged here. Re-running
            ``refresh_corp_actions`` (which knows the audit window)
            clears them again. This keeps the missing-CA flag's
            semantic clean: "no audit lane has confirmed this bar's
            corp-action coverage" rather than "we'd guess we have
            coverage based on raw min/max".
        """
        entry = self._resolve_universe_entry(symbol)
        if entry is None:
            existing = store.find_market_instrument_by_ticker(symbol)
            if existing is None or existing.primary_provider != "eodhd":
                logger.warning("refresh_market_history(eodhd): %s not in seeded universe", symbol)
                return RefreshStats(source="eodhd", count=0)
            eodhd_ticker = existing.provider_symbols_json.get("eodhd", existing.primary_ticker)
            entry = self._entry_from_existing(existing, eodhd_ticker)
        elif store.get_market_instrument(entry.instrument_id) is None:
            self._seed_single_entry(store, entry)

        try:
            bars, raw_payload, request_params = self.client.get_daily_bars_with_raw(
                entry.eodhd_ticker, start_date=start, end_date=end,
            )
        except EODHDNotFoundError:
            logger.warning("EODHD ticker not found: %s", entry.eodhd_ticker)
            return RefreshStats(source="eodhd", count=0)
        except EODHDAPIError:
            logger.warning("EODHD fetch failed for %s", entry.eodhd_ticker, exc_info=True)
            return RefreshStats(source="eodhd", count=0)

        # Issue #69 slice 2: capture raw payload BEFORE the empty-bars
        # short-circuit. If the parser starts returning zero bars after a
        # provider field rename, the audit lane is exactly what's needed
        # to re-project once the parser is fixed — discarding the body
        # there would mean burning EODHD quota to re-fetch.
        if raw_payload:
            self._capture_bars_raw(
                store, entry.eodhd_ticker, raw_payload, request_params,
            )

        if not bars:
            return RefreshStats(source="eodhd", count=0)

        adjustment_applied = check_adjustment_applied(bars)
        # FX / crypto / spot-metal lines have no corporate actions and
        # don't "delist" in the equity sense — gate the equity-only flags
        # to corp-action-bearing asset classes so non-equity bars don't
        # surface bogus warnings to downstream agents.
        carries_corp_actions = entry.asset_class in _CORP_ACTION_BEARING_ASSET_CLASSES
        # Break detection: run when adjustments are present (so the series
        # is comparable across corporate actions) OR when the series has
        # no corporate actions to begin with — for FX/crypto/spot-metal
        # tapes a provider splice or scale jump still needs to surface as
        # has_break_detected, even though adjusted_close equals close
        # because there's nothing to adjust for.
        break_dates = (
            detect_history_breaks(bars, threshold=self.break_threshold)
            if adjustment_applied or not carries_corp_actions
            else []
        )
        break_set = set(break_dates)

        segment_id = f"{entry.instrument_id}:eodhd:{entry.eodhd_ticker}"
        count = 0
        for bar in bars:
            flags_json: dict[str, Any] = {}
            if not check_ohlc_sanity(bar):
                flags_json["ohlc_sanity"] = "failed"
            if not adjustment_applied:
                flags_json["adjustment_check"] = "raw_only"
            if carries_corp_actions:
                flags_json["corp_acts_missing"] = "eodhd_eod_endpoint_has_no_div_split"
            if bar.date in break_set:
                flags_json["break_detected"] = True

            store.upsert_market_price_bar(
                MarketPriceBarRecord(
                    instrument_id=entry.instrument_id,
                    source_segment_id=segment_id,
                    date=bar.date,
                    bar_interval="1d",
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    adjusted_open=bar.adj_open,
                    adjusted_high=bar.adj_high,
                    adjusted_low=bar.adj_low,
                    adjusted_close=bar.adj_close,
                    adjusted_volume=bar.adj_volume,
                    dividend_cash=bar.div_cash,
                    split_factor=bar.split_factor,
                    source_name="EODHD",
                    source_symbol=entry.eodhd_ticker,
                    has_break_detected=bar.date in break_set,
                    has_pre2018_delisted=(
                        carries_corp_actions
                        and bar.date < PRE2018_CUTOFF
                        and not adjustment_applied
                    ),
                    has_missing_corp_acts=carries_corp_actions,
                    has_mapping_review_needed=False,
                    quality_flags_json=flags_json,
                )
            )
            count += 1

        if break_dates:
            # Only upgrade history_status; never downgrade a prior alert
            # on a partial-window refresh.
            store.update_instrument_history_status(entry.instrument_id, "break_detected")

        # Re-project corp actions from market_corp_actions_raw onto the
        # bars we just upserted — without this, a refresh would wipe any
        # dividend_cash / split_factor that an earlier refresh_corp_actions
        # run had projected (the bar PK collision means INSERT OR REPLACE
        # rewrites the row to the EOD-endpoint defaults). Cheap when raw
        # is empty (one SELECT, no UPDATEs); cheap when raw has rows
        # because the index ``idx_market_corp_actions_raw_latest`` covers
        # the lookup.
        if carries_corp_actions:
            self._reproject_corp_actions(store, entry)

        return RefreshStats(source="eodhd", count=count)

    @staticmethod
    def _capture_bars_raw(
        store: SQLiteEngineStore,
        ticker: str,
        payload: list[dict[str, Any]],
        request_params: dict[str, str],
    ) -> int:
        """Land one ``market_price_bars_raw`` row for the fetched payload.

        Idempotent — same canonicalized hash dedupes via INSERT OR
        IGNORE. Issue #69 slice 2.
        """
        snapshot_epoch_ms = int(datetime.now(UTC).timestamp() * 1000)
        record = MarketPriceBarsRawRecord(
            provider="eodhd",
            ticker=ticker,
            snapshot_epoch_ms=snapshot_epoch_ms,
            content_hash=bars_content_hash(payload),
            payload_json=json.dumps(payload, sort_keys=True, ensure_ascii=False),
            fetched_at=datetime.now(UTC).isoformat(),
            request_params_json=json.dumps(request_params, sort_keys=True),
        )
        try:
            return store.insert_market_price_bars_raw([record])
        except Exception:
            logger.warning(
                "market_price_bars_raw write failed for %s", ticker, exc_info=True,
            )
            return 0

    @staticmethod
    def _reproject_corp_actions(
        store: SQLiteEngineStore,
        entry: EODHDUniverseEntry,
    ) -> int:
        """Read the latest snapshot per ``(action_type, event_date)``
        from ``market_corp_actions_raw`` and UPDATE the matching bars.
        Returns the number of bar rows updated by the projection.

        This helper is window-agnostic: it only touches bars that have
        an event in raw. The targeted
        :meth:`SQLiteEngineStore.update_market_price_bar_corp_action`
        clears ``has_missing_corp_acts`` on those specific dates as a
        side-effect (the bar now carries an authoritative value).
        Bars without events keep whatever flag they had before.

        Audit-window clearing of ``has_missing_corp_acts`` is the
        responsibility of ``refresh_corp_actions`` exclusively because
        only that callsite knows the user-supplied audit bounds.
        Routine ``refresh_market_history`` callers don't know whether
        a window has been audited, so they shouldn't clear flags
        outside event dates. Trade-off documented on
        :meth:`refresh_market_history` — between
        ``refresh_corp_actions`` runs, a bar refresh will re-flag
        no-event bars; the next ``refresh_corp_actions`` clears them
        again.

        Same-day dividend rows (e.g. regular + special) accumulate via
        ``+=`` so a quarterly + special-distribution day lands the
        combined cash on the bar instead of whichever row was iterated
        last.
        """
        latest = store.latest_market_corp_actions_for_ticker(
            provider="eodhd", ticker=entry.eodhd_ticker,
        )
        if not latest:
            return 0
        merged: dict[str, dict[str, float]] = {}
        for rec in latest:
            payload = json.loads(rec.payload_json)
            slot = merged.setdefault(rec.event_date, {})
            if rec.action_type == "dividend":
                value = payload.get("value")
                if value is not None:
                    try:
                        # Accumulate: same ex-date can carry multiple
                        # rows (regular + special). Last-write-wins
                        # would silently drop the special.
                        slot["dividend_cash"] = (
                            slot.get("dividend_cash", 0.0) + float(value)
                        )
                    except (TypeError, ValueError):
                        continue
            elif rec.action_type == "split":
                ratio = str(payload.get("split", ""))
                if "/" in ratio:
                    new_part, _, old_part = ratio.partition("/")
                    try:
                        new_v = float(new_part)
                        old_v = float(old_part)
                    except (TypeError, ValueError):
                        continue
                    if old_v != 0:
                        slot["split_factor"] = new_v / old_v
        projected_bars = 0
        for event_date, fields in merged.items():
            if not fields:
                continue
            projected_bars += store.update_market_price_bar_corp_action(
                instrument_id=entry.instrument_id,
                date=event_date,
                dividend_cash=fields.get("dividend_cash"),
                split_factor=fields.get("split_factor"),
            )
        return projected_bars

    def refresh_corp_actions(
        self,
        store: SQLiteEngineStore,
        symbol: str,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> RefreshStats:
        """Fetch dividends + splits for ``symbol`` and project into bars.

        Two-layer write (mandatory):

        1. Pull raw rows via ``client.get_historical_dividends`` /
           ``get_historical_splits``.
        2. Insert into ``market_corp_actions_raw`` (idempotent on
           content_hash; revisions preserved as separate rows).
        3. Re-read the latest snapshot per ``(action_type, event_date)``
           **from raw**, then UPDATE the corresponding
           ``market_price_bars`` row's ``dividend_cash`` /
           ``split_factor``.

        Step 3 reads from raw rather than from the in-memory response so
        the audit lane is load-bearing — projection logic can be re-run
        in isolation without a second EODHD call. The PK choice
        (``action_type, event_date`` per ticker) means a re-run on
        unchanged data is a no-op.

        Returns ``RefreshStats(source="eodhd", count=<rows projected>)``
        — the count of bars whose corp-action fields were written this
        run (raw inserts can be much higher when revisions accumulate;
        ``count`` here mirrors the price-bar lane's "rows that landed
        in the read-side table" semantics).
        """
        entry = self._resolve_universe_entry(symbol)
        if entry is None:
            existing = store.find_market_instrument_by_ticker(symbol)
            if existing is None or existing.primary_provider != "eodhd":
                logger.warning(
                    "refresh_corp_actions(eodhd): %s not in seeded universe", symbol
                )
                return RefreshStats(source="eodhd", count=0)
            eodhd_ticker = existing.provider_symbols_json.get(
                "eodhd", existing.primary_ticker
            )
            entry = self._entry_from_existing(existing, eodhd_ticker)

        try:
            dividends = self.client.get_historical_dividends(
                entry.eodhd_ticker, start_date=start, end_date=end
            )
            splits = self.client.get_historical_splits(
                entry.eodhd_ticker, start_date=start, end_date=end
            )
        except EODHDNotFoundError:
            logger.warning(
                "EODHD corp-action endpoint not found for %s", entry.eodhd_ticker
            )
            return RefreshStats(source="eodhd", count=0)
        except EODHDAPIError:
            logger.warning(
                "EODHD corp-action fetch failed for %s",
                entry.eodhd_ticker, exc_info=True,
            )
            return RefreshStats(source="eodhd", count=0)

        snapshot_epoch_ms = int(datetime.now(UTC).timestamp() * 1000)
        fetched_at = datetime.now(UTC).isoformat()

        raw_records: list[MarketCorpActionsRawRecord] = []
        for d in dividends:
            raw_records.append(
                _build_dividend_raw(d, snapshot_epoch_ms, fetched_at)
            )
        for s in splits:
            raw_records.append(
                _build_split_raw(s, snapshot_epoch_ms, fetched_at)
            )
        store.insert_market_corp_actions_raw(raw_records)

        # Project every (action_type, event_date) snapshot onto the
        # matching bars. Reads from raw, not from the in-memory
        # response, so a future projection-only re-run produces
        # identical results without re-fetching from EODHD.
        projected_bars = self._reproject_corp_actions(store, entry)

        # Clear missing-CA across the window the caller asked us to
        # refresh — that IS the audit window, distinct from the
        # inferred-from-raw window the reprojection helper uses for
        # routine refresh_market_history calls. When raw was empty
        # (verified-empty window) the helper above no-op'd; here we
        # still know the audit window covered [start, end] so the
        # clear is the right thing to do.
        store.clear_market_price_bars_missing_corp_acts(
            instrument_id=entry.instrument_id,
            start=start,
            end=end,
        )

        return RefreshStats(source="eodhd", count=projected_bars)

    def refresh_universe(
        self,
        store: SQLiteEngineStore,
        *,
        lookback_days: int = 365,
    ) -> RefreshStats:
        self.seed_universe(store)
        start = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        total = 0
        for entry in self.universe:
            stats = self.refresh_market_history(store, entry.eodhd_ticker, start=start)
            total += stats.count
            if self.request_sleep:
                time.sleep(self.request_sleep)
        return RefreshStats(source="eodhd", count=total)

    # -- read API ----------------------------------------------------------

    def get_market_history(
        self,
        store: SQLiteEngineStore,
        symbol: str,
        *,
        start: str | None = None,
        end: str | None = None,
        adjusted: bool = True,
    ) -> list[dict[str, Any]]:
        instrument = self._resolve_instrument(store, symbol)
        if instrument is None:
            return []
        bars = store.list_market_price_bars(instrument.instrument_id, start=start, end=end)
        rows: list[dict[str, Any]] = []
        for bar in bars:
            flags: list[str] = []
            if bar.has_break_detected:
                flags.append("break_detected")
            if bar.has_missing_corp_acts:
                flags.append("missing_corp_acts")
            if bar.has_pre2018_delisted:
                flags.append("pre2018_delisted")
            if bar.has_mapping_review_needed:
                flags.append("mapping_review_needed")

            if adjusted:
                price_open = bar.adjusted_open if bar.adjusted_open is not None else bar.open
                price_high = bar.adjusted_high if bar.adjusted_high is not None else bar.high
                price_low = bar.adjusted_low if bar.adjusted_low is not None else bar.low
                price_close = bar.adjusted_close if bar.adjusted_close is not None else bar.close
                price_volume = bar.adjusted_volume if bar.adjusted_volume is not None else bar.volume
            else:
                price_open, price_high, price_low = bar.open, bar.high, bar.low
                price_close, price_volume = bar.close, bar.volume

            rows.append(
                {
                    "instrument_id": instrument.instrument_id,
                    "ticker": instrument.primary_ticker,
                    "name": instrument.name,
                    "market": instrument.market,
                    "isin": instrument.isin,
                    "openfigi": instrument.openfigi or instrument.composite_figi,
                    "history_status": instrument.history_status,
                    "date": bar.date,
                    "open": price_open,
                    "high": price_high,
                    "low": price_low,
                    "close": price_close,
                    "adjusted_close": bar.adjusted_close if bar.adjusted_close is not None else bar.close,
                    "volume": price_volume,
                    "quality_flags": flags,
                    "source": bar.source_name,
                    "agent_summary": _agent_summary(instrument, bar, price_close),
                }
            )
        return rows

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _entry_from_existing(
        existing: MarketInstrumentRecord, eodhd_ticker: str
    ) -> EODHDUniverseEntry:
        """Rebuild an ``EODHDUniverseEntry`` from a stored instrument
        when the symbol resolves via the DB rather than the seeded
        universe — same shape as the seed-time entry, used by both
        :meth:`refresh_market_history` and :meth:`refresh_corp_actions`."""
        return EODHDUniverseEntry(
            instrument_id=existing.instrument_id,
            eodhd_ticker=eodhd_ticker,
            primary_ticker=existing.primary_ticker,
            exchange_code=existing.exchange_code,
            name=existing.name,
            asset_class=existing.asset_class,
            market=existing.market,
            currency=existing.currency,
            isin=existing.isin,
            composite_figi=existing.composite_figi,
            share_class_figi=existing.share_class_figi,
            description_for_agent=existing.description_for_agent,
        )

    def _resolve_universe_entry(self, symbol: str) -> EODHDUniverseEntry | None:
        # Look through the instance's own universe first so callers can pass
        # a custom tuple without falling through to the module-level default.
        for candidate in self.universe:
            if symbol in (candidate.instrument_id, candidate.eodhd_ticker):
                return candidate
        bare = symbol.upper()
        for candidate in self.universe:
            if candidate.primary_ticker == bare:
                return candidate
        return None

    def _resolve_instrument(
        self, store: SQLiteEngineStore, symbol: str
    ) -> MarketInstrumentRecord | None:
        entry = self._resolve_universe_entry(symbol)
        if entry is not None:
            return store.get_market_instrument(entry.instrument_id)
        return store.find_market_instrument_by_ticker(symbol)


# ── Slice-2 raw-record builders (issue #67) ───────────────────────────────
# Mutable-field sets follow the calendar-lane parser's contract: include
# every field EODHD can revise between snapshots, exclude identity fields
# already in the PK. Hash format = sha256 over "key=value|key=value|..." in
# sorted-key order, missing/null normalized to "".
_DIVIDEND_MUTABLE_FIELDS = (
    "value", "unadjustedValue", "currency", "period",
    "declarationDate", "recordDate", "paymentDate",
)
_SPLIT_MUTABLE_FIELDS = ("split",)


def _content_hash(row: dict[str, Any], fields: tuple[str, ...]) -> str:
    parts = []
    for key in sorted(fields):
        value = row.get(key)
        parts.append("" if value is None else str(value))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _stable_json(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, ensure_ascii=False)


def _build_dividend_raw(
    div: EODHDDividend, snapshot_epoch_ms: int, fetched_at: str
) -> MarketCorpActionsRawRecord:
    payload = {
        "date": div.date,
        "declarationDate": div.declaration_date,
        "recordDate": div.record_date,
        "paymentDate": div.payment_date,
        "period": div.period,
        "value": div.value,
        "unadjustedValue": div.unadjusted_value,
        "currency": div.currency,
    }
    return MarketCorpActionsRawRecord(
        provider="eodhd",
        ticker=div.ticker,
        action_type="dividend",
        event_date=div.date,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=_content_hash(payload, _DIVIDEND_MUTABLE_FIELDS),
        payload_json=_stable_json(payload),
        fetched_at=fetched_at,
    )


def _build_split_raw(
    split: EODHDSplit, snapshot_epoch_ms: int, fetched_at: str
) -> MarketCorpActionsRawRecord:
    payload = {"date": split.date, "split": split.raw_ratio}
    return MarketCorpActionsRawRecord(
        provider="eodhd",
        ticker=split.ticker,
        action_type="split",
        event_date=split.date,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=_content_hash(payload, _SPLIT_MUTABLE_FIELDS),
        payload_json=_stable_json(payload),
        fetched_at=fetched_at,
    )


def _agent_summary(
    instrument: MarketInstrumentRecord,
    bar: MarketPriceBarRecord,
    close: float,
) -> str:
    status_phrase = {
        "provider_continuous": "its history is provider-continuous through EODHD",
        "break_detected": "its history has an unresolved break (pending lazy repair)",
        "stitched": "its history was stitched from multiple ticker segments",
        "manual_review": "its history is flagged for manual review",
    }.get(instrument.history_status, f"history_status={instrument.history_status}")
    return (
        f"{instrument.primary_ticker} closed at {close:.2f} {instrument.currency} "
        f"on {bar.date}. {status_phrase[0].upper()}{status_phrase[1:]}."
    )
