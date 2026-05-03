"""Lazy identity + history repair flow for break-detected instruments.

Implements the flow described in issue #1 under "Lazy Repair Flow":

    break_detected
      -> read local market_symbol_history for existing segments
      -> query EODHD search by ISIN / ticker (ID Mapping)
      -> query OpenFIGI by ISIN / ticker
      -> scan EODHD exchange-symbol-list with delisted=1
      -> fuzzy match by company name
      -> write market_symbol_history
      -> (re)fetch each segment
      -> stitch, rerun quality checks

The repair service is side-effect driven: it only writes to
``market_symbol_history`` and updates ``history_status``. Re-fetch of
underlying bars is handled by the EODHD provider through its normal
``refresh_market_history`` path; this service just publishes segment
metadata for ticker windows.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from ingestion.market.scrapers._eodhd_identity import (
    EODHDIdentityClient,
    EODHDSearchHit,
    EODHDSymbolChangeEvent,
)
from ingestion.market.scrapers._openfigi import OpenFIGIClient, OpenFIGIHit
from storage import (
    MarketInstrumentRecord,
    MarketSymbolHistoryRecord,
    SQLiteEngineStore,
)

logger = logging.getLogger(__name__)


@dataclass
class RepairReport:
    instrument_id: str
    segments_discovered: int = 0
    segments_written: int = 0
    confidence_breakdown: dict[str, int] = field(default_factory=dict)
    final_history_status: str = "break_detected"
    notes: list[str] = field(default_factory=list)


class IdentityRepairService:
    """Stitch break_detected histories by consulting EODHD + OpenFIGI.

    The service is deliberately idempotent: running it twice on the same
    instrument does not duplicate segments (segment_id is deterministic).
    Instruments without break_detected status are skipped.
    """

    source_name = "identity_repair"

    def __init__(
        self,
        *,
        eodhd: EODHDIdentityClient | None = None,
        openfigi: OpenFIGIClient | None = None,
        refetch_callback: Callable[[MarketInstrumentRecord, list[MarketSymbolHistoryRecord]], None] | None = None,
    ) -> None:
        self.eodhd = eodhd or EODHDIdentityClient()
        self.openfigi = openfigi or OpenFIGIClient()
        self.refetch_callback = refetch_callback

    # -- public entrypoints ------------------------------------------------

    def repair(self, store: SQLiteEngineStore, instrument_id: str) -> RepairReport:
        instrument = store.get_market_instrument(instrument_id)
        if instrument is None:
            return RepairReport(instrument_id=instrument_id, notes=["instrument_not_found"])
        if instrument.history_status != "break_detected":
            return RepairReport(
                instrument_id=instrument_id,
                final_history_status=instrument.history_status,
                notes=["skipped_no_break"],
            )
        return self._repair_instrument(store, instrument)

    def repair_all_breaks(self, store: SQLiteEngineStore) -> list[RepairReport]:
        """Run the repair flow for every instrument flagged break_detected."""
        reports: list[RepairReport] = []
        with store._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT instrument_id FROM market_instruments "
                "WHERE history_status = 'break_detected'"
            ).fetchall()
        for row in rows:
            reports.append(self.repair(store, row["instrument_id"]))
        return reports

    # -- core flow ---------------------------------------------------------

    def _repair_instrument(
        self, store: SQLiteEngineStore, instrument: MarketInstrumentRecord
    ) -> RepairReport:
        report = RepairReport(instrument_id=instrument.instrument_id)

        # Step 0: read local market_symbol_history.
        local_segments = store.list_symbol_segments(instrument.instrument_id)
        known_segment_ids = {seg.segment_id for seg in local_segments}
        report.notes.append(f"local_segments={len(local_segments)}")

        # Step 1..4: collect candidate segments from external sources.
        candidates = self._gather_candidates(instrument)
        report.segments_discovered = len(candidates)

        # Step 5: persist new segments.
        for candidate in candidates:
            if candidate.segment_id in known_segment_ids:
                continue
            store.upsert_market_symbol_segment(candidate)
            report.segments_written += 1
            report.confidence_breakdown[candidate.mapping_confidence] = (
                report.confidence_breakdown.get(candidate.mapping_confidence, 0) + 1
            )

        # Step 6: trigger refetch for each segment if caller provided one.
        if self.refetch_callback is not None and report.segments_written > 0:
            try:
                self.refetch_callback(instrument, list(candidates))
            except Exception:
                logger.warning(
                    "refetch_callback failed for %s", instrument.instrument_id, exc_info=True
                )
                report.notes.append("refetch_failed")

        # Step 7: promote history_status.
        # Only promote to "stitched" when a refetch callback actually ran —
        # writing segment metadata alone does not repair the has_break_detected
        # rows already persisted in market_price_bars. Without a refetch the
        # instrument keeps break_detected so downstream consumers still see
        # the original state.
        refetch_ran = (
            self.refetch_callback is not None
            and report.segments_written > 0
            and "refetch_failed" not in report.notes
        )
        if refetch_ran:
            store.update_instrument_history_status(instrument.instrument_id, "stitched")
            report.final_history_status = "stitched"
        elif report.segments_written > 0:
            report.final_history_status = instrument.history_status  # still break_detected
            report.notes.append("segments_discovered_pending_refetch")
        elif report.segments_discovered == 0:
            store.update_instrument_history_status(instrument.instrument_id, "manual_review")
            report.final_history_status = "manual_review"
            report.notes.append("no_candidates_found")
        else:
            report.final_history_status = instrument.history_status
            report.notes.append("all_candidates_already_known")
        return report

    # -- candidate discovery ----------------------------------------------

    def _gather_candidates(
        self, instrument: MarketInstrumentRecord
    ) -> list[MarketSymbolHistoryRecord]:
        candidates: dict[str, MarketSymbolHistoryRecord] = {}
        eodhd_exchange = _eodhd_exchange_for(instrument.exchange_code)

        # 1. EODHD search — prefer ISIN; fall back to primary ticker for
        # indices and other instruments that carry no ISIN.
        search_queries: list[tuple[str, str]] = []
        if instrument.isin:
            search_queries.append((instrument.isin, "auto_isin"))
        if instrument.primary_ticker:
            search_queries.append((instrument.primary_ticker, "auto_isin"))
        for query, confidence in search_queries:
            try:
                hits = self.eodhd.search(query)
            except Exception:
                logger.warning(
                    "EODHD search failed for %s (%s)",
                    instrument.instrument_id, query, exc_info=True,
                )
                hits = []
            for hit in hits:
                seg = _segment_from_search_hit(instrument.instrument_id, hit, confidence)
                if seg is not None:
                    candidates.setdefault(seg.segment_id, seg)

        # 2. OpenFIGI — by ISIN preferred, by primary ticker as fallback.
        if instrument.isin:
            try:
                openfigi_hits = self.openfigi.map_by_isin(instrument.isin)
            except Exception:
                logger.warning(
                    "OpenFIGI map_by_isin failed for %s",
                    instrument.instrument_id, exc_info=True,
                )
                openfigi_hits = []
            for hit in openfigi_hits:
                seg = _segment_from_openfigi_hit(instrument.instrument_id, hit, "auto_figi")
                if seg is not None:
                    candidates.setdefault(seg.segment_id, seg)
        elif instrument.primary_ticker:
            try:
                openfigi_hits = self.openfigi.map_by_ticker(
                    instrument.primary_ticker,
                    exch_code=eodhd_exchange or None,
                )
            except Exception:
                logger.warning(
                    "OpenFIGI map_by_ticker failed for %s",
                    instrument.instrument_id, exc_info=True,
                )
                openfigi_hits = []
            for hit in openfigi_hits:
                seg = _segment_from_openfigi_hit(instrument.instrument_id, hit, "auto_figi")
                if seg is not None:
                    candidates.setdefault(seg.segment_id, seg)

        # 3. EODHD delisted-list scan. The endpoint requires an EODHD
        # exchange code (e.g. "US"), not provider-local codes like
        # "NYSEARCA"; _eodhd_exchange_for translates when it can.
        if eodhd_exchange:
            try:
                delisted = self.eodhd.list_exchange_symbols(
                    eodhd_exchange, delisted=True
                )
            except Exception:
                logger.warning(
                    "EODHD delisted list failed for %s", eodhd_exchange, exc_info=True
                )
                delisted = []
            for row in _fuzzy_name_matches(instrument.name, delisted):
                seg = _segment_from_symbol_list_entry(
                    instrument.instrument_id, row, "name_match"
                )
                if seg is not None and seg.segment_id not in candidates:
                    candidates[seg.segment_id] = seg

        # 4. EODHD symbol-change-history: filter on both ticker AND exchange
        # so a rename on a different venue (e.g. a US "SAP" event) does not
        # leak into a German SAP instrument.
        try:
            changes = self.eodhd.symbol_change_history()
        except Exception:
            logger.warning("EODHD symbol_change_history failed", exc_info=True)
            changes = []
        for change in _rename_events_for_instrument(
            changes,
            primary_ticker=instrument.primary_ticker,
            eodhd_exchange=eodhd_exchange,
        ):
            for seg in _segments_from_rename_event(instrument.instrument_id, change):
                candidates.setdefault(seg.segment_id, seg)

        return list(candidates.values())


# ── Helpers ────────────────────────────────────────────────────────────────


def _segment_from_search_hit(
    instrument_id: str, hit: EODHDSearchHit, confidence: str
) -> MarketSymbolHistoryRecord | None:
    if not hit.code or not hit.exchange:
        return None
    ticker = f"{hit.code}.{hit.exchange}"
    return MarketSymbolHistoryRecord(
        segment_id=f"{instrument_id}:eodhd_search:{ticker}",
        instrument_id=instrument_id,
        ticker=ticker,
        provider_name="eodhd",
        exchange_code=hit.exchange,
        isin=hit.isin,
        figi="",
        valid_from="1900-01-01",
        valid_to="",
        event_type="manual_link",
        mapping_confidence=confidence,
        source_name="eodhd.search",
        raw_json={
            "name": hit.name,
            "type": hit.type,
            "country": hit.country,
            "currency": hit.currency,
        },
    )


def _segment_from_openfigi_hit(
    instrument_id: str, hit: OpenFIGIHit, confidence: str
) -> MarketSymbolHistoryRecord | None:
    if not hit.ticker:
        return None
    ticker = f"{hit.ticker}.{hit.exch_code}" if hit.exch_code else hit.ticker
    return MarketSymbolHistoryRecord(
        segment_id=f"{instrument_id}:openfigi:{hit.composite_figi or hit.figi or ticker}",
        instrument_id=instrument_id,
        ticker=ticker,
        provider_name="openfigi",
        exchange_code=hit.exch_code,
        isin="",
        figi=hit.composite_figi or hit.figi,
        valid_from="1900-01-01",
        valid_to="",
        event_type="manual_link",
        mapping_confidence=confidence,
        source_name="openfigi.mapping",
        raw_json={
            "name": hit.name,
            "security_type": hit.security_type,
            "market_sector": hit.market_sector,
            "description": hit.security_description,
        },
    )


def _segment_from_symbol_list_entry(
    instrument_id: str, row, confidence: str
) -> MarketSymbolHistoryRecord | None:
    if not row.code or not row.exchange:
        return None
    ticker = f"{row.code}.{row.exchange}"
    return MarketSymbolHistoryRecord(
        segment_id=f"{instrument_id}:eodhd_delisted:{ticker}",
        instrument_id=instrument_id,
        ticker=ticker,
        provider_name="eodhd",
        exchange_code=row.exchange,
        isin=row.isin,
        figi="",
        valid_from="1900-01-01",
        valid_to="",
        event_type="delisting",
        mapping_confidence=confidence,
        source_name="eodhd.exchange_symbol_list.delisted",
        raw_json={"name": row.name, "type": row.type, "country": row.country},
    )


# Map provider-local exchange codes to EODHD exchange codes. EODHD treats
# NYSE, NYSE Arca, Nasdaq and OTC as a single "US" universe. When a code
# is already a valid EODHD code (e.g. "XETRA", "HK") we pass it through.
_EODHD_EXCHANGE_ALIAS: dict[str, str] = {
    "NYSEARCA": "US",
    "NASDAQ": "US",
    "NYSE": "US",
    "OTC": "US",
    "OTCBB": "US",
    "AMEX": "US",
    "BATS": "US",
    "IEX": "US",
    "US": "US",
    "INDX": "INDX",
}


def _eodhd_exchange_for(exchange_code: str) -> str:
    """Return the EODHD exchange suffix for a provider-local code."""
    if not exchange_code:
        return ""
    return _EODHD_EXCHANGE_ALIAS.get(exchange_code.upper(), exchange_code)


def _rename_events_for_instrument(
    events: list[EODHDSymbolChangeEvent],
    *,
    primary_ticker: str,
    eodhd_exchange: str,
) -> list[EODHDSymbolChangeEvent]:
    """Filter rename events by both ticker AND exchange.

    Without the exchange filter a DE_SAP repair would pick up US "SAP"
    rename events and write *.US segments to the German instrument. When
    the caller has no mapped exchange (``eodhd_exchange`` empty) we still
    return matches so index/macro instruments can stitch — the caller is
    expected to scope universes at that point.
    """
    if not primary_ticker:
        return []
    matches: list[EODHDSymbolChangeEvent] = []
    for e in events:
        ticker_match = e.old_symbol == primary_ticker or e.new_symbol == primary_ticker
        if not ticker_match:
            continue
        if eodhd_exchange and e.exchange and e.exchange != eodhd_exchange:
            continue
        matches.append(e)
    return matches


def _segments_from_rename_event(
    instrument_id: str, event: EODHDSymbolChangeEvent
) -> list[MarketSymbolHistoryRecord]:
    segments: list[MarketSymbolHistoryRecord] = []
    effective = event.effective or "1900-01-01"
    if event.old_symbol:
        old_ticker = f"{event.old_symbol}.{event.exchange}"
        segments.append(
            MarketSymbolHistoryRecord(
                segment_id=f"{instrument_id}:rename_old:{old_ticker}:{effective}",
                instrument_id=instrument_id,
                ticker=old_ticker,
                provider_name="eodhd",
                exchange_code=event.exchange,
                valid_from="1900-01-01",
                valid_to=effective,
                event_type="ticker_rename",
                mapping_confidence="provider_native",
                source_name="eodhd.symbol_change_history",
                raw_json={
                    "company_name": event.company_name,
                    "new_symbol": event.new_symbol,
                },
            )
        )
    if event.new_symbol:
        new_ticker = f"{event.new_symbol}.{event.exchange}"
        segments.append(
            MarketSymbolHistoryRecord(
                segment_id=f"{instrument_id}:rename_new:{new_ticker}:{effective}",
                instrument_id=instrument_id,
                ticker=new_ticker,
                provider_name="eodhd",
                exchange_code=event.exchange,
                valid_from=effective,
                valid_to="",
                event_type="ticker_rename",
                mapping_confidence="provider_native",
                source_name="eodhd.symbol_change_history",
                raw_json={
                    "company_name": event.company_name,
                    "old_symbol": event.old_symbol,
                },
            )
        )
    return segments


def _fuzzy_name_matches(query: str, rows) -> list:
    """Return rows whose name shares a meaningful overlap with ``query``.

    Uses a simple token-set Jaccard similarity — good enough for "Apple
    Inc." vs "APPLE INC" or "Tencent Holdings Ltd" vs "Tencent Holdings".
    Threshold 0.6 filters out false positives on common words.
    """
    query_tokens = _normalize_tokens(query)
    if not query_tokens:
        return []
    hits: list = []
    for row in rows:
        candidate_tokens = _normalize_tokens(row.name)
        if not candidate_tokens:
            continue
        overlap = len(query_tokens & candidate_tokens)
        union = len(query_tokens | candidate_tokens)
        if union and overlap / union >= 0.6:
            hits.append(row)
    return hits


def _normalize_tokens(text: str) -> set[str]:
    if not text:
        return set()
    lowered = text.lower()
    for punct in (",", ".", "(", ")", "/", "-", "'", '"'):
        lowered = lowered.replace(punct, " ")
    # Strip common corporate-suffix noise so "Apple Inc" ≈ "Apple".
    stopwords = {
        "inc", "incorporated", "corp", "corporation", "co", "company",
        "ltd", "limited", "llc", "plc", "sa", "se", "ag", "nv", "the",
        "group", "holdings", "holding", "class", "common", "stock",
    }
    tokens = {t for t in lowered.split() if t and t not in stopwords}
    return tokens


__all__ = [
    "IdentityRepairService",
    "RepairReport",
]
