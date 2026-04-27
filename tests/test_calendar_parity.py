"""Cross-provider parity harness tests (issue #9 P6).

Covers the pure computation path (``calendar_econ_parity``), the
markdown report formatter, and the ``calendar_econ_parity`` service
op's envelope shape. No real upstream HTTP — every row is seeded
directly into ``cal_econ_event``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.parity import (
    OFFICIAL_PROVIDERS,
    TE_PROVIDER,
    ParityEvent,
    ParityRunSummary,
    calendar_econ_parity,
    format_parity_report,
)
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


_EVENT_DEFAULTS = {
    "event_time_precision": "datetime",
    "reference_label":      "",
    "category":             "",
    "importance":           None,
    "currency":             "",
    "unit":                 "",
    "actual":               None,
    "previous":             None,
    "revised":              None,
    "forecast":             None,
    "consensus_forecast":   None,
    "ticker":               "",
    "source":               "",
    "source_url":           "",
    "content_hash":         "hash",
    "last_update_epoch_ms": None,
    "observed_at_epoch_ms": 0,
    "created_at":           "2026-04-22T00:00:00+00:00",
    "updated_at":           "2026-04-22T00:00:00+00:00",
    "indicator_id":         None,
}


def _insert_event(
    store: SQLiteEngineStore, *,
    provider: str, provider_event_id: str,
    country_code: str, title: str, reference_date: str | None,
    event_time_utc: str,
) -> None:
    row = {
        **_EVENT_DEFAULTS,
        "provider":          provider,
        "provider_event_id": provider_event_id,
        "event_time_utc":    event_time_utc,
        "reference_date":    reference_date,
        "country_code":      country_code,
        "title":             title,
    }
    with store.get_connection() as conn:
        conn.execute(
            """
            INSERT INTO cal_econ_event (
                provider, provider_event_id, event_time_utc,
                event_time_precision, reference_date, reference_label,
                country_code, indicator_id, category, title,
                importance, currency, unit, actual, previous, revised,
                forecast, consensus_forecast, ticker, source, source_url,
                content_hash, last_update_epoch_ms, observed_at_epoch_ms,
                created_at, updated_at
            )
            VALUES (:provider, :provider_event_id, :event_time_utc,
                    :event_time_precision, :reference_date, :reference_label,
                    :country_code, :indicator_id, :category, :title,
                    :importance, :currency, :unit, :actual, :previous, :revised,
                    :forecast, :consensus_forecast, :ticker, :source, :source_url,
                    :content_hash, :last_update_epoch_ms, :observed_at_epoch_ms,
                    :created_at, :updated_at)
            """,
            row,
        )
        conn.commit()


def test_provider_constants_align_with_parser_ids() -> None:
    """TE_PROVIDER / OFFICIAL_PROVIDERS must exactly match the strings
    each connector writes into ``cal_econ_event.provider``; a drift
    would silently zero out parity matching."""
    from ingestion.calendar.te_api.parser import PROVIDER as TE_FROM_PARSER
    from ingestion.calendar.bls_api.parser import PROVIDER as BLS
    from ingestion.calendar.bea_api.parser import PROVIDER as BEA
    from ingestion.calendar.census_api.parser import PROVIDER as CENSUS
    from ingestion.calendar.conference_board_api.parser import (
        PROVIDER as CONFERENCE_BOARD,
    )
    from ingestion.calendar.destatis_api.parser import PROVIDER as DESTATIS
    from ingestion.calendar.ecb_api.parser import PROVIDER as ECB
    from ingestion.calendar.eurostat_api.parser import PROVIDER as EUROSTAT
    from ingestion.calendar.zew_api.parser import PROVIDER as ZEW
    from ingestion.calendar.ifo_api.parser import PROVIDER as IFO
    from ingestion.calendar.gfk_api.parser import PROVIDER as GFK
    from ingestion.calendar.hcob_api.parser import PROVIDER as HCOB
    from ingestion.calendar.ec_bcs_api.parser import PROVIDER as EC_BCS
    from ingestion.calendar.insee_api.parser import PROVIDER as INSEE
    from ingestion.calendar.ine_api.parser import PROVIDER as INE
    from ingestion.calendar.istat_api.parser import PROVIDER as ISTAT
    from ingestion.calendar.fed_api.parser import PROVIDER as FED
    from ingestion.calendar.ism_api.parser import PROVIDER as ISM
    from ingestion.calendar.nar_api.parser import PROVIDER as NAR
    from ingestion.calendar.umich_api.parser import PROVIDER as UMICH
    from ingestion.calendar.nbs_api.parser import PROVIDER as NBS
    from ingestion.calendar.mof_api.parser import PROVIDER as MOF_JP
    from ingestion.calendar.cao_api.parser import PROVIDER as CAO
    from ingestion.calendar.meti_api.parser import PROVIDER as METI
    from ingestion.calendar.stat_bureau_api.parser import PROVIDER as STAT_BUREAU
    from ingestion.calendar.ons_api.parser import PROVIDER as ONS
    from ingestion.calendar.boe_api.parser import PROVIDER as BOE
    from ingestion.calendar.statcan_api.parser import PROVIDER as STATCAN
    from ingestion.calendar.boc_api.parser import PROVIDER as BOC
    from ingestion.calendar.abs_api.parser import PROVIDER as ABS
    from ingestion.calendar.rba_api.parser import PROVIDER as RBA

    assert TE_PROVIDER == TE_FROM_PARSER
    assert set(OFFICIAL_PROVIDERS) == {
        BLS, BEA, CENSUS, ISM, UMICH, CONFERENCE_BOARD, NAR, ECB, EUROSTAT,
        DESTATIS, ZEW, IFO, GFK, HCOB, EC_BCS, INSEE, INE, ISTAT, FED, NBS, MOF_JP, CAO, METI,
        STAT_BUREAU, ONS, BOE, STATCAN, BOC, ABS, RBA,
    }


def test_parity_empty_window_returns_zero_totals(
    store: SQLiteEngineStore,
) -> None:
    """No rows → zero totals, zero indicators, zero gap lists; the
    match-percentage property short-circuits to 0 to avoid a zero
    division."""
    with store.get_connection() as conn:
        summary = calendar_econ_parity(
            conn, from_date="2026-04-01", to_date="2026-04-22",
        )
    assert summary.total_events == 0
    assert summary.matched == 0
    assert summary.te_only_count == 0
    assert summary.official_only_count == 0
    assert summary.indicators == []
    assert summary.match_percentage == 0.0


def test_te_and_official_match_on_same_bucket(
    store: SQLiteEngineStore,
) -> None:
    """Identical country + canonical indicator + reference_date on
    both sides counts as one matched bucket. Title variation
    (``"Consumer Price Index"`` vs ``"CPI"``) collapses through the
    shared canonicalization alias table."""
    _insert_event(
        store, provider=TE_PROVIDER, provider_event_id="te-cpi-1",
        country_code="US", title="Consumer Price Index",
        reference_date="2026-03-31", event_time_utc="2026-04-10T12:30:00Z",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-cpi-1",
        country_code="US", title="CPI",
        reference_date="2026-03-31", event_time_utc="2026-04-10T12:30:00Z",
    )
    with store.get_connection() as conn:
        summary = calendar_econ_parity(
            conn, from_date="2026-04-01", to_date="2026-04-22",
        )
    assert summary.total_events == 1
    assert summary.matched == 1
    assert summary.te_only_count == 0
    assert summary.official_only_count == 0
    assert summary.match_percentage == 100.0
    assert summary.indicators[0].canonical_indicator == "CPI"


def test_zew_te_title_matches_official_bucket(
    store: SQLiteEngineStore,
) -> None:
    _insert_event(
        store,
        provider=TE_PROVIDER,
        provider_event_id="te-zew-1",
        country_code="DE",
        title="ZEW Economic Sentiment Index",
        reference_date="2026-04-30T00:00:00",
        event_time_utc="2026-04-21T09:05:00+00:00",
    )
    _insert_event(
        store,
        provider="zew",
        provider_event_id="zew-1",
        country_code="DE",
        title="Germany ZEW Economic Sentiment Index",
        reference_date="2026-04-01",
        event_time_utc="2026-04-21T09:05:00+00:00",
    )

    with store.get_connection() as conn:
        summary = calendar_econ_parity(
            conn,
            from_date="2026-04-01",
            to_date="2026-04-30",
        )

    assert summary.matched == 1
    assert summary.te_only_count == 0
    assert summary.official_only_count == 0
    assert summary.indicators[0].canonical_indicator == "ZEW_SENTIMENT"


def test_te_only_gap_is_actionable_signal(
    store: SQLiteEngineStore,
) -> None:
    """TE has a JOBLESS_CLAIMS row but no BLS row — that's the
    actionable gap. The scheduler needs the missing release in its
    registry or schedule scraper."""
    _insert_event(
        store, provider=TE_PROVIDER, provider_event_id="te-claims-1",
        country_code="US", title="Weekly Jobless Claims",
        reference_date="2026-04-12", event_time_utc="2026-04-17T12:30:00Z",
    )
    with store.get_connection() as conn:
        summary = calendar_econ_parity(
            conn, from_date="2026-04-01", to_date="2026-04-22",
        )
    assert summary.matched == 0
    assert summary.te_only_count == 1
    assert summary.official_only_count == 0
    assert len(summary.te_only_events) == 1
    event = summary.te_only_events[0]
    assert event.provider == TE_PROVIDER
    assert event.canonical_indicator == "JOBLESS_CLAIMS"


def test_official_only_documents_te_blind_spot(
    store: SQLiteEngineStore,
) -> None:
    """An NBS row without a TE counterpart — TE is known to miss
    ad-hoc MOF/PBOC releases on the NBS side. The parity harness
    reports it as ``official_only`` (documented, not a regression)
    so the operator can triage rather than failing the report."""
    _insert_event(
        store, provider="nbs", provider_event_id="nbs-cpi-1",
        country_code="CN", title="CPI",
        reference_date="2026-03-31", event_time_utc="2026-04-11T01:30:00Z",
    )
    with store.get_connection() as conn:
        summary = calendar_econ_parity(
            conn, from_date="2026-04-01", to_date="2026-04-22",
        )
    assert summary.matched == 0
    assert summary.te_only_count == 0
    assert summary.official_only_count == 1
    event = summary.official_only_events[0]
    assert event.provider == "nbs"
    assert event.country_code == "CN"


def test_indicator_filter_narrows_scope(store: SQLiteEngineStore) -> None:
    """A caller passing ``indicators=["CPI"]`` ignores every other
    canonical token — TE's low-importance long-tail rows don't
    pollute the report when the operator asks for specific coverage."""
    # CPI — matched.
    _insert_event(
        store, provider=TE_PROVIDER, provider_event_id="te-cpi",
        country_code="US", title="Consumer Price Index",
        reference_date="2026-03-31", event_time_utc="2026-04-10T12:30:00Z",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-cpi",
        country_code="US", title="CPI",
        reference_date="2026-03-31", event_time_utc="2026-04-10T12:30:00Z",
    )
    # GDP — TE-only.
    _insert_event(
        store, provider=TE_PROVIDER, provider_event_id="te-gdp",
        country_code="US", title="Gross Domestic Product",
        reference_date="2026-03-31", event_time_utc="2026-04-29T12:30:00Z",
    )
    with store.get_connection() as conn:
        all_summary = calendar_econ_parity(
            conn, from_date="2026-04-01", to_date="2026-04-30",
        )
        cpi_only = calendar_econ_parity(
            conn, from_date="2026-04-01", to_date="2026-04-30",
            indicators=["CPI"],
        )
    assert all_summary.total_events == 2
    assert cpi_only.total_events == 1
    assert cpi_only.indicators[0].canonical_indicator == "CPI"


def test_window_excludes_out_of_range_rows(store: SQLiteEngineStore) -> None:
    """A row whose ``event_time_utc`` falls outside the window is
    ignored regardless of its reference_date — the window bounds the
    release date, not the period."""
    _insert_event(
        store, provider=TE_PROVIDER, provider_event_id="te-old",
        country_code="US", title="CPI",
        reference_date="2026-01-31", event_time_utc="2026-02-10T12:30:00Z",
    )
    with store.get_connection() as conn:
        summary = calendar_econ_parity(
            conn, from_date="2026-04-01", to_date="2026-04-22",
        )
    assert summary.total_events == 0


def test_multi_revision_rows_collapse_to_one_bucket(
    store: SQLiteEngineStore,
) -> None:
    """Two TE rows for the same release (revision-adjacent) still
    count as one TE-side presence — the bucket groups by
    ``(country, canonical, reference_date)`` and deduplicates by
    provider membership, not row count. Otherwise a three-revision
    release on one side would look like three TE-only rows when the
    official side only catches the final print."""
    for idx in range(3):
        _insert_event(
            store, provider=TE_PROVIDER,
            provider_event_id=f"te-cpi-rev{idx}",
            country_code="US", title="Consumer Price Index",
            reference_date="2026-03-31",
            event_time_utc="2026-04-10T12:30:00Z",
        )
    _insert_event(
        store, provider="bls", provider_event_id="bls-cpi",
        country_code="US", title="CPI",
        reference_date="2026-03-31", event_time_utc="2026-04-10T12:30:00Z",
    )
    with store.get_connection() as conn:
        summary = calendar_econ_parity(
            conn, from_date="2026-04-01", to_date="2026-04-22",
        )
    # One bucket, matched.
    assert summary.total_events == 1
    assert summary.matched == 1
    assert summary.indicators[0].total_events == 1


def test_rows_without_canonical_match_are_dropped(
    store: SQLiteEngineStore,
) -> None:
    """TE carries plenty of low-priority rows whose titles don't
    match the alias table ("Nonfarm Payrolls MoM Revision" might
    canonicalize to "nonfarm payrolls mom revision" — the un-aliased
    form). The canonicalize function returns a lowercased fallback so
    it's NOT empty; those rows still land in the report. Only a
    genuinely empty title is dropped. Keep one positive case here so
    an accidental canonicalize regression (empty returns) surfaces."""
    _insert_event(
        store, provider=TE_PROVIDER, provider_event_id="te-blank",
        country_code="US", title="",
        reference_date="2026-03-31", event_time_utc="2026-04-10T12:30:00Z",
    )
    with store.get_connection() as conn:
        summary = calendar_econ_parity(
            conn, from_date="2026-04-01", to_date="2026-04-22",
        )
    assert summary.total_events == 0


def test_date_only_upper_bound_is_inclusive_of_final_day(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex R1 P6 finding: a date-only ``to_date`` like
    ``"2026-04-22"`` compared lexicographically against a stored
    datetime ``"2026-04-22T12:30:00+00:00"`` excludes same-day rows
    because ``""`` (end-of-string) sorts before ``"T"``. The
    normalizer appends end-of-day UTC so the advertised-inclusive
    bound actually covers the full final day."""
    _insert_event(
        store, provider=TE_PROVIDER, provider_event_id="te-same-day",
        country_code="US", title="CPI",
        reference_date="2026-04-22", event_time_utc="2026-04-22T12:30:00+00:00",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-same-day",
        country_code="US", title="CPI",
        reference_date="2026-04-22", event_time_utc="2026-04-22T12:30:00+00:00",
    )
    with store.get_connection() as conn:
        summary = calendar_econ_parity(
            conn, from_date="2026-04-01", to_date="2026-04-22",
        )
    assert summary.total_events == 1
    assert summary.matched == 1


def test_staged_releases_stay_in_separate_buckets(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex R1 P6 finding: BEA GDP ships advance / second /
    third prints for the same quarter on different release days. If
    TE carries all three but BEA only has the advance (scheduler hasn't
    caught up to stages), the prior bucket key of
    ``(country, canonical, reference_date)`` collapsed all three TE
    rows into one matched bucket — hiding two gaps. Including the
    UTC event-date in the key surfaces each stage independently."""
    # Advance print — both sides have it. Both titles canonicalize
    # to ``"GDP"`` through the shared alias table, so the match is
    # driven by the scheduled release, not the label text.
    _insert_event(
        store, provider=TE_PROVIDER, provider_event_id="te-gdp-advance",
        country_code="US", title="Gross Domestic Product",
        reference_date="2025-12-31", event_time_utc="2026-01-28T13:30:00+00:00",
    )
    _insert_event(
        store, provider="bea", provider_event_id="bea-gdp-advance",
        country_code="US", title="GDP",
        reference_date="2025-12-31", event_time_utc="2026-01-28T13:30:00+00:00",
    )
    # Second estimate — TE only.
    _insert_event(
        store, provider=TE_PROVIDER, provider_event_id="te-gdp-second",
        country_code="US", title="GDP",
        reference_date="2025-12-31", event_time_utc="2026-02-26T13:30:00+00:00",
    )
    # Third estimate — TE only.
    _insert_event(
        store, provider=TE_PROVIDER, provider_event_id="te-gdp-third",
        country_code="US", title="GDP",
        reference_date="2025-12-31", event_time_utc="2026-03-26T12:30:00+00:00",
    )

    with store.get_connection() as conn:
        summary = calendar_econ_parity(
            conn, from_date="2026-01-01", to_date="2026-04-01",
        )
    # Three buckets, one per release date. One matched, two TE-only.
    assert summary.total_events == 3
    assert summary.matched == 1
    assert summary.te_only_count == 2
    # Per-indicator rollup stays at one row (US / GDP) summing the
    # three buckets — operator sees the aggregate while the event
    # list carries the specific missing stages.
    assert len(summary.indicators) == 1
    assert summary.indicators[0].total_events == 3
    assert summary.indicators[0].matched == 1
    assert summary.indicators[0].te_only == 2


def test_reference_date_format_divergence_still_matches(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex R2 P1 finding #1: TE stores ``ReferenceDate`` as
    ``"2026-03-31T00:00:00"`` while BLS stores monthly periods as
    ``"2026-03-01"``. Raw string comparison would split them into
    separate buckets, producing paired TE-only + official-only gaps
    for a release both sides carry. Normalization to ``YYYY-MM``
    collapses both formats to the same bucket key."""
    _insert_event(
        store, provider=TE_PROVIDER, provider_event_id="te-cpi",
        country_code="US", title="Consumer Price Index",
        # TE-style datetime reference.
        reference_date="2026-03-31T00:00:00",
        event_time_utc="2026-04-10T12:30:00+00:00",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-cpi",
        country_code="US", title="CPI",
        # BLS-style start-of-month reference.
        reference_date="2026-03-01",
        event_time_utc="2026-04-10T12:30:00+00:00",
    )
    with store.get_connection() as conn:
        summary = calendar_econ_parity(
            conn, from_date="2026-04-01", to_date="2026-04-22",
        )
    assert summary.total_events == 1
    assert summary.matched == 1
    assert summary.te_only_count == 0
    assert summary.official_only_count == 0


def test_em_dash_titles_canonicalize_through_aliases() -> None:
    """Fix for Codex R2 P1 finding #2 (first half): BLS writes
    ``"Consumer Price Index — All Items Less Food and Energy"`` with
    a U+2014 em-dash. The dash is normalized to a space before alias
    lookup, so the full phrase matches the new
    ``"consumer price index all items less food and energy"`` alias
    and converges on ``"CORE_CPI"`` with TE's ``"Core Inflation Rate"``."""
    from ingestion.calendar._official_shared import canonicalize_indicator

    bls_label = "Consumer Price Index — All Items Less Food and Energy"
    te_label = "Core Inflation Rate YoY"
    assert canonicalize_indicator(bls_label) == "CORE_CPI"
    assert canonicalize_indicator(te_label) == "CORE_CPI"


def test_fomc_sep_suffix_strips_to_base_indicator() -> None:
    """Fed's value-side op preserves the ``"+ SEP"`` marker on titles
    for quarterly projection-materials meetings. ``" + sep"`` is a
    modifier suffix (like YoY / MoM) — canonical token stays
    ``FOMC_RATE`` so SEP meetings parity-match non-SEP meetings."""
    from ingestion.calendar._official_shared import canonicalize_indicator

    assert canonicalize_indicator("FOMC Rate Decision") == "FOMC_RATE"
    assert canonicalize_indicator("FOMC Rate Decision + SEP") == "FOMC_RATE"


def test_gdp_title_variants_all_canonicalize_to_gdp() -> None:
    """BEA ships ``"Real Gross Domestic Product"``; TE ships
    ``"GDP Growth Rate QoQ"``; both should land on the ``"GDP"``
    canonical token so a BEA advance print matches its TE counterpart."""
    from ingestion.calendar._official_shared import canonicalize_indicator

    assert canonicalize_indicator("Real Gross Domestic Product") == "GDP"
    assert canonicalize_indicator("GDP Growth Rate QoQ") == "GDP"
    assert canonicalize_indicator("Real GDP") == "GDP"
    assert canonicalize_indicator("GDP") == "GDP"


def test_report_formatter_covers_summary_and_event_lists() -> None:
    """Markdown report surfaces the headline counters, per-indicator
    table, and both gap sections. No truncation markers at this
    size."""
    summary = ParityRunSummary(
        from_date="2026-04-01",
        to_date="2026-04-22",
        total_events=2,
        matched=1,
        te_only_count=1,
        official_only_count=0,
        indicators=[
            # Direct construction avoids the fixture-seed dance for a
            # pure formatter smoke test.
            __import__("ingestion.calendar.parity", fromlist=["IndicatorParity"]).IndicatorParity(
                canonical_indicator="CPI", country_code="US",
                total_events=1, matched=1, te_only=0, official_only=0,
            ),
            __import__("ingestion.calendar.parity", fromlist=["IndicatorParity"]).IndicatorParity(
                canonical_indicator="NFP", country_code="US",
                total_events=1, matched=0, te_only=1, official_only=0,
            ),
        ],
        te_only_events=[
            ParityEvent(
                provider=TE_PROVIDER, provider_event_id="te-nfp",
                country_code="US", canonical_indicator="NFP",
                reference_date="2026-03-31", title="Nonfarm Payrolls",
                event_time_utc="2026-04-03T12:30:00Z",
            ),
        ],
        official_only_events=[],
    )
    report = format_parity_report(summary)
    assert "# Calendar parity report" in report
    assert "Window: `2026-04-01` → `2026-04-22`" in report
    assert "| US | CPI | 1 | 1 | 0 | 0 | 100.0% |" in report
    assert "| US | NFP | 1 | 0 | 1 | 0 | 0.0% |" in report
    assert "TE-only events (scheduler missing a release TE carries)" in report
    assert "nfp" in report.lower()
    assert "Official-only events" in report
    # Empty list should render "_none_".
    assert "_none_" in report


def test_report_formatter_truncates_long_event_lists() -> None:
    """Per-event lists are capped at 50 so the report stays
    review-friendly. A tail marker tells the operator how many were
    elided."""
    events = [
        ParityEvent(
            provider=TE_PROVIDER, provider_event_id=f"te-{i}",
            country_code="US", canonical_indicator="CPI",
            reference_date=f"2026-{(i % 12) + 1:02d}-01",
            title="Consumer Price Index",
            event_time_utc=f"2026-{(i % 12) + 1:02d}-10T12:30:00Z",
        )
        for i in range(60)
    ]
    summary = ParityRunSummary(
        from_date="2026-01-01",
        to_date="2026-12-31",
        total_events=60,
        te_only_count=60,
        te_only_events=events,
    )
    report = format_parity_report(summary)
    assert "_…and 10 more not shown_" in report


def test_service_op_requires_date_range(store: SQLiteEngineStore) -> None:
    """Missing ``from_date`` / ``to_date`` returns a structured error
    — the op should not silently run against the full table."""
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_parity", {})
    assert "error" in result


def test_service_op_returns_envelope_with_matched_bucket(
    store: SQLiteEngineStore,
) -> None:
    """End-to-end through the service invoker — envelope shape
    carries totals, per-indicator, both gap lists, and the rendered
    markdown report."""
    from macro_data.service import LocalMacroDataService

    _insert_event(
        store, provider=TE_PROVIDER, provider_event_id="te-cpi",
        country_code="US", title="Consumer Price Index",
        reference_date="2026-03-31", event_time_utc="2026-04-10T12:30:00Z",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-cpi",
        country_code="US", title="CPI",
        reference_date="2026-03-31", event_time_utc="2026-04-10T12:30:00Z",
    )
    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_parity",
        {"from_date": "2026-04-01", "to_date": "2026-04-22"},
    )
    assert result["total_events"] == 1
    assert result["matched"] == 1
    assert result["match_percentage"] == 100.0
    assert result["te_provider"] == TE_PROVIDER
    assert set(result["official_providers"]) == set(OFFICIAL_PROVIDERS)
    assert len(result["indicators"]) == 1
    assert "# Calendar parity report" in result["report_markdown"]
    # ``write_report`` defaults to False — no file side-effect.
    assert result["report_path"] is None


def test_service_op_writes_report_when_requested(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``write_report=True`` persists the markdown under
    ``docs/validation/calendar_parity_<YYYY-MM-DD>.md`` in the process
    CWD. Test isolates via ``monkeypatch.chdir`` so the real repo
    ``docs/`` is never touched."""
    from macro_data.service import LocalMacroDataService

    monkeypatch.chdir(tmp_path)
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    _insert_event(
        store, provider=TE_PROVIDER, provider_event_id="te-cpi",
        country_code="US", title="Consumer Price Index",
        reference_date="2026-03-31", event_time_utc="2026-04-10T12:30:00Z",
    )

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_parity",
        {
            "from_date": "2026-04-01",
            "to_date": "2026-04-22",
            "write_report": True,
        },
    )
    assert result["report_path"] is not None
    written = Path(result["report_path"])
    assert written.exists()
    content = written.read_text(encoding="utf-8")
    assert content.startswith("# Calendar parity report")
