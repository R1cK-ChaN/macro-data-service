"""Recurring schedule + value-side refresh across official-source connectors.

The target end-state for issue #9 (item #1) — "every US / EU / China
headline release we care about lands in ``cal_econ_event`` from its
official source within minutes of publication" — needs the per-
connector ops to run on a recurring cron, not by hand.

Two driver entry points, one shared per-connector loop:

- :func:`refresh_all_schedules` (P-sched-1) — schedule-side: invokes
  every connector's forward-looking schedule scrape (BLS / BEA / Census
  / ISM / U Michigan / Conference Board / NAR / ECB / Eurostat / Destatis / ZEW / Ifo / GfK / HCOB / INSEE / INE /
  ISTAT / Fed FOMC / Fed releasedates / NBS / Statistics Bureau JP / BoJ / BoJ Tankan / MoF JP / CAO /
  CAO GDP / METI).
  Daily-cron candidate.
- :func:`sweep_value_side` (P-sched-2) — value-side: invokes every
  connector's value-bearing scrape (BLS / BEA / Census / ISM /
  U Michigan / Conference Board / NAR / ECB / Eurostat / Destatis / ZEW / Ifo / GfK / INSEE / INE /
  ISTAT / Fed-values / BoJ-values / Statistics Bureau JP-values / BoJ Tankan-values / MoF JP-values /
  CAO-values / CAO GDP-values / METI-values).
  Frequent-cron candidate — repeatedly runs to pick up new values once
  the release crosses its scheduled time, so the calendar's ``actual``
  fills within minutes of publication.

Each connector gets its own connection lifecycle so a failure inside
one connector rolls back only that connector's partial writes — the
remaining connectors still commit. This is the correct semantics for
independent per-source caches: an NBS HTTP 403 shouldn't undo the
BLS schedule refresh that just succeeded.

Nothing runs automatically from this module — a caller (the
cron-scheduled service op
:func:`.service._op_calendar_econ_refresh_schedules` /
:func:`.service._op_calendar_econ_sweep_values` or a future
operator-driven trigger) invokes the entry points.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

# Default ECB window: ~180 days back in ``"YYYY-MM"`` SDMX form.
# The ECB FM dataflow only publishes observations on Governing
# Council meeting days (~8 per year), so six months reliably covers
# the last three-to-four policy decisions. Without a bound, each
# frequent-cron sweep would issue an unbounded SDMX request and
# re-project the full ECB history — wasteful on bandwidth and on
# the projector's merge CASE.
_ECB_CRON_WINDOW_DAYS = 180

# Schedule-aware burst (issue #37). Cap of 30 attempts × 60-second
# spacing matches the 30-minute look-ahead — the burst expires
# naturally at the upper end of the window. Lookback of 1h absorbs
# late publications + sweep-clock skew.
_BURST_WINDOW_LOOKBACK = timedelta(hours=1)
_BURST_WINDOW_LOOKAHEAD = timedelta(minutes=30)
_BURST_MAX_ATTEMPTS = 30
_BURST_INTERVAL_SECONDS = 60.0

from .bea_api import fetch_bea_calendar, schedule_bea_calendar
from .bls_api import fetch_bls_calendar, schedule_bls_calendar
from .boj_api import fetch_boj_calendar, fetch_boj_statement_values
from .boj_speeches_api import fetch_boj_speeches_calendar
from .boj_tankan_api import (
    fetch_boj_tankan_calendar,
    fetch_boj_tankan_outlines,
)
from .cao_api import (
    fetch_cao_calendar,
    fetch_cao_consumer_confidence_values,
)
from .cao_gdp_api import fetch_cao_gdp_calendar, fetch_cao_gdp_values
from .mof_api import fetch_mof_calendar, fetch_mof_trade_values
from .meti_api import fetch_meti_calendar, fetch_meti_values
from .stat_bureau_api import fetch_stat_bureau_calendar, fetch_stat_bureau_values
from .census_api import fetch_census_calendar, schedule_census_calendar
from .conference_board_api import (
    fetch_conference_board_calendar,
    schedule_conference_board_calendar,
)
from .ecb_api import fetch_ecb_calendar, schedule_ecb_calendar
from .ecb_speeches_api import fetch_ecb_speeches_calendar
from .eia_api import fetch_eia_calendar
from .eurostat_api import fetch_eurostat_calendar, schedule_eurostat_calendar
from .destatis_api import fetch_destatis_calendar, schedule_destatis_calendar
from .dol_api import fetch_dol_calendar
from .ons_api import fetch_ons_calendar
from .boe_api import fetch_boe_calendar
from .boe_speeches_api import fetch_boe_speeches_calendar
from .statcan_api import fetch_statcan_calendar
from .boc_api import fetch_boc_calendar
from .abs_api import fetch_abs_calendar
from .rba_api import fetch_rba_calendar
from .mospi_api import fetch_mospi_calendar
from .rbi_api import fetch_rbi_calendar
from .kostat_api import fetch_kostat_calendar
from .bok_api import fetch_bok_calendar
from .ibge_api import fetch_ibge_calendar
from .bcb_api import fetch_bcb_calendar
from .tuik_api import fetch_tuik_calendar
from .tcmb_api import fetch_tcmb_calendar
from .inegi_api import fetch_inegi_calendar
from .banxico_api import fetch_banxico_calendar
from .statssa_api import fetch_statssa_calendar
from .sarb_api import fetch_sarb_calendar
from .bi_api import fetch_bi_calendar
from .zew_api import fetch_zew_calendar, schedule_zew_calendar
from .ifo_api import fetch_ifo_calendar, schedule_ifo_calendar
from .gfk_api import fetch_gfk_calendar, schedule_gfk_calendar
from .hcob_api import fetch_hcob_calendar, schedule_hcob_calendar
from .ec_bcs_api import fetch_ec_bcs_calendar, schedule_ec_bcs_calendar
from .insee_api import fetch_insee_calendar, schedule_insee_calendar
from .ine_api import fetch_ine_calendar, schedule_ine_calendar
from .istat_api import fetch_istat_calendar, schedule_istat_calendar
from .fed_api import (
    fetch_fed_calendar,
    fetch_fed_releasedates,
    fetch_fed_statement_values,
)
from .fed_speeches_api import fetch_fed_speeches_calendar
from .ism_api import fetch_ism_calendar, schedule_ism_calendar
from .nar_api import fetch_nar_calendar, schedule_nar_calendar
from .nbs_api import fetch_nbs_calendar, fetch_nbs_values
from .umich_api import fetch_umich_calendar, schedule_umich_calendar
from .scheduler_state import (
    DAILY_BUDGET_CAPS,
    get_connector_state,
    is_budget_exhausted,
    is_cooling,
    mark_connector_failure,
    mark_connector_success,
    record_connector_requests,
    today_utc_iso,
)

logger = logging.getLogger(__name__)


# Connector identifier — also the key used in summary dicts and the
# ``connectors=[…]`` subset argument on the service op.
ConnectorName = str

_ConnectorFn = Callable[[sqlite3.Connection, bool], Any]


def _bls(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_bls_calendar(conn, dry_run=dry_run)


def _bea(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_bea_calendar(conn, dry_run=dry_run)


def _census(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_census_calendar(conn, dry_run=dry_run)


def _ism(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_ism_calendar(conn, dry_run=dry_run)


def _umich(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_umich_calendar(conn, dry_run=dry_run)


def _conference_board(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_conference_board_calendar(conn, dry_run=dry_run)


def _nar(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_nar_calendar(conn, dry_run=dry_run)


def _ecb(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_ecb_calendar(conn, dry_run=dry_run)


def _eia(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # EIA combines schedule + value in one JSON response — no
    # separate schedule scrape exists. Pulls the most recent 60
    # days of weekly stocks; the projector idempotently merges so
    # repeated runs don't write duplicates.
    from ingestion.timeseries.scrapers.eia import EIAClient
    client = EIAClient()
    if not dry_run and not client.api_key:
        raise RuntimeError("EIA_API_KEY not set")
    return fetch_eia_calendar(conn, client, dry_run=dry_run)


def _dol(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # DOL combines schedule + value too — each Thursday press
    # release carries the headline figures and reference week.
    return fetch_dol_calendar(conn, dry_run=dry_run)


def _ons(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # ONS exposes per-indicator JSON timeseries that carry the
    # latest observation plus its publication ``updateDate`` —
    # schedule and value land together, like DOL / EIA.
    return fetch_ons_calendar(conn, dry_run=dry_run)


def _boe(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # BoE Bank-Rate.asp page lists every rate-change MPC decision
    # with date + new rate. Combined schedule + value, mirroring
    # the FOMC-statement-values shape but for past announcements.
    return fetch_boe_calendar(conn, dry_run=dry_run)


def _statcan(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # StatCan WDS exposes per-vector latest-N JSON that carries
    # the latest observation plus its publication ``releaseTime``
    # — schedule and value land together, like ONS / DOL / EIA.
    return fetch_statcan_calendar(conn, dry_run=dry_run)


def _boc(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # BoC Valet observations sweep picks up any new overnight
    # rate change since the last run. Mirrors the BoE shape —
    # change-only, hold decisions stay outside this connector.
    return fetch_boc_calendar(conn, dry_run=dry_run)


def _abs(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # ABS release-calendar HTML scrape — schedule-only slice. Each
    # release card carries a UTC ``<time datetime>`` and a URL slug
    # encoding the reference period, so the projector lands the
    # event on its actual publication time without a separate
    # value-side fetch. ``actual=NULL`` until the P2 SDMX value
    # lookup ships.
    return fetch_abs_calendar(conn, dry_run=dry_run)


def _rba(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # RBA cash-rate-target HTML re-fetch picks up any new MPB
    # decision (change OR hold) since the last sweep. Unlike the
    # BoC Valet pattern, the RBA table publishes hold decisions in
    # the same row format as moves, so the connector covers every
    # scheduled MPB announcement in P1.
    return fetch_rba_calendar(conn, dry_run=dry_run)


def _mospi(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # MoSPI release-calendar JSON API — schedule-only slice. The API
    # response carries one row per scheduled / past release with
    # ``year``/``month``/``day`` and a ``title`` substring-matched
    # against the indicator registry. ``actual=NULL`` until the P2
    # PDF value-extraction lands.
    return fetch_mospi_calendar(conn, dry_run=dry_run)


def _rbi(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # RBI annualpolicy.aspx HTML scrape — schedule-only slice. Each
    # scheduled MPC meeting closing day projects as one calendar
    # event at 10:00 IST. ``actual=NULL`` until the P2 per-meeting
    # Resolution press-release scrape lands.
    return fetch_rbi_calendar(conn, dry_run=dry_run)


def _kostat(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # KOSTAT release-schedule HTML scrape — schedule-only slice. Each
    # row carries title + ``Mon. DD (Day.)`` publication date plus the
    # reference period parsed from the title. ``actual=NULL`` until
    # the P2 per-release news-list scrape lands.
    return fetch_kostat_calendar(conn, dry_run=dry_run)


def _bok(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # BOK Meeting Dates HTML scrape — schedule-only slice. Each
    # scheduled MPB meeting projects as one calendar event at 09:50
    # KST. ``actual=NULL`` until the P2 per-meeting Monetary Policy
    # Decision press-release scrape lands.
    return fetch_bok_calendar(conn, dry_run=dry_run)


def _ibge(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # IBGE monthly release-calendar HTML scrape — schedule-only slice
    # (issue #84). Each event row carries a ``data-divulgacao`` ISO
    # timestamp + product-anchored title link, parsed against an
    # indicator allowlist. The fetcher walks a rolling forward window
    # (current month + next three) so a daily sweep keeps the
    # calendar's lookahead fresh. ``actual=NULL`` until the P2 per-
    # release detail-page scrape lands.
    return fetch_ibge_calendar(conn, dry_run=dry_run)


def _bcb(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # BCB Copom history JSON re-fetch picks up any new Copom decision
    # since the last sweep. Unlike the schedule-only KOSTAT / RBI /
    # MoSPI / BOK pattern, the JSON service exposes target Selic
    # inline (RBA-style coverage), so the P1 slice ships value-bearing
    # events for every Copom announcement — change OR hold OR
    # extraordinary OR monocratic-presidential.
    return fetch_bcb_calendar(conn, dry_run=dry_run)


def _tuik(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # TÜİK national release-calendar JSON sweep — schedule-only slice
    # (issue #86). The unified national feed covers every Turkish
    # statistical agency; the connector filters to ``sorumluKisaAd ==
    # 'TÜİK'`` and matches each row's ``adi`` against an indicator
    # allowlist (CPI / PPI / GDP / IP / Unemployment / Trade Balance).
    # The fetcher walks a rolling year window (current + next year)
    # so a daily sweep keeps the calendar's lookahead fresh.
    # ``actual=NULL`` until the P2 per-release detail-page scrape lands.
    return fetch_tuik_calendar(conn, dry_run=dry_run)


def _tcmb(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # TCMB 1-Week Repo rate-history HTML re-fetch picks up any new PPK
    # rate-change decision since the last sweep. The page lists *only*
    # rate changes (hold decisions deferred to P2); each row carries
    # the new policy rate inline next to the announcement date, so the
    # P1 slice ships value-bearing events on day one and ``(TR,
    # TCMB_RATE)`` joins the parity whitelist immediately.
    return fetch_tcmb_calendar(conn, dry_run=dry_run)


def _inegi(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # INEGI saladeprensa calendar JSON sweep — schedule-only slice
    # (issue #88). One POST per distinct ``idPrograma`` referenced by
    # the indicator set, against a 90-day-back / 365-day-forward date
    # window. Indicators that share a programme id (CPI / INPC_15 both
    # key on idPrograma 2353) are post-filtered by ``programa``
    # substring + cadence shape. ``actual=NULL`` until the P2 per-
    # release boletín scrape lands.
    return fetch_inegi_calendar(conn, dry_run=dry_run)


def _banxico(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # Banxico Tasa Objetivo decisions HTML re-fetch picks up any new
    # Junta de Gobierno announcement since the last sweep. The page
    # encodes both holds (absolute rate inline) and changes (basis-
    # point delta inline); a cumulative walk seeded from the oldest
    # hold yields the absolute rate for every decision, so the P1
    # slice ships value-bearing events on day one and ``(MX,
    # BANXICO_RATE)`` joins the parity whitelist immediately.
    return fetch_banxico_calendar(conn, dry_run=dry_run)


def _statssa(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # Stats SA Publication Schedule AJAX sweep — schedule-only slice
    # (issue #90). One POST per month inside a current + 14-month
    # rolling window. Indicators match on the Stats SA Publication
    # Number (``PPN``) plus a per-indicator cadence filter.
    # ``actual=NULL`` until the P2 per-release detail-page scrape lands.
    return fetch_statssa_calendar(conn, dry_run=dry_run)


def _sarb(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # SARB MRDREPOR JSON timeseries re-fetch picks up any new repo-rate
    # change since the last sweep. The endpoint lists *changes only*
    # (hold decisions deferred to P2); each row carries the new policy
    # rate inline next to the effective date, so the P1 slice ships
    # value-bearing events on day one. Same TCMB-shape deferral —
    # parity whitelist stays empty until a P2 MPC-statement scrape
    # delivers authoritative announcement dates AND hold coverage.
    return fetch_sarb_calendar(conn, dry_run=dry_run)


def _bi(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # Bank Indonesia BI-Rate history HTML re-fetch picks up any new
    # Board of Governors decision since the last sweep. Page 1 of the
    # rate table lists every meeting (change OR hold) with the new
    # rate inline, so the P1 slice ships value-bearing events on day
    # one and ``(ID, BI_RATE)`` joins the parity whitelist immediately
    # — RBA / BCB / Banxico-style coverage rather than the TCMB / SARB
    # rate-change-only deferral.
    return fetch_bi_calendar(conn, dry_run=dry_run)


def _eurostat(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_eurostat_calendar(conn, dry_run=dry_run)


def _destatis(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_destatis_calendar(conn, dry_run=dry_run)


def _zew(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_zew_calendar(conn, dry_run=dry_run)


def _ifo(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_ifo_calendar(conn, dry_run=dry_run)


def _gfk(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_gfk_calendar(conn, dry_run=dry_run)


def _hcob(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_hcob_calendar(conn, dry_run=dry_run)


def _ec_bcs(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_ec_bcs_calendar(conn, dry_run=dry_run)


def _insee(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_insee_calendar(conn, dry_run=dry_run)


def _ine(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_ine_calendar(conn, dry_run=dry_run)


def _istat(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_istat_calendar(conn, dry_run=dry_run)


def _fed_fomc(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_fed_calendar(conn, dry_run=dry_run)


def _fed_releases(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_fed_releasedates(conn, dry_run=dry_run)


def _nbs(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # ``calendar_url=None`` triggers the auto-discovery path added in
    # P5a: the fetcher resolves the current year's article from the
    # NBS release-calendar index before scraping it.
    return fetch_nbs_calendar(conn, calendar_url=None, dry_run=dry_run)


def _boj(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_boj_calendar(conn, dry_run=dry_run)


def _boj_tankan(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_boj_tankan_calendar(conn, dry_run=dry_run)


def _mof(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_mof_calendar(conn, dry_run=dry_run)


def _cao(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_cao_calendar(conn, dry_run=dry_run)


def _cao_gdp(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_cao_gdp_calendar(conn, dry_run=dry_run)


def _meti(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_meti_calendar(conn, dry_run=dry_run)


def _stat_bureau(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_stat_bureau_calendar(conn, dry_run=dry_run)


def _fed_speeches(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # Fed per-year speeches archive HTML scrape — schedule-only
    # slice (issue #56). Each Board / Vice Chair / Chair speech
    # projects as one calendar event with ``actual=NULL`` and
    # ``event_time_precision='date'``.
    return fetch_fed_speeches_calendar(conn, dry_run=dry_run)


def _ecb_speeches(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # ECB official speeches CSV download — schedule-only slice
    # (issue #56). Single GET against the pipe-separated dataset
    # the ECB refreshes monthly.
    return fetch_ecb_speeches_calendar(conn, dry_run=dry_run)


def _boe_speeches(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # BoE speeches sitemap HTML scrape — schedule-only slice
    # (issue #56). Single GET; current-format ``/speech/<YYYY>/
    # <month>/<slug>`` rows project at month precision.
    return fetch_boe_speeches_calendar(conn, dry_run=dry_run)


def _boj_speeches(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # BoJ per-year speeches archive HTML scrape — schedule-only
    # slice (issue #56). Filtered to rate-setting roles (Governor /
    # Deputy Governor / Member of the Policy Board); Executive
    # Directors and below are skipped.
    return fetch_boj_speeches_calendar(conn, dry_run=dry_run)


# Sequencing matters for operator inspection — BLS first (highest
# trader impact, cheapest surface), Fed / ECB in the middle, NBS last
# (most upstream-fragile so a failure there is easiest to triage at
# the end of the log).
_DEFAULT_CONNECTORS: tuple[tuple[ConnectorName, _ConnectorFn], ...] = (
    ("bls", _bls),
    ("bea", _bea),
    ("census", _census),
    ("ism", _ism),
    ("umich", _umich),
    ("conference-board", _conference_board),
    ("nar", _nar),
    ("ecb", _ecb),
    ("eia", _eia),
    ("dol", _dol),
    ("ons", _ons),
    ("boe", _boe),
    ("statcan", _statcan),
    ("boc", _boc),
    ("abs", _abs),
    ("rba", _rba),
    ("mospi", _mospi),
    ("rbi", _rbi),
    ("kostat", _kostat),
    ("bok", _bok),
    ("ibge", _ibge),
    ("bcb", _bcb),
    ("tuik", _tuik),
    ("tcmb", _tcmb),
    ("inegi", _inegi),
    ("banxico", _banxico),
    ("statssa", _statssa),
    ("sarb", _sarb),
    ("bank-indonesia", _bi),
    ("eurostat", _eurostat),
    ("destatis", _destatis),
    ("zew", _zew),
    ("ifo", _ifo),
    ("gfk", _gfk),
    ("hcob", _hcob),
    ("ec-bcs", _ec_bcs),
    ("insee", _insee),
    ("ine", _ine),
    ("istat", _istat),
    ("fed-fomc", _fed_fomc),
    ("fed-releases", _fed_releases),
    ("nbs", _nbs),
    ("stat-bureau-jp", _stat_bureau),
    ("boj", _boj),
    ("boj-tankan", _boj_tankan),
    ("mof-jp", _mof),
    ("cao", _cao),
    ("cao-gdp", _cao_gdp),
    ("meti", _meti),
    ("fed-speeches", _fed_speeches),
    ("ecb-speeches", _ecb_speeches),
    ("boe-speeches", _boe_speeches),
    ("boj-speeches", _boj_speeches),
)

ALL_CONNECTORS: tuple[ConnectorName, ...] = tuple(
    name for name, _ in _DEFAULT_CONNECTORS
)


# Value-side connector identifiers. ``nbs-values`` (issue #49) covers
# CPI / PPI / Industrial Production / Fixed Asset Investment / Retail
# Sales — PMI / GDP stay schedule-only because the English NBS press-
# release listing doesn't carry PMI articles and GDP uses a table-
# format parser deferred to P2. Fed's FOMC calendar doesn't belong
# here either — the value-bearing Fed op is
# ``fetch_fed_statement_values`` exposed as ``fed-values`` below.
ALL_VALUE_SIDE_CONNECTORS: tuple[ConnectorName, ...] = (
    "bls",
    "bea",
    "census",
    "ism",
    "umich",
    "conference-board",
    "nar",
    "ecb",
    "eia",
    "dol",
    "ons",
    "boe",
    "statcan",
    "boc",
    "abs",
    "rba",
    "mospi",
    "rbi",
    "kostat",
    "bok",
    "ibge",
    "bcb",
    "tuik",
    "tcmb",
    "inegi",
    "banxico",
    "statssa",
    "sarb",
    "bank-indonesia",
    "eurostat",
    "destatis",
    "zew",
    "ifo",
    "gfk",
    "hcob",
    "ec-bcs",
    "insee",
    "ine",
    "istat",
    "fed-values",
    "nbs-values",
    "stat-bureau-jp-values",
    "boj-values",
    "boj-tankan-values",
    "mof-jp-values",
    "cao-values",
    "cao-gdp-values",
    "meti-values",
    # Speech connectors are schedule-only — listed here so the
    # value-side sweep keeps the slug-anchored ids fresh as new
    # speeches land throughout the day (the upstream archives
    # update intra-day for currently-running events).
    "fed-speeches",
    "ecb-speeches",
    "boe-speeches",
    "boj-speeches",
)


@dataclass
class ConnectorResult:
    """Per-connector outcome of a single scheduler pass."""

    connector: ConnectorName
    ok: bool
    error: str | None = None
    summary: dict[str, Any] = field(default_factory=dict)
    wall_seconds: float = 0.0


@dataclass
class RefreshRunSummary:
    """Aggregate outcome of a :func:`refresh_all_schedules` pass."""

    connectors_planned: list[ConnectorName] = field(default_factory=list)
    dry_run: bool = True
    results: list[ConnectorResult] = field(default_factory=list)
    unknown_connectors: list[ConnectorName] = field(default_factory=list)
    wall_seconds: float = 0.0

    @property
    def ok_count(self) -> int:
        return sum(1 for r in self.results if r.ok)

    @property
    def failed_count(self) -> int:
        # Unknown connector names count as failures so a cron/operator
        # typo surfaces in the top-level envelope rather than silently
        # skipping the intended source.
        return sum(1 for r in self.results if not r.ok) + len(
            self.unknown_connectors
        )


def _summary_to_dict(summary: Any) -> dict[str, Any]:
    """Normalize a connector's RunSummary into a JSON-serializable dict.

    Connectors return dataclass instances with heterogeneous fields;
    this flattens the public surface into a dict of primitives so the
    service op can forward the result without importing every per-
    connector dataclass type.
    """
    if summary is None:
        return {}
    out: dict[str, Any] = {}
    for field_name, value in vars(summary).items():
        if field_name.startswith("_"):
            continue
        out[field_name] = value
    return out


def _summary_is_total_outage(summary: Any) -> bool:
    """True when the summary looks like a connector-wide failure.

    Partial failures (one BLS series out of nine, one FOMC statement
    URL out of eight, one ECB page of two) produce a non-None
    ``_summary_failure_reason`` so the operator sees ``ok=False`` on
    the card, but they should **not** trip the circuit breaker — the
    other items were written and the next sweep should still try the
    working subset.

    Full-outage signals (treat as breaker-relevant):

    - ``fetch_error`` singular string (BEA / Fed-releasedates) —
      set only when the connector's single HTTP surface is down.
    - ``fetch_errors`` dict (ECB press calendar + bulletin) **with
      zero ``entries_parsed`` / ``events_upserted``** — some pages
      failed and none delivered rows. If any page parsed entries,
      partial.
    - BLS ``series_failed`` covering every planned series (no
      ``series_ok``).
    - Auto-discovery value sweeps with ``pending_releases`` where
      every due release failed and zero observations landed.
    - Fed-values ``meetings_fetched == 0`` when the plan had
      meetings — the op auto-discovers past FOMC rows; a planned
      run that failed on every URL is a statement-page outage.

    Returns False when no markers are present (clean run) or when
    the markers look partial.
    """
    if summary is None:
        return False
    if getattr(summary, "fetch_error", None):
        return True
    fetch_errors = getattr(summary, "fetch_errors", None)
    if fetch_errors:
        entries_parsed = getattr(summary, "entries_parsed", None)
        events_upserted = getattr(summary, "events_upserted", None)
        parsed_something = (
            (entries_parsed is not None and entries_parsed > 0)
            or (events_upserted is not None and events_upserted > 0)
        )
        if not parsed_something:
            return True
    series_planned = getattr(summary, "series_planned", None)
    series_failed = getattr(summary, "series_failed", None)
    series_ok = getattr(summary, "series_ok", None)
    pending_releases = getattr(summary, "pending_releases", None)
    observations_seen = getattr(summary, "observations_seen", None)
    if (
        pending_releases is not None
        and pending_releases > 0
        and observations_seen == 0
        and series_failed
        and len(series_failed) >= pending_releases
    ):
        return True
    if series_failed and series_planned is not None:
        if not series_ok and len(series_failed) >= len(series_planned):
            return True
    # EIA's summary uses ``indicators_planned`` / ``indicators_ok``
    # in place of the BLS-style ``series_*`` names. Mirror the same
    # all-failed shape so a complete EIA upstream outage trips the
    # breaker instead of getting misread as a clean run.
    indicators_planned = getattr(summary, "indicators_planned", None)
    indicators_ok = getattr(summary, "indicators_ok", None)
    if (
        series_failed and indicators_planned is not None
        and not indicators_ok
        and len(series_failed) >= len(indicators_planned)
    ):
        return True
    # DOL's summary records per-release outages in
    # ``releases_failed``. A run where the listing parsed entries
    # but every PDF GET 403'd / parse-failed wrote zero observations
    # — the listing was reachable but the publisher / Akamai stack
    # is down. Treat as a total outage so the breaker cools the
    # connector instead of hammering it on every sweep.
    listing_entries = getattr(summary, "listing_entries", None)
    releases_failed = getattr(summary, "releases_failed", None)
    if (
        listing_entries is not None and listing_entries > 0
        and observations_seen == 0
        and releases_failed
    ):
        return True
    meetings_planned = getattr(summary, "meetings_planned", None)
    meetings_fetched = getattr(summary, "meetings_fetched", None)
    if (
        meetings_planned is not None
        and meetings_fetched == 0
        and meetings_planned > 0
    ):
        return True
    # BoJ Tankan value-side mirrors the same shape but names the
    # counters ``releases_*`` (the Tankan surface is "releases", not
    # "meetings"). Without this branch, a run where every outline URL
    # 404s would set ``ok=False`` via ``fetch_failures`` without
    # tripping the breaker, and the frequent cron would keep polling
    # the broken surface.
    releases_planned = getattr(summary, "releases_planned", None)
    releases_fetched = getattr(summary, "releases_fetched", None)
    if (
        releases_planned is not None
        and releases_fetched == 0
        and releases_planned > 0
    ):
        return True
    return False


def _summary_failure_reason(summary: Any) -> str | None:
    """Detect connector-level fetch failures stashed inside the summary.

    Connectors catch their HTTP exception and record it on the
    summary rather than propagating, so the driver would otherwise
    report a clean run when a source contributed zero fresh rows:

    - BEA / Fed-releasedates set ``fetch_error`` on a 502.
    - ECB collects per-page failures in ``fetch_errors``.
    - BLS collects per-series failures in ``series_failed`` (tuples
      of ``(series_id, reason)``) — a bls.gov 403 / layout drift lands
      every series there.
    - Fed-values collects per-URL failures in ``fetch_failures`` /
      ``parse_failures`` (tuples of ``(closing_iso, reason)``). A 404
      on a single statement page or an upstream layout drift lands
      there without raising.

    Any non-empty marker flags ``ok=False`` so the top-level envelope
    surfaces the outage. Partial failures (one page out of eight)
    also register as non-ok so the operator notices rather than
    finding the loss weeks later; per-item detail stays visible in
    the summary dict.
    """
    if summary is None:
        return None
    fetch_error = getattr(summary, "fetch_error", None)
    if fetch_error:
        return str(fetch_error)
    fetch_errors = getattr(summary, "fetch_errors", None)
    if fetch_errors:
        return str(fetch_errors)
    series_failed = getattr(summary, "series_failed", None)
    if series_failed:
        count = len(series_failed)
        first = series_failed[0]
        return f"{count} series failed (e.g. {first})"
    fetch_failures = getattr(summary, "fetch_failures", None)
    if fetch_failures:
        count = len(fetch_failures)
        first = fetch_failures[0]
        return f"{count} fetch failures (e.g. {first})"
    parse_failures = getattr(summary, "parse_failures", None)
    if parse_failures:
        count = len(parse_failures)
        first = parse_failures[0]
        return f"{count} parse failures (e.g. {first})"
    # Issue #50 — DOL UI Claims fetcher records per-release outages
    # in ``releases_failed`` (list of (release_iso_or_indicator,
    # reason) tuples). Surface so the scheduler envelope flips
    # ``ok=False`` when DOL silently drops every Thursday.
    releases_failed = getattr(summary, "releases_failed", None)
    if releases_failed:
        count = len(releases_failed)
        first = releases_failed[0]
        return f"{count} release failures (e.g. {first})"
    return None


def _run_connector_with_breaker(
    name: ConnectorName,
    fn: _ConnectorFn,
    *,
    connection_factory: Callable[[], sqlite3.Connection],
    state_conn: sqlite3.Connection,
    dry_run: bool,
    log_prefix: str,
    budget_cap: int | None = None,
) -> ConnectorResult:
    """Run one connector with the P-sched-3 circuit breaker applied.

    Lifecycle per connector:

    1. Read persisted state. If ``cooling_until_ms`` is still in the
       future — or the connector's daily request budget is exhausted
       at ``budget_cap`` — skip and return ``ok=False`` carrying the
       skip reason. No data connection is opened for either skip path.
    2. Otherwise open a data connection, invoke ``fn``, commit on
       success / rollback on exception (mirroring the pre-breaker
       semantics).
    3. In execute mode, persist the updated state (reset on success,
       increment on failure) and accumulate any ``requests_made``
       surfaced by the connector summary (or attached to an exception)
       into today's budget. Dry-run checks state but never mutates.

    ``budget_cap`` is None by default because only the value-side
    sweep driver hits the API surface that carries a cap (BLS
    ``api.bls.gov``, 500 queries / key / UTC day). Schedule-side
    refresh scrapes HTML (``bls.gov/schedule/…``) which has no
    documented cap, so a same-connector exhausted-cap state must not
    freeze the forward schedule for the rest of the day. The
    success/failure/request-recording path still runs on both sides
    so the breaker state and the budget counter stay coherent
    regardless of which driver triggered the run.

    The helper uses a caller-supplied ``state_conn`` — separate from
    the per-connector data connection — so a data-side rollback can't
    unwind the circuit-breaker counter.
    """
    connector_started = time.monotonic()
    # ``start_ms`` answers "is the breaker cooling right now?" at the
    # point the driver decided whether to skip. ``failure_ms`` is
    # resolved fresh at mark time (below) because a slow connector
    # can consume minutes between start and the failure signal, and
    # using the stale start timestamp for ``cooling_until_ms``
    # computation would shorten the effective cool-down window.
    start_ms = int(time.time() * 1000)

    state = get_connector_state(state_conn, name)
    if is_cooling(state, start_ms):
        cooling_until_iso = datetime.fromtimestamp(
            (state.cooling_until_ms or 0) / 1000, tz=timezone.utc,
        ).isoformat()
        reason = (
            f"circuit breaker cooling until {cooling_until_iso} "
            f"({state.consecutive_failures} consecutive failures; "
            f"last: {state.last_error or '-'})"
        )
        return ConnectorResult(
            connector=name,
            ok=False,
            error=reason,
            wall_seconds=round(time.monotonic() - connector_started, 3),
        )

    # Budget-aware skip. Schedule-side refresh passes
    # ``budget_cap=None`` because its BLS path scrapes HTML (uncapped);
    # value-side sweep passes ``DAILY_BUDGET_CAPS.get(name)`` so only
    # capped connectors (BLS today) can short-circuit on exhaustion.
    skip_check_today_iso = today_utc_iso()
    if is_budget_exhausted(state, budget_cap, today_iso=skip_check_today_iso):
        reason = (
            f"daily request budget exhausted "
            f"({state.requests_today}/{budget_cap} on {skip_check_today_iso})"
        )
        return ConnectorResult(
            connector=name,
            ok=False,
            error=reason,
            wall_seconds=round(time.monotonic() - connector_started, 3),
        )

    data_conn = connection_factory()
    try:
        summary = fn(data_conn, dry_run)
        if not dry_run:
            data_conn.commit()
    except Exception as exc:
        try:
            data_conn.rollback()
        except Exception:
            pass
        logger.warning(
            "calendar %s failed for %s: %s", log_prefix, name, exc,
        )
        if not dry_run:
            # Fresh timestamp so ``cooling_until_ms`` is anchored on
            # the actual failure moment, not the start of a slow run.
            mark_connector_failure(
                state_conn, name, error=str(exc),
                now_ms=int(time.time() * 1000),
            )
            # Record requests consumed before the exception so a
            # partial-then-fail BLS run (chunk 1 succeeded, chunk 2
            # 500'd) still ticks the persisted counter. The
            # connector shim attaches ``exc.requests_made`` before
            # re-raising; absent attribute means the connector
            # consumed zero capped requests (or doesn't track).
            exc_requests_made = getattr(exc, "requests_made", None)
            if isinstance(exc_requests_made, int) and exc_requests_made > 0:
                record_connector_requests(
                    state_conn, name, exc_requests_made,
                    today_iso=today_utc_iso(),
                )
            state_conn.commit()
        return ConnectorResult(
            connector=name,
            ok=False,
            error=str(exc),
            wall_seconds=round(time.monotonic() - connector_started, 3),
        )
    finally:
        try:
            data_conn.close()
        except Exception:
            pass

    failure_reason = _summary_failure_reason(summary)
    if not dry_run:
        # Partial failures (one BLS series, one FOMC URL) land as
        # ``ok=False`` on the card but must not trip the breaker —
        # only a connector-wide outage does. ``_summary_is_total_outage``
        # distinguishes the two.
        if failure_reason is not None and _summary_is_total_outage(summary):
            mark_connector_failure(
                state_conn, name, error=failure_reason,
                now_ms=int(time.time() * 1000),
            )
        else:
            # Reset on clean run OR on partial failure — the breaker
            # is about "is this source reachable at all?", not "did
            # every item land?". A partial run proves reachability
            # and resets the consecutive counter.
            mark_connector_success(state_conn, name)

        # Accumulate any requests the connector surfaced into today's
        # budget. Summaries without ``requests_made`` (BEA / ECB / Fed
        # / NBS today) leave the counter untouched. ``today_iso`` is
        # resolved at record time — a value-side sweep that crossed
        # UTC midnight attributes consumption to the current day. The
        # client counter resets internally at midnight, so the
        # observable delta already reflects post-midnight requests
        # only; attributing them to the new day keeps the persisted
        # counter aligned with the client's reset semantics.
        requests_made = getattr(summary, "requests_made", None)
        if isinstance(requests_made, int) and requests_made > 0:
            record_connector_requests(
                state_conn, name, requests_made,
                today_iso=today_utc_iso(),
            )
        state_conn.commit()

    return ConnectorResult(
        connector=name,
        ok=failure_reason is None,
        error=failure_reason,
        summary=_summary_to_dict(summary),
        wall_seconds=round(time.monotonic() - connector_started, 3),
    )


def refresh_all_schedules(
    connection_factory: Callable[[], sqlite3.Connection],
    *,
    dry_run: bool = True,
    connectors: Iterable[ConnectorName] | None = None,
    _connector_overrides: (
        dict[ConnectorName, _ConnectorFn] | None
    ) = None,
) -> RefreshRunSummary:
    """Refresh every connector's forward-looking schedule rows.

    Parameters
    ----------
    connection_factory:
        Callable returning a fresh :class:`sqlite3.Connection`. Each
        connector gets its own connection; the driver commits on
        success and rolls back on exception so partial success across
        connectors is the default.
    dry_run:
        When ``True`` (default) no HTTP call is made and no row is
        written; each per-connector dry-run returns its indicator /
        series plan so the caller can inspect the scope.
    connectors:
        Optional subset of connector names to run. Omit (or pass the
        full tuple) to run the full roster. Order follows
        :data:`_DEFAULT_CONNECTORS`.
    _connector_overrides:
        Test seam — swaps in fake per-connector functions so unit
        tests can exercise the isolation + aggregation logic without
        hitting real upstreams. Production callers omit this.
    """
    started = time.monotonic()
    requested = (
        tuple(connectors) if connectors is not None else ALL_CONNECTORS
    )
    valid_names = set(ALL_CONNECTORS)
    # A caller typo (``"fed-fomcc"``) would otherwise silently drop
    # below — the plan builds from ``_DEFAULT_CONNECTORS`` membership.
    # Keep the unknown names so the top-level summary surfaces them
    # and ``failed_count`` reflects the skipped source.
    unknown_connectors = [n for n in requested if n not in valid_names]
    selected_set = {n for n in requested if n in valid_names}
    overrides = _connector_overrides or {}

    plan: list[tuple[ConnectorName, _ConnectorFn]] = []
    for name, fn in _DEFAULT_CONNECTORS:
        if name not in selected_set:
            continue
        plan.append((name, overrides.get(name, fn)))

    run_summary = RefreshRunSummary(
        connectors_planned=[name for name, _ in plan],
        dry_run=dry_run,
        unknown_connectors=unknown_connectors,
    )

    state_conn = connection_factory()
    try:
        for name, fn in plan:
            run_summary.results.append(_run_connector_with_breaker(
                name, fn,
                connection_factory=connection_factory,
                state_conn=state_conn,
                dry_run=dry_run,
                log_prefix="schedule refresh",
                budget_cap=None,
            ))
    finally:
        try:
            state_conn.close()
        except Exception:
            pass

    run_summary.wall_seconds = time.monotonic() - started
    return run_summary


# ──────────────────────────────────────────────────────────────────────────
# Value-side sweep (P-sched-2)
# ──────────────────────────────────────────────────────────────────────────


# Schedule-aware burst predicates (issue #37). Each entry is the SQL
# WHERE-clause body that selects the rows a value-side connector is
# responsible for filling. Combined with the ``actual IS NULL`` and
# ``event_time_utc BETWEEN window_start AND window_end`` filters at
# query time, this counts how many releases the connector should
# pick up in the burst window. Providers shared across two value
# connectors (``boj`` for MPM + Tankan, ``cao`` for CCI + GDP) are
# disambiguated by title so each connector's burst sees only its own
# pending rows.
_VALUE_SIDE_DUE_ROW_FILTERS: dict[ConnectorName, str] = {
    "bls":                   "provider = 'bls'",
    "bea":                   "provider = 'bea'",
    "census":                "provider = 'census'",
    "ism":                   "provider = 'ism'",
    "umich":                 "provider = 'umich'",
    "conference-board":      "provider = 'conference-board'",
    "nar":                   "provider = 'nar'",
    # ECB intentionally absent — the value fetcher writes new
    # ``'ECB % Rate'`` rows rather than filling ``actual`` on the
    # schedule rows (`ECB Monetary Policy Decision` /
    # `ECB Economic Bulletin`), so neither row shape supports the
    # burst's "until ``actual`` lands" completion check. Falling
    # through to the hourly baseline here is an explicit slice cap
    # — Issue #37 P2 candidate to add a separate trigger /
    # completion-row tracker for ECB.
    # EIA + DOL + ONS + BoE + StatCan + BoC + ABS + RBA intentionally
    # absent — each connector writes rows only after the value is
    # published (the API / press-release / Bank Rate page / ABS
    # release-calendar HTML / RBA cash-rate table carries period +
    # value together), so a pre-release schedule row never exists for
    # the burst's "until ``actual`` lands" check to fire on. Falls
    # through to the hourly baseline (same explicit slice cap as ECB
    # above). Adding a release-time-based trigger that pre-seeds rows
    # is the issue #37 P2 follow-up.
    "eurostat":              "provider = 'eurostat'",
    "destatis":              "provider = 'destatis'",
    "zew":                   "provider = 'zew'",
    "ifo":                   "provider = 'ifo'",
    "gfk":                   "provider = 'gfk'",
    "hcob":                  "provider = 'hcob'",
    "ec-bcs":                "provider = 'ec-bcs'",
    "insee":                 "provider = 'insee'",
    "ine":                   "provider = 'ine'",
    "istat":                 "provider = 'istat'",
    # ``fetch_fed_statement_values`` only fills rows matching
    # ``title LIKE 'FOMC Rate Decision%'``; the other Fed-released
    # series (Beige Book, H.4.1, H.8) ride on ``fed-releases``
    # schedule-side and stay ``actual IS NULL`` permanently.
    "fed-values":            "provider = 'federal-reserve' AND title LIKE 'FOMC Rate Decision%'",
    # Issue #49 — five indicators with a press-release listing
    # fragment registered in ``nbs_api.indicators``. PMI / GDP
    # schedule rows still ship ``actual=NULL`` and stay outside the
    # burst's completion check; the next quarterly slice extends
    # coverage by adding ``listing_title_fragment`` + value patterns.
    "nbs-values": (
        "provider = 'nbs' AND title IN ("
        "'China Consumer Price Index',"
        "'China Producer Price Index',"
        "'China Industrial Production',"
        "'China Fixed Asset Investment',"
        "'China Retail Sales')"
    ),
    "stat-bureau-jp-values": "provider = 'stat-bureau-jp'",
    "boj-values":            "provider = 'boj' AND title = 'BoJ Interest Rate Decision'",
    "boj-tankan-values":     "provider = 'boj' AND title LIKE 'Tankan %'",
    "mof-jp-values":         "provider = 'mof-jp'",
    "cao-values":            "provider = 'cao' AND title = 'Consumer Confidence'",
    "cao-gdp-values":        "provider = 'cao' AND title LIKE 'GDP %'",
    "meti-values":           "provider = 'meti'",
}


# Per-connector publication buffer (issue #37 / Codex round 1 P2 #3).
# Six value-side connectors only attempt fetch ``buffer`` past the
# scheduled ``event_time_utc`` (deliberate, to avoid hammering a
# not-yet-published page and tripping the breaker — see the BoJ
# fetcher's own ``_discover_pending_closings`` docstring). The burst
# window for those connectors must shift back by the same amount,
# otherwise a row at ``sweep_start + 10min`` is counted as due even
# though the connector cannot fetch it for another hour, producing
# 30 attempts of guaranteed no-ops. Connectors absent from the map
# default to ``timedelta(0)`` — the standard ``[−1h, +30min]`` window.
_VALUE_SIDE_FETCH_BUFFER: dict[ConnectorName, timedelta] = {
    "boj-values":            timedelta(hours=1),
    "boj-tankan-values":     timedelta(hours=1),
    "mof-jp-values":         timedelta(hours=1),
    "stat-bureau-jp-values": timedelta(hours=1),
    "cao-gdp-values":        timedelta(hours=1),
    "meti-values":           timedelta(hours=1),
}


def _count_due_rows(
    conn: sqlite3.Connection,
    predicate: str,
    *,
    window_start_iso: str,
    window_end_iso: str,
) -> int:
    """Count rows the connector still needs to fill in the burst window.

    ``actual IS NULL`` selects unfilled rows; ``event_time_utc BETWEEN
    window_start AND window_end`` clamps to the burst window resolved at
    sweep start. ``predicate`` narrows to one connector's roster — one
    of :data:`_VALUE_SIDE_DUE_ROW_FILTERS`.
    """
    # Upper bound is exclusive (``<`` not ``<=``) so a row at
    # exactly ``window_end_iso`` doesn't count as due. With 30
    # attempts × 60s spacing, the burst's last attempt fires ~29
    # minutes after sweep start; a row whose eligibility opens at
    # the +30-min boundary would otherwise be counted but get no
    # attempt at the time it became fetchable.
    row = conn.execute(
        f"""
        SELECT COUNT(*) FROM cal_econ_event
        WHERE actual IS NULL
          AND event_time_utc >= ?
          AND event_time_utc < ?
          AND ({predicate})
        """,
        (window_start_iso, window_end_iso),
    ).fetchone()
    return int(row[0]) if row else 0


def _result_indicates_skip(result: ConnectorResult) -> bool:
    """True when the breaker short-circuited (cooling or budget out).

    The breaker's two pre-call skip paths return ``ok=False`` with a
    distinctive ``error`` string but never invoke the connector. The
    burst loop must stop immediately on either: cooling means the
    source is presumed down (more attempts compound the failure
    counter), and budget exhaustion means subsequent attempts can't
    actually hit the API anyway.
    """
    if result.ok or not result.error:
        return False
    return (
        "circuit breaker cooling" in result.error
        or "daily request budget exhausted" in result.error
    )


def sweep_value_side(
    connection_factory: Callable[[], sqlite3.Connection],
    *,
    dry_run: bool = True,
    start_year: int | None = None,
    end_year: int | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    connectors: Iterable[ConnectorName] | None = None,
    _connector_overrides: (
        dict[ConnectorName, _ConnectorFn] | None
    ) = None,
    _burst_max_attempts: int = _BURST_MAX_ATTEMPTS,
    _burst_interval_seconds: float = _BURST_INTERVAL_SECONDS,
    _burst_clock: Callable[[], datetime] | None = None,
    _burst_sleep: Callable[[float], None] | None = None,
) -> RefreshRunSummary:
    """Invoke every value-side connector to fill ``actual`` on recent rows.

    Frequent-cron candidate — repeatedly runs to pick up new values
    once a release crosses its scheduled time, so a BLS CPI 08:30 ET
    release lands on the calendar within minutes of publication.

    Parameters
    ----------
    connection_factory:
        Callable returning a fresh :class:`sqlite3.Connection`. Each
        connector gets its own connection; commit on success, rollback
        on exception, so partial success across connectors is allowed.
    dry_run:
        When ``True`` (default) no HTTP and no DB writes. Connector
        dry-runs return their plan.
    start_year / end_year:
        Year window passed through to BLS / BEA / Census value-side ops.
        Default: ``(current_year − 1, current_year)`` — a
        two-year window large enough that the freshness guard still
        skips recomputing settled rows while any just-published
        observation lands.
    start_period / end_period:
        Optional SDMX-period strings forwarded to ECB and Eurostat.
        Destatis uses the resolved year bounds; ZEW / Ifo / GfK / INSEE / INE / ISTAT
        auto-discover due rows from ``cal_econ_event``.
        ``None`` means the driver picks its recent window.
    connectors:
        Optional subset of :data:`ALL_VALUE_SIDE_CONNECTORS`. Omit to
        run the full value-side plan.
    _connector_overrides:
        Test seam — swap in fake functions to exercise isolation +
        aggregation without hitting real upstreams.

    Operator note: BLS and BEA need API-key env vars
    (``BLS_API_KEY`` / ``BEA_API_KEY``). A missing key raises from the
    per-connector shim and lands as ``ok=False`` on the result rather
    than aborting the sweep — the other connectors still run.

    Schedule-aware burst (issue #37). For each connector, the driver
    counts unfilled rows in the burst window
    (``[now − 1h, now + 30min]``) before the first invocation. If any
    are due, the connector runs in a burst loop at
    ``_burst_interval_seconds`` cadence until (a) every windowed row
    has ``actual``, (b) ``_burst_max_attempts`` is reached, (c) the
    window's upper bound has elapsed, or (d) the breaker short-
    circuits (cooling / budget exhausted). Connectors with no due
    rows in the window run exactly once — the existing baseline
    behavior. The burst window is fixed at sweep start so events
    drifting past ``+30min`` mid-burst aren't double-counted; the
    next hourly sweep is the catch-all for any row that didn't fill.
    """
    started = time.monotonic()
    now_utc = datetime.now(timezone.utc)
    resolved_start_year = start_year if start_year is not None else now_utc.year - 1
    resolved_end_year = end_year if end_year is not None else now_utc.year
    resolved_start_period = start_period if start_period is not None else (
        (now_utc - timedelta(days=_ECB_CRON_WINDOW_DAYS)).strftime("%Y-%m")
    )
    resolved_end_period = end_period  # ``None`` → ECB client defaults to "latest"

    def _bls_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Dry-run doesn't issue HTTP, so we defer the client construction
        # + API-key check to execute mode. Calling the library function
        # with ``dry_run=True`` returns the plan regardless of auth.
        from ingestion.timeseries.scrapers.bls import BLSClient
        client = BLSClient()
        if not dry_run and not client.api_key:
            raise RuntimeError("BLS_API_KEY not set")
        # Snapshot the client's daily counter so the driver can still
        # attribute consumed requests to the budget when ``get_series``
        # raises mid-call (chunk N succeeded, chunk N+1 500'd). Without
        # this the persisted counter would undershoot actual BLS API
        # usage and the breaker's budget skip could be defeated.
        requests_before = client.daily_query_count
        try:
            return fetch_bls_calendar(
                conn, client,
                start_year=resolved_start_year,
                end_year=resolved_end_year,
                dry_run=dry_run,
            )
        except Exception as exc:
            consumed = max(0, client.daily_query_count - requests_before)
            if consumed > 0:
                exc.requests_made = consumed  # read by scheduler.py
            raise

    def _bea_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        from ingestion.timeseries.scrapers.bea import BEAClient
        client = BEAClient()
        if not dry_run and not client.api_key:
            raise RuntimeError("BEA_API_KEY not set")
        return fetch_bea_calendar(
            conn, client,
            start_year=resolved_start_year,
            end_year=resolved_end_year,
            dry_run=dry_run,
        )

    def _census_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        from ingestion.calendar.census_api import CensusEITSClient
        client = CensusEITSClient()
        return fetch_census_calendar(
            conn, client,
            start_year=resolved_start_year,
            end_year=resolved_end_year,
            dry_run=dry_run,
        )

    def _ism_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_ism_calendar(conn, dry_run=dry_run)

    def _umich_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_umich_calendar(conn, dry_run=dry_run)

    def _conference_board_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_conference_board_calendar(conn, dry_run=dry_run)

    def _nar_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_nar_calendar(conn, dry_run=dry_run)

    def _ecb_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # ECB SDMX has no auth — plain HTTP against data-api.ecb.europa.eu.
        from ingestion.timeseries.sdmx.providers.ecb import ECBClient
        client = ECBClient()
        return fetch_ecb_calendar(
            conn, client,
            start_period=resolved_start_period,
            end_period=resolved_end_period,
            dry_run=dry_run,
        )

    def _eia_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Same client as the schedule-side; the EIA API returns
        # schedule + value in one shot. ``EIA_API_KEY`` required.
        from ingestion.timeseries.scrapers.eia import EIAClient
        client = EIAClient()
        if not dry_run and not client.api_key:
            raise RuntimeError("EIA_API_KEY not set")
        return fetch_eia_calendar(conn, client, dry_run=dry_run)

    def _dol_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # DOL ETA listing → release PDF → headline figures. No API
        # key required. Sits behind Akamai bot protection — runs
        # use the browser-shaped session in :mod:`dol_api.listing`.
        return fetch_dol_calendar(conn, dry_run=dry_run)

    def _ons_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # ONS JSON timeseries — each fetch returns latest period +
        # value, so the same op fills schedule and value.
        return fetch_ons_calendar(conn, dry_run=dry_run)

    def _boe_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # BoE Bank-Rate.asp re-fetch picks up any new rate-change
        # decision row published since the last sweep.
        return fetch_boe_calendar(conn, dry_run=dry_run)

    def _statcan_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # StatCan WDS — each fetch returns latest period + value
        # per indicator, so the same op fills schedule and value.
        return fetch_statcan_calendar(conn, dry_run=dry_run)

    def _boc_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Valet observations re-fetch picks up any new overnight-
        # rate change since the last sweep.
        return fetch_boc_calendar(conn, dry_run=dry_run)

    def _abs_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # ABS release-calendar HTML scrape re-runs to pick up any
        # newly-published indicator releases. P1 publishes
        # schedule-only rows; the value lookup is the P2 follow-up.
        return fetch_abs_calendar(conn, dry_run=dry_run)

    def _rba_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # RBA cash-rate page re-fetch picks up the latest MPB
        # decision (change OR hold) since the last sweep.
        return fetch_rba_calendar(conn, dry_run=dry_run)

    def _mospi_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # MoSPI release-calendar JSON re-fetch picks up newly-
        # published indicator releases. P1 publishes schedule-only
        # rows; the per-release PDF value lookup is the P2 follow-up.
        return fetch_mospi_calendar(conn, dry_run=dry_run)

    def _rbi_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # RBI annualpolicy.aspx re-fetch picks up any updates to the
        # MPC meeting schedule. P1 publishes schedule-only rows; the
        # per-meeting Resolution scrape that fills the new repo rate
        # is the P2 follow-up.
        return fetch_rbi_calendar(conn, dry_run=dry_run)

    def _kostat_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # KOSTAT release-schedule HTML re-fetch picks up newly-
        # published indicator releases. P1 publishes schedule-only
        # rows; the per-release news-list scrape is the P2 follow-up.
        return fetch_kostat_calendar(conn, dry_run=dry_run)

    def _bok_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # BOK Meeting Dates re-fetch picks up any updates to the MPB
        # schedule. P1 publishes schedule-only rows; the per-meeting
        # Monetary Policy Decision press-release scrape that fills the
        # new Base Rate is the P2 follow-up.
        return fetch_bok_calendar(conn, dry_run=dry_run)

    def _ibge_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # IBGE monthly calendar re-fetch picks up newly-published
        # indicator releases. P1 publishes schedule-only rows; the
        # per-release detail-page scrape that fills ``actual`` is the
        # P2 follow-up.
        return fetch_ibge_calendar(conn, dry_run=dry_run)

    def _bcb_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # BCB Copom history re-fetch picks up any new Copom decision
        # since the last sweep. Each fetch returns target Selic + new
        # rate inline, so the same op fills schedule and value.
        return fetch_bcb_calendar(conn, dry_run=dry_run)

    def _tuik_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # TÜİK national-calendar re-fetch picks up newly-published
        # indicator releases. P1 publishes schedule-only rows; the
        # per-release detail-page scrape that fills ``actual`` is the
        # P2 follow-up.
        return fetch_tuik_calendar(conn, dry_run=dry_run)

    def _tcmb_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # TCMB 1-Week Repo rate-history re-fetch picks up any new PPK
        # rate-change decision since the last sweep. The page returns
        # announcement date + new rate inline, so the same op fills
        # schedule and value (rate changes only — holds deferred to P2).
        return fetch_tcmb_calendar(conn, dry_run=dry_run)

    def _inegi_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # INEGI saladeprensa calendar re-fetch picks up newly-published
        # boletín entries. P1 publishes schedule-only rows; the per-
        # release detail-page scrape that fills ``actual`` is the P2
        # follow-up.
        return fetch_inegi_calendar(conn, dry_run=dry_run)

    def _banxico_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Banxico Tasa Objetivo decisions re-fetch picks up any new
        # Junta de Gobierno announcement since the last sweep. Each
        # fetch returns the cumulative-walked absolute rate inline,
        # so the same op fills schedule and value.
        return fetch_banxico_calendar(conn, dry_run=dry_run)

    def _statssa_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Stats SA Publication Schedule re-fetch picks up newly-
        # scheduled indicator releases. P1 publishes schedule-only
        # rows; the per-release detail-page scrape that fills
        # ``actual`` is the P2 follow-up.
        return fetch_statssa_calendar(conn, dry_run=dry_run)

    def _sarb_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # SARB MRDREPOR JSON re-fetch picks up any new repo-rate
        # change since the last sweep. Each fetch returns the new
        # policy rate inline next to the effective date, so the same
        # op fills schedule and value (rate changes only — holds
        # deferred to P2).
        return fetch_sarb_calendar(conn, dry_run=dry_run)

    def _bi_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Bank Indonesia BI-Rate history re-fetch picks up any new
        # Board of Governors announcement since the last sweep. Each
        # fetch returns the new BI-Rate inline next to the meeting
        # date, so the same op fills schedule and value.
        return fetch_bi_calendar(conn, dry_run=dry_run)

    def _fed_speeches_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Fed speeches archive re-fetch picks up newly-posted Board
        # speeches throughout the day. Schedule-only — no value to
        # fill (issue #56).
        return fetch_fed_speeches_calendar(conn, dry_run=dry_run)

    def _ecb_speeches_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # ECB speeches CSV re-fetch picks up newly-published
        # Executive Board speeches. Schedule-only — no value to fill
        # (issue #56).
        return fetch_ecb_speeches_calendar(conn, dry_run=dry_run)

    def _boe_speeches_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # BoE speeches sitemap re-fetch picks up newly-listed
        # speeches. Schedule-only — no value to fill (issue #56).
        return fetch_boe_speeches_calendar(conn, dry_run=dry_run)

    def _boj_speeches_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # BoJ per-year speeches archive re-fetch picks up newly-
        # published Policy Board speeches. Schedule-only — no value
        # to fill (issue #56).
        return fetch_boj_speeches_calendar(conn, dry_run=dry_run)

    def _eurostat_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Eurostat JSON-stat has no auth.
        from ingestion.timeseries.sdmx.providers.eurostat import EurostatClient
        client = EurostatClient()
        return fetch_eurostat_calendar(
            conn,
            client,
            start_period=resolved_start_period,
            end_period=resolved_end_period,
            dry_run=dry_run,
        )

    def _destatis_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        from ingestion.calendar.destatis_api import DestatisGenesisClient
        client = DestatisGenesisClient.from_env()
        return fetch_destatis_calendar(
            conn,
            client,
            start_year=resolved_start_year,
            end_year=resolved_end_year,
            dry_run=dry_run,
        )

    def _zew_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_zew_calendar(conn, dry_run=dry_run)

    def _ifo_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_ifo_calendar(conn, dry_run=dry_run)

    def _gfk_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_gfk_calendar(conn, dry_run=dry_run)

    def _hcob_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_hcob_calendar(conn, dry_run=dry_run)

    def _ec_bcs_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_ec_bcs_calendar(conn, dry_run=dry_run)

    def _insee_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_insee_calendar(conn, dry_run=dry_run)

    def _ine_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_ine_calendar(conn, dry_run=dry_run)

    def _istat_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_istat_calendar(conn, dry_run=dry_run)

    def _fed_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # fetch_fed_statement_values auto-discovers past FOMC rows
        # with ``actual IS NULL`` — no year window needed.
        return fetch_fed_statement_values(conn, dry_run=dry_run)

    def _stat_bureau_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_stat_bureau_values(conn, dry_run=dry_run)

    def _boj_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Auto-discovers past BoJ MPM rows with ``actual IS NULL`` —
        # mirrors the Fed-values shape, no year window needed.
        return fetch_boj_statement_values(conn, dry_run=dry_run)

    def _boj_tankan_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Tankan's schedule source (``yoshi/index.htm``) is past-only
        # — a newly-published release appears there only after
        # 08:50 JST on release day. The daily schedule pass may
        # therefore lag the release by several hours, which would
        # leave the value-side sweep with no ``actual IS NULL`` row
        # to discover. Seed the schedule side first so the same
        # sweep that observes the release also fills its value.
        fetch_boj_tankan_calendar(conn, dry_run=dry_run)
        return fetch_boj_tankan_outlines(conn, dry_run=dry_run)

    def _mof_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Auto-discovers past Balance-of-Trade rows with
        # ``actual IS NULL`` and fetches each release's XML feed.
        # Unlike Tankan, MoF's schedule page is forward-looking —
        # calendar rows exist well before the release — so no
        # in-sweep seed is needed.
        return fetch_mof_trade_values(conn, dry_run=dry_run)

    def _cao_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # The ``shouhi-e.html`` landing page carries at most one
        # release at a time — CAO overwrites it on each new
        # publication. The value scraper does a single GET per
        # sweep, parses the reference month + CCI value from the
        # two deterministic sentences, and upserts onto the
        # matching pending row. project_events' full upsert handles
        # both the insert-new and update-existing paths, so an
        # in-sweep schedule seed isn't required.
        return fetch_cao_consumer_confidence_values(conn, dry_run=dry_run)

    def _cao_gdp_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # The SNA archive is past-only, so seed the staged GDP rows
        # from the archive before discovering pending CSV values.
        fetch_cao_gdp_calendar(conn, dry_run=dry_run)
        return fetch_cao_gdp_values(conn, dry_run=dry_run)

    def _meti_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_meti_values(conn, dry_run=dry_run)

    def _nbs_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Auto-discovers past NBS schedule rows with ``actual IS NULL``
        # and resolves each release's article URL on the public
        # press-release listing. No year window — the listing's first
        # page covers the burst window plus the daily catch-up.
        return fetch_nbs_values(conn, dry_run=dry_run)

    value_side_map: dict[ConnectorName, _ConnectorFn] = {
        "bls":        _bls_values,
        "bea":        _bea_values,
        "census":     _census_values,
        "ism":        _ism_values,
        "umich":      _umich_values,
        "conference-board": _conference_board_values,
        "nar":        _nar_values,
        "ecb":        _ecb_values,
        "eia":        _eia_values,
        "dol":        _dol_values,
        "ons":        _ons_values,
        "boe":        _boe_values,
        "statcan":    _statcan_values,
        "boc":        _boc_values,
        "abs":        _abs_values,
        "rba":        _rba_values,
        "mospi":      _mospi_values,
        "rbi":        _rbi_values,
        "kostat":     _kostat_values,
        "bok":        _bok_values,
        "ibge":       _ibge_values,
        "bcb":        _bcb_values,
        "tuik":       _tuik_values,
        "tcmb":       _tcmb_values,
        "inegi":      _inegi_values,
        "banxico":    _banxico_values,
        "statssa":    _statssa_values,
        "sarb":       _sarb_values,
        "bank-indonesia": _bi_values,
        "eurostat":   _eurostat_values,
        "destatis":   _destatis_values,
        "zew":        _zew_values,
        "ifo":        _ifo_values,
        "gfk":        _gfk_values,
        "hcob":       _hcob_values,
        "ec-bcs":     _ec_bcs_values,
        "insee":      _insee_values,
        "ine":        _ine_values,
        "istat":      _istat_values,
        "fed-values": _fed_values,
        "nbs-values": _nbs_values,
        "stat-bureau-jp-values": _stat_bureau_values,
        "boj-values": _boj_values,
        "boj-tankan-values": _boj_tankan_values,
        "mof-jp-values": _mof_values,
        "cao-values": _cao_values,
        "cao-gdp-values": _cao_gdp_values,
        "meti-values": _meti_values,
        "fed-speeches": _fed_speeches_values,
        "ecb-speeches": _ecb_speeches_values,
        "boe-speeches": _boe_speeches_values,
        "boj-speeches": _boj_speeches_values,
    }

    requested = (
        tuple(connectors) if connectors is not None else ALL_VALUE_SIDE_CONNECTORS
    )
    valid_names = set(ALL_VALUE_SIDE_CONNECTORS)
    unknown_connectors = [n for n in requested if n not in valid_names]
    selected_set = {n for n in requested if n in valid_names}
    overrides = _connector_overrides or {}

    plan: list[tuple[ConnectorName, _ConnectorFn]] = []
    for name in ALL_VALUE_SIDE_CONNECTORS:
        if name not in selected_set:
            continue
        plan.append((name, overrides.get(name, value_side_map[name])))

    run_summary = RefreshRunSummary(
        connectors_planned=[name for name, _ in plan],
        dry_run=dry_run,
        unknown_connectors=unknown_connectors,
    )

    clock = _burst_clock or (lambda: datetime.now(timezone.utc))
    sleep_fn = _burst_sleep or time.sleep
    sweep_start_dt = clock()
    burst_deadline_dt = sweep_start_dt + _BURST_WINDOW_LOOKAHEAD

    state_conn = connection_factory()
    try:
        for name, fn in plan:
            predicate = _VALUE_SIDE_DUE_ROW_FILTERS.get(name)
            fetch_buffer = _VALUE_SIDE_FETCH_BUFFER.get(name, timedelta(0))
            # Count predicate selects events whose connector-side
            # eligibility window already opens inside the burst's
            # wall-clock budget. Subtract ``fetch_buffer`` from both
            # ends: a BoJ event at ``sweep_start − 30min`` becomes
            # eligible only at ``sweep_start + 30min`` (its buffer
            # opens then), so for the initial count it sits at the
            # window's upper edge — same shape as a non-buffered
            # event scheduled for ``sweep_start + 30min``.
            window_start_iso = (
                sweep_start_dt - _BURST_WINDOW_LOOKBACK - fetch_buffer
            ).isoformat()
            window_end_iso = (
                sweep_start_dt + _BURST_WINDOW_LOOKAHEAD - fetch_buffer
            ).isoformat()

            # Dry-run skips the burst — no values can fill, so a burst
            # loop would just spin without writes. Single invocation
            # preserves the existing dry-run plan envelope.
            initial_due = 0
            if not dry_run and predicate is not None:
                initial_due = _count_due_rows(
                    state_conn, predicate,
                    window_start_iso=window_start_iso,
                    window_end_iso=window_end_iso,
                )

            attempts = 0
            stopped_reason = "single_pass"
            last_result: ConnectorResult | None = None
            while True:
                attempts += 1
                last_result = _run_connector_with_breaker(
                    name, fn,
                    connection_factory=connection_factory,
                    state_conn=state_conn,
                    dry_run=dry_run,
                    log_prefix="value-side sweep",
                    budget_cap=DAILY_BUDGET_CAPS.get(name),
                )
                if initial_due == 0:
                    break
                if _result_indicates_skip(last_result):
                    stopped_reason = "breaker_or_budget"
                    break
                remaining = _count_due_rows(
                    state_conn, predicate or "1=1",
                    window_start_iso=window_start_iso,
                    window_end_iso=window_end_iso,
                )
                if remaining == 0:
                    stopped_reason = "actual_filled"
                    break
                if attempts >= _burst_max_attempts:
                    stopped_reason = "max_attempts"
                    break
                if clock() >= burst_deadline_dt:
                    stopped_reason = "window_closed"
                    break
                sleep_fn(_burst_interval_seconds)

            assert last_result is not None  # loop runs at least once
            if initial_due > 0:
                last_result.summary["burst_attempts"] = attempts
                last_result.summary["burst_initial_due"] = initial_due
                last_result.summary["burst_stopped_reason"] = stopped_reason
            run_summary.results.append(last_result)
    finally:
        try:
            state_conn.close()
        except Exception:
            pass

    run_summary.wall_seconds = time.monotonic() - started
    return run_summary
