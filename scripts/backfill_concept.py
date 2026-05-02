#!/usr/bin/env python3
"""Full historical backfill across `concept_map` (issue #114 P2).

Iterates every ``(concept_id, source_id, provider_series_id)`` row in
``concept_map`` and pulls the source's maximum available history into
``indicator_vintages``. Idempotent on
``(source, series_id, observation_date, vintage_date)`` via
``INSERT OR IGNORE`` semantics in the writer; resumable through a
cursor at ``.macro-data/backfill_cursor.json`` keyed by
``(source_id, provider_series_id)``.

Per-source quality tag:

* FRED  → first try ALFRED ``get_vintages(start_date='1776-07-04')`` and
          write ``native_pit`` rows; fall back to deep ``get_series`` and
          write ``single_observation`` if ALFRED returns nothing.
* Else  → one-shot deep history via the source's existing scraper, with
          ``vintage_quality='single_observation'`` (we have no revision
          context for these — issue #114 P2 contract).

Usage::

    PYTHONPATH=src python3 scripts/backfill_concept.py --concept CPI_US
    PYTHONPATH=src python3 scripts/backfill_concept.py --source fred
    PYTHONPATH=src python3 scripts/backfill_concept.py --all
    PYTHONPATH=src python3 scripts/backfill_concept.py --all --reset
    PYTHONPATH=src python3 scripts/backfill_concept.py --all --dry-run

Acknowledged: this is operator-run, not a CI step. Wall time hours, not
minutes — BLS is the rate-limit bottleneck.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from ingestion.normalization import normalize_observation_date  # noqa: E402
from storage import (  # noqa: E402
    IndicatorVintageRecord,
    SQLiteEngineStore,
    default_engine_db_path,
)

logger = logging.getLogger("backfill_concept")

CURSOR_PATH = REPO_ROOT / ".macro-data" / "backfill_cursor.json"
EARLIEST_BLS_YEAR = 1947  # BLS CPI starts here
# FRED API rejects ``1776-07-04`` for some series (e.g. ``DFF``) when both
# ``observation_start`` and ``realtime_start`` are set to that floor — the
# canonical ALFRED default — with HTTP 400. ``1900-01-01`` is wide enough
# to cover every series we map (oldest is CPIAUCSL @ 1947) and avoids the
# server-side validation quirk.
EARLIEST_FRED_DATE = "1900-01-01"
BLS_YEAR_CHUNK = 20  # BLS API limit per single POST


@dataclass(frozen=True)
class BackfillRow:
    concept_id: str
    source_id: str
    provider_series_id: str
    obs_family_id: str


@dataclass
class BackfillResult:
    rows_written: int = 0
    quality: str = ""
    skipped: bool = False
    error: str = ""

    def as_dict(self) -> dict[str, Any]:
        d = {"rows_written": self.rows_written, "quality": self.quality}
        if self.skipped:
            d["skipped"] = True
        if self.error:
            d["error"] = self.error
        return d


# ── Cursor ───────────────────────────────────────────────────────────


def cursor_key(source: str, series_id: str) -> str:
    return f"{source}::{series_id}"


def load_cursor() -> dict[str, dict[str, Any]]:
    if not CURSOR_PATH.is_file():
        return {}
    try:
        return json.loads(CURSOR_PATH.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("cursor unreadable, treating as empty: %s", exc)
        return {}


def save_cursor(state: dict[str, dict[str, Any]]) -> None:
    CURSOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CURSOR_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(CURSOR_PATH)


# ── concept_map iteration ────────────────────────────────────────────


def list_backfill_rows(
    store: SQLiteEngineStore,
    *,
    concept_filter: str | None,
    source_filter: str | None,
) -> list[BackfillRow]:
    sql = "SELECT concept_id, source_id, provider_series_id, obs_family_id FROM concept_map"
    clauses: list[str] = []
    params: list[str] = []
    if concept_filter:
        clauses.append("concept_id = ?")
        params.append(concept_filter)
    if source_filter:
        clauses.append("source_id = ?")
        params.append(source_filter)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY source_id, provider_series_id"
    with store._connection(commit=False) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [
        BackfillRow(
            concept_id=r["concept_id"],
            source_id=r["source_id"],
            provider_series_id=r["provider_series_id"],
            obs_family_id=r["obs_family_id"] or "",
        )
        for r in rows
    ]


# ── Per-source backfill handlers ─────────────────────────────────────


def backfill_fred(
    store: SQLiteEngineStore,
    row: BackfillRow,
    *,
    dry_run: bool,
) -> BackfillResult:
    """FRED: prefer ALFRED vintages, fall back to deep get_series."""
    from ingestion.timeseries.scrapers.fred import FredAPIError, FredClient

    client = FredClient()
    if not client.api_key:
        return BackfillResult(error="missing FRED_API_KEY", skipped=True)

    # ── ALFRED first. Some FRED series reject the canonical
    # ``observation_start=1776-07-04`` realtime sweep (e.g. ``DFF``)
    # with HTTP 400 — fall through to the deep ``get_series`` fetch
    # rather than failing the entire row.
    try:
        vintages = client.get_vintages(
            row.provider_series_id,
            start_date=EARLIEST_FRED_DATE,
            realtime_start=EARLIEST_FRED_DATE,
        )
    except FredAPIError as exc:
        logger.info("ALFRED unavailable for %s (%s) — falling back to get_series",
                    row.provider_series_id, exc)
        vintages = []

    if vintages:
        if dry_run:
            return BackfillResult(rows_written=len(vintages), quality="native_pit")
        for v in vintages:
            store.upsert_indicator_vintage(
                IndicatorVintageRecord(
                    series_id=row.provider_series_id,
                    source="fred",
                    observation_date=v.date,
                    vintage_date=v.vintage_date,
                    value=v.value,
                    metadata={"concept_id": row.concept_id},
                    obs_family_id=row.obs_family_id or None,
                    vintage_quality="native_pit",
                )
            )
        return BackfillResult(rows_written=len(vintages), quality="native_pit")

    # ── Fall back to deep get_series (no realtime period)
    try:
        observations = client.get_series(
            row.provider_series_id, start_date=EARLIEST_FRED_DATE, limit=100000,
        )
    except FredAPIError as exc:
        return BackfillResult(error=f"FRED get_series failed: {exc}")

    return _write_single_observations(
        store, row, observations, source="fred", dry_run=dry_run,
    )


def backfill_bls(
    store: SQLiteEngineStore,
    row: BackfillRow,
    *,
    dry_run: bool,
) -> BackfillResult:
    """BLS: chunked by 20-year window from 1947 to current year."""
    from ingestion.timeseries.scrapers.bls import BLSAPIError, BLSClient

    client = BLSClient()
    if not client.api_key:
        return BackfillResult(error="missing BLS_API_KEY", skipped=True)

    end_year = datetime.now(UTC).year
    all_obs: list[Any] = []
    try:
        for start in range(EARLIEST_BLS_YEAR, end_year + 1, BLS_YEAR_CHUNK):
            end = min(start + BLS_YEAR_CHUNK - 1, end_year)
            chunk = client.get_series_single(
                row.provider_series_id, start_year=start, end_year=end,
            )
            all_obs.extend(chunk)
            time.sleep(0.5)  # be courteous
    except BLSAPIError as exc:
        return BackfillResult(error=f"BLS failed: {exc}")

    return _write_single_observations(
        store, row, all_obs, source="bls", dry_run=dry_run,
    )


def backfill_eia(
    store: SQLiteEngineStore,
    row: BackfillRow,
    *,
    dry_run: bool,
) -> BackfillResult:
    """EIA: full deep history via configured route."""
    from ingestion.series_config import EIA_SERIES
    from ingestion.timeseries.scrapers.eia import EIAAPIError, EIAClient

    cfg = next(
        (c for c in EIA_SERIES.values() if c["series_id"] == row.provider_series_id),
        None,
    )
    if cfg is None:
        return BackfillResult(error=f"no EIA_SERIES config for {row.provider_series_id}")

    client = EIAClient()
    if not client.api_key:
        return BackfillResult(error="missing EIA_API_KEY", skipped=True)

    try:
        observations = client.get_series(
            cfg["route"], params=dict(cfg["params"]),
            series_id=row.provider_series_id, limit=100000,
        )
    except EIAAPIError as exc:
        return BackfillResult(error=f"EIA failed: {exc}")
    frequency = str(cfg.get("params", {}).get("frequency", "daily")).lower()
    return _write_single_observations(
        store, row, observations, source="eia",
        dry_run=dry_run, frequency=frequency,
    )


def backfill_treasury_fiscal(
    store: SQLiteEngineStore,
    row: BackfillRow,
    *,
    dry_run: bool,
) -> BackfillResult:
    from ingestion.timeseries.scrapers.treasury_fiscal import TreasuryFiscalClient

    client = TreasuryFiscalClient()
    sid = row.provider_series_id
    fetcher: Callable[[], Iterable[Any]] | None = None
    # provider_series_id values come from concept_map seeds.
    if sid == "TREAS_DEBT_TOTAL":
        fetcher = lambda: client.fetch_debt_outstanding(limit=100000)  # noqa: E731
    elif sid == "TREAS_TGA_BALANCE":
        fetcher = lambda: client.fetch_tga_balance(limit=100000)  # noqa: E731
    elif sid == "TREAS_AVG_RATE":
        fetcher = lambda: client.fetch_avg_interest_rates(limit=100000)  # noqa: E731

    if fetcher is None:
        return BackfillResult(error=f"no treasury fetcher for {sid}", skipped=True)
    observations = list(fetcher())
    return _write_single_observations(
        store, row, observations, source="treasury_fiscal", dry_run=dry_run,
    )


def backfill_nyfed(
    store: SQLiteEngineStore,
    row: BackfillRow,
    *,
    dry_run: bool,
) -> BackfillResult:
    """NY Fed reference rates: ``/last/{n}.json`` caps server-side, so for
    deep history use ``/search.json`` with MM/DD/YYYY date bounds.
    """
    from ingestion.timeseries.scrapers.nyfed import NYFedRatesClient

    client = NYFedRatesClient()
    sid = row.provider_series_id
    rate_map = {
        "NYFED_EFFR": ("unsecured/effr", "EFFR"),
        "NYFED_OBFR": ("unsecured/obfr", "OBFR"),
        "NYFED_SOFR": ("secured/sofr", "SOFR"),
    }
    if sid in rate_map:
        path, rate_type = rate_map[sid]
        observations = _nyfed_search_rates(client, path, rate_type)
    elif sid == "NYFED_GSCPI":
        observations = client.fetch_gscpi(last_n=None)
    else:
        return BackfillResult(error=f"no nyfed fetcher for {sid}", skipped=True)
    return _write_single_observations(
        store, row, observations, source="nyfed", dry_run=dry_run,
    )


def _nyfed_search_rates(client, path: str, rate_type: str):
    """NY Fed reference rates via the ``/search.json`` endpoint (MM/DD/YYYY).

    The ``/last/{n}`` endpoint caps server-side, so passing a large ``n``
    returns HTTP 400. ``/search`` accepts arbitrary ranges. EFFR / OBFR
    publication started 2000-07-03; SOFR began 2018-04-03; we extend the
    floor to 1990-01-01 to be safe — NY Fed silently caps to each
    series' earliest available date.
    """
    url = f"{client.BASE_URL}/{path}/search.json"
    params = {"startDate": "01/01/1990", "endDate": "12/31/2099"}
    response = client.session.get(url, params=params, timeout=60)
    response.raise_for_status()
    return client._parse_rates(response.json(), rate_type)


def backfill_worldbank(
    store: SQLiteEngineStore,
    row: BackfillRow,
    *,
    dry_run: bool,
) -> BackfillResult:
    from ingestion.timeseries.scrapers.worldbank import (
        WorldBankAPIError, WorldBankClient,
    )

    # World Bank concept_map series_ids look like "WB_GDP_GROWTH_US" — our
    # generated ID. Resolve to (indicator_code, country_code) via WORLDBANK_SERIES.
    from ingestion.series_config import WORLDBANK_SERIES

    cfg = next(
        (c for c in WORLDBANK_SERIES.values() if c.series_id == row.provider_series_id),
        None,
    )
    if cfg is None:
        return BackfillResult(
            error=f"no WORLDBANK_SERIES config for {row.provider_series_id}",
            skipped=True,
        )
    client = WorldBankClient()
    try:
        rows, _total = client.fetch_indicator_raw(
            cfg.indicator, cfg.country,
            per_page=20000,
        )
    except WorldBankAPIError as exc:
        return BackfillResult(error=f"World Bank failed: {exc}")
    # World Bank returns dicts; values can be null. Filter to ones with values.
    valid = [r for r in rows if r.get("value") is not None and r.get("date")]
    if dry_run:
        return BackfillResult(rows_written=len(valid), quality="single_observation")
    scrape_iso = datetime.now(UTC).isoformat()
    written = 0
    for r in valid:
        try:
            value = float(r["value"])
        except (TypeError, ValueError):
            continue
        observation_date = normalize_observation_date(str(r["date"]), "annual")
        store.upsert_indicator_vintage(
            IndicatorVintageRecord(
                series_id=row.provider_series_id,
                source="worldbank",
                observation_date=observation_date,
                vintage_date=scrape_iso,
                value=value,
                metadata={"concept_id": row.concept_id, "indicator": cfg.indicator},
                obs_family_id=row.obs_family_id or None,
                vintage_quality="single_observation",
            )
        )
        written += 1
    return BackfillResult(rows_written=written, quality="single_observation")


def backfill_sdmx(
    store: SQLiteEngineStore,
    row: BackfillRow,
    *,
    dry_run: bool,
    provider: str,
) -> BackfillResult:
    """Generic SDMX backfill (IMF, ECB, BIS, Bundesbank).

    Looks up the dataflow + key from the relevant SERIES dict, calls
    ``client.get_data`` with no period bounds, writes each observation
    as a ``single_observation`` vintage tagged at ``utc_now``. OECD and
    Eurostat have non-standard signatures (positional version / dataset
    URL shape) and live in their own handlers.
    """
    cfg, client = _resolve_sdmx(provider, row.provider_series_id)
    if cfg is None or client is None:
        return BackfillResult(
            error=f"no {provider.upper()} config / client for {row.provider_series_id}",
            skipped=True,
        )
    try:
        kwargs: dict[str, Any] = {"series_id": row.provider_series_id, "limit": 0}
        if cfg.get("version"):
            kwargs["version"] = cfg["version"]
        observations = client.get_data(cfg["dataflow"], cfg["key"], **kwargs)
    except Exception as exc:  # noqa: BLE001 — provider-specific failure modes are diverse
        return BackfillResult(error=f"{provider} SDMX failed: {exc}")
    return _write_single_observations(
        store, row, observations, source=provider, dry_run=dry_run,
    )


def backfill_oecd(
    store: SQLiteEngineStore,
    row: BackfillRow,
    *,
    dry_run: bool,
) -> BackfillResult:
    """OECD signature: ``get_data(dataflow_id, version, key, *, series_id, ...)``."""
    from ingestion.series_config import OECD_SERIES
    from ingestion.timeseries.sdmx.providers.oecd import OECDClient

    cfg = next(
        (c for c in OECD_SERIES.values() if c.series_id == row.provider_series_id),
        None,
    )
    if cfg is None:
        return BackfillResult(
            error=f"no OECD_SERIES config for {row.provider_series_id}",
            skipped=True,
        )
    client = OECDClient()
    # OECD rejects ``version=latest`` in the data URL — resolve it via
    # ``get_dataflow`` first (the same pattern the production OECD
    # ingestion client uses). ``limit=None`` returns all observations
    # (limit=0 is "trim to zero" in the parser, not "no cap").
    #
    # When ``cfg.key`` is ``None`` the selector lives in ``cfg.filters``
    # — passing ``key=""`` would resolve to ``all`` and download the
    # whole dataflow under a single ``provider_series_id``, mixing
    # countries / measures. ``fetch_data`` honours ``filters`` via
    # ``_resolve_key`` when ``key`` is ``None``.
    try:
        dataflow = client.get_dataflow(
            cfg.dataflow, agency_id=cfg.agency_id, version=cfg.version or "latest",
        )
        kwargs: dict[str, Any] = {
            "agency_id": cfg.agency_id,
            "version": dataflow.version,
            "series_id": row.provider_series_id,
            "limit": None,
        }
        if cfg.key:
            kwargs["key"] = cfg.key
        else:
            kwargs["filters"] = cfg.filters
            kwargs["use_defaults"] = True
        observations = client.fetch_data(cfg.dataflow, **kwargs)
    except Exception as exc:  # noqa: BLE001
        return BackfillResult(error=f"OECD failed: {exc}")
    return _write_single_observations(
        store, row, observations, source="oecd", dry_run=dry_run,
    )


def backfill_eurostat(
    store: SQLiteEngineStore,
    row: BackfillRow,
    *,
    dry_run: bool,
) -> BackfillResult:
    """Eurostat uses dataset+params shape, not dataflow+key."""
    from ingestion.series_config import EUROSTAT_SERIES
    from ingestion.timeseries.sdmx.providers.eurostat import EurostatClient

    cfg = next(
        (c for c in EUROSTAT_SERIES.values() if c["series_id"] == row.provider_series_id),
        None,
    )
    if cfg is None:
        return BackfillResult(
            error=f"no EUROSTAT_SERIES config for {row.provider_series_id}",
            skipped=True,
        )
    client = EurostatClient()
    # ``limit=0`` slices the Eurostat parser output to ``[:0]`` — pass a
    # large integer to get the full series instead.
    try:
        observations = client.get_dataset(
            cfg["dataset"],
            params=dict(cfg["params"]),
            series_id=row.provider_series_id,
            limit=10**9,
        )
    except Exception as exc:  # noqa: BLE001
        return BackfillResult(error=f"Eurostat failed: {exc}")
    return _write_single_observations(
        store, row, observations, source="eurostat", dry_run=dry_run,
    )


def backfill_rateprobability(
    store: SQLiteEngineStore,  # noqa: ARG001
    row: BackfillRow,  # noqa: ARG001
    *,
    dry_run: bool,  # noqa: ARG001
) -> BackfillResult:
    return BackfillResult(
        skipped=True,
        error="rateprobability is a current-snapshot source — no historical depth to backfill",
    )


def backfill_snapshot_only(
    store: SQLiteEngineStore,  # noqa: ARG001
    row: BackfillRow,
    *,
    dry_run: bool,  # noqa: ARG001
) -> BackfillResult:
    """Sources whose upstream only exposes the current period.

    AISI weekly steel, ISM PMI report pages, sentix composite — each
    publishes only the latest report. Deep-history backfill would
    require parsing archived report URLs (paid or scraped), which is
    out of #114 P2 scope. The regular daily refresh continues to
    accumulate vintages organically through ``upsert_indicator_observation``.
    """
    return BackfillResult(
        skipped=True,
        error=f"{row.source_id} publishes only the current period — deep-history "
              "backfill not wired (regular refresh accumulates over time)",
    )


def backfill_mof_jgb(
    store: SQLiteEngineStore,
    row: BackfillRow,
    *,
    dry_run: bool,
) -> BackfillResult:
    """MOF JGB: download the official CSV (full history) and pick the
    requested maturity column."""
    from ingestion.timeseries.scrapers.mof_jgb import MOFJGBClient

    # MOF_JP_GOVT_10Y → maturity column "10Y"
    if not row.provider_series_id.startswith("MOF_JP_GOVT_"):
        return BackfillResult(
            error=f"unrecognised mof_jp series_id {row.provider_series_id}",
            skipped=True,
        )
    maturity = row.provider_series_id.removeprefix("MOF_JP_GOVT_")
    client = MOFJGBClient()
    try:
        all_series = client.get_all_series_with_raw((maturity,), limit=0)
    except Exception as exc:  # noqa: BLE001
        return BackfillResult(error=f"MOF JGB fetch failed: {exc}")
    observations, _payload, _params = all_series.get(maturity, ([], {}, {}))
    return _write_single_observations(
        store, row, observations, source="mof_jp", dry_run=dry_run,
    )


def backfill_redbook(
    store: SQLiteEngineStore,
    row: BackfillRow,
    *,
    dry_run: bool,
) -> BackfillResult:
    """Redbook (TE-hosted): historical YoY retail-sales feed.

    Concept_map only seeds ``REDBOOK_RETAIL_SALES_YOY_US`` today; if the
    seed list grows to multiple tickers, this handler stays a single
    full-history fetch since TE's historical endpoint returns the
    series' entire range in one call.
    """
    from ingestion.timeseries.scrapers.redbook import RedbookAuthError, RedbookClient

    client = RedbookClient()
    try:
        rows = client.fetch_historical_rows(start_date="1990-01-01")
    except RedbookAuthError as exc:
        return BackfillResult(error=f"Redbook auth: {exc}", skipped=True)
    except Exception as exc:  # noqa: BLE001
        return BackfillResult(error=f"Redbook fetch failed: {exc}")
    return _write_single_observations(
        store, row, rows, source="redbook", dry_run=dry_run,
        frequency="weekly",
    )


# ── SDMX dispatch helper ─────────────────────────────────────────────


def _resolve_sdmx(provider: str, series_id: str):  # type: ignore[return]
    if provider == "imf":
        from ingestion.series_config import IMF_SERIES
        from ingestion.timeseries.sdmx.providers.imf import IMFClient

        cfg = next((c for c in IMF_SERIES.values() if c["series_id"] == series_id), None)
        return cfg, IMFClient() if cfg else None
    if provider == "ecb":
        from ingestion.series_config import ECB_SERIES
        from ingestion.timeseries.sdmx.providers.ecb import ECBClient

        cfg = next((c for c in ECB_SERIES.values() if c["series_id"] == series_id), None)
        return cfg, ECBClient() if cfg else None
    if provider == "bis":
        from ingestion.series_config import BIS_SERIES
        from ingestion.timeseries.sdmx.providers.bis import BISClient

        cfg = next((c for c in BIS_SERIES.values() if c["series_id"] == series_id), None)
        return cfg, BISClient() if cfg else None
    if provider == "bundesbank":
        from ingestion.series_config import BUNDESBANK_SERIES
        from ingestion.timeseries.sdmx.providers.bundesbank import BundesbankClient

        cfg = next(
            (c for c in BUNDESBANK_SERIES.values() if c["series_id"] == series_id), None,
        )
        return cfg, BundesbankClient() if cfg else None
    return None, None


# ── Generic single-observation writer ────────────────────────────────


def _coerce_value(obs: Any) -> float | None:
    """Pull a numeric value off whatever record shape the scraper returns.

    Different scrapers return different field names: SDMX/EIA/FRED expose
    ``value``; ``NYFedRate`` exposes ``rate``; ``NYFedGSCPI`` exposes
    ``index_value``; treasury_fiscal exposes ``value``. Centralising the
    lookup keeps the writer source-agnostic.
    """
    for attr in ("value", "rate", "index_value", "current_value"):
        v = getattr(obs, attr, None)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def _write_single_observations(
    store: SQLiteEngineStore,
    row: BackfillRow,
    observations: list[Any],
    *,
    source: str,
    dry_run: bool,
    frequency: str = "",
) -> BackfillResult:
    """Write each observation as a ``single_observation`` vintage.

    ``vintage_date = observation_date`` makes the write idempotent — a
    rerun (after ``--reset`` or a crash) re-collides on the existing PK
    via ``INSERT OR IGNORE`` rather than spawning a fresh duplicate
    vintage per scrape time. Issue #114 P2 originally specified
    ``vintage_date = scrape_time``; we relax to observation_date because
    a one-shot deep-history pull has no revision context — there's no
    information loss in collapsing the stamp.
    """
    if dry_run:
        return BackfillResult(rows_written=len(observations), quality="single_observation")
    written = 0
    for obs in observations:
        value = _coerce_value(obs)
        if value is None:
            continue
        date_str = (
            getattr(obs, "date", "")
            or getattr(obs, "period", "")
            or getattr(obs, "effective_date", "")
        )
        if not date_str:
            continue
        try:
            observation_date = normalize_observation_date(
                date_str, frequency or "daily",
            )
        except Exception:  # noqa: BLE001
            observation_date = str(date_str)
        store.upsert_indicator_vintage(
            IndicatorVintageRecord(
                series_id=row.provider_series_id,
                source=source,
                observation_date=observation_date,
                vintage_date=observation_date,  # idempotent stable tag
                value=value,
                metadata={"concept_id": row.concept_id},
                obs_family_id=row.obs_family_id or None,
                vintage_quality="single_observation",
            )
        )
        written += 1
    return BackfillResult(rows_written=written, quality="single_observation")


# ── Source dispatcher ────────────────────────────────────────────────


def dispatch(
    store: SQLiteEngineStore, row: BackfillRow, *, dry_run: bool,
) -> BackfillResult:
    if row.source_id == "fred":
        return backfill_fred(store, row, dry_run=dry_run)
    if row.source_id == "bls":
        return backfill_bls(store, row, dry_run=dry_run)
    if row.source_id == "eia":
        return backfill_eia(store, row, dry_run=dry_run)
    if row.source_id == "treasury_fiscal":
        return backfill_treasury_fiscal(store, row, dry_run=dry_run)
    if row.source_id == "nyfed":
        return backfill_nyfed(store, row, dry_run=dry_run)
    if row.source_id == "worldbank":
        return backfill_worldbank(store, row, dry_run=dry_run)
    if row.source_id == "eurostat":
        return backfill_eurostat(store, row, dry_run=dry_run)
    if row.source_id == "oecd":
        return backfill_oecd(store, row, dry_run=dry_run)
    if row.source_id in {"imf", "ecb", "bis", "bundesbank"}:
        return backfill_sdmx(store, row, dry_run=dry_run, provider=row.source_id)
    if row.source_id == "rateprobability":
        return backfill_rateprobability(store, row, dry_run=dry_run)
    if row.source_id == "mof_jp":
        return backfill_mof_jgb(store, row, dry_run=dry_run)
    if row.source_id == "redbook":
        return backfill_redbook(store, row, dry_run=dry_run)
    if row.source_id in {"aisi", "ism", "sentix"}:
        return backfill_snapshot_only(store, row, dry_run=dry_run)
    return BackfillResult(
        skipped=True,
        error=f"no backfill handler wired for source {row.source_id!r}",
    )


# ── CLI entry-point ──────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--concept", help="single concept_id to backfill (e.g. CPI_US)")
    parser.add_argument("--source", help="restrict to one source_id (fred, bls, ...)")
    parser.add_argument("--all", action="store_true", help="full sweep across concept_map")
    parser.add_argument("--dry-run", action="store_true",
                        help="resolve config + count rows, write nothing")
    parser.add_argument("--reset", action="store_true",
                        help="clear cursor before running")
    parser.add_argument("--db-path", type=Path, default=None,
                        help="engine.db (default: .macro-data/engine.db)")
    parser.add_argument("--log-level", default="INFO",
                        choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not (args.concept or args.source or args.all):
        parser.error("must supply --concept / --source / --all")

    db_path = args.db_path or default_engine_db_path()
    store = SQLiteEngineStore(db_path=db_path)
    store.init_schema()
    store.seed_concept_map()

    if args.reset and CURSOR_PATH.is_file():
        CURSOR_PATH.unlink()
        logger.info("cursor reset")

    rows = list_backfill_rows(
        store, concept_filter=args.concept, source_filter=args.source,
    )
    if not rows:
        logger.error("no concept_map rows match filter")
        return 2

    cursor = load_cursor()
    completed = 0
    skipped = 0
    failed = 0
    written_total = 0
    started = datetime.now(UTC).isoformat()

    for i, row in enumerate(rows, start=1):
        key = cursor_key(row.source_id, row.provider_series_id)
        prior = cursor.get(key)
        if prior and prior.get("status") == "completed" and not args.reset:
            logger.info("[%d/%d] %s skip — already completed (%d rows)",
                        i, len(rows), key, prior.get("rows_written", 0))
            skipped += 1
            continue

        logger.info("[%d/%d] %s backfilling …", i, len(rows), key)
        try:
            result = dispatch(store, row, dry_run=args.dry_run)
        except Exception as exc:  # noqa: BLE001
            logger.exception("[%d/%d] %s unexpected failure", i, len(rows), key)
            cursor[key] = {
                "status": "failed", "error": f"unexpected: {exc}",
                "started_at": started,
            }
            failed += 1
            save_cursor(cursor)
            continue

        if result.error and not result.skipped:
            failed += 1
            if not args.dry_run:
                cursor[key] = {
                    "status": "failed", "error": result.error,
                    "started_at": started,
                }
            logger.warning("[%d/%d] %s FAILED: %s",
                           i, len(rows), key, result.error)
        elif result.skipped:
            skipped += 1
            if not args.dry_run:
                cursor[key] = {
                    "status": "skipped", "reason": result.error,
                    "started_at": started,
                }
            logger.info("[%d/%d] %s skipped: %s",
                        i, len(rows), key, result.error)
        else:
            completed += 1
            written_total += result.rows_written
            if not args.dry_run:
                cursor[key] = {
                    "status": "completed",
                    "rows_written": result.rows_written,
                    "quality": result.quality,
                    "completed_at": datetime.now(UTC).isoformat(),
                }
            logger.info("[%d/%d] %s OK — %d rows tagged %s%s",
                        i, len(rows), key, result.rows_written,
                        result.quality, " (dry-run)" if args.dry_run else "")
        # Dry-run runs never persist cursor state — they're a planning
        # tool, not a write attempt. Real runs persist after every row
        # so an interrupted multi-hour sweep can resume.
        if not args.dry_run:
            save_cursor(cursor)

    print()
    print("--- backfill summary ---")
    print(f"rows scanned    : {len(rows)}")
    print(f"completed       : {completed}")
    print(f"skipped         : {skipped}")
    print(f"failed          : {failed}")
    print(f"vintages written: {written_total}{' (dry-run)' if args.dry_run else ''}")
    print(f"cursor          : {CURSOR_PATH}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
