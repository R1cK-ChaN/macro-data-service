"""Mocked tests for the ECB speeches calendar connector (issue #56 P1).

The captured fixture
``tests/fixtures/ecb_speeches/all_ECB_speeches.csv`` was downloaded
live on 2026-04-27 from
``ecb.europa.eu/press/key/shared/data/all_ECB_speeches.csv``ed and trimmed
to 2018-01-01 onward (~880 rows). The full upstream CSV is ~3,000
rows / 57 MB and exceeds GitHub's recommended max file size; the
slimmed fixture covers the same parser surfaces (Executive Board
speakers, monthly density, recent + lookback) without bloating the
repo.

No real HTTP in CI — every test injects the ``csv_fetcher`` seam.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.ecb_speeches_api import (
    EcbSpeechesCsvParseError,
    fetch_ecb_speeches_calendar,
    parse_speeches_csv,
    speech_to_records,
)
from ingestion.calendar.ecb_speeches_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "ecb_speeches" / "all_ECB_speeches.csv"
)


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _csv_text() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8-sig")


# ── parser ───────────────────────────────────────────────────────


def test_parse_speeches_csv_extracts_recent_entries() -> None:
    """The fixture's most recent entries (2026-Q1) must parse with
    speaker / title / subtitle intact."""
    speeches = parse_speeches_csv(_csv_text())
    assert len(speeches) >= 500
    schnabel = next(
        s for s in speeches
        if s.delivery_date.isoformat() == "2026-03-27"
        and s.speaker == "Isabel Schnabel"
    )
    assert "geopolitical fragmentation" in schnabel.title
    assert "Executive Board" in schnabel.subtitle


def test_parse_speeches_csv_orders_by_date_ascending() -> None:
    """Output sorted by delivery date ascending so a consumer can
    paginate or eyeball latest entries with a tail rather than a
    sort."""
    speeches = parse_speeches_csv(_csv_text())
    iso_list = [s.delivery_date.isoformat() for s in speeches]
    assert iso_list == sorted(iso_list)


def test_parse_speeches_csv_keeps_empty_speaker_rows() -> None:
    """Some Governing-Council-attributed entries carry an empty
    ``speakers`` field; the parser must keep these rows but flag
    the missing speaker so downstream display can fall back."""
    csv_text = (
        "date|speakers|title|subtitle|contents\r\n"
        "2026-03-25||The outlook for the euro area economy|Statement|Body\r\n"
    )
    [speech] = parse_speeches_csv(csv_text)
    assert speech.speaker is None
    assert speech.title == "The outlook for the euro area economy"


def test_parse_speeches_csv_raises_on_unexpected_header() -> None:
    """The CSV format is contractual; a column reorder is a drift
    signal that must surface, not silently shift columns."""
    csv_text = (
        "Date|Speaker|Title|Body\r\n"
        "2026-01-01|Lagarde|x|y\r\n"
    )
    with pytest.raises(EcbSpeechesCsvParseError, match="header"):
        parse_speeches_csv(csv_text)


def test_parse_speeches_csv_raises_on_empty_file() -> None:
    with pytest.raises(EcbSpeechesCsvParseError, match="empty"):
        parse_speeches_csv("")


def test_parse_speeches_csv_raises_on_unparseable_date() -> None:
    csv_text = (
        "date|speakers|title|subtitle|contents\r\n"
        "26 March 2026|Lagarde|Title|Subtitle|Body\r\n"
    )
    with pytest.raises(EcbSpeechesCsvParseError, match="date"):
        parse_speeches_csv(csv_text)


def test_parse_speeches_csv_raises_on_zero_data_rows() -> None:
    csv_text = "date|speakers|title|subtitle|contents\r\n"
    with pytest.raises(EcbSpeechesCsvParseError, match="zero data rows"):
        parse_speeches_csv(csv_text)


# ── projection ───────────────────────────────────────────────────


def test_speech_to_records_anchors_on_delivery_date_with_date_precision() -> None:
    speeches = parse_speeches_csv(_csv_text())
    schnabel = next(
        s for s in speeches
        if s.delivery_date.isoformat() == "2026-03-27"
        and s.speaker == "Isabel Schnabel"
    )
    raw_rec, event_rec = speech_to_records(
        schnabel, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "EU"
    assert event_rec.currency == "EUR"
    assert event_rec.actual is None
    assert event_rec.event_time_precision == "date"
    assert event_rec.event_time_utc.startswith("2026-03-27T00:00:00")
    assert event_rec.reference_date == "2026-03-27"
    assert event_rec.title.startswith("ECB Speech — Isabel Schnabel:")
    assert event_rec.source == "European Central Bank"
    assert raw_rec.provider == PROVIDER
    _, event_rec_again = speech_to_records(
        schnabel, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


def test_speech_to_records_distinct_provider_ids_per_unique_row() -> None:
    """Two speeches sharing the same date but different titles must
    project to distinct ids; same date + same title (rare ECB
    re-publication) collapses to one id intentionally so a
    duplicate row in the CSV is idempotent."""
    speeches = parse_speeches_csv(_csv_text())
    ids = {
        speech_to_records(s, snapshot_epoch_ms=1_800_000_000_000)[1].provider_event_id
        for s in speeches
    }
    distinct_keys = {(s.delivery_date, s.title) for s in speeches}
    # Each unique (date, title) pair should map to one id; CSV-side
    # duplicates collapse, distinct titles do not collide.
    assert len(ids) == len(distinct_keys)


def test_speech_to_records_falls_back_to_bare_title_for_empty_speaker() -> None:
    csv_text = (
        "date|speakers|title|subtitle|contents\r\n"
        "2026-03-25||The outlook for the euro area economy|Statement|Body\r\n"
    )
    [speech] = parse_speeches_csv(csv_text)
    _, event_rec = speech_to_records(
        speech, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.title == "ECB Speech: The outlook for the euro area economy"


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_ecb_speeches_calendar_writes_one_event_per_speech(
    store: SQLiteEngineStore,
) -> None:
    """The trimmed fixture (2018+) lists ~880 speeches; use ≥ 500 to
    absorb any future re-trim while still exercising the multi-row
    projection surface."""
    def fetcher() -> str:
        return _csv_text()

    with store._connection(commit=True) as conn:
        summary = fetch_ecb_speeches_calendar(
            conn,
            dry_run=False,
            csv_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert summary.speeches_parsed >= 500
    assert summary.events_upserted == summary.speeches_parsed


def test_fetch_ecb_speeches_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    def broken() -> str:
        raise RuntimeError("simulated 503 from ECB CDN")

    with store._connection(commit=True) as conn:
        summary = fetch_ecb_speeches_calendar(
            conn, dry_run=False, csv_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_ecb_speeches_calendar_records_parse_error_on_drift(
    store: SQLiteEngineStore,
) -> None:
    def empty_header() -> str:
        return "date|speakers|title|subtitle|contents\r\n"

    with store._connection(commit=True) as conn:
        summary = fetch_ecb_speeches_calendar(
            conn, dry_run=False, csv_fetcher=empty_header,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_ecb_speeches_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_ecb_speeches_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == ["ECB_SPEECHES"]


def test_fetch_ecb_speeches_calendar_idempotent_on_repeat(
    store: SQLiteEngineStore,
) -> None:
    """A second sweep over the same CSV must not add new rows. The
    slug-anchored provider_event_id collapses re-projected rows on
    upsert; a small number of historical (date, title) pairs in the
    upstream CSV (an old T2S Framework Agreement address is the
    canonical example, dropped from the trimmed 2018+ fixture)
    would collapse the same way."""
    def fetcher() -> str:
        return _csv_text()
    with store._connection(commit=True) as conn:
        fetch_ecb_speeches_calendar(
            conn, dry_run=False, csv_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    with store._connection(commit=False) as conn:
        first_count = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()[0]
    with store._connection(commit=True) as conn:
        fetch_ecb_speeches_calendar(
            conn, dry_run=False, csv_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_001,
        )
    with store._connection(commit=False) as conn:
        second_count = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()[0]
    assert second_count == first_count
    assert first_count > 0


# ── scheduler + agency wiring ───────────────────────────────────


def test_ecb_speeches_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "ecb-speeches" in ALL_CONNECTORS
    assert "ecb-speeches" in ALL_VALUE_SIDE_CONNECTORS


def test_ecb_speeches_agency_attribution_provider_only_in_p1() -> None:
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    agency = provider_to_agency("ecb-speeches")
    assert agency is not None and agency.agency_id == "ECB_SPEECHES"
    assert agency.indicators == frozenset()
    assert agency_for("EU", "ECB_SPEECHES") is None
