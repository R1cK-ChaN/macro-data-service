from __future__ import annotations

import io
import struct
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.fetchers._nyfed import NYFedFetcher
from ingestion.timeseries.scrapers.nyfed import (
    GSCPI_DATA_URL,
    NYFedGSCPI,
    NYFedRatesClient,
    _parse_gscpi_biff8_stream,
    parse_gscpi_workbook,
)
from ingestion.source_capabilities import SourceCapabilityManager
from storage.sqlite import SQLiteEngineStore


@dataclass(frozen=True)
class _FakeRate:
    date: str = "2026-03-31"
    rate: float = 4.31
    percentile_1: float | None = None
    percentile_25: float | None = None
    percentile_75: float | None = None
    percentile_99: float | None = None
    volume_billions: float | None = None
    target_rate_from: float | None = None
    target_rate_to: float | None = None


class _FakeResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


class _FakeSession:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.calls: list[tuple[str, int]] = []

    def get(self, url: str, timeout: int) -> _FakeResponse:
        self.calls.append((url, timeout))
        return _FakeResponse(self.content)


class _FakeNYFedClient:
    def fetch_sofr(self, last_n: int) -> list[_FakeRate]:
        return [_FakeRate(rate=4.31)]

    def fetch_effr(self, last_n: int) -> list[_FakeRate]:
        return [_FakeRate(rate=4.32)]

    def fetch_obfr(self, last_n: int) -> list[_FakeRate]:
        return [_FakeRate(rate=4.30)]

    def fetch_gscpi(self, last_n: int) -> list[NYFedGSCPI]:
        return [NYFedGSCPI(date="2026-03-31", value=0.6769936395789531)]


def _record(record_type: int, payload: bytes = b"") -> bytes:
    return struct.pack("<HH", record_type, len(payload)) + payload


def _biff_string(value: str) -> bytes:
    return struct.pack("<HB", len(value), 0) + value.encode("latin1")


def _boundsheet(offset: int, name: str) -> bytes:
    return _record(
        0x0085,
        struct.pack("<IBBBB", offset, 0, 0, len(name), 0) + name.encode("latin1"),
    )


def _labelsst(row: int, col: int, string_idx: int) -> bytes:
    return _record(0x00FD, struct.pack("<HHHI", row, col, 0, string_idx))


def _number(row: int, col: int, value: float) -> bytes:
    return _record(0x0203, struct.pack("<HHH", row, col, 0) + struct.pack("<d", value))


def _minimal_biff8_gscpi_stream() -> bytes:
    strings = ["Date", "GSCPI", "31-Jan-2026", "28-Feb-2026"]
    sst = _record(
        0x00FC,
        struct.pack("<II", len(strings), len(strings))
        + b"".join(_biff_string(value) for value in strings),
    )
    workbook_bof = _record(0x0809, b"\x00" * 16)
    workbook_eof = _record(0x000A)

    sheet = b"".join([
        _record(0x0809, b"\x00" * 16),
        _labelsst(0, 0, 0),
        _labelsst(0, 1, 1),
        _labelsst(1, 0, 2),
        _number(1, 1, 0.4398),
        _labelsst(2, 0, 3),
        _number(2, 1, 0.5433),
        _record(0x000A),
    ])
    dummy_bound = _boundsheet(0, "GSCPI Monthly Data")
    sheet_offset = len(workbook_bof) + len(dummy_bound) + len(sst) + len(workbook_eof)
    return b"".join([
        workbook_bof,
        _boundsheet(sheet_offset, "GSCPI Monthly Data"),
        sst,
        workbook_eof,
        sheet,
    ])


def _minimal_ooxml_gscpi_workbook() -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr(
            "xl/workbook.xml",
            """
            <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
              xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
              <sheets>
                <sheet name="GSCPI Monthly Data" sheetId="1" r:id="rId1"/>
              </sheets>
            </workbook>
            """,
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            """
            <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
              <Relationship Id="rId1" Type="worksheet" Target="worksheets/sheet1.xml"/>
            </Relationships>
            """,
        )
        zf.writestr(
            "xl/sharedStrings.xml",
            """
            <sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
              <si><t>Date</t></si>
              <si><t>GSCPI</t></si>
              <si><t>31-Jan-2026</t></si>
              <si><t>28-Feb-2026</t></si>
            </sst>
            """,
        )
        zf.writestr(
            "xl/worksheets/sheet1.xml",
            """
            <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
              <sheetData>
                <row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>
                <row r="2"><c r="A2" t="s"><v>2</v></c><c r="B2"><v>0.4398</v></c></row>
                <row r="3"><c r="A3" t="s"><v>3</v></c><c r="B3"><v>0.5433</v></c></row>
              </sheetData>
            </worksheet>
            """,
        )
    return out.getvalue()


def _empty_ooxml_gscpi_workbook() -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr(
            "xl/workbook.xml",
            """
            <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
              xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
              <sheets>
                <sheet name="GSCPI Monthly Data" sheetId="1" r:id="rId1"/>
              </sheets>
            </workbook>
            """,
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            """
            <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
              <Relationship Id="rId1" Type="worksheet" Target="worksheets/sheet1.xml"/>
            </Relationships>
            """,
        )
        zf.writestr(
            "xl/worksheets/sheet1.xml",
            """
            <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
              <sheetData>
                <row r="1"><c r="A1" t="inlineStr"><is><t>Date</t></is></c></row>
              </sheetData>
            </worksheet>
            """,
        )
    return out.getvalue()


def test_gscpi_biff8_stream_parser_reads_monthly_data() -> None:
    rows = _parse_gscpi_biff8_stream(_minimal_biff8_gscpi_stream())

    assert rows == [
        NYFedGSCPI(date="2026-01-31", value=0.4398),
        NYFedGSCPI(date="2026-02-28", value=0.5433),
    ]


def test_gscpi_ooxml_parser_reads_monthly_data() -> None:
    rows = parse_gscpi_workbook(_minimal_ooxml_gscpi_workbook())

    assert rows == [
        NYFedGSCPI(date="2026-01-31", value=0.4398),
        NYFedGSCPI(date="2026-02-28", value=0.5433),
    ]


def test_gscpi_parser_rejects_invalid_download_body() -> None:
    with pytest.raises(ValueError, match="contained no observations"):
        parse_gscpi_workbook(b"<html>temporarily unavailable</html>")


def test_gscpi_parser_rejects_empty_workbook() -> None:
    with pytest.raises(ValueError, match="contained no observations"):
        parse_gscpi_workbook(_empty_ooxml_gscpi_workbook())


def test_fetch_gscpi_uses_official_workbook_url() -> None:
    client = NYFedRatesClient()
    fake_session = _FakeSession(_minimal_ooxml_gscpi_workbook())
    client.session = fake_session

    rows = client.fetch_gscpi(last_n=1)

    assert rows == [NYFedGSCPI(date="2026-02-28", value=0.5433)]
    assert fake_session.calls == [(GSCPI_DATA_URL, 30)]


def test_nyfed_fetcher_surfaces_gscpi_series() -> None:
    fetcher = NYFedFetcher(client=_FakeNYFedClient())

    results = fetcher.fetch()
    gscpi = next(result for result in results if result.series_id == "NYFED_GSCPI")

    assert len(results) == 4
    assert gscpi.source == "nyfed"
    assert gscpi.series_metadata == {"category": "supply_chain", "type": "gscpi"}
    assert gscpi.observations[0].date == "2026-03-31"
    assert gscpi.observations[0].value == 0.6769936395789531


def test_nyfed_gscpi_seed_families_concept_schedule_and_discovery(
    tmp_path: Path,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    store.seed_obs_sources_and_families()
    store.seed_concept_map()
    store.seed_release_schedules()

    family = store.get_obs_family("us.supply_chain.gscpi")
    assert family is not None
    assert family.source_id == "nyfed"
    assert family.provider_series_id == "NYFED_GSCPI"
    assert family.unit == "index"
    assert family.frequency == "monthly"

    mappings = store.get_concept_series("GSCPI_US")
    assert len(mappings) == 1
    assert mappings[0].source_id == "nyfed"
    assert mappings[0].provider_series_id == "NYFED_GSCPI"
    assert mappings[0].obs_family_id == "us.supply_chain.gscpi"

    schedule = store.get_release_schedule("GSCPI_US")
    assert schedule is not None
    assert schedule.rule_type == "business_day_of_month"
    assert schedule.rule_json == {
        "calendar": "us_federal",
        "ordinal": 4,
        "time": "10:00",
        "timezone": "America/New_York",
    }
    assert schedule.frequency == "monthly"

    manager = SourceCapabilityManager(store)
    entities = manager.list_entities("nyfed_rates", query="GSCPI", limit=10)["entities"]
    assert [entity["entity_id"] for entity in entities] == ["GSCPI"]
    assert entities[0]["metadata"]["series_id"] == "NYFED_GSCPI"
