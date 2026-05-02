"""Client for NY Fed public time-series data."""

from __future__ import annotations

import io
import logging
import posixpath
import re
import struct
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from xml.etree import ElementTree as ET

import requests

logger = logging.getLogger(__name__)

GSCPI_DATA_URL = (
    "https://www.newyorkfed.org/medialibrary/research/interactives/"
    "gscpi/downloads/gscpi_data.xlsx"
)


@dataclass(frozen=True)
class NYFedRate:
    """A single rate observation from the NY Fed Markets API."""

    date: str
    type: str  # SOFR, EFFR, or OBFR
    rate: float
    percentile_1: float | None = None
    percentile_25: float | None = None
    percentile_75: float | None = None
    percentile_99: float | None = None
    volume_billions: float | None = None
    target_rate_from: float | None = None
    target_rate_to: float | None = None


@dataclass(frozen=True)
class NYFedGSCPI:
    """A single Global Supply Chain Pressure Index observation."""

    date: str
    value: float


class NYFedRatesClient:
    """Fetches NY Fed reference rates and research-product series."""

    BASE_URL = "https://markets.newyorkfed.org/api/rates"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "AnalystEngine/1.0",
        })

    def fetch_sofr(self, last_n: int = 5) -> list[NYFedRate]:
        """Fetch the last N SOFR observations."""
        url = f"{self.BASE_URL}/secured/sofr/last/{last_n}.json"
        return self._fetch_rates(url, "SOFR")

    def fetch_effr(self, last_n: int = 5) -> list[NYFedRate]:
        """Fetch the last N EFFR observations."""
        url = f"{self.BASE_URL}/unsecured/effr/last/{last_n}.json"
        return self._fetch_rates(url, "EFFR")

    def fetch_obfr(self, last_n: int = 5) -> list[NYFedRate]:
        """Fetch the last N OBFR observations."""
        url = f"{self.BASE_URL}/unsecured/obfr/last/{last_n}.json"
        return self._fetch_rates(url, "OBFR")

    def fetch_all_rates(self, last_n: int = 5) -> list[NYFedRate]:
        """Fetch SOFR, EFFR, and OBFR with a short delay between requests."""
        all_rates: list[NYFedRate] = []
        all_rates.extend(self.fetch_sofr(last_n))
        time.sleep(0.5)
        all_rates.extend(self.fetch_effr(last_n))
        time.sleep(0.5)
        all_rates.extend(self.fetch_obfr(last_n))
        return all_rates

    def fetch_gscpi(self, last_n: int | None = 30) -> list[NYFedGSCPI]:
        """Fetch Global Supply Chain Pressure Index observations."""
        response = self.session.get(GSCPI_DATA_URL, timeout=30)
        response.raise_for_status()
        rows = parse_gscpi_workbook(response.content)
        if last_n is None:
            return rows
        if last_n <= 0:
            return []
        return rows[-last_n:]

    def _fetch_rates(self, url: str, rate_type: str) -> list[NYFedRate]:
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        return self._parse_rates(data, rate_type)

    def _parse_rates(self, data: dict, rate_type: str) -> list[NYFedRate]:
        rates: list[NYFedRate] = []
        for obs in data.get("refRates", []):
            try:
                volume_raw = obs.get("volumeInBillions")
                volume = float(volume_raw) if volume_raw is not None else None

                rates.append(NYFedRate(
                    date=obs.get("effectiveDate", ""),
                    type=rate_type,
                    rate=float(obs.get("percentRate", 0)),
                    percentile_1=_float_or_none(obs.get("percentPercentile1")),
                    percentile_25=_float_or_none(obs.get("percentPercentile25")),
                    percentile_75=_float_or_none(obs.get("percentPercentile75")),
                    percentile_99=_float_or_none(obs.get("percentPercentile99")),
                    volume_billions=volume,
                    target_rate_from=_float_or_none(obs.get("targetRateFrom")),
                    target_rate_to=_float_or_none(obs.get("targetRateTo")),
                ))
            except (ValueError, TypeError):
                continue
        return rates


def parse_gscpi_workbook(payload: bytes) -> list[NYFedGSCPI]:
    """Parse the NY Fed GSCPI workbook payload into sorted observations."""
    parse_error: Exception | None = None
    if zipfile.is_zipfile(io.BytesIO(payload)):
        try:
            rows = _parse_gscpi_ooxml(payload)
            if rows:
                return rows
        except Exception as exc:
            parse_error = exc
    try:
        rows = _parse_gscpi_biff8(payload)
    except Exception as exc:
        raise ValueError("Unable to parse NY Fed GSCPI workbook") from exc
    if rows:
        return rows
    if parse_error is not None:
        raise ValueError("Unable to parse NY Fed GSCPI workbook") from parse_error
    raise ValueError("NY Fed GSCPI workbook contained no observations")


def _parse_gscpi_ooxml(payload: bytes) -> list[NYFedGSCPI]:
    with zipfile.ZipFile(io.BytesIO(payload)) as workbook:
        shared_strings = _ooxml_shared_strings(workbook)
        for sheet_path in _ooxml_sheet_paths(workbook, preferred="GSCPI Monthly Data"):
            cells = _ooxml_sheet_cells(workbook, sheet_path, shared_strings)
            rows = _gscpi_rows_from_cells(cells)
            if rows:
                return rows
    return []


def _ooxml_shared_strings(workbook: zipfile.ZipFile) -> list[str]:
    try:
        root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    strings: list[str] = []
    for item in root.findall("x:si", ns):
        strings.append("".join(t.text or "" for t in item.findall(".//x:t", ns)))
    return strings


def _ooxml_sheet_paths(
    workbook: zipfile.ZipFile, *, preferred: str,
) -> list[str]:
    try:
        wb_root = ET.fromstring(workbook.read("xl/workbook.xml"))
        rel_root = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
    except KeyError:
        return sorted(
            path for path in workbook.namelist()
            if path.startswith("xl/worksheets/sheet") and path.endswith(".xml")
        )

    rels = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rel_root
        if "Id" in rel.attrib and "Target" in rel.attrib
    }
    rel_id_key = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
    sheets: list[tuple[bool, str]] = []
    for sheet in wb_root.findall(
        ".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}sheet"
    ):
        rel_id = sheet.attrib.get(rel_id_key, "")
        target = rels.get(rel_id, "")
        if not target:
            continue
        path = target.lstrip("/") if target.startswith("/") else posixpath.join("xl", target)
        path = posixpath.normpath(path)
        sheets.append((sheet.attrib.get("name") == preferred, path))
    return [path for _, path in sorted(sheets, reverse=True)]


def _ooxml_sheet_cells(
    workbook: zipfile.ZipFile,
    sheet_path: str,
    shared_strings: list[str],
) -> dict[tuple[int, int], object]:
    root = ET.fromstring(workbook.read(sheet_path))
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    cells: dict[tuple[int, int], object] = {}
    for cell in root.findall(".//x:c", ns):
        ref = cell.attrib.get("r", "")
        row_col = _cell_ref_to_row_col(ref)
        if row_col is None:
            continue
        value = _ooxml_cell_value(cell, shared_strings, ns)
        if value is not None:
            cells[row_col] = value
    return cells


def _ooxml_cell_value(
    cell: ET.Element,
    shared_strings: list[str],
    ns: dict[str, str],
) -> object | None:
    cell_type = cell.attrib.get("t", "")
    if cell_type == "inlineStr":
        return "".join(t.text or "" for t in cell.findall(".//x:t", ns))

    value = cell.find("x:v", ns)
    if value is None or value.text is None:
        return None
    text = value.text.strip()
    if cell_type == "s":
        idx = int(text)
        return shared_strings[idx] if 0 <= idx < len(shared_strings) else ""
    if cell_type == "str":
        return text
    try:
        return float(text)
    except ValueError:
        return text


def _parse_gscpi_biff8(payload: bytes) -> list[NYFedGSCPI]:
    import olefile

    stream = io.BytesIO(payload)
    if not olefile.isOleFile(stream):
        return []
    stream.seek(0)
    ole = olefile.OleFileIO(stream)
    workbook_stream = "Workbook" if ole.exists("Workbook") else "Book"
    data = ole.openstream(workbook_stream).read()
    return _parse_gscpi_biff8_stream(data)


def _parse_gscpi_biff8_stream(data: bytes) -> list[NYFedGSCPI]:
    sheets: list[tuple[str, int]] = []
    sst_parts: list[bytes] = []
    collecting_sst = False

    for _offset, record_type, payload in _iter_biff_records(data):
        if record_type == 0x0085:
            sheets.append(_parse_bound_sheet(payload))
        if record_type == 0x00FC:
            collecting_sst = True
            sst_parts.append(payload)
        elif collecting_sst and record_type == 0x003C:
            sst_parts.append(payload)
        elif collecting_sst:
            collecting_sst = False

    strings = _parse_biff_sst(b"".join(sst_parts)) if sst_parts else []
    preferred = next(
        (offset for name, offset in sheets if name == "GSCPI Monthly Data"),
        sheets[0][1] if sheets else 0,
    )
    cells = _parse_biff_sheet_cells(data, preferred, strings)
    return _gscpi_rows_from_cells(cells)


def _iter_biff_records(
    data: bytes, start: int = 0,
) -> list[tuple[int, int, bytes]]:
    records: list[tuple[int, int, bytes]] = []
    pos = start
    while pos + 4 <= len(data):
        record_type, length = struct.unpack_from("<HH", data, pos)
        pos += 4
        payload = data[pos:pos + length]
        records.append((pos - 4, record_type, payload))
        pos += length
        if record_type == 0x000A:
            break
    return records


def _parse_bound_sheet(payload: bytes) -> tuple[str, int]:
    offset = struct.unpack_from("<I", payload, 0)[0]
    char_count = payload[6]
    flags = payload[7]
    width = 2 if flags & 0x01 else 1
    raw = payload[8:8 + char_count * width]
    name = raw.decode("utf-16le" if width == 2 else "latin1")
    return name, offset


def _parse_biff_sst(payload: bytes) -> list[str]:
    if len(payload) < 8:
        return []
    unique_count = struct.unpack_from("<I", payload, 4)[0]
    pos = 8
    strings: list[str] = []
    for _ in range(unique_count):
        text, pos = _read_biff_string(payload, pos)
        strings.append(text)
    return strings


def _read_biff_string(payload: bytes, pos: int) -> tuple[str, int]:
    char_count = struct.unpack_from("<H", payload, pos)[0]
    pos += 2
    flags = payload[pos]
    pos += 1
    is_wide = bool(flags & 0x01)
    has_rich = bool(flags & 0x08)
    has_ext = bool(flags & 0x04)
    rich_runs = 0
    ext_size = 0
    if has_rich:
        rich_runs = struct.unpack_from("<H", payload, pos)[0]
        pos += 2
    if has_ext:
        ext_size = struct.unpack_from("<I", payload, pos)[0]
        pos += 4
    width = 2 if is_wide else 1
    raw = payload[pos:pos + char_count * width]
    pos += len(raw)
    pos += rich_runs * 4 + ext_size
    return raw.decode("utf-16le" if is_wide else "latin1"), pos


def _parse_biff_sheet_cells(
    data: bytes,
    start: int,
    strings: list[str],
) -> dict[tuple[int, int], object]:
    cells: dict[tuple[int, int], object] = {}
    for _offset, record_type, payload in _iter_biff_records(data, start):
        if record_type == 0x00FD:
            row, col, _xf, string_idx = struct.unpack_from("<HHHI", payload, 0)
            if 0 <= string_idx < len(strings):
                cells[(row, col)] = strings[string_idx]
        elif record_type == 0x0203:
            row, col, _xf = struct.unpack_from("<HHH", payload, 0)
            cells[(row, col)] = struct.unpack_from("<d", payload, 6)[0]
        elif record_type == 0x027E:
            row, col, _xf, rk = struct.unpack_from("<HHHI", payload, 0)
            cells[(row, col)] = _decode_biff_rk(rk)
        elif record_type == 0x00BD:
            row, first_col = struct.unpack_from("<HH", payload, 0)
            last_col = struct.unpack_from("<H", payload, len(payload) - 2)[0]
            pos = 4
            for col in range(first_col, last_col + 1):
                _xf, rk = struct.unpack_from("<HI", payload, pos)
                pos += 6
                cells[(row, col)] = _decode_biff_rk(rk)
        elif record_type == 0x0006:
            row, col, _xf = struct.unpack_from("<HHH", payload, 0)
            cells[(row, col)] = struct.unpack_from("<d", payload, 6)[0]
    return cells


def _decode_biff_rk(raw: int) -> float:
    multiplied = bool(raw & 0x01)
    is_integer = bool(raw & 0x02)
    value_bits = raw & 0xFFFFFFFC
    if is_integer:
        value = value_bits >> 2
        if value & (1 << 29):
            value -= 1 << 30
        result = float(value)
    else:
        result = struct.unpack("<d", struct.pack("<II", 0, value_bits))[0]
    return result / 100 if multiplied else result


def _gscpi_rows_from_cells(
    cells: dict[tuple[int, int], object],
) -> list[NYFedGSCPI]:
    rows: list[NYFedGSCPI] = []
    for row in sorted({row for row, _col in cells}):
        obs_date = _normalize_gscpi_date(cells.get((row, 0)))
        value = _float_or_none(cells.get((row, 1)))
        if obs_date and value is not None:
            rows.append(NYFedGSCPI(date=obs_date, value=value))
    return sorted(rows, key=lambda item: item.date)


def _normalize_gscpi_date(value: object) -> str | None:
    if isinstance(value, (int, float)):
        if 1 <= float(value) <= 100000:
            return (date(1899, 12, 30) + timedelta(days=int(value))).isoformat()
        return None
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text.lower() == "date":
        return None
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except ValueError:
        pass
    for fmt in ("%d-%b-%Y", "%m/%d/%Y", "%b %Y"):
        try:
            parsed = datetime.strptime(text, fmt).date()
            if fmt == "%b %Y":
                next_month = date(parsed.year + (parsed.month == 12), parsed.month % 12 + 1, 1)
                parsed = next_month - timedelta(days=1)
            return parsed.isoformat()
        except ValueError:
            continue
    return None


def _cell_ref_to_row_col(ref: str) -> tuple[int, int] | None:
    match = re.fullmatch(r"([A-Z]+)([0-9]+)", ref)
    if match is None:
        return None
    col = 0
    for char in match.group(1):
        col = col * 26 + (ord(char) - ord("A") + 1)
    return int(match.group(2)) - 1, col - 1


def _float_or_none(val: str | float | None) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
