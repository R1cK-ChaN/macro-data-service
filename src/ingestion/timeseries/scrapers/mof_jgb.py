"""Japan Ministry of Finance JGB interest-rate CSV client."""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime

import requests


MOF_JGB_INTEREST_RATE_URL = (
    "https://www.mof.go.jp/english/policy/jgbs/reference/"
    "interest_rate/historical/jgbcme_all.csv"
)


@dataclass(frozen=True)
class MOFJGBObservation:
    """A single constant-maturity JGB yield observation."""

    date: str
    maturity: str
    value: float


class MOFJGBClient:
    """Client for MOF's official JGB constant-maturity interest-rate CSV."""

    def __init__(self, *, timeout: int = 30) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "text/csv,*/*",
            "User-Agent": "AnalystEngine/1.0",
        })

    def get_all_series_with_raw(
        self,
        maturities: list[str] | tuple[str, ...],
        *,
        limit: int = 30,
    ) -> dict[str, tuple[list[MOFJGBObservation], dict, dict[str, str]]]:
        response = self.session.get(MOF_JGB_INTEREST_RATE_URL, timeout=self.timeout)
        response.raise_for_status()
        text = response.content.decode("utf-8-sig")
        parsed = parse_jgb_interest_rate_csv(text)
        result: dict[str, tuple[list[MOFJGBObservation], dict, dict[str, str]]] = {}
        for maturity in maturities:
            observations = parsed.get(maturity, [])
            if limit:
                observations = observations[:limit]
            payload = _series_payload(maturity, observations)
            params = {
                "url": MOF_JGB_INTEREST_RATE_URL,
                "maturity": maturity,
                "lastNObservations": str(limit),
            }
            result[maturity] = (observations, payload, params)
        return result

    def get_series_with_raw(
        self,
        maturity: str,
        *,
        limit: int = 30,
    ) -> tuple[list[MOFJGBObservation], dict, dict[str, str]]:
        return self.get_all_series_with_raw((maturity,), limit=limit)[maturity]


def parse_jgb_interest_rate_csv(text: str) -> dict[str, list[MOFJGBObservation]]:
    """Parse MOF's JGB constant-maturity CSV into maturity-keyed observations."""
    reader = csv.reader(io.StringIO(text))
    header: list[str] | None = None
    rows: list[list[str]] = []
    for raw in reader:
        if not raw:
            continue
        cells = [cell.strip() for cell in raw]
        if cells and cells[0].lower() == "date":
            header = cells
            continue
        if header is not None:
            rows.append(cells)
    if header is None:
        raise ValueError("MOF JGB CSV missing Date header")

    by_maturity: dict[str, list[MOFJGBObservation]] = {
        maturity: [] for maturity in header[1:] if maturity
    }
    for row in rows:
        if not row or not row[0].strip():
            continue
        date = _normalize_mof_date(row[0])
        for idx, maturity in enumerate(header[1:], start=1):
            if not maturity or idx >= len(row):
                continue
            raw_value = row[idx].strip()
            if raw_value in {"", "-"}:
                continue
            try:
                value = float(raw_value)
            except ValueError:
                continue
            by_maturity.setdefault(maturity, []).append(
                MOFJGBObservation(date=date, maturity=maturity, value=value)
            )

    for observations in by_maturity.values():
        observations.sort(key=lambda obs: obs.date, reverse=True)
    return by_maturity


def _normalize_mof_date(raw: str) -> str:
    value = raw.strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            pass
    raise ValueError(f"unsupported MOF JGB date: {raw!r}")


def _series_payload(maturity: str, observations: list[MOFJGBObservation]) -> dict:
    return {
        "source_url": MOF_JGB_INTEREST_RATE_URL,
        "maturity": maturity,
        "observations": [
            {"date": obs.date, "value": obs.value}
            for obs in observations
        ],
    }
