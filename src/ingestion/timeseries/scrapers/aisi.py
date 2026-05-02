"""American Iron and Steel Institute weekly raw steel production client."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import requests
from bs4 import BeautifulSoup


AISI_INDUSTRY_DATA_URL = "https://www.steel.org/industry-data/"


@dataclass(frozen=True)
class AISIObservation:
    """A single metric from AISI's latest weekly raw steel paragraph."""

    date: str
    metric: str
    value: float


@dataclass(frozen=True)
class AISIWeeklySteelReport:
    week_ending: str
    production_net_tons: float
    capability_utilization_rate: float
    prior_year_week_ending: str
    prior_year_production_net_tons: float
    prior_year_capability_utilization_rate: float
    yoy_percent: float
    previous_week_ending: str
    previous_week_production_net_tons: float
    previous_week_capability_utilization_rate: float
    wow_percent: float


class AISIClient:
    """Client for AISI's public weekly raw steel production page."""

    def __init__(self, *, timeout: int = 30) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "text/html,*/*",
            "User-Agent": "AnalystEngine/1.0",
        })

    def get_all_series_with_raw(
        self,
        metrics: list[str] | tuple[str, ...],
    ) -> dict[str, tuple[list[AISIObservation], dict, dict[str, str]]]:
        response = self.session.get(AISI_INDUSTRY_DATA_URL, timeout=self.timeout)
        response.raise_for_status()
        text = response.text
        report = parse_weekly_raw_steel_page(text)
        result: dict[str, tuple[list[AISIObservation], dict, dict[str, str]]] = {}
        for metric in metrics:
            value = getattr(report, metric)
            observations = [
                AISIObservation(
                    date=report.week_ending,
                    metric=metric,
                    value=float(value),
                )
            ]
            payload = _series_payload(metric, observations, report)
            params = {
                "url": AISI_INDUSTRY_DATA_URL,
                "metric": metric,
                "lastNObservations": "1",
            }
            result[metric] = (observations, payload, params)
        return result

    def get_series_with_raw(
        self,
        metric: str,
    ) -> tuple[list[AISIObservation], dict, dict[str, str]]:
        return self.get_all_series_with_raw((metric,))[metric]


def parse_weekly_raw_steel_page(html: str) -> AISIWeeklySteelReport:
    """Parse AISI's latest weekly raw steel production text."""
    text = _visible_text(html)

    current = _require_match(
        re.compile(
            r"In the week ending on (?P<date>[A-Z][a-z]+ \d{1,2}, \d{4})\s*,\s+"
            r"domestic raw steel production was (?P<production>[\d,]+) net tons "
            r"while the capability utilization rate was (?P<utilization>[-+]?\d+(?:\.\d+)?) percent\.",
            re.IGNORECASE,
        ),
        text,
        "current AISI weekly raw steel production",
    )
    prior_year = _require_match(
        re.compile(
            r"Production was (?P<production>[\d,]+) net tons in the week ending "
            r"(?P<date>[A-Z][a-z]+ \d{1,2}, \d{4}), while the capability utilization "
            r"then was (?P<utilization>[-+]?\d+(?:\.\d+)?) percent\.",
            re.IGNORECASE,
        ),
        text,
        "AISI prior-year weekly raw steel production",
    )
    yoy = _require_match(
        re.compile(
            r"The current week production represents an? (?P<pct>[-+]?\d+(?:\.\d+)?) "
            r"percent (?P<direction>increase|decrease) from the same period in the previous year",
            re.IGNORECASE,
        ),
        text,
        "AISI year-over-year steel production change",
    )
    wow = _require_match(
        re.compile(
            r"Production for the week ending (?P<date>[A-Z][a-z]+ \d{1,2}, \d{4}) "
            r"is (?P<direction>up|down|increased|decreased) (?P<pct>[-+]?\d+(?:\.\d+)?) "
            r"percent from the previous week ending (?P<previous_date>[A-Z][a-z]+ \d{1,2}, \d{4}) "
            r"when production was (?P<previous_production>[\d,]+) net tons and the rate "
            r"of capability utilization was (?P<previous_utilization>[-+]?\d+(?:\.\d+)?) percent\.",
            re.IGNORECASE,
        ),
        text,
        "AISI week-over-week steel production change",
    )

    week_ending = _parse_aisi_date(current.group("date"))
    if _parse_aisi_date(wow.group("date")) != week_ending:
        raise ValueError("AISI weekly raw steel production dates disagree")

    return AISIWeeklySteelReport(
        week_ending=week_ending,
        production_net_tons=float(_parse_int(current.group("production"))),
        capability_utilization_rate=float(current.group("utilization")),
        prior_year_week_ending=_parse_aisi_date(prior_year.group("date")),
        prior_year_production_net_tons=float(_parse_int(prior_year.group("production"))),
        prior_year_capability_utilization_rate=float(prior_year.group("utilization")),
        yoy_percent=_signed_percent(yoy.group("pct"), yoy.group("direction")),
        previous_week_ending=_parse_aisi_date(wow.group("previous_date")),
        previous_week_production_net_tons=float(_parse_int(wow.group("previous_production"))),
        previous_week_capability_utilization_rate=float(wow.group("previous_utilization")),
        wow_percent=_signed_percent(wow.group("pct"), wow.group("direction")),
    )


def _visible_text(html: str) -> str:
    if "<" in html and ">" in html:
        text = BeautifulSoup(html, "html.parser").get_text(" ")
    else:
        text = html
    return " ".join(text.replace("\xa0", " ").split())


def _require_match(pattern: re.Pattern[str], text: str, label: str) -> re.Match[str]:
    match = pattern.search(text)
    if match is None:
        raise ValueError(f"unable to parse {label}")
    return match


def _parse_aisi_date(raw: str) -> str:
    return datetime.strptime(raw.strip(), "%B %d, %Y").date().isoformat()


def _parse_int(raw: str) -> int:
    return int(raw.replace(",", ""))


def _signed_percent(raw: str, direction: str) -> float:
    value = float(raw)
    return value if direction.lower() in {"increase", "increased", "up"} else -value


def _series_payload(
    metric: str,
    observations: list[AISIObservation],
    report: AISIWeeklySteelReport,
) -> dict[str, Any]:
    return {
        "source_url": AISI_INDUSTRY_DATA_URL,
        "metric": metric,
        "week_ending": report.week_ending,
        "observations": [
            {"date": obs.date, "value": obs.value}
            for obs in observations
        ],
        "report": asdict(report),
    }
