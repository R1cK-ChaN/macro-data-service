"""ISM PMI report scraper.

Parses the official Manufacturing and Services PMI report pages into the
RawSeries boundary used by the economic-data pipeline.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ingestion.calendar.ism_api.indicators import ISM_REPORTS_URL

ISM_SOURCE_NAME = "Institute for Supply Management"


@dataclass(frozen=True)
class ISMObservation:
    date: str
    survey: str
    metric: str
    measure: str
    value: float


@dataclass(frozen=True)
class ISMReportMetric:
    metric: str
    label: str
    index_value: float
    previous_value: float | None
    change_value: float | None


@dataclass(frozen=True)
class ISMReport:
    survey: str
    reference_date: str
    reference_label: str
    report_title: str
    source_url: str
    metrics: dict[str, ISMReportMetric]


class ISMReportParseError(ValueError):
    """Raised when an ISM report page does not contain the expected table."""


_HTTP_HEADERS = {
    "User-Agent": "curl/8.5.0",
    "Accept": "*/*",
}

_MONTH_NAMES: dict[str, int] = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

_TITLE_RE = re.compile(
    r"\b(?P<month>january|february|march|april|may|june|july|august|"
    r"september|october|november|december)\s+(?P<year>\d{4})\b"
    r".*?\b(?P<survey>manufacturing|services)\s+pmi\b.*?\breport\b",
    re.IGNORECASE,
)
_NUMBER_RE = re.compile(r"[-+\u2212]?\d+(?:\.\d+)?")

_REPORT_PATHS = {
    "manufacturing": "/reports/ism-pmi-reports/pmi/",
    "services": "/reports/ism-pmi-reports/services/",
}

_METRIC_LABELS: dict[str, dict[str, str]] = {
    "manufacturing": {
        "manufacturing pmi": "pmi",
        "new orders": "new_orders",
        "production": "production",
        "employment": "employment",
        "supplier deliveries": "supplier_deliveries",
        "inventories": "inventories",
        "customers inventories": "customers_inventories",
        "prices": "prices",
        "backlog of orders": "backlog_of_orders",
        "new export orders": "new_export_orders",
        "imports": "imports",
    },
    "services": {
        "services pmi": "pmi",
        "business activity production": "business_activity",
        "business activity": "business_activity",
        "new orders": "new_orders",
        "employment": "employment",
        "supplier deliveries": "supplier_deliveries",
        "inventories": "inventories",
        "prices": "prices",
        "backlog of orders": "backlog_of_orders",
        "new export orders": "new_export_orders",
        "imports": "imports",
        "inventory sentiment": "inventory_sentiment",
    },
}


def _normalize_text(text: str) -> str:
    return " ".join(text.replace("\xa0", " ").split())


def _label_key(text: str) -> str:
    cleaned = (
        text.replace("\xa0", " ")
        .replace("®", "")
        .replace("™", "")
        .replace("*", "")
        .replace("\u2019", "'")
    )
    cleaned = re.sub(r"[^A-Za-z0-9]+", " ", cleaned).strip().lower()
    return re.sub(r"\s+", " ", cleaned)


def _parse_float(text: str) -> float | None:
    if "N/A" in text.upper():
        return None
    match = _NUMBER_RE.search(text.replace("\u2212", "-"))
    if match is None:
        return None
    return float(match.group(0).replace("+", ""))


def _report_title(soup: BeautifulSoup) -> tuple[str, str, str, str]:
    for heading in soup.find_all(["h1", "h2"]):
        text = _normalize_text(heading.get_text(" ", strip=True))
        match = _TITLE_RE.search(text)
        if match is None:
            continue
        month_name = match.group("month").lower()
        year = int(match.group("year"))
        ref = date(year, _MONTH_NAMES[month_name], 1)
        return (
            match.group("survey").lower(),
            ref.isoformat(),
            f"{month_name.capitalize()} {year}",
            text,
        )
    raise ISMReportParseError("ISM PMI report title with month/year not found")


def _extract_table_metrics(
    soup: BeautifulSoup,
    *,
    survey: str,
) -> dict[str, ISMReportMetric]:
    aliases = _METRIC_LABELS[survey]
    metrics: dict[str, ISMReportMetric] = {}
    for row in soup.find_all("tr"):
        cells = [
            _normalize_text(cell.get_text(" ", strip=True))
            for cell in row.find_all(["th", "td"])
        ]
        if len(cells) < 3:
            continue
        label = _label_key(cells[0])
        metric = aliases.get(label)
        if metric is None:
            continue
        values = [_parse_float(cell) for cell in cells[1:]]
        numbers = [value for value in values if value is not None]
        if len(numbers) < 2:
            continue
        current = numbers[0]
        previous = numbers[1]
        change = numbers[2] if len(numbers) >= 3 else round(current - previous, 1)
        metrics[metric] = ISMReportMetric(
            metric=metric,
            label=cells[0],
            index_value=current,
            previous_value=previous,
            change_value=change,
        )
    return metrics


def parse_ism_report_page(html: str, *, source_url: str) -> ISMReport:
    soup = BeautifulSoup(html, "html.parser")
    survey, reference_date, reference_label, title = _report_title(soup)
    metrics = _extract_table_metrics(soup, survey=survey)
    expected = set(_METRIC_LABELS[survey].values())
    missing = sorted(expected - set(metrics))
    if missing:
        raise ISMReportParseError(
            f"{survey} ISM report table missing metrics: {missing}"
        )
    return ISMReport(
        survey=survey,
        reference_date=reference_date,
        reference_label=reference_label,
        report_title=title,
        source_url=source_url,
        metrics=metrics,
    )


def discover_current_report_urls(
    landing_html: str,
    *,
    surveys: Iterable[str] | None = None,
) -> dict[str, str]:
    wanted = set(surveys or _REPORT_PATHS)
    soup = BeautifulSoup(landing_html, "html.parser")
    urls: dict[str, str] = {}
    for link in soup.find_all("a", href=True):
        href = str(link["href"])
        text = _label_key(link.get_text(" ", strip=True))
        for survey in sorted(wanted):
            fragment = _REPORT_PATHS[survey]
            if survey not in urls and fragment in href and "view report" in text:
                urls[survey] = urljoin(ISM_REPORTS_URL, href)
    for link in soup.find_all("a", href=True):
        href = str(link["href"])
        for survey in sorted(wanted - set(urls)):
            fragment = _REPORT_PATHS[survey]
            if fragment in href:
                urls[survey] = urljoin(ISM_REPORTS_URL, href)
    missing = sorted(wanted - set(urls))
    if missing:
        raise ISMReportParseError(f"current ISM report URLs not found: {missing}")
    return urls


class ISMClient:
    def __init__(self, *, timeout: float = 30.0) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(_HTTP_HEADERS)

    def get_all_series_with_raw(
        self,
        series_config: dict[str, dict[str, Any]],
    ) -> dict[str, tuple[list[ISMObservation], dict, dict[str, str]]]:
        surveys = sorted({str(cfg["survey"]) for cfg in series_config.values()})
        landing_html = self._get_text(ISM_REPORTS_URL)
        urls = discover_current_report_urls(landing_html, surveys=surveys)
        reports = {
            survey: parse_ism_report_page(
                self._get_text(url),
                source_url=url,
            )
            for survey, url in urls.items()
        }

        result: dict[str, tuple[list[ISMObservation], dict, dict[str, str]]] = {}
        for cfg in series_config.values():
            report = reports[str(cfg["survey"])]
            obs, payload, params = self._series_payload(cfg, report)
            result[str(cfg["series_id"])] = (obs, payload, params)
        return result

    def get_series_with_raw(
        self,
        cfg: dict[str, Any],
    ) -> tuple[list[ISMObservation], dict, dict[str, str]]:
        return self.get_all_series_with_raw({"series": cfg})[str(cfg["series_id"])]

    def _get_text(self, url: str) -> str:
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return response.text

    def _series_payload(
        self,
        cfg: dict[str, Any],
        report: ISMReport,
    ) -> tuple[list[ISMObservation], dict, dict[str, str]]:
        metric = str(cfg["metric"])
        measure = str(cfg["measure"])
        row = report.metrics[metric]
        value = row.index_value if measure == "index" else row.change_value
        observations: list[ISMObservation] = []
        if value is not None:
            observations.append(
                ISMObservation(
                    date=report.reference_date,
                    survey=report.survey,
                    metric=metric,
                    measure=measure,
                    value=value,
                )
            )
        payload = {
            "source_url": report.source_url,
            "survey": report.survey,
            "metric": metric,
            "measure": measure,
            "reference_date": report.reference_date,
            "reference_label": report.reference_label,
            "observations": [
                {"date": obs.date, "value": obs.value}
                for obs in observations
            ],
            "report": {
                "survey": report.survey,
                "reference_date": report.reference_date,
                "reference_label": report.reference_label,
                "report_title": report.report_title,
                "metrics": {
                    name: asdict(metric_row)
                    for name, metric_row in sorted(report.metrics.items())
                },
            },
        }
        params = {
            "url": report.source_url,
            "survey": report.survey,
            "metric": metric,
            "measure": measure,
            "lastNObservations": "1",
        }
        return observations, payload, params
