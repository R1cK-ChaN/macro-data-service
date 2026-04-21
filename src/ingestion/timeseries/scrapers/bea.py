"""BEA (Bureau of Economic Analysis) API client — GDP, PCE, income, trade data."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field

import requests

from env import get_env_value

logger = logging.getLogger(__name__)


class BEAAPIError(RuntimeError):
    """Base error for BEA API failures."""


class BEARateLimitError(BEAAPIError):
    """Raised when BEA throttles a request (HTTP 429)."""


class BEAResponseError(BEAAPIError):
    """Raised when BEA returns an error inside the response body."""


@dataclass(frozen=True)
class BEAObservation:
    """A single observation from the BEA API."""

    series_id: str
    date: str            # YYYY-MM-DD (normalized)
    value: float | None
    table_name: str
    line_number: str
    line_description: str
    raw: dict = field(default_factory=dict)
    """Original upstream row dict (``TimePeriod`` / ``DataValue`` /
    ``LineNumber`` / ``LineDescription`` / ``NoteRef`` / …). Calendar-lane
    callers hash on this so note-only revisions land as new raw audit
    rows. Populated by :meth:`BEAClient.get_data`; empty-dict default
    keeps fixtures and external callers that construct
    ``BEAObservation`` manually unaffected."""


@dataclass(frozen=True)
class BEADataset:
    """A BEA dataset from GetDatasetList."""

    dataset_name: str
    description: str


@dataclass(frozen=True)
class BEAParameter:
    """A parameter definition for a BEA dataset."""

    parameter_name: str
    parameter_data_type: str
    parameter_description: str
    parameter_is_required: str


@dataclass(frozen=True)
class BEAParameterValue:
    """An allowed value for a BEA dataset parameter."""

    key: str
    description: str


_MIN_REQUEST_INTERVAL = 0.6  # ~100 requests/min


class BEAClient:
    """Client for the BEA REST API."""

    BASE_URL = "https://apps.bea.gov/api/data"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or get_env_value("BEA_API_KEY")
        self.session = requests.Session()
        self._last_request_time = 0.0

    # -- Public methods ------------------------------------------------------

    def list_datasets(self) -> list[BEADataset]:
        """Return all available BEA datasets."""
        if not self.api_key:
            return []
        data = self._get("GetDatasetList")
        raw = data.get("Dataset", [])
        if isinstance(raw, dict):
            raw = [raw]
        return [
            BEADataset(
                dataset_name=d.get("DatasetName", ""),
                description=d.get("DatasetDescription", ""),
            )
            for d in raw
        ]

    def get_parameters(self, dataset_name: str) -> list[BEAParameter]:
        """Return parameter definitions for a dataset."""
        if not self.api_key:
            return []
        data = self._get("GetParameterList", DatasetName=dataset_name)
        raw = data.get("Parameter", [])
        if isinstance(raw, dict):
            raw = [raw]
        return [
            BEAParameter(
                parameter_name=p.get("ParameterName", ""),
                parameter_data_type=p.get("ParameterDataType", ""),
                parameter_description=p.get("ParameterDescription", ""),
                parameter_is_required=p.get("ParameterIsRequiredFlag", "0"),
            )
            for p in raw
        ]

    def get_parameter_values(
        self, dataset_name: str, parameter_name: str,
    ) -> list[BEAParameterValue]:
        """Return allowed values for a dataset parameter."""
        if not self.api_key:
            return []
        data = self._get(
            "GetParameterValues",
            DatasetName=dataset_name,
            ParameterName=parameter_name,
        )
        raw = data.get("ParamValue", [])
        if isinstance(raw, dict):
            raw = [raw]
        return [
            BEAParameterValue(
                key=self._extract_param_key(v),
                description=v.get("Description", v.get("Desc", "")),
            )
            for v in raw
        ]

    @staticmethod
    def _extract_param_key(row: dict) -> str:
        """Extract the key value from a ParamValue row.

        BEA uses different field names per parameter type:
        TableName→"TableName", Frequency→"FrequencyID", etc.
        Fall back to "Key" or first non-Description field.
        """
        if "Key" in row:
            return str(row["Key"])
        if "key" in row:
            return str(row["key"])
        # Try common BEA field names
        for field in ("TableName", "FrequencyID", "TableID", "IndustryID",
                      "GeoFips", "Indicator", "TypeOfInvestment", "Component"):
            if field in row:
                return str(row[field])
        # Fall back to first non-Description field
        for k, v in row.items():
            if k != "Description":
                return str(v)
        return ""

    def get_data(self, dataset_name: str, **params: str) -> list[BEAObservation]:
        """Fetch data from any BEA dataset.

        Parameters are dataset-specific (e.g. TableName, Frequency, Year).
        """
        if not self.api_key:
            return []
        data = self._get("GetData", DatasetName=dataset_name, **params)
        raw = data.get("Data", [])
        if isinstance(raw, dict):
            raw = [raw]

        observations: list[BEAObservation] = []
        table_name = params.get("TableName", params.get("tablename", ""))
        for row in raw:
            raw_value = row.get("DataValue", "")
            parsed = self._parse_data_value(raw_value)
            period = row.get("TimePeriod", "")
            observations.append(BEAObservation(
                series_id=f"BEA_{dataset_name}_{table_name}_{row.get('LineNumber', '')}",
                date=self._normalize_time_period(period),
                value=parsed,
                table_name=table_name or row.get("TableName", ""),
                line_number=str(row.get("LineNumber", "")),
                line_description=row.get("LineDescription", row.get("SeriesDescription", "")),
                raw=dict(row),
            ))
        return observations

    def get_nipa_table(
        self,
        table_name: str,
        *,
        frequency: str = "Q",
        year: str = "2020,2021,2022,2023,2024,2025",
    ) -> list[BEAObservation]:
        """Convenience method for NIPA table data.

        Year accepts: "ALL", single year "2024", or comma-separated "2020,2021,2022".
        """
        return self.get_data(
            "NIPA",
            TableName=table_name,
            Frequency=frequency,
            Year=year,
        )

    def count_nipa_tables(self) -> int:
        """Return the number of available NIPA tables."""
        values = self.get_parameter_values("NIPA", "TableName")
        return len(values)

    # -- Internal helpers ----------------------------------------------------

    def _get(self, method: str, **params: str) -> dict:
        """Execute a BEA API GET request with throttling and error handling."""
        self._throttle()
        query: dict[str, str] = {
            "UserID": self.api_key or "",
            "method": method,
            "ResultFormat": "JSON",
        }
        query.update(params)

        try:
            resp = self.session.get(self.BASE_URL, params=query, timeout=30)
        except requests.RequestException as exc:
            raise BEAAPIError(f"BEA request failed: {exc}") from exc

        if resp.status_code == 429:
            raise BEARateLimitError(f"BEA rate limit (HTTP 429)")

        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            raise BEAAPIError(f"BEA HTTP {resp.status_code}: {exc}") from exc

        try:
            body = resp.json()
        except ValueError as exc:
            raise BEAAPIError(f"BEA returned non-JSON response") from exc

        beaapi = body.get("BEAAPI", {})

        # BEA can signal errors at the top level or inside Results
        top_error = beaapi.get("Error")
        if top_error:
            if isinstance(top_error, list):
                top_error = top_error[0]
            code = top_error.get("APIErrorCode", "")
            desc = top_error.get("APIErrorDescription", "")
            raise BEAResponseError(f"BEA error {code}: {desc}")

        results = beaapi.get("Results", {})

        # Some datasets (GDPbyIndustry) return Results as a list
        if isinstance(results, list):
            results = results[0] if results else {}

        # BEA signals errors inside the JSON envelope
        error = results.get("Error")
        if error:
            if isinstance(error, list):
                error = error[0]
            code = error.get("APIErrorCode", "")
            desc = error.get("APIErrorDescription", "")
            raise BEAResponseError(f"BEA error {code}: {desc}")

        return results

    def _throttle(self) -> None:
        """Enforce minimum interval between requests."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    @staticmethod
    def _parse_data_value(raw: str) -> float | None:
        """Parse a BEA DataValue string into a float or None.

        Handles comma-formatted numbers ("21,542.5"), suppressed values
        ("(NA)", "(D)", "---"), and negatives ("-1,234.5").
        """
        if not raw:
            return None
        cleaned = raw.strip()
        if cleaned in ("(NA)", "(D)", "---", "N/A", "..."):
            return None
        # Strip commas
        cleaned = cleaned.replace(",", "")
        try:
            return float(cleaned)
        except ValueError:
            return None

    @staticmethod
    def _normalize_time_period(period: str) -> str:
        """Convert BEA time period to YYYY-MM-DD.

        "2024Q1" → "2024-03-31", "2024Q2" → "2024-06-30",
        "2024Q3" → "2024-09-30", "2024Q4" → "2024-12-31",
        "2024M01" → "2024-01-01", "2024" → "2024-12-31".
        """
        period = period.strip()

        # Quarterly: 2024Q1 → end of quarter
        m = re.match(r"^(\d{4})Q(\d)$", period)
        if m:
            year, q = m.group(1), int(m.group(2))
            quarter_ends = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
            return f"{year}-{quarter_ends.get(q, '12-31')}"

        # Monthly: 2024M01 → first of month
        m = re.match(r"^(\d{4})M(\d{2})$", period)
        if m:
            return f"{m.group(1)}-{m.group(2)}-01"

        # Annual: 2024 → end of year
        m = re.match(r"^(\d{4})$", period)
        if m:
            return f"{m.group(1)}-12-31"

        # Already a date or unknown format — return as-is
        return period
