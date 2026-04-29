"""BIS SDMX provider — CSV format override."""

from __future__ import annotations

import csv
import io

import requests

from .._base_client import SDMXClient
from .._config import BIS_CONFIG
from .._errors import BISAPIError, BISRateLimitError
from .._parsing import normalize_date
from .._types import SDMXObservation


class BISClient(SDMXClient):
    """Client for the BIS SDMX REST API (no API key required).

    Uses CSV format for data responses (unlike JSON for other providers).
    """

    def __init__(self, *, timeout: int = 30) -> None:
        super().__init__(
            BIS_CONFIG, timeout=timeout,
            api_error_cls=BISAPIError,
            rate_limit_error_cls=BISRateLimitError,
        )
        # BIS uses */* accept for CSV data
        self.session.headers["Accept"] = "*/*"

    def _build_data_url(self, dataflow_id, key, **kwargs):
        version = kwargs.get("version", "1.0")
        return f"{self.config.base_url}/data/dataflow/BIS/{dataflow_id}/{version}/{key}"

    def _build_structure_url(self, dataflow_id, version):
        return f"{self.config.base_url}/structure/dataflow/BIS/{dataflow_id}/{version}"

    def _build_dataflow_list_url(self):
        return f"{self.config.base_url}/structure/dataflow/BIS"

    def _build_estimate_url(self, dataflow_id, **kwargs):
        version = kwargs.get("version", "1.0")
        return f"{self.config.base_url}/data/dataflow/BIS/{dataflow_id}/{version}/."

    def get_data(self, dataflow_id, key, *, series_id, start_period=None,
                 end_period=None, limit=100, **kwargs):
        url = self._build_data_url(dataflow_id, key, **kwargs)
        params: dict[str, str] = {
            "detail": "dataonly",
            "format": "csvdata",
        }
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if limit:
            params["lastNObservations"] = str(limit)

        response = self._get(url, params)
        return self._parse_csv(response.text, series_id=series_id, dataflow=dataflow_id, limit=limit)

    def get_data_with_raw(self, dataflow_id, key=".", *, series_id="",
                          start_period=None, end_period=None, limit=100, **kwargs):
        """BIS uses CSV — no SDMX-JSON canonicalization.

        Skipping raw capture here is deliberate: the issue #69 ``obs_raw``
        canonicalizer is JSON-shape-aware. Adding CSV-shape canonicalization
        is a separate slice; until then BIS continues writing typed rows
        only and the audit lane skips it (returns empty payload).
        """
        observations = self.get_data(
            dataflow_id, key,
            series_id=series_id,
            start_period=start_period,
            end_period=end_period,
            limit=limit,
            **kwargs,
        )
        return observations, {}, {}

    @staticmethod
    def _parse_csv(text, *, series_id, dataflow, limit):
        reader = csv.DictReader(io.StringIO(text))
        observations: list[SDMXObservation] = []
        for row in reader:
            try:
                val = row.get("OBS_VALUE")
                if val is None or val == "":
                    continue
                period = row.get("TIME_PERIOD", "")
                if not period:
                    continue
                observations.append(SDMXObservation(
                    series_id=series_id,
                    date=normalize_date(period),
                    value=float(val),
                    dataflow=dataflow,
                ))
            except (ValueError, TypeError):
                continue
        observations.sort(key=lambda o: o.date, reverse=True)
        return observations[:limit] if limit else observations

    def estimate_size(self, dataflow_id, version="1.0", **kwargs):
        """CSV-based size estimation: count rows minus header."""
        total_series = 0
        time_periods = 1
        try:
            url = self._build_estimate_url(dataflow_id, version=version)
            params = {"detail": "dataonly", "format": "csvdata", "lastNObservations": "1"}
            response = self._get(url, params)
            lines = response.text.strip().split("\n")
            total_series = max(len(lines) - 1, 0)
        except (BISAPIError, BISRateLimitError):
            pass

        if total_series == 0:
            try:
                structure = self.get_datastructure(dataflow_id, version)
                total_series = 1
                for d in structure.dimensions:
                    if not d.is_time and d.code_count > 0:
                        total_series *= d.code_count
            except (BISAPIError, BISRateLimitError):
                pass

        from .._types import SDMXSizeEstimate
        return SDMXSizeEstimate(
            dataflow_id=dataflow_id,
            version=version,
            total_series=total_series,
            time_periods=time_periods,
            estimated_observations=total_series * time_periods,
        )
