"""IMF SDMX 3.0 provider — API key auth + vintage queries."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Sequence

from .._base_client import SDMXClient
from .._config import IMF_CONFIG
from .._errors import IMFAPIError, IMFRateLimitError
from .._json_parser import parse_sdmx_json_observations
from .._parsing import normalize_date
from .._types import SDMXObservation


@dataclass(frozen=True)
class IMFVintageObservation:
    """A single vintage (point-in-time) observation from the IMF SDMX 3.0 API."""

    series_id: str
    date: str
    vintage_date: str
    value: float
    dataflow: str = ""


def _normalize_imf_date(raw: str) -> str:
    m = re.match(r"^(\d{4})-M(\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    return normalize_date(raw)


def _parse_imf_vintage_payload(
    payload: dict[str, Any],
    *,
    series_id: str,
    dataflow: str,
    vintage_date: str,
    limit: int,
) -> list[IMFVintageObservation]:
    observations = parse_sdmx_json_observations(
        payload,
        series_id=series_id,
        dataflow=dataflow,
        limit=limit,
        normalize_date_fn=_normalize_imf_date,
    )
    return [
        IMFVintageObservation(
            series_id=obs.series_id,
            date=obs.date,
            vintage_date=vintage_date,
            value=obs.value,
            dataflow=obs.dataflow,
        )
        for obs in observations
    ]


class IMFClient(SDMXClient):
    """Client for the IMF SDMX 3.0 API (requires API key)."""

    def __init__(self, api_key: str | None = None, *, timeout: int = 30) -> None:
        super().__init__(
            IMF_CONFIG, timeout=timeout, api_key=api_key,
            api_error_cls=IMFAPIError,
            rate_limit_error_cls=IMFRateLimitError,
        )

    def _normalize_date(self, raw: str) -> str:
        return _normalize_imf_date(raw)

    def _build_dataflow_list_url(self):
        return f"{self.config.base_url}/structure/dataflow/{self.config.default_agency}"

    def _build_structure_url(self, dataflow_id, version):
        return (
            f"{self.config.base_url}/structure/dataflow/"
            f"{self.config.default_agency}/{dataflow_id}/{version}"
        )

    def _build_data_url(self, dataflow_id, key, **kwargs):
        version = kwargs.get("version")
        if not version:
            flows = self.list_dataflows()
            match = next((f for f in flows if f.id == dataflow_id), None)
            version = match.version if match else "1.0"
        return (
            f"{self.config.base_url}/data/dataflow/"
            f"{self.config.default_agency}/{dataflow_id}/{version}/{key}"
        )

    def _build_estimate_url(self, dataflow_id, **kwargs):
        return self._build_data_url(dataflow_id, "all", **kwargs)

    def get_data(self, dataflow_id, key="all", *, series_id="", version="",
                 start_period=None, end_period=None, limit=100, **kwargs):
        observations, _payload, _params = self.get_data_with_raw(
            dataflow_id, key,
            series_id=series_id, version=version,
            start_period=start_period, end_period=end_period,
            limit=limit, **kwargs,
        )
        return observations

    def get_data_with_raw(self, dataflow_id, key="all", *, series_id="",
                          version="", start_period=None, end_period=None,
                          limit=100, **kwargs):
        """IMF SDMX 3.0 fetch returning parsed obs + raw payload + params."""
        url = self._build_data_url(dataflow_id, key, version=version)
        params: dict[str, str] = {
            "attributes": "none",
            "measures": "all",
            "format": "jsondata",
        }
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if limit:
            params["lastNObservations"] = str(limit)

        response = self._get(url, params)
        observations = self._parse_data_response(
            response, series_id=series_id, dataflow=dataflow_id, limit=limit,
        )
        try:
            payload = self._response_json(
                response, context=f"{dataflow_id}/{series_id or '*'}",
            )
        except ValueError:
            payload = {}
        record_params = dict(params)
        record_params["dataflow_id"] = dataflow_id
        record_params["key"] = key
        if version:
            record_params["version"] = version
        return observations, payload, record_params

    def get_vintages(
        self,
        dataflow_id: str,
        key: str,
        *,
        series_id: str,
        version: str = "",
        as_of_dates: Sequence[str] = (),
        limit: int = 1,
    ) -> list[IMFVintageObservation]:
        """Fetch point-in-time vintage data using the SDMX 3.0 ``asOf`` parameter."""
        vintages, _payload, _params = self.get_vintages_with_raw(
            dataflow_id,
            key,
            series_id=series_id,
            version=version,
            as_of_dates=as_of_dates,
            limit=limit,
        )
        return vintages

    def get_vintages_with_raw(
        self,
        dataflow_id: str,
        key: str,
        *,
        series_id: str,
        version: str = "",
        as_of_dates: Sequence[str] = (),
        limit: int = 1,
    ) -> tuple[list[IMFVintageObservation], dict[str, Any], dict[str, Any]]:
        """Fetch IMF point-in-time vintages plus raw SDMX responses."""
        if not version:
            flows = self.list_dataflows()
            match = next((f for f in flows if f.id == dataflow_id), None)
            version = match.version if match else "1.0"

        vintages: list[IMFVintageObservation] = []
        responses: list[dict[str, Any]] = []
        for as_of in as_of_dates:
            url = (
                f"{self.config.base_url}/data/dataflow/"
                f"{self.config.default_agency}/{dataflow_id}/{version}/{key}"
            )
            params: dict[str, str] = {
                "attributes": "none",
                "measures": "all",
                "format": "jsondata",
                "asOf": as_of,
            }
            if limit:
                params["lastNObservations"] = str(limit)

            try:
                response = self._get(url, params)
                payload = self._response_json(
                    response, context=f"{dataflow_id}/{series_id}",
                )
                vintages.extend(_parse_imf_vintage_payload(
                    payload,
                    series_id=series_id,
                    dataflow=dataflow_id,
                    vintage_date=as_of,
                    limit=limit,
                ))
                responses.append({
                    "asOf": as_of,
                    "params": dict(params),
                    "payload": payload,
                })
            except (IMFAPIError, IMFRateLimitError):
                continue

        raw_payload = {
            "dataflow_id": dataflow_id,
            "key": key,
            "series_id": series_id,
            "version": version,
            "responses": responses,
        }
        record_params: dict[str, Any] = {
            "dataflow_id": dataflow_id,
            "key": key,
            "series_id": series_id,
            "version": version,
            "asOfDates": list(as_of_dates),
            "limit": limit,
        }
        return vintages, raw_payload, record_params
