"""Bundesbank SDMX Web Service client."""

from __future__ import annotations

from typing import Any

from .._base_client import SDMXClient
from .._config import BUNDESBANK_CONFIG
from .._errors import BundesbankAPIError, BundesbankRateLimitError
from .._types import SDMXObservation


class BundesbankClient(SDMXClient):
    """Client for Bundesbank SDMX data endpoints."""

    def __init__(self, *, timeout: int = 30) -> None:
        super().__init__(
            BUNDESBANK_CONFIG,
            timeout=timeout,
            api_error_cls=BundesbankAPIError,
            rate_limit_error_cls=BundesbankRateLimitError,
        )

    def _setup_session(self, api_key: str | None) -> None:
        _ = api_key
        self.session.headers.update({
            "Accept": "application/vnd.sdmx.data+json;version=1.0.0",
            "User-Agent": "AnalystEngine/1.0",
        })

    def get_data_with_raw(
        self,
        dataflow_id: str,
        key: str = ".",
        *,
        series_id: str = "",
        start_period: str | None = None,
        end_period: str | None = None,
        limit: int = 100,
        **kwargs: Any,
    ) -> tuple[list[SDMXObservation], dict, dict[str, str]]:
        _ = kwargs
        url = self._build_data_url(dataflow_id, key)
        params: dict[str, str] = {}
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
        return observations, payload, record_params
