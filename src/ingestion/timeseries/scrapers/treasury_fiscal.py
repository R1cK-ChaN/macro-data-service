"""Treasury Fiscal Data API client — federal debt, TGA balance, interest rates."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TreasuryFiscalObservation:
    """A single observation from the Treasury Fiscal Data API."""

    series_id: str
    date: str
    value: float
    metadata: dict[str, Any] = field(default_factory=dict)


def _parse_treasury_debt(payload: dict) -> list[TreasuryFiscalObservation]:
    """Parse Treasury ``debt_to_penny`` rows into typed observations.

    Standalone helper so the issue #116 re-projection path can replay a
    stored ``obs_raw`` row through the same parser the live HTTP path
    uses.
    """
    observations: list[TreasuryFiscalObservation] = []
    for row in payload.get("data", []):
        try:
            val = row.get("tot_pub_debt_out_amt")
            if val is None or val == "":
                continue
            observations.append(TreasuryFiscalObservation(
                series_id="TREAS_DEBT_TOTAL",
                date=row.get("record_date", ""),
                value=float(val),
                metadata={
                    "debt_held_public": row.get("debt_held_public_amt", ""),
                    "intragov_holdings": row.get("intragov_hold_amt", ""),
                },
            ))
        except (ValueError, TypeError):
            continue
    return observations


def _parse_treasury_tga(payload: dict) -> list[TreasuryFiscalObservation]:
    """Parse Treasury ``operating_cash_balance`` (TGA) rows."""
    observations: list[TreasuryFiscalObservation] = []
    for row in payload.get("data", []):
        try:
            val = row.get("open_today_bal")
            if val is None or val == "" or val == "null":
                continue
            observations.append(TreasuryFiscalObservation(
                series_id="TREAS_TGA_BALANCE",
                date=row.get("record_date", ""),
                value=float(val),
                metadata={"account_type": row.get("account_type", "")},
            ))
        except (ValueError, TypeError):
            continue
    return observations


def _parse_treasury_avg_rates(payload: dict) -> list[TreasuryFiscalObservation]:
    """Parse Treasury ``avg_interest_rates`` rows."""
    observations: list[TreasuryFiscalObservation] = []
    for row in payload.get("data", []):
        try:
            val = row.get("avg_interest_rate_amt")
            if val is None or val == "":
                continue
            observations.append(TreasuryFiscalObservation(
                series_id="TREAS_AVG_RATE",
                date=row.get("record_date", ""),
                value=float(val),
                metadata={"security_desc": row.get("security_desc", "")},
            ))
        except (ValueError, TypeError):
            continue
    return observations


class TreasuryFiscalClient:
    """Client for the Treasury Fiscal Data API (no API key required)."""

    BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "AnalystEngine/1.0",
        })

    def get_dataset(
        self,
        endpoint: str,
        *,
        fields: str | None = None,
        filter_str: str | None = None,
        sort: str = "-record_date",
        page_size: int = 100,
    ) -> list[dict]:
        """Fetch raw rows from a Treasury Fiscal Data endpoint."""
        _payload, rows, _params = self.get_dataset_with_raw(
            endpoint, fields=fields, filter_str=filter_str,
            sort=sort, page_size=page_size,
        )
        return rows

    def get_dataset_with_raw(
        self,
        endpoint: str,
        *,
        fields: str | None = None,
        filter_str: str | None = None,
        sort: str = "-record_date",
        page_size: int = 100,
    ) -> tuple[dict, list[dict], dict[str, str]]:
        """Fetch a Treasury endpoint and return ``(payload, data_rows, params)``.

        The full envelope (``meta`` + pagination) lives in ``payload`` so the
        issue #116 ``obs_raw`` write path can hash + replay it; ``data_rows``
        is the projection used by the typed fetchers.
        """
        url = f"{self.BASE_URL}/{endpoint}"
        params: dict = {
            "page[size]": page_size,
            "page[number]": 1,
            "sort": sort,
        }
        if fields:
            params["fields"] = fields
        if filter_str:
            params["filter"] = filter_str
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        audit_params = {"endpoint": endpoint, **{k: str(v) for k, v in params.items()}}
        return payload, rows, audit_params

    def fetch_debt_outstanding(self, *, limit: int = 30) -> list[TreasuryFiscalObservation]:
        """Fetch total public debt outstanding (Debt to the Penny)."""
        observations, _payload, _params = self.fetch_debt_outstanding_with_raw(limit=limit)
        return observations

    def fetch_debt_outstanding_with_raw(
        self, *, limit: int = 30,
    ) -> tuple[list[TreasuryFiscalObservation], dict, dict[str, str]]:
        payload, _rows, params = self.get_dataset_with_raw(
            "v2/accounting/od/debt_to_penny",
            fields="record_date,debt_held_public_amt,intragov_hold_amt,tot_pub_debt_out_amt",
            page_size=limit,
        )
        return _parse_treasury_debt(payload), payload, params

    def fetch_tga_balance(self, *, limit: int = 30) -> list[TreasuryFiscalObservation]:
        """Fetch Treasury General Account (TGA) closing balance."""
        observations, _payload, _params = self.fetch_tga_balance_with_raw(limit=limit)
        return observations

    def fetch_tga_balance_with_raw(
        self, *, limit: int = 30,
    ) -> tuple[list[TreasuryFiscalObservation], dict, dict[str, str]]:
        payload, _rows, params = self.get_dataset_with_raw(
            "v1/accounting/dts/operating_cash_balance",
            fields="record_date,account_type,open_today_bal",
            filter_str="account_type:eq:Treasury General Account (TGA) Closing Balance",
            page_size=limit,
        )
        return _parse_treasury_tga(payload), payload, params

    def fetch_avg_interest_rates(self, *, limit: int = 12) -> list[TreasuryFiscalObservation]:
        """Fetch average interest rates on Treasury securities."""
        observations, _payload, _params = self.fetch_avg_interest_rates_with_raw(limit=limit)
        return observations

    def fetch_avg_interest_rates_with_raw(
        self, *, limit: int = 12,
    ) -> tuple[list[TreasuryFiscalObservation], dict, dict[str, str]]:
        payload, _rows, params = self.get_dataset_with_raw(
            "v2/accounting/od/avg_interest_rates",
            fields="record_date,security_desc,avg_interest_rate_amt",
            page_size=limit,
        )
        return _parse_treasury_avg_rates(payload), payload, params
