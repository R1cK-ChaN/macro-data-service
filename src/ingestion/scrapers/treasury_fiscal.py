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
        """Fetch raw rows from a Treasury Fiscal Data endpoint.

        Args:
            endpoint: API path, e.g. ``"v2/accounting/od/debt_to_penny"``.
            fields: Comma-separated field names to return.
            filter_str: Filter expression, e.g. ``"account_type:eq:Federal Reserve Account"``.
            sort: Sort expression (prefix ``-`` for descending).
            page_size: Number of rows per page.
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
        return response.json().get("data", [])

    def fetch_debt_outstanding(self, *, limit: int = 30) -> list[TreasuryFiscalObservation]:
        """Fetch total public debt outstanding (Debt to the Penny)."""
        rows = self.get_dataset(
            "v2/accounting/od/debt_to_penny",
            fields="record_date,debt_held_public_amt,intragov_hold_amt,tot_pub_debt_out_amt",
            page_size=limit,
        )
        observations: list[TreasuryFiscalObservation] = []
        for row in rows:
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

    def fetch_tga_balance(self, *, limit: int = 30) -> list[TreasuryFiscalObservation]:
        """Fetch Treasury General Account (TGA) closing balance."""
        rows = self.get_dataset(
            "v1/accounting/dts/operating_cash_balance",
            fields="record_date,account_type,open_today_bal",
            filter_str="account_type:eq:Treasury General Account (TGA) Closing Balance",
            page_size=limit,
        )
        observations: list[TreasuryFiscalObservation] = []
        for row in rows:
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

    def fetch_avg_interest_rates(self, *, limit: int = 12) -> list[TreasuryFiscalObservation]:
        """Fetch average interest rates on Treasury securities."""
        rows = self.get_dataset(
            "v2/accounting/od/avg_interest_rates",
            fields="record_date,security_desc,avg_interest_rate_amt",
            page_size=limit,
        )
        observations: list[TreasuryFiscalObservation] = []
        for row in rows:
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
