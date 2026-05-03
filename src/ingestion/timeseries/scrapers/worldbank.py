"""World Bank REST API client — full catalog discovery and indicator data.

Uses the World Bank Indicators API v2 at ``api.worldbank.org/v2`` which
provides free, unauthenticated access to ~16,000 development indicators
across 200+ countries.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class WorldBankAPIError(RuntimeError):
    """Base error for World Bank API failures."""


class WorldBankRateLimitError(WorldBankAPIError):
    """Raised when the World Bank API throttles a request (HTTP 429)."""


class WorldBankResponseFormatError(WorldBankAPIError):
    """Raised when the World Bank API returns an unexpected response format."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_date(raw: str) -> str:
    """Normalize World Bank date strings to YYYY-MM-DD.

    Handles: ``"2023"`` → ``"2023-01-01"`` (annual data).
    """
    if re.match(r"^\d{4}$", raw):
        return f"{raw}-01-01"
    return raw


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class WorldBankObservation:
    """A single observation from the World Bank API."""

    series_id: str
    date: str
    value: float
    indicator: str = ""
    country_code: str = ""
    country_name: str = ""


@dataclass(frozen=True)
class WorldBankSource:
    """A World Bank data source/database (e.g. WDI, IDS)."""

    id: str
    name: str
    description: str = ""
    url: str = ""


@dataclass(frozen=True)
class WorldBankTopic:
    """A World Bank topic grouping (e.g. Education, Health)."""

    id: str
    name: str
    note: str = ""


@dataclass(frozen=True)
class WorldBankIndicatorInfo:
    """Metadata for a World Bank indicator from the catalog."""

    id: str
    name: str
    source_id: str = ""
    source_name: str = ""
    source_note: str = ""
    topics: tuple[WorldBankTopic, ...] = ()


@dataclass(frozen=True)
class WorldBankCountry:
    """A World Bank country/economy."""

    id: str
    iso2_code: str = ""
    name: str = ""
    region: str = ""
    income_level: str = ""
    lending_type: str = ""


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class WorldBankClient:
    """Client for the World Bank Indicators API v2 (no API key required)."""

    BASE_URL = "https://api.worldbank.org/v2"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "AnalystEngine/1.0",
        })
        # Instance-level caches for rarely-changing catalog data
        self._sources_cache: list[WorldBankSource] | None = None
        self._topics_cache: list[WorldBankTopic] | None = None
        self._countries_cache: list[WorldBankCountry] | None = None

    # -- low-level helpers ---------------------------------------------------

    def _get_json(self, url: str, *, params: dict[str, str] | None = None) -> requests.Response:
        """Issue a GET request with error handling."""
        response = self.session.get(url, params=params, timeout=30)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "60")
            raise WorldBankRateLimitError(
                f"World Bank rate limit hit (429). Retry-After: {retry_after}s"
            )
        response.raise_for_status()
        return response

    def _get_all_pages(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        per_page: int = 1000,
        max_pages: int | None = None,
    ) -> list[dict]:
        """Fetch all pages from a World Bank v2 endpoint.

        World Bank returns ``[{page_info}, [records]]`` where ``page_info``
        contains ``page``, ``pages``, ``per_page``, ``total``.
        """
        records, _ = self._get_all_pages_with_total(
            url, params=params, per_page=per_page, max_pages=max_pages,
        )
        return records

    def _get_all_pages_with_total(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        per_page: int = 1000,
        max_pages: int | None = None,
    ) -> tuple[list[dict], int]:
        """Fetch all pages and return ``(records, api_reported_total)``.

        The ``api_reported_total`` is the ``total`` field from the first
        page's metadata — the number the API *claims* to have.  Callers
        can compare ``len(records) == api_reported_total`` to detect
        pagination truncation.
        """
        params = dict(params) if params else {}
        params["format"] = "json"
        params["per_page"] = str(per_page)

        all_records: list[dict] = []
        api_total = 0
        page = 1

        while True:
            params["page"] = str(page)
            resp = self._get_json(url, params=params)
            data = resp.json()

            if not isinstance(data, list) or len(data) < 2:
                break

            page_info = data[0]
            if page == 1:
                api_total = int(page_info.get("total", 0))
            records = data[1]
            if records:
                all_records.extend(records)

            total_pages = int(page_info.get("pages", 1))
            if page >= total_pages:
                break
            if max_pages is not None and page >= max_pages:
                break
            page += 1

        return all_records, api_total

    def get_api_total(self, endpoint: str, *, params: dict[str, str] | None = None) -> int:
        """Return the API-reported ``total`` count for an endpoint (single request).

        Useful for quick count verification without fetching all records.
        """
        p = dict(params) if params else {}
        p["format"] = "json"
        p["per_page"] = "1"
        resp = self._get_json(f"{self.BASE_URL}/{endpoint}", params=p)
        data = resp.json()
        if isinstance(data, list) and len(data) >= 1 and isinstance(data[0], dict):
            return int(data[0].get("total", 0))
        return 0

    # -- catalog counts (for data validation) ---------------------------------

    def count_sources(self) -> int:
        """Return the API-reported total number of sources."""
        return self.get_api_total("source")

    def count_topics(self) -> int:
        """Return the API-reported total number of topics."""
        return self.get_api_total("topic")

    def count_countries(self) -> int:
        """Return the API-reported total number of countries/economies."""
        return self.get_api_total("country")

    def count_indicators(self, *, source_id: str | None = None, topic_id: str | None = None) -> int:
        """Return the API-reported total number of indicators."""
        if topic_id:
            endpoint = f"topic/{topic_id}/indicator"
        elif source_id:
            endpoint = f"source/{source_id}/indicator"
        else:
            endpoint = "indicator"
        return self.get_api_total(endpoint)

    def count_indicator_observations(self, indicator_code: str, country: str = "all") -> int:
        """Return the API-reported total observations for an indicator/country."""
        return self.get_api_total(
            f"country/{country}/indicator/{indicator_code}",
        )

    # -- catalog discovery ---------------------------------------------------

    def list_sources(self) -> list[WorldBankSource]:
        """List all World Bank data sources/databases."""
        if self._sources_cache is not None:
            return self._sources_cache

        records, api_total = self._get_all_pages_with_total(f"{self.BASE_URL}/source")
        sources: list[WorldBankSource] = []
        for rec in records:
            try:
                sources.append(WorldBankSource(
                    id=str(rec.get("id", "")),
                    name=rec.get("name", ""),
                    description=rec.get("description", ""),
                    url=rec.get("url", ""),
                ))
            except (KeyError, TypeError):
                continue
        if api_total and len(records) != api_total:
            logger.warning(
                "World Bank source pagination mismatch: fetched %d, API reports %d",
                len(records), api_total,
            )
        self._sources_cache = sources
        return sources

    def list_topics(self) -> list[WorldBankTopic]:
        """List all World Bank topic categories."""
        if self._topics_cache is not None:
            return self._topics_cache

        records, api_total = self._get_all_pages_with_total(f"{self.BASE_URL}/topic")
        topics: list[WorldBankTopic] = []
        for rec in records:
            try:
                tid = rec.get("id", "")
                name = rec.get("value", "")
                if not tid or not name:
                    continue
                topics.append(WorldBankTopic(
                    id=str(tid),
                    name=name,
                    note=rec.get("sourceNote", ""),
                ))
            except (KeyError, TypeError):
                continue
        if api_total and len(records) != api_total:
            logger.warning(
                "World Bank topic pagination mismatch: fetched %d, API reports %d",
                len(records), api_total,
            )
        self._topics_cache = topics
        return topics

    def list_countries(
        self,
        *,
        income_level: str | None = None,
    ) -> list[WorldBankCountry]:
        """List all World Bank countries/economies."""
        if self._countries_cache is not None and income_level is None:
            return self._countries_cache

        params: dict[str, str] = {}
        if income_level:
            params["incomeLevel"] = income_level

        records, api_total = self._get_all_pages_with_total(
            f"{self.BASE_URL}/country", params=params,
        )
        countries: list[WorldBankCountry] = []
        for rec in records:
            try:
                countries.append(WorldBankCountry(
                    id=rec.get("id", ""),
                    iso2_code=rec.get("iso2Code", ""),
                    name=rec.get("name", ""),
                    region=(rec.get("region") or {}).get("value", ""),
                    income_level=(rec.get("incomeLevel") or {}).get("value", ""),
                    lending_type=(rec.get("lendingType") or {}).get("value", ""),
                ))
            except (KeyError, TypeError):
                continue
        if api_total and len(records) != api_total:
            logger.warning(
                "World Bank country pagination mismatch: fetched %d, API reports %d",
                len(records), api_total,
            )

        if income_level is None:
            self._countries_cache = countries
        return countries

    def list_indicators(
        self,
        *,
        source_id: str | None = None,
        topic_id: str | None = None,
        per_page: int = 1000,
        max_pages: int | None = None,
    ) -> list[WorldBankIndicatorInfo]:
        """List indicators, optionally filtered by source or topic.

        - ``/v2/indicator`` — all indicators
        - ``/v2/source/{id}/indicator`` — by source database
        - ``/v2/topic/{id}/indicator`` — by topic
        """
        if topic_id:
            url = f"{self.BASE_URL}/topic/{topic_id}/indicator"
        elif source_id:
            url = f"{self.BASE_URL}/source/{source_id}/indicator"
        else:
            url = f"{self.BASE_URL}/indicator"

        records, api_total = self._get_all_pages_with_total(
            url, per_page=per_page, max_pages=max_pages,
        )
        if max_pages is None and api_total and len(records) != api_total:
            logger.warning(
                "World Bank indicator pagination mismatch: fetched %d, API reports %d",
                len(records), api_total,
            )
        return self._parse_indicator_records(records)

    def search_indicators(
        self,
        query: str,
        *,
        source_id: str | None = None,
        topic_id: str | None = None,
        limit: int = 50,
    ) -> list[WorldBankIndicatorInfo]:
        """Search indicators by name/ID substring match (client-side filter)."""
        all_indicators = self.list_indicators(source_id=source_id, topic_id=topic_id)
        query_lower = query.lower()
        matches = [
            ind for ind in all_indicators
            if query_lower in ind.id.lower() or query_lower in ind.name.lower()
        ]
        return matches[:limit]

    @staticmethod
    def _parse_indicator_records(records: list[dict]) -> list[WorldBankIndicatorInfo]:
        """Parse raw indicator records into typed objects."""
        indicators: list[WorldBankIndicatorInfo] = []
        for rec in records:
            try:
                topics_raw = rec.get("topics") or []
                topics = tuple(
                    WorldBankTopic(
                        id=str(t.get("id", "")),
                        name=t.get("value", ""),
                    )
                    for t in topics_raw
                    if t.get("id") and t.get("value")
                )
                source = rec.get("source") or {}
                indicators.append(WorldBankIndicatorInfo(
                    id=rec.get("id", ""),
                    name=rec.get("name", ""),
                    source_id=str(source.get("id", "")),
                    source_name=source.get("value", ""),
                    source_note=rec.get("sourceNote", ""),
                    topics=topics,
                ))
            except (KeyError, TypeError):
                continue
        return indicators

    # -- data fetch ----------------------------------------------------------

    def get_indicator(
        self,
        indicator_code: str,
        country: str = "all",
        *,
        series_id: str,
        start_year: int | None = None,
        end_year: int | None = None,
        limit: int = 50,
        per_page: int = 1000,
        fetch_all_pages: bool = False,
    ) -> list[WorldBankObservation]:
        """Fetch indicator observations for a country (or all countries)."""
        observations, _payload, _params = self.get_indicator_with_raw(
            indicator_code, country,
            series_id=series_id,
            start_year=start_year, end_year=end_year,
            limit=limit, per_page=per_page, fetch_all_pages=fetch_all_pages,
        )
        return observations

    def get_indicator_with_raw(
        self,
        indicator_code: str,
        country: str = "all",
        *,
        series_id: str,
        start_year: int | None = None,
        end_year: int | None = None,
        limit: int = 50,
        per_page: int = 1000,
        fetch_all_pages: bool = False,
    ) -> tuple[list[WorldBankObservation], dict, dict[str, str]]:
        """Fetch indicator observations and return ``(parsed, payload, params)``.

        Used by the issue #116 ``obs_raw`` write path. The single-page
        path returns the API's literal ``[{page_info}, [records]]``
        envelope as ``payload['response']``; the paginated path
        accumulates all records under the same shape so the parser
        replay path is identical regardless of which fetch mode landed
        the audit row.
        """
        url = f"{self.BASE_URL}/country/{country}/indicator/{indicator_code}"
        params: dict[str, str] = {}
        if start_year and end_year:
            params["date"] = f"{start_year}:{end_year}"
        elif start_year:
            params["date"] = f"{start_year}:2099"
        elif end_year:
            params["date"] = f"1960:{end_year}"

        audit_params: dict[str, str] = {
            "url": url,
            "indicator": indicator_code,
            "country": country,
            **params,
        }

        if fetch_all_pages:
            records, api_total = self._get_all_pages_with_total(
                url, params=params, per_page=per_page,
            )
            audit_params["per_page"] = str(per_page)
            audit_params["fetch_all_pages"] = "true"
            payload = {
                "response": [{"total": api_total, "page": 1, "pages": 1, "per_page": per_page}, records],
            }
            observations = self._parse_observation_records(
                records, series_id=series_id, indicator=indicator_code,
            )
            return observations, payload, audit_params

        # Single-page fetch
        params["format"] = "json"
        params["per_page"] = str(limit)
        audit_params["format"] = "json"
        audit_params["per_page"] = str(limit)
        response = self._get_json(url, params=params)
        body = response.json()
        observations = self._parse_json(
            body, series_id=series_id, indicator=indicator_code, limit=limit,
        )
        payload = {"response": body if isinstance(body, list) else []}
        return observations, payload, audit_params

    def fetch_indicator_raw(
        self,
        indicator_code: str,
        country: str = "all",
        *,
        per_page: int = 1000,
    ) -> tuple[list[dict], int]:
        """Fetch all raw records for an indicator (including null values).

        Returns ``(raw_records, api_total)`` for data validation.
        Every row the API claims to have is returned, whether
        ``value`` is ``null`` or not.
        """
        url = f"{self.BASE_URL}/country/{country}/indicator/{indicator_code}"
        return self._get_all_pages_with_total(url, per_page=per_page)

    def fetch_indicator_bulk(
        self,
        indicator_code: str,
        *,
        series_id_prefix: str | None = None,
        countries: list[str] | None = None,
        start_year: int | None = None,
        end_year: int | None = None,
        per_page: int = 1000,
    ) -> list[WorldBankObservation]:
        """Fetch all observations for an indicator across countries.

        Uses ``country=all`` with pagination or semicolon-joined country list
        (e.g. ``USA;CHN;GBR``). Each observation gets a series_id of
        ``{prefix}_{COUNTRY}`` or ``WB_{indicator}_{COUNTRY}``.
        """
        country_str = ";".join(countries) if countries else "all"
        prefix = series_id_prefix or f"WB_{indicator_code}"

        url = f"{self.BASE_URL}/country/{country_str}/indicator/{indicator_code}"
        params: dict[str, str] = {}
        if start_year and end_year:
            params["date"] = f"{start_year}:{end_year}"
        elif start_year:
            params["date"] = f"{start_year}:2099"

        records = self._get_all_pages(url, params=params, per_page=per_page)
        return self._parse_observation_records(
            records, series_id=None, indicator=indicator_code, series_id_prefix=prefix,
        )

    # -- parsing -------------------------------------------------------------

    @staticmethod
    def _parse_observation_records(
        records: list[dict],
        *,
        series_id: str | None,
        indicator: str,
        series_id_prefix: str | None = None,
    ) -> list[WorldBankObservation]:
        """Parse raw observation records (from paginated results) into typed objects."""
        observations: list[WorldBankObservation] = []
        for record in records:
            try:
                value = record.get("value")
                if value is None:
                    continue
                date_raw = record.get("date", "")
                if not date_raw:
                    continue

                country_info = record.get("country") or {}
                country_code = country_info.get("id", "")
                country_name = country_info.get("value", "")

                if series_id:
                    sid = series_id
                elif series_id_prefix:
                    sid = f"{series_id_prefix}_{country_code}"
                else:
                    sid = f"WB_{indicator}_{country_code}"

                observations.append(WorldBankObservation(
                    series_id=sid,
                    date=_normalize_date(str(date_raw)),
                    value=float(value),
                    indicator=indicator,
                    country_code=country_code,
                    country_name=country_name,
                ))
            except (ValueError, TypeError):
                continue

        observations.sort(key=lambda o: (o.country_code, o.date), reverse=True)
        return observations

    @staticmethod
    def _parse_json(
        data: list | dict,
        *,
        series_id: str,
        indicator: str,
        limit: int,
    ) -> list[WorldBankObservation]:
        """Parse World Bank JSON response into observations.

        World Bank returns ``[{page_info}, [{record}, ...]]``.
        """
        observations: list[WorldBankObservation] = []

        if not isinstance(data, list) or len(data) < 2:
            return observations

        records = data[1]
        if not records:
            return observations

        for record in records:
            try:
                value = record.get("value")
                if value is None:
                    continue
                date_raw = record.get("date", "")
                if not date_raw:
                    continue

                country_info = record.get("country") or {}

                observations.append(WorldBankObservation(
                    series_id=series_id,
                    date=_normalize_date(str(date_raw)),
                    value=float(value),
                    indicator=indicator,
                    country_code=country_info.get("id", ""),
                    country_name=country_info.get("value", ""),
                ))
            except (ValueError, TypeError):
                continue

        observations.sort(key=lambda o: o.date, reverse=True)
        return observations[:limit]
