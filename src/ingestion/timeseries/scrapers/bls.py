"""BLS (Bureau of Labor Statistics) API v2 client — macro time-series data."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import requests

from env import get_env_value

logger = logging.getLogger(__name__)


class BLSAPIError(RuntimeError):
    """Base error for BLS API failures."""


class BLSRateLimitError(BLSAPIError):
    """Raised when BLS throttles a request (HTTP 429) or daily budget exhausted."""


class BLSResponseError(BLSAPIError):
    """Raised when BLS returns status != REQUEST_SUCCEEDED."""


@dataclass(frozen=True)
class BLSObservation:
    """A single observation from the BLS API."""

    series_id: str
    date: str       # YYYY-MM-DD (normalized from BLS year+period)
    value: float
    period: str     # original BLS period code: M01..M13, Q01..Q05, S01, S02, A01
    raw: dict = field(default_factory=dict)
    """Original upstream dict (``year``/``period``/``value``/``footnotes``/
    ``periodName``/``latest``/…). Calendar-lane callers hash on this
    so footnote-only revisions land as new raw audit rows. Populated
    by :meth:`BLSClient._parse_observation`; empty-dict default keeps
    test fixtures and external callers that construct ``BLSObservation``
    manually unaffected."""


@dataclass(frozen=True)
class BLSSurvey:
    """A BLS survey program."""

    survey_abbreviation: str   # e.g. "CU", "CE", "WP"
    survey_name: str           # e.g. "CPI-All Urban Consumers"


@dataclass(frozen=True)
class BLSSeriesInfo:
    """Metadata for a BLS series (from catalog)."""

    series_id: str
    survey_abbreviation: str
    survey_name: str
    series_title: str
    begin_year: str
    end_year: str
    catalog: dict = field(default_factory=dict)


_MAX_SERIES_PER_REQUEST = 50
_MAX_YEARS_PER_QUERY = 20
_DAILY_QUERY_WARN = 400
_DAILY_QUERY_LIMIT = 490


class BLSClient:
    """Client for the BLS Public Data API v2."""

    BASE_URL = "https://api.bls.gov/publicAPI/v2"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or get_env_value("BLS_API_KEY")
        self.session = requests.Session()
        self._daily_query_count = 0
        self._query_date = ""

    @property
    def daily_query_count(self) -> int:
        """Queries made against the BLS API since the last UTC-day reset.

        Read-only view onto the in-memory counter so the calendar
        scheduler's P-sched-3-budget tracker can attribute consumption
        to the ``bls`` connector without poking at private state.
        """
        return self._daily_query_count

    # -- Public methods ------------------------------------------------------

    def get_series(
        self,
        series_ids: str | list[str],
        *,
        start_year: int | None = None,
        end_year: int | None = None,
        catalog: bool = False,
        calculations: bool = False,
        annual_average: bool = False,
    ) -> dict[str, list[BLSObservation]]:
        """Fetch observations for one or more series (batch, up to 50 per request).

        Returns dict mapping series_id -> list[BLSObservation].
        Auto-chunks if >50 series or >20 year span.
        """
        if not self.api_key:
            return {}
        if isinstance(series_ids, str):
            series_ids = [series_ids]
        if not series_ids:
            return {}

        now_year = datetime.now(timezone.utc).year
        if start_year is None:
            start_year = now_year - 10
        if end_year is None:
            end_year = now_year

        # Build year chunks (max 20 years per query)
        year_ranges = self._chunk_years(start_year, end_year)
        # Build series chunks (max 50 per request)
        series_chunks = [
            series_ids[i : i + _MAX_SERIES_PER_REQUEST]
            for i in range(0, len(series_ids), _MAX_SERIES_PER_REQUEST)
        ]

        results: dict[str, list[BLSObservation]] = {sid: [] for sid in series_ids}
        for chunk in series_chunks:
            for yr_start, yr_end in year_ranges:
                payload = self._post_timeseries(
                    chunk,
                    start_year=yr_start,
                    end_year=yr_end,
                    catalog=catalog,
                    calculations=calculations,
                    annual_average=annual_average,
                )
                for series_data in payload.get("Results", {}).get("series", []):
                    sid = series_data.get("seriesID", "")
                    for obs in series_data.get("data", []):
                        parsed = self._parse_observation(sid, obs)
                        if parsed is not None:
                            results.setdefault(sid, []).append(parsed)

        return results

    def get_series_single(
        self,
        series_id: str,
        *,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> list[BLSObservation]:
        """Convenience: fetch one series, return flat list."""
        result = self.get_series(
            series_id, start_year=start_year, end_year=end_year,
        )
        return result.get(series_id, [])

    def get_series_single_with_raw(
        self,
        series_id: str,
        *,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> tuple[list[BLSObservation], dict, dict[str, Any]]:
        """Fetch one series and return parsed obs + raw HTTP body + request params.

        Used by the issue #69 ``obs_raw`` write path. Mirrors
        :meth:`get_series_single` but does not chunk: assumes the
        ``end_year - start_year`` span fits in one BLS POST (≤ 20 years).
        Larger spans fall back to ``get_series_single`` for the parsed
        observations and skip the raw write — slice 1 ships single-
        request capture; multi-chunk audit is deferred until a caller
        actually needs > 20 years and the existing chunking helper can
        be extended.
        """
        if not self.api_key:
            return [], {}, {}
        now_year = datetime.now(timezone.utc).year
        if start_year is None:
            start_year = now_year - 10
        if end_year is None:
            end_year = now_year
        if end_year - start_year + 1 > _MAX_YEARS_PER_QUERY:
            # Span exceeds single-POST limit; fall back to chunked fetch
            # without raw capture.
            return self.get_series_single(
                series_id, start_year=start_year, end_year=end_year,
            ), {}, {}

        request_body: dict[str, Any] = {
            "seriesid": [series_id],
            "startyear": str(start_year),
            "endyear": str(end_year),
        }
        payload = self._post_timeseries(
            [series_id], start_year=start_year, end_year=end_year,
        )
        observations: list[BLSObservation] = []
        for series_data in payload.get("Results", {}).get("series", []):
            sid = series_data.get("seriesID", "")
            if sid != series_id:
                continue
            for obs in series_data.get("data", []):
                parsed = self._parse_observation(sid, obs)
                if parsed is not None:
                    observations.append(parsed)
        return observations, payload, request_body

    def get_series_info(self, series_id: str) -> BLSSeriesInfo:
        """Fetch series metadata via catalog=True."""
        if not self.api_key:
            return BLSSeriesInfo(
                series_id=series_id,
                survey_abbreviation="",
                survey_name="",
                series_title="",
                begin_year="",
                end_year="",
            )
        now_year = datetime.now(timezone.utc).year
        payload = self._post_timeseries(
            [series_id],
            start_year=now_year,
            end_year=now_year,
            catalog=True,
        )
        series_list = payload.get("Results", {}).get("series", [])
        if not series_list:
            raise BLSAPIError(f"Series {series_id} not found")
        s = series_list[0]
        cat = s.get("catalog", {})
        # Extract year range from the data array if available
        data = s.get("data", [])
        years = [d.get("year", "") for d in data if d.get("year")]
        begin_year = cat.get("begin_year", "")
        end_year = cat.get("end_year", "")
        if not begin_year and years:
            begin_year = min(years)
        if not end_year and years:
            end_year = max(years)
        return BLSSeriesInfo(
            series_id=s.get("seriesID", series_id),
            survey_abbreviation=cat.get("survey_abbreviation", ""),
            survey_name=cat.get("survey_name", ""),
            series_title=cat.get("series_title", ""),
            begin_year=begin_year,
            end_year=end_year,
            catalog=cat,
        )

    def list_surveys(self) -> list[BLSSurvey]:
        """GET /surveys/ — list all BLS survey programs."""
        if not self.api_key:
            return []
        try:
            response = self.session.get(
                f"{self.BASE_URL}/surveys",
                params={"registrationkey": self.api_key},
                timeout=30,
            )
            self._check_http(response)
            self._increment_query_count()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise BLSAPIError(f"Failed to list surveys: {exc}") from exc

        surveys: list[BLSSurvey] = []
        for s in payload.get("Results", {}).get("survey", []):
            surveys.append(BLSSurvey(
                survey_abbreviation=s.get("survey_abbreviation", ""),
                survey_name=s.get("survey_name", ""),
            ))
        return surveys

    # -- Catalog methods (flat-file based) ------------------------------------

    _CATALOG_BASE = "https://download.bls.gov/pub/time.series"

    def get_survey_series_catalog(
        self,
        survey_prefix: str,
    ) -> list[str]:
        """Download the full series list for a survey from BLS flat files.

        BLS publishes tab-delimited catalog files at
        download.bls.gov/pub/time.series/{prefix}/{prefix}.series.
        This does NOT count against the 500 queries/day API limit.
        """
        prefix = survey_prefix.lower()
        url = f"{self._CATALOG_BASE}/{prefix}/{prefix}.series"
        try:
            response = self.session.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; MacroDataService/1.0)",
                    "Accept": "text/plain,*/*",
                },
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Failed to fetch catalog for %s: %s", survey_prefix, exc)
            return []
        lines = response.text.strip().split("\n")
        if len(lines) <= 1:
            return []
        series_ids: list[str] = []
        for line in lines[1:]:  # skip header
            parts = line.split("\t")
            if parts:
                sid = parts[0].strip()
                if sid:
                    series_ids.append(sid)
        return series_ids

    def count_survey_series(self, survey_prefix: str) -> int:
        """Return the total series count for a survey (from flat-file catalog)."""
        return len(self.get_survey_series_catalog(survey_prefix))

    # -- Internal methods ----------------------------------------------------

    def _post_timeseries(
        self,
        series_ids: list[str],
        *,
        start_year: int,
        end_year: int,
        catalog: bool = False,
        calculations: bool = False,
        annual_average: bool = False,
    ) -> dict:
        """Low-level POST to /timeseries/data/ with rate tracking."""
        self._check_daily_budget()

        body: dict = {
            "seriesid": series_ids,
            "startyear": str(start_year),
            "endyear": str(end_year),
            "registrationkey": self.api_key,
        }
        if catalog:
            body["catalog"] = True
        if calculations:
            body["calculations"] = True
        if annual_average:
            body["annualaverage"] = True

        try:
            response = self.session.post(
                f"{self.BASE_URL}/timeseries/data/",
                json=body,
                timeout=30,
            )
            self._check_http(response)
            self._increment_query_count()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise BLSAPIError(f"BLS API request failed: {exc}") from exc

        status = payload.get("status", "")
        if status != "REQUEST_SUCCEEDED":
            messages = payload.get("message", [])
            raise BLSResponseError(
                f"BLS status={status}: {messages}"
            )
        return payload

    def _check_http(self, response: requests.Response) -> None:
        """Convert HTTP errors to BLSAPIError/BLSRateLimitError."""
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            if response.status_code == 429:
                raise BLSRateLimitError(f"BLS rate limit: {exc}") from exc
            raise BLSAPIError(
                f"BLS API error {response.status_code}: {exc}"
            ) from exc

    def _check_daily_budget(self) -> None:
        """Track daily query count; warn/raise if approaching limit."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._query_date != today:
            self._query_date = today
            self._daily_query_count = 0

        if self._daily_query_count >= _DAILY_QUERY_LIMIT:
            raise BLSRateLimitError(
                f"Daily query budget exhausted ({self._daily_query_count}/{_DAILY_QUERY_LIMIT})"
            )

    def _increment_query_count(self) -> None:
        self._daily_query_count += 1
        if self._daily_query_count == _DAILY_QUERY_WARN:
            logger.warning(
                "BLS daily query count at %d/%d (80%% budget consumed)",
                self._daily_query_count, _DAILY_QUERY_LIMIT,
            )

    @staticmethod
    def _chunk_years(
        start_year: int, end_year: int,
    ) -> list[tuple[int, int]]:
        """Split a year range into chunks of at most 20 years."""
        chunks: list[tuple[int, int]] = []
        y = start_year
        while y <= end_year:
            chunk_end = min(y + _MAX_YEARS_PER_QUERY - 1, end_year)
            chunks.append((y, chunk_end))
            y = chunk_end + 1
        return chunks

    @staticmethod
    def _normalize_period(year: str, period: str) -> str:
        """Convert BLS year + period code to YYYY-MM-DD.

        Monthly: M01-M12 → YYYY-MM-01, M13 (annual avg) → YYYY-12-31
        Quarterly: Q01-Q04 → last day of quarter, Q05 (annual) → YYYY-12-31
        Semi-annual: S01 → YYYY-06-30, S02 → YYYY-12-31
        Annual: A01 → YYYY-12-31
        """
        if period.startswith("M"):
            month = int(period[1:])
            if month == 13:
                return f"{year}-12-31"
            return f"{year}-{month:02d}-01"
        if period.startswith("Q"):
            q = int(period[1:])
            quarter_end = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31", 5: "12-31"}
            return f"{year}-{quarter_end.get(q, '12-31')}"
        if period.startswith("S"):
            s = int(period[1:])
            return f"{year}-06-30" if s == 1 else f"{year}-12-31"
        # Annual or unknown
        return f"{year}-12-31"

    @staticmethod
    def _parse_observation(
        series_id: str, obs: dict,
    ) -> BLSObservation | None:
        """Parse a single BLS observation dict into BLSObservation."""
        value_str = obs.get("value", "")
        if value_str == "-" or value_str == "":
            return None
        try:
            value = float(value_str)
        except (ValueError, TypeError):
            return None
        year = obs.get("year", "")
        period = obs.get("period", "")
        if not year or not period:
            return None
        date = BLSClient._normalize_period(year, period)
        return BLSObservation(
            series_id=series_id,
            date=date,
            value=value,
            period=period,
            raw=dict(obs),
        )
