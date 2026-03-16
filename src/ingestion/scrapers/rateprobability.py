"""Client for rateprobability.com — FedWatch-equivalent FOMC rate probabilities."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

import requests

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FedMeetingProbability:
    """Probability data for a single FOMC meeting date."""

    meeting_date: str
    implied_rate: float
    prob_move_pct: float
    is_cut: bool
    num_moves: int
    change_bps: float


@dataclass(frozen=True)
class FedRateProbability:
    """Full snapshot of Fed rate probabilities from rateprobability.com."""

    as_of: str
    current_band: str
    midpoint: float
    effr: float
    meetings: list[FedMeetingProbability]
    snapshots: dict[str, list[FedMeetingProbability]] = field(default_factory=dict)


class RateProbabilityClient:
    """Fetches FOMC meeting rate probabilities from rateprobability.com."""

    BASE_URL = "https://rateprobability.com/api/latest"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "AnalystEngine/1.0",
        })

    def fetch_probabilities(self) -> FedRateProbability:
        """Fetch the latest rate probability data."""
        response = self.session.get(self.BASE_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        return self._parse(data)

    def _parse(self, data: dict) -> FedRateProbability:
        today = data.get("today", {})
        as_of = today.get("as_of", "")
        current_band = today.get("current band", "")
        midpoint = float(today.get("midpoint", 0))
        effr = float(today.get("most_recent_effr", 0))

        meetings = self._parse_meetings(today.get("rows", []))

        # Historical snapshots live at top-level keys: ago_1w, ago_3w, ago_6w, ago_10w
        snapshots: dict[str, list[FedMeetingProbability]] = {}
        for label in ("ago_1w", "ago_3w", "ago_6w", "ago_10w"):
            snapshot = data.get(label)
            if isinstance(snapshot, dict) and "rows" in snapshot:
                snapshots[label] = self._parse_snapshot_rows(snapshot["rows"])

        return FedRateProbability(
            as_of=as_of,
            current_band=current_band,
            midpoint=midpoint,
            effr=effr,
            meetings=meetings,
            snapshots=snapshots,
        )

    def _parse_meetings(self, raw: list[dict]) -> list[FedMeetingProbability]:
        meetings: list[FedMeetingProbability] = []
        for m in raw:
            try:
                meetings.append(FedMeetingProbability(
                    meeting_date=m.get("meeting_iso", ""),
                    implied_rate=float(m.get("implied_rate_post_meeting", 0)),
                    prob_move_pct=float(m.get("prob_move_pct", 0)),
                    is_cut=bool(m.get("prob_is_cut", False)),
                    num_moves=int(m.get("num_moves", 0)),
                    change_bps=float(m.get("change_bps", 0)),
                ))
            except (ValueError, TypeError):
                continue
        return meetings

    def _parse_snapshot_rows(self, raw: list[dict]) -> list[FedMeetingProbability]:
        """Parse simplified snapshot rows (only meeting_iso + implied)."""
        meetings: list[FedMeetingProbability] = []
        for m in raw:
            try:
                meetings.append(FedMeetingProbability(
                    meeting_date=m.get("meeting_iso", ""),
                    implied_rate=float(m.get("implied", 0)),
                    prob_move_pct=0,
                    is_cut=False,
                    num_moves=0,
                    change_bps=0,
                ))
            except (ValueError, TypeError):
                continue
        return meetings
