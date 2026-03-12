from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import requests

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://weibo.com/",
}


@dataclass(frozen=True)
class WeiboTrendItem:
    word: str
    note: str
    word_scheme: str
    category: str
    realpos: int
    num: int
    raw_hot: int
    onboard_time: int
    label_name: str = ""
    topic_flag: int = 0
    raw_json: dict[str, Any] = field(default_factory=dict)


class WeiboTrendClient:
    HOT_BAND_URL = "https://weibo.com/ajax/statuses/hot_band"

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout: int = 15,
    ) -> None:
        self._timeout = timeout
        self._session = session or requests.Session()
        self._owns_session = session is None
        self._session.headers.update(_DEFAULT_HEADERS)

    def fetch_hot_band(self) -> list[WeiboTrendItem]:
        response = self._session.get(self.HOT_BAND_URL, timeout=self._timeout)
        response.raise_for_status()
        payload = response.json()
        return self._parse_hot_band(payload)

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    @staticmethod
    def _parse_hot_band(payload: dict[str, Any]) -> list[WeiboTrendItem]:
        data = payload.get("data", {})
        items = data.get("band_list", [])
        if not isinstance(items, list):
            return []
        parsed: list[WeiboTrendItem] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            word = str(item.get("word") or "").strip()
            note = str(item.get("note") or word).strip()
            word_scheme = str(item.get("word_scheme") or word).strip()
            if not word or not note or not word_scheme:
                continue
            try:
                realpos = int(item.get("realpos", 0) or 0)
            except (TypeError, ValueError):
                realpos = 0
            try:
                num = int(item.get("num", 0) or 0)
            except (TypeError, ValueError):
                num = 0
            try:
                raw_hot = int(item.get("raw_hot", 0) or 0)
            except (TypeError, ValueError):
                raw_hot = 0
            try:
                onboard_time = int(item.get("onboard_time", 0) or 0)
            except (TypeError, ValueError):
                onboard_time = 0
            try:
                topic_flag = int(item.get("topic_flag", 0) or 0)
            except (TypeError, ValueError):
                topic_flag = 0
            parsed.append(
                WeiboTrendItem(
                    word=word,
                    note=note,
                    word_scheme=word_scheme,
                    category=str(item.get("category") or "").strip(),
                    realpos=realpos,
                    num=num,
                    raw_hot=raw_hot,
                    onboard_time=onboard_time,
                    label_name=str(item.get("label_name") or "").strip(),
                    topic_flag=topic_flag,
                    raw_json=item,
                )
            )
        return parsed
