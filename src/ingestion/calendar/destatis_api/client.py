"""Small GENESIS-Online REST client for Destatis calendar values."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Mapping

import requests


DESTATIS_GENESIS_BASE_URL = "https://www-genesis.destatis.de/genesisWS/rest/2020"
DESTATIS_USERNAME_ENV = "DESTATIS_GENESIS_USERNAME"
DESTATIS_PASSWORD_ENV = "DESTATIS_GENESIS_PASSWORD"


class DestatisGenesisError(RuntimeError):
    """Raised when GENESIS returns an application-level error payload."""


@dataclass
class DestatisGenesisClient:
    """POST-based GENESIS-Online client.

    Since July 2025 Destatis' RESTful/JSON interface uses POST methods.
    Authentication rides in ``username`` / ``password`` headers; public
    access can use the guest credentials while registered users can
    pass their token as ``username``.
    """

    username: str = "GAST"
    password: str = "GAST"
    base_url: str = DESTATIS_GENESIS_BASE_URL
    timeout: float = 30.0
    session: requests.Session = field(default_factory=requests.Session)

    @classmethod
    def from_env(cls) -> "DestatisGenesisClient":
        """Build a client from environment credentials with guest fallback."""
        return cls(
            username=os.getenv(DESTATIS_USERNAME_ENV, "GAST"),
            password=os.getenv(DESTATIS_PASSWORD_ENV, "GAST"),
        )

    def tablefile(
        self,
        table_name: str,
        *,
        start_year: int | None = None,
        end_year: int | None = None,
        extra_params: Mapping[str, str] | None = None,
    ) -> str:
        """Download a GENESIS table as flat CSV text."""
        url = f"{self.base_url.rstrip('/')}/data/tablefile"
        data: dict[str, str] = {
            "compress": "false",
            "name": table_name,
            "area": "free",
            "stand": "01.01.1970 01:00",
            "format": "datencsv",
            "language": "de",
            "transpose": "false",
            "job": "false",
            "quality": "off",
        }
        if start_year is not None:
            data["startyear"] = str(start_year)
        if end_year is not None:
            data["endyear"] = str(end_year)
        if extra_params:
            data.update({str(k): str(v) for k, v in extra_params.items()})

        response = self.session.post(
            url,
            data=data,
            headers={
                "accept": "application/octet-stream",
                "username": self.username,
                "password": self.password,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        text = response.content.decode(response.encoding or "utf-8-sig")
        return self._extract_response_text(text)

    @staticmethod
    def _extract_response_text(text: str) -> str:
        stripped = text.strip()
        if not stripped.startswith(("{", "[")):
            return text
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            return text
        if isinstance(payload, list):
            return text
        status = payload.get("Status") or payload.get("status")
        if isinstance(status, dict):
            code = str(status.get("Code") or status.get("code") or "")
            content = str(
                status.get("Content")
                or status.get("content")
                or status.get("Message")
                or status.get("message")
                or ""
            )
            if code and code not in {"0", "00", "OK"}:
                raise DestatisGenesisError(content or f"GENESIS status {code}")
        message = payload.get("message") or payload.get("Message")
        if message:
            raise DestatisGenesisError(str(message))
        obj = payload.get("Object") or payload.get("object")
        if isinstance(obj, dict):
            content = obj.get("Content") or obj.get("content")
            if isinstance(content, str):
                return content
        return text
