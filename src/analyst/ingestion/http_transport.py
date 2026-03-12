"""HTTP transport factory with Cloudflare TLS-fingerprint bypass."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def create_cf_session(*, impersonate: str = "chrome", headers: dict[str, str] | None = None):
    """Return a curl_cffi Session that impersonates a real browser.

    Falls back to a plain requests.Session (with a warning) when curl_cffi
    is not installed so the rest of the codebase keeps working in degraded mode.
    """
    try:
        from curl_cffi.requests import Session

        session = Session(impersonate=impersonate)
        if headers:
            session.headers.update(headers)
        return session
    except ImportError:
        import requests

        logger.warning(
            "curl_cffi not installed — falling back to requests.Session. "
            "Cloudflare-protected sites will likely return 403."
        )
        session = requests.Session()
        if headers:
            session.headers.update(headers)
        return session
