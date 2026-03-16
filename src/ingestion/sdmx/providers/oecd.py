"""OECD SDMX provider — placeholder for the most complex provider.

The full OECD client (1,136 lines) uses XML for structure endpoints,
has unique methods (build_key, enumerate_series, series_to_filters),
and uses a different URL pattern with v2 fallback.

The existing ``scrapers/oecd.py`` remains the authoritative implementation.
This module provides a re-export hook so that the OECD client participates
in the unified error hierarchy and type system.
"""

from __future__ import annotations

from .._errors import OECDAPIError, OECDRateLimitError

# The full OECDClient stays in scrapers/oecd.py for now.
# When scrapers/oecd.py is converted to a thin wrapper (Phase 3),
# the full implementation will move here.

__all__ = ["OECDAPIError", "OECDRateLimitError"]
