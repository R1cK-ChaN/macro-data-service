#!/usr/bin/env python3
"""Entrypoint alias for the EODHD bulk market daily refresh."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from backfill_market_bars_bulk import main  # noqa: E402


if __name__ == "__main__":
    sys.exit(main())
