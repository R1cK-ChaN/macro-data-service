"""Issue #48 — EC BCS listing parser must handle the live ECL widget.

The live press-releases page wraps each PDF in a
``<div class="ecl-file" data-ecl-file="" id="ecl-file-…">`` widget where
the anchor's nearest ``<div>`` parent is ``ecl-file__action`` (carrying
only the "Download" link text). The pre-#48 parser walked
``find_parent(["li", "article", "div", "tr", "p"])`` and landed on that
narrow action div, so the haystack was ``"Download Download"`` and the
``label_pattern`` never matched. Result: every value-side scrape raised
``EC BCS press release not found for …`` and tripped the breaker.

Fixtures captured live 2026-04-26:
- ``ec_bcs_listing/press_releases_april_2026_live.html`` — 12 entries
  Apr → Jan 2026, all using the new ``ecl-file`` widget.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.calendar.ec_bcs_api.schedule import (
    EcBcsScheduleParseError,
    resolve_press_release_link,
)


LIVE_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "ec_bcs_listing"
    / "press_releases_april_2026_live.html"
)


def _live_html() -> str:
    return LIVE_FIXTURE.read_text(encoding="utf-8")


def test_resolves_flash_cci_on_live_listing_for_2026_04_22() -> None:
    """The exact production-failure case: Flash CCI for 2026-04-22.

    The live listing carries this row inside an ``ecl-file`` widget;
    the resolver must reach the title via the anchor's
    ``data-untranslated-label`` (or via the ``ecl-file`` container
    walk-up) instead of the narrow ``ecl-file__action`` div.
    """
    resolved = resolve_press_release_link(
        _live_html(),
        series_id="EC_BCS_CCI_FLASH",
        release_date=date(2026, 4, 22),
    )
    assert "11ffc7fa-f14b-4ed7-a44c-2e45fa85fec5_en" in resolved.source_url
    assert resolved.release_date == "2026-04-22"


def test_resolves_esi_on_live_listing_for_2026_03_30() -> None:
    """Same widget, different label pattern — ESI publishes as
    "Press release business and consumer survey results (incl. ESI ...)
    - 30 March 2026"."""
    resolved = resolve_press_release_link(
        _live_html(),
        series_id="EC_BCS_ESI",
        release_date=date(2026, 3, 30),
    )
    assert "d2316c53-1c0a-4350-b077-e4523fc4d08b_en" in resolved.source_url


def test_does_not_pick_statistical_annex_for_esi_on_live_listing() -> None:
    """The 2026-03-30 ESI press release sits next to a "Statistical
    annex to press release - 30 March 2026" PDF that has the same
    date. The label pattern must keep the resolver from matching the
    annex (which lacks "business and consumer survey results")."""
    resolved = resolve_press_release_link(
        _live_html(),
        series_id="EC_BCS_ESI",
        release_date=date(2026, 3, 30),
    )
    assert "statistical_annex" not in resolved.source_url.lower()


def test_resolves_flash_cci_on_live_listing_for_2026_03_23() -> None:
    """A second Flash CCI on the same fixture, different month —
    confirms the month-discriminator works against the new widget."""
    resolved = resolve_press_release_link(
        _live_html(),
        series_id="EC_BCS_CCI_FLASH",
        release_date=date(2026, 3, 23),
    )
    assert "ef5fc817-8dda-4af0-b7db-90d8b50c8e11_en" in resolved.source_url


def test_raises_on_live_listing_when_release_date_absent() -> None:
    """The loud-fail path on the live widget — a release date the
    listing doesn't carry must still raise rather than silently
    misalign onto a different month."""
    with pytest.raises(EcBcsScheduleParseError, match="2099-01-01"):
        resolve_press_release_link(
            _live_html(),
            series_id="EC_BCS_CCI_FLASH",
            release_date=date(2099, 1, 1),
        )


def test_metadata_date_wins_over_typo_in_card_title() -> None:
    """The Jan-2026 Flash CCI card on the live page carries the EU's
    own typo: title says ``"22 January 2025"`` but the metadata says
    ``"22 JANUARY 2026"``. A backfill query for 2025-01-22 must NOT
    match this card just because its title text contains "January 2025"
    — metadata is the source of truth.
    """
    # 2026-01-22 hits the actual card.
    resolved = resolve_press_release_link(
        _live_html(),
        series_id="EC_BCS_CCI_FLASH",
        release_date=date(2026, 1, 22),
    )
    assert "Flash_consumer_2026_01_en" in resolved.source_url

    # 2025-01-22 must NOT pick up the typo card — there is no real
    # 2025-01-22 entry on this listing, so the resolver must raise.
    with pytest.raises(EcBcsScheduleParseError):
        resolve_press_release_link(
            _live_html(),
            series_id="EC_BCS_CCI_FLASH",
            release_date=date(2025, 1, 22),
        )
