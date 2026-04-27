"""TÜİK indicator whitelist — issue #86 P1.

Six headline Turkish indicators ship in P1, all served by the unified
national release-calendar JSON at
``www.tuik.gov.tr/Kurumsal/GetYillikHaberBulteniListesi?yil=YYYY``.
Each row carries an ``adi`` (release title in Turkish) whose exact-
prefix the matcher tests against ``adi_prefixes`` to identify the
indicator. Exact prefix rather than substring sidesteps the
``İşgücü İstatistikleri`` vs ``Tarımsal İşletme İşgücü`` collision —
both rows contain the substring ``"İşgücü"`` but only the first is
the headline labour-force release.

- **CPI (TÜFE)** — Tüketici Fiyat Endeksi (TÜFE). The headline
  consumer-price release; published on the 3rd of each month at
  10:00 TRT.
- **PPI (Yİ-ÜFE / D-PPI)** — Yurt İçi Üretici Fiyat Endeksi. The
  domestic producer-price release; published alongside CPI on the
  3rd of each month.
- **GDP** — Dönemsel Gayrisafi Yurt İçi Hasıla, the quarterly
  national-accounts release. TÜİK also publishes annual GDP
  (``Yıllık Gayrisafi Yurt İçi Hasıla``) — that distinct annual
  release is out of scope for the headline quarterly bucket here.
- **INDUSTRIAL_PRODUCTION** — Sanayi Üretim Endeksi. Monthly
  industrial-production index, typically lag-2 to data month.
- **UNEMPLOYMENT_RATE** — İşgücü İstatistikleri. The monthly labour-
  force survey; the matcher anchors on the exact prefix to skip the
  agriculture-specific ``Tarımsal İşletme İşgücü Ücret Yapısı``
  variant that shares the ``İşgücü`` token.
- **TRADE_BALANCE** — Dış Ticaret İstatistikleri. The monthly
  foreign-trade headline; ``Dış Ticaret Endeksleri`` (price/volume
  indices) is a distinct release excluded from this bucket.

The schedule-only slice publishes events with ``actual=NULL``. TÜİK's
press-release pages reachable from each row's ``link`` carry the
value side; per-indicator value extraction (CPI MoM, PPI MoM, IP
index, unemployment rate, GDP QoQ %, trade balance USD) is deferred
to P2 alongside that detail-page scrape.

Default release time per indicator follows TÜİK's ``gTarih`` field —
nearly all releases publish at 10:00 TRT. The
``release_time_local`` field allows a per-indicator override if a
future indicator ships at a different hour.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TUIKIndicatorSpec:
    """Downstream-shape metadata for one TÜİK calendar indicator."""

    indicator: str           # canonical token ("CPI")
    country_code: str        # always "TR" for TÜİK
    title: str               # cal_econ_event.title
    unit: str                # cal_econ_event.unit (placeholder for schedule-only slice)
    importance: str
    category: str
    adi_prefixes: tuple[str, ...]  # exact-prefix match against TÜİK's ``adi`` field
    frequency: str           # "monthly" / "quarterly"
    release_time_local: str  # TRT wall-clock release time ("10:00")


# Anchored on exact ``adi`` prefix. Substring matching collides
# (``İşgücü`` matches both the headline and the agricultural
# variant); a stable prefix-set is the cleanest disambiguation.
INDICATOR_REGISTRY: dict[str, TUIKIndicatorSpec] = {
    "CPI": TUIKIndicatorSpec(
        indicator="CPI",
        country_code="TR",
        title="Turkey Consumer Price Index",
        unit="index",
        importance="high",
        category="Prices",
        adi_prefixes=("Tüketici Fiyat Endeksi (TÜFE)",),
        frequency="monthly",
        release_time_local="10:00",
    ),
    "PPI": TUIKIndicatorSpec(
        indicator="PPI",
        country_code="TR",
        title="Turkey Producer Price Index",
        unit="index",
        importance="high",
        category="Prices",
        adi_prefixes=("Yurt İçi Üretici Fiyat Endeksi",),
        frequency="monthly",
        release_time_local="10:00",
    ),
    "GDP": TUIKIndicatorSpec(
        indicator="GDP",
        country_code="TR",
        title="Turkey GDP",
        unit="index",
        importance="high",
        category="Growth",
        adi_prefixes=("Dönemsel Gayrisafi Yurt İçi Hasıla",),
        frequency="quarterly",
        release_time_local="10:00",
    ),
    "INDUSTRIAL_PRODUCTION": TUIKIndicatorSpec(
        indicator="INDUSTRIAL_PRODUCTION",
        country_code="TR",
        title="Turkey Industrial Production",
        unit="index",
        importance="high",
        category="Production",
        adi_prefixes=("Sanayi Üretim Endeksi",),
        frequency="monthly",
        release_time_local="10:00",
    ),
    "UNEMPLOYMENT_RATE": TUIKIndicatorSpec(
        indicator="UNEMPLOYMENT_RATE",
        country_code="TR",
        title="Turkey Unemployment Rate",
        unit="percent",
        importance="high",
        category="Labor",
        adi_prefixes=("İşgücü İstatistikleri",),
        frequency="monthly",
        release_time_local="10:00",
    ),
    "TRADE_BALANCE": TUIKIndicatorSpec(
        indicator="TRADE_BALANCE",
        country_code="TR",
        title="Turkey Balance of Trade",
        unit="USD",
        importance="high",
        category="Trade",
        adi_prefixes=("Dış Ticaret İstatistikleri",),
        frequency="monthly",
        release_time_local="10:00",
    ),
}


__all__ = ["INDICATOR_REGISTRY", "TUIKIndicatorSpec"]
