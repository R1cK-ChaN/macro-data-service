"""Indicator-name canonicalisation.

Convert an upstream release title into a short canonical token. Two
sources publishing the same event under different labels normalize to
the same string so ``synthesize_event_id`` yields identical ids and the
parity harness (P6) can match BLS/BEA rows to their TE counterparts.

Matching is alias-table-based, not fuzzy. Adding a new alias is a
one-liner — we'd rather miss a match and fix it than silently collapse
two different indicators into one token.

Canonicalisation rules, applied in order:

1. Lowercase, strip, collapse internal whitespace, drop parenthesised
   qualifiers (``"Consumer Price Index (CPI)"`` → ``"consumer price index"``).
2. Strip common suffixes that are structural-modifier noise at the
   aggregator level (``" yoy"``, ``" mom"``, ``" qoq"``, ``" sa"``,
   ``" nsa"``). Connectors that care about the modifier keep it in
   their own typed field; the canonical token is the indicator family.
3. Alias-table lookup. First-hit wins. Unknown input falls through
   unchanged — the canonical function is total, never raises.

The alias set is deliberately small in P0. Per-phase connectors extend
it as live-validation surfaces the upstream labels they actually see.
"""

from __future__ import annotations

import re

_WHITESPACE_RE = re.compile(r"\s+")
_PARENS_RE = re.compile(r"\s*\([^)]*\)")

# Modifier suffixes that describe the *transformation* of an indicator,
# not the indicator itself. Stripped before alias lookup so
# "CPI YoY" / "CPI MoM" / "CPI" all normalize to "CPI".
_MODIFIER_SUFFIXES: tuple[str, ...] = (
    " year-on-year",
    " year over year",
    " month-on-month",
    " month over month",
    " quarter-on-quarter",
    " quarter over quarter",
    " yoy",
    " mom",
    " qoq",
    " sa",
    " nsa",
    " (sa)",
    " (nsa)",
    " annualized",
    " annualised",
    " adv",
    " advance",
    " prel",
    " prelim",
    " preliminary",
    " final",
    " flash",
    " revised",
    # Fed FOMC rows stored with ``has_sep=True`` carry the
    # "+ SEP" marker on ``title``; strip it before alias lookup so
    # quarterly projection-materials meetings still resolve to
    # ``FOMC_RATE``.
    " + sep",
)

# Dash characters that appear in provider titles but must not affect
# alias matching. Em-dash (U+2014) and en-dash (U+2013) show up in BLS
# report titles ("Consumer Price Index — All Items Less Food and
# Energy") and in some Fed labels; non-breaking hyphen (U+2011) shows
# up where providers protect phrases from line-breaks. Normalized to a
# plain space so whitespace collapse yields a predictable form.
_DASH_CHARS = ("—", "–", "‑", " - ")

# Alias → canonical token. Keys are the post-step-1/2 normalized string
# (lowercase, whitespace-collapsed, modifier-stripped). Values are the
# canonical tokens used in provider_event_id synthesis.
#
# Small and conservative by design. Each official-source connector
# extends this when its live probe surfaces a new upstream label. A
# missing alias is recoverable (add and re-run); a wrong one silently
# merges two indicators, which is much worse.
_ALIASES: dict[str, str] = {
    # ── US prices ────────────────────────────────────────────────
    "cpi": "CPI",
    "consumer price index": "CPI",
    "consumer price index for all urban consumers": "CPI",
    "cpi-u": "CPI",
    "inflation rate": "CPI",
    "core cpi": "CORE_CPI",
    "consumer price index all items less food and energy": "CORE_CPI",
    "cpi less food and energy": "CORE_CPI",
    "cpi ex food and energy": "CORE_CPI",
    "core inflation rate": "CORE_CPI",
    "ppi": "PPI",
    "producer price index": "PPI",
    "producer price index final demand": "PPI",
    "producer price index for final demand": "PPI",
    "core ppi": "CORE_PPI",
    "ppi less food and energy": "CORE_PPI",
    # BLS long-form Core PPI title (after dash → space normalization)
    "producer price index final demand less foods, energy, and trade services": "CORE_PPI",
    # ── US labour ────────────────────────────────────────────────
    "employment situation": "NFP",
    "nonfarm payrolls": "NFP",
    "nonfarm payroll employment": "NFP",
    "non-farm payrolls": "NFP",
    "non farm payrolls": "NFP",           # TE title
    "nfp": "NFP",
    "unemployment rate": "UNEMPLOYMENT_RATE",
    "average hourly earnings": "AHE",
    "average hourly earnings total private": "AHE",   # BLS long-form title
    "average weekly hours": "AWH",
    "average weekly hours total private": "AWH",      # BLS long-form title
    "jolts": "JOLTS",
    "job openings": "JOLTS",
    "job openings and labor turnover survey": "JOLTS",
    "employment cost index": "ECI",
    # BLS long-form ECI title (parens stripped before lookup)
    "employment cost index all civilian workers, total compensation": "ECI",
    "productivity": "PRODUCTIVITY",
    "productivity and costs": "PRODUCTIVITY",
    "initial jobless claims": "JOBLESS_CLAIMS",
    "weekly jobless claims": "JOBLESS_CLAIMS",
    "unemployment insurance weekly claims": "JOBLESS_CLAIMS",
    # ── US growth / BEA ─────────────────────────────────────────
    "gdp": "GDP",
    "gross domestic product": "GDP",
    "gdp growth rate": "GDP",
    "real gdp": "GDP",
    "real gross domestic product": "GDP",
    "personal income and outlays": "PERSONAL_INCOME",
    "personal income": "PERSONAL_INCOME",
    "personal consumption expenditures": "PCE",
    "pce": "PCE",
    "core pce": "CORE_PCE",
    "pce ex food and energy": "CORE_PCE",
    "international trade in goods and services": "TRADE_BALANCE",
    "trade balance": "TRADE_BALANCE",
    "corporate profits": "CORPORATE_PROFITS",
    "retail sales": "RETAIL_SALES",
    "retail sales m/m": "RETAIL_SALES",
    "advance monthly sales for retail and food services": "RETAIL_SALES",
    "durable goods orders": "DURABLE_GOODS_ORDERS",
    "durable goods orders m/m": "DURABLE_GOODS_ORDERS",
    "advance report on durable goods": "DURABLE_GOODS_ORDERS",
    "housing starts": "HOUSING_STARTS",
    "new residential construction": "HOUSING_STARTS",
    "building permits": "BUILDING_PERMITS",
    # ── Central banks ───────────────────────────────────────────
    "fomc meeting": "FOMC_RATE",
    "fomc rate decision": "FOMC_RATE",
    "federal funds target rate": "FOMC_RATE",
    "interest rate decision": "FOMC_RATE",
    "fed interest rate decision": "FOMC_RATE",        # TE title for FOMC
    "mro rate": "ECB_MRO",
    "main refinancing operations rate": "ECB_MRO",
    "ecb main refinancing operations rate": "ECB_MRO",
    "ecb interest rate decision": "ECB_MP_DECISION",   # TE title for ECB decision event (not rate value)
    "ecb_mro": "ECB_MRO",
    "deposit facility rate": "ECB_DFR",
    "ecb deposit facility rate": "ECB_DFR",
    "dfr": "ECB_DFR",
    "ecb_dfr": "ECB_DFR",
    "marginal lending facility rate": "ECB_MLF",
    "ecb marginal lending facility rate": "ECB_MLF",
    "marginal lending rate": "ECB_MLF",               # TE title for ECB MLF
    "mlf": "ECB_MLF",
    "ecb_mlf": "ECB_MLF",
    "ecb economic bulletin": "ECB_BULLETIN",
    "ecb monetary policy decision": "ECB_MP_DECISION",
    "monetary policy meeting": "ECB_MP_DECISION",
    "governing council monetary policy meeting": "ECB_MP_DECISION",
    "monetary policy statement": "ECB_PRESS_CONF",
    "beige book": "BEIGE_BOOK",
    "h.4.1": "FED_H41",
    "h.8": "FED_H8",
    "summary of economic projections": "FED_SEP",
    "sep": "FED_SEP",
    # ── China (NBS) ─────────────────────────────────────────────
    "industrial production": "INDUSTRIAL_PRODUCTION",
    "fixed asset investment": "FIXED_ASSET_INVESTMENT",
    "manufacturing pmi": "MFG_PMI",
    "non-manufacturing pmi": "NON_MFG_PMI",
    "ism manufacturing pmi": "MFG_PMI",
    "nbs manufacturing pmi": "MFG_PMI",
    "nbs non-manufacturing pmi": "NON_MFG_PMI",
    "michigan consumer sentiment": "MICHIGAN_SENTIMENT",
    "university of michigan consumer sentiment": "MICHIGAN_SENTIMENT",
    "uom consumer sentiment": "MICHIGAN_SENTIMENT",
    "prelim uom consumer sentiment": "MICHIGAN_SENTIMENT",
    "revised uom consumer sentiment": "MICHIGAN_SENTIMENT",
    "cb consumer confidence": "CB_CONSUMER_CONFIDENCE",
    "consumer confidence": "CB_CONSUMER_CONFIDENCE",
    "consumer confidence index": "CB_CONSUMER_CONFIDENCE",
    "conference board consumer confidence": "CB_CONSUMER_CONFIDENCE",
    "cb leading index": "CB_LEADING_INDEX",
    "leading index": "CB_LEADING_INDEX",
    "leading economic index": "CB_LEADING_INDEX",
    "conference board leading economic index": "CB_LEADING_INDEX",
    "existing home sales": "EXISTING_HOME_SALES",
    "existing-home sales": "EXISTING_HOME_SALES",
    "pending home sales": "PENDING_HOME_SALES",
    "pending home sales index": "PENDING_HOME_SALES",
    # NBS's ``cal_econ_event.title`` carries a ``"China "`` country
    # prefix on every indicator (so a multi-country display surface
    # can tell the origin at a glance). The parity harness
    # canonicalizes every row's title; without these aliases each
    # NBS indicator's events would bucket separately from TE's rows
    # for the same indicator, surfacing as spurious official-only
    # gaps even when reference dates align.
    "china gdp": "GDP",
    "china consumer price index": "CPI",
    "china producer price index": "PPI",
    "china industrial production": "INDUSTRIAL_PRODUCTION",
    "china fixed asset investment": "FIXED_ASSET_INVESTMENT",
    "china retail sales": "RETAIL_SALES",
    "china manufacturing pmi": "MFG_PMI",
    "china non-manufacturing pmi": "NON_MFG_PMI",
}


def canonicalize_indicator(label: str) -> str:
    """Normalize ``label`` to a canonical indicator token.

    Unknown inputs pass through as the normalized form (lowercase, no
    parens, collapsed whitespace) so a later alias addition doesn't
    invalidate previously-synthesised event ids — the un-aliased form
    is already stable.

    Parameters
    ----------
    label:
        Upstream release title. May be empty.

    Returns
    -------
    str
        Canonical token (``"CPI"``, ``"NFP"``, …) for aliased inputs,
        or the normalized un-aliased form for everything else. Empty
        input returns empty string.
    """
    if not label:
        return ""
    text = _PARENS_RE.sub("", label).strip().lower()
    # Replace em-dash / en-dash / non-breaking hyphen with a space so
    # the whitespace collapse below yields a predictable surface for
    # alias lookup regardless of which dash glyph the upstream used.
    for dash in _DASH_CHARS:
        text = text.replace(dash, " ")
    text = _WHITESPACE_RE.sub(" ", text).strip()

    # Strip modifier suffixes iteratively — "CPI YoY SA" → "CPI".
    changed = True
    while changed:
        changed = False
        for suffix in _MODIFIER_SUFFIXES:
            if text.endswith(suffix):
                text = text[: -len(suffix)].rstrip()
                changed = True
                break

    return _ALIASES.get(text, text)
