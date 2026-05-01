# US Workbook Source Mapping Implementation Notes

Date: 2026-05-01
Issue: #112
Slices: EODHD market-lane MOVE/GBOND mappings; FRED WEI/HVS mappings; BIS Total Credit mappings

## Implemented Rows

The EODHD market universe now includes the quote-style rows from the first
implementation slice.

| Workbook family | EODHD ticker | Instrument id | Asset class |
| --- | --- | --- | --- |
| MOVE index | `MOVE.INDX` | `US_MOVE` | `index` |
| China 10Y government yield | `CN10Y.GBOND` | `RATES_CN_10Y_GBOND` | `rate` |
| Germany 2Y government yield | `DE2Y.GBOND` | `RATES_DE_2Y_GBOND` | `rate` |
| Germany 5Y government yield | `DE5Y.GBOND` | `RATES_DE_5Y_GBOND` | `rate` |
| Germany 10Y government yield | `DE10Y.GBOND` | `RATES_DE_10Y_GBOND` | `rate` |
| Germany 30Y government yield | `DE30Y.GBOND` | `RATES_DE_30Y_GBOND` | `rate` |
| Japan 2Y government yield | `JP2Y.GBOND` | `RATES_JP_2Y_GBOND` | `rate` |
| Japan 3Y government yield | `JP3Y.GBOND` | `RATES_JP_3Y_GBOND` | `rate` |
| Japan 5Y government yield | `JP5Y.GBOND` | `RATES_JP_5Y_GBOND` | `rate` |
| Japan 10Y government yield | `JP10Y.GBOND` | `RATES_JP_10Y_GBOND` | `rate` |
| Japan 30Y government yield | `JP30Y.GBOND` | `RATES_JP_30Y_GBOND` | `rate` |

## Validation

Focused tests cover universe seeding, ticker lookup, duplicate ticker guards,
macro-lane instrument-id separation, clean quality flags for GBOND rate bars,
quote-only MOVE handling, rate-aware zero/negative yield handling, and
percent-formatted rate summaries.

## FRED WEI/HVS Rows

| Workbook family | FRED series | Concept id | Obs family |
| --- | --- | --- | --- |
| Weekly Economic Index | `WEI` | `WEI_US` | `us.growth.weekly_economic_index` |
| Homeownership rate | `RHORUSQ156N` | `HOMEOWNERSHIP_RATE_US` | `us.housing.homeownership_rate` |
| Rental vacancy rate | `RRVRUSQ156N` | `RENTAL_VACANCY_RATE_US` | `us.housing.rental_vacancy_rate` |
| Homeowner vacancy rate | `RHVRUSQ156N` | `HOMEOWNER_VACANCY_RATE_US` | `us.housing.homeowner_vacancy_rate` |

Focused tests cover FRED config presence, obs-family seeding, concept-map
seeding, and FRED source discovery for the workbook HVS and WEI rows.

## BIS Total Credit Rows

| Workbook family | BIS dataflow | BIS key | Series id | Concept id | Obs family |
| --- | --- | --- | --- | --- | --- |
| General government leverage | `WS_TC` v2.0 | `Q.US.G.A.N.770.A` | `BIS_TC_GOV_US` | `GOV_LEVERAGE_US` | `us.credit.gov_leverage` |
| Household leverage | `WS_TC` v2.0 | `Q.US.H.A.M.770.A` | `BIS_TC_HH_US` | `HOUSEHOLD_LEVERAGE_US` | `us.credit.household_leverage` |
| Non-financial corporation leverage | `WS_TC` v2.0 | `Q.US.N.A.M.770.A` | `BIS_TC_NFC_US` | `NFC_LEVERAGE_US` | `us.credit.nfc_leverage` |

Focused tests cover BIS config presence, obs-family seeding, concept-map
seeding, and release-schedule seeding for the workbook sector leverage rows.

Remaining source families for later slices: NY Fed GSCPI, Bundesbank official
yields, Japan MOF official yields, AISI weekly steel, ISM subcomponents,
Redbook, Sentix US, SOFR-OIS, and exact Wind/Bloomberg parity.
