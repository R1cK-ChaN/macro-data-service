# US Source Mapping Implementation Notes

Date: 2026-05-01
Issue: #112
Slices: EODHD market-lane MOVE/GBOND mappings; FRED WEI/HVS mappings; BIS Total Credit mappings; NY Fed GSCPI mapping; Bundesbank Germany yield mappings; Japan MOF JGB yield mappings; AISI weekly steel mappings; ISM official PMI report mappings; Redbook weekly retail-sales mappings

The inspected US workbook is a coverage sample for finding omitted fields. Source
grouping here follows the indicator's economic meaning, official publisher,
frequency, and instrument type.

## Implemented Rows

The EODHD market universe now includes the quote-style rows from the first
implementation slice.

| Indicator/source group | EODHD ticker | Instrument id | Asset class |
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

| Indicator/source group | FRED series | Concept id | Obs family |
| --- | --- | --- | --- |
| Weekly Economic Index | `WEI` | `WEI_US` | `us.growth.weekly_economic_index` |
| Homeownership rate | `RHORUSQ156N` | `HOMEOWNERSHIP_RATE_US` | `us.housing.homeownership_rate` |
| Rental vacancy rate | `RRVRUSQ156N` | `RENTAL_VACANCY_RATE_US` | `us.housing.rental_vacancy_rate` |
| Homeowner vacancy rate | `RHVRUSQ156N` | `HOMEOWNER_VACANCY_RATE_US` | `us.housing.homeowner_vacancy_rate` |

Focused tests cover FRED config presence, obs-family seeding, concept-map
seeding, and FRED source discovery for the HVS and WEI rows.

## BIS Total Credit Rows

| Indicator/source group | BIS dataflow | BIS key | Series id | Concept id | Obs family |
| --- | --- | --- | --- | --- | --- |
| General government leverage | `WS_TC` v2.0 | `Q.US.G.A.N.770.A` | `BIS_TC_GOV_US` | `GOV_LEVERAGE_US` | `us.credit.gov_leverage` |
| Household leverage | `WS_TC` v2.0 | `Q.US.H.A.M.770.A` | `BIS_TC_HH_US` | `HOUSEHOLD_LEVERAGE_US` | `us.credit.household_leverage` |
| Non-financial corporation leverage | `WS_TC` v2.0 | `Q.US.N.A.M.770.A` | `BIS_TC_NFC_US` | `NFC_LEVERAGE_US` | `us.credit.nfc_leverage` |

Focused tests cover BIS config presence, obs-family seeding, concept-map
seeding, and release-schedule seeding for the sector leverage rows.

## NY Fed GSCPI Row

| Indicator/source group | Source file | Series id | Concept id | Obs family |
| --- | --- | --- | --- | --- |
| Global Supply Chain Pressure Index | NY Fed `gscpi_data.xlsx` | `NYFED_GSCPI` | `GSCPI_US` | `us.supply_chain.gscpi` |

Focused tests cover legacy Excel and OOXML parsing, fetcher output,
obs-family seeding, concept-map seeding, release-schedule seeding, source
discovery, subject alias resolution, US holiday handling, and NY release-time
conversion for the GSCPI row.

## Bundesbank Germany Yield Rows

| Indicator/source group | Bundesbank dataflow | Bundesbank key | Series id | Concept id | Obs family |
| --- | --- | --- | --- | --- | --- |
| Germany 2Y federal securities yield | `BBSSY` | `D.REN.EUR.A610.000000WT0202.A` | `BUNDESBANK_DE_GOVT_2Y` | `DE_GOVT_2Y` | `de.rates.govt_2y` |
| Germany 5Y federal securities yield | `BBSSY` | `D.REN.EUR.A620.000000WT0505.A` | `BUNDESBANK_DE_GOVT_5Y` | `DE_GOVT_5Y` | `de.rates.govt_5y` |
| Germany 7Y federal securities yield | `BBSSY` | `D.REN.EUR.A607.000000WT7070.A` | `BUNDESBANK_DE_GOVT_7Y` | `DE_GOVT_7Y` | `de.rates.govt_7y` |
| Germany 10Y federal securities yield | `BBSSY` | `D.REN.EUR.A630.000000WT1010.A` | `BUNDESBANK_DE_GOVT_10Y` | `DE_GOVT_10Y` | `de.rates.govt_10y` |
| Germany 15Y federal securities yield | `BBSSY` | `D.REN.EUR.A615.000000WT1515.A` | `BUNDESBANK_DE_GOVT_15Y` | `DE_GOVT_15Y` | `de.rates.govt_15y` |
| Germany 30Y federal securities yield | `BBSSY` | `D.REN.EUR.A640.000000WT3030.A` | `BUNDESBANK_DE_GOVT_30Y` | `DE_GOVT_30Y` | `de.rates.govt_30y` |

Focused tests cover official key config, Bundesbank SDMX Accept-header and
query parameters, fetcher output, SDMX raw snapshot hashing, obs-family seeding,
concept-map seeding, release-schedule seeding, subject alias resolution, source
discovery, and the orchestrator save path into `indicators` and `obs_raw`.

## Japan MOF JGB Yield Rows

| Indicator/source group | MOF CSV column | Series id | Concept id | Obs family |
| --- | --- | --- | --- | --- |
| Japan 1Y government yield | `1Y` | `MOF_JP_GOVT_1Y` | `JP_GOVT_1Y` | `jp.rates.govt_1y` |
| Japan 2Y government yield | `2Y` | `MOF_JP_GOVT_2Y` | `JP_GOVT_2Y` | `jp.rates.govt_2y` |
| Japan 3Y government yield | `3Y` | `MOF_JP_GOVT_3Y` | `JP_GOVT_3Y` | `jp.rates.govt_3y` |
| Japan 4Y government yield | `4Y` | `MOF_JP_GOVT_4Y` | `JP_GOVT_4Y` | `jp.rates.govt_4y` |
| Japan 5Y government yield | `5Y` | `MOF_JP_GOVT_5Y` | `JP_GOVT_5Y` | `jp.rates.govt_5y` |
| Japan 6Y government yield | `6Y` | `MOF_JP_GOVT_6Y` | `JP_GOVT_6Y` | `jp.rates.govt_6y` |
| Japan 7Y government yield | `7Y` | `MOF_JP_GOVT_7Y` | `JP_GOVT_7Y` | `jp.rates.govt_7y` |
| Japan 8Y government yield | `8Y` | `MOF_JP_GOVT_8Y` | `JP_GOVT_8Y` | `jp.rates.govt_8y` |
| Japan 9Y government yield | `9Y` | `MOF_JP_GOVT_9Y` | `JP_GOVT_9Y` | `jp.rates.govt_9y` |
| Japan 10Y government yield | `10Y` | `MOF_JP_GOVT_10Y` | `JP_GOVT_10Y` | `jp.rates.govt_10y` |
| Japan 15Y government yield | `15Y` | `MOF_JP_GOVT_15Y` | `JP_GOVT_15Y` | `jp.rates.govt_15y` |
| Japan 20Y government yield | `20Y` | `MOF_JP_GOVT_20Y` | `JP_GOVT_20Y` | `jp.rates.govt_20y` |
| Japan 25Y government yield | `25Y` | `MOF_JP_GOVT_25Y` | `JP_GOVT_25Y` | `jp.rates.govt_25y` |
| Japan 30Y government yield | `30Y` | `MOF_JP_GOVT_30Y` | `JP_GOVT_30Y` | `jp.rates.govt_30y` |
| Japan 40Y government yield | `40Y` | `MOF_JP_GOVT_40Y` | `JP_GOVT_40Y` | `jp.rates.govt_40y` |

Focused tests cover official CSV parsing, client raw payloads, fetcher output,
raw snapshot hashing, obs-family seeding, concept-map seeding, release-schedule
seeding, subject alias resolution, source discovery, country-scoped validation,
and the orchestrator save path into `indicators` and `obs_raw`.

## AISI Weekly Steel Rows

| Indicator/source group | AISI page metric | Series id | Concept id | Obs family |
| --- | --- | --- | --- | --- |
| US raw steel production weekly value | `production_net_tons` | `AISI_RAW_STEEL_PRODUCTION_US` | `RAW_STEEL_PRODUCTION_US` | `us.industry.raw_steel_production` |
| US raw steel production WoW | `wow_percent` | `AISI_RAW_STEEL_WOW_US` | `RAW_STEEL_PRODUCTION_WOW_US` | `us.industry.raw_steel_production_wow` |
| US raw steel production YoY | `yoy_percent` | `AISI_RAW_STEEL_YOY_US` | `RAW_STEEL_PRODUCTION_YOY_US` | `us.industry.raw_steel_production_yoy` |

Focused tests cover official page parsing, client raw payloads, fetcher output,
raw snapshot hashing, obs-family seeding, concept-map seeding, release-schedule
seeding, subject alias resolution, source discovery, country-scoped validation,
US-holiday-aware weekly release timing, and the orchestrator save path into
`indicators` and `obs_raw`.

## ISM Official PMI Report Rows

| Indicator/source group | ISM report metric group | Series id pattern | Concept id pattern | Obs family pattern |
| --- | --- | --- | --- | --- |
| Manufacturing PMI headline and point change | Manufacturing PMI | `ISM_MFG_PMI_*` | `ISM_MFG_PMI_*` | `us.growth.ism_mfg_pmi*` |
| Manufacturing subcomponent indexes and point changes | New Orders, Production, Employment, Supplier Deliveries, Inventories, Customers' Inventories, Prices, Backlog of Orders, New Export Orders, Imports | `ISM_MFG_<METRIC>_*` | `ISM_MFG_<METRIC>_*` | `us.growth.ism_mfg_<metric>*` |
| Services PMI headline and point change | Services PMI | `ISM_SERVICES_PMI_*` | `ISM_SERVICES_PMI_*` | `us.growth.ism_services_pmi*` |
| Services subcomponent indexes and point changes | Business Activity, New Orders, Employment, Supplier Deliveries, Inventories, Prices, Backlog of Orders, New Export Orders, Imports, Inventory Sentiment | `ISM_SERVICES_<METRIC>_*` | `ISM_SERVICES_<METRIC>_*` | `us.growth.ism_services_<metric>*` |

Focused tests cover official Manufacturing and Services report parsing, current
report discovery, client raw payloads, fetcher output, raw snapshot hashing,
obs-family seeding, concept-map seeding, release-schedule seeding, subject alias
resolution, source discovery, country-scoped validation, and the orchestrator
save path into `indicators` and `obs_raw`.

## Redbook Weekly Retail Sales Row

| Indicator/source group | Authorized feed | Source symbol | Series id | Concept id | Obs family |
| --- | --- | --- | --- | --- | --- |
| US Redbook weekly retail-sales YoY | Trading Economics historical API with Redbook Research source attribution | `UNITEDSTAREDIND` | `REDBOOK_RETAIL_SALES_YOY_US` | `REDBOOK_RETAIL_SALES_YOY_US` | `us.consumer.redbook_retail_sales_yoy` |

Focused tests cover historical-feed parsing, client raw payloads, fetcher
output, raw snapshot hashing, obs-family seeding, concept-map seeding,
release-schedule seeding, subject alias resolution, source discovery,
country-scoped validation, New York release-time conversion, and the
orchestrator save path into `indicators` and `obs_raw`.

Remaining source groups for later slices: Sentix US, SOFR-OIS, and exact
Wind/Bloomberg parity.
