# US Workbook Source Mapping Implementation Notes

Date: 2026-05-01
Issue: #112
Slice: EODHD market-lane MOVE and GBOND mappings

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

Remaining source families for later slices: FRED/Census WEI and HVS fields,
BIS Total Credit leverage ratios, NY Fed GSCPI, Bundesbank official yields,
Japan MOF official yields, AISI weekly steel, ISM subcomponents, Redbook,
Sentix US, SOFR-OIS, and exact Wind/Bloomberg parity.
