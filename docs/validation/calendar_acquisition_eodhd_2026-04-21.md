# EODHD Calendar Acquisition Validation — 2026-04-21

Scope: verifies the **acquisition** step only (fetch + parse).
Storage and downstream API are under our control and may be adjusted once upstream shape is understood correctly.

## Budget

- Requests spent this run: **9**
- EODHD All-in-One plan: per-call consumption (no tight cap)
- Probes planned: 9 / executed: 9

## Probes

### Probe 1 — `earnings_date_window`

- Purpose: earnings shape over a week-ahead window
- Expected shape: `{earnings: [row, row, ...]}`
- Request path: `/api/calendar/earnings?from=2026-04-21&to=2026-04-28`
- Status: **ok**
- HTTP elapsed: 3044 ms
- Row count: 7546

#### Field diff (first row)

- Observed: `actual`, `before_after_market`, `code`, `currency`, `date`, `difference`, `estimate`, `percent`, `report_date`
- Read by parser: `actual`, `before_after_market`, `code`, `currency`, `date`, `difference`, `estimate`, `percent`, `report_date`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `before_after_market`: 'BeforeMarket'=3545, 'AfterMarket'=3526, None=475
- `currency`: None=6950, 'USD'=541, 'EUR'=17, 'CNY'=5, 'SEK'=5, 'CAD'=4, 'MXN'=4, 'CHF'=3 (+11 more values)

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "actual": null,
  "before_after_market": "BeforeMarket",
  "code": "5UR.F",
  "currency": null,
  "date": "2026-03-31",
  "difference": 0,
  "estimate": 1.52,
  "percent": null,
  "report_date": "2026-04-21"
}
```

</details>

### Probe 2 — `earnings_symbols`

- Purpose: earnings scoped to AAPL.US + MSFT.US
- Expected shape: `{earnings: [row, row, ...]}`
- Request path: `/api/calendar/earnings?symbols=AAPL.US,MSFT.US`
- Status: **ok**
- HTTP elapsed: 1426 ms
- Row count: 0

### Probe 3 — `ipos_date_window`

- Purpose: IPO shape + deal_type vocabulary over 30-day window
- Expected shape: `{ipos: [row, row, ...]}`
- Request path: `/api/calendar/ipos?from=2026-04-21&to=2026-05-21`
- Status: **ok**
- HTTP elapsed: 1323 ms
- Row count: 9

#### Field diff (first row)

- Observed: `amended_date`, `code`, `currency`, `deal_type`, `exchange`, `filing_date`, `name`, `offer_price`, `price_from`, `price_to`, `shares`, `start_date`
- Read by parser: `amended_date`, `code`, `currency`, `deal_type`, `exchange`, `filing_date`, `name`, `offer_price`, `price_from`, `price_to`, `shares`, `start_date`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `deal_type`: 'Expected'=9
- `exchange`: 'NASDAQ'=6, 'NYSE'=3
- `currency`: None=9

#### Parser dry-parse: 9/9 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "amended_date": "2026-04-22",
  "code": "EMI.US",
  "currency": null,
  "deal_type": "Expected",
  "exchange": "NYSE",
  "filing_date": "2026-04-22",
  "name": "Eaton Vance Michigan Municipal Income Trust",
  "offer_price": 0,
  "price_from": 0,
  "price_to": 0,
  "shares": 3000000,
  "start_date": "2026-04-22"
}
```

</details>

### Probe 4 — `splits_date_window`

- Purpose: split shape over 30-day window
- Expected shape: `{splits: [row, row, ...]}`
- Request path: `/api/calendar/splits?from=2026-04-21&to=2026-05-21`
- Status: **ok**
- HTTP elapsed: 1355 ms
- Row count: 225

#### Field diff (first row)

- Observed: `code`, `new_shares`, `old_shares`, `optionable`, `split_date`
- Read by parser: `code`, `new_shares`, `old_shares`, `optionable`, `split_date`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `optionable`: 'N'=225

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "code": "1120.SR",
  "new_shares": 3,
  "old_shares": 2,
  "optionable": "N",
  "split_date": "2026-04-21"
}
```

</details>

### Probe 5 — `dividends_symbol_filter`

- Purpose: dividend shape under filter[symbol] — JSON:API envelope
- Expected shape: `{meta, data: [row, row, ...], links}`
- Request path: `/api/calendar/dividends?filter[symbol]=AAPL.US`
- Status: **ok**
- HTTP elapsed: 1454 ms
- Row count: 90

#### Field diff (first row)

- Observed: `date`, `symbol`
- Read by parser: `date`, `symbol`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "date": "2026-02-09",
  "symbol": "AAPL.US"
}
```

</details>

### Probe 6 — `dividends_date_eq_filter`

- Purpose: dividend shape under filter[date_eq]=<yesterday>
- Expected shape: `{meta, data: [row, row, ...], links}`
- Request path: `/api/calendar/dividends?filter[date_eq]=2026-04-20`
- Status: **ok**
- HTTP elapsed: 1536 ms
- Row count: 426

#### Field diff (first row)

- Observed: `date`, `symbol`
- Read by parser: `date`, `symbol`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "date": "2026-04-20",
  "symbol": "CMRE.US"
}
```

</details>

### Probe 7 — `trends_single_symbol`

- Purpose: trend shape for 1 symbol — is outer list wrapped?
- Expected shape: `{trends: [[row, ...]]} (or flat [row, ...] if single-symbol)`
- Request path: `/api/calendar/trends?symbols=AAPL.US`
- Status: **ok**
- HTTP elapsed: 847 ms
- Row count: 92
- Note: trends payload wrapped [[…]] (1 outer groups → 92 rows)

#### Field diff (first row)

- Observed: `code`, `date`, `earningsEstimateAvg`, `earningsEstimateGrowth`, `earningsEstimateHigh`, `earningsEstimateLow`, `earningsEstimateNumberOfAnalysts`, `earningsEstimateYearAgoEps`, `epsRevisionsDownLast30days`, `epsRevisionsUpLast30days`, `epsRevisionsUpLast7days`, `epsTrend30daysAgo`, `epsTrend60daysAgo`, `epsTrend7daysAgo`, `epsTrend90daysAgo`, `epsTrendCurrent`, `growth`, `period`, `revenueEstimateAvg`, `revenueEstimateGrowth`, `revenueEstimateHigh`, `revenueEstimateLow`, `revenueEstimateNumberOfAnalysts`, `revenueEstimateYearAgoEps`
- Read by parser: `code`, `date`, `earningsEstimateAvg`, `earningsEstimateGrowth`, `earningsEstimateHigh`, `earningsEstimateLow`, `earningsEstimateNumberOfAnalysts`, `earningsEstimateYearAgoEps`, `epsRevisionsDownLast30days`, `epsRevisionsUpLast30days`, `epsRevisionsUpLast7days`, `epsTrend30daysAgo`, `epsTrend60daysAgo`, `epsTrend7daysAgo`, `epsTrend90daysAgo`, `epsTrendCurrent`, `growth`, `period`, `revenueEstimateAvg`, `revenueEstimateGrowth`, `revenueEstimateHigh`, `revenueEstimateLow`, `revenueEstimateNumberOfAnalysts`, `revenueEstimateYearAgoEps`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `period`: '+1q'=36, '0q'=36, '+1y'=10, '0y'=10

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "code": "AAPL.US",
  "date": "2027-09-30",
  "earningsEstimateAvg": "9.3657",
  "earningsEstimateGrowth": "0.1001",
  "earningsEstimateHigh": "10.3800",
  "earningsEstimateLow": "8.3600",
  "earningsEstimateNumberOfAnalysts": "40.0000",
  "earningsEstimateYearAgoEps": "8.5138",
  "epsRevisionsDownLast30days": "5.0000",
  "epsRevisionsUpLast30days": "2.0000",
  "epsRevisionsUpLast7days": "2.0000",
  "epsTrend30daysAgo": "9.3661",
  "epsTrend60daysAgo": "9.3051",
  "epsTrend7daysAgo": "9.3356",
  "epsTrend90daysAgo": "9.1356",
  "epsTrendCurrent": "9.3657",
  "growth": "0.0963",
  "period": "+1y",
  "revenueEstimateAvg": "498934116700.00",
  "revenueEstimateGrowth": "0.0710",
  "revenueEstimateHigh": "534295000000.00",
  "revenueEstimateLow": "473881148700.00",
  "revenueEstimateNumberOfAnalysts": "41.00",
  "revenueEstimateYearAgoEps": null
}
```

</details>

### Probe 8 — `trends_multi_symbol`

- Purpose: trend shape for 2 symbols — confirm [[…]] nesting
- Expected shape: `{trends: [[row, ...], [row, ...]]}`
- Request path: `/api/calendar/trends?symbols=AAPL.US,MSFT.US`
- Status: **ok**
- HTTP elapsed: 1626 ms
- Row count: 184
- Note: trends payload wrapped [[…]] (2 outer groups → 184 rows)

#### Field diff (first row)

- Observed: `code`, `date`, `earningsEstimateAvg`, `earningsEstimateGrowth`, `earningsEstimateHigh`, `earningsEstimateLow`, `earningsEstimateNumberOfAnalysts`, `earningsEstimateYearAgoEps`, `epsRevisionsDownLast30days`, `epsRevisionsUpLast30days`, `epsRevisionsUpLast7days`, `epsTrend30daysAgo`, `epsTrend60daysAgo`, `epsTrend7daysAgo`, `epsTrend90daysAgo`, `epsTrendCurrent`, `growth`, `period`, `revenueEstimateAvg`, `revenueEstimateGrowth`, `revenueEstimateHigh`, `revenueEstimateLow`, `revenueEstimateNumberOfAnalysts`, `revenueEstimateYearAgoEps`
- Read by parser: `code`, `date`, `earningsEstimateAvg`, `earningsEstimateGrowth`, `earningsEstimateHigh`, `earningsEstimateLow`, `earningsEstimateNumberOfAnalysts`, `earningsEstimateYearAgoEps`, `epsRevisionsDownLast30days`, `epsRevisionsUpLast30days`, `epsRevisionsUpLast7days`, `epsTrend30daysAgo`, `epsTrend60daysAgo`, `epsTrend7daysAgo`, `epsTrend90daysAgo`, `epsTrendCurrent`, `growth`, `period`, `revenueEstimateAvg`, `revenueEstimateGrowth`, `revenueEstimateHigh`, `revenueEstimateLow`, `revenueEstimateNumberOfAnalysts`, `revenueEstimateYearAgoEps`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `period`: '+1q'=72, '0q'=72, '+1y'=20, '0y'=20

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "code": "AAPL.US",
  "date": "2027-09-30",
  "earningsEstimateAvg": "9.3657",
  "earningsEstimateGrowth": "0.1001",
  "earningsEstimateHigh": "10.3800",
  "earningsEstimateLow": "8.3600",
  "earningsEstimateNumberOfAnalysts": "40.0000",
  "earningsEstimateYearAgoEps": "8.5138",
  "epsRevisionsDownLast30days": "5.0000",
  "epsRevisionsUpLast30days": "2.0000",
  "epsRevisionsUpLast7days": "2.0000",
  "epsTrend30daysAgo": "9.3661",
  "epsTrend60daysAgo": "9.3051",
  "epsTrend7daysAgo": "9.3356",
  "epsTrend90daysAgo": "9.1356",
  "epsTrendCurrent": "9.3657",
  "growth": "0.0963",
  "period": "+1y",
  "revenueEstimateAvg": "498934116700.00",
  "revenueEstimateGrowth": "0.0710",
  "revenueEstimateHigh": "534295000000.00",
  "revenueEstimateLow": "473881148700.00",
  "revenueEstimateNumberOfAnalysts": "41.00",
  "revenueEstimateYearAgoEps": null
}
```

</details>

### Probe 9 — `dividend_details_aapl`

- Purpose: per-ticker dividend extended fields — amount / currency / D/R/P dates
- Expected shape: `list[row] (top-level array, not enveloped)`
- Request path: `/api/div/AAPL.US?from=2025-10-23&to=2026-04-21`
- Status: **ok**
- HTTP elapsed: 1567 ms
- Row count: 2

#### Field diff (first row)

- Observed: `currency`, `date`, `declarationDate`, `paymentDate`, `period`, `recordDate`, `unadjustedValue`, `value`
- Read by parser: `currency`, `date`, `declarationDate`, `paymentDate`, `period`, `recordDate`, `unadjustedValue`, `value`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `period`: 'Quarterly'=2
- `currency`: 'USD'=2

#### Parser dry-parse: 2/2 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "currency": "USD",
  "date": "2025-11-10",
  "declarationDate": "2025-10-30",
  "paymentDate": "2025-11-13",
  "period": "Quarterly",
  "recordDate": "2025-11-10",
  "unadjustedValue": 0.26,
  "value": 0.26
}
```

</details>

## Summary

- Unknown-observed fields: ✓ none
- Missing-expected fields: ✓ none
- Type mismatches: ✓ none
- Parse failures in sample: ✓ none

### Action items

- Acquisition layer matches parser expectations. No scaffold changes required.
