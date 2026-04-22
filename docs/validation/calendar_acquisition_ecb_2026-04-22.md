# ECB Calendar Acquisition Validation — 2026-04-22

Scope: verifies the **acquisition** step only (fetch + parse).
Storage and downstream API are under our control and may be adjusted once upstream shape is understood correctly.

## Budget

- Requests spent this run: **3**
- ECB Data Portal: no auth, unspecified rate limit (polite)
- Probes planned: 3 / executed: 3

## Probes

### Probe 1 — `ecb_mro_two_year_window`

- Purpose: ECB Main Refinancing Operations Rate
- Expected shape: `list[SDMXObservation] after client parse`
- Request path: `GET https://data-api.ecb.europa.eu/service/data/FM/B.U2.EUR.4F.KR.MRR_FR.LEV?format=jsondata&startPeriod=2024-04-22&endPeriod=2026-04-22`
- Status: **ok**
- HTTP elapsed: 1415 ms
- Row count: 8
- Note: raw observations: 8 | rate changes after collapse: 8

#### Field diff (first row)

- Observed: `dataflow`, `date`, `series_id`, `value`
- Read by parser: `dataflow`, `date`, `series_id`, `value`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `value`: 2.15=1, 2.4=1, 2.65=1, 2.9=1, 3.15=1, 3.4=1, 3.65=1, 4.25=1

#### Parser dry-parse: 8/8 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "dataflow": "FM",
  "date": "2025-06-11",
  "series_id": "FM.B.U2.EUR.4F.KR.MRR_FR.LEV",
  "value": 2.15
}
```

</details>

### Probe 2 — `ecb_dfr_two_year_window`

- Purpose: ECB Deposit Facility Rate
- Expected shape: `list[SDMXObservation] after client parse`
- Request path: `GET https://data-api.ecb.europa.eu/service/data/FM/B.U2.EUR.4F.KR.DFR.LEV?format=jsondata&startPeriod=2024-04-22&endPeriod=2026-04-22`
- Status: **ok**
- HTTP elapsed: 322 ms
- Row count: 8
- Note: raw observations: 8 | rate changes after collapse: 8

#### Field diff (first row)

- Observed: `dataflow`, `date`, `series_id`, `value`
- Read by parser: `dataflow`, `date`, `series_id`, `value`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `value`: 2.0=1, 2.25=1, 2.5=1, 2.75=1, 3.0=1, 3.25=1, 3.5=1, 3.75=1

#### Parser dry-parse: 8/8 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "dataflow": "FM",
  "date": "2025-06-11",
  "series_id": "FM.B.U2.EUR.4F.KR.DFR.LEV",
  "value": 2.0
}
```

</details>

### Probe 3 — `ecb_mlf_two_year_window`

- Purpose: ECB Marginal Lending Facility Rate
- Expected shape: `list[SDMXObservation] after client parse`
- Request path: `GET https://data-api.ecb.europa.eu/service/data/FM/B.U2.EUR.4F.KR.MLFR.LEV?format=jsondata&startPeriod=2024-04-22&endPeriod=2026-04-22`
- Status: **ok**
- HTTP elapsed: 597 ms
- Row count: 8
- Note: raw observations: 8 | rate changes after collapse: 8

#### Field diff (first row)

- Observed: `dataflow`, `date`, `series_id`, `value`
- Read by parser: `dataflow`, `date`, `series_id`, `value`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `value`: 2.4=1, 2.65=1, 2.9=1, 3.15=1, 3.4=1, 3.65=1, 3.9=1, 4.5=1

#### Parser dry-parse: 8/8 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "dataflow": "FM",
  "date": "2025-06-11",
  "series_id": "FM.B.U2.EUR.4F.KR.MLFR.LEV",
  "value": 2.4
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
