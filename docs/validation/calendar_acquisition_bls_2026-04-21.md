# BLS Calendar Acquisition Validation — 2026-04-21

Scope: verifies the **acquisition** step only (fetch + parse).
Storage and downstream API are under our control and may be adjusted once upstream shape is understood correctly.

## Budget

- Requests spent this run: **2**
- BLS Public Data API v2 free-tier daily cap: 500
- Probes planned: 2 / executed: 2

## Probes

### Probe 1 — `cpi_two_year_window`

- Purpose: CPI-U, All Items, NSA — headline US inflation print (highest trader-impact BLS release)
- Expected shape: `list[BLSObservation] after client parse`
- Request path: `POST https://api.bls.gov/publicAPI/v2/timeseries/data/ seriesid=[CUUR0000SA0] startyear=2025 endyear=2026`
- Status: **ok**
- HTTP elapsed: 1928 ms
- Row count: 14

#### Field diff (first row)

- Observed: `footnotes`, `latest`, `period`, `periodName`, `value`, `year`
- Read by parser: `footnotes`, `latest`, `period`, `periodName`, `value`, `year`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `period`: 'M03'=2, 'M02'=2, 'M01'=2, 'M12'=1, 'M11'=1, 'M09'=1, 'M08'=1, 'M07'=1 (+3 more values)

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "footnotes": [
    {}
  ],
  "latest": "true",
  "period": "M03",
  "periodName": "March",
  "value": "330.213",
  "year": "2026"
}
```

</details>

### Probe 2 — `nfp_two_year_window`

- Purpose: Total nonfarm employment, SA (thousands) — the BLS Employment Situation headline
- Expected shape: `list[BLSObservation] after client parse`
- Request path: `POST https://api.bls.gov/publicAPI/v2/timeseries/data/ seriesid=[CES0000000001] startyear=2025 endyear=2026`
- Status: **ok**
- HTTP elapsed: 348 ms
- Row count: 15

#### Field diff (first row)

- Observed: `footnotes`, `latest`, `period`, `periodName`, `value`, `year`
- Read by parser: `footnotes`, `latest`, `period`, `periodName`, `value`, `year`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `period`: 'M03'=2, 'M02'=2, 'M01'=2, 'M12'=1, 'M11'=1, 'M10'=1, 'M09'=1, 'M08'=1 (+4 more values)

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "footnotes": [
    {
      "code": "P",
      "text": "preliminary"
    }
  ],
  "latest": "true",
  "period": "M03",
  "periodName": "March",
  "value": "158637",
  "year": "2026"
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
