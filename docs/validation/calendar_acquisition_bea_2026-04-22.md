# BEA Calendar Acquisition Validation — 2026-04-22

Scope: verifies the **acquisition** step only (fetch + parse).
Storage and downstream API are under our control and may be adjusted once upstream shape is understood correctly.

## Budget

- Requests spent this run: **4**
- BEA REST API free-tier daily cap: 1000
- Probes planned: 4 / executed: 4

## Probes

### Probe 1 — `gdp_two_year_window`

- Purpose: Real Gross Domestic Product
- Expected shape: `list[BEAObservation] after client parse`
- Request path: `GET https://apps.bea.gov/api/data?DatasetName=NIPA&TableName=T10101&Frequency=Q&Year=2025,2026 (filter LineNumber=1)`
- Status: **ok**
- HTTP elapsed: 1394 ms
- Row count: 4

#### Field diff (first row)

- Observed: `CL_UNIT`, `DataValue`, `LineDescription`, `LineNumber`, `METRIC_NAME`, `NoteRef`, `SeriesCode`, `TableName`, `TimePeriod`, `UNIT_MULT`
- Read by parser: `DataValue`, `LineDescription`, `LineNumber`, `NoteRef`, `TimePeriod`
- Ignored by parser (known-but-unread): (none)
- ⚠️ **UNKNOWN_OBSERVED**: `CL_UNIT`, `METRIC_NAME`, `SeriesCode`, `TableName`, `UNIT_MULT`
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `TimePeriod`: '2025Q4'=1, '2025Q3'=1, '2025Q2'=1, '2025Q1'=1

#### Parser dry-parse: 4/4 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "CL_UNIT": "Percent change, annual rate",
  "DataValue": "0.5",
  "LineDescription": "Gross domestic product",
  "LineNumber": "1",
  "METRIC_NAME": "Fisher Quantity Index",
  "NoteRef": "T10101",
  "SeriesCode": "A191RL",
  "TableName": "T10101",
  "TimePeriod": "2025Q4",
  "UNIT_MULT": "0"
}
```

</details>

### Probe 2 — `personal_income_two_year_window`

- Purpose: Personal Income
- Expected shape: `list[BEAObservation] after client parse`
- Request path: `GET https://apps.bea.gov/api/data?DatasetName=NIPA&TableName=T20600&Frequency=M&Year=2025,2026 (filter LineNumber=1)`
- Status: **ok**
- HTTP elapsed: 810 ms
- Row count: 14

#### Field diff (first row)

- Observed: `CL_UNIT`, `DataValue`, `LineDescription`, `LineNumber`, `METRIC_NAME`, `NoteRef`, `SeriesCode`, `TableName`, `TimePeriod`, `UNIT_MULT`
- Read by parser: `DataValue`, `LineDescription`, `LineNumber`, `NoteRef`, `TimePeriod`
- Ignored by parser (known-but-unread): (none)
- ⚠️ **UNKNOWN_OBSERVED**: `CL_UNIT`, `METRIC_NAME`, `SeriesCode`, `TableName`, `UNIT_MULT`
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `TimePeriod`: '2026M02'=1, '2026M01'=1, '2025M12'=1, '2025M11'=1, '2025M10'=1, '2025M09'=1, '2025M08'=1, '2025M07'=1 (+6 more values)

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "CL_UNIT": "Level",
  "DataValue": "26,660,972",
  "LineDescription": "Personal income",
  "LineNumber": "1",
  "METRIC_NAME": "Current Dollars",
  "NoteRef": "T20600",
  "SeriesCode": "A065RC",
  "TableName": "T20600",
  "TimePeriod": "2026M02",
  "UNIT_MULT": "6"
}
```

</details>

### Probe 3 — `pce_two_year_window`

- Purpose: PCE Price Index
- Expected shape: `list[BEAObservation] after client parse`
- Request path: `GET https://apps.bea.gov/api/data?DatasetName=NIPA&TableName=T20804&Frequency=M&Year=2025,2026 (filter LineNumber=1)`
- Status: **ok**
- HTTP elapsed: 377 ms
- Row count: 14

#### Field diff (first row)

- Observed: `CL_UNIT`, `DataValue`, `LineDescription`, `LineNumber`, `METRIC_NAME`, `NoteRef`, `SeriesCode`, `TableName`, `TimePeriod`, `UNIT_MULT`
- Read by parser: `DataValue`, `LineDescription`, `LineNumber`, `NoteRef`, `TimePeriod`
- Ignored by parser (known-but-unread): (none)
- ⚠️ **UNKNOWN_OBSERVED**: `CL_UNIT`, `METRIC_NAME`, `SeriesCode`, `TableName`, `UNIT_MULT`
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `TimePeriod`: '2026M02'=1, '2026M01'=1, '2025M12'=1, '2025M11'=1, '2025M10'=1, '2025M09'=1, '2025M08'=1, '2025M07'=1 (+6 more values)

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "CL_UNIT": "Level",
  "DataValue": "129.449",
  "LineDescription": "Personal consumption expenditures (PCE)",
  "LineNumber": "1",
  "METRIC_NAME": "Fisher Price Index",
  "NoteRef": "T20804",
  "SeriesCode": "DPCERG",
  "TableName": "T20804",
  "TimePeriod": "2026M02",
  "UNIT_MULT": "0"
}
```

</details>

### Probe 4 — `corporate_profits_two_year_window`

- Purpose: Corporate Profits
- Expected shape: `list[BEAObservation] after client parse`
- Request path: `GET https://apps.bea.gov/api/data?DatasetName=NIPA&TableName=T11200&Frequency=Q&Year=2025,2026 (filter LineNumber=13)`
- Status: **ok**
- HTTP elapsed: 567 ms
- Row count: 4

#### Field diff (first row)

- Observed: `CL_UNIT`, `DataValue`, `LineDescription`, `LineNumber`, `METRIC_NAME`, `NoteRef`, `SeriesCode`, `TableName`, `TimePeriod`, `UNIT_MULT`
- Read by parser: `DataValue`, `LineDescription`, `LineNumber`, `NoteRef`, `TimePeriod`
- Ignored by parser (known-but-unread): (none)
- ⚠️ **UNKNOWN_OBSERVED**: `CL_UNIT`, `METRIC_NAME`, `SeriesCode`, `TableName`, `UNIT_MULT`
- MISSING_EXPECTED: ✓ none

#### Enum observations (all rows)

- `TimePeriod`: '2025Q4'=1, '2025Q3'=1, '2025Q2'=1, '2025Q1'=1

#### Parser dry-parse: 4/4 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "CL_UNIT": "Level",
  "DataValue": "4,352,096",
  "LineDescription": "Corporate profits with IVA and CCAdj",
  "LineNumber": "13",
  "METRIC_NAME": "Current Dollars",
  "NoteRef": "T11200",
  "SeriesCode": "A051RC",
  "TableName": "T11200",
  "TimePeriod": "2025Q4",
  "UNIT_MULT": "6"
}
```

</details>

## Summary

- Unknown-observed fields: ⚠️ found
- Missing-expected fields: ✓ none
- Type mismatches: ✓ none
- Parse failures in sample: ✓ none

### Action items

- Review UNKNOWN_OBSERVED fields per probe — may be new TE columns worth reading or ignoring explicitly.
