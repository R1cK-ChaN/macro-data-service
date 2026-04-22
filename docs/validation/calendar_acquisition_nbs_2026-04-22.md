# NBS Calendar Acquisition Validation — 2026-04-22

Scope: verifies the **acquisition** step only (fetch + parse).
Storage and downstream API are under our control and may be adjusted once upstream shape is understood correctly.

## Budget

- Requests spent this run: **2**
- stats.gov.cn: no auth, HTTP-only, flaky from non-CN IPs
- Probes planned: 1 / executed: 1

## Probes

### Probe 1 — `nbs_yearly_calendar_2026`

- Purpose: NBS yearly release calendar article for 2026 — covers every registered indicator in one fetch
- Expected shape: `list[NBSReleaseEntry] after HTML parse`
- Request path: `GET https://www.stats.gov.cn/english/PressRelease/ReleaseCalendar/202512/t20251226_1962154.html`
- Status: **ok**
- HTTP elapsed: 11088 ms
- Row count: 85
- Note: entries by indicator: MANUFACTURING_PMI=12, NON_MANUFACTURING_PMI=12, CPI=12, PPI=12, INDUSTRIAL_PRODUCTION=11, FIXED_ASSET_INVESTMENT=11, RETAIL_SALES=11, GDP=4

#### Enum observations (all rows)

- `indicator`: MANUFACTURING_PMI=12, NON_MANUFACTURING_PMI=12, CPI=12, PPI=12, INDUSTRIAL_PRODUCTION=11, FIXED_ASSET_INVESTMENT=11, RETAIL_SALES=11, GDP=4

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "date_cell": "31/Thu",
  "day": 31,
  "indicator": "MANUFACTURING_PMI",
  "month": 12,
  "release_time_local": "9:30",
  "year": 2026
}
```

</details>

## Summary

- Unknown-observed fields: ✓ none
- Missing-expected fields: ✓ none
- Type mismatches: ✓ none
- Parse failures in sample: ✓ none
- Probe-level failures: ✓ none

### Action items

- Acquisition layer matches parser expectations. No scaffold changes required.
