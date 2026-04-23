# ISM Calendar Acquisition Validation — 2026-04-23

Scope: verifies the **acquisition** step only (fetch + parse).
Storage and downstream API are under our control and may be adjusted once upstream shape is understood correctly.

## Budget

- Requests spent this run: **3**
- ISM plan: unknown cap
- Probes planned: 2 / executed: 2

## Probes

### Probe 1 — `ism_release_calendar`

- Purpose: ISM Manufacturing PMI release dates from the public release-calendar table
- Expected shape: `HTML -> ISM schedule entries / report value`
- Request path: `GET https://www.ismworld.org/supply-management-news-and-reports/reports/rob-report-calendar/`
- Status: **ok**
- HTTP elapsed: 403 ms
- Row count: 12
- Note: entries by series: ISM_MANUFACTURING_PMI=12

#### Enum observations (all rows)

- `series_id`: ISM_MANUFACTURING_PMI=12

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "event_time_utc": "2026-12-01T15:00:00+00:00",
  "reference_date": "2026-11-01",
  "release_date": "2026-12-01",
  "series_id": "ISM_MANUFACTURING_PMI"
}
```

</details>

### Probe 2 — `ism_current_manufacturing_report`

- Purpose: ISM PMI reports hub discovery plus current Manufacturing PMI report value parse
- Expected shape: `HTML -> ISM schedule entries / report value`
- Request path: `GET https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/ -> GET https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/march/`
- Status: **ok**
- HTTP elapsed: 925 ms
- Row count: 1

#### Parser dry-parse: 1/1 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "actual": "52.7",
  "previous": "52.4",
  "reference_date": "2026-03-01",
  "series_id": "ISM_MANUFACTURING_PMI",
  "source_url": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/march/"
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
