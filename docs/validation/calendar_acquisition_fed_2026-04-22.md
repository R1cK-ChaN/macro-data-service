# Fed Calendar Acquisition Validation — 2026-04-22

Scope: verifies the **acquisition** step only (fetch + parse).
Storage and downstream API are under our control and may be adjusted once upstream shape is understood correctly.

## Budget

- Requests spent this run: **1**
- federalreserve.gov: no auth, HTML scrape (browser-UA required)
- Probes planned: 2 / executed: 1

## Probes

### Probe 1 — `fomc_calendar`

- Purpose: FOMC meeting calendar — 6-year rolling panel of meeting dates + SEP markers
- Expected shape: `list[FomcMeetingEntry] / list[FedReleaseEntry]`
- Request path: `GET https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm`
- Status: **ok**
- HTTP elapsed: 220 ms
- Row count: 56
- Note: year span: 2021 → 2027 (7 distinct years) | SEP meetings: 28

#### Enum observations (all rows)

- `year`: 2026=8, 2025=8, 2024=8, 2023=8, 2022=8, 2021=8, 2027=8

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "closing_date": "2027-12-08",
  "date_cell": "7-8*",
  "has_sep": true,
  "month_name": "December",
  "year": 2027
}
```

</details>

### Probe 2 — `fed_releasedates`

- Purpose: Fed release calendar — rolling ~60-day Beige Book / H.4.1 / H.8 schedule
- Expected shape: `list[FomcMeetingEntry] / list[FedReleaseEntry]`
- Request path: `GET https://www.federalreserve.gov/newsevents/releasedates.htm`
- Status: **http_error**
- Note: HTTPError: 404 Client Error: Not Found for url: https://www.federalreserve.gov/newsevents/releasedates.htm

## Summary

- Unknown-observed fields: ✓ none
- Missing-expected fields: ✓ none
- Type mismatches: ✓ none
- Parse failures in sample: ✓ none
- Probe-level failures: ⚠️ 1 of 2

### Action items

- 1 probe(s) failed outright (status ≠ ``ok``). Each probe's Note lines carry the error — upstream drift (URL / DOM / payload shape) is the most common cause. Resolve before treating the remaining field-diff signal as authoritative.
