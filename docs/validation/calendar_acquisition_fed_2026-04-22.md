# Fed Calendar Acquisition Validation — 2026-04-22

Scope: verifies the **acquisition** step only (fetch + parse).
Storage and downstream API are under our control and may be adjusted once upstream shape is understood correctly.

## Budget

- Requests spent this run: **2**
- federalreserve.gov: no auth, HTML scrape (browser-UA required)
- Probes planned: 2 / executed: 2

## Probes

### Probe 1 — `fomc_calendar`

- Purpose: FOMC meeting calendar — 6-year rolling panel of meeting dates + SEP markers
- Expected shape: `list[FomcMeetingEntry] / list[FedReleaseEntry]`
- Request path: `GET https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm`
- Status: **ok**
- HTTP elapsed: 511 ms
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

- Purpose: Fed calendar JSON — rolling Beige Book / H.4.1 / H.8 schedule
- Expected shape: `list[FomcMeetingEntry] / list[FedReleaseEntry]`
- Request path: `GET https://www.federalreserve.gov/json/calendar.json`
- Status: **ok**
- HTTP elapsed: 441 ms
- Row count: 889
- Note: entries by indicator: FED_H41=417, FED_H8=416, BEIGE_BOOK=56

#### Enum observations (all rows)

- `series_id`: FED_H41=417, FED_H8=416, BEIGE_BOOK=56

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "event_time_utc": "2026-12-31T21:30:00+00:00",
  "release_date": "2026-12-31",
  "release_time_local": "4:30 PM",
  "release_title": "H.4.1 - Factors Affecting Reserve Balances",
  "series_id": "FED_H41"
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
