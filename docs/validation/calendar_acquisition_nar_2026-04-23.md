# NAR Calendar Acquisition Validation — 2026-04-23

Scope: verifies the **acquisition** step only (fetch + parse).
Storage and downstream API are under our control and may be adjusted once upstream shape is understood correctly.

## Budget

- Requests spent this run: **3**
- NAR public HTML: no auth, unspecified rate limit (polite)
- Probes planned: 3 / executed: 3

## Probes

### Probe 1 — `nar_statistical_release_schedule`

- Purpose: NAR statistical release schedule rows for Existing-Home Sales and Pending Home Sales Index
- Expected shape: `HTML -> NAR schedule entries / current values`
- Request path: `GET https://www.nar.realtor/newsroom/nar-statistical-news-release-schedule`
- Status: **ok**
- HTTP elapsed: 163 ms
- Row count: 16
- Note: entries by series: NAR_EXISTING_HOME_SALES=8, NAR_PENDING_HOME_SALES_MOM=8

#### Enum observations (all rows)

- `series_id`: NAR_EXISTING_HOME_SALES=8, NAR_PENDING_HOME_SALES_MOM=8

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "event_time_utc": "2026-12-17T15:00:00+00:00",
  "raw_title": "November Pending Home Sales Index",
  "reference_date": "2026-11-01",
  "release_date": "2026-12-17",
  "series_id": "NAR_PENDING_HOME_SALES_MOM"
}
```

</details>

### Probe 2 — `nar_existing_home_sales`

- Purpose: NAR current Existing Home Sales million-SAAR parse
- Expected shape: `HTML -> NAR schedule entries / current values`
- Request path: `GET https://www.nar.realtor/research-and-statistics/housing-statistics/existing-home-sales`
- Status: **ok**
- HTTP elapsed: 187 ms
- Row count: 1

#### Parser dry-parse: 1/1 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "actual": "3.98",
  "previous": null,
  "raw_change": "-3.6",
  "reference_date": "2026-03-01",
  "series_id": "NAR_EXISTING_HOME_SALES",
  "source_url": "https://www.nar.realtor/research-and-statistics/housing-statistics/existing-home-sales"
}
```

</details>

### Probe 3 — `nar_pending_home_sales`

- Purpose: NAR current Pending Home Sales MoM percent parse
- Expected shape: `HTML -> NAR schedule entries / current values`
- Request path: `GET https://www.nar.realtor/research-and-statistics/housing-statistics/pending-home-sales`
- Status: **ok**
- HTTP elapsed: 178 ms
- Row count: 1

#### Parser dry-parse: 1/1 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "actual": "1.5",
  "previous": null,
  "raw_change": null,
  "reference_date": "2026-03-01",
  "series_id": "NAR_PENDING_HOME_SALES_MOM",
  "source_url": "https://www.nar.realtor/research-and-statistics/housing-statistics/pending-home-sales"
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
