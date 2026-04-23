# U Michigan Calendar Acquisition Validation — 2026-04-23

Scope: verifies the **acquisition** step only (fetch + parse).
Storage and downstream API are under our control and may be adjusted once upstream shape is understood correctly.

## Budget

- Requests spent this run: **3**
- U Michigan public HTML/PDF: no auth, unspecified rate limit (polite)
- Probes planned: 2 / executed: 2

## Probes

### Probe 1 — `umich_release_dates_2026`

- Purpose: U Michigan Consumer Sentiment preliminary/final release dates for 2026
- Expected shape: `HTML/PDF -> U Michigan schedule entries / current value`
- Request path: `GET https://data.sca.isr.umich.edu/survey-info.php -> GET https://data.sca.isr.umich.edu/fetchdoc.php?docid=79628`
- Status: **ok**
- HTTP elapsed: 2738 ms
- Row count: 24
- Note: entries by stage: preliminary=12, final=12

#### Enum observations (all rows)

- `release_stage`: preliminary=12, final=12

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "event_time_utc": "2026-12-18T15:00:00+00:00",
  "reference_date": "2026-12-01",
  "release_date": "2026-12-18",
  "release_stage": "final",
  "series_id": "UMICH_CONSUMER_SENTIMENT"
}
```

</details>

### Probe 2 — `umich_current_results`

- Purpose: U Michigan current Consumer Sentiment table value parse
- Expected shape: `HTML/PDF -> U Michigan schedule entries / current value`
- Request path: `GET https://www.sca.isr.umich.edu/`
- Status: **ok**
- HTTP elapsed: 982 ms
- Row count: 1

#### Parser dry-parse: 1/1 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "actual": "47.6",
  "previous": "53.3",
  "reference_date": "2026-04-01",
  "release_stage": "preliminary",
  "series_id": "UMICH_CONSUMER_SENTIMENT",
  "source_url": "https://www.sca.isr.umich.edu/"
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
