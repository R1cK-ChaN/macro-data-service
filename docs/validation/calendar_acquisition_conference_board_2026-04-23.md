# Conference Board Calendar Acquisition Validation — 2026-04-23

Scope: verifies the **acquisition** step only (fetch + parse).
Storage and downstream API are under our control and may be adjusted once upstream shape is understood correctly.

## Budget

- Requests spent this run: **3**
- Conference Board public HTML/JSON: no auth, unspecified rate limit (polite)
- Probes planned: 3 / executed: 3

## Probes

### Probe 1 — `conference_board_release_calendar`

- Purpose: Conference Board economic-indicator calendar rows for US Consumer Confidence and US Leading Index
- Expected shape: `JSON/HTML -> Conference Board schedule entries / current values`
- Request path: `GET https://www.conference-board.org/data/calendar/events.json.cfm?from=1773014400000&to=1792454400000`
- Status: **ok**
- HTTP elapsed: 1452 ms
- Row count: 14
- Note: entries by series: TCB_LEADING_INDEX=7, TCB_CONSUMER_CONFIDENCE=7

#### Enum observations (all rows)

- `series_id`: TCB_LEADING_INDEX=7, TCB_CONSUMER_CONFIDENCE=7

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "calendar_event_id": "120",
  "event_time_utc": "2026-09-29T14:00:00+00:00",
  "reference_date": "2026-09-01",
  "release_date": "2026-09-29",
  "series_id": "TCB_CONSUMER_CONFIDENCE",
  "source_url": "https://www.conference-board.org/topics/consumer-confidence"
}
```

</details>

### Probe 2 — `conference_board_consumer_confidence`

- Purpose: Conference Board current Consumer Confidence value parse
- Expected shape: `JSON/HTML -> Conference Board schedule entries / current values`
- Request path: `GET https://www.conference-board.org/topics/consumer-confidence`
- Status: **ok**
- HTTP elapsed: 3497 ms
- Row count: 1

#### Parser dry-parse: 1/1 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "actual": "91.8",
  "index_level": null,
  "previous": "91.0",
  "reference_date": "2026-03-01",
  "series_id": "TCB_CONSUMER_CONFIDENCE",
  "source_url": "https://www.conference-board.org/topics/consumer-confidence"
}
```

</details>

### Probe 3 — `conference_board_us_leading_index`

- Purpose: Conference Board current US Leading Index monthly-change parse
- Expected shape: `JSON/HTML -> Conference Board schedule entries / current values`
- Request path: `GET https://www.conference-board.org/topics/us-leading-indicators`
- Status: **ok**
- HTTP elapsed: 2705 ms
- Row count: 1

#### Parser dry-parse: 1/1 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "actual": "-0.1",
  "index_level": "97.5",
  "previous": "-0.2",
  "reference_date": "2026-01-01",
  "series_id": "TCB_LEADING_INDEX",
  "source_url": "https://www.conference-board.org/topics/us-leading-indicators"
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
