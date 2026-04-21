# TE Calendar Acquisition Validation — 2026-04-21

Scope: verifies the **acquisition** step only (fetch + parse).
Storage and downstream API are under our control and may be adjusted once upstream shape is understood correctly.

## Budget

- Requests spent this run: **5**
- TE basic-plan monthly cap: 1000
- Probes planned: 5 / executed: 5

## Probes

### Probe 1 — `country_all_last_7d`

- Purpose: baseline 22-field shape over a dense recent window
- Expected shape: `list[22-field dict]`
- Request path: `/calendar/country/All/2026-04-14/2026-04-21`
- Status: **ok**
- HTTP elapsed: 2884 ms
- Row count: 454

#### Field diff (first row)

- Observed: `Actual`, `CalendarId`, `Category`, `Country`, `Currency`, `Date`, `DateSpan`, `Event`, `Forecast`, `Importance`, `LastUpdate`, `Previous`, `Reference`, `ReferenceDate`, `Revised`, `Source`, `SourceURL`, `Symbol`, `TEForecast`, `Ticker`, `URL`, `Unit`
- Read by parser: `Actual`, `CalendarId`, `Category`, `Country`, `Currency`, `Date`, `Event`, `Forecast`, `Importance`, `LastUpdate`, `Previous`, `Reference`, `ReferenceDate`, `Revised`, `Source`, `SourceURL`, `TEForecast`, `Ticker`, `Unit`
- Ignored by parser (known-but-unread): `DateSpan`, `Symbol`, `URL`
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none
- ⚠️ **Type warnings**:
  - Date='2026-04-14T00:00:00' has no timezone marker — parser will treat as UTC
  - LastUpdate='2025-12-03T18:05:14.037' has no timezone marker — parser will treat as UTC

#### Enum observations (all rows)

- `Importance`: 1=372, 2=71, 3=11
- `Currency`: ''=406, '$'=20, '€'=12, '£'=3, 'NZ$'=3, 'TRY'=2, '¥'=2, 'C$'=2 (+4 more values)
- `Country`: 'United States'=74, 'Euro Area'=26, 'Canada'=18, 'United Kingdom'=17, 'France'=16, 'China'=14, 'New Zealand'=13, 'India'=13 (+70 more values)
- `Category`: 'Calendar'=43, 'Interest Rate'=39, 'Inflation Rate'=31, 'Inflation Rate Mom'=27, 'Holidays'=23, 'Balance of Trade'=19, 'Industrial Production'=13, 'Producer Prices Change'=12 (+149 more values)

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "Actual": "",
  "CalendarId": "415717",
  "Category": "Holidays",
  "Country": "Cambodia",
  "Currency": "",
  "Date": "2026-04-14T00:00:00",
  "DateSpan": "0",
  "Event": "Cambodian New Year",
  "Forecast": "",
  "Importance": 1,
  "LastUpdate": "2025-12-03T18:05:14.037",
  "Previous": "",
  "Reference": "",
  "ReferenceDate": null,
  "Revised": "",
  "Source": "",
  "SourceURL": "",
  "Symbol": "",
  "TEForecast": "",
  "Ticker": "HOLIDAYSCAMBODIA",
  "URL": "/cambodia/holidays",
  "Unit": ""
}
```

</details>

### Probe 2 — `country_all_2024_01`

- Purpose: older era — shape drift vs recent window
- Expected shape: `list[22-field dict]`
- Request path: `/calendar/country/All/2024-01-01/2024-01-07`
- Status: **ok**
- HTTP elapsed: 1240 ms
- Row count: 558

#### Field diff (first row)

- Observed: `Actual`, `CalendarId`, `Category`, `Country`, `Currency`, `Date`, `DateSpan`, `Event`, `Forecast`, `Importance`, `LastUpdate`, `Previous`, `Reference`, `ReferenceDate`, `Revised`, `Source`, `SourceURL`, `Symbol`, `TEForecast`, `Ticker`, `URL`, `Unit`
- Read by parser: `Actual`, `CalendarId`, `Category`, `Country`, `Currency`, `Date`, `Event`, `Forecast`, `Importance`, `LastUpdate`, `Previous`, `Reference`, `ReferenceDate`, `Revised`, `Source`, `SourceURL`, `TEForecast`, `Ticker`, `Unit`
- Ignored by parser (known-but-unread): `DateSpan`, `Symbol`, `URL`
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none
- ⚠️ **Type warnings**:
  - Date='2024-01-01T00:00:00' has no timezone marker — parser will treat as UTC
  - LastUpdate='2023-11-22T12:30:03.12' has no timezone marker — parser will treat as UTC

#### Enum observations (all rows)

- `Importance`: 1=469, 2=76, 3=13
- `Currency`: ''=522, '$'=21, '€'=4, '£'=3, 'QAR'=2, 'PKR'=1, 'ARS'=1, 'DKK'=1 (+3 more values)
- `Country`: 'United States'=63, 'Germany'=28, 'Brazil'=16, 'France'=16, 'Euro Area'=14, 'United Kingdom'=14, 'Spain'=13, 'Canada'=11 (+133 more values)
- `Category`: 'Holidays'=191, 'Manufacturing PMI'=41, 'Inflation Rate'=21, 'Composite PMI'=20, 'Services PMI'=15, 'Inflation Rate Mom'=13, 'Foreign Exchange Reserves'=12, 'Balance of Trade'=10 (+128 more values)

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "Actual": "",
  "CalendarId": "339988",
  "Category": "Holidays",
  "Country": "Albania",
  "Currency": "",
  "Date": "2024-01-01T00:00:00",
  "DateSpan": "0",
  "Event": "New Year’s Day",
  "Forecast": "",
  "Importance": 1,
  "LastUpdate": "2023-11-22T12:30:03.12",
  "Previous": "",
  "Reference": "",
  "ReferenceDate": null,
  "Revised": "",
  "Source": "",
  "SourceURL": "",
  "Symbol": "",
  "TEForecast": "",
  "Ticker": "HOLIDAYSALBANIA",
  "URL": "/albania/holidays",
  "Unit": ""
}
```

</details>

### Probe 3 — `country_us_last_7d`

- Purpose: country-scoped + URL encoding on spaces
- Expected shape: `list[22-field dict]`
- Request path: `/calendar/country/United%20States/2026-04-14/2026-04-21`
- Status: **ok**
- HTTP elapsed: 256 ms
- Row count: 74

#### Field diff (first row)

- Observed: `Actual`, `CalendarId`, `Category`, `Country`, `Currency`, `Date`, `DateSpan`, `Event`, `Forecast`, `Importance`, `LastUpdate`, `Previous`, `Reference`, `ReferenceDate`, `Revised`, `Source`, `SourceURL`, `Symbol`, `TEForecast`, `Ticker`, `URL`, `Unit`
- Read by parser: `Actual`, `CalendarId`, `Category`, `Country`, `Currency`, `Date`, `Event`, `Forecast`, `Importance`, `LastUpdate`, `Previous`, `Reference`, `ReferenceDate`, `Revised`, `Source`, `SourceURL`, `TEForecast`, `Ticker`, `Unit`
- Ignored by parser (known-but-unread): `DateSpan`, `Symbol`, `URL`
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none
- ⚠️ **Type warnings**:
  - Date='2026-04-14T10:00:00' has no timezone marker — parser will treat as UTC
  - LastUpdate='2026-04-14T10:00:00.627' has no timezone marker — parser will treat as UTC

#### Enum observations (all rows)

- `Importance`: 1=50, 2=23, 3=1
- `Currency`: ''=70, '$'=4
- `Country`: 'United States'=74
- `Category`: 'Interest Rate'=10, 'Calendar'=3, 'Nfib Business Optimism Index'=1, 'ADP Employment Change Weekly'=1, 'Producer Prices Change'=1, 'Producer Prices'=1, 'Producer Price Inflation MoM'=1, 'PPI Ex Food Energy and Trade Services YoY'=1 (+55 more values)

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "Actual": "95.8",
  "CalendarId": "399197",
  "Category": "Nfib Business Optimism Index",
  "Country": "United States",
  "Currency": "",
  "Date": "2026-04-14T10:00:00",
  "DateSpan": "0",
  "Event": "NFIB Business Optimism Index",
  "Forecast": "98.6",
  "Importance": 1,
  "LastUpdate": "2026-04-14T10:00:00.627",
  "Previous": "98.8",
  "Reference": "Mar",
  "ReferenceDate": "2026-03-31T00:00:00",
  "Revised": "",
  "Source": "National Federation of Independent Business",
  "SourceURL": "http://www.nfib.com",
  "Symbol": "UNITEDSTANFIBUSOPTIN",
  "TEForecast": "97",
  "Ticker": "UNITEDSTANFIBUSOPTIN",
  "URL": "/united-states/nfib-business-optimism-index",
  "Unit": ""
}
```

</details>

### Probe 4 — `updates_pointer`

- Purpose: pointer shape: is it really 4 fields?
- Expected shape: `list[pointer dict (CalendarId/Country/Event/LastUpdate)]`
- Request path: `/calendar/updates`
- Status: **ok**
- HTTP elapsed: 637 ms
- Row count: 1000 (⚠️ truncated at 1000)

#### Field diff (first row)

- Observed: `CalendarId`, `Country`, `Event`, `LastUpdate`
- Read by parser: `CalendarId`, `Country`, `Event`, `LastUpdate`
- Ignored by parser (known-but-unread): (none)
- UNKNOWN_OBSERVED: ✓ none
- ⚠️ **MISSING_EXPECTED**: `Actual`, `Category`, `Currency`, `Date`, `Forecast`, `Importance`, `Previous`, `Reference`, `ReferenceDate`, `Revised`, `Source`, `SourceURL`, `TEForecast`, `Ticker`, `Unit`
- ⚠️ **Type warnings**:
  - LastUpdate='2026-04-21T00:27:55.917' has no timezone marker — parser will treat as UTC

#### Enum observations (all rows)

- `Importance`: None=1000
- `Currency`: None=1000
- `Country`: 'United States'=145, 'Japan'=59, 'United Kingdom'=54, 'Canada'=42, 'Euro Area'=38, 'India'=32, 'China'=30, 'France'=26 (+78 more values)
- `Category`: None=1000

#### Parser dry-parse: 10/10 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "CalendarId": "399714",
  "Country": "United Kingdom",
  "Event": "Retail Price Index YoY",
  "LastUpdate": "2026-04-21T00:27:55.917"
}
```

</details>

### Probe 5 — `calendarid_rehydrate`

- Purpose: rehydration shape vs /country/All full-row shape
- Expected shape: `list[22-field dict]`
- Request path: `/calendar/calendarid/415717,419748,415720`
- Status: **ok**
- HTTP elapsed: 258 ms
- Row count: 3

#### Field diff (first row)

- Observed: `Actual`, `CalendarId`, `Category`, `Country`, `Currency`, `Date`, `DateSpan`, `Event`, `Forecast`, `Importance`, `LastUpdate`, `Previous`, `Reference`, `ReferenceDate`, `Revised`, `Source`, `SourceURL`, `Symbol`, `TEForecast`, `Ticker`, `URL`, `Unit`
- Read by parser: `Actual`, `CalendarId`, `Category`, `Country`, `Currency`, `Date`, `Event`, `Forecast`, `Importance`, `LastUpdate`, `Previous`, `Reference`, `ReferenceDate`, `Revised`, `Source`, `SourceURL`, `TEForecast`, `Ticker`, `Unit`
- Ignored by parser (known-but-unread): `DateSpan`, `Symbol`, `URL`
- UNKNOWN_OBSERVED: ✓ none
- MISSING_EXPECTED: ✓ none
- ⚠️ **Type warnings**:
  - Date='2026-04-14T00:00:00' has no timezone marker — parser will treat as UTC
  - LastUpdate='2025-12-03T18:05:14.037' has no timezone marker — parser will treat as UTC

#### Enum observations (all rows)

- `Importance`: 1=3
- `Currency`: ''=3
- `Country`: 'Cambodia'=1, 'Laos'=1, 'IMF'=1
- `Category`: 'Holidays'=2, 'Calendar'=1

#### Parser dry-parse: 3/3 rows parsed

<details><summary>Sample row JSON</summary>

```json
{
  "Actual": "",
  "CalendarId": "415717",
  "Category": "Holidays",
  "Country": "Cambodia",
  "Currency": "",
  "Date": "2026-04-14T00:00:00",
  "DateSpan": "0",
  "Event": "Cambodian New Year",
  "Forecast": "",
  "Importance": 1,
  "LastUpdate": "2025-12-03T18:05:14.037",
  "Previous": "",
  "Reference": "",
  "ReferenceDate": null,
  "Revised": "",
  "Source": "",
  "SourceURL": "",
  "Symbol": "",
  "TEForecast": "",
  "Ticker": "HOLIDAYSCAMBODIA",
  "URL": "/cambodia/holidays",
  "Unit": ""
}
```

</details>

## Summary

- Unknown-observed fields: ✓ none
- Missing-expected fields: ⚠️ found
- Type mismatches: ⚠️ found
- Parse failures in sample: ✓ none

### Action items

- Review MISSING_EXPECTED — parser reads fields that never arrived. Either defensive defaults are masking it or we're overspec'd.
- Review type warnings — type coercion quirks that silently corrupt event rows.
