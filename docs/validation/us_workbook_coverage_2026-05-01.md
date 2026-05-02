# US workbook field coverage validation

Date: 2026-05-01
Workbook inspected: `美国数据库20260307.xlsx`
User-supplied filename: `.~美国数据库20260307.xlsx`

The `.~` file is a 165-byte Excel lock file. Field extraction used sibling workbook `美国数据库20260307.xlsx`.

## Result

Coverage is partial. The repo has connected official-source equivalents for core US macro fields. Exact Wind/Bloomberg parity requires licensed Wind and Bloomberg adapters.

Coverage definitions:
- connected: configured exact or direct-equivalent source path exists in repo
- partial: source connector or provider exists, while the exact series, config, transform, or scheduled sync needs work
- missing: no configured source path found in repo scan

Field counts:
- connected: 58
- partial: 284
- missing: 59
- total extracted unique fields: 401

Input source labels:
- Bloomberg security: 5
- Wind/workbook label: 396

Source capability and storage pipeline use separate gates. A connected source
adapter can discover or fetch data from an institution, while a series reaches
storage after provider-series config, obs-family metadata, concept mapping,
fetcher output, and schedule/vintage handling are wired.

## Evidence From Repo

- Source capability adapters are registered for FRED, BLS, EIA, Treasury Fiscal, NY Fed rates/research, market watchlist, OECD, World Bank, Eurostat, ECB, IMF, BIS, Census, and BEA. Exact Wind/Bloomberg source parity requires licensed adapters.
- FRED configured macro series include CPI/core CPI/core PCE, NFP, unemployment, claims, GDP, real GDP, retail sales, industrial production, 2Y/10Y/30Y Treasury, 10Y real yield, 10Y-2Y spread, Fed balance sheet, M2, reverse repo, TGA, broad dollar, CNY/USD, HY OAS, and VIX.
- BLS configured series include headline CPI/core/food/energy/shelter, PPI/core PPI, NFP/private payrolls, average hourly earnings, average weekly hours, unemployment, LFPR, JOLTS openings/hires/quits, ECI, productivity, and unit labor costs.
- BEA configs cover NIPA GDP summary/contributions/real GDP/PCE/personal income plus ITA current-account and goods-balance datasets; latest sync for arbitrary BEA datasets remains a follow-up.
- The market watchlist covers DXY, USD/JPY, USD/CNY, VIX, 5Y/10Y/30Y Treasury proxies, and WTI.
- ISM current-value coverage is the Manufacturing PMI headline.

## Coverage By Sheet

| Sheet | connected | partial | missing |
| --- | ---: | ---: | ---: |
| MOVE 和VIX | 1 | 0 | 1 |
| 主要国家国债收益率利差 | 2 | 0 | 2 |
| 劳动力市场（月度指标） | 8 | 81 | 0 |
| 周度数据 | 3 | 4 | 4 |
| 季度数据 | 8 | 3 | 0 |
| 德国国债 | 6 | 0 | 0 |
| 日本国债 | 0 | 0 | 7 |
| 月度数据 | 11 | 29 | 3 |
| 汇率 | 5 | 12 | 0 |
| 流动性SOFR-OIS | 1 | 0 | 1 |
| 美债 | 5 | 6 | 0 |
| 美国GDP分稿 | 2 | 72 | 0 |
| 美国ISM制造业PMI（月度指标） | 0 | 2 | 42 |
| 美国国债 | 6 | 13 | 0 |
| 美联储金融流动性（负债规模-TGA-逆回购） | 3 | 0 | 0 |
| 通胀（月度指标） | 4 | 63 | 0 |

## Source/Indicator Groups

| Source/indicator group | Status | Notes |
| --- | --- | --- |
| Exact Wind/Bloomberg source | missing | The workbook source layer is Wind plus Bloomberg securities. Exact parity requires licensed Wind and Bloomberg adapters. |
| BEA GDP/PCE/ITA detail | partial | BEA client and NIPA/ITA configs exist; arbitrary-dataset latest sync needs production wiring. |
| BLS CPI/labor detail | partial | BLS connector and headline series exist; detailed CPI weights/components, seasonal food/energy components, payroll industries, race unemployment, U1-U6, and detailed JOLTS ids need config mapping. |
| ISM PMI | partial | ISM Manufacturing headline value is registered; subcomponents and services PMI fields need connectors/config. |
| Treasury curve | partial | 2Y/5Y proxy/10Y/30Y/10Y real/10Y-2Y are configured; additional maturities and TIPS tenors need series additions. |
| FX | partial | DXY, broad dollar, EUR/USD, USD/CNY, and USD/JPY are covered; dollar sub-indexes and additional FX pairs need configured series/watchlist entries. |
| Global sovereign yields | partial | Germany yield series are configured through Bundesbank BBSSY. Japan and China official parity paths remain open. |
| Market volatility | partial | VIX is configured; MOVE needs a configured source path. |
| GSCPI | connected | NY Fed GSCPI is configured through the NY Fed research-data fetcher. |

## Source Policy For Follow-Up Wiring

Current coverage status remains a repo-configuration finding. A field marked missing can still have an available vendor or official source path outside the current configured universe.

| Source/indicator group | Canonical source path | Repo action |
| --- | --- | --- |
| MOVE index | Market lane through EODHD `MOVE.INDX`; official reference is ICE MOVE | Add `MOVE.INDX` to the EODHD market universe/config. |
| China 10Y government yield | Market lane through EODHD `CN10Y.GBOND`; ChinaBond official path for official parity | Add EODHD GBOND market config and keep ChinaBond as the official-source parity path. |
| Germany yields | Bundesbank daily federal securities yields; EODHD GBOND as market backup where available | Connected in issue #112 slice 5 through Bundesbank BBSSY, obs-family, concept-map, release-schedule, subject aliases, and SDMX raw snapshots. |
| Japan yields | Japan MOF JGB interest-rate CSV; EODHD GBOND as market backup where available | Add Japan MOF mappings for 1Y/2Y/3Y/5Y/7Y/10Y/30Y and optional EODHD GBOND market rows. |
| WEI | FRED/Dallas Fed `WEI` | Connected in issue #112 slice 2 through FRED `MACRO_SERIES`, obs-family, concept-map, release-schedule, and subject aliases. |
| Housing vacancy and homeownership | Census HVS official data or FRED Census-hosted mirrors `RHORUSQ156N`, `RRVRUSQ156N`, `RHVRUSQ156N` | Connected in issue #112 slice 2 through FRED Census-hosted HVS series, obs-families, concept-map, release-schedules, and subject aliases. |
| Sector leverage ratios | BIS Total Credit Statistics | Connected in issue #112 slice 3 through BIS `WS_TC` v2.0 government, household, and non-financial corporation leverage mappings. |
| GSCPI | NY Fed GSCPI research-product data | Connected in issue #112 slice 4 through `NYFED_GSCPI`, obs-family, concept-map, release-schedule, and subject aliases. |
| Redbook weekly YoY | Redbook Research / Johnson Redbook official or authorized source | Decide authorized source and add connector/config. |
| Weekly raw steel production | AISI weekly raw steel production page | Add AISI scraper for weekly value, WoW, and YoY fields. |
| ISM manufacturing/services subcomponents | ISM official PMI report pages | Extend the ISM connector beyond Manufacturing PMI headline to full manufacturing/services subcomponents. |
| Sentix US current/expectations/headline | Sentix official account or authorized data vendor | Decide authorized source and add connector/config. |
| SOFR-OIS / `USSOC BGN Curncy` | Bloomberg BGN or licensed SOFR OIS curve vendor | Store as a rates-market quote/curve series with raw snapshots. |

EODHD remains a market/quote source for exchange, FX, index, GBOND, and related quote-style observations. Canonical economic ingestion uses official sources with vintage/as-of support where available, or repo-owned raw snapshots from first ingestion onward.

The liquidity fields in the coverage sample are connected through existing FRED/Treasury paths:

| Workbook field | Current repo source path | Status |
| --- | --- | --- |
| `FARBLIAB Index` / Fed balance sheet | FRED `WALCL` | connected |
| `FARBDTRS Index` / TGA | FRED `WTREGEN`; Treasury Fiscal Data `TREAS_TGA_BALANCE` | connected |
| `FARWRRA Index` / reverse repo | FRED `RRPONTSYD` | connected |

## Field-Level Map

Full field-level map: `docs/validation/us_workbook_coverage_2026-05-01.csv`

Recommended implementation sequence: add Japan MOF yields, AISI weekly steel, and full ISM official report parsing. Licensed-source decisions remain for Redbook, Sentix US, SOFR-OIS, and exact Wind/Bloomberg parity.
