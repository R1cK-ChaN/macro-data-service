# US workbook field coverage validation

Date: 2026-05-01
Workbook inspected: `美国数据库20260307.xlsx`
User-supplied filename: `.~美国数据库20260307.xlsx`

The `.~` file is a 165-byte Excel lock file. Field extraction used sibling workbook `美国数据库20260307.xlsx`.

## Result

Coverage is partial. The repo has connected official-source equivalents for core US macro fields. Exact Wind/Bloomberg parity requires licensed Wind and Bloomberg adapters.

Coverage definitions:
- connected: configured exact or direct-equivalent source path exists in repo
- partial: source family exists, while the exact series, config, transform, or scheduled sync needs work
- missing: no configured source path found in repo scan

Field counts:
- connected: 44
- partial: 284
- missing: 73
- total extracted unique fields: 401

Workbook source labels:
- Bloomberg security: 5
- Wind/workbook label: 396

## Evidence From Repo

- Source capability adapters are registered for FRED, BLS, EIA, Treasury Fiscal, NY Fed rates, market watchlist, OECD, World Bank, Eurostat, ECB, IMF, BIS, Census, and BEA. Exact Wind/Bloomberg source parity requires licensed adapters.
- FRED configured macro series include CPI/core CPI/core PCE, NFP, unemployment, claims, GDP, real GDP, retail sales, industrial production, 2Y/10Y/30Y Treasury, 10Y real yield, 10Y-2Y spread, Fed balance sheet, M2, reverse repo, TGA, broad dollar, CNY/USD, HY OAS, and VIX.
- BLS configured series include headline CPI/core/food/energy/shelter, PPI/core PPI, NFP/private payrolls, average hourly earnings, average weekly hours, unemployment, LFPR, JOLTS openings/hires/quits, ECI, productivity, and unit labor costs.
- BEA configs cover NIPA GDP summary/contributions/real GDP/PCE/personal income plus ITA current-account and goods-balance datasets; latest sync for arbitrary BEA datasets remains a follow-up.
- The market watchlist covers DXY, USD/JPY, USD/CNY, VIX, 5Y/10Y/30Y Treasury proxies, and WTI.
- ISM current-value coverage is the Manufacturing PMI headline.

## Coverage By Sheet

| Sheet | connected | partial | missing |
| --- | ---: | ---: | ---: |
| MOVE 和VIX | 1 | 0 | 1 |
| 主要国家国债收益率利差 | 1 | 0 | 3 |
| 劳动力市场（月度指标） | 6 | 80 | 0 |
| 周度数据 | 2 | 4 | 5 |
| 季度数据 | 2 | 3 | 6 |
| 德国国债 | 0 | 0 | 6 |
| 日本国债 | 0 | 0 | 7 |
| 月度数据 | 10 | 29 | 4 |
| 汇率 | 5 | 12 | 0 |
| 流动性SOFR-OIS | 1 | 0 | 1 |
| 美债 | 5 | 6 | 0 |
| 美国GDP分稿 | 2 | 72 | 0 |
| 美国ISM制造业PMI（月度指标） | 0 | 2 | 42 |
| 美国国债 | 6 | 13 | 0 |
| 美联储金融流动性（负债规模-TGA-逆回购） | 3 | 0 | 0 |
| 通胀（月度指标） | 4 | 63 | 0 |

## Gap Families

| Family | Status | Notes |
| --- | --- | --- |
| Exact Wind/Bloomberg source | missing | The workbook source layer is Wind plus Bloomberg securities. Exact parity requires licensed Wind and Bloomberg adapters. |
| BEA GDP/PCE/ITA detail | partial | BEA client and NIPA/ITA configs exist; arbitrary-dataset latest sync needs production wiring. |
| BLS CPI/labor detail | partial | BLS connector and headline series exist; detailed CPI weights/components, seasonal food/energy components, payroll industries, race unemployment, U1-U6, and detailed JOLTS ids need config mapping. |
| ISM PMI | partial | ISM Manufacturing headline value is registered; subcomponents and services PMI fields need connectors/config. |
| Treasury curve | partial | 2Y/5Y proxy/10Y/30Y/10Y real/10Y-2Y are configured; additional maturities and TIPS tenors need series additions. |
| FX | partial | DXY, broad dollar, EUR/USD, USD/CNY, and USD/JPY are covered; dollar sub-indexes and additional FX pairs need configured series/watchlist entries. |
| Global sovereign yields | missing | Germany, Japan, and China yield series need configured source paths. |
| Market volatility | partial | VIX is configured; MOVE needs a configured source path. |
| GSCPI | missing | NY Fed GSCPI needs a configured source path. |

## Field-Level Map

Full field-level map: `docs/validation/us_workbook_coverage_2026-05-01.csv`

Recommended implementation sequence: add source mappings for the partial official-source families first, then decide whether exact Wind/Bloomberg parity requires licensed adapters.
