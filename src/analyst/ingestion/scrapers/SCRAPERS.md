# Scrapers – Data Reference

Each scraper module targets one financial site and exposes clients for every
scrapable data section. All clients use `curl_cffi` (via `create_cf_session`)
for TLS-fingerprint bypass of Cloudflare protection.

All news clients support **pagination** — single-page fetches for polling,
plus `fetch_all_news()` convenience methods for backfill.

---

## Shared Data Types (`_common.py`)

| Dataclass | Purpose |
|-----------|---------|
| `ScrapedNewsItem` | A news/article headline from any site (`image_url`, `raw_json` included) |
| `ScrapedIndicator` | A macro-economic indicator snapshot |
| `ScrapedMarketQuote` | A market price quote (`symbol`, `raw_json` included) |

Calendar data uses the existing `StoredEventRecord` from `analyst.storage`.

---

## 1. Investing.com (`investing.py`)

### InvestingCalendarClient

Scrapes the **economic calendar** via Investing.com's internal JSON API.

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch(date_from, date_to)` | `list[StoredEventRecord]` | Events for a single date range |
| `fetch_range(days_back, days_forward)` | `list[StoredEventRecord]` | Multi-day sweep with 1.5 s delay between days |

**Fields per event:** source, event_id, timestamp (epoch seconds), country,
indicator, category, importance (low/medium/high), actual, forecast, previous,
revised_previous, surprise, currency, raw_json.

**Anti-bot:** POST to `/Service/getCalendarFilteredData` with
`X-Requested-With: XMLHttpRequest`. Retries 3 times with exponential backoff.

### InvestingNewsClient

Scrapes **news articles** from the `/news/<category>` HTML pages.

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_news(category, page=1)` | `list[ScrapedNewsItem]` | Articles for one category page |
| `fetch_all_news(category, max_pages=3)` | `list[ScrapedNewsItem]` | Paginate through multiple pages with 1.5 s delay |

**Supported categories:** `latest-news`, `economy-news`,
`commodities-news`, `cryptocurrency-news`, `forex-news`,
`stock-market-news`, `economic-indicators`, `world-news`,
`most-popular-news`.

**Fields per item:**

| Field | Example |
|-------|---------|
| `title` | "Oil at $100 could lift U.S. inflation…" |
| `url` | Full article URL |
| `published_at` | `2026-03-08 08:14:56` (server local time) |
| `description` | First-paragraph snippet |
| `author` | "Reuters", "Investing.com" |
| `category` | Extracted from URL path (e.g. `economy-news`) |
| `raw_json.comments` | Comment count (int, when visible on the page) |

**Typical yield:** ~20 articles per category page.

---

## 2. ForexFactory (`forexfactory.py`)

### ForexFactoryCalendarClient

Scrapes the **economic calendar** table from the ForexFactory calendar page.

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch(week)` | `list[StoredEventRecord]` | Events for `"this"` week or a specific week string |

**Fields per event:** source, event_id, timestamp (epoch seconds), country,
indicator, category, importance (low/medium/high from colour), actual,
forecast, previous, surprise, raw_json.

### ForexFactoryNewsClient

Scrapes the **news feed** from the ForexFactory `/news` page.

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_news(page=1)` | `list[ScrapedNewsItem]` | News articles for one page |
| `fetch_all_news(max_pages=3)` | `list[ScrapedNewsItem]` | Paginate through multiple pages with 1.5 s delay |

**Fields per item:**

| Field | Example |
|-------|---------|
| `title` | "Week Ahead: War and More War" |
| `url` | `https://www.forexfactory.com/news/1387525-…` |
| `description` | Article preview / first paragraph |
| `author` | Source site: "reuters.com", "zerohedge.com", "@realDonaldTrump" |
| `importance` | `high` / `medium` / `low` (from colour badge, if present) |
| `image_url` | Article thumbnail (when rendered on the page) |
| `raw_json.time_ago` | "3 hr ago" |
| `raw_json.comments` | Comment count (int) |

**Typical yield:** ~20-35 news items per fetch.

**Note:** Impact badges only appear on breaking-news items. Regular articles
have `importance=""`.

---

## 3. TradingEconomics (`tradingeconomics.py`)

### TradingEconomicsCalendarClient

Scrapes the **economic calendar** with per-event importance (3 requests,
one per importance level).

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch()` | `list[StoredEventRecord]` | Today's calendar events at all importance levels |

### TradingEconomicsNewsClient

Fetches the **news stream** from TE's internal JSON API (`/ws/stream.ashx`).

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_news(start=0, count=20)` | `list[ScrapedNewsItem]` | *count* news items from offset *start* |
| `fetch_all_news(max_items=100, batch_size=20)` | `list[ScrapedNewsItem]` | Paginate through the stream with 1 s delay between batches |

**Fields per item:**

| Field | Example |
|-------|---------|
| `title` | "China FX Reserves Highest in Over 10 Years" |
| `url` | `https://tradingeconomics.com/china/foreign-exchange-reserves` |
| `published_at` | `2026-03-07T04:19:00.057` (UTC) |
| `description` | Full article body text |
| `author` | "Farida Husna", or "CALCULATOR" for auto-generated summaries |
| `category` | Indicator name: "Foreign Exchange Reserves", "Inflation Rate", "Crypto" |
| `importance` | `high` / `medium` / `low` (numeric 3/2/1 mapped) |
| `image_url` | Article image or thumbnail URL (whichever is non-empty) |
| `raw_json.country` | "China", "United States", "Crypto" |
| `raw_json.id` | Numeric stream item ID |
| `raw_json.expiration` | When the item expires from the stream |
| `raw_json.html` | Rich HTML body with embedded symbol links (if present) |
| `raw_json.type` | Item type, e.g. "indicator" (if present) |

**Typical yield:** 20 items per batch (configurable via `count`).

### TradingEconomicsIndicatorsClient

Scrapes **macro-economic indicator tables** from the country indicators page.

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_indicators(country)` | `list[ScrapedIndicator]` | All indicators for a country |

**Country parameter:** URL slug, e.g. `"united-states"`, `"japan"`,
`"euro-area"`, `"china"`.

**Category detection:** Uses the site's native tab-pane taxonomy (e.g.
`gdp`, `labour`, `prices`, `money`, `trade`, `government`, `business`,
`consumer`, `housing`) when available. Falls back to preceding section
headings, then to a keyword-based heuristic (`categorize_event`) as
last resort.

**Fields per indicator:**

| Field | Example |
|-------|---------|
| `name` | "Unemployment Rate" |
| `last` | "4.4" |
| `previous` | "4.3" |
| `highest` | "14.8" |
| `lowest` | "2.5" |
| `unit` | "percent", "Thousand", "USD Billion" |
| `date` | "Feb/26" |
| `country` | "US" (ISO 2-letter code) |
| `category` | Native section id (e.g. "labour") or auto-detected fallback |
| `url` | Detail page link |

**Typical yield:** ~400 indicators for the US.

### TradingEconomicsMarketsClient

Scrapes the **market overview tables** from the TE news page sidebar.

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_markets()` | `list[ScrapedMarketQuote]` | Current market snapshot |

**Fields per quote:**

| Field | Example |
|-------|---------|
| `name` | "Crude Oil", "Bitcoin", "US500" |
| `asset_class` | `commodity` / `fx` / `index` / `stock` / `bond` / `crypto` |
| `price` | "90.900" |
| `change` | "9.89" |
| `change_pct` | "12.21%" |
| `url` | Detail page link |
| `symbol` | Row identifier, e.g. "CL1:COM", "XAUUSD:CUR", "BTCUSD:CUR" |
| `raw_json.decimals` | Display precision from `data-decimals` (when present) |

**Asset classes returned (6):**

| Class | Items | Examples |
|-------|-------|---------|
| `commodity` | 15 | Crude Oil, Brent, Gold, Natural Gas, Copper |
| `fx` | 15 | EURUSD, GBPUSD, USDJPY, USDCNY |
| `index` | 15 | US500, US30, DAX, FTSE 100, Nikkei 225 |
| `stock` | 15 | Apple, Tesla, Microsoft, Amazon, Nvidia |
| `bond` | 15 | US 10Y, UK 10Y, Japan 10Y, Germany 10Y |
| `crypto` | 15 | Bitcoin, Ether, Binance, Solana, XRP |

**Typical yield:** ~90 quotes total.

---

## 4. Reuters (`reuters.py`)

### ReutersNewsClient

Scrapes **article listings** from Reuters section pages by parsing three card
types (`HeroCard`, `BasicCard`, `MediaStoryCard`) via stable `data-testid`
attributes.

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_news(section="markets")` | `list[ScrapedNewsItem]` | Articles for a single section page |
| `fetch_all_news(sections, sleep_between=1.5)` | `list[ScrapedNewsItem]` | Multiple sections with 1.5 s delay between requests |

**Supported sections:** `markets`, `business`, `world`, `sustainability`,
`legal`, `technology`.

**Fields per item:**

| Field | Example |
|-------|---------|
| `title` | "Iran war threatens prolonged hit to global energy markets" |
| `url` | `https://www.reuters.com/business/energy/iran-war-…` |
| `published_at` | `2026-03-07T18:21:43.709Z` (ISO 8601 UTC) |
| `description` | Body snippet (when present on `MediaStoryCard`) |
| `category` | Kicker label: "Business", "Energy", "Sustainability" |
| `image_url` | Card thumbnail URL |
| `raw_json.card_type` | `HeroCard` / `BasicCard` / `MediaStoryCard` |

**Typical yield:** ~10-15 articles per section, ~25-30 across 3 sections
(deduplicated).

### ReutersArticleClient

Fetches and parses **full Reuters articles** with structured metadata.
Uses `curl_cffi` and Reuters-specific selectors for cleaner extraction than
the generic `ArticleFetcher`.

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_article(url)` | `ReutersArticle` | Single article with full text and metadata |
| `fetch_articles(urls, sleep_between=1.0)` | `list[ReutersArticle]` | Batch fetch with 1.0 s delay |

**Metadata sources:**

- **JSON-LD** (`@type: "NewsArticle"`): `articleSection`, `keywords`,
  `datePublished`, `image`.
- **HTML**: `<h1 data-testid="Heading">` for headline,
  `<a data-testid="AuthorNameLink">` for authors, `<time>` for date.
- **Body**: paragraph `<div>` elements matched by CSS-module class prefix
  `article-body-module__paragraph__*`.  Boilerplate lines (sign-up prompts,
  trust badges) are filtered out.

**`ReutersArticle` fields:**

| Field | Type | Example |
|-------|------|---------|
| `url` | `str` | Article URL |
| `title` | `str` | "Iran war threatens prolonged hit to global energy markets" |
| `content` | `str` | Full body as plain text (paragraphs joined by `\n\n`) |
| `authors` | `list[str]` | `["Timour Azhari", "Marwa Rashad"]` |
| `published_at` | `str` | `2026-03-07T11:14:35.637Z` |
| `section` | `str` | "Energy" |
| `keywords` | `list[str]` | `["markets commodities energy", "energy oil gas"]` |
| `image_url` | `str` | Lead image URL |
| `fetched` | `bool` | `True` on success |
| `error` | `str \| None` | Error message on failure |

**Keywords cleaning:** Internal Reuters tag codes (`COM`, `ENER`,
`REPI:OPEC`) are stripped.  `TOPIC:*` tags are cleaned and kept (e.g.
`TOPIC:ENERGY-OIL-GAS` → `"energy oil gas"`).

---

## 5. Bloomberg (`bloomberg.py`)

> **Transport:** `curl_cffi` with TLS fingerprint impersonation. Cookies
> are exported from a real Chrome session via `browser_cookie3`.

### Setup

```bash
pip install browser-cookie3
```

**Cookie export** — log in to bloomberg.com in your regular Chrome browser,
then export cookies:

```python
import browser_cookie3, json
from pathlib import Path

cj = list(browser_cookie3.chrome(domain_name=".bloomberg.com"))
cookies = [{"name": c.name, "value": c.value, "domain": c.domain,
            "path": c.path, "expires": c.expires or -1,
            "secure": bool(c.secure), "httpOnly": False, "sameSite": "Lax"}
           for c in cj]
out = Path.home() / ".analyst" / "bloomberg_cookies.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(cookies, indent=2))
```

### BloombergNewsClient

Scrapes **article listings** from Bloomberg section pages by navigating
with Playwright, then parsing the rendered HTML via three strategies:
`__NEXT_DATA__` JSON → JSON-LD → DOM `<article>` elements.

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_news(section="markets")` | `list[ScrapedNewsItem]` | Articles for a single section page |
| `fetch_all_news(sections, sleep_between=1.5)` | `list[ScrapedNewsItem]` | Multiple sections with 1.5 s delay, dedup by URL |

**Supported sections:** `markets`, `economics`, `technology`, `politics`,
`wealth`, `opinion`, `green`.

**Fields per item:**

| Field | Example |
|-------|---------|
| `title` | "Fed Signals Further Rate Cuts Amid Slowdown" |
| `url` | `https://www.bloomberg.com/news/articles/2026-03-…` |
| `published_at` | `2026-03-08T14:30:00Z` |
| `description` | Article summary / abstract |
| `category` | Section or primary category from JSON |
| `image_url` | Lead image URL |

### BloombergArticleClient

Fetches and parses **full Bloomberg articles** with structured metadata.
Requires an authenticated session (cookies from `login()`).

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_article(url)` | `BloombergArticle` | Single article with full text and metadata |
| `fetch_articles(urls, sleep_between=1.5)` | `list[BloombergArticle]` | Batch fetch with 1.5 s delay |

**Metadata sources (3-tier):**

1. **JSON-LD** (`@type: "Article"`): `headline`, `datePublished`, `author`,
   `articleSection`, `keywords`, `image`.
2. **OpenGraph meta tags**: `og:title`, `og:image`, `article:published_time`,
   `article:author`, `article:section`.
3. **DOM selectors**: `<h1>` for headline, `<a href="/authors/…">` for byline,
   `<time>` for date, `<p>` elements in body container for content.

**`BloombergArticle` fields:**

| Field | Type | Example |
|-------|------|---------|
| `url` | `str` | Article URL |
| `title` | `str` | "Fed Signals Further Rate Cuts Amid Slowdown" |
| `content` | `str` | Full body as plain text (paragraphs joined by `\n\n`) |
| `authors` | `list[str]` | `["Craig Torres", "Liz Capo McCormick"]` |
| `published_at` | `str` | `2026-03-08T14:30:00Z` |
| `section` | `str` | "Markets" |
| `keywords` | `list[str]` | `["federal reserve", "interest rates"]` |
| `image_url` | `str` | Lead image URL |
| `lede` | `str` | Bloomberg-specific article summary / description |
| `fetched` | `bool` | `True` on success |
| `error` | `str \| None` | Error message on failure |

**Body filtering:** Sign-up prompts, newsletter CTAs, terms-of-service links,
and related teasers are stripped from article content.

---

## 6. rateprobability.com (`rateprobability.py`)

> **Transport:** Plain `requests.Session` — no Cloudflare or bot protection.

### RateProbabilityClient

Fetches **FedWatch-equivalent FOMC rate probabilities** from the
rateprobability.com JSON API (`/api/latest`). Updated every 2 minutes at source.

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_probabilities()` | `FedRateProbability` | Full snapshot of current rate probabilities and historical comparisons |

**`FedRateProbability` fields:**

| Field | Type | Example |
|-------|------|---------|
| `as_of` | `str` | `"2026-03-08T14:30:00Z"` |
| `current_band` | `str` | `"4.25-4.50"` |
| `midpoint` | `float` | `4.375` |
| `effr` | `float` | `4.33` |
| `meetings` | `list[FedMeetingProbability]` | Per-meeting probability data |
| `snapshots` | `dict[str, list]` | Historical comparisons (1w, 3w, 6w, 10w ago) |

**`FedMeetingProbability` fields:**

| Field | Type | Example |
|-------|------|---------|
| `meeting_date` | `str` | `"2026-06-17"` |
| `implied_rate` | `float` | `4.125` |
| `prob_move_pct` | `float` | `72.5` |
| `is_cut` | `bool` | `True` |
| `num_moves` | `int` | `1` |
| `change_bps` | `float` | `-25.0` |

**Storage:** Upserted into `indicators` table as `FEDPROB_{meeting_date}`
series (source `rateprobability`), value = `implied_rate`.

---

## 7. NY Fed Markets API (`nyfed.py`)

> **Transport:** Plain `requests.Session` — official public API, no bot protection.

### NYFedRatesClient

Fetches **daily reference rates** (SOFR, EFFR, OBFR) from the NY Fed
Markets API (`markets.newyorkfed.org`).

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_sofr(last_n=5)` | `list[NYFedRate]` | Last N SOFR observations |
| `fetch_effr(last_n=5)` | `list[NYFedRate]` | Last N EFFR observations |
| `fetch_obfr(last_n=5)` | `list[NYFedRate]` | Last N OBFR observations |
| `fetch_all_rates(last_n=5)` | `list[NYFedRate]` | All three rate types with 0.5 s delay between |

**`NYFedRate` fields:**

| Field | Type | Example |
|-------|------|---------|
| `date` | `str` | `"2026-03-07"` |
| `type` | `str` | `"SOFR"`, `"EFFR"`, `"OBFR"` |
| `rate` | `float` | `4.31` |
| `percentile_1` | `float \| None` | `4.30` |
| `percentile_25` | `float \| None` | `4.31` |
| `percentile_75` | `float \| None` | `4.32` |
| `percentile_99` | `float \| None` | `4.34` |
| `volume_billions` | `float \| None` | `2180.0` |
| `target_rate_from` | `float \| None` | `4.25` (EFFR only) |
| `target_rate_to` | `float \| None` | `4.50` (EFFR only) |

**Storage:** Upserted into `indicators` table as `NYFED_SOFR`,
`NYFED_EFFR`, `NYFED_OBFR` series (source `nyfed`), value = rate.

---

## 8. Financial Times (`ft.py`)

> **Transport:** `curl_cffi` with TLS fingerprint impersonation. Cookies
> are exported from a real Chrome session via `browser_cookie3`.

### Setup

```bash
pip install browser-cookie3
```

**Cookie export** — log in to ft.com in your regular Chrome browser,
then export cookies:

```python
import browser_cookie3, json
from pathlib import Path

cj = list(browser_cookie3.chrome(domain_name=".ft.com"))
cj += list(browser_cookie3.chrome(domain_name="ft.com"))
seen, cookies = set(), []
for c in cj:
    key = (c.name, c.domain)
    if key not in seen and "ft.com" in c.domain:
        seen.add(key)
        cookies.append({"name": c.name, "value": c.value, "domain": c.domain,
                        "path": c.path, "expires": c.expires or -1,
                        "secure": bool(c.secure), "httpOnly": False, "sameSite": "Lax"})
out = Path.home() / ".analyst" / "ft_cookies.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(cookies, indent=2))
```

### FTNewsClient

Scrapes **article listings** from FT section pages by navigating
with Playwright, then parsing the rendered HTML via three strategies:
`__NEXT_DATA__` JSON → JSON-LD → DOM `<article>` elements.

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_news(section="markets")` | `list[ScrapedNewsItem]` | Articles for a single section page |
| `fetch_all_news(sections, sleep_between=1.5)` | `list[ScrapedNewsItem]` | Multiple sections with 1.5 s delay, dedup by URL |

**Supported sections:** `markets`, `world`, `companies`, `opinion`,
`climate`, `technology`.

**Fields per item:**

| Field | Example |
|-------|---------|
| `title` | "Bank of England holds rates amid inflation uncertainty" |
| `url` | `https://www.ft.com/content/abc123-…` |
| `published_at` | `2026-03-08T14:30:00Z` |
| `description` | Article standfirst / summary |
| `category` | Section or primary category from JSON |
| `image_url` | Lead image URL |

### FTArticleClient

Fetches and parses **full FT articles** with structured metadata.
Requires an authenticated session (cookies from `login()`).

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_article(url)` | `FTArticle` | Single article with full text and metadata |
| `fetch_articles(urls, sleep_between=1.5)` | `list[FTArticle]` | Batch fetch with 1.5 s delay |

**Metadata sources (3-tier):**

1. **JSON-LD** (`@type: "Article"`): `headline`, `datePublished`, `author`,
   `articleSection`, `keywords`, `image`.
2. **OpenGraph meta tags**: `og:title`, `og:image`, `article:published_time`,
   `article:author`, `article:section`.
3. **DOM selectors**: `<h1>` for headline, `<a href="/stream/…">` for byline,
   `<time>` for date, `<p>` elements in body container for content.

**`FTArticle` fields:**

| Field | Type | Example |
|-------|------|---------|
| `url` | `str` | Article URL |
| `title` | `str` | "Bank of England holds rates amid inflation uncertainty" |
| `content` | `str` | Full body as plain text (paragraphs joined by `\n\n`) |
| `authors` | `list[str]` | `["Chris Giles", "Valentina Romei"]` |
| `published_at` | `str` | `2026-03-08T14:30:00Z` |
| `section` | `str` | "Markets" |
| `keywords` | `list[str]` | `["bank of england", "interest rates"]` |
| `image_url` | `str` | Lead image URL |
| `standfirst` | `str` | FT-specific subheading summary |
| `fetched` | `bool` | `True` on success |
| `error` | `str \| None` | Error message on failure |

**Body filtering:** Sign-up prompts, newsletter CTAs, subscriber barriers,
and topic-follow prompts are stripped from article content.

---

## 9. Wall Street Journal (`wsj.py`)

> **Transport:** `curl_cffi` with TLS fingerprint impersonation. WSJ's bot
> detection blocks Playwright, so cookies are exported from a real Chrome
> session via `browser_cookie3`.

### Setup

```bash
pip install browser-cookie3
```

**Cookie export** — log in to wsj.com in your regular Chrome browser,
then export cookies:

```python
import browser_cookie3, json
from pathlib import Path

cj = browser_cookie3.chrome(domain_name=".wsj.com")
cookies = [{"name": c.name, "value": c.value, "domain": c.domain,
            "path": c.path, "expires": c.expires or -1,
            "secure": bool(c.secure), "httpOnly": False, "sameSite": "Lax"}
           for c in cj]
out = Path.home() / ".analyst" / "wsj_cookies.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(cookies, indent=2))
```

### WSJNewsClient

Scrapes **article listings** from WSJ section pages by navigating
with Playwright, then parsing the rendered HTML via three strategies:
`__NEXT_DATA__` JSON → JSON-LD → DOM `<article>` elements.

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_news(section="markets")` | `list[ScrapedNewsItem]` | Articles for a single section page |
| `fetch_all_news(sections, sleep_between=1.5)` | `list[ScrapedNewsItem]` | Multiple sections with 1.5 s delay, dedup by URL |

**Supported sections:** `markets`, `economy`, `business`, `tech`,
`politics`, `opinion`, `world`.

**Fields per item:**

| Field | Example |
|-------|---------|
| `title` | "Treasury Yields Rise on Stronger-Than-Expected Jobs Data" |
| `url` | `https://www.wsj.com/finance/stocks/treasury-yields-…` |
| `published_at` | `2026-03-08T14:30:00Z` |
| `description` | Article dek / summary |
| `category` | Section or primary category from JSON |
| `image_url` | Lead image URL |

### WSJArticleClient

Fetches and parses **full WSJ articles** with structured metadata.
Requires an authenticated session (cookies from `login()`).

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_article(url)` | `WSJArticle` | Single article with full text and metadata |
| `fetch_articles(urls, sleep_between=1.5)` | `list[WSJArticle]` | Batch fetch with 1.5 s delay |

**Metadata sources (3-tier):**

1. **JSON-LD** (`@type: "Article"`): `headline`, `datePublished`, `author`,
   `articleSection`, `keywords`, `image`.
2. **OpenGraph/meta tags**: `og:title`, `og:image`, `article:published_time`,
   `article:section`, `<meta name="author">` for byline.
3. **DOM selectors**: `<h1>` for headline, `<a href="/author/…">` for byline,
   `<time>` for date, `<p class="…Paragraph…">` for body content.

**`WSJArticle` fields:**

| Field | Type | Example |
|-------|------|---------|
| `url` | `str` | Article URL |
| `title` | `str` | "Treasury Yields Rise on Stronger-Than-Expected Jobs Data" |
| `content` | `str` | Full body as plain text (paragraphs joined by `\n\n`) |
| `authors` | `list[str]` | `["Sam Goldfarb", "Matt Grossman"]` |
| `published_at` | `str` | `2026-03-08T14:30:00Z` |
| `section` | `str` | "Markets" |
| `keywords` | `list[str]` | `["treasurys", "bond market"]` |
| `image_url` | `str` | Lead image URL |
| `dek` | `str` | WSJ-specific sub-headline summary |
| `fetched` | `bool` | `True` on success |
| `error` | `str \| None` | Error message on failure |

**Body filtering:** Subscribe prompts, copyright notices, Dow Jones
legalese, and "What to Read Next" sections are stripped from article
content.

---

## 10. Government Reports (`gov_report.py`)

> **Transport:** Plain `requests.Session` with browser User-Agent — no
> Cloudflare or TLS fingerprint impersonation needed.

Scrapes **official government statistical releases** from US, CN, JP, and EU
institutions. Each source is defined as a declarative config dict with a
scraping strategy. The scraper fetches listing pages, finds the most recent
matching release, then extracts the full article content as Markdown.

### Data Type

`GovReportItem` (frozen dataclass):

| Field | Type | Example |
|-------|------|---------|
| `source` | `str` | `"gov_bls"`, `"gov_国家统计局"` |
| `source_id` | `str` | `"us_bls_cpi"`, `"cn_nbs_gdp"` |
| `title` | `str` | `"Consumer Price Index News Release"` |
| `url` | `str` | Detail page URL |
| `published_at` | `str` | `"2026-03-09"` (ISO date) |
| `institution` | `str` | `"BLS"`, `"国家统计局"`, `"ECB"` |
| `country` | `str` | `"US"`, `"CN"`, `"JP"`, `"EU"` |
| `language` | `str` | `"en"`, `"zh"` |
| `data_category` | `str` | `"inflation"`, `"gdp"`, `"monetary_policy"` |
| `importance` | `str` | `"high"`, `"medium"` |
| `content_markdown` | `str` | Full article body as Markdown (up to 15 000 chars) |

### Scraping Strategies

| Strategy | How it works |
|----------|-------------|
| `fixed_url` | Fetch a single known URL directly (e.g. BLS press releases) |
| `listing_keywords` | Fetch listing page, find first `<a>` matching keywords (+ optional `link_must_contain` URL filter), follow to detail page |
| `listing_regex` | Fetch listing page, find first `<a>` whose `href` matches a regex pattern, follow to detail page |
| `rss` | Parse RSS/Atom feed, fetch the most recent entry's detail page |

### GovReportClient (unified facade)

| Method | Returns | Description |
|--------|---------|-------------|
| `fetch_all()` | `list[GovReportItem]` | All regions sequentially |
| `fetch_us()` | `list[GovReportItem]` | US sources only |
| `fetch_cn()` | `list[GovReportItem]` | CN sources only |
| `fetch_jp()` | `list[GovReportItem]` | JP sources only |
| `fetch_eu()` | `list[GovReportItem]` | EU sources only |

Each region also has a standalone client (`USGovReportClient`,
`CNGovReportClient`, `JPGovReportClient`, `EUGovReportClient`) with a
single `fetch_all()` method.

### Sources by Region

**US (11 sources):**

| Source ID | Institution | Category | Strategy |
|-----------|-------------|----------|----------|
| `us_bls_cpi` | BLS | inflation | `fixed_url` |
| `us_bls_ppi` | BLS | inflation | `fixed_url` |
| `us_bls_nfp` | BLS | employment | `fixed_url` |
| `us_bea_gdp` | BEA | gdp | `listing_keywords` |
| `us_bea_pce` | BEA | inflation | `listing_keywords` |
| `us_bea_trade` | BEA | trade | `listing_keywords` |
| `us_fed_fomc_minutes` | Federal Reserve | monetary_policy | `listing_regex` |
| `us_fed_ip` | Federal Reserve | industrial_production | `listing_regex` |
| `us_census_retail` | Census Bureau | consumption | `fixed_url` |
| `us_census_housing` | Census Bureau | housing | `listing_keywords` |
| `us_treasury_tic` | Treasury | capital_flows | `listing_keywords` |
| `us_treasury_debt` | Treasury | fiscal_policy | `listing_keywords` |
| `us_umich_sentiment` | UMich | consumer_sentiment | `fixed_url` |

**CN (12 sources):**

| Source ID | Institution | Category | Strategy |
|-----------|-------------|----------|----------|
| `cn_nbs_cpi` | 国家统计局 | inflation | `listing_keywords` |
| `cn_nbs_ppi` | 国家统计局 | inflation | `listing_keywords` |
| `cn_nbs_gdp` | 国家统计局 | gdp | `listing_keywords` |
| `cn_nbs_pmi` | 国家统计局 | manufacturing | `listing_keywords` |
| `cn_nbs_industrial` | 国家统计局 | industrial_production | `listing_keywords` |
| `cn_nbs_retail` | 国家统计局 | consumption | `listing_keywords` |
| `cn_nbs_fai` | 国家统计局 | investment | `listing_keywords` |
| `cn_pboc_monetary` | 中国人民银行 | money_supply | `listing_keywords` |
| `cn_pboc_lpr` | 中国人民银行 | interest_rate | `listing_keywords` |
| `cn_customs_trade` | 海关总署 | trade | `listing_keywords` |
| `cn_mof_fiscal` | 财政部 | fiscal_policy | `listing_keywords` |
| `cn_mof_bonds` | 财政部 | bond_issuance | `listing_keywords` |
| `cn_safe_fx` | 国家外汇管理局 | fx_reserves | `listing_keywords` |
| `cn_caixin_pmi` | Caixin/S&P Global | manufacturing | `listing_keywords` |

**JP (4 sources):**

| Source ID | Institution | Category | Strategy |
|-----------|-------------|----------|----------|
| `jp_boj_statement` | Bank of Japan | monetary_policy | `listing_regex` |
| `jp_boj_outlook` | Bank of Japan | monetary_policy | `listing_regex` |
| `jp_boj_minutes` | Bank of Japan | monetary_policy | `listing_regex` |
| `jp_cao_gdp` | Cabinet Office | gdp | `listing_regex` |

**EU (8 sources):**

| Source ID | Institution | Category | Strategy |
|-----------|-------------|----------|----------|
| `eu_ecb_statement` | ECB | monetary_policy | `listing_regex` |
| `eu_ecb_minutes` | ECB | monetary_policy | `listing_regex` |
| `eu_ecb_bulletin` | ECB | economic_conditions | `listing_regex` |
| `eu_ecb_press` | ECB | press_releases | `rss` |
| `eu_ecb_speeches` | ECB | speeches | `rss` |
| `eu_eurostat_cpi` | Eurostat | inflation | `listing_keywords` |
| `eu_eurostat_gdp` | Eurostat | gdp | `listing_keywords` |
| `eu_eurostat_employment` | Eurostat | employment | `listing_keywords` |

**Typical yield:** ~28 items total (US 13, CN 7, JP 3, EU 5). Some sources
return `None` when no recent matching release is found or when the detail page
is a PDF. Failed fetches are logged as warnings and skipped.

**Anti-bot notes:** Chinese Customs (`cn_customs_trade`) returns 412
Precondition Failed due to server-side bot detection. This source may
intermittently fail; errors are caught and logged gracefully.

---

## 11. FRED / ALFRED (`fred.py`)

> **Transport:** Plain `requests.Session` — official public API.

### FredClient

Fetches **macro time-series** from FRED and **vintage/revision history** from
ALFRED (Archival FRED). Extracted from the inline implementation in `sources.py`.

| Method | Returns | Description |
|--------|---------|-------------|
| `get_series(series_id, *, start_date, limit=100)` | `list[FredObservation]` | Recent observations for a series |
| `get_series_info(series_id)` | `dict` | Series metadata (title, frequency, units) |
| `search_series(query, *, limit=10)` | `list[dict]` | Search FRED for series matching a text query |
| `get_vintages(series_id, *, start_date, vintage_dates=None)` | `list[FredVintageObservation]` | All vintage observations (ALFRED `output_type=2`) |
| `get_revision_history(series_id, observation_date)` | `list[FredVintageObservation]` | All revisions for a specific observation date |

**`FredObservation` fields:**

| Field | Type | Example |
|-------|------|---------|
| `series_id` | `str` | `"DGS10"` |
| `date` | `str` | `"2026-03-10"` |
| `value` | `float` | `4.25` |

**`FredVintageObservation` fields:**

| Field | Type | Example |
|-------|------|---------|
| `series_id` | `str` | `"GDP"` |
| `date` | `str` | `"2025-10-01"` (observation date) |
| `vintage_date` | `str` | `"2025-10-30"` (publication date) |
| `value` | `float` | `28538.1` |

**Auth:** `FRED_API_KEY` environment variable.

**Key vintage series:** `GDP`, `GDPC1`, `CPIAUCSL`, `PAYEMS`, `UNRATE`,
`INDPRO`, `RSAFS` — the monthly/quarterly macro that gets revised.

**Storage:** Latest values in `indicators` table (source `fred`), vintages
in `indicator_vintages` table (source `fred`).

---

## 12. EIA — Energy Information Administration (`eia.py`)

> **Transport:** Plain `requests.Session` — official public API v2.

### EIAClient

Fetches **US energy data** (oil, gas, electricity, renewables) from the
EIA Open Data API v2.

| Method | Returns | Description |
|--------|---------|-------------|
| `get_series(route, *, params, series_id, start=None, limit=100)` | `list[EIAObservation]` | Observations from a dataset route |
| `get_metadata(route)` | `dict` | Dataset metadata/facets |

**`EIAObservation` fields:**

| Field | Type | Example |
|-------|------|---------|
| `series_id` | `str` | `"EIA_BRENT"` |
| `date` | `str` | `"2026-03-10"` |
| `value` | `float` | `72.50` |
| `unit` | `str` | `"$/bbl"` |

**Auth:** `EIA_API_KEY` environment variable.
**Rate limit:** 0.5 s delay between requests.
**Max rows:** 5000 per request.

**Key series configured in `sources.py`:**

| Key | Series ID | Description |
|-----|-----------|-------------|
| `petroleum_brent` | `EIA_BRENT` | Brent crude oil spot prices |
| `petroleum_wti` | `EIA_WTI` | WTI crude oil spot prices |
| `petroleum_stocks` | `EIA_CRUDE_STOCKS` | Weekly petroleum stocks |
| `natgas_futures` | `EIA_NATGAS` | Natural gas futures |
| `petroleum_supply` | `EIA_PETROL_SUPPLY` | US petroleum supply & disposition |

**Storage:** `indicators` table (source `eia`).

---

## 13. Treasury Fiscal Data (`treasury_fiscal.py`)

> **Transport:** Plain `requests.Session` — fully open API, no key required.

### TreasuryFiscalClient

Fetches **federal fiscal data** from the Treasury Fiscal Data API:
total public debt, Treasury General Account balance, and average interest rates.

| Method | Returns | Description |
|--------|---------|-------------|
| `get_dataset(endpoint, *, fields, filter_str, sort, page_size)` | `list[dict]` | Raw rows from any fiscal data endpoint |
| `fetch_debt_outstanding(*, limit=30)` | `list[TreasuryFiscalObservation]` | Total public debt (Debt to the Penny) |
| `fetch_tga_balance(*, limit=30)` | `list[TreasuryFiscalObservation]` | Treasury General Account closing balance |
| `fetch_avg_interest_rates(*, limit=12)` | `list[TreasuryFiscalObservation]` | Average interest rates on Treasury securities |

**`TreasuryFiscalObservation` fields:**

| Field | Type | Example |
|-------|------|---------|
| `series_id` | `str` | `"TREAS_DEBT_TOTAL"` |
| `date` | `str` | `"2026-03-10"` |
| `value` | `float` | `36500000000000` |
| `metadata` | `dict` | `{"debt_held_public": "...", "intragov_holdings": "..."}` |

**Auth:** None — fully open.

**Storage:** `indicators` table (source `treasury_fiscal`).

---

## 14. IMF — SDMX 3.0 API (`imf.py`)

> **Transport:** Plain `requests.Session` — Azure-hosted API, requires
> `Ocp-Apim-Subscription-Key` header.  SDMX 3.0 supports JSON responses
> and point-in-time vintage queries via the `asOf` parameter.

### IMFClient

Fetches **macro time-series** from the IMF SDMX 3.0 API — CPI, FX reserves,
GDP, and trade dataflows covering China, Japan, Euro Area, and US.
Supports **vintage/revision history** via `asOf` queries.

**Base URL:** `https://api.imf.org/external/sdmx/3.0`

| Method | Returns | Description |
|--------|---------|-------------|
| `get_data(dataflow_id, key, *, series_id, version, start_period, limit)` | `list[IMFObservation]` | Observations from any SDMX dataflow |
| `get_vintages(dataflow_id, key, *, series_id, version, as_of_dates, start_period, limit)` | `list[IMFVintageObservation]` | Point-in-time vintage observations for multiple asOf dates |

**`IMFObservation` fields:**

| Field | Type | Example |
|-------|------|---------|
| `series_id` | `str` | `"IMF_CN_CPI"` |
| `date` | `str` | `"2025-09-01"` |
| `value` | `float` | `103.519` |
| `dataflow` | `str` | `"CPI"` |

**`IMFVintageObservation` fields:**

| Field | Type | Example |
|-------|------|---------|
| `series_id` | `str` | `"IMF_CN_GDP"` |
| `date` | `str` | `"2024-01-01"` (observation date) |
| `vintage_date` | `str` | `"2025-06-01"` (asOf date) |
| `value` | `float` | `30478.6` |
| `dataflow` | `str` | `"QNEA"` |

**Key series configured in `sources.py`:**

| Series ID | Dataflow | Version | Key | Description |
|-----------|----------|---------|-----|-------------|
| `IMF_CN_CPI` | CPI | 5.0.0 | CHN.CPI._T.IX.M | China CPI Index |
| `IMF_CN_GDP` | QNEA | 7.0.0 | CHN.B1GQ.V.NSA.XDC.Q | China Real GDP (LCU, NSA) |
| `IMF_CN_FX_RESERVES` | IRFCL | 11.0.0 | CHN.IRFCLDT1_IRFCL54_USD | China FX Reserves (USD) |
| `IMF_JP_CPI` | CPI | 5.0.0 | JPN.CPI._T.IX.M | Japan CPI Index |
| `IMF_JP_GDP` | QNEA | 7.0.0 | JPN.B1GQ.V.SA.XDC.Q | Japan Real GDP (LCU, SA) |
| `IMF_EU_CPI` | CPI | 5.0.0 | G163.HICP._T.IX.M | Euro Area HICP Index |
| `IMF_GLOBAL_TRADE` | ITG | 4.0.0 | USA.XG.FOB_USD.M | US Exports of Goods (USD) |

**Vintage series:** `cn_gdp`, `jp_gdp` — GDP is the classic revision-tracked macro aggregate.

**Country codes:** ISO alpha-3 (CHN, JPN, USA) — not the legacy 2-letter codes.
**Date format:** `2025-M09` for monthly, `2024-Q1` for quarterly.
**URL format:** `/data/dataflow/IMF.STA/{flow}/{version}/{key}` — requires exact dataflow version (wildcard `*` fails for multi-DSD flows).
**Auth:** `IMF_API_KEY` environment variable (subscription key from https://portal.api.imf.org).
**Rate limit:** 1.0 s delay between requests.
**Storage:** `indicators` table (source `imf`), vintages in `indicator_vintages` table (source `imf`).

---

## 15. Eurostat — Euro Area Indicators (`eurostat.py`)

> **Transport:** Plain `requests.Session` — official public API, no key required.

### EurostatClient

Fetches **Euro Area structured indicators** from the Eurostat JSON-stat
dissemination API — HICP, GDP, unemployment, industrial production, trade.

| Method | Returns | Description |
|--------|---------|-------------|
| `get_dataset(dataset_code, *, params, series_id, limit)` | `list[EurostatObservation]` | Observations from a dataset |

**`EurostatObservation` fields:**

| Field | Type | Example |
|-------|------|---------|
| `series_id` | `str` | `"ESTAT_HICP"` |
| `date` | `str` | `"2024-01-01"` |
| `value` | `float` | `2.8` |
| `dataset` | `str` | `"prc_hicp_manr"` |

**Key series configured in `sources.py`:**

| Series ID | Dataset | Description |
|-----------|---------|-------------|
| `ESTAT_HICP` | prc_hicp_manr | EA HICP YoY % |
| `ESTAT_GDP` | namq_10_gdp | EA GDP QoQ % |
| `ESTAT_UNEMPLOYMENT` | une_rt_m | EA Unemployment Rate |
| `ESTAT_INDPRO` | sts_inpr_m | EA Industrial Production MoM |
| `ESTAT_ESI` | teibs010 | EA Economic Sentiment Indicator |

**Auth:** None — fully open.
**Rate limit:** 0.5 s delay between requests.
**Storage:** `indicators` table (source `eurostat`).

---

## 16. BIS — Bank for International Settlements (`bis.py`)

> **Transport:** Plain `requests.Session` — official public API, no key required.

### BISClient

Fetches **cross-border data** from the BIS SDMX-JSON API — policy rates,
effective exchange rates, credit gaps, and property prices across US, EU,
JP, CN, and GB.

| Method | Returns | Description |
|--------|---------|-------------|
| `get_data(dataflow_id, key, *, series_id, start_period, limit)` | `list[BISObservation]` | Observations from a dataflow |

**`BISObservation` fields:**

| Field | Type | Example |
|-------|------|---------|
| `series_id` | `str` | `"BIS_POLICY_US"` |
| `date` | `str` | `"2024-01-01"` |
| `value` | `float` | `4.33` |
| `dataflow` | `str` | `"WS_CBPOL"` |

**Key series configured in `sources.py`:**

| Series ID | Dataflow | Key | Description |
|-----------|----------|-----|-------------|
| `BIS_POLICY_US` | WS_CBPOL | M.US | US Policy Rate |
| `BIS_POLICY_EU` | WS_CBPOL | M.XM | ECB Policy Rate |
| `BIS_POLICY_JP` | WS_CBPOL | M.JP | BOJ Policy Rate |
| `BIS_POLICY_CN` | WS_CBPOL | M.CN | PBOC Policy Rate |
| `BIS_POLICY_GB` | WS_CBPOL | M.GB | BOE Policy Rate |
| `BIS_EER_US` | WS_EER | M.R.B.US | US Real Effective Exchange Rate |
| `BIS_EER_CN` | WS_EER | M.R.B.CN | CN Real Effective Exchange Rate |
| `BIS_EER_EU` | WS_EER | M.R.B.XM | EU Real Effective Exchange Rate |
| `BIS_CREDIT_GAP_US` | WS_CREDIT_GAP | Q.US.P | US Credit-to-GDP Gap |
| `BIS_CREDIT_GAP_CN` | WS_CREDIT_GAP | Q.CN.P | CN Credit-to-GDP Gap |
| `BIS_PROPERTY_US` | WS_SPP | Q.R.US | US Real Property Prices |
| `BIS_PROPERTY_CN` | WS_SPP | Q.R.CN | CN Real Property Prices |

**Auth:** None — fully open.
**Rate limit:** 0.5 s delay between requests.
**Storage:** `indicators` table (source `bis`).

---

## 17. ECB — SDMX 2.1 API (`ecb.py`)

> **Transport:** Plain `requests.Session` — official public API, no key required.

### ECBClient

Fetches **Euro Area monetary data** from the ECB Data Portal SDMX API —
money supply (M1/M2/M3), deposit facility rate, and EUR/USD exchange rate.

**Base URL:** `https://data-api.ecb.europa.eu/service/data`

| Method | Returns | Description |
|--------|---------|-------------|
| `get_data(dataflow_id, key, *, series_id, start_period, limit)` | `list[ECBObservation]` | Observations from any SDMX dataflow |

**`ECBObservation` fields:**

| Field | Type | Example |
|-------|------|---------|
| `series_id` | `str` | `"ECB_EA_M1"` |
| `date` | `str` | `"2024-12-01"` |
| `value` | `float` | `16700000.0` |
| `dataflow` | `str` | `"BSI"` |

**Key series configured in `sources.py`:**

| Series ID | Dataflow | Key | Description |
|-----------|----------|-----|-------------|
| `ECB_EA_M1` | BSI | M.U2.Y.V.M10.X.I.U2.2300.Z01.E | EA M1 Money Supply |
| `ECB_EA_M2` | BSI | M.U2.Y.V.M20.X.I.U2.2300.Z01.E | EA M2 Money Supply |
| `ECB_EA_M3` | BSI | M.U2.Y.V.M30.X.I.U2.2300.Z01.E | EA M3 Money Supply |
| `ECB_EA_M3_GROWTH` | BSI | M.U2.Y.V.M30.X.R.A.2300.Z01.E | EA M3 Annual Growth Rate |
| `ECB_EA_DEPOSIT_RATE` | FM | B.U2.EUR.4F.KR.DFR.LEV | ECB Deposit Facility Rate |
| `ECB_EURUSD` | EXR | M.USD.EUR.SP00.A | EUR/USD Exchange Rate |

**Auth:** None — fully open.
**Rate limit:** 0.5 s delay between requests.
**Storage:** `indicators` table (source `ecb`).

---

## 18. OECD — SDMX REST v2 API (`oecd.py`)

> **Transport:** Plain `requests.Session` — official public API, no key required.

### OECDClient

Fetches OECD Data Explorer data through the SDMX REST API. The client now
supports both the original curated macro series and catalogue-driven discovery
across OECD Data Explorer dataflows.

**Base URL:** `https://sdmx.oecd.org/public/rest`

| Method | Returns | Description |
|--------|---------|-------------|
| `list_dataflows(*, agency_id, version)` | `list[OECDDataflow]` | Enumerate OECD/Data Explorer dataflows from the live catalogue |
| `search_dataflows(query, *, agency_id, limit)` | `list[OECDDataflow]` | Search the live OECD catalogue by id/name/description |
| `get_dataflow(dataflow_id, *, agency_id, version)` | `OECDDataflow` | Resolve one dataflow and its live version metadata |
| `get_structure(dataflow_id, *, agency_id, version)` | `OECDDataStructure` | Resolve datastructure and codelists for a dataflow |
| `summarize_structure(dataflow_id, *, agency_id, version)` | `OECDStructureSummary` | Compact summary for inspection / config generation |
| `build_key(dataflow_id, filters, *, agency_id, version, use_defaults)` | `str` | Build an exact SDMX series key from dimension filters |
| `enumerate_series(dataflow_id, *, agency_id, version, key, filters, observation_limit, max_series)` | `list[OECDSeries]` | Sample concrete series from a dataflow |
| `series_to_filters(dataflow_id, series, *, agency_id, version)` | `dict[str, str]` | Convert a sampled series back into exact dimension filters |
| `fetch_data(dataflow_id, *, agency_id, version, key, filters, series_id, start_period, end_period, limit)` | `list[OECDObservation]` | Fetch observations from a dataflow |
| `get_data(dataflow_id, version, key, *, series_id, start_period, limit)` | `list[OECDObservation]` | Observations from an SDMX dataflow with dimension key filter in URL path |

**`OECDObservation` fields:**

| Field | Type | Example |
|-------|------|---------|
| `series_id` | `str` | `"OECD_CLI_US"` |
| `date` | `str` | `"2024-12-01"` |
| `value` | `float` | `100.1` |
| `dataflow` | `str` | `"DSD_STES@DF_CLI"` |
| `series_key` | `str` | `"USA.M.LI.IX._Z.NOR.IX._Z.H"` |

**Key series configured in `sources.py`:**

| Series ID | Dataflow | Version | Key | Description |
|-----------|----------|---------|-----|-------------|
| `OECD_CLI_US` | DSD_STES@DF_CLI | 4.1 | USA.M.LI.IX._Z.NOR.IX._Z.H | US Composite Leading Indicator |
| `OECD_CLI_CN` | DSD_STES@DF_CLI | 4.1 | CHN.M.LI.IX._Z.NOR.IX._Z.H | CN Composite Leading Indicator |
| `OECD_CLI_JP` | DSD_STES@DF_CLI | 4.1 | JPN.M.LI.IX._Z.NOR.IX._Z.H | JP Composite Leading Indicator |
| `OECD_CLI_EU` | DSD_STES@DF_CLI | 4.1 | G4E.M.LI.IX._Z.NOR.IX._Z.H | Major 4 EU CLI |
| `OECD_CONSUMER_CONF_US` | DSD_STES@DF_CS | 4.0 | USA.M.CCICP.\*.\*.\*.\*.\*.\* | US Consumer Confidence (OECD) |
| `OECD_BUSINESS_CONF_US` | DSD_STES@DF_BTS | 4.0 | USA.M.BCICP.\*.\*.\*.\*.\*.\* | US Business Confidence (OECD) |
| `OECD_UNEMP_US` | DSD_KEI@DF_KEI | 4.0 | USA.M.UNEMP.PT_LF._T.Y._Z | US Unemployment Rate |

**Auth:** None — fully open.
**Rate limit:** 1.0 s delay between requests.
**Storage:** `indicators` table (source `oecd`).

**Catalogue support:** `OECDIngestionClient` can now:
- list OECD dataflows across Data Explorer
- summarize live datastructures
- generate reviewable `OECDSeriesConfig` snippets from sampled series
- dynamically ingest latest observations from arbitrary OECD dataflows with non-hardcoded series ids

---

## 19. World Bank — REST API (`worldbank.py`)

> **Transport:** Plain `requests.Session` — official public API, no key required.

### WorldBankClient

Fetches **development indicators** from the World Bank Indicators API v2 —
GDP per capita (PPP), GDP growth, and current account balance as % of GDP.

**Base URL:** `https://api.worldbank.org/v2`

**Response format:** `[{page_info}, [{record}, ...]]` — page metadata in
first element, data array in second element.

| Method | Returns | Description |
|--------|---------|-------------|
| `get_indicator(indicator_code, country, *, series_id, start_year, limit)` | `list[WorldBankObservation]` | Observations for a country indicator |

**`WorldBankObservation` fields:**

| Field | Type | Example |
|-------|------|---------|
| `series_id` | `str` | `"WB_GDP_PCAP_US"` |
| `date` | `str` | `"2023-01-01"` |
| `value` | `float` | `85000.5` |
| `indicator` | `str` | `"NY.GDP.PCAP.PP.CD"` |

**Key series configured in `sources.py`:**

| Series ID | Indicator | Country | Description |
|-----------|-----------|---------|-------------|
| `WB_GDP_PCAP_US` | NY.GDP.PCAP.PP.CD | USA | US GDP per Capita PPP |
| `WB_GDP_PCAP_CN` | NY.GDP.PCAP.PP.CD | CHN | CN GDP per Capita PPP |
| `WB_GDP_GROWTH_US` | NY.GDP.MKTP.KD.ZG | USA | US GDP Growth % |
| `WB_CA_GDP_US` | BN.CAB.XOKA.GD.ZS | USA | US Current Account % GDP |

**Auth:** None — fully open.
**Rate limit:** 0.5 s delay between requests.
**Storage:** `indicators` table (source `worldbank`).

---

## Summary Matrix

| Site | Calendar | News | Articles | Indicators | Markets | Gov Reports |
|------|:--------:|:----:|:--------:|:----------:|:-------:|:-----------:|
| **Investing.com** | `InvestingCalendarClient` | `InvestingNewsClient` | — | — | — | — |
| **ForexFactory** | `ForexFactoryCalendarClient` | `ForexFactoryNewsClient` | — | — | — | — |
| **TradingEconomics** | `TradingEconomicsCalendarClient` | `TradingEconomicsNewsClient` | — | `TradingEconomicsIndicatorsClient` | `TradingEconomicsMarketsClient` | — |
| **Reuters** | — | `ReutersNewsClient` | `ReutersArticleClient` | — | — | — |
| **Bloomberg** | — | `BloombergNewsClient` | `BloombergArticleClient` | — | — | — |
| **FT** | — | `FTNewsClient` | `FTArticleClient` | — | — | — |
| **WSJ** | — | `WSJNewsClient` | `WSJArticleClient` | — | — | — |
| **rateprobability.com** | — | — | — | `RateProbabilityClient` | — | — |
| **NY Fed** | — | — | — | `NYFedRatesClient` | — | — |
| **Gov (US/CN/JP/EU)** | — | — | — | — | — | `GovReportClient` |
| **FRED / ALFRED** | — | — | — | `FredClient` | — | — |
| **EIA** | — | — | — | `EIAClient` | — | — |
| **Treasury Fiscal** | — | — | — | `TreasuryFiscalClient` | — | — |
| **IMF** | — | — | — | `IMFClient` | — | — |
| **Eurostat** | — | — | — | `EurostatClient` | — | — |
| **BIS** | — | — | — | `BISClient` | — | — |
| **ECB** | — | — | — | `ECBClient` | — | — |
| **OECD** | — | — | — | `OECDClient` | — | — |
| **World Bank** | — | — | — | `WorldBankClient` | — | — |

## Running Tests

```bash
# Unit tests only (no network, fast)
pytest tests/test_scrapers.py -m "not live" -v

# Live integration tests (hits real endpoints)
pytest tests/test_scrapers.py -m live -v

# All scraper tests
pytest tests/test_scrapers.py -v

# Government report scraper tests
pytest tests/test_gov_report.py -x -q

# Structured data API tests (FRED, EIA, Treasury, IMF, Eurostat, BIS)
pytest tests/test_fred.py tests/test_eia.py tests/test_treasury_fiscal.py tests/test_imf.py tests/test_eurostat.py tests/test_bis.py tests/test_ecb.py tests/test_oecd.py tests/test_worldbank.py -x -q
```
