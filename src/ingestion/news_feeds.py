"""RSS/Atom feed definitions for macro-finance news."""

from dataclasses import dataclass


@dataclass(frozen=True)
class FeedInfo:
    name: str
    url: str
    category: str


def _gnews(query: str, when: str = "1d") -> str:
    """Build a Google News RSS search URL."""
    return f"https://news.google.com/rss/search?q={query}+when:{when}&hl=en-US&gl=US&ceid=US:en"


RSS_FEEDS: list[FeedInfo] = [
    # -- markets (8) --
    FeedInfo("CNBC", "https://www.cnbc.com/id/100003114/device/rss/rss.html", "markets"),
    FeedInfo("MarketWatch", _gnews("site:marketwatch.com+markets", "1d"), "markets"),
    FeedInfo("Yahoo Finance", "https://finance.yahoo.com/rss/topstories", "markets"),
    FeedInfo("Seeking Alpha", "https://seekingalpha.com/market_currents.xml", "markets"),
    FeedInfo("Reuters Markets", _gnews("site:reuters.com+markets+stocks", "1d"), "markets"),
    FeedInfo("Bloomberg Markets", _gnews("site:bloomberg.com+markets", "1d"), "markets"),
    FeedInfo("Investing.com News", _gnews("site:investing.com+markets", "1d"), "markets"),
    FeedInfo("WSJ Markets", "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain", "markets"),
    FeedInfo("BBC Business", "https://feeds.bbci.co.uk/news/business/rss.xml", "markets"),
    FeedInfo("Nikkei Asia", "https://asia.nikkei.com/rss/feed/nar", "markets"),

    # -- forex (3) --
    FeedInfo("Forex News", _gnews('("forex"+OR+"currency"+OR+"FX+market")+trading', "1d"), "forex"),
    FeedInfo("Dollar Watch", _gnews('("dollar+index"+OR+DXY+OR+"US+dollar"+forex+OR+"euro+dollar"+trading)', "2d"), "forex"),
    FeedInfo("Central Bank Rates", _gnews('("central+bank"+rate+OR+"Fed+rate"+OR+"interest+rate+decision"+OR+"monetary+policy")+-crypto+-bitcoin', "2d"), "forex"),

    # -- bonds (3) --
    FeedInfo("Bond Market", _gnews('("bond+market"+OR+"treasury+yields"+OR+"bond+yields"+OR+"10-year+yield"+OR+"fixed+income")+-site:vietnambiz.vn+-site:vietstock.vn', "2d"), "bonds"),
    FeedInfo("Treasury Watch", _gnews('("US+Treasury"+OR+"Treasury+auction"+OR+"10-year+yield"+OR+"2-year+yield")', "2d"), "bonds"),
    FeedInfo("Corporate Bonds", _gnews('("corporate+bond"+OR+"high+yield+bond"+OR+"high+yield+debt"+OR+"investment+grade+bond"+OR+"credit+spread")', "3d"), "bonds"),

    # -- commodities (4) --
    FeedInfo("Oil & Gas", _gnews("(oil+price+OR+OPEC+OR+%22natural+gas%22+OR+%22crude+oil%22+OR+WTI+OR+Brent)", "1d"), "commodities"),
    FeedInfo("Gold & Metals", _gnews('("gold+price"+OR+"silver+price"+OR+"copper+futures"+OR+"platinum+futures"+OR+"precious+metals")+-site:meyka.com', "2d"), "commodities"),
    FeedInfo("Agriculture", _gnews("(wheat+OR+corn+OR+soybeans+OR+coffee+OR+sugar)+price+OR+commodity", "3d"), "commodities"),
    FeedInfo("Commodity Trading", _gnews('("commodity+trading"+OR+"futures+market"+OR+CME+OR+NYMEX+OR+COMEX)', "2d"), "commodities"),
    FeedInfo("Uranium & Nuclear", _gnews("uranium+OR+%22nuclear+energy%22+price+OR+market+OR+supply", "3d"), "commodities"),
    FeedInfo("Lithium & Battery Metals", _gnews("lithium+OR+cobalt+OR+nickel+%22battery%22+price+OR+supply+OR+EV", "3d"), "commodities"),

    # -- crypto (4) --
    FeedInfo("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/", "crypto"),
    FeedInfo("Cointelegraph", "https://cointelegraph.com/rss", "crypto"),
    FeedInfo("The Block", _gnews("site:theblock.co", "1d"), "crypto"),
    FeedInfo("Crypto News", _gnews('(bitcoin+OR+ethereum+OR+crypto+OR+"digital+assets")', "1d"), "crypto"),
    FeedInfo("DeFi News", _gnews('(DeFi+OR+"decentralized+finance"+OR+DEX+OR+"yield+farming")', "3d"), "crypto"),

    # -- centralbanks (8) --
    FeedInfo("Federal Reserve", "https://www.federalreserve.gov/feeds/press_all.xml", "centralbanks"),
    FeedInfo("ECB Press", "https://www.ecb.europa.eu/rss/press.html", "centralbanks"),
    FeedInfo("ECB Watch", _gnews('("European+Central+Bank"+OR+ECB+OR+Lagarde)+(rate+OR+euro+OR+inflation+OR+"monetary+policy")+-crypto+-bitcoin', "3d"), "centralbanks"),
    FeedInfo("BoE Official", "https://www.bankofengland.co.uk/rss/news", "centralbanks"),
    FeedInfo("BoJ Watch", _gnews('("Bank+of+Japan"+OR+BoJ)+(rate+OR+yen+OR+yield+OR+"monetary+policy")+-crypto+-bitcoin', "3d"), "centralbanks"),
    FeedInfo("BoE Watch", _gnews('("Bank+of+England"+OR+BoE)+(rate+OR+sterling+OR+inflation+OR+"monetary+policy")+-crypto+-bitcoin', "3d"), "centralbanks"),
    FeedInfo("PBoC Watch", _gnews('("People%27s+Bank+of+China"+OR+PBoC+OR+PBOC)', "7d"), "centralbanks"),
    FeedInfo("Global Central Banks", _gnews('("rate+hike"+OR+"rate+cut"+OR+"interest+rate+decision")+central+bank', "3d"), "centralbanks"),
    FeedInfo("RBI India", _gnews('%22Reserve+Bank+of+India%22+OR+RBI+rate+OR+rupee+OR+%22monetary+policy%22', "3d"), "centralbanks"),
    FeedInfo("Central Bank Balance Sheets", _gnews('%22quantitative+easing%22+OR+%22quantitative+tightening%22+OR+%22balance+sheet%22+%22central+bank%22', "7d"), "centralbanks"),

    # -- economic (5) --
    FeedInfo("WSJ Economy", "https://feeds.content.dowjones.io/public/rss/socialeconomyfeed", "economic"),
    FeedInfo("IMF News", "https://www.imf.org/en/news/rss", "economic"),
    FeedInfo("Economic Data", _gnews('(CPI+OR+inflation+OR+GDP+OR+"jobs+report"+OR+"nonfarm+payrolls"+OR+PMI)', "2d"), "economic"),
    FeedInfo("Trade & Tariffs", _gnews('(tariff+OR+"trade+war"+OR+"trade+deficit"+OR+sanctions)', "2d"), "economic"),
    FeedInfo("Housing Market", _gnews('("housing+market"+OR+"home+prices"+OR+"mortgage+rates"+OR+REIT)', "3d"), "economic"),

    # -- ipo (3) --
    FeedInfo("IPO News", _gnews('(IPO+OR+"initial+public+offering"+OR+SPAC+OR+"direct+listing")', "3d"), "ipo"),
    FeedInfo("Earnings Reports", _gnews('("earnings+report"+OR+"quarterly+earnings"+OR+"earnings+beat"+OR+"earnings+surprise"+OR+"earnings+miss")+-site:defenseworld.net+-site:simplywall.st', "2d"), "ipo"),
    FeedInfo("M&A News", _gnews('("merger"+OR+"acquisition"+OR+"takeover+bid"+OR+"buyout")+billion', "3d"), "ipo"),

    # -- derivatives (2) --
    FeedInfo("Options Market", _gnews('(VIX+OR+"equity+options"+OR+"options+expiration"+OR+"options+market"+OR+"put+call+ratio")+-site:vietnambiz.vn', "2d"), "derivatives"),
    FeedInfo("Futures Trading", _gnews('("futures+trading"+OR+"S%26P+500+futures"+OR+"Nasdaq+futures")', "1d"), "derivatives"),

    # -- fintech (3) --
    FeedInfo("Fintech News", _gnews('(fintech+OR+"payment+technology"+OR+"neobank"+OR+"digital+banking")', "3d"), "fintech"),
    FeedInfo("Trading Tech", _gnews('("algorithmic+trading"+OR+"trading+platform"+OR+"quantitative+finance")', "7d"), "fintech"),
    FeedInfo("Blockchain Finance", _gnews('("blockchain+finance"+OR+"tokenization"+OR+"digital+securities"+OR+CBDC)', "7d"), "fintech"),

    # -- regulation (5) --
    FeedInfo("SEC", "https://www.sec.gov/news/pressreleases.rss", "regulation"),
    FeedInfo("BIS", "https://www.bis.org/doclist/all_pressrels.rss", "regulation"),
    FeedInfo("Financial Regulation", _gnews("(SEC+OR+CFTC+OR+FINRA+OR+FCA)+regulation+OR+enforcement", "3d"), "regulation"),
    FeedInfo("Banking Rules", _gnews('("Basel+III"+OR+"Basel+IV"+OR+"Basel+regulation"+OR+"Basel+accord"+OR+"capital+requirements"+bank+OR+"banking+regulation")', "7d"), "regulation"),
    FeedInfo("Crypto Regulation", _gnews('(crypto+regulation+OR+"digital+asset"+regulation+OR+"stablecoin"+regulation)', "7d"), "regulation"),

    # -- institutional (3) --
    FeedInfo("Hedge Fund News", _gnews('("hedge+fund"+OR+"Bridgewater+Associates"+OR+"Citadel+LLC"+OR+"Renaissance+Technologies"+OR+"Two+Sigma"+OR+"DE+Shaw")', "7d"), "institutional"),
    FeedInfo("Private Equity", _gnews('("private+equity"+OR+Blackstone+OR+KKR+OR+Apollo+OR+Carlyle)', "3d"), "institutional"),
    FeedInfo("Sovereign Wealth", _gnews('("sovereign+wealth+fund"+OR+"pension+fund"+OR+"institutional+investor")', "7d"), "institutional"),

    # -- analysis (4) --
    FeedInfo("NBER Working Papers", "https://www.nber.org/rss/new.xml", "analysis"),
    FeedInfo("Market Outlook", _gnews('("stock+market+outlook"+OR+"Wall+Street+outlook"+OR+"stock+market+forecast"+OR+"bull+market"+OR+"bear+market")+-site:openpr.com', "3d"), "analysis"),
    FeedInfo("Risk & Volatility", _gnews('("S%26P+500"+volatility+OR+VIX+OR+CBOE+OR+"risk+off"+stocks+OR+"market+correction")+-site:marketsmojo.com', "3d"), "analysis"),
    FeedInfo("Bank Research", _gnews('("Goldman+Sachs"+OR+"JPMorgan"+OR+"Morgan+Stanley")+forecast+OR+outlook', "3d"), "analysis"),

    # -- china (7 RSS) --
    FeedInfo("SCMP China Economy", "https://www.scmp.com/rss/318421/feed", "china"),
    FeedInfo("SCMP China", "https://www.scmp.com/rss/4/feed", "china"),
    FeedInfo("SCMP Business", "https://www.scmp.com/rss/92/feed", "china"),
    FeedInfo("Xinhua China", "http://www.xinhuanet.com/english/rss/chinarss.xml", "china"),
    FeedInfo("Xinhua Business", "http://www.xinhuanet.com/english/rss/businessrss.xml", "china"),
    FeedInfo("China Daily BizChina", "https://www.chinadaily.com.cn/rss/bizchina_rss.xml", "china"),
    FeedInfo("China Daily News", "https://www.chinadaily.com.cn/rss/china_rss.xml", "china"),
    FeedInfo("CGTN Business", "https://www.cgtn.com/subscribe/rss/section/business.xml", "china"),
    FeedInfo("China Trade Watch", _gnews('(China+trade+OR+"China+tariff"+OR+"US+China"+trade)', "2d"), "china"),
    FeedInfo("China Markets", _gnews('("Shanghai+composite"+OR+"Hang+Seng"+OR+"CSI+300"+OR+"A-shares"+OR+"H-shares")', "2d"), "china"),
    FeedInfo("China Tech", _gnews('(Alibaba+OR+Tencent+OR+BYD+OR+Huawei+OR+Xiaomi)+stock+OR+earnings+OR+regulation', "3d"), "china"),
    FeedInfo("China Policy", _gnews('("China+GDP"+OR+"China+PMI"+OR+"China+CPI"+OR+"China+stimulus"+OR+"NPC"+OR+"NDRC")', "3d"), "china"),

    # -- thinktanks (5) --
    FeedInfo("Foreign Policy", "https://foreignpolicy.com/feed/", "thinktanks"),
    FeedInfo("Atlantic Council", "https://www.atlanticcouncil.org/feed/", "thinktanks"),
    FeedInfo("AEI", "https://www.aei.org/feed/", "thinktanks"),
    FeedInfo("CSIS", _gnews("site:csis.org", "7d"), "thinktanks"),
    FeedInfo("War on the Rocks", "https://warontherocks.com/feed", "thinktanks"),
    FeedInfo("Brookings", "https://www.brookings.edu/feed/", "thinktanks"),
    FeedInfo("Carnegie", "https://carnegieendowment.org/rss/solr/?lang=en", "thinktanks"),
    FeedInfo("International Crisis Group", _gnews("site:crisisgroup.org", "7d"), "thinktanks"),

    # -- government (2) --
    FeedInfo("Federal Reserve (Gov)", "https://www.federalreserve.gov/feeds/press_all.xml", "government"),
    FeedInfo("SEC (Gov)", "https://www.sec.gov/news/pressreleases.rss", "government"),

    # -- wireservices (4) --
    FeedInfo("AP News", _gnews("source:associated_press", "1d"), "wireservices"),
    FeedInfo("France24 World", "https://www.france24.com/en/rss", "wireservices"),
    FeedInfo("France24 Asia-Pacific", "https://www.france24.com/en/asia-pacific/rss", "wireservices"),
    FeedInfo("France24 Business", "https://www.france24.com/en/business/rss", "wireservices"),

    # -- global (6 RSS) --
    FeedInfo("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml", "global"),
    FeedInfo("BBC Asia", "https://feeds.bbci.co.uk/news/world/asia/rss.xml", "global"),
    FeedInfo("CNA Asia", "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=6511", "global"),
    FeedInfo("CNA Business", "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=6936", "global"),
    FeedInfo("Economist Leaders", "https://www.economist.com/leaders/rss.xml", "global"),
    FeedInfo("Economist Finance", "https://www.economist.com/finance-and-economics/rss.xml", "global"),
    FeedInfo("Forbes Business", "https://www.forbes.com/business/feed", "global"),

    # -- energy (5) --
    FeedInfo("IEA News", "https://www.iea.org/news/rss", "energy"),
    FeedInfo("OPEC News", _gnews("site:opec.org", "3d"), "energy"),
    FeedInfo("LNG & Gas", _gnews('LNG+OR+%22liquefied+natural+gas%22+OR+%22natural+gas%22+supply+OR+price', "2d"), "energy"),
    FeedInfo("Energy Intelligence", _gnews('%22energy+market%22+OR+%22power+grid%22+OR+%22energy+crisis%22+OR+%22energy+transition%22', "2d"), "energy"),
    FeedInfo("Renewable Energy", _gnews('%22renewable+energy%22+OR+%22solar+energy%22+OR+%22wind+power%22+investment+OR+market', "3d"), "energy"),

    # -- shipping (4) --
    FeedInfo("gCaptain", "https://gcaptain.com/feed/", "shipping"),
    FeedInfo("Shipping Watch", _gnews('%22Baltic+Dry+Index%22+OR+%22container+shipping%22+OR+%22freight+rate%22', "2d"), "shipping"),
    FeedInfo("Port & Logistics", _gnews('%22port+congestion%22+OR+%22supply+chain+disruption%22+OR+%22shipping+delay%22', "2d"), "shipping"),
    FeedInfo("Splash247", _gnews("site:splash247.com+shipping", "3d"), "shipping"),

    # -- emergingmarkets (8) --
    FeedInfo("India Markets", _gnews('India+%22stock+market%22+OR+Sensex+OR+Nifty+OR+rupee', "2d"), "emergingmarkets"),
    FeedInfo("ASEAN Markets", _gnews("ASEAN+OR+Vietnam+OR+Thailand+OR+Indonesia+stock+OR+market+OR+economy", "2d"), "emergingmarkets"),
    FeedInfo("Brazil & LatAm", _gnews('Brazil+OR+Mexico+%22stock+market%22+OR+Bovespa+OR+peso+OR+real+economy', "2d"), "emergingmarkets"),
    FeedInfo("Middle East Markets", _gnews('Saudi+OR+UAE+OR+%22Gulf%22+stock+OR+market+OR+%22sovereign+wealth%22', "2d"), "emergingmarkets"),
    FeedInfo("Africa Markets", _gnews('Africa+%22stock+market%22+OR+%22sovereign+bond%22+OR+%22frontier+market%22', "3d"), "emergingmarkets"),
    FeedInfo("Turkey & Eastern Europe", _gnews("Turkey+OR+Poland+lira+OR+economy+OR+%22emerging+market%22", "3d"), "emergingmarkets"),
    FeedInfo("EM Currency Crisis", _gnews('%22emerging+market%22+currency+OR+crisis+OR+devaluation+OR+%22capital+outflow%22', "2d"), "emergingmarkets"),
    FeedInfo("EM Sovereign Debt", _gnews('%22sovereign+debt%22+OR+%22sovereign+bond%22+OR+%22debt+restructuring%22+emerging', "3d"), "emergingmarkets"),

    # -- energy_geopol (4) --
    FeedInfo("Sanctions & Export Controls", _gnews("sanctions+OR+%22export+controls%22+OR+OFAC+financial+OR+energy", "2d"), "energy_geopol"),
    FeedInfo("Middle East Oil Risk", _gnews('%22Middle+East%22+OR+Iran+OR+%22Red+Sea%22+oil+OR+tanker+OR+shipping', "2d"), "energy_geopol"),
    FeedInfo("Russia Energy", _gnews("Russia+OR+Ukraine+energy+OR+gas+OR+oil+OR+pipeline+sanctions", "2d"), "energy_geopol"),
    FeedInfo("Supply Chain Geopol", _gnews('%22supply+chain%22+risk+OR+disruption+geopolitical+OR+war+OR+conflict', "2d"), "energy_geopol"),

    # -- debt (5) --
    FeedInfo("Credit Ratings", _gnews('%22credit+rating%22+OR+%22Moody%27s%22+OR+%22S%26P+Global%22+OR+Fitch+downgrade+OR+upgrade', "3d"), "debt"),
    FeedInfo("High Yield & Distress", _gnews('%22high+yield%22+OR+%22junk+bond%22+OR+%22distressed+debt%22+OR+%22credit+spread%22', "3d"), "debt"),
    FeedInfo("Sovereign Default", _gnews('%22sovereign+default%22+OR+%22debt+crisis%22+OR+%22debt+ceiling%22+OR+IMF+bailout', "3d"), "debt"),
    FeedInfo("Corporate Debt", _gnews('%22corporate+bond%22+issuance+OR+refinancing+OR+%22leveraged+loan%22', "3d"), "debt"),
    FeedInfo("World Bank", "https://www.worldbank.org/en/news/rss.xml", "debt"),

    # -- labor (4) --
    FeedInfo("Wage Inflation", _gnews('%22wage+growth%22+OR+%22wage+inflation%22+OR+%22labor+cost%22+OR+%22minimum+wage%22', "3d"), "labor"),
    FeedInfo("Labor Disputes", _gnews("strike+OR+%22labor+dispute%22+OR+%22union+negotiation%22+OR+walkout+industry", "3d"), "labor"),
    FeedInfo("Jobless Claims", _gnews('%22jobless+claims%22+OR+%22unemployment+rate%22+OR+%22job+openings%22+OR+JOLTS', "2d"), "labor"),
    FeedInfo("Layoffs & Restructuring", _gnews("layoffs+OR+%22mass+layoff%22+OR+restructuring+OR+%22job+cuts%22+tech+OR+finance", "2d"), "labor"),

    # -- realestate (4) --
    FeedInfo("Commercial RE", _gnews('%22commercial+real+estate%22+OR+%22office+vacancy%22+OR+%22CRE+market%22', "3d"), "realestate"),
    FeedInfo("REIT News", _gnews('REIT+OR+%22real+estate+investment+trust%22+earnings+OR+dividend', "3d"), "realestate"),
    FeedInfo("Property Markets", _gnews('%22property+market%22+OR+%22home+prices%22+OR+%22real+estate%22+crash+OR+bubble', "3d"), "realestate"),
    FeedInfo("Construction", _gnews('%22construction+spending%22+OR+%22building+permits%22+OR+%22housing+starts%22', "3d"), "realestate"),

    # -- insurance (3) --
    FeedInfo("Insurance Industry", _gnews('%22insurance+industry%22+OR+%22insurance+company%22+earnings+OR+regulation', "3d"), "insurance"),
    FeedInfo("Catastrophe Risk", _gnews('catastrophe+OR+%22natural+disaster%22+OR+hurricane+insurance+OR+%22insured+losses%22', "3d"), "insurance"),
    FeedInfo("Reinsurance", _gnews('reinsurance+OR+%22Lloyd%27s+of+London%22+OR+%22Swiss+Re%22+OR+%22Munich+Re%22', "7d"), "insurance"),

    # -- semiconductors (4) --
    FeedInfo("Semiconductor Industry", _gnews('TSMC+OR+%22semiconductor%22+OR+%22chip+shortage%22+OR+foundry+OR+%22chip+industry%22', "2d"), "semiconductors"),
    FeedInfo("AI & Compute", _gnews('%22AI+chip%22+OR+%22GPU%22+OR+Nvidia+OR+%22data+center%22+chip+OR+compute', "2d"), "semiconductors"),
    FeedInfo("Semiconductor Policy", _gnews('%22CHIPS+Act%22+OR+%22semiconductor%22+subsidy+OR+%22chip+war%22+OR+%22export+control%22', "3d"), "semiconductors"),
    FeedInfo("SemiEngineering", _gnews("site:semiengineering.com", "3d"), "semiconductors"),

    # -- esg (4) --
    FeedInfo("Carbon Markets", _gnews('%22carbon+market%22+OR+%22carbon+credit%22+OR+%22emissions+trading%22+OR+ETS+price', "3d"), "esg"),
    FeedInfo("ESG Regulation", _gnews('ESG+regulation+OR+%22sustainable+finance%22+OR+%22green+bond%22', "3d"), "esg"),
    FeedInfo("Climate Risk Finance", _gnews('%22climate+risk%22+financial+OR+bank+OR+insurance+OR+%22stranded+assets%22', "3d"), "esg"),
    FeedInfo("FAO Food Prices", "https://www.fao.org/news/rss-feed/en/", "esg"),
]


FEED_CATEGORIES: list[str] = sorted({feed.category for feed in RSS_FEEDS})


def get_feeds(category: str | None = None) -> list[FeedInfo]:
    """Return feeds, optionally filtered by category."""
    if category is None:
        return list(RSS_FEEDS)
    return [feed for feed in RSS_FEEDS if feed.category == category]
