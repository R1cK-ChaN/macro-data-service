"""TradingEconomics country and aggregate code maps."""

from __future__ import annotations

TE_COUNTRY_MAP: dict[str, str] = {
    # G10 + majors
    "united states": "US", "china": "CN", "japan": "JP", "germany": "DE",
    "united kingdom": "UK", "france": "FR", "canada": "CA", "australia": "AU",
    "new zealand": "NZ", "switzerland": "CH", "singapore": "SG", "south korea": "KR",
    "india": "IN", "brazil": "BR", "mexico": "MX", "indonesia": "ID",
    "italy": "IT", "spain": "ES", "netherlands": "NL", "turkey": "TR",
    "euro area": "EU", "european union": "EU", "hong kong": "HK",
    "saudi arabia": "SA", "south africa": "ZA", "russia": "RU",
    "sweden": "SE", "norway": "NO", "denmark": "DK", "poland": "PL",
    "taiwan": "TW", "thailand": "TH", "malaysia": "MY", "philippines": "PH",
    "vietnam": "VN", "colombia": "CO", "chile": "CL", "argentina": "AR",
    "nigeria": "NG", "egypt": "EG", "israel": "IL", "austria": "AT",
    "belgium": "BE", "ireland": "IE", "portugal": "PT", "greece": "GR",
    "finland": "FI", "czech republic": "CZ", "romania": "RO", "hungary": "HU",
    # Europe
    "iceland": "IS", "luxembourg": "LU", "malta": "MT", "cyprus": "CY",
    "estonia": "EE", "latvia": "LV", "lithuania": "LT",
    "slovakia": "SK", "slovenia": "SI", "bulgaria": "BG", "croatia": "HR",
    "serbia": "RS", "montenegro": "ME", "macedonia": "MK", "albania": "AL",
    "bosnia and herzegovina": "BA", "kosovo": "XK", "moldova": "MD",
    "belarus": "BY", "ukraine": "UA", "faroe islands": "FO",
    # Middle East + Central Asia
    "united arab emirates": "AE", "qatar": "QA", "kuwait": "KW", "bahrain": "BH",
    "oman": "OM", "jordan": "JO", "lebanon": "LB", "iraq": "IQ", "iran": "IR",
    "palestine": "PS",
    "kazakhstan": "KZ", "uzbekistan": "UZ", "kyrgyzstan": "KG",
    "tajikistan": "TJ", "turkmenistan": "TM",
    "armenia": "AM", "azerbaijan": "AZ", "georgia": "GE",
    # South + Southeast Asia, Pacific
    "pakistan": "PK", "bangladesh": "BD", "sri lanka": "LK", "nepal": "NP",
    "bhutan": "BT", "maldives": "MV", "myanmar": "MM", "cambodia": "KH",
    "laos": "LA", "brunei": "BN", "east timor": "TL",
    "macau": "MO", "mongolia": "MN",
    "papua new guinea": "PG", "fiji": "FJ",
    # Africa
    "algeria": "DZ", "morocco": "MA", "tunisia": "TN", "libya": "LY",
    "ghana": "GH", "kenya": "KE", "uganda": "UG", "tanzania": "TZ",
    "ethiopia": "ET", "rwanda": "RW", "burundi": "BI", "somalia": "SO",
    "senegal": "SN", "ivory coast": "CI", "cameroon": "CM",
    "angola": "AO", "mozambique": "MZ", "zambia": "ZM", "zimbabwe": "ZW",
    "namibia": "NA", "botswana": "BW", "mauritius": "MU", "seychelles": "SC",
    "madagascar": "MG", "malawi": "MW", "lesotho": "LS", "swaziland": "SZ",
    "cape verde": "CV", "sao tome and principe": "ST",
    "congo": "CG", "republic of the congo": "CG",
    "mali": "ML", "guinea": "GN", "guinea bissau": "GW", "gambia": "GM",
    "sierra leone": "SL", "liberia": "LR", "eritrea": "ER", "gabon": "GA",
    "benin": "BJ", "mauritania": "MR", "comoros": "KM",
    "central african republic": "CF",
    # Americas
    "costa rica": "CR", "panama": "PA", "guatemala": "GT", "el salvador": "SV",
    "honduras": "HN", "nicaragua": "NI", "jamaica": "JM", "cuba": "CU",
    "dominican republic": "DO", "trinidad and tobago": "TT", "barbados": "BB",
    "suriname": "SR",
    "peru": "PE", "ecuador": "EC", "bolivia": "BO", "paraguay": "PY",
    "uruguay": "UY", "venezuela": "VE",
    # 2016-2022 extended coverage: countries TE published events for
    # before ~2019 and later dropped from the feed. ISO codes keep
    # historical events from earlier backfills queryable.
    # Europe & micro-states
    "andorra": "AD", "liechtenstein": "LI", "monaco": "MC", "san marino": "SM",
    "isle of man": "IM", "greenland": "GL",
    # Middle East / Africa (additional)
    "syria": "SY", "yemen": "YE", "afghanistan": "AF", "sudan": "SD",
    "south sudan": "SS", "burkina faso": "BF", "chad": "TD", "niger": "NE",
    "togo": "TG", "djibouti": "DJ", "equatorial guinea": "GQ",
    # North Korea: full name (PRK) via TE's short-form
    "north korea": "KP",
    # Pacific / island nations (small-state coverage TE has since dropped)
    "kiribati": "KI", "micronesia": "FM", "palau": "PW", "samoa": "WS",
    "solomon islands": "SB", "tonga": "TO", "vanuatu": "VU",
    "new caledonia": "NC", "northern mariana islands": "MP",
    # Caribbean
    "bahamas": "BS", "bermuda": "BM", "cayman islands": "KY",
    "aruba": "AW", "puerto rico": "PR",
    "antigua and barbuda": "AG", "dominica": "DM", "grenada": "GD",
    "haiti": "HT", "belize": "BZ", "guyana": "GY",
    "st kitts and nevis": "KN", "saint kitts and nevis": "KN",
    "st lucia": "LC", "saint lucia": "LC",
    # Supra-national aggregates: codes drawn from ISO-3166-1's formal
    # user-assigned QM-QZ range. Downstream filters these aggregates
    # explicitly via country_code IN ('QM','QP','QS','QT','QW').
    "imf":   "QM",  # Monetary (IMF)
    "opec":  "QP",  # Petroleum (OPEC)
    "world": "QW",  # World aggregate
    "g20":   "QT",  # Twenty (G20)
    "g7":    "QS",  # Seven (G7)
}

SUPRA_NATIONAL_CODES: frozenset[str] = frozenset({"QM", "QP", "QS", "QT", "QW"})

__all__ = ["SUPRA_NATIONAL_CODES", "TE_COUNTRY_MAP"]
