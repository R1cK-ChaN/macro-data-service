from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from analyst.ingestion.scrapers.forexfactory import ForexFactoryNewsClient
from analyst.ingestion.scrapers.investing import InvestingNewsClient
from analyst.ingestion.scrapers._selector_versioning import (
    dom_structure_fingerprint,
    extract_with_selector_versions,
    reset_dom_fingerprint_cache,
    SelectorVersion,
    selector_field,
)


class SelectorVersioningTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_dom_fingerprint_cache()

    def test_dom_structure_fingerprint_ignores_text_but_detects_structure_changes(self) -> None:
        html_a = """
        <html>
          <body>
            <article>
              <h2>Alpha</h2>
              <p>First body copy.</p>
            </article>
          </body>
        </html>
        """
        html_b = """
        <html>
          <body>
            <article>
              <h2>Bravo</h2>
              <p>Different words, same shape.</p>
            </article>
          </body>
        </html>
        """
        html_c = """
        <html>
          <body>
            <article>
              <header><h2>Alpha</h2></header>
              <p>First body copy.</p>
            </article>
          </body>
        </html>
        """

        self.assertEqual(dom_structure_fingerprint(html_a), dom_structure_fingerprint(html_b))
        self.assertNotEqual(dom_structure_fingerprint(html_a), dom_structure_fingerprint(html_c))

    def test_investing_prefers_primary_selector_version(self) -> None:
        html = """
        <html>
          <body>
            <article data-test="article-item" data-id="101">
              <a data-test="article-title-link" href="/news/economy-news/fed-keeps-rates-steady-101">
                Fed keeps rates steady as inflation cools
              </a>
              <p data-test="article-description">Policymakers left the target range unchanged.</p>
              <time data-test="article-publish-date" datetime="2026-03-12T12:00:00Z"></time>
              <span data-test="news-provider-name">Reuters</span>
              <span data-test="article-comments">12 comments</span>
            </article>
            <article data-test="article-item" data-id="102">
              <a data-test="article-title-link" href="/news/economy-news/us-yields-slip-after-auction-102">
                US yields slip after strong Treasury auction
              </a>
              <p data-test="article-description">Demand at the long end improved.</p>
              <time data-test="article-publish-date" datetime="2026-03-12T13:00:00Z"></time>
              <span data-test="news-provider-name">AP</span>
            </article>
          </body>
        </html>
        """

        client = InvestingNewsClient.__new__(InvestingNewsClient)
        items = client._parse_news_html(html, "economy-news")

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].raw_json["selector_version"], "data-test-v1")
        self.assertEqual(items[0].raw_json["comments"], 12)
        self.assertEqual(items[0].category, "economy-news")
        self.assertEqual(items[0].author, "Reuters")

    def test_investing_falls_back_to_legacy_selector_version(self) -> None:
        html = """
        <html>
          <body>
            <article class="js-article-item" data-id="201">
              <a class="title" href="/news/stock-market-news/stocks-rally-after-cpi-201">
                Stocks rally after cooler CPI print
              </a>
            </article>
            <article class="js-article-item" data-id="202">
              <a class="title" href="/news/stock-market-news/dollar-eases-before-auction-202">
                Dollar eases before closely watched auction
              </a>
            </article>
          </body>
        </html>
        """

        client = InvestingNewsClient.__new__(InvestingNewsClient)
        with self.assertLogs("analyst.ingestion.scrapers.investing", level="WARNING") as captured:
            items = client._parse_news_html(html, "stock-market-news")

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].raw_json["selector_version"], "legacy-js-article-item")
        self.assertEqual(items[0].raw_json["data_id"], "201")
        self.assertTrue(any("Selector fallback engaged" in message for message in captured.output))

    def test_forexfactory_logs_dom_change_and_uses_generic_fallback(self) -> None:
        html_v1 = """
        <html>
          <body>
            <div class="news-block">
              <div class="news-block__title">
                <a href="/news/11111-dollar-pulls-back-after-cpi">
                  Dollar pulls back after CPI surprise fades
                </a>
              </div>
              <div class="news-block__details">From Reuters|2 hours ago|14 comments</div>
              <div class="news-block__preview">The dollar retraced earlier gains.</div>
              <span class="universal-impact high"></span>
              <img src="https://img.example.com/dollar.jpg" />
            </div>
          </body>
        </html>
        """
        html_v2 = """
        <html>
          <body>
            <article>
              <h2>
                <a href="/news/22222-yen-strengthens-on-boj-rumors">
                  Yen strengthens on renewed BOJ tightening rumours
                </a>
              </h2>
              <time>3 hours ago</time>
              <p>The yen gained as investors repriced the policy path.</p>
              <span class="universal-impact medium"></span>
              <img src="https://img.example.com/yen.jpg" />
            </article>
          </body>
        </html>
        """

        client = ForexFactoryNewsClient.__new__(ForexFactoryNewsClient)
        first_items = client._parse_news_html(html_v1)
        self.assertEqual(first_items[0].raw_json["selector_version"], "news-block-v1")

        with self.assertLogs("analyst.ingestion.scrapers.forexfactory", level="WARNING") as captured:
            second_items = client._parse_news_html(html_v2)

        self.assertEqual(len(second_items), 1)
        self.assertEqual(second_items[0].raw_json["selector_version"], "generic-article-v2")
        self.assertNotEqual(
            first_items[0].raw_json["dom_fingerprint"],
            second_items[0].raw_json["dom_fingerprint"],
        )
        self.assertTrue(any("Selector fallback engaged" in message for message in captured.output))
        self.assertTrue(any("DOM fingerprint changed" in message for message in captured.output))

    def test_selector_version_can_pass_on_item_count_even_with_lower_confidence(self) -> None:
        html = """
        <html>
          <body>
            <div class="card"><a href="/news/1">Headline number 1 is long enough</a></div>
            <div class="card"><a href="/news/2">Headline number 2 is long enough</a></div>
            <div class="card"><a href="/news/3">Headline number 3 is long enough</a></div>
            <div class="card"><a href="/news/4">Headline number 4 is long enough</a></div>
            <div class="card"><a href="/news/5">Headline number 5 is long enough</a></div>
            <div class="card"><a href="/news/6">Headline number 6 is long enough</a></div>
            <div class="card"><a href="/news/7">Headline number 7 is long enough</a></div>
            <div class="card"><a href="/news/8">Headline number 8 is long enough</a></div>
            <div class="card"><a href="/news/9">Headline number 9 is long enough</a></div>
            <div class="card"><a href="/news/10">Headline number 10 is long enough</a></div>
            <div class="card"></div>
            <div class="card"></div>
            <div class="card"></div>
            <div class="card"></div>
            <div class="card"></div>
          </body>
        </html>
        """
        version = SelectorVersion(
            name="count-threshold-v1",
            item_selectors=("div.card",),
            fields={
                "title": selector_field("a"),
                "url": selector_field("a", attr="href"),
            },
            min_confidence=0.8,
            success_items=10,
        )

        def build_item(fields: dict[str, str], _node, _context) -> dict[str, str] | None:
            title = fields.get("title", "")
            url = fields.get("url", "")
            if not title or not url:
                return None
            return {"title": title, "url": url}

        result = extract_with_selector_versions(
            html,
            source="test",
            context="count-threshold",
            versions=(version,),
            build_item=build_item,
        )

        self.assertEqual(result.version, "count-threshold-v1")
        self.assertEqual(len(result.items), 10)
        self.assertLess(result.confidence, 0.8)


if __name__ == "__main__":
    unittest.main()
