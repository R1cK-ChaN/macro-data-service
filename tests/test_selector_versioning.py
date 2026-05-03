from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.scrapers._selector_versioning import (
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
