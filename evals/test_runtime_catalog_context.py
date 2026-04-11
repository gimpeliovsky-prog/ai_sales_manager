from __future__ import annotations

import unittest

from app.runtime_catalog_context import (
    build_catalog_prefetch_context,
    build_catalog_preview_context,
    catalog_lookup_backoff_terms,
    should_prefetch_catalog_preview,
)


class RuntimeCatalogContextTests(unittest.TestCase):
    def test_no_match_guidance_forbids_invented_variants(self) -> None:
        context = build_catalog_prefetch_context({"items": []}, search_term="mugs")
        self.assertIn("found no exact matches", context)
        self.assertIn("Do not suggest example models, variants, or subtypes", context)

    def test_catalog_preview_context_uses_real_items(self) -> None:
        context = build_catalog_preview_context(
            {
                "items": [
                    {"display_item_name": "Laptop", "item_code": "SKU002"},
                    {"display_item_name": "Book", "item_code": "SKU003"},
                ]
            }
        )
        self.assertIn("Runtime catalog preview already ran", context)
        self.assertIn("Laptop (SKU002)", context)
        self.assertIn("Book (SKU003)", context)
        self.assertIn("instead of saying no match", context)

    def test_prefetch_catalog_preview_when_browsing_without_anchor(self) -> None:
        self.assertTrue(
            should_prefetch_catalog_preview(
                lead_profile={
                    "product_interest": None,
                    "catalog_item_code": None,
                    "next_action": "ask_need",
                },
                intent="browse_catalog",
            )
        )

    def test_catalog_lookup_backoff_terms_try_shorter_suffixes(self) -> None:
        candidates = catalog_lookup_backoff_terms("probably gaming laptop")
        self.assertEqual(candidates[0], "probably gaming laptop")
        self.assertIn("gaming laptop", candidates)
        self.assertIn("laptop", candidates)

    def test_prefetch_catalog_preview_after_broad_no_match_followup(self) -> None:
        self.assertTrue(
            should_prefetch_catalog_preview(
                lead_profile={
                    "product_interest": "computer",
                    "product_resolution_status": "broad",
                    "catalog_lookup_status": "no_match",
                    "next_action": "show_matching_options",
                },
                intent="browse_catalog",
            )
        )


if __name__ == "__main__":
    unittest.main()
