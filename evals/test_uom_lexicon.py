from __future__ import annotations

import unittest

from app.uom_lexicon import load_uom_lexicon
from app.uom_semantics import canonical_uom, localize_uom_label, resolve_catalog_uom


class UomLexiconTests(unittest.TestCase):
    def test_loads_multilingual_aliases(self) -> None:
        lexicon = load_uom_lexicon()
        self.assertIn("aliases", lexicon)
        self.assertIn("piece", lexicon["aliases"])
        self.assertIn("pcs", lexicon["aliases"]["piece"])
        self.assertIn("шт", lexicon["aliases"]["piece"])
        self.assertIn("יחידה", lexicon["aliases"]["piece"])
        self.assertIn("قطعة", lexicon["aliases"]["piece"])

    def test_canonical_uom_is_data_driven(self) -> None:
        self.assertEqual(canonical_uom("pcs"), "piece")
        self.assertEqual(canonical_uom("шт"), "piece")
        self.assertEqual(canonical_uom("יחידות"), "piece")
        self.assertEqual(canonical_uom("كرتونة"), "box")

    def test_localize_uom_label_uses_lexicon_labels(self) -> None:
        self.assertEqual(localize_uom_label("pcs", "en"), "pieces")
        self.assertEqual(localize_uom_label("pcs", "ru"), "шт.")
        self.assertEqual(localize_uom_label("pcs", "he"), "יחידות")
        self.assertEqual(localize_uom_label("pcs", "ar"), "قطع")

    def test_tenant_overrides_still_merge(self) -> None:
        config = {
            "uom_aliases": {"bundle": ["bundle", "bundles"]},
            "uom_labels": {"en": {"bundle": "bundles"}},
        }
        self.assertEqual(canonical_uom("bundles", config), "bundle")
        self.assertEqual(localize_uom_label("bundles", "en", config), "bundles")

    def test_resolve_catalog_uom_keeps_semantic_match(self) -> None:
        result = resolve_catalog_uom(
            "шт",
            [{"uom": "Nos", "display_name": "pieces", "conversion_factor": 1}],
            config={"uom_aliases": {"piece": ["Nos", "pieces"]}},
        )
        self.assertTrue(result["resolved"])
        self.assertEqual(result["match_type"], "semantic")

    def test_resolve_catalog_uom_preserves_error_contract(self) -> None:
        result = resolve_catalog_uom("шт", [{"uom": "box", "display_name": "boxes"}])
        self.assertFalse(result["resolved"])
        self.assertEqual(result["reason"], "no_matching_uom")
        self.assertEqual(result["canonical_uom"], "piece")


if __name__ == "__main__":
    unittest.main()
