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
        self.assertIn("\u0448\u0442", lexicon["aliases"]["piece"])
        self.assertIn("\u05d9\u05d7\u05d9\u05d3\u05d4", lexicon["aliases"]["piece"])
        self.assertIn("\u0642\u0637\u0639\u0629", lexicon["aliases"]["piece"])
        self.assertIn("set", lexicon["aliases"])

    def test_canonical_uom_is_data_driven(self) -> None:
        self.assertEqual(canonical_uom("pcs"), "piece")
        self.assertEqual(canonical_uom("\u0448\u0442"), "piece")
        self.assertEqual(canonical_uom("\u05d9\u05d7\u05d9\u05d3\u05d5\u05ea"), "piece")
        self.assertEqual(canonical_uom("\u0643\u0631\u062a\u0648\u0646\u0629"), "box")
        self.assertEqual(canonical_uom("Set"), "set")

    def test_localize_uom_label_uses_lexicon_labels(self) -> None:
        self.assertEqual(localize_uom_label("pcs", "en"), "pieces")
        self.assertEqual(localize_uom_label("pcs", "ru"), "\u0448\u0442.")
        self.assertEqual(localize_uom_label("pcs", "he"), "\u05d9\u05d7\u05d9\u05d3\u05d5\u05ea")
        self.assertEqual(localize_uom_label("pcs", "ar"), "\u0642\u0637\u0639")
        self.assertEqual(localize_uom_label("Set", "he"), "\u05e1\u05d8\u05d9\u05dd")
        self.assertEqual(localize_uom_label("sets", "ru"), "\u043a\u043e\u043c\u043f\u043b\u0435\u043a\u0442\u044b")

    def test_tenant_overrides_still_merge(self) -> None:
        config = {
            "uom_aliases": {"bundle": ["bundle", "bundles"]},
            "uom_labels": {"en": {"bundle": "bundles"}},
        }
        self.assertEqual(canonical_uom("bundles", config), "bundle")
        self.assertEqual(localize_uom_label("bundles", "en", config), "bundles")

    def test_resolve_catalog_uom_keeps_semantic_match(self) -> None:
        result = resolve_catalog_uom(
            "\u0448\u0442",
            [{"uom": "Nos", "display_name": "pieces", "conversion_factor": 1}],
            config={"uom_aliases": {"piece": ["Nos", "pieces"]}},
        )
        self.assertTrue(result["resolved"])
        self.assertEqual(result["match_type"], "semantic")

    def test_resolve_catalog_uom_preserves_error_contract(self) -> None:
        result = resolve_catalog_uom("\u0448\u0442", [{"uom": "box", "display_name": "boxes"}])
        self.assertFalse(result["resolved"])
        self.assertEqual(result["reason"], "no_matching_uom")
        self.assertEqual(result["canonical_uom"], "piece")


if __name__ == "__main__":
    unittest.main()
