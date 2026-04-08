from __future__ import annotations

import unittest

from app.lead_lexicon import lexicon_default, load_lead_lexicon, signal_regexes, signal_terms
from app.lead_management import _multi_item_default_uom, _signal_matches, normalize_catalog_lookup_query


class LeadLexiconTests(unittest.TestCase):
    def test_loads_multilingual_signal_terms(self) -> None:
        lexicon = load_lead_lexicon()
        self.assertIn("signal_terms", lexicon)
        self.assertIn("signal_regexes", lexicon)
        self.assertIn("urgency", lexicon["signal_terms"])
        self.assertIn("asap", signal_terms("urgency"))
        self.assertIn("срочно", signal_terms("urgency"))
        self.assertIn("דחוף", signal_terms("urgency"))
        self.assertIn("عاجل", signal_terms("urgency"))
        self.assertTrue(signal_regexes("correction_quantity"))
        self.assertEqual(lexicon_default("multi_item_default_uom"), "box")

    def test_catalog_normalization_uses_lexicon_cleanup(self) -> None:
        self.assertEqual(normalize_catalog_lookup_query("добавь в заказ t-shirt"), "t-shirt")
        self.assertEqual(normalize_catalog_lookup_query("add to this order backpack"), "backpack")
        self.assertEqual(normalize_catalog_lookup_query("show me the backpack"), "backpack")
        self.assertEqual(normalize_catalog_lookup_query("show me backpack"), "backpack")

    def test_signal_matches_use_lexicon_terms(self) -> None:
        self.assertTrue(_signal_matches("измени количество в заказе", "correction_quantity"))
        self.assertTrue(_signal_matches("להוסיף להזמנה", "order_correction"))
        self.assertTrue(_signal_matches("أريد عرض سعر", "quote"))
        self.assertTrue(_signal_matches("please change it from 5 to 10", "correction_quantity"))

    def test_multi_item_default_uom_uses_lexicon_default(self) -> None:
        self.assertEqual(_multi_item_default_uom(None), "box")
        self.assertEqual(_multi_item_default_uom({"multi_item_default_uom": "pack"}), "pack")


if __name__ == "__main__":
    unittest.main()
