from __future__ import annotations

import unittest

from app.interaction_lexicon import load_interaction_lexicon
from app.interaction_patterns import has_add_to_order_intent, has_explicit_confirmation, has_order_change_intent


class InteractionLexiconTests(unittest.TestCase):
    def test_loader_exposes_expected_sections(self) -> None:
        lexicon = load_interaction_lexicon()
        self.assertIn("confirm_terms", lexicon)
        self.assertIn("conversational_confirm_regexes", lexicon)
        self.assertIn("order_change_terms", lexicon)

    def test_explicit_confirmation_uses_multilingual_lexicon(self) -> None:
        self.assertTrue(has_explicit_confirmation("יאללה"))
        self.assertTrue(has_explicit_confirmation("давай оформляй"))
        self.assertTrue(has_explicit_confirmation("موافق"))

    def test_negative_confirmation_still_blocks_positive_terms(self) -> None:
        self.assertFalse(has_explicit_confirmation("не оформляй"))
        self.assertFalse(has_explicit_confirmation("לא"))

    def test_add_to_order_and_order_change_intents_use_lexicon(self) -> None:
        self.assertTrue(has_add_to_order_intent("добавь в заказ футболку"))
        self.assertTrue(has_order_change_intent("remove item from order"))
        self.assertTrue(has_order_change_intent("שנה כמות"))


if __name__ == "__main__":
    unittest.main()
