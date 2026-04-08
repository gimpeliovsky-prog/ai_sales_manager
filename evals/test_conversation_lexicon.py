from __future__ import annotations

import unittest

from app.conversation_flow import classify_behavior, classify_intent
from app.conversation_lexicon import contact_details_regex, load_conversation_lexicon


class ConversationLexiconTests(unittest.TestCase):
    def test_loader_exposes_expected_sections(self) -> None:
        lexicon = load_conversation_lexicon()
        self.assertIn("service_terms", lexicon)
        self.assertIn("human_terms", lexicon)
        self.assertIn("contact_details_patterns", lexicon)

    def test_add_to_order_intent_uses_multilingual_lexicon(self) -> None:
        intent, confidence = classify_intent("добавь в заказ еще товар")
        self.assertEqual(intent, "add_to_order")
        self.assertGreaterEqual(confidence, 0.88)

    def test_human_handoff_intent_uses_multilingual_lexicon(self) -> None:
        intent, confidence = classify_intent("אני צריך מנהל")
        self.assertEqual(intent, "human_handoff")
        self.assertGreaterEqual(confidence, 0.9)

    def test_human_handoff_intent_uses_regex_bucket_for_inflected_forms(self) -> None:
        intent, confidence = classify_intent("Хочу менеджера")
        self.assertEqual(intent, "human_handoff")
        self.assertGreaterEqual(confidence, 0.9)

    def test_service_behavior_uses_arabic_lexicon(self) -> None:
        behavior, confidence = classify_behavior("أرسل الفاتورة من فضلك", session={})
        self.assertEqual(behavior, "service_request")
        self.assertGreaterEqual(confidence, 0.9)

    def test_contact_details_regex_matches_intro_or_phone(self) -> None:
        regex = contact_details_regex()
        self.assertIsNotNone(regex.search("my name is Peter"))
        self.assertIsNotNone(regex.search("055-770-4571"))


if __name__ == "__main__":
    unittest.main()
