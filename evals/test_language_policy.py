from __future__ import annotations

import unittest

from app.language_policy import resolve_conversation_language


class LanguagePolicyTests(unittest.TestCase):
    def test_switches_locked_language_on_strong_hebrew_signal(self) -> None:
        current_lang, lang_to_lock = resolve_conversation_language(
            locked_lang="en",
            user_text="\u05d0\u05e0\u05d9 \u05e8\u05d5\u05e6\u05d4 5 laptop",
            default_lang="en",
        )
        self.assertEqual(current_lang, "he")
        self.assertEqual(lang_to_lock, "he")

    def test_switches_back_to_english_on_clear_english_message(self) -> None:
        current_lang, lang_to_lock = resolve_conversation_language(
            locked_lang="he",
            user_text="I need the invoice",
            default_lang="en",
        )
        self.assertEqual(current_lang, "en")
        self.assertEqual(lang_to_lock, "en")


if __name__ == "__main__":
    unittest.main()
