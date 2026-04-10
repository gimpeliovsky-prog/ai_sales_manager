from __future__ import annotations

import unittest

from app.buyer_intake import truncate_inbound_text
from app.i18n import text as i18n_text


class RuntimeHardeningTests(unittest.TestCase):
    def test_truncate_inbound_text_limits_payload_size(self) -> None:
        raw = "x" * 5000
        normalized = truncate_inbound_text(raw, max_chars=4000)
        self.assertEqual(len(normalized), 4000)

    def test_truncate_inbound_text_strips_null_bytes_and_whitespace(self) -> None:
        normalized = truncate_inbound_text(" \x00 hello \x00 ", max_chars=100)
        self.assertEqual(normalized, "hello")

    def test_runtime_temporary_error_translation_exists(self) -> None:
        message = i18n_text("runtime.temporary_error", "en")
        self.assertIn("temporary error", message.lower())


if __name__ == "__main__":
    unittest.main()
