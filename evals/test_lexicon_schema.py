from __future__ import annotations

import unittest

from app.lexicon_schema import validate_all_lexicons


class LexiconSchemaTests(unittest.TestCase):
    def test_all_lexicons_match_expected_schema(self) -> None:
        self.assertEqual(validate_all_lexicons(), [])


if __name__ == "__main__":
    unittest.main()
