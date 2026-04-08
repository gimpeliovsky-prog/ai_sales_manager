from __future__ import annotations

import unittest

from app.lead_runtime_config import (
    configured_signal_regexes,
    configured_signal_terms,
    lead_config,
    lead_config_from_ai_policy,
    merged_uom_config,
    multi_item_default_uom,
    single_item_cleanup_terms,
)


class LeadRuntimeConfigTests(unittest.TestCase):
    def test_lead_config_normalizes_non_dict(self) -> None:
        self.assertEqual(lead_config(None), {})
        self.assertEqual(lead_config("x"), {})

    def test_signal_terms_merge_lexicon_and_overrides(self) -> None:
        config = {"signal_terms": {"quote": ["special quote phrase"]}}
        terms = configured_signal_terms(config, "quote")
        self.assertIn("quote", terms)
        self.assertIn("special quote phrase", terms)

    def test_signal_regexes_merge_lexicon_and_overrides(self) -> None:
        config = {"signal_regexes": {"correction_quantity": [r"\bcustom regex\b"]}}
        patterns = configured_signal_regexes(config, "correction_quantity")
        self.assertTrue(patterns)
        self.assertIn(r"\bcustom regex\b", patterns)

    def test_single_item_cleanup_terms_merge_lexicon_and_overrides(self) -> None:
        config = {"single_item_cleanup_terms": ["custom cleanup phrase"]}
        terms = single_item_cleanup_terms(config)
        self.assertIn("order", terms)
        self.assertIn("custom cleanup phrase", terms)

    def test_multi_item_default_uom_prefers_override(self) -> None:
        self.assertEqual(multi_item_default_uom(None), "box")
        self.assertEqual(multi_item_default_uom({"multi_item_default_uom": "pack"}), "pack")

    def test_merged_uom_config_preserves_legacy_alias_keys(self) -> None:
        config = {
            "uom_aliases": {"piece": ["pieces"]},
            "single_item_uom_terms": {"box": ["carton"]},
            "uom_labels": {"en": {"piece": "pieces"}},
        }
        merged = merged_uom_config(config, "single_item_uom_terms")
        self.assertIn("uom_aliases", merged)
        self.assertIn("piece", merged["uom_aliases"])
        self.assertIn("box", merged["uom_aliases"])
        self.assertIn("uom_labels", merged)

    def test_lead_config_from_ai_policy_merges_catalog_uom_defaults(self) -> None:
        ai_policy = {
            "lead_management": {"signal_terms": {"quote": ["special quote phrase"]}},
            "catalog": {
                "uom_aliases": {"piece": ["Nos"]},
                "uom_labels": {"en": {"piece": "pieces"}},
            },
        }
        config = lead_config_from_ai_policy(ai_policy)
        self.assertIn("signal_terms", config)
        self.assertIn("uom_aliases", config)
        self.assertIn("uom_labels", config)
        self.assertEqual(config["uom_aliases"]["piece"], ["Nos"])


if __name__ == "__main__":
    unittest.main()
