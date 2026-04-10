import unittest

from app.llm_state_updater import parse_llm_signal_classification, parse_llm_state_update


class LlmStateUpdaterTests(unittest.TestCase):
    def test_parse_small_talk_intent(self) -> None:
        parsed = parse_llm_state_update(
            '{"intent":"small_talk","signal_type":"small_talk","signal_emotion":"positive","signal_preserves_deal":true,"behavior_class":"returning_customer","confidence":0.61,"next_action":"ask_need","lead_patch":{},"reason":"Greeting and social check-in without a commercial request."}'
        )
        self.assertTrue(parsed["valid"])
        self.assertEqual(parsed["intent"], "small_talk")
        self.assertEqual(parsed["signal_type"], "small_talk")
        self.assertEqual(parsed["signal_emotion"], "positive")
        self.assertTrue(parsed["signal_preserves_deal"])
        self.assertEqual(parsed["behavior_class"], "returning_customer")
        self.assertEqual(parsed["next_action"], "ask_need")

    def test_parse_drops_small_talk_product_interest_patch(self) -> None:
        parsed = parse_llm_state_update(
            '{"intent":"find_product","signal_type":"deal_progress","behavior_class":"returning_customer","confidence":0.83,"lead_patch":{"product_interest":"how are you"}}'
        )
        self.assertTrue(parsed["valid"])
        self.assertEqual(parsed["lead_patch"], {})

    def test_parse_signal_classifier_output(self) -> None:
        parsed = parse_llm_signal_classification(
            '{"signal_type":"price_objection","signal_emotion":"skeptical","signal_preserves_deal":true,"confidence":0.91,"reason":"Customer says the offer is too expensive."}'
        )
        self.assertTrue(parsed["valid"])
        self.assertEqual(parsed["signal_type"], "price_objection")
        self.assertEqual(parsed["signal_emotion"], "skeptical")
        self.assertTrue(parsed["signal_preserves_deal"])

    def test_parse_signal_classifier_rejects_unknown_signal(self) -> None:
        parsed = parse_llm_signal_classification(
            '{"signal_type":"unknown_thing","signal_emotion":"neutral","signal_preserves_deal":true,"confidence":0.8}'
        )
        self.assertFalse(parsed["valid"])
        self.assertIsNone(parsed["signal_type"])


if __name__ == "__main__":
    unittest.main()
