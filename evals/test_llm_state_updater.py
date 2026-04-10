import unittest

from app.llm_state_updater import parse_llm_state_update


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


if __name__ == "__main__":
    unittest.main()
