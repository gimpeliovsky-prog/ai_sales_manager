import unittest

from app.greeting_policy import returning_customer_prefix, select_contact_display_name, should_send_known_buyer_greeting


class GreetingPolicyTests(unittest.TestCase):
    def test_returning_customer_prefix_does_not_include_name(self) -> None:
        self.assertEqual(returning_customer_prefix("en"), "Glad to help again.")

    def test_select_contact_display_name_prefers_contact(self) -> None:
        self.assertEqual(select_contact_display_name("Peter", "my name is peter tel"), "Peter")

    def test_select_contact_display_name_preserves_existing_name_when_contact_missing(self) -> None:
        self.assertEqual(select_contact_display_name(None, "Peter"), "Peter")

    def test_should_send_known_buyer_greeting_for_new_known_buyer_greeting(self) -> None:
        self.assertTrue(
            should_send_known_buyer_greeting(
                user_text="hello",
                buyer_identified=True,
                stage="new",
                conversation_reopened=False,
            )
        )

    def test_should_not_send_known_buyer_greeting_when_buyer_unknown(self) -> None:
        self.assertFalse(
            should_send_known_buyer_greeting(
                user_text="hello",
                buyer_identified=False,
                stage="new",
                conversation_reopened=False,
            )
        )


if __name__ == "__main__":
    unittest.main()
