import unittest

from app.buyer_intake import get_intro_sales_contact_message


class BuyerIntakeMessageTests(unittest.TestCase):
    def test_intro_sales_contact_message_is_friendlier_in_english(self) -> None:
        self.assertEqual(
            get_intro_sales_contact_message("en"),
            "Hello. To get started, please send your name and phone number.",
        )

    def test_intro_sales_contact_message_is_localized_in_hebrew(self) -> None:
        self.assertEqual(
            get_intro_sales_contact_message("he"),
            "שלום. כדי להתחיל, שלח לי בבקשה את השם ומספר הטלפון שלך.",
        )


if __name__ == "__main__":
    unittest.main()
