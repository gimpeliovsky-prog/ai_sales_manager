import unittest

from app.agent import _select_company_candidate_query


class CompanyCandidateSelectionTests(unittest.TestCase):
    def test_numeric_reply_selects_candidate_company_number(self) -> None:
        candidates = [
            {"company_name": "Alpha Ltd", "company_number": "111111111"},
            {"company_name": "Beta Ltd", "company_number": "222222222"},
        ]

        self.assertEqual(_select_company_candidate_query("2", candidates), "222222222")

    def test_exact_company_number_reply_matches_existing_candidate(self) -> None:
        candidates = [
            {"company_name": "Alpha Ltd", "company_number": "111111111"},
            {"company_name": "Beta Ltd", "company_number": "222222222"},
        ]

        self.assertEqual(_select_company_candidate_query("222222222", candidates), "222222222")


if __name__ == "__main__":
    unittest.main()
