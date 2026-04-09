from __future__ import annotations

import unittest

from app.buyer_intake import clean_company_candidate, get_known_buyer_greeting
from app.buyer_resolver import resolve_buyer_from_intro


class _FakeLicenseClient:
    def __init__(self) -> None:
        self.resolve_calls: list[dict[str, str | None]] = []
        self.create_called = False

    async def resolve_buyer(
        self,
        company_code: str,
        *,
        channel_type: str,
        channel_user_id: str,
        phone: str | None = None,
        full_name: str | None = None,
    ) -> dict:
        self.resolve_calls.append(
            {
                "company_code": company_code,
                "channel_type": channel_type,
                "channel_user_id": channel_user_id,
                "phone": phone,
                "full_name": full_name,
            }
        )
        return {"found": True, "erp_customer_id": "CUST-0001", "erp_customer_name": "Peter P Gimpel"}

    async def create_buyer(self, *args, **kwargs) -> dict:
        self.create_called = True
        return {"found": True, "erp_customer_id": "SHOULD-NOT-HAPPEN"}


class BuyerResolutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_intro_resolution_uses_resolve_only(self) -> None:
        fake = _FakeLicenseClient()
        result = await resolve_buyer_from_intro(
            session={},
            company_code="dev",
            channel="telegram",
            channel_uid="12345",
            full_name="Peter P Gimpel",
            phone="+972557704571",
            lc=fake,  # type: ignore[arg-type]
        )
        self.assertEqual(result.get("erp_customer_id"), "CUST-0001")
        self.assertEqual(len(fake.resolve_calls), 1)
        self.assertFalse(fake.create_called)

    def test_extracts_company_candidate(self) -> None:
        self.assertEqual(clean_company_candidate("I work at TopClean"), "TopClean")
        self.assertEqual(clean_company_candidate("אני עובד ב Kad"), "Kad")
        self.assertIsNone(clean_company_candidate("company"))

    def test_known_buyer_greeting_uses_name(self) -> None:
        greeting = get_known_buyer_greeting("en", "Peter")
        self.assertIn("Peter", greeting)


if __name__ == "__main__":
    unittest.main()
