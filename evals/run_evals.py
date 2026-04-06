from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "ai_sales_manager") not in sys.path:
    sys.path.insert(0, str(ROOT / "ai_sales_manager"))

from app.conversation_flow import derive_conversation_state, get_handoff_message  # noqa: E402
from app.language_policy import resolve_conversation_language  # noqa: E402
from app.prompt_registry import build_runtime_system_prompt  # noqa: E402
from app.tool_policy import evaluate_tool_call  # noqa: E402


def _load_cases(filename: str) -> list[dict[str, Any]]:
    return json.loads((Path(__file__).resolve().parent / filename).read_text(encoding="utf-8"))


def _assert_subset(actual: dict[str, Any], expected: dict[str, Any], case_name: str) -> list[str]:
    failures: list[str] = []
    for key, expected_value in expected.items():
        actual_value = actual.get(key)
        if actual_value != expected_value:
            failures.append(
                f"{case_name}: expected {key}={expected_value!r}, got {actual_value!r}"
            )
    return failures


def run_conversation_flow_evals() -> list[str]:
    failures: list[str] = []
    for case in _load_cases("conversation_flow_cases.json"):
        actual = derive_conversation_state(
            session=case["session"],
            user_text=case["user_text"],
            channel=case["channel"],
            needs_intro=case["needs_intro"],
            customer_identified=case["customer_identified"],
            active_order_name=case["active_order_name"],
            ai_policy=case.get("ai_policy"),
        )
        failures.extend(_assert_subset(actual, case["expected"], case["name"]))
    return failures


def run_tool_policy_evals() -> list[str]:
    failures: list[str] = []
    for case in _load_cases("tool_policy_cases.json"):
        actual = evaluate_tool_call(
            tool_name=case["tool_name"],
            inputs=case["inputs"],
            session=case["session"],
            tenant=case["tenant"],
            user_text=case["user_text"],
        )
        blocked = actual is not None
        expected = case["expected"]
        if blocked != expected["blocked"]:
            failures.append(f"{case['name']}: expected blocked={expected['blocked']!r}, got {blocked!r}")
            continue
        if expected["blocked"]:
            error_text = str((actual or {}).get("error") or "")
            expected_fragment = str(expected.get("error_contains") or "")
            if expected_fragment and expected_fragment not in error_text:
                failures.append(
                    f"{case['name']}: expected error containing {expected_fragment!r}, got {error_text!r}"
                )
    return failures


def run_prompt_override_evals() -> list[str]:
    failures: list[str] = []
    tenant = {
        "company_name": "ACME",
        "company_code": "acme",
        "ai_policy": {
            "prompt_overrides": {
                "stage_prompts": {
                    "discover": ["Prefer premium positioning for this tenant."]
                },
                "handoff_messages": {
                    "ru": "Передаю живому менеджеру."
                },
            }
        },
    }
    prompt = build_runtime_system_prompt(
        tenant=tenant,
        lang="ru",
        channel="telegram",
        stage="discover",
        behavior_class="explorer",
    )
    if "Prefer premium positioning for this tenant." not in prompt:
        failures.append("prompt_override_stage_append: expected custom stage prompt to appear in system prompt")

    handoff_message = get_handoff_message("ru", ai_policy=tenant["ai_policy"])
    if handoff_message != "Передаю живому менеджеру.":
        failures.append(
            f"handoff_message_override: expected custom handoff message, got {handoff_message!r}"
        )
    return failures


def run_language_lock_evals() -> list[str]:
    failures: list[str] = []
    current_lang, lang_to_lock = resolve_conversation_language(
        locked_lang=None,
        user_text="שלום, צריך הצעת מחיר",
        default_lang="ru",
    )
    if current_lang != "he" or lang_to_lock != "he":
        failures.append(
            f"language_lock_first_message: expected ('he', 'he'), got ({current_lang!r}, {lang_to_lock!r})"
        )

    current_lang, lang_to_lock = resolve_conversation_language(
        locked_lang="he",
        user_text="price please",
        default_lang="ru",
    )
    if current_lang != "he" or lang_to_lock is not None:
        failures.append(
            f"language_lock_persists: expected ('he', None), got ({current_lang!r}, {lang_to_lock!r})"
        )

    current_lang, lang_to_lock = resolve_conversation_language(
        locked_lang=None,
        user_text="+972541234567",
        default_lang="ru",
    )
    if current_lang != "ru" or lang_to_lock is not None:
        failures.append(
            f"language_lock_waits_for_signal: expected ('ru', None), got ({current_lang!r}, {lang_to_lock!r})"
        )
    return failures


def main() -> int:
    failures = run_conversation_flow_evals()
    failures.extend(run_tool_policy_evals())
    failures.extend(run_prompt_override_evals())
    failures.extend(run_language_lock_evals())
    if failures:
        print("AI sales evals failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("AI sales evals passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
