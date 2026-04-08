Conversation-flow lexicons are data files, not code.

How to add a new language:
1. Add customer-facing phrases to the relevant `*_terms` buckets.
2. Use `*_regexes` or `contact_details_patterns` only when plain phrases are too weak because of inflection or morphology.
3. Prefer high-signal phrases that materially affect routing or intent classification.
4. Add eval or unit-test coverage for each new language bucket you introduce.

Schema:
- `service_terms`
- `price_terms`
- `direct_buy_terms`
- `explore_terms`
- `frustrated_terms`
- `order_terms`
- `add_to_order_terms`
- `human_terms`
- `service_regexes`
- `price_regexes`
- `direct_buy_regexes`
- `explore_regexes`
- `frustrated_regexes`
- `order_regexes`
- `add_to_order_regexes`
- `human_regexes`
- `contact_details_patterns`
