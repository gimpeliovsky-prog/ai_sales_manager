Interaction-pattern lexicons are data files, not code.

How to add a new language:
1. Add exact customer phrases to the relevant `*_terms` buckets.
2. Use `*_regexes` only when phrase lists are too weak because of morphology, word order, or conversational combinations.
3. Keep confirmation and negation signals conservative because these markers affect order actions.
4. Add unit or eval coverage for each new language bucket you introduce.

Schema:
- `confirm_terms`
- `negative_confirm_terms`
- `add_to_order_terms`
- `order_change_terms`
- `confirm_regexes`
- `negative_confirm_regexes`
- `conversational_confirm_regexes`
- `add_to_order_regexes`
- `order_change_regexes`
