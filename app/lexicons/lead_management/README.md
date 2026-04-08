Lead-management lexicons are data files, not code.

How to add a new language:
1. Copy one of the existing `*.json` files.
2. Add phrases only to the semantic slots you need.
3. Keep all phrases in natural customer wording.
4. Prefer high-signal phrases over speculative synonyms.
5. Add or update tests in `evals/test_lead_lexicon.py`.

Schema:
- `browse_scaffolding_terms`: phrases that mean "show me options"
- `yes_terms`: affirmative markers used in low-risk confirmation helpers
- `contact_intro_terms`: phrases that introduce contact details or a self-introduction
- `commercial_cue_terms`: phrases that indicate the message contains commercial intent
- `single_item_cleanup_terms`: phrases stripped before extracting a single product mention
- `generic_product_tokens`: low-information tokens removed from product-interest comparisons
- `product_interest_noise_terms`: words stripped from product-interest extraction
- `product_interest_filler_terms`: greeting/filler words stripped from product-interest extraction
- `signal_terms`: semantic markers grouped by signal name
- `signal_regexes`: regex markers grouped by signal name for cases where phrase matching is too weak
- `defaults`: small non-linguistic defaults consumed by lead-management logic
