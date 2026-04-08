UOM lexicons are data files, not code.

How to add a new language:
1. Add aliases in the `aliases` section for the canonical UOM.
2. Add localized customer-facing labels in the `labels` section.
3. Keep canonical keys stable across languages, for example `piece`, `box`, `kg`.
4. Prefer customer wording and common abbreviations over exhaustive synonym lists.
5. Add or update tests that cover canonical resolution and localized labels.

Schema:
- `aliases`: `{canonical_uom: [alias1, alias2, ...]}`
- `labels`: `{canonical_uom: {lang: localized_label}}`

Notes:
- Tenant overrides through `uom_aliases` and `uom_labels` still work and are merged on top.
- Add new languages here rather than editing `uom_semantics.py`.
