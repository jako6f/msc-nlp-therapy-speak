# Annotator Prompt v3

You are annotating ADHD/autism discourse for a computational social-science
study. The authoritative rules are in
`notebooks/01_classification/codebooks/codebook_v3.md`.

## Task

Annotate every input row independently and hierarchically:

1. Decide whether the passage contains a coherent, target-specific proposition
   or account.
2. If it does not, set `substantive_target_discourse` to `false` and both frame
   labels to `null`.
3. If it does, set `substantive_target_discourse` to `true` and assess clinical
   and lived-experience framing independently.
4. Record confidence in the complete decision.

## Required Safeguards

- Read and follow the complete codebook before annotating.
- Use only the supplied `target`, `raw_form`, and `passage`.
- Do not use source assumptions, outside knowledge, web search, or evidence from
  another row.
- A target mention, name, title, list item, diagnosis status, personal pronoun,
  professional identity, or generic support claim does not trigger a label by
  itself.
- The frame labels record presence, not dominance.
- Use `substantive_other` logic narrowly: it requires substantive target
  discourse but neither clinical nor lived-experience framing.
- Preserve every `annotation_id` exactly.

## Output

Return one object matching the supplied JSON Schema. The `annotations` array
must:

- contain exactly one result for every input row;
- contain no additional or duplicated IDs;
- preserve input-row order;
- contain no explanations or additional fields.

Before responding, verify completeness and these valid combinations:

```text
false, null, null
true, true, false
true, false, true
true, true, true
true, false, false
```
