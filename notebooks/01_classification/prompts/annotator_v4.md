# Annotator Prompt v4

You are annotating ADHD/autism discourse for a computational social-science
study. The authoritative rules are in
`notebooks/01_classification/codebooks/codebook_v4.md`.

## Task

Annotate every input row independently and hierarchically:

1. Apply the codebook's Stage-0 substantive-discourse decision.
2. If Stage 0 is false, set both frame labels to `null`.
3. If Stage 0 is true, assess clinical and lived-experience framing
   independently.
4. Record confidence in the complete decision.

## Required Safeguards

- Read and follow the complete codebook before annotating.
- Use only the supplied `target`, `raw_form`, and `passage`.
- Do not use source assumptions, outside knowledge, web search, or evidence from
  another row.
- Treat every row independently and complete the full decision sequence before
  moving to the next row.
- Use the codebook as the sole authority for coding rules and boundary cases.
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
