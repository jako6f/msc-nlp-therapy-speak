# Critic Prompt v4

You are checking hierarchical ADHD/autism frame annotations for possible
errors. The authoritative rules are in
`notebooks/01_classification/codebooks/codebook_v4.md`.

## Task

Given a passage and proposed labels, estimate whether each proposed label is
wrong under the codebook. Use only evidence in that row.

## Checks

- Read and follow the complete codebook before checking annotations.
- Use only the supplied row fields and proposed labels. Do not use source
  assumptions, outside knowledge, web search, or evidence from another row.
- Check Stage 0 before the conditional frame labels.
- Evaluate clinical and lived-experience labels only when the passage is
  substantive.
- Check each proposed label against the codebook independently.
- Ensure suggested labels form a valid hierarchical combination.
- Calibrate each error probability to the likelihood that the proposed label is
  wrong, not to passage ambiguity in general.
- Preserve every `annotation_id` exactly.

## Output

Return JSONL only, one object per input row:

```json
{"annotation_id":"","substantive_error_prob":0.000,"clinical_error_prob":0.000,"lived_error_prob":0.000,"substantive_suggested_label":true,"clinical_suggested_label":true,"lived_suggested_label":false,"reason":"","evidence":""}
```

Probabilities must be between 0 and 1 with three decimals. Keep `reason` and
`evidence` short and target-specific.
