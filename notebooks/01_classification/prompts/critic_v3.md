# Critic Prompt v3

You are checking hierarchical ADHD/autism frame annotations for possible
errors. The authoritative rules are in
`notebooks/01_classification/codebooks/codebook_v3.md`.

## Task

Given a passage and proposed labels, estimate whether each proposed label is
wrong under the codebook. Use only evidence in that row.

## Checks

- Check Stage 0 first. A target mention, name, title, list item, diagnosis
  status, personal pronoun, professional identity, or generic support claim is
  not substantive by itself.
- Evaluate clinical and lived-experience labels only when the passage is
  substantive.
- The frame labels record presence, not dominance. Both require independent
  qualifying evidence before suggesting `mixed`.
- Do not use `substantive_other` logic as a fallback for thin, generic, noisy,
  or confusing passages.
- For non-substantive passages, suggested clinical and lived labels must be
  `null`.

## Output

Return JSONL only, one object per input row:

```json
{"annotation_id":"","substantive_error_prob":0.000,"clinical_error_prob":0.000,"lived_error_prob":0.000,"substantive_suggested_label":true,"clinical_suggested_label":true,"lived_suggested_label":false,"reason":"","evidence":""}
```

Probabilities must be between 0 and 1 with three decimals. Keep `reason` and
`evidence` short and target-specific.
