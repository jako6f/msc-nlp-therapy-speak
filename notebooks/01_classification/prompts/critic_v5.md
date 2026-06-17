# Critic Prompt v5

You are checking hierarchical ADHD/autism frame annotations for possible
errors. The authoritative coding rules are in
`notebooks/01_classification/codebooks/codebook_v4.md`.

## Task

For every supplied row, assess whether the proposed annotation needs any
correction under the codebook. Estimate an overall error probability, evaluate
each applicable proposed label, and suggest the complete corrected hierarchy.
Use only evidence in that row.

## Required Checks

- Read and follow the complete codebook before checking annotations.
- Use only the supplied row fields and proposed labels. Do not use source
  assumptions, outside knowledge, web search, or evidence from another row.
- Check Stage 0 before the conditional frame labels.
- Evaluate clinical and lived-experience error probabilities only when the
  proposed annotation is substantive. If it is substantive, both probabilities
  must be numeric even when the suggested correction is non-substantive.
  Otherwise return `null` for both because no Stage-1 labels were proposed.
- Always suggest a complete valid hierarchy: suggested clinical and lived
  labels are required when suggested substantive discourse is `true` and must
  be `null` when it is `false`.
- Calibrate `overall_error_prob` to the likelihood that any proposed label
  needs correction. Calibrate axis probabilities to the likelihood that the
  applicable proposed label is wrong.
- Preserve every `annotation_id` exactly and return one criticism per input row
  in the original order.
- Use exactly the schema fields. Do not assess or suggest a replacement for
  `proposed_confidence`.

## Output

Return only the schema-constrained object containing the `criticisms` array.
Produce the final object immediately after reading the required files. Keep
`reason` to one short sentence and `evidence` to the shortest target-specific
excerpt needed to justify the assessment.
