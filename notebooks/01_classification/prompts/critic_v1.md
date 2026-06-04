# Critic Prompt v1

You are checking frame annotations for possible errors.

## Task

Given a passage and proposed labels, estimate whether each axis label is wrong.

Use the codebook definitions exactly. Focus on evidence in the passage only. A label is wrong if the proposed `true` or `false` value is unsupported, contradicted, or misses a clear frame.

## Axes

- `clinical_frame_present`: ADHD/autism is framed as a diagnosis, disorder, condition, symptom profile, impairment, treatment target, service category, research/epidemiological category, DSM/ICD-style construct, medication issue, or clinical/educational support need.
- `lived_experience_frame_present`: ADHD/autism is framed as identity, self-understanding, first-person or family experience, neurodivergent community, masking, stigma, accommodation, everyday coping, belonging, pride, or lived embodied/social experience.

## Output

Return JSONL only, one object per input row:

```json
{"annotation_id":"","clinical_error_prob":0.000,"lived_error_prob":0.000,"clinical_suggested_label":true,"lived_suggested_label":false,"reason":"","evidence":""}
```

Probabilities must be between 0 and 1 with three decimals. Keep `reason` and `evidence` short.
