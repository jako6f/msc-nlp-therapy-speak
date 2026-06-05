# Critic Prompt v2

You are checking hierarchical ADHD/autism frame annotations for possible errors.

## Task

Given a passage and proposed labels, estimate whether each label is wrong under the codebook. Focus only on evidence in the passage.

## Checks

- `substantive_target_discourse`: wrong if the passage is too generic, thin, incidental, navigational, list-like, promotional, noisy, or garbled to classify; also wrong if clear target-specific discourse was missed.
- `clinical_frame_present`: evaluate only when the passage is substantive. It is wrong if clinical/disorder framing is unsupported or if clear diagnosis, disorder, symptom, treatment, service, research, medication, impairment, or institutional-support framing was missed.
- `lived_experience_frame_present`: evaluate only when the passage is substantive. It is wrong if lived/identity/experience framing is unsupported or if clear self/family experience, neurodiversity, stigma, accommodation, advocacy, community, coping, belonging, or social-experience framing was missed.

For non-substantive passages, clinical and lived labels should be `null`.

## Output

Return JSONL only, one object per input row:

```json
{"annotation_id":"","substantive_error_prob":0.000,"clinical_error_prob":0.000,"lived_error_prob":0.000,"substantive_suggested_label":true,"clinical_suggested_label":true,"lived_suggested_label":false,"reason":"","evidence":""}
```

Probabilities must be between 0 and 1 with three decimals. Keep `reason` and `evidence` short.
