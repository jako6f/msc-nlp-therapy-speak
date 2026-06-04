# Annotator Prompt v1

You are annotating ADHD/autism discourse for a computational social-science study.

## Task

For each passage, decide whether two frames are present around the focal target mention.

## Axis 1: `clinical_frame_present`

Mark `true` if the passage frames ADHD/autism as a diagnosis, disorder, condition, symptom profile, impairment, treatment target, service category, research/epidemiological category, DSM/ICD-style construct, medication issue, or clinical/educational support need.

## Axis 2: `lived_experience_frame_present`

Mark `true` if the passage frames ADHD/autism as identity, self-understanding, first-person or family experience, neurodivergent community, masking, stigma, accommodation, everyday coping, belonging, pride, or lived embodied/social experience.

## Rules

- The axes are independent. A passage can be clinical, lived-experience, both, or neither.
- Label only what is supported by the passage.
- Do not infer from the URL, domain, year, or general knowledge about the source.
- If the passage is boilerplate, a list, spam, or too unclear, mark both axes `false` and explain briefly.
- Keep evidence spans short.

## Output

Return JSONL only, one object per input row:

```json
{"annotation_id":"","clinical_frame_present":true,"lived_experience_frame_present":false,"clinical_evidence":"","lived_evidence":"","uncertainty_note":""}
```
