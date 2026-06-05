# Annotator Prompt v2

You are annotating ADHD/autism discourse for a computational social-science study.

## Task

For each passage, apply the codebook hierarchically.

Stage 0: decide whether the passage contains enough coherent, target-specific discourse about ADHD/autism to classify its frame.

Stage 1: only if Stage 0 is true, decide whether clinical and/or lived-experience framing is present.

## Labels

- `substantive_target_discourse`: true when the passage contains enough coherent, target-specific discourse about ADHD/autism, autistic/ADHD people, traits, diagnosis, support, treatment, research, stigma, inclusion, or everyday life to classify framing. If uncertain, use false.
- `clinical_frame_present`: true only for substantive passages that frame ADHD/autism as a diagnosis, disorder, condition, symptom profile, impairment, treatment target, service/research category, medication issue, or clinical/educational/institutional support need.
- `lived_experience_frame_present`: true only for substantive passages that frame ADHD/autism through identity, self/family experience, neurodiversity, stigma, accommodation, advocacy, community, everyday coping, belonging, pride, or embodied/social experience.
- `confidence`: one of `high`, `medium`, `low`.

## Rules

- Use only evidence in the passage. Do not infer from URL, domain, year, source reputation, or outside knowledge.
- If the target mention is navigational, list-like, generic, promotional, incidental, noisy, garbled, or too thin, set `substantive_target_discourse` to false.
- If `substantive_target_discourse` is false, set both frame labels to `null`.
- If `substantive_target_discourse` is true, set both frame labels to true or false.
- Generic cues such as support, diagnosed, condition, resources, consultant, foundation, or awareness are not enough by themselves.

## Output

Return JSONL only, one object per input row:

```json
{"annotation_id":"","substantive_target_discourse":true,"clinical_frame_present":true,"lived_experience_frame_present":false,"confidence":"high"}
```
