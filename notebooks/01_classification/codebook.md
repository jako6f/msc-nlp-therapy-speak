# ADHD/Autism Frame Classification Codebook

## Purpose

This codebook supports annotation of ADHD/autism mention passages for a diachronic lexical-semantic-change analysis. The goal is to distinguish clinical/disorder framing from identity and lived-experience framing before downstream LSC measures are interpreted.

## Annotation Unit

Annotate one target mention passage at a time. The passage is the `target_sentence_plus_adjacent` field from the shared LSC context table.

Use only the evidence visible in the passage. Do not infer from the URL, domain, publication year, or general knowledge about the source.

## Axis 1: Clinical Frame Present

Mark `clinical_frame_present = TRUE` when the passage frames ADHD/autism as a diagnosis, disorder, medical or psychiatric condition, symptom profile, impairment, treatment target, service category, research category, epidemiological category, DSM/ICD-style construct, medication issue, or clinical/educational support need.

Typical positive cues:

- diagnosis, diagnosed, diagnostic, screening, assessment, symptoms, disorder, condition;
- treatment, medication, therapy, intervention, clinical care, clinician, healthcare, psychiatry;
- impairment, developmental delay, behavioural difficulties, comorbidity, prevalence, risk factor;
- school, disability, special education, services, support needs when framed institutionally or clinically.

Mark `clinical_frame_present = FALSE` when the target is mentioned without such framing, or when the passage is too noisy or non-substantive to establish the frame.

## Axis 2: Lived-Experience Frame Present

Mark `lived_experience_frame_present = TRUE` when the passage frames ADHD/autism through identity, self-understanding, first-person or family experience, neurodivergent community, masking, stigma, accommodations, everyday coping, belonging, pride, or embodied/social experience.

Typical positive cues:

- first-person or family experience: "I have ADHD", "my autistic child", "as an autistic adult";
- identity/community language: autistic identity, neurodivergent, community, belonging, pride;
- lived practical experience: masking, sensory experience, executive-function struggles, accommodations, stigma, self-advocacy;
- self-understanding or meaning-making around diagnosis or traits.

Mark `lived_experience_frame_present = FALSE` when the passage only treats ADHD/autism as an abstract category, clinical construct, research object, product/service category, or unclear mention.

## Derived Frame

Do not annotate this manually. It is derived from the two axes.

| clinical_frame_present | lived_experience_frame_present | derived_frame |
|---|---|---|
| TRUE | FALSE | `clinical_only` |
| FALSE | TRUE | `lived_only` |
| TRUE | TRUE | `mixed` |
| FALSE | FALSE | `other_non_substantive` |

## Boilerplate, Lists, and Noisy Text

If a passage is mostly navigation, keyword stuffing, spam, product lists, unrelated text, or extraction noise, mark both axes `FALSE` unless the frame is still clearly recoverable.

Use `uncertainty_note` to explain borderline cases briefly.

## Borderline Examples

Add examples during the pilot.

### Clinical Only

- Example:
- Reason:

### Lived Only

- Example:
- Reason:

### Mixed

- Example:
- Reason:

### Other or Non-Substantive

- Example:
- Reason:

## Pilot Revision Log

Record changes to the codebook here. Increment `codebook_version` when a rule changes in a way that could affect labels.

| date | codebook_version | change | reason |
|---|---|---|---|
| 2026-06-04 | v0.1 | Initial template. | Created before pilot annotation. |
