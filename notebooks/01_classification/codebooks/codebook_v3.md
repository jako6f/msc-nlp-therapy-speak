# ADHD/Autism Discourse Frame Annotation Codebook

**Codebook version:** v0.3  
**Status:** Post-pilot polished codebook; superseded by v0.4  
**Last updated:** 2026-06-09

## 1. Purpose

This codebook distinguishes how a passage discusses ADHD or autism. It separates:

1. **Clinical or disorder-construct discourse**, which presents the target as a
   diagnosable, assessable, treatable, researchable, or impairing condition.
2. **Lived-experience discourse**, which presents the target through identity,
   subjectivity, everyday life, relationships, participation, community, or
   social meaning.
3. **Other substantive target discourse**, which makes a meaningful
   target-specific claim but does not use either frame.
4. **Non-substantive or insufficient mentions**, which do not contain enough
   coherent target-specific discourse to classify.

The task is hierarchical. First decide whether the passage contains substantive
target discourse. Only then assess the clinical and lived-experience frames.
This prevents incidental or noisy mentions from becoming ordinary negative
examples for the two substantive frames.

## 2. Evidence Boundary

Annotate only the text in `target_sentence_plus_adjacent`.

- Use the target term identified in the annotation record.
- Use all relevant evidence within the supplied passage.
- Evidence about another diagnosis, condition, or population counts only when the
  passage explicitly applies it to the target.
- Do not open URLs or use the source domain, publication date, page title,
  author identity, or outside knowledge.
- Do not infer a frame from what the wider page probably contains.
- Do not require the target term to appear in every sentence.
- When extraction noise is present, use only coherent text whose relationship
  to the target is clear.

The task concerns the frame expressed in the passage, not the coder's view of
the target, the factual accuracy of the passage, or the author's presumed
intent.

## 3. Variables and Allowed Values

Annotate four variables:

| variable | allowed values | meaning |
|---|---|---|
| `substantive_target_discourse` | `TRUE`, `FALSE` | Whether the passage contains enough coherent, target-specific discourse to classify |
| `clinical_frame_present` | `TRUE`, `FALSE`, `NA` | Whether a clinical/disorder-construct frame is present, conditional on substantive discourse |
| `lived_experience_frame_present` | `TRUE`, `FALSE`, `NA` | Whether a lived-experience frame is present, conditional on substantive discourse |
| `confidence` | `high`, `medium`, `low` | Confidence in the complete annotation decision |

`derived_frame` is calculated from the three frame variables. Do not annotate it
manually.

## 4. Required Decision Order

Apply these steps in order:

1. **Stage 0:** Does the passage contain a coherent, target-specific proposition
   or account?
2. If Stage 0 is `FALSE`, set both frame labels to `NA` and stop.
3. If Stage 0 is `TRUE`, assess the clinical frame and lived-experience frame
   independently.
4. Derive the final frame from the label combination.
5. Record confidence.

The frame variables record presence, not the dominant or primary frame. A
passage is mixed whenever both frames independently meet their definitions,
even if one receives more space or emphasis.

When uncertain at Stage 0, code `substantive_target_discourse = FALSE`.
Do not use clinical or lived labels to rescue an insufficient passage.

## 5. Stage 0: Substantive Target Discourse

### 5.1 Definition

Code `substantive_target_discourse = TRUE` only when the passage expresses at
least one coherent proposition, description, argument, or account specifically
about:

- ADHD or autism as a category, condition, or concept;
- ADHD/autistic people or communities;
- target-specific experiences, identities, needs, practices, services, or
  representation.

A target mention alone is not substantive discourse.

### 5.2 Code `TRUE` when

The passage contains enough target-specific content to determine whether a
clinical frame, lived-experience frame, both, or neither is present. Examples
include:

- explaining diagnostic criteria, symptoms, causes, treatment, or prevalence;
- describing an autistic person's everyday or social experience;
- discussing ADHD identity, stigma, advocacy, or accessibility;
- explicitly offering assessment or treatment for ADHD or autism;
- making a coherent claim about how autism is represented in media.

A short passage or standalone headline can be substantive if it expresses a
clear, classifiable target-specific claim.

### 5.3 Code `FALSE` when

The target appears only as:

- navigation, a tag, breadcrumb, footer, citation fragment, or related-link
  label;
- an organisation, programme, event, product, book, or page name without a
  target-specific claim;
- one item in a generic list of conditions, populations, providers, resources,
  or topics;
- a generic statement that merely says support or resources exist;
- promotional or administrative text without target-specific content;
- incoherent, truncated, duplicated, or contaminated extraction;
- a thin biographical status mention that does not develop a clinical,
  lived-experience, or other target-specific proposition.

Passage length does not determine substantiveness. Long event descriptions and
resource blurbs can still be non-substantive.

### 5.4 Stage 0 boundary test

Ask:

> If the target term, target-named organisation, or target-named event were
> removed, would the remaining passage still communicate essentially the same
> point?

If yes, the passage is usually non-substantive. This is a diagnostic aid, not a
replacement for the definition above.

## 6. Stage 1A: Clinical Frame

### 6.1 Definition

Code `clinical_frame_present = TRUE` when substantive discourse presents ADHD
or autism as a nosological, medical, psychiatric, psychological, diagnostic,
epidemiological, research, treatment, impairment, or institutional service
object.

The target need not be described negatively. A clinical frame can be neutral,
supportive, critical, or positive.

### 6.2 Positive indicators

Clinical evidence includes target-specific discussion of:

- diagnosis, diagnostic criteria, screening, assessment, or misdiagnosis;
- symptoms, deficits, dysfunction, severity, impairment, or comorbidity;
- causes, risk factors, genetics, neurobiology, prognosis, or prevalence;
- medication, therapy, treatment, intervention, rehabilitation, or prevention;
- clinical research, trials, case reports, or patient cohorts;
- eligibility, institutional provision, or professional services that treat,
  assess, or manage the target as a condition;
- legal or educational mitigation explicitly responding to target-related
  impairment.

### 6.3 Do not code clinical merely because

- the word `diagnosed`, `patient`, `support`, `therapy`, or `treatment` appears;
- the author is a clinician or the source is a health website;
- a professional credential or provider list mentions autism or ADHD;
- a person simply states that they or a family member has a diagnosis;
- clinical terminology is quoted but the passage does not meaningfully discuss
  the target as a clinical object.

Clinical evidence must be both target-specific and substantively developed.

## 7. Stage 1B: Lived-Experience Frame

### 7.1 Definition

Code `lived_experience_frame_present = TRUE` when substantive discourse presents
ADHD or autism through personal or collective experience, identity, everyday
life, relationships, participation, community, or social meaning.

The passage does not need to be first-person. Lived experience may be described
by the person concerned, a family member, a journalist, an advocate, or another
speaker.

### 7.2 Positive indicators

Lived-experience evidence includes target-specific discussion of:

- first-person or family accounts of feelings, practical challenges, coping,
  adaptation, or meaning-making;
- identity, self-understanding, disclosure, belonging, or neurodivergence;
- everyday participation at home, school, work, or in relationships;
- stigma, stereotypes, discrimination, acceptance, or social recognition;
- community, peer support, advocacy, or collective voice;
- accommodation, accessibility, inclusion, or rights as experienced social
  participation;
- strengths, preferences, communication styles, or ways of being;
- family or caregiver experience when their emotional or practical perspective
  is genuinely foregrounded.

### 7.3 Do not code lived experience merely because

- the passage uses `I`, `we`, `my child`, or another personal pronoun;
- a person or family member is said to have ADHD or autism;
- the passage mentions people, families, support, awareness, inclusion, or
  community in generic terms;
- a clinical description is compassionate or person-centred;
- symptoms or impairment are described without a developed experiential,
  identity, or social perspective.

Lived-experience evidence must be target-specific and substantively developed.

## 8. High-Impact Boundary Rules

Apply these rules after the general definitions. They resolve recurring pilot
ambiguities.

### 8.1 Diagnosis as status versus clinical object

A diagnosis mention alone is not clinical evidence.

- **Status only:** "My daughter has ADHD." This may be non-substantive if no
  further target-specific account is supplied.
- **Clinical object:** "The clinician assessed her for ADHD using..." Code
  clinical.
- **Lived account:** "Learning I was autistic changed how I understood years of
  social exhaustion." Code lived experience.
- **Both:** "After assessment confirmed ADHD, medication helped at work but I
  struggled with how colleagues treated me." Code mixed.

### 8.2 Services, support, resources, and lists

Distinguish an explicit target-specific service proposition from a generic list.

- Code clinical when the passage explicitly presents ADHD/autism as something
  the named service assesses, treats, or manages.
- Code lived experience when the passage substantively describes
  target-specific peer support, accessibility, advocacy, or participation.
- Code non-substantive when the target appears only in a list of conditions,
  providers, audiences, resources, or services.
- Generic claims such as "support is available for people with autism" are
  insufficient unless the passage explains the target-specific form or purpose
  of that support.

### 8.3 First-person, family, and caregiver language

Personal language identifies a speaker, not a frame.

- Code lived experience only when the passage communicates a target-specific
  subjective, practical, relational, identity, or social account.
- Code clinical when a first-person passage substantively discusses diagnosis,
  symptoms, treatment, or impairment as clinical objects.
- Code both when both forms of evidence independently meet their definitions.

### 8.4 Coping versus intervention

- Everyday strategies chosen or described as part of living with the target
  usually support a lived-experience label.
- Professional treatment or intervention intended to reduce symptoms or
  impairment supports a clinical label.
- A passage can contain both.

### 8.5 Neurodiversity, accessibility, and accommodation

Neurodiversity, identity, accessibility, and participation usually support a
lived-experience label when target-specific and substantive.

Do not automatically add a clinical label because a diagnosis or clinical term
also appears. Add clinical only when the passage independently treats the target
as a clinical, impairment, treatment, assessment, research, or service object.

### 8.6 Professional profiles and credentials

Professional identity is not frame evidence.

- "Autism consultant" in a provider list is non-substantive.
- A profile that explains how the professional assesses or treats autism is
  clinical.
- A profile that substantively discusses autistic people's perspectives,
  participation, or advocacy is lived experience.

### 8.7 Organisations, events, titles, and media

- A target-named organisation, event, programme, book, or article title alone is
  non-substantive.
- Event promotion remains non-substantive when the passage discusses logistics,
  fundraising, or participants but makes no target-specific claim.
- A coherent claim about target representation, terminology, or public
  discourse may be `substantive_other` when neither clinical nor lived evidence
  is present.

### 8.8 Mixed-frame threshold

Code `mixed` only when clinical and lived-experience evidence each independently
meet their definitions.

Do not infer lived experience merely because a clinical feature affects life.
Do not infer clinical framing merely because a lived account mentions a
diagnosis. When one frame is explicit and the other is only implied, code only
the explicit frame.

### 8.9 Substantive other

Use `substantive_other` narrowly. It requires a coherent target-specific
proposition, but no qualifying clinical or lived-experience evidence.

Typical cases include:

- a metalinguistic claim about the public use of the word `autism`;
- analysis of autism as a media category without discussing people's
  experiences or a disorder construct;
- a target-specific administrative or political claim that does not develop
  clinical or lived-experience content.

Do not use `substantive_other` as a fallback for confusing, thin, noisy, or
generic passages. Those are non-substantive.

## 9. Derived Frame

Derive the final frame exactly as follows:

| substantive | clinical | lived experience | `derived_frame` |
|---|---|---|---|
| `FALSE` | `NA` | `NA` | `non_substantive_or_insufficient` |
| `TRUE` | `TRUE` | `FALSE` | `clinical_only` |
| `TRUE` | `FALSE` | `TRUE` | `lived_only` |
| `TRUE` | `TRUE` | `TRUE` | `mixed` |
| `TRUE` | `FALSE` | `FALSE` | `substantive_other` |

No other combination is valid.

## 10. Confidence

Confidence describes certainty in the complete annotation, not the strength or
severity of the frame.

| value | use when |
|---|---|
| `high` | The relevant rule clearly applies and no plausible competing coding remains. |
| `medium` | The passage is classifiable, but a boundary rule or competing cue requires judgement. |
| `low` | Genuine ambiguity remains after applying the codebook, but one valid label combination is still more defensible than the alternatives. |

Use only `high`, `medium`, or `low`.

Do not use low confidence instead of the conservative Stage 0 rule. If there is
not enough coherent target-specific discourse to classify, code Stage 0
`FALSE`.

## 11. Canonical Examples

These examples illustrate the minimum reasoning required. Treat them as
precedents for the stated distinction, not as keyword templates.

| passage | substantive | clinical | lived | derived frame | reason |
|---|---|---|---|---|---|
| "Our contributors include autism consultants, psychologists, teachers, and occupational therapists." | `FALSE` | `NA` | `NA` | `non_substantive_or_insufficient` | Autism appears only in a provider list. |
| "The clinic assesses ADHD and offers medication management for diagnosed patients." | `TRUE` | `TRUE` | `FALSE` | `clinical_only` | ADHD is explicitly an assessment and treatment object. |
| "As an autistic employee, I plan recovery time after crowded meetings and ask colleagues to send written agendas." | `TRUE` | `FALSE` | `TRUE` | `lived_only` | The passage develops an everyday first-person account and accommodation practice. |
| "Assessment confirmed ADHD; treatment reduced her symptoms, while workplace stigma continued to affect her sense of belonging." | `TRUE` | `TRUE` | `TRUE` | `mixed` | Diagnosis/treatment and social experience are independently explicit. |
| "The study counts how often the word autism appears in television programme titles." | `TRUE` | `FALSE` | `FALSE` | `substantive_other` | It makes a coherent target-specific metalinguistic claim without either substantive frame. |

### Contrast pairs

| non-qualifying case | qualifying case | distinction |
|---|---|---|
| "Join the Autism Foundation annual gala." | "The study counts uses of the word autism in television programme titles." | A target-named event is not target discourse; a target-specific metalinguistic claim is. |
| "We support families affected by ADHD." | "Parents describe changing household routines to help their ADHD children manage mornings." | Generic support language is insufficient; a developed family practice is lived experience. |
| "She has an autism diagnosis." | "Clinicians frequently miss autism in adult women because the screening criteria..." | Diagnostic status alone is insufficient; diagnostic analysis is clinical. |
| "Services include anxiety, ADHD, and sleep problems." | "The ADHD service provides diagnostic assessment and medication review." | A condition list is insufficient; an explicit target-specific service proposition is clinical. |

## 12. Orthogonal Features

Do not confuse the following properties with the frame labels:

- **Register:** professional, academic, journalistic, or informal language can
  express either frame.
- **Stance or valence:** supportive, critical, stigmatising, and neutral passages
  can express either frame.
- **Genre:** research articles, personal blogs, news reports, service pages, and
  advocacy pages do not determine labels by themselves.
- **Factual accuracy:** annotate the expressed frame even when a claim appears
  inaccurate.

## 13. Annotation Procedure

For each row:

1. Read the complete `target_sentence_plus_adjacent` passage.
2. Apply the Stage 0 substantive-discourse gate.
3. If Stage 0 is `FALSE`, enter `NA` for both frame labels.
4. If Stage 0 is `TRUE`, assess clinical and lived experience independently.
5. Check the resulting combination against the derived-frame table.
6. Record `high`, `medium`, or `low` confidence.
7. Do not edit source-text or metadata columns.

For spreadsheet annotation:

- use uppercase `TRUE`, `FALSE`, and `NA`;
- do not leave required annotation cells blank;
- do not add a manually coded `derived_frame` column;
- preserve the row order and `annotation_id`.

## 14. Machine-Readable Decision Contract

Human and LLM coders must obey these constraints:

```text
if substantive_target_discourse == FALSE:
    clinical_frame_present = NA
    lived_experience_frame_present = NA

if substantive_target_discourse == TRUE:
    clinical_frame_present in {TRUE, FALSE}
    lived_experience_frame_present in {TRUE, FALSE}

confidence in {high, medium, low}
```

Return only values permitted by the annotation task's requested output schema.
Explanations may support adjudication when explicitly requested, but they must
not replace or alter the required labels.

## 15. Final Checklist

Before submitting an annotation, confirm:

- I used only the supplied passage.
- Stage 0 reflects a coherent target-specific proposition, not a target mention.
- Generic names, lists, and support language did not trigger a substantive
  frame.
- Clinical and lived-experience evidence each independently meet their
  definitions.
- Diagnosis status, first-person language, and professional identity did not
  trigger a frame by themselves.
- `substantive_other` is not being used for an insufficient passage.
- The label combination is valid.
- Confidence is one of `high`, `medium`, or `low`.
