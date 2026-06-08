# ADHD/Autism Frame Classification Codebook

**Project:** Diachronic lexical-semantic-change analysis of ADHD/autism discourse
**Codebook version:** v0.2 provisional
**Status:** Pilot annotation codebook
**Last updated:** 2026-06-05

---

## 1. Purpose

This codebook supports annotation of ADHD/autism mention passages for a diachronic lexical-semantic-change analysis. The goal is to distinguish whether target mentions are part of:

1. sufficiently coherent, target-specific discourse; and, if so,
2. clinical/disorder framing, lived-experience/identity framing, both, or neither.

The revised workflow uses a two-stage annotation scheme:

```text
Stage 0: Is there enough substantive target-specific discourse to classify the frame?

Stage 1: If yes, is ADHD/autism framed clinically and/or through lived experience?
```

This separation is necessary because many Common Crawl passages contain ADHD/autism terms in noisy, generic, navigational, promotional, organisational, or list-like contexts. Such passages should not be forced into clinical/lived-experience categories.

---

## 2. Annotation Unit

Annotate one target mention passage at a time.

The annotation unit is the `target_sentence_plus_adjacent` field from the shared LSC context table.

Use only the evidence visible in the passage. Do **not** infer from:

* URL;
* domain;
* publication year;
* source reputation;
* outside knowledge about the organisation, author, or topic.

If the passage itself does not provide enough target-specific evidence, code conservatively.

---

## 3. Core Variables

Annotate the following fields:

| Field                            | Values                    |         Annotated manually? | Description                                                                                                                                                                |
| -------------------------------- | ------------------------- | --------------------------: | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `substantive_target_discourse`   | `TRUE` / `FALSE`          |                         Yes | Whether the passage contains enough coherent, target-specific discourse to classify ADHD/autism framing.                                                                   |
| `clinical_frame_present`         | `TRUE` / `FALSE` / `NA`   | Yes, only if Stage 0 = TRUE | Whether ADHD/autism is framed as a diagnosis, disorder, condition, impairment, treatment target, service/research category, or clinical/institutional support need.        |
| `lived_experience_frame_present` | `TRUE` / `FALSE` / `NA`   | Yes, only if Stage 0 = TRUE | Whether ADHD/autism is framed through identity, lived experience, self/family perspective, neurodiversity, community, stigma, accommodation, advocacy, or everyday coping. |
| `derived_frame`                  | see Section 7             |                          No | Automatically derived from the three fields above.                                                                                                                         |
| `confidence`                     | `high` / `medium` / `low` |                         Yes | Annotator confidence in the coding decision.                                                                                                                               |

Important: if `substantive_target_discourse = FALSE`, then set:

```text
clinical_frame_present = NA
lived_experience_frame_present = NA
```

Do not code them as `FALSE`. The frame question is not applicable when the passage lacks sufficient target-specific substance.

---

## 4. Stage 0: Substantive Target Discourse

### 4.1 Core Question

Ask:

> Does the passage contain enough coherent, target-specific information about ADHD/autism, ADHD/autistic people, traits, experiences, diagnosis, support, treatment, services, research, stigma, inclusion, or everyday life to classify the target framing?

If yes, code:

```text
substantive_target_discourse = TRUE
```

If no, code:

```text
substantive_target_discourse = FALSE
```

If uncertain, code `FALSE`.

This field is a **sufficiency gate**, not a quality rating. Some passages coded `FALSE` may be coherent English, but they are too generic, incidental, or thin to classify the target frame.

---

### 4.2 Code TRUE When

Code `substantive_target_discourse = TRUE` when the passage makes a coherent target-specific claim, description, explanation, evaluation, or account.

Typical TRUE cases include passages that discuss:

* ADHD/autism symptoms, diagnosis, treatment, assessment, impairment, medication, intervention, support needs, or research;
* autistic/ADHD people’s experiences, identity, stigma, self-understanding, family experience, social participation, masking, accommodation, community, or advocacy;
* neurodiversity and social recognition of autistic/ADHD people;
* target-specific school, workplace, legal, medical, therapeutic, or social support contexts;
* target-specific challenges, resources, rights, barriers, capacities, behaviours, or coping strategies.

Examples:

```text
People with ASD often have difficulty communicating, fail to respond to social cues, tend to repeat particular actions, and sometimes become preoccupied with certain objects.
```

```text
Autistic people face barriers to inclusion in employment and education, and the autism rights movement advocates for autistic voices and perspectives.
```

```text
She has ADHD and uses body doubling to complete important but not urgent work tasks.
```

---

### 4.3 Code FALSE When

Code `substantive_target_discourse = FALSE` when the target mention is not sufficiently developed to classify its frame.

Typical FALSE cases include:

* navigation menus;
* keyword lists;
* search results;
* headline feeds;
* article recommendation lists;
* event calendars;
* organisation names only;
* fundraising or promotional blurbs where autism/ADHD appears only in an event/foundation name;
* generic resource-directory text;
* generic provider lists;
* spam;
* scraped notes;
* garbled extraction;
* incoherent medical or commercial text;
* target terms appearing only in metadata, titles, archive labels, or link text;
* passages where the surrounding prose is substantive but not about the target.

Examples:

```text
next: Different Types of Educational Assessment Tests ~ back to Parent Advocate homepage ~ adhd library articles ~ all add/adhd articles
```

Reason: ADHD appears only in site navigation.

```text
Eagles Autism Foundation ... Eagles Autism Challenge ... celebrity golf outing ... celebrity bartending event
```

Reason: autism appears mainly in organisation/event names; the passage is about fundraising and sports events.

```text
Autism spectrum disorder should be performed. Pentosan toms, with a dose conversion table...
```

Reason: garbled extraction/no coherent target-specific proposition.

```text
Our articles feature advice from specialists in various fields (autism consultants, psychologists, speech-language pathologists, occupational therapists...)
```

Reason: autism appears only in a generic provider list; the passage is about the publication/resource brand, not autism itself.

```text
The Autism Society has documents available containing resources to support individuals with Autism and their families.
```

Reason: generic resource-directory language; too thin to establish clinical or lived-experience framing unless the passage gives more target-specific detail.

```text
SoulFriends is a portal for information, services and products on human potential, social fulfillment and spiritual actualization. fb tw ln Avoiding ADHD Insanity Laurie Dupar, PMHNP, RN, PCC, CPCC Albert Einstein once said‚Ä¶‚ÄùInsanity: doing the same thing over and over again and expecting different results. ‚Äù Sound familiar?
```

Reason: The passage is mostly portal/navigation metadata plus a title: “Avoiding ADHD Insanity”. The target-bearing content does not yet provide enough coherent ADHD-specific discourse to classify the frame; it only introduces a page/article heading and author credentials.

---

### 4.4 Conservative Rule

Use this rule whenever unsure:

> Code `substantive_target_discourse = TRUE` only when the passage contains enough coherent, target-specific evidence to classify the ADHD/autism frame. If uncertain, code `FALSE`.

This makes the downstream clinical/lived-experience labels cleaner and reduces contamination from boilerplate, lists, generic support language, and incidental mentions.

---

## 5. Stage 1A: Clinical Frame Present

Only annotate this field when:

```text
substantive_target_discourse = TRUE
```

Otherwise set:

```text
clinical_frame_present = NA
```

### 5.1 Core Question

Ask:

> Does the passage frame ADHD/autism as a diagnosis, disorder, condition, symptom profile, impairment, treatment target, service category, research category, epidemiological category, DSM/ICD-style construct, medication issue, or clinical/educational/institutional support need?

If yes:

```text
clinical_frame_present = TRUE
```

If no:

```text
clinical_frame_present = FALSE
```

---

### 5.2 Typical Positive Cues

Clinical frame is usually present when ADHD/autism is framed through:

* diagnosis, diagnosed, diagnostic, screening, assessment;
* symptoms, symptom profile, disorder, condition, syndrome;
* treatment, medication, therapy, intervention, clinical care;
* healthcare, clinician, psychiatrist, psychologist, practitioner;
* impairment, developmental delay, behavioural difficulties;
* comorbidity, prevalence, risk factor, epidemiology;
* research category, study population, trial, intervention efficacy;
* special education, disability services, school support, support needs when framed institutionally or clinically;
* medication abuse, prescription, treatment response;
* legal/forensic mitigation through diagnosis, medication, or clinical explanation.

---

### 5.3 Clinical Frame Examples

#### Clinical-only example: developmental/symptom description

```text
Cabibihan's thoughts turned to using robots as a tool for working with children who do have developmental issues, specifically autism spectrum disorders (ASD). People with ASD often have difficulty communicating, fail to respond to social cues, tend to repeat particular actions and sometimes become preoccupied with certain objects.
```

Coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = TRUE
lived_experience_frame_present = FALSE
derived_frame = clinical_only
```

Reason: ASD is framed as a developmental/clinical category with communication, social-cue, repetitive-action, and preoccupation features.

#### Clinical-only example: intervention/research framing

```text
Developing a parents' focused model to improve the daily functioning of young children with symptoms of ADHD; investigating its feasibility and efficacy - a pilot study.
```

Coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = TRUE
lived_experience_frame_present = FALSE
derived_frame = clinical_only
```

Reason: ADHD is framed through symptoms, daily functioning, intervention efficacy, and pilot-study research.

#### Clinical-only example: legal/forensic mitigation

```text
Roberts had recently been diagnosed with ADHD, had been prescribed medication and was not a “sophisticated criminal”.
```

Coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = TRUE
lived_experience_frame_present = FALSE
derived_frame = clinical_only
```

Reason: ADHD is invoked through diagnosis and medication as a legal/mitigating category.

#### Clinical-only example: treatment/service list

```text
We can help you with... Attention Deficit Disorder (ADD/ADHD), PTSD, anxiety, panic disorders, depression, sleep disorders, behavioural problems...
```

Coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = TRUE
lived_experience_frame_present = FALSE
derived_frame = clinical_only
```

Reason: ADD/ADHD is listed as a condition/problem addressed by health-care practitioners or treatment technology.

---

### 5.4 Clinical Frame Caveats

Clinical terminology is evidence, not an automatic decision rule.

Do **not** code clinical merely because the passage contains words such as:

```text
diagnosed
condition
disorder
support
needs
therapy
```

Ask how those words function in the local passage.

#### Diagnosis as clinical object

Code clinical when diagnosis is framed as:

* assessment;
* medical classification;
* treatment gateway;
* institutional service category;
* impairment explanation;
* medication/prescription context;
* legal/forensic mitigation;
* research/epidemiological category.

#### Diagnosis as biographical/status context

Do not automatically code clinical when diagnosis functions merely as background identity/status within everyday self/family experience.

Example:

```text
Haselberger's son has not been diagnosed with ADHD, but he continues to find body doubling helpful. So does Haselberger, who does have ADHD. She asks her husband to be around while she completes certain tasks.
```

Coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = FALSE
lived_experience_frame_present = TRUE
derived_frame = lived_only
```

Reason: diagnosis is mentioned only as background status; the substantive frame is everyday coping and task management.

#### Neurodiversity using clinical terms

Do not code clinical merely because neurodiversity discourse names diagnostic categories.

Example:

```text
Neurodiversity is the positive recognition of the fact that human beings are neurologically diverse. This variability can manifest itself in various ways in those affected by autism spectrum disorder, ADHD, dyslexia and dyspraxia. The concept of neurodiversity is intended to destigmatise people who are neurodifferent and recognise their potential within society.
```

Coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = FALSE
lived_experience_frame_present = TRUE
derived_frame = lived_only
```

Reason: clinical labels are subordinated to neurodiversity, destigmatisation, and social recognition.

---

## 6. Stage 1B: Lived-Experience Frame Present

Only annotate this field when:

```text
substantive_target_discourse = TRUE
```

Otherwise set:

```text
lived_experience_frame_present = NA
```

### 6.1 Core Question

Ask:

> Does the passage frame ADHD/autism through identity, self-understanding, first-person or family experience, neurodivergent community, masking, stigma, accommodation, everyday coping, belonging, pride, advocacy, inclusion, autistic/ADHD voices, or embodied/social experience?

If yes:

```text
lived_experience_frame_present = TRUE
```

If no:

```text
lived_experience_frame_present = FALSE
```

---

### 6.2 Typical Positive Cues

Lived-experience frame is usually present when ADHD/autism is framed through:

* first-person experience: “I have ADHD”, “as an autistic adult”;
* family experience: “my autistic child”, “parents describe...”;
* everyday coping: task management, routines, body doubling, sensory coping, executive-function strategies;
* self-understanding or meaning-making around diagnosis or traits;
* identity/community language: autistic identity, neurodivergent, neurodiversity, community, belonging, pride;
* masking, sensory experience, executive-function struggles;
* stigma, marginalisation, misunderstanding, discrimination;
* accommodation, accessibility, inclusion, workplace/school participation;
* advocacy, self-advocacy, rights movements, autistic/ADHD voices;
* strengths, capabilities, counternarratives to deficit-only views.

---

### 6.3 Lived-Experience Examples

#### Lived-only example: autism rights and self-advocacy

```text
Autism still continues to be heavily misunderstood and stigmatized, creating barriers to inclusion both in employment and in the educational system. The need for inclusion and accessibility has given rise to the autism rights movement, a grassroots effort by individuals with autism to share their distinct perspectives and advocate for themselves in their own voices.
```

Coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = FALSE
lived_experience_frame_present = TRUE
derived_frame = lived_only
```

Reason: autism is framed through stigma, inclusion, accessibility, rights, self-advocacy, and autistic perspectives.

#### Lived-only example: everyday coping

```text
Haselberger's son has not been diagnosed with ADHD, but he continues to find body doubling helpful. So does Haselberger, who does have ADHD. She asks her husband to be around while she completes certain tasks, and she organises regular video calls with work peers to make progress on important but not urgent goals.
```

Coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = FALSE
lived_experience_frame_present = TRUE
derived_frame = lived_only
```

Reason: the passage foregrounds everyday coping, task management, family/work routines, and practical ADHD experience.

#### Lived-only example: neurodiversity and destigmatisation

```text
Neurodiversity is the positive recognition of the fact that human beings are neurologically diverse. The concept of neurodiversity is intended to destigmatise people who are neurodifferent and recognise their potential within society.
```

Coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = FALSE
lived_experience_frame_present = TRUE
derived_frame = lived_only
```

Reason: the dominant frame is neurodiversity, destigmatisation, social recognition, and valuing neurodifferent people.

#### Lived-only example: academic module through neurodiversity

```text
This module offers you an opportunity to learn about autism through the lens of neurodiversity. Neurodiversity offers a new perspective on our understanding of autism by emphasising the strengths and capabilities of autistic people, as well as challenging the marginalisation and underrepresentation of autistic voices in research and everyday life.
```

Coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = FALSE
lived_experience_frame_present = TRUE
derived_frame = lived_only
```

Reason: autism is framed through neurodiversity, autistic voices, strengths, capabilities, marginalisation, and everyday life.

---

### 6.4 Lived-Experience Caveats

Do not code lived-experience merely because the passage uses warm, supportive, humanising, or inclusion-adjacent language.

Generic phrases such as the following are not enough on their own:

```text
support individuals with autism and their families
autism awareness event
resources for people with autism
autism centre
autism foundation
```

These may be lived-experience-adjacent, but they are too generic unless the passage provides substantive target-specific content about experience, stigma, coping, identity, advocacy, accommodation, community, or social participation.

#### Skills/coping/accommodation language

Skills, coping, communication, emotional literacy, and self-regulation count as lived-experience only when framed as everyday experience, self/family perspective, self-advocacy, or accommodation.

When framed as intervention targets, developmental needs, therapy goals, or programme outcomes, code clinical instead.

Example:

```text
The Incredible Years Autism Parenting Program provides tailored approaches to support communication, emotional literacy, and self-regulation. Parents and teachers collaborate to use visual prompts, pretend play, and structured praise to address developmental needs.
```

Coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = TRUE
lived_experience_frame_present = FALSE
derived_frame = clinical_only
```

Reason: communication and self-regulation are framed as intervention/developmental targets.

---

## 7. Derived Frames

Do not annotate `derived_frame` manually. Derive it from the three annotation fields.

| `substantive_target_discourse` | `clinical_frame_present` | `lived_experience_frame_present` | `derived_frame`                   |
| ------------------------------ | ------------------------ | -------------------------------- | --------------------------------- |
| `FALSE`                        | `NA`                     | `NA`                             | `non_substantive_or_insufficient` |
| `TRUE`                         | `TRUE`                   | `FALSE`                          | `clinical_only`                   |
| `TRUE`                         | `FALSE`                  | `TRUE`                           | `lived_only`                      |
| `TRUE`                         | `TRUE`                   | `TRUE`                           | `mixed`                           |
| `TRUE`                         | `FALSE`                  | `FALSE`                          | `substantive_other`               |

---

## 8. Substantive Other

Use `substantive_other` only when:

```text
substantive_target_discourse = TRUE
clinical_frame_present = FALSE
lived_experience_frame_present = FALSE
```

### Substantive Other example:

```text
14% of the videos were potentially damaging content. And then what was so fascinating to me, because it feels like everyone is saying they have ADHD, is 100% of the videos in their study on ADHD were misleading. I mean, that‚Äôs just what.
```

This category is for coherent target-specific discourse that does not fit either focal frame.



This should be relatively rare.

### Substantive Other example

```text
And then there is the week that has been for the Eagles and the fans and the Eagles Autism Foundation that hammered home once again the connection that makes it all work and that, truly, means everything in the end. "It's been incredible," said Ryan Hammond, the executive director of the Eagles Autism Foundation, who has been busier than busy in the spring and these first few days of summer with the Eagles Autism Challenge in May and then back-to-back events this week ‚Äì an exclusive and ultra-premium two-day golf outing at the legendary Merion Golf Club followed by the Second Annual Eagles Celebrity Bartending Event at the Jersey Shore featuring Jason Kelce, Jordan Mailata, and Kelce's brother, Kansas City All-Pro tight end Travis Kelce. "We had 22 threesomes pay $25,000 each and were joined by a celebrity to play golf and had an unbelievable experience.```

Possible examples include:

* purely metalinguistic discussion of the word “autism” or “ADHD”;
* neutral bibliographic discussion of a title containing the target term;
* target-specific factual context that does not invoke diagnosis, disorder, identity, experience, stigma, support, community, or social meaning.

Do not use `substantive_other` for noisy, thin, generic, navigational, or incidental mentions. Those should be `non_substantive_or_insufficient`.

---

## 9. Mixed Frame

Code `mixed` when both clinical and lived-experience frames are substantively present.

### Mixed example: parenting, treatment, and family experience

```text
When parents first hear that their child has ADHD, many feel as if they've been set adrift on an emotional sea of guilt, isolation, confusion and fear. To help these parents and their children navigate the challenges of home life, school, and ADHD treatment, Tracey Bromley Goodwin and Holly Oberacker have created Navigating ADHD.
```

Coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = TRUE
lived_experience_frame_present = TRUE
derived_frame = mixed
```

Reason: clinical frame appears through ADHD treatment; lived-experience frame appears through parental guilt, isolation, fear, and navigating home/school life.

### Mixed example: diagnostic categories and everyday social functioning

```text
Challenges are commonly experienced by individuals with autism spectrum disorders, social communication disorder, Aspergers, ADHD, non-verbal learning disability and similar diagnoses, including children and adults experiencing social learning difficulties who have not received a diagnosis.
```

Possible coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = TRUE
lived_experience_frame_present = TRUE
derived_frame = mixed
confidence = medium
```

Reason: clinical frame appears through diagnoses and speech-language/social-communication framing. Lived-experience frame may be present through everyday interpersonal and social-learning difficulties. This is a threshold case: code mixed only if the passage provides enough social/everyday experience content beyond diagnostic listing.

---

## 10. Orthogonal Dimensions Not Being Annotated

The following dimensions may matter analytically, but they are not the target of this codebook.

### 10.1 Professional vs Lay Register

Do not code clinical merely because a passage sounds professional, scientific, educational, or medical.

A professional passage can be lived-experience framed:

```text
This module examines autism through neurodiversity, autistic voices, marginalisation, and everyday life.
```

A lay passage can be clinical:

```text
My child was diagnosed with ADHD and prescribed medication for symptoms.
```

The annotation target is the local semantic framing of ADHD/autism, not who is speaking.

### 10.2 Stance or Valence

Do not code lived-experience merely because a passage is sympathetic.

Do not code clinical merely because a passage is negative, derogatory, pathologising, or offensive.

Offensive language should be annotated according to how ADHD/autism is conceptually framed.

Example:

```text
The character displays symptoms of autism and schizophrenia clubbed together.
```

Coding:

```text
substantive_target_discourse = TRUE
clinical_frame_present = TRUE
lived_experience_frame_present = FALSE
derived_frame = clinical_only
```

Reason: offensive tone does not determine the frame; the target is framed as a symptom/disorder category.

### 10.3 Source Genre

Do not infer frame from genre alone.

A passage can come from:

* academic writing;
* journalism;
* charity websites;
* service pages;
* blogs;
* legal reporting;
* school/newsletters;
* promotional material.

Always code what the local passage says about the target.

---

## 11. General Borderline Principles

Use these principles when cases are difficult.

### 11.1 Substantiveness Comes First

If the passage is too generic, thin, list-like, navigational, or noisy to classify the target frame, code:

```text
substantive_target_discourse = FALSE
clinical_frame_present = NA
lived_experience_frame_present = NA
derived_frame = non_substantive_or_insufficient
```

Do this even if the passage contains weak clinical- or lived-adjacent cues.

### 11.2 Clinical Vocabulary Is Not Sufficient

Terms such as `diagnosed`, `disorder`, `condition`, or `symptoms` are cues, not automatic labels.

Code clinical only when the passage substantively frames the target through diagnosis, medical classification, symptoms, impairment, treatment, assessment, services, research, medication, or institutional support.

### 11.3 Generic Support Language Is Not Sufficient

Phrases such as “support people with autism and their families” are not sufficient for lived-experience coding unless the passage gives substantive content about lived experience, family experience, coping, stigma, inclusion, accommodation, identity, advocacy, community, or everyday life.

### 11.4 Neurodiversity Usually Indicates Lived-Experience Framing

When neurodiversity is used to emphasise difference, strengths, identity, destigmatisation, social recognition, inclusion, or autistic/ADHD voices, code lived-experience.

Do not code clinical merely because the same passage mentions diagnostic labels.

### 11.5 Intervention Targets Are Usually Clinical

When communication, emotional literacy, self-regulation, social skills, behaviour, or coping are framed as intervention targets, developmental needs, therapy goals, or programme outcomes, code clinical rather than lived-experience.

### 11.6 Everyday Coping Is Usually Lived-Experience

When practical strategies are framed from the perspective of everyday living, self/family routines, work, school, relationships, or embodied experience, code lived-experience.

### 11.7 Legal/Forensic Diagnosis Use Is Clinical

When diagnosis, medication, or clinical status is used to explain, mitigate, or contextualise criminal/legal behaviour, code clinical.

### 11.8 Humanising Clinical Passages Are Not Automatically Lived-Experience

A clinical or behavioural passage may recognise that autistic/ADHD behaviours are meaningful. This does not automatically make it lived-experience unless the passage foregrounds the person’s own perspective, identity, everyday life, social participation, or self/family experience.

---

## 12. Example Bank

### 12.1 Clinical Only

#### Example A: ASD symptoms and robot intervention

```text
Cabibihan's thoughts turned to using robots as a tool for working with children who do have developmental issues, specifically autism spectrum disorders (ASD). People with ASD often have difficulty communicating, fail to respond to social cues, tend to repeat particular actions and sometimes become preoccupied with certain objects.
```

```text
substantive_target_discourse = TRUE
clinical_frame_present = TRUE
lived_experience_frame_present = FALSE
derived_frame = clinical_only
confidence = high
```

#### Example B: lead exposure and ADHD as symptom/risk cue

```text
A teacher or learning intervention professional who notices behaviour associated with high blood lead levels, such as hearing loss, poor handwriting, poor coordination, learning difficulties, ADD or ADHD, and delinquency.
```

```text
substantive_target_discourse = TRUE
clinical_frame_present = TRUE
lived_experience_frame_present = FALSE
derived_frame = clinical_only
confidence = high
```

#### Example C: behavioural/therapeutic discussion of echolalia

```text
In the past, it was sometimes suggested that this verbal behavior should be extinguished since it appeared non-meaningful. However, many now think that echolalia does serve a purpose for the individual with autism.
```

```text
substantive_target_discourse = TRUE
clinical_frame_present = TRUE
lived_experience_frame_present = FALSE
derived_frame = clinical_only
confidence = medium
```

Reason: this is behavioural/intervention-oriented. Recognising behaviour as meaningful is not by itself lived-experience framing.

---

### 12.2 Lived Only

#### Example A: autism rights

```text
The need for inclusion and accessibility has given rise to the autism rights movement, a grassroots effort by individuals with autism to share their distinct perspectives and advocate for themselves in their own voices.
```

```text
substantive_target_discourse = TRUE
clinical_frame_present = FALSE
lived_experience_frame_present = TRUE
derived_frame = lived_only
confidence = high
```

#### Example B: ADHD body doubling

```text
Haselberger's now-13-year-old son has not been diagnosed with ADHD, but he continues to find body doubling helpful. So does Haselberger, who does have ADHD. She asks her husband to be around while she completes certain tasks.
```

```text
substantive_target_discourse = TRUE
clinical_frame_present = FALSE
lived_experience_frame_present = TRUE
derived_frame = lived_only
confidence = medium
```

#### Example C: neurodiversity and destigmatisation

```text
The concept of neurodiversity is intended to destigmatise people who are neurodifferent and recognise their potential within society.
```

```text
substantive_target_discourse = TRUE
clinical_frame_present = FALSE
lived_experience_frame_present = TRUE
derived_frame = lived_only
confidence = medium
```

---

### 12.3 Mixed

#### Example A: parenting, home life, treatment

```text
When parents first hear that their child has ADHD, many feel guilt, isolation, confusion and fear. To help these parents and their children navigate the challenges of home life, school, and ADHD treatment, the authors created Navigating ADHD.
```

```text
substantive_target_discourse = TRUE
clinical_frame_present = TRUE
lived_experience_frame_present = TRUE
derived_frame = mixed
confidence = medium
```

#### Example B: social learning and diagnosis

```text
Challenges are commonly experienced by individuals with autism spectrum disorders, social communication disorder, Aspergers, ADHD and similar diagnoses, including children and adults experiencing social learning difficulties who have not received a diagnosis.
```

```text
substantive_target_discourse = TRUE
clinical_frame_present = TRUE
lived_experience_frame_present = TRUE
derived_frame = mixed
confidence = medium
```

Reason: clinical diagnostic framing is explicit. Lived-experience framing is plausible if the passage substantively describes everyday social interaction and emotional/social experience.

---

### 12.4 Non-Substantive or Insufficient

#### Example A: navigation

```text
next: Different Types of Educational Assessment Tests ~ back to Parent Advocate homepage ~ adhd library articles ~ all add/adhd articles
```

```text
substantive_target_discourse = FALSE
clinical_frame_present = NA
lived_experience_frame_present = NA
derived_frame = non_substantive_or_insufficient
confidence = high
```

#### Example B: organisation/event name

```text
Eagles Autism Foundation ... Eagles Autism Challenge ... celebrity golf outing ... celebrity bartending event.
```

```text
substantive_target_discourse = FALSE
clinical_frame_present = NA
lived_experience_frame_present = NA
derived_frame = non_substantive_or_insufficient
confidence = high
```

#### Example C: generic provider/resource list

```text
Our articles feature advice from specialists in various fields: autism consultants, psychologists, speech-language pathologists, occupational therapists, teachers...
```

```text
substantive_target_discourse = FALSE
clinical_frame_present = NA
lived_experience_frame_present = NA
derived_frame = non_substantive_or_insufficient
confidence = medium
```

#### Example D: generic resource blurb

```text
The Autism Society has several documents available containing resources to support individuals with Autism and their families.
```

```text
substantive_target_discourse = FALSE
clinical_frame_present = NA
lived_experience_frame_present = NA
derived_frame = non_substantive_or_insufficient
confidence = medium
```

Reason: support/resource language is present, but the passage is too generic to classify the target frame.

#### Example E: garbled extraction

```text
Autism spectrum disorder should be performed. Pentosan toms, with a dose conversion table; patients injecting up to 70%...
```

```text
substantive_target_discourse = FALSE
clinical_frame_present = NA
lived_experience_frame_present = NA
derived_frame = non_substantive_or_insufficient
confidence = high
```

---

## 13. Annotation Procedure

For each passage:

1. Read the target passage only.
2. Ignore URL, domain, publication year, and outside source knowledge.
3. Identify the target mention.
4. Apply Stage 0:

   * Is there enough coherent, target-specific discourse?
   * If no or uncertain, code `substantive_target_discourse = FALSE` and stop.
5. If Stage 0 is TRUE, apply Stage 1A:

   * Is clinical/disorder framing present?
6. Apply Stage 1B:

   * Is lived-experience/identity/social-experience framing present?
7. Derive the final frame automatically.
8. Add confidence (`high`, `medium`, or `low`).

---

## 14. Recommended Annotation Sheet Columns

Human annotation is performed in the protected `annotations` sheet of the generated XLSX workbook. Context and metadata columns are locked and stored explicitly as text; only the four annotation columns are editable. Use the workbook dropdowns rather than typing alternative label forms. Save completed workbooks with the `_completed.xlsx` suffix for ingestion. Do not export or resave the annotation workbook as CSV through Excel.

The annotation table contains:

```text
annotation_id
context_id
analysis_unit
lsc_year
raw_form
target_sentence_plus_adjacent
substantive_target_discourse
clinical_frame_present
lived_experience_frame_present
confidence
annotation_round
codebook_version
```

Optional metadata columns can be retained for later analysis but should not guide annotation:

```text
document_id
year
domain
url
source_year
published_year
```

---

## 15. Downstream Modelling Implications

This codebook is designed to support a hierarchical supervised classification workflow.

### 15.1 Human Annotation

Human annotators produce:

```text
substantive_target_discourse ∈ {TRUE, FALSE}
clinical_frame_present ∈ {TRUE, FALSE, NA}
lived_experience_frame_present ∈ {TRUE, FALSE, NA}
```

Clinical and lived-experience labels are only meaningful where:

```text
substantive_target_discourse = TRUE
```

### 15.2 Supervised Classification

Recommended model design:

```text
shared text encoder
    ├── Head 0: p(substantive_target_discourse = TRUE)
    ├── Head 1: p(clinical_frame_present = TRUE | substantive_target_discourse = TRUE)
    └── Head 2: p(lived_experience_frame_present = TRUE | substantive_target_discourse = TRUE)
```

During training:

* Head 0 is trained on all labelled examples.
* Heads 1 and 2 are trained only on examples where `substantive_target_discourse = TRUE`.
* For `substantive_target_discourse = FALSE`, clinical/lived labels are masked from the loss.

This avoids training the model to treat non-substantive passages as meaningful negative examples for clinical/lived framing.

### 15.3 Predicted Outputs

For each context, store:

```text
p_substantive
p_clinical_given_substantive
p_lived_given_substantive
predicted_substantive_target_discourse
predicted_clinical_frame_present
predicted_lived_experience_frame_present
predicted_derived_frame
```

Optional probability-derived frame weights:

```text
w_non_substantive_or_insufficient = 1 - p_substantive

w_clinical_only = p_substantive
                  × p_clinical_given_substantive
                  × (1 - p_lived_given_substantive)

w_lived_only = p_substantive
               × (1 - p_clinical_given_substantive)
               × p_lived_given_substantive

w_mixed = p_substantive
          × p_clinical_given_substantive
          × p_lived_given_substantive

w_substantive_other = p_substantive
                      × (1 - p_clinical_given_substantive)
                      × (1 - p_lived_given_substantive)
```

These weights are approximate but useful for sensitivity checks and probability-weighted trend estimates.

### 15.4 Use in Semantic-Drift Analysis

Use all validated mentions for broad salience diagnostics if needed.

Use `substantive_target_discourse = TRUE` contexts for frame-sensitive semantic analysis.

Compute LSC measures separately for:

```text
clinical_only
lived_only
mixed
substantive_other
```

The key analytic payoff is to distinguish:

```text
within-frame semantic change
```

from:

```text
between-frame compositional change
```

For example, an aggregate shift in sentiment, intensity, or breadth may reflect a changing mix of clinical and lived-experience discourse rather than semantic change within either frame.

---

## 16. Pilot Revision Log

| date       | codebook_version | change                                                                                                                                                                                                                                                                                                 | reason                                                                                                                                                                                                                                                             |
| ---------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 2026-06-04 | v0.1             | Initial template.                                                                                                                                                                                                                                                                                      | Created before pilot annotation.                                                                                                                                                                                                                                   |
| 2026-06-05 | v0.2             | Restructured into a two-stage annotation workflow with `substantive_target_discourse` as a preliminary sufficiency gate, followed by clinical/lived-experience frame coding only for substantive target discourse. Added `non_substantive_or_insufficient` and `substantive_other` derived categories. | Pilot examples showed that the previous two-axis scheme conflated boilerplate/noisy/generic target mentions with genuine non-clinical/non-lived target discourse, and made clinical/lived labels too sensitive to generic service, support, and resource language. |
