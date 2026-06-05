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

Caveats:
- Disability-rights/accessibility discourse should not automatically count as clinical unless tied to support services, diagnosis, impairment, or institutional assessment.

Mark `clinical_frame_present = FALSE` when the target is mentioned without such framing, or when the passage is too noisy or non-substantive to establish the frame.

## Axis 2: Lived-Experience Frame Present

Mark `lived_experience_frame_present = TRUE` when the passage frames ADHD/autism through identity, self-understanding, first-person or family experience, neurodivergent community, masking, stigma, accommodations, everyday coping, belonging, pride, or embodied/social experience.

Typical positive cues:

- first-person or family experience: "I have ADHD", "my autistic child", "as an autistic adult";
- identity/community language: autistic identity, neurodivergent, community, belonging, pride;
- lived practical experience: masking, sensory experience, executive-function struggles, accommodations, stigma, self-advocacy;
- self-understanding or meaning-making around diagnosis or traits.

Caveats:
- Skills/coping/accommodation language counts as lived-experience only when framed as everyday experience, self-advocacy, or accommodation from the autistic person/family perspective; when framed as intervention targets or developmental needs, code clinical/support instead.

Mark `lived_experience_frame_present = FALSE` when the passage only treats ADHD/autism as an abstract category, clinical construct, research object, product/service category, or unclear mention.

## Derived Frames

Do not annotate this manually. It is derived from the two axes.

| clinical_frame_present | lived_experience_frame_present | derived_frame |
|---|---|---|
| TRUE | FALSE | `clinical_only` |
| FALSE | TRUE | `lived_only` |
| TRUE | TRUE | `mixed` |
| FALSE | FALSE | `other_non_substantive` |


## Boilerplate, Lists, and Noisy Text

If a passage is mostly navigation, keyword stuffing, spam, product lists, unrelated text, or extraction noise, mark both axes `FALSE` unless the frame is still clearly recoverable.

Here a few more hints:


Use `uncertainty_note` to explain borderline cases briefly.

## Borderline Examples

Add examples during the pilot.

### Clinical Only

- Example: "Profits from weekly sales are reinvested in Lees Corner students. Some examples of how profits are reinvested include the purchase of adaptive physical education equipment and a sensory table for an autism classroom. Chorus Fifth and sixth grade students have the opportunity to participate in the Lees Corner Chorus."
- Reason: "autism classroom", adaptive PE equipment, and sensory table frame autism through institutional/educational support provision. No identity, self-understanding, or lived-experience framing is visible. Borderline because the passage is school-news/list-like rather than substantive discourse, but the educational-support frame is still recoverable.

- Example: "The police officers who were involved said it was the most frightening incident of their careers with 'ferocious, prolonged and determined violence' directed towards them with unprecedented hostility. Detective superintendent James Riccio on the "harrowing" experience of the officer inside the van Nicholas Lewin, defending, said Roberts had recently been diagnosed with ADHD, had been prescribed medication and was not a ‚Äúsophisticated criminal‚Äù. ‚ÄúMr Roberts is not a rampant fire starter, twisted or other,‚Äù he said."
- Reason: ADHD is invoked through recent diagnosis and prescribed medication, apparently as a legal/mitigating explanation for behaviour. There is no first-person, coping, identity, family, or everyday-experience framing.

#### Boarderline rules:
- profession/resource lists mentioning “autism consultants” should not automatically trigger clinical_frame_present unless the passage gives substantive autism-specific assessment, intervention, support, symptoms, or service context.
- diagnostic terms such as “diagnosed” should trigger clinical_frame_present only when diagnosis is substantively framed as assessment, medical classification, treatment, symptoms, impairment, services, or institutional need. If diagnosis functions merely as biographical/status context within everyday coping or self/family experience, code lived-experience rather than clinical.
- In legal/forensic contexts, code clinical when diagnosis or medication is used as an explanatory or mitigating category.

### Lived Only

- Example: "While progress has been made over the past several years in improving inclusion and representation, autism still continues to be heavily misunderstood and stigmatized, creating barriers to inclusion both in employment and in the educational system. The need for inclusion and accessibility has given rise to the autism rights movement, a grassroots effort by individuals with autism to share their distinct perspectives and advocate for themselves in their own voices. Unlike the broader disability rights movement, the autism rights movement is relatively young, dating back to the early 1990s and the emergence of the internet and online communities."
- Reason: Autism is framed through stigma, inclusion, accessibility, rights, self-advocacy, online communities, and individuals sharing their own perspectives. No diagnosis, symptoms, treatment, assessment, or clinical/service framing is present.

- Example: "Haselberger‚Äôs now-13-year-old son has not been diagnosed with ADHD, but he continues to find body doubling helpful. So does Haselberger, who does have ADHD. She asks her husband to be around while she completes certain tasks, and she organizes regular video calls with work peers to make progress on ‚Äúimportant but not urgent‚Äù goals."
- Reason: ADHD diagnosis is mentioned only as background status. The substantive frame concerns everyday coping, task management, family/work routines, and practical experience with body doubling. Diagnosis functions merely as biographical/status context within everyday coping or self/family experience

- Example: "Neurodiversity is the positive recognition of the fact that human beings are neurologically diverse. This variability can manifest itself in various ways in those affected by an autism spectrum disorder, attention deficit and hyperactivity disorder (ADHD), and disorders such as dyslexia and dyspraxia. The concept of neurodiversity is intended to destigmatize people who are neurodifferent and recognize their potential within society."
- Reason: The dominant frame is neurodiversity, destigmatisation, social recognition, and valuing neurodifferent people’s potential. Although “autism spectrum disorder”, “ADHD”, and “disorders” are clinical terms, they function here as background labels within an explicitly anti-stigma/neurodiversity frame, not as diagnosis, treatment, impairment, or symptom discourse.
codebook issue: Clinical terminology alone should not trigger clinical_frame_present when it is subordinated to a neurodiversity, destigmatisation, or social-recognition frame.

#### Boarderline rules:
- If diagnosis functions merely as biographical/status context within everyday coping or self/family experience, code lived-experience rather than clinical
- Clinical terminology alone should not trigger clinical_frame_present when it is subordinated to a neurodiversity, destigmatisation, or social-recognition frame.

### Mixed

- Example: "He's here to discuss his new book Stranger to My Self: Inside Depersonalization, the Hidden Epidemic on this edition of the HealthyPlace Mental Health Radio Show. Other Recent HealthyPlace Radio Shows - Parenting an ADHD Child the Right Way: When parents first hear that their child has ADHD, many feel as if they've been set adrift on an emotional sea of guilt, isolation, confusion and fear. To help these parents and their children navigate the challenges of home life, school, and ADHD treatment, Tracey Bromley Goodwin and Holly Oberacker have created Navigating ADHD: Your Guide to the Flip Side of ADHD."
- Reason: Clinical frame is present through “child has ADHD” and “ADHD treatment”. Lived-experience frame is also present through parental guilt, isolation, confusion, fear, and navigating home/school challenges.

#### Boarderline rules:

### Other or Non-Substantive

- Example: "When you can come into the meeting with your priorities written down in a businesslike manner, then you'll begin to feel in control and will know you are a driving force at that meeting. next: Different Types of Educational Assessment Tests ~ back to Parent Advocate homepage ~ adhd library articles ~ all add/adhd articles APA Reference Staff, H. (2007, June 8)."
- Reason: Target appears in site navigation adjacent to substantive but non-target-specific prose.

- Example: "Inspirations provides uplifting success stories and timely advice in the area of special needs, brought to you by experts in various fields. Our articles feature timely advice from specialist in various fields (autism consultants, psychologists, speech-language pathologists, occupational therapists, teachers, etc) and resources that are often difficult to find. A unique tool for parents, caregivers, teachers, professionals and people with special needs, Inspirations shares success stores with the intent to inspire, while serving as a vehicle for educating the public-at-large and break down barriers."
- Reason: Autism appears only inside a generic service/provider list: “autism consultants, psychologists, speech-language pathologists…”. The passage is about the publication/resource brand, not substantively about autism as a clinical construct or lived experience.

#### Boarderline rules:
- headline-only or link-list mentions should be coded both FALSE unless the headline itself clearly encodes a frame.
- long scraped index/notes/list passages should be coded other_non_substantive unless the target-bearing item itself contains a clear frame.


## Pilot Revision Log

Record changes to the codebook here. Increment `codebook_version` when a rule changes in a way that could affect labels.

| date | codebook_version | change | reason |
|---|---|---|---|
| 2026-06-04 | v0.1 | Initial template. | Created before pilot annotation. |
