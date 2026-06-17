# ADHD/Autism Discourse Frame Codebook Revision Log

This file records changes to the annotation codebook. Increment `codebook_version`
when a change could affect an annotation decision. Preserve the version recorded
with completed annotations; do not retrospectively relabel them without a
documented adjudication exercise.

| date | codebook_version | change | reason |
|---|---|---|---|
| 2026-06-04 | v0.1 | Created the initial two-axis pilot template. | Established preliminary clinical and lived-experience frame definitions before pilot annotation. |
| 2026-06-05 | v0.2 | Introduced a hierarchical workflow: a preliminary `substantive_target_discourse` gate followed by conditional clinical and lived-experience coding. Added `non_substantive_or_insufficient` and `substantive_other`. | Early pilot cases showed that the two-axis scheme treated noisy, navigational, and generic mentions as meaningful negative examples for both frames. |
| 2026-06-09 | v0.3 | Consolidated the completed pilot's refinements into a coder-facing final codebook. Clarified the evidence boundary, decision order, service/list distinction, diagnosis-as-status rule, first-person and family discourse, media representation, mixed-frame threshold, `substantive_other`, and the three-level confidence scale. Removed contradictory and duplicate examples. | Continuous pilot refinement produced useful detail but also minor inconsistencies and redundancies. Future human and LLM coders require one explicit, precedence-based decision contract. |
| 2026-06-10 | v0.4 | Added final adjudication-derived nuances for category-set claims, concrete institutional provision, self-theorising symptom language, and legal/forensic mitigation. | Pilot disagreement review showed that generic-list exclusions were sometimes applied too broadly and that several discourse functions required explicit precedence rules. |

## v0.3 Pilot-Derived Clarifications

- Made a coherent target-specific proposition the explicit Stage 0 threshold.
- Classified target-named organisations, events, titles, navigation, and generic
  provider/resource lists as non-substantive unless they make a target-specific
  claim.
- Distinguished explicit target assessment/treatment services from generic
  service or condition lists.
- Clarified that diagnosis status, personal pronouns, family language,
  professional identity, and compassionate clinical language do not trigger a
  frame by themselves.
- Required clinical and lived-experience evidence to qualify independently
  before assigning `mixed`; the labels record presence rather than dominance.
- Restricted `substantive_other` to coherent target-specific propositions that
  meet neither frame definition.
- Standardised confidence to `high`, `medium`, or `low`.
- Replaced contradictory or weak precedents with compact canonical examples and
  contrast pairs.

## v0.4 Final Adjudication Clarifications

- A target occurring in a list is not automatically non-substantive. A coherent
  class-level claim is substantive when it clearly applies to the target.
- Concrete institutional provision organised for ADHD/autistic people is
  substantive clinical/institutional-support discourse.
- Self-theorising symptom language used to interpret everyday experience is
  lived experience unless a separate formal clinical object is present.
- Legal, disciplinary, or forensic mitigation that uses ADHD/autism as an
  explanatory clinical object is substantive clinical discourse.

Use v0.4 for subsequent human validation, Codex annotation, Claude criticism,
and classifier metadata. The original pilot workbook was explicitly
re-adjudicated where documented in the pilot disagreement audit.
