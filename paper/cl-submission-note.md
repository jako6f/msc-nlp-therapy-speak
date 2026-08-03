# CL submission — portal ancillaries

Paste-ready material for the *Computational Linguistics* OJS portal
(<https://submissions.cljournal.org>), matching the journal's Original Submission
Checklist. Upload `paper/cl-short-paper.pdf`.

---

## 1. Section / article type

**Papers** → Short Paper.

The manuscript sets `\dochead{Short Paper}` and runs to **20 journal pages of main
content**, within the 15–25 page band for short papers. Acknowledgments, appendices, and
references are excluded from that count per the guidelines; the full PDF is 28 pages.

---

## 2. Contributors section (under Publications)

> Author names (first & last, no initials), affiliations and email address.

| Field | Value |
|---|---|
| First name | Jakob |
| Last name | Lütkemeier |
| Affiliation | Trinity College Dublin, School of Social Sciences and Philosophy |
| Email | jakob.luetkemeier@gmail.com |
| Country | Ireland |
| Role | Sole author; contact author |

Author name and affiliation appear on the first page of the manuscript, as the single-blind
review process requires.

---

## 3. Also required as a separate text file

> The title, authors' names, and abstract should also be provided separately in a text
> file, with the contact author's email address clearly indicated.

```text
Title:   Is Lexical Semantic Change Measurement-Invariant? Lexicon, Encoder, and
         Discourse Composition in a Web-Scale Study of ADHD and Autism

Author:  Jakob Lütkemeier
         Trinity College Dublin, School of Social Sciences and Philosophy
         Contact author — jakob.luetkemeier@gmail.com

Abstract:
Diachronic measures of lexical semantic change are increasingly used to license claims
about cultural change, but those claims inherit whatever the instrument does. We ask
whether two such threats—dependence on the resources chosen to operationalise a
dimension, and confounding with change in discourse composition—are large enough to
alter published conclusions. Building a 13-year corpus of general web discourse from
Common Crawl and stratifying 96,864 target mentions by discourse frame with an
LLM-in-the-loop classifier, we estimate annual sentiment, intensity, and breadth
trajectories for two diagnostic concepts, ADHD and autism, against three
negative-emotion comparators, and then re-estimate every trajectory under the alternative
lexicon and encoder that the same framework originally specified. Fifteen of twenty-seven
series change their descriptive conclusion, and the failures are dimension-specific:
sentiment is close to invariant while intensity and breadth are not. Crossing the two
lexicons shows why—they agree about valence and disagree about arousal on the same
tokens—and a perturbation probe shows the two encoders differ by an order of magnitude in
how much non-target material moves them. Substantively, neither predicted form of concept
creep is supported, and a shift-share decomposition shows the null is conservative: the
corpus composition was drifting in the direction that manufactures apparent creep. We argue
that multi-resource comparison and composition stratification belong in the primary
analysis of semantic change, not in a robustness appendix.
```

---

## 4. Comments to the Editor

Paste the block below verbatim into the **"Comments for the Editor"** field.

```text
KEY WORDS

lexical semantic change; diachronic NLP; measurement validity; discourse composition;
contextual embeddings; Common Crawl; corpus construction; LLM-assisted annotation;
concept creep

DECLARATIONS

1. This submission has not been published elsewhere, in or submitted for publication to
   another refereed archival publication.

2. This submission has not appeared in any conference or workshop proceedings.

3. This submission is not covered by a non-disclosure agreement and is available for peer
   review without restriction.

ADDITIONAL NOTES

Code, configuration, and the derived annual estimates behind every figure and table are
publicly available at https://github.com/jako6f/msc-nlp-therapy-speak. The repository
includes the corpus-collection pipeline, the frame codebook and annotation prompts, and the
scripts that regenerate each figure and table in the paper from the tracked outputs.

Generative language models are used in this work in two distinct capacities, both of which
I would like to state explicitly. First, methodologically: the frame-annotation workflow
described in Section 4.1 uses a large language model as an annotator and a second model as
a critic, with human adjudication authoritative throughout and no model suggestion altering
a label without human review. This is a documented instrument of the study, and its
reliability against human judgement is reported in Table 1. Second, as development support:
AI coding assistants were used while implementing parts of the collection pipeline. All
code was specified, reviewed, and tested by the author, and I take full responsibility for
the content and correctness of the paper.

The empirical work derives from my MSc dissertation at Trinity College Dublin. The
dissertation is not publicly deposited and no part of it, or of this paper, has been
published or submitted anywhere; the declarations above therefore hold as written. The
paper has been substantially rewritten for this submission around a different central
question — the measurement invariance of the analysis, rather than the substantive result —
and it reports analyses that the dissertation did not contain, notably the conclusion
concordance across operationalisations (Table 4), the decomposition of the lexicon
disagreement into coverage and rating components (Table 5), the encoder perturbation probe
(Table 6), and the shift-share decomposition of composition against within-frame change.
```

---

## 5. Format and style compliance

| Requirement | Status |
|---|---|
| PDF, single-spaced, CL style | Yes — `clv2025.cls`, `\documentclass[final]` |
| `clv2025.cls` (2025-01-01) | Yes, unmodified, copied to `paper/clv2025.cls` |
| `compling.bst` citations and references | Yes — `\bibliographystyle{compling}` |
| Abstract 150–250 words | 221 words |
| Author name/affiliation on first page | Yes (single-blind) |
| Main content 15–25 pages | 20 pages |
| Compiles clean | Zero errors, zero LaTeX warnings, zero pdfTeX warnings, zero overfull boxes |

Build with `latexmk -pdf cl-short-paper.tex` from `paper/`.

---

## 6. On acceptance

- `\pageonefooter` currently holds the template placeholder; the editorial office supplies
  the action editor name and the submission/revision/acceptance dates.
- `\jvol`, `\jnum`, `\jyear` are placeholders and are set by the journal.
- A Copyright Transfer Agreement signed by the author will be required.
