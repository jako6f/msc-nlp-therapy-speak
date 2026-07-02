# Internal CL-Oriented Dissertation Style Guide

Status: internal working guide. Trinity dissertation requirements remain authoritative for the submitted MSc document. These notes translate Computational Linguistics (CL) conventions into practical writing, structure, table, figure, and future-publication guidance for this dissertation.

## Source Hierarchy

Use the following order when requirements conflict:

1. Trinity dissertation template and programme guidance.
2. Project-specific methodological defensibility and reproducibility.
3. Computational Linguistics style and submission conventions.
4. General ACL/NLP expectations for clarity, artefact description, and reproducibility.

Primary CL sources checked:

- CL style files and guidelines: <https://submissions.cljournal.org/index.php/cljournal/StyleGuide>
- CL submissions page: <https://submissions.cljournal.org/index.php/cljournal/about/submissions>
- CLV2025 class manual: <https://submissions.cljournal.org/stylefiles/COLI_manual.pdf>
- CLV2025 template PDF: <https://submissions.cljournal.org/stylefiles/COLI_template.pdf>
- Original submission checklist: <https://submissions.cljournal.org/index.php/cljournal/OriginalSubmissionChecklist>
- Revision/final submission checklist: <https://submissions.cljournal.org/index.php/cljournal/ResubmissionsandFinalAccepts>

## High-Level Implications

- CL is article-oriented; the dissertation is book/chapter-oriented. Do not force the Trinity submission into CL formatting now.
- Use CL as a discipline for prose, evidence, heading hierarchy, captions, citations, and eventual article conversion.
- Avoid dissertation-only scaffolding in the main analytic prose. Phrases such as "placeholder," "leave blank," and "provisional" should disappear from the final submitted dissertation except where explicitly needed.
- Treat the eventual CL article as a compressed derivative: Introduction, Related Work, Data/Materials, Methods, Results, Discussion, Conclusion, and Appendices or supplementary material.

## Writing Style

- Prefer direct, technical prose over tutorial prose. Define concepts once, then use them consistently.
- Keep paragraphs focused on one claim, one methodological decision, or one result.
- Avoid over-explaining implementation details unless they affect validity, corpus membership, measurement, or reproducibility.
- Use signposting sparingly. State what the section does only when it helps the reader follow a complex argument.
- Avoid unsupported evaluative language. Replace "high-quality," "robust," or "large-scale" with concrete criteria, thresholds, or counts where possible.
- Distinguish clearly between empirical claims, methodological assumptions, limitations, and interpretation.
- Use first-person singular only where dissertation voice requires it; otherwise prefer impersonal academic prose.

## Structure And Headings

- For the dissertation, keep the Trinity chapter shell in `main.tex`.
- Within chapters, use numbered `\section{}` and `\subsection{}` for substantive structure.
- Avoid starred headings for content that should appear in the table of contents or carry argumentative weight.
- Do not go deeper than `\subsubsection{}` unless unavoidable.
- Keep heading titles short and descriptive.
- Avoid isolated subsections with only one short paragraph.

Recommended dissertation-to-CL mapping:

| Dissertation chapter | Later CL article section |
|---|---|
| Introduction | Introduction |
| Related Work | Related Work |
| Data | Data |
| Methods | Methods |
| Results | Results |
| Discussion | Discussion |
| Conclusion | Conclusion |

## Citations And References

- The dissertation build now uses natbib-compatible author-year citations with CL's `compling.bst`.
- Use `\citep{}` for parenthetical citations and `\citet{}` for author-in-sentence citations.
- Avoid raw plain-text citations in final LaTeX prose.
- Cite datasets, software, lexicons, and major processing tools when they materially affect measurement or corpus membership.
- Do not cite routine infrastructure packages unless they are methodologically consequential.

## Tables

- Use tables for compact comparisons, parameter sets, term lists, and quantitative summaries that readers may need to inspect.
- Use `booktabs` style: no vertical rules, minimal horizontal rules, aligned numeric columns, compact captions.
- Captions should be self-contained and explain what the table reports, not merely name the object.
- Each table should be introduced in the prose before it appears.
- Avoid overly wide tables in the main text. Move exhaustive term lists, run manifests, or detailed parameter grids to appendices.
- Report units in column headers where possible.

## Figures

- Use figures for process diagrams, conceptual architecture, and result patterns that are easier to compare visually than in tables.
- Keep pipeline figures conceptual. Do not turn diagrams into package inventories or config dumps.
- Captions should identify the figure's purpose and scope.
- Every figure should be referenced and interpreted in the text.
- Prefer simple, legible typography and restrained colour. The figure should remain readable when printed in grayscale.
- Use appendices for extra diagnostics, alternate plots, or validation samples.

## Methods And Materials Reporting

- Report data source, time span, crawl selection rule, sampling design, filtering stages, validation logic, deduplication logic, and final output counts.
- Report methodologically consequential software and package choices when they change corpus membership, extracted text, language filtering, metadata, or measurement.
- Keep routine implementation packages out of the prose unless they matter for reproducibility or validity.
- For Common Crawl, explicitly distinguish WET use from WARC use because this is central to the pipeline design.
- For any final statistical or modelling method, include enough detail to reproduce the analysis without reading the code first.

## Reproducibility And Artefacts

- CL submissions expect source files and style compliance at submission/final-version stage; the project should therefore preserve clean LaTeX source, bibliography, figures, and final PDF outputs.
- For the dissertation, reproducibility emphasis remains on the collection and analysis pipeline, not TeXstudio build mechanics.
- Public-facing methods should point to the repository and describe which outputs are final analysis-ready products.
- Internal generated files, logs, and LaTeX auxiliary files should not be part of the methodological story.

