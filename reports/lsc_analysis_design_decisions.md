# LSC Analysis Design Decisions

This document records consequential analysis-design decisions that should persist across notebooks. It is not a running implementation log; avoid obvious code-level details and record only decisions that affect interpretation, reproducibility, comparability, or downstream analysis.

## Decisions

| date | stage | decision | rationale | applies to | revisit if |
|---|---|---|---|---|---|
| 2026-06-02 | Shared LSC context preprocessing | Keep up to 3 mentions per document per analysis unit in the shared context table. | Reduces document-level dominance while preserving repeated-use signal and keeping downstream notebooks manageable. | Default shared mention table for Sentiment, Intensity, Severity/Intensifier, Breadth, and Thematic planning. | A later analysis requires uncapped mentions for a clearly justified diagnostic or sensitivity check. |
| 2026-06-02 | Shared LSC context preprocessing | Resolve overlapping raw-form matches within an analysis unit by keeping the longest span. | Prevents nested expressions such as `autism spectrum` from being counted both as `autism_spectrum` and `autism` at the same offset, while preserving raw-form diagnostics. | Mention-level shared context table and all downstream unit-year counts derived from it. | A later raw-form analysis needs intentionally nested target counts. |
| 2026-06-02 | Shared LSC context preprocessing | Collapse same-sentence acronym/expansion pairs within an analysis unit when they occur within a short local window. | Prevents constructions such as `attention deficit ... (ADHD)` and `autism spectrum ... (ASD)` from contributing two semantic contexts for one conceptual mention, while retaining `collapsed_raw_forms`, `collapsed_matched_texts`, and `collapsed_match_count` diagnostics. | Shared context table, raw-form diagnostics, manual samples, and all downstream semantic analyses using context rows. | A downstream analysis needs to treat acronym and expansion mentions as separate usage events. |
| 2026-06-02 | Shared LSC context preprocessing | Use `published_ts` as the primary semantic time source and define `lsc_year = published_year`; exclude rows with missing/unparseable publication dates or publication years outside 2014-2026 from the main shared context table. | Semantic-change analyses should date usage by document publication rather than crawl/capture year. The missing-date share is small enough to exclude, while pre-window pages crawled later would otherwise introduce survivorship-biased historical contexts. | Sentiment, Intensity, Severity/Intensifier, Breadth, Thematic analyses, and their shared unit-year diagnostics. | A later sensitivity analysis shows that excluded missing-date or pre-window rows materially change substantive conclusions. |

## What To Record

- Context-unit definitions and changes.
- Sampling caps, exclusion thresholds, or pooling rules.
- Aggregation levels for main analysis versus diagnostics.
- Criteria for creating a composite baseline.
- Model, lexicon, or resource choices with methodological consequences.
- Deviations from Baes et al. or from `reports/diachronic_lsc_analysis_plan.md`.

## What Not To Record

- Routine file naming.
- Ordinary plotting choices.
- Temporary debugging steps.
- Obvious notebook mechanics.
