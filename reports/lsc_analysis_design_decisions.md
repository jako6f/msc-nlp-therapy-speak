# LSC Analysis Design Decisions

This document records consequential analysis-design decisions that should persist across notebooks. It is not a running implementation log; avoid obvious code-level details and record only decisions that affect interpretation, reproducibility, comparability, or downstream analysis.

## Decisions

| date | stage | decision | rationale | applies to | revisit if |
|---|---|---|---|---|---|
| 2026-06-02 | Shared LSC context preprocessing | Keep up to 3 mentions per document per analysis unit in the shared context table. | Reduces document-level dominance while preserving repeated-use signal and keeping downstream notebooks manageable. | Default shared mention table for Sentiment, Intensity, Severity/Intensifier, Breadth, and Thematic planning. | A later analysis requires uncapped mentions for a clearly justified diagnostic or sensitivity check. |
| 2026-06-02 | Shared LSC context preprocessing | Resolve overlapping raw-form matches within an analysis unit by keeping the longest span. | Prevents nested expressions such as `autism spectrum` from being counted both as `autism_spectrum` and `autism` at the same offset, while preserving raw-form diagnostics. | Mention-level shared context table and all downstream unit-year counts derived from it. | A later raw-form analysis needs intentionally nested target counts. |
| 2026-06-02 | Shared LSC context preprocessing | Collapse same-sentence acronym/expansion pairs within an analysis unit when they occur within a short local window. | Prevents constructions such as `attention deficit ... (ADHD)` and `autism spectrum ... (ASD)` from contributing two semantic contexts for one conceptual mention, while retaining `collapsed_raw_forms`, `collapsed_matched_texts`, and `collapsed_match_count` diagnostics. | Shared context table, raw-form diagnostics, manual samples, and all downstream semantic analyses using context rows. | A downstream analysis needs to treat acronym and expansion mentions as separate usage events. |
| 2026-06-02 | Shared LSC context preprocessing | Use `published_ts` as the primary semantic time source and define `lsc_year = published_year`; exclude rows with missing/unparseable publication dates or publication years outside 2014-2026 from the main shared context table. | Semantic-change analyses should date usage by document publication rather than crawl/capture year. The missing-date share is small enough to exclude, while pre-window pages crawled later would otherwise introduce survivorship-biased historical contexts. | Sentiment, Intensity, Severity/Intensifier, Breadth, Thematic analyses, and their shared unit-year diagnostics. | A later sensitivity analysis shows that excluded missing-date or pre-window rows materially change substantive conclusions. |
| 2026-06-02 | Sentiment | Lemmatise local collocate windows with spaCy `en_core_web_sm` before NRC-VAD matching. | Baes' sentiment method uses a lemmatised corpus; the small spaCy model is sufficient for short target windows and avoids a heavier dependency than needed. | Sentiment and the reusable VAD collocate handoff for Intensity. | Lemma errors or coverage diagnostics show systematic distortion for key target forms or baselines. |
| 2026-06-02 | Sentiment | Exclude only the focal mention's own target or baseline lexical material from its local VAD collocate window. | Cross-unit affective words are substantively meaningful collocates; for example, `frustration` should remain eligible around ADHD or Autism mentions even though it is also a baseline term. | Sentiment and the reusable VAD collocate handoff for Intensity. | Cross-unit terms visibly dominate trajectories and require a documented sensitivity analysis. |
| 2026-06-02 | Sentiment | Keep stopwords in the main NRC-VAD collocate index after removing target forms, punctuation, numerals, and one-character tokens. | Baes' sentiment/arousal collocate scripts do not show explicit stopword removal, so this maximises comparability. | Main Sentiment index and reusable VAD collocate table. | Function-word matches visibly dominate trajectories; then add a stopword-excluded sensitivity pass. |
| 2026-06-02 | Sentiment | Match NRC-VAD v2.1 greedily by multi-word expressions first, then unmatched unigrams. | NRC-VAD v2.1 includes rated multi-word expressions; matching them before unigrams uses the available resource without changing the local-window design. | Sentiment and Intensity VAD preprocessing. | MWE matches prove sparse or produce implausible high-impact contributors. |
| 2026-06-02 | Sentiment | Estimate annual uncertainty with document-level bootstrap confidence intervals. | Collocate observations are clustered by document; bootstrapping documents is more defensible than treating collocates as independent. | Annual Sentiment estimates and plots. | Runtime becomes prohibitive or a later modelling stage adopts a different uncertainty framework. |
| 2026-06-03 | Intensity | Compute the primary arousal index from the same reusable NRC-VAD collocate handoff used for Sentiment. | Arousal and valence are parallel VAD dimensions; freezing the collocate table prevents preprocessing drift between the two affective measures. | Primary Intensity arousal notebook and later integrated VAD comparisons. | The Sentiment handoff is rebuilt with changed tokenisation, exclusion, or matching rules. |
| 2026-06-03 | Breadth | Compute semantic breadth from local XL-LEXEME target-token embeddings with a configurable cap of 250 contexts per analysis-unit year for the current run. | XL-LEXEME is target-aware, so pooling the marked target tokens better matches the model objective than generic sentence pooling. The cap makes the current run tractable while preserving the same notebook path for larger or uncapped future data. | Primary Breadth notebook, saved embeddings, and Breadth diagnostics. | The local XL-LEXEME resource changes or future compute makes an uncapped run practical. |

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
