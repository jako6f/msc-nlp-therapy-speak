# Diachronic LSC Analysis Results

This document records the analysis outputs that correspond to `reports/diachronic_lsc_analysis_plan.md`. It is a compact method/results handoff for the dissertation write-up, not a replacement for the notebooks or full provenance files.

## 0. Shared Semantic Contexts

### Method Actually Run

The shared LSC context layer was rebuilt from `data/processed/corpus/corpus_documents.parquet`, using publication year as `lsc_year` and retaining WARC-validated, English, deduplicated contexts in the 2014-2026 window. Mention rows remain capped at three per document per analysis unit.

### Outputs

- `data/interim/lsc/contexts/lsc_mention_contexts.parquet`
- `data/interim/lsc/contexts/lsc_context_extraction_summary.csv`
- `data/interim/lsc/contexts/lsc_context_audit_checks.csv`
- `data/interim/lsc/contexts/lsc_context_audit_flags.csv`

### Results

The rebuilt processed corpus contains 336,178 documents. The shared context table contains 293,670 capped mention contexts: 28,611 ADHD, 68,253 Autism, and 196,806 baseline contexts. The context audit produced no flags.

The publication-date filter is consequential: 465,400 mention contexts existed before the publication-year filter, 10,608 rows lacked parseable publication timestamps, and 161,122 rows were published before the 2014 analysis window. After filtering and capping, 212,651 unique documents and 135,771 unique registered domains contribute contexts.

### Interpretation

The shared context layer is ready for frame-aware downstream analyses. The large number of pre-2014 publication exclusions should remain visible in provenance reporting because the corpus is crawl-year sampled but semantic analyses use publication year.

## 1. Frame Classification — Clinical and Lived-Experience Discourse Strata

### Method Actually Run

The final annotation contract is codebook version `v0.4`, implemented in `notebooks/01_classification/codebooks/codebook_v4.md`. A hierarchical classifier was trained from 3,200 labelled passages: 200 human pilot labels plus 3,000 human-corrected LLM-assisted labels. A separate 200-passage human validation set was held out from training and prompt/codebook calibration.

The classifier uses shared `sentence-transformers/all-mpnet-base-v2` embeddings and three logistic heads: one for substantive target discourse, one for clinical framing conditional on substantive discourse, and one for lived-experience framing conditional on substantive discourse. The clinical and lived heads are trained only on substantive examples.

### Outputs

- `data/processed/lsc/classification/frame_classifier_validation_metrics.csv`
- `data/processed/lsc/classification/lsc_target_context_frame_labels.csv`
- `data/processed/lsc/classification/lsc_frame_counts_by_year_unit.csv`
- `reports/tables/lsc/classification/lsc_classification_validation_metrics.csv`
- `reports/tables/lsc/classification/lsc_classification_frame_composition_overall.csv`
- `reports/tables/lsc/classification/lsc_classification_frame_composition_by_year.csv`
- `reports/figures/lsc/classification/lsc_classification_frame_composition.png`
- `reports/figures/lsc/classification/lsc_classification_frame_composition.pdf`

### Validation Results

On the held-out 200-passage human validation set, the substantive-discourse head reached F1 = 0.861. The clinical-given-substantive head reached F1 = 0.894, and the lived-experience-given-substantive head reached F1 = 0.760. The hard five-way derived-frame macro F1 was lower at 0.443, mainly because rare classes are difficult to recover as hard labels.

### Corpus-Level Frame Results

The classifier was applied to 96,864 ADHD/autism target contexts from the rebuilt shared context table: 28,611 ADHD contexts and 68,253 Autism contexts.

For ADHD, predicted frame composition is 42.1% clinical-only, 12.6% lived-only, 7.0% mixed, 35.8% non-substantive or insufficient, and 2.5% substantive-other. For Autism, predicted frame composition is 29.8% clinical-only, 18.8% lived-only, 9.3% mixed, 38.8% non-substantive or insufficient, and 3.3% substantive-other.

### Interpretation

The classification layer is sufficient for its role as a stratification and composition-control layer. It should not be presented as a standalone modelling contribution. Downstream analyses should focus primarily on clinical-only, lived-only, and mixed contexts where sample size permits. Non-substantive contexts are best treated as a corpus-quality and discourse-composition stratum rather than as a semantic frame. `substantive_other` should remain available in tables but interpreted cautiously because it is sparse.

## 2. Measure 1 — Salience

Pending frame-aware refactor and rerun.

## 3. Measure 2 — Intensity

Pending frame-aware refactor and rerun.

## 4. Measure 3 — Breadth

Pending frame-aware refactor and rerun.

## 5. Measure 4 — Sentiment

Pending frame-aware refactor and rerun.

## 6. Measure 5 — Thematic Evolution

Pending implementation.

## 7. Integrated Synthesis

Pending completion of the individual LSC measures.
