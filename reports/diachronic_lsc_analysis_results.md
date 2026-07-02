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
- `reports/tables/lsc/classification/lsc_classification_validation_performance.tex`
- `reports/tables/lsc/classification/lsc_classification_frame_composition_overall.csv`
- `reports/tables/lsc/classification/lsc_classification_frame_composition_by_year.csv`
- `reports/figures/lsc/classification/lsc_classification_frame_composition.png`
- `reports/figures/lsc/classification/lsc_classification_frame_composition.pdf`

### Validation Results

On the held-out 200-passage human validation set, the substantive-discourse head reached precision = 0.887, recall = 0.837, F1 = 0.861, accuracy = 0.810, and balanced accuracy = 0.791. The two frame heads are evaluated only among the 141 passages marked as substantive target discourse in the human validation labels, because clinical and lived-experience labels are structurally undefined for non-substantive passages. Under that conditional evaluation, the clinical-framing head reached precision = 0.903, recall = 0.886, F1 = 0.894, accuracy = 0.844, and balanced accuracy = 0.804. The lived-experience head reached precision = 0.704, recall = 0.826, F1 = 0.760, accuracy = 0.830, and balanced accuracy = 0.829.

The final derived-frame labels remain saved for composition summaries and audit checks, but sparse five-way derived-frame performance is not used as the headline validation result. This matches the hierarchical classifier design and avoids treating rare derived combinations such as `mixed` and `substantive_other` as the main performance target.

### Corpus-Level Frame Results

The classifier was applied to 96,864 ADHD/autism target contexts from the rebuilt shared context table: 28,611 ADHD contexts and 68,253 Autism contexts.

For ADHD, predicted frame composition is 42.1% clinical-only, 12.6% lived-only, 7.0% mixed, 35.8% non-substantive or insufficient, and 2.5% substantive-other. For Autism, predicted frame composition is 29.8% clinical-only, 18.8% lived-only, 9.3% mixed, 38.8% non-substantive or insufficient, and 3.3% substantive-other.

### Interpretation

The classification layer is sufficient for its role as a stratification and composition-control layer. It should not be presented as a standalone modelling contribution. Downstream analyses should focus primarily on clinical-only, lived-only, and mixed contexts where sample size permits. Non-substantive contexts are best treated as a corpus-quality and discourse-composition stratum rather than as a semantic frame. `substantive_other` should remain available in tables but interpreted cautiously because it is sparse.

## 2. Measure 1 — Salience

### Method Actually Run

The Salience audit remains source-year based, using WARC-validated hits per million minimum-length WET tokens as the primary series. This differs from the semantic LSC measures, which use publication year. Trend models are compact OLS regressions of the annual primary salience index on centred year, with the same residual-autocorrelation diagnostic used in the other scalar notebooks. The report-facing Salience figure is now a two-panel view: absolute ADHD/autism source-year prominence plus a 2014-indexed target/comparator comparison.

### Outputs

- `data/processed/lsc/salience/salience_unit_year.csv`
- `data/processed/lsc/salience/salience_trend_summary.csv`
- `data/processed/lsc/salience/salience_trend_models.csv`
- `data/processed/lsc/salience/salience_audit_checks.csv`
- `data/processed/lsc/salience/salience_denominator_audit.csv`
- `data/processed/lsc/salience/salience_publication_year_status.csv`
- `data/processed/lsc/salience/salience_raw_form_year.csv`
- `reports/figures/lsc/salience/lsc_salience_primary_trajectories.png`
- `reports/figures/lsc/salience/lsc_salience_primary_trajectories.pdf`

### Results

The audit table contains seven checks: six pass and one optional domain-concentration diagnostic is marked not available in the processed trend handoff. Publication-year and raw-form composition remain table diagnostics rather than report figures.

In the source-year trend summaries, ADHD salience has a positive but non-significant slope of 0.0073 hits per million tokens per year (p = 0.118). Autism salience has a negative slope of -0.0228 hits per million tokens per year (p = 0.024). These are source-year prominence trends, not publication-year semantic estimates.

## 3. Measure 2 — Intensity

### Method Actually Run

The Intensity notebook reuses the NRC-VAD collocate handoff produced for Sentiment and reports annual arousal means. ADHD and Autism are reported for `substantive_core_overall`, `clinical_only`, `lived_only`, and `mixed`; baseline terms remain separate and unframed. The notebook writes document-bootstrap intervals, compact Baes-style trend models, coverage diagnostics, audit flags, and one report-facing three-panel trajectory figure. The figure shows annual lines, bootstrap intervals, and dashed OLS summaries for every plotted series while preserving visual hierarchy through condition-specific hue families.

### Outputs

- `data/processed/lsc/intensity/lsc_intensity_annual_arousal.csv`
- `data/processed/lsc/intensity/lsc_intensity_coverage.csv`
- `data/processed/lsc/intensity/lsc_intensity_top_collocates.csv`
- `data/processed/lsc/intensity/lsc_intensity_trend_models.csv`
- `data/processed/lsc/intensity/lsc_intensity_audit_flags.csv`
- `reports/figures/lsc/intensity/lsc_intensity_arousal_trajectories.png`
- `reports/figures/lsc/intensity/lsc_intensity_arousal_trajectories.pdf`

### Results

The annual arousal table contains the expected 143 rows: 13 years for ADHD and Autism across four target strata, plus 13 years for each of the three unframed baselines. The trend-model table contains 11 series-level models.

For the substantive-core aggregate, ADHD arousal has a positive slope of 0.0010 per year (p = 0.020), while Autism arousal has a positive slope of 0.0011 per year (p = 0.067). Neither aggregate target trend is flagged for residual autocorrelation.

## 4. Measure 3 — Breadth

### Method Actually Run

The Breadth notebook estimates annual semantic breadth from XL-LEXEME target-token embeddings, using mean pairwise cosine distance within each analysis-unit/year/frame cell. ADHD and Autism are reported for `substantive_core_overall`, `clinical_only`, `lived_only`, and `mixed`; baseline terms remain separate and unframed. The executed sampling policy uses all markable ADHD/autism target contexts and caps only baseline terms at 1,000 contexts per baseline-year using deterministic domain-stratified sampling. The report-facing output is one compact three-panel trajectory view that embeds clinical/lived frame traces under the dominant Overall target trajectories. Mixed-frame and sampling diagnostics remain in tables.

### Outputs

- `data/interim/lsc/breadth/lsc_breadth_sampled_contexts.parquet`
- `data/interim/lsc/breadth/lsc_breadth_embeddings.npy`
- `data/interim/lsc/breadth/lsc_breadth_embeddings_normalised.npy`
- `data/interim/lsc/breadth/lsc_breadth_embedding_index.csv`
- `data/processed/lsc/breadth/lsc_breadth_annual_scores.csv`
- `data/processed/lsc/breadth/lsc_breadth_sampling_diagnostics.csv`
- `data/processed/lsc/breadth/lsc_breadth_raw_form_diagnostics.csv`
- `data/processed/lsc/breadth/lsc_breadth_trend_models.csv`
- `data/processed/lsc/breadth/lsc_breadth_audit_flags.csv`
- `reports/figures/lsc/breadth/lsc_breadth_trajectories.png`
- `reports/figures/lsc/breadth/lsc_breadth_trajectories.pdf`

### Results

The executed run embedded 145,430 marked contexts: 107,081 uncapped target-frame contexts and 38,349 capped baseline contexts. The annual breadth table contains the expected 143 rows: 13 years for ADHD and Autism across four target strata, plus 13 years for each of the three unframed baselines. No target-token marking failures were recorded. One small-cell flag was raised for ADHD mixed-frame contexts in 2026, where only 42 sampled contexts from 38 documents were available.

For the substantive-core aggregate, ADHD breadth has a negative slope of -0.0020 mean pairwise cosine distance per year (p < 0.001; adjusted R2 = 0.775), moving from 0.1384 in 2014 to 0.1168 in 2026. Autism substantive-core breadth is approximately flat in the linear model, with a slope of 0.0001 per year (p = 0.637), moving from 0.0960 in 2014 to 0.0977 in 2026; this aggregate is flagged for residual autocorrelation and shows nonlinear sensitivity, so it should not be interpreted as a simple monotonic increase.

Within ADHD, the decline is clearest in clinical-only contexts (-0.0018 per year, p < 0.001) and is directionally negative but weaker in mixed contexts (-0.0014 per year, p = 0.057). ADHD lived-only breadth is essentially flat and flagged for residual autocorrelation. Within Autism, lived-only contexts decline (-0.0010 per year, p = 0.030), clinical-only contexts have a positive linear slope but are autocorrelation- and curvature-sensitive, and mixed contexts are not linear-trending.

### Interpretation

The primary XL-LEXEME Breadth results support a narrowing of ADHD substantive-core semantic usage across the 2014-2026 publication-year window, especially in clinical-only discourse. Autism does not show the same aggregate narrowing in the main target-aware embedding analysis; its frame-specific patterns are more heterogeneous and require caution because the clinical-only and aggregate series are flagged for residual autocorrelation/nonlinearity. The encoder robustness check in Section 6 qualifies how strongly this finding should be framed.

## 5. Measure 4 — Sentiment

### Method Actually Run

The Sentiment notebook computes annual NRC-VAD valence means from lemmatised target-window collocates. ADHD and Autism are reported for `substantive_core_overall`, `clinical_only`, `lived_only`, and `mixed`; baseline terms remain separate and unframed. The notebook writes document-bootstrap intervals, frame/context diagnostics, coverage outputs, top collocates, compact trend models, audit flags, and one report-facing three-panel trajectory figure. The figure shows annual lines, bootstrap intervals, and dashed OLS summaries for every plotted series while preserving visual hierarchy through condition-specific hue families.

### Outputs

- `data/processed/lsc/sentiment/lsc_sentiment_annual_valence.csv`
- `data/processed/lsc/sentiment/lsc_sentiment_coverage.csv`
- `data/processed/lsc/sentiment/lsc_sentiment_top_collocates.csv`
- `data/processed/lsc/sentiment/lsc_sentiment_frame_context_diagnostics.csv`
- `data/processed/lsc/sentiment/lsc_sentiment_trend_models.csv`
- `data/processed/lsc/sentiment/lsc_sentiment_audit_flags.csv`
- `reports/figures/lsc/sentiment/lsc_sentiment_valence_trajectories.png`
- `reports/figures/lsc/sentiment/lsc_sentiment_valence_trajectories.pdf`

### Results

The annual valence table contains the expected 143 rows: 13 years for ADHD and Autism across four target strata, plus 13 years for each of the three unframed baselines. The trend-model table contains 11 series-level models.

For the substantive-core aggregate, ADHD valence has a positive but non-significant slope of 0.0012 per year (p = 0.099), and Autism valence has a positive but non-significant slope of 0.0006 per year (p = 0.110). Neither aggregate target trend is flagged for residual autocorrelation.

## 6. Robustness Checks — Deliberate Baes Deviations

### Method Actually Run

The Warriner Sentiment and Intensity robustness notebooks rerun the frame-aware collocate analyses with Warriner et al. norms instead of NRC-VAD. They keep the same publication-year axis, target/core-frame strata, separate baselines, +/-5-token window, lemmatisation, focal-term exclusion, document-level bootstrap, and compact trend-model convention. Warriner scores are saved on the native 1-9 scale and on a 0-1 scaled version for comparison with NRC-VAD trends.

The Baes-MPNet Breadth robustness notebook reruns target-frame Breadth with `sentence-transformers/all-mpnet-base-v2`, matching Baes et al.'s generic sentence-embedding approach more closely than the main XL-LEXEME analysis. It keeps the publication-year axis, substantive-core target-frame strata, document-level bootstrap, mean pairwise cosine distance, and compact trend-model convention fixed. Comparator terms are deliberately excluded because this check tests the encoder substitution rather than re-estimating the full baseline contrast.

The integrated robustness-figure notebook turns the completed Intensity and Breadth checks into one appendix-style method-sensitivity figure. The figure plots ADHD and Autism substantive-core Overall trajectories as change from each method's own 2014 value, so the comparison is not distorted by different raw score scales.

### Outputs

- `notebooks/02_sentiment/02_warriner_valence_robustness_check.ipynb`
- `notebooks/03_intensity/02_warriner_arousal_robustness_check.ipynb`
- `notebooks/04_breadth/02_baes_mpnet_breadth_robustness_check.ipynb`
- `notebooks/07_integrated_synthesis/02_lsc_robustness_figures.ipynb`
- `data/interim/lsc/warriner_vad/lsc_warriner_collocate_matches.parquet`
- `data/interim/lsc/warriner_vad/lsc_warriner_context_coverage.parquet`
- `data/processed/lsc/sentiment/robustness_warriner/lsc_sentiment_warriner_annual_valence.csv`
- `data/processed/lsc/sentiment/robustness_warriner/lsc_sentiment_warriner_trend_models.csv`
- `data/processed/lsc/sentiment/robustness_warriner/lsc_sentiment_warriner_nrc_trend_comparison.csv`
- `data/processed/lsc/intensity/robustness_warriner/lsc_intensity_warriner_annual_arousal.csv`
- `data/processed/lsc/intensity/robustness_warriner/lsc_intensity_warriner_trend_models.csv`
- `data/processed/lsc/intensity/robustness_warriner/lsc_intensity_warriner_nrc_trend_comparison.csv`
- `data/processed/lsc/breadth/robustness_baes_mpnet/lsc_baes_mpnet_breadth_annual_scores.csv`
- `data/processed/lsc/breadth/robustness_baes_mpnet/lsc_baes_mpnet_breadth_trend_models.csv`
- `data/processed/lsc/breadth/robustness_baes_mpnet/lsc_baes_mpnet_xl_lexeme_trend_comparison.csv`
- `reports/tables/lsc/robustness/lsc_method_robustness_figure_data.csv`
- `reports/figures/lsc/robustness/lsc_method_robustness_intensity_breadth.png`
- `reports/figures/lsc/robustness/lsc_method_robustness_intensity_breadth.pdf`

### Results

The Warriner annual Sentiment and Intensity tables each contain the expected 143 annual rows. Each check raises one small-cell audit flag and no broad coverage warning. The MPNet Breadth table contains the expected 104 target-only annual rows: 13 years for ADHD and Autism across four target strata. It raises the same substantive small-cell concern for ADHD mixed-frame contexts in 2026, where only 42 sampled contexts from 38 documents are available.

For Sentiment, Warriner valence does not support a strong substantive-core target trend. ADHD has a very small negative scaled slope (-0.0001 per year, p = 0.728), while Autism has a small positive scaled slope (0.0003 per year, p = 0.069). ADHD therefore differs in direction from the weak positive NRC-VAD slope, but both estimates are small and non-significant. Autism remains weakly positive under both resources. This supports the main interpretation that aggregate Sentiment trends are weak and should not be foregrounded as strong evidence of connotational change.

For Intensity, Warriner arousal is more consequential. ADHD substantive-core arousal reverses from a positive NRC-VAD slope (+0.0010 per year, p = 0.020) to a negative Warriner slope (-0.0005 per year, p = 0.003). Autism changes from a borderline positive NRC-VAD slope (+0.0011 per year, p = 0.067) to essentially flat under Warriner (-0.0000 per year, p = 0.946). The Intensity result is therefore lexicon-sensitive rather than robust to the Baes-style Warriner substitution.

For Breadth, the MPNet check materially changes the target-level interpretation. ADHD substantive-core Breadth remains negative in direction but is much weaker under MPNet (-0.0004 per year, p = 0.445) than under XL-LEXEME (-0.0020 per year, p < 0.001). Autism shifts from approximately flat under XL-LEXEME (+0.0001 per year, p = 0.637) to significant narrowing under MPNet (-0.0014 per year, p = 0.001). At the frame level, Autism lived-only narrowing is the clearest replicated finding, with negative significant slopes under both encoders.

### Interpretation

The deliberate Baes-style checks validate that the methodological deviations are inspectable, but they do not uniformly reinforce the main estimates. NRC-VAD valence is adequate for the study's modest Sentiment conclusion because no strong aggregate target valence trend appears under either lexicon. NRC-VAD arousal should not be presented as a stable Intensity finding because the direction and significance of the aggregate target trends depend materially on the affective norm resource.

XL-LEXEME remains the methodologically preferred Breadth encoder because it represents the marked target token rather than the generic sentence. However, the MPNet check shows that the Breadth results are not encoder-invariant. The main Breadth interpretation should therefore be framed as a target-aware embedding finding, not as a generic sentence-embedding finding.

## 7. Measure 5 — Thematic Evolution

### Method Actually Run

Thematic evolution was implemented as a frame-aware diachronic Word2Vec neighbour-similarity analysis rather than as a scalar index. The notebook uses target-centred substantive ADHD/autism passages, canonicalises raw target forms to `adhd_concept` and `autism_concept`, lemmatises and content-filters the passages, trains one global skip-gram Word2Vec model per target/frame corpus, and then continues training annual models initialised from the corresponding global model.

The modelled strata are Overall substantive-core discourse, Clinical/disorder, and Lived experience. Mixed-frame contexts contribute to the Overall model but are not shown as a fourth report panel. Annual top-five neighbour lists are saved as the full audit trail, while report-facing line figures plot moderately stable neighbours: at least two annual top-five appearances and at least ten finite trajectory years. Detailed input, token, model, training, audit, and Overall/Mixed contribution diagnostics are displayed in the notebook rather than exported as separate processed CSVs.

### Outputs

- `data/interim/lsc/thematic_evolution/lsc_thematic_tokenised_contexts.parquet`
- `data/interim/lsc/thematic_evolution/word2vec_models/`
- `data/processed/lsc/thematic_evolution/lsc_thematic_annual_top_neighbours.csv`
- `data/processed/lsc/thematic_evolution/lsc_thematic_plotted_neighbours.csv`
- `data/processed/lsc/thematic_evolution/lsc_thematic_neighbour_similarity_trajectories.csv`
- `data/processed/lsc/thematic_evolution/lsc_thematic_execution_summary.json`
- `reports/figures/lsc/thematic_evolution/lsc_thematic_neighbour_similarity_adhd.png`
- `reports/figures/lsc/thematic_evolution/lsc_thematic_neighbour_similarity_adhd.pdf`
- `reports/figures/lsc/thematic_evolution/lsc_thematic_neighbour_similarity_autism.png`
- `reports/figures/lsc/thematic_evolution/lsc_thematic_neighbour_similarity_autism.pdf`
- `reports/figures/lsc/thematic_evolution/appendix/lsc_thematic_neighbour_rank_heatmap_adhd.png`
- `reports/figures/lsc/thematic_evolution/appendix/lsc_thematic_neighbour_rank_heatmap_adhd.pdf`
- `reports/figures/lsc/thematic_evolution/appendix/lsc_thematic_neighbour_rank_heatmap_autism.png`
- `reports/figures/lsc/thematic_evolution/appendix/lsc_thematic_neighbour_rank_heatmap_autism.pdf`

### Results

The run used 57,112 tokenised target contexts, of which 56,857 were usable after content filtering. All six target-frame model groups loaded from cache in the final rerun. The full annual top-neighbour table contains the expected 390 rows: two targets, three reported strata, thirteen years, and five annual neighbours. The stable plotted-neighbour table contains 24 rows, and the trajectory table contains 312 annual similarity rows. No audit flags were raised.

For ADHD, stable Overall and Clinical/disorder neighbours are dominated by child, disorder, condition, symptom, and diagnose language. The Lived experience panel is much sparser after stable-neighbour filtering: only help meets the report-facing stability rule. This is substantively informative rather than merely a plotting issue, because ADHD lived-experience annual top-neighbour lists show high churn.

For Autism, Overall and Clinical/disorder are anchored by child and disorder, with research, study, and developmental also appearing in the clinical panel. Lived-experience Autism has a more social/support-oriented neighbourhood: people, child, and support pass the stability rule. The exploratory heatmaps make the contrast between stable core neighbours and one-year annual neighbours visible.

### Interpretation

The thematic analysis supports a frame-aware interpretation: clinical/disorder discourse remains more closely associated with diagnostic, child-developmental, and research language, while lived-experience Autism discourse is more associated with people/support language. ADHD lived-experience thematic results should be presented cautiously because stable neighbours are sparse. Overall panels remain useful as headline summaries, but mixed-frame contribution diagnostics should be checked when an Overall neighbour does not appear as a stable neighbour in either separate frame.

## 8. Integrated Synthesis

### Method Actually Run

The integrated regression notebook combines the four completed scalar trend-model outputs. It does not refit regressions. It harmonises the existing Salience, Sentiment, Intensity, and Breadth trend-model CSVs, then exports one main-text target-frame regression table and one appendix baseline-comparator regression table.

The main table reports ADHD and Autism trajectories by frame: Salience has an Overall source-year row, while Sentiment, Intensity, and Breadth include Overall, clinical, and lived-experience rows. The appendix table reports the three unframed comparator terms in the same compact style. Mixed-frame rows remain in the source measure outputs and combined CSV but are excluded from the publication-facing tables for compactness.

A separate synthesis notebook creates the method-sensitivity robustness figure described in Section 6. A further diagnostic notebook fits and visualises quadratic trend summaries for the target-frame rows where the linear residual diagnostics suggested possible curvature: Autism Breadth and ADHD/Autism Sentiment.

### Outputs

- `reports/tables/lsc/regression/lsc_regression_models_combined.csv`
- `reports/tables/lsc/regression/lsc_regression_target_frames.csv`
- `reports/tables/lsc/regression/lsc_regression_target_frames.tex`
- `reports/tables/lsc/regression/lsc_regression_baseline_comparators.csv`
- `reports/tables/lsc/regression/lsc_regression_baseline_comparators.tex`
- `reports/tables/lsc/diagnostics/lsc_quadratic_trend_diagnostics_models.csv`
- `reports/tables/lsc/diagnostics/lsc_quadratic_trend_diagnostics_flagged.csv`
- `reports/tables/lsc/diagnostics/lsc_quadratic_trend_diagnostics_flagged.tex`
- `reports/tables/lsc/diagnostics/lsc_quadratic_trend_diagnostics_figure_data.csv`
- `reports/figures/lsc/diagnostics/lsc_quadratic_diagnostics_breadth_autism.png`
- `reports/figures/lsc/diagnostics/lsc_quadratic_diagnostics_breadth_autism.pdf`
- `reports/figures/lsc/diagnostics/lsc_quadratic_diagnostics_sentiment_targets.png`
- `reports/figures/lsc/diagnostics/lsc_quadratic_diagnostics_sentiment_targets.pdf`

### Results

The combined regression CSV contains 38 model rows: five Salience source-year models and eleven models each for Sentiment, Intensity, and Breadth. The main-text target-frame table contains 20 rows: two Salience Overall target rows and, for each semantic measure, ADHD/Autism Overall, clinical, and lived-experience rows. The appendix baseline table contains 12 rows, one for each comparator term by measure.

The quadratic diagnostic notebook now focuses on the rows needed for reporting: Overall and Clinical trajectories are plotted together, and lived-experience rows are dropped because they were not flagged. The model summary contains six plotted diagnostic rows, while the appendix table reports only the four rows whose linear residual diagnostics were flagged. These models are interpretive checks, not replacements for the linear trend summaries. Autism Breadth is the clearest case where a linear trend is misleading: the Overall and Clinical rows both have residual-autocorrelation flags, in-window inverted-U vertices around 2020-2021, and large adjusted-$R^2$ improvements under a quadratic form. The Clinical Breadth row is especially important because its linear slope is positive and nominally significant, but the AR(1) sensitivity slope is near zero and the quadratic diagnostic suggests a rise into the early 2020s followed by decline.

For Sentiment, the main nonlinear signal is Clinical rather than aggregate. ADHD Clinical Sentiment and Autism Clinical Sentiment both show residual-autocorrelation flags, in-window U-shaped vertices around 2019-2020, and large adjusted-$R^2$ improvements under a quadratic form. Overall Sentiment is retained in the figure only as context for the same target groups and years; it is not included in the flagged quadratic table. The diagnostic figures therefore support a narrower interpretation: some Clinical-frame annual series are curved enough that the linear slopes should be treated as summaries of net movement, not as claims of monotonic change.
