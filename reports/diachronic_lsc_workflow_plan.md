# Diachronic LSC Workflow Plan

This document tracks the practical analysis workflow. Methodological detail belongs in `reports/diachronic_lsc_analysis_plan.md`; consequential design decisions belong in `reports/lsc_analysis_design_decisions.md`.

## Current Stage

| Field | Value |
|---|---|
| Stage | Frame Classification |
| Status | hierarchical schema update in progress; pilot annotation restart pending |
| Blocking dependency | human pilot/validation annotation and ACT-inspired LLM annotation/criticism |
| Next action | Complete `data/interim/lsc/classification/human_annotation/frame_pilot_annotation.xlsx` using `notebooks/01_classification/codebook_v2.md`, save it as `frame_pilot_annotation_completed.xlsx`, then ingest labels with `02_ingest_human_annotations.ipynb` |

## Stage Order

| order | stage | notebook | status | required input | expected output |
|---:|---|---|---|---|---|
| 1 | Shared LSC context preprocessing | `notebooks/00_lsc_data_prep/01_build_lsc_contexts.ipynb` | complete | `data/processed/corpus/corpus_documents.parquet` | `data/interim/lsc/contexts/lsc_mention_contexts.parquet` |
| 2 | Shared context audit | `notebooks/00_lsc_data_prep/02_lsc_sample_audit.ipynb` | complete | shared context table | `lsc_context_audit_checks.csv`, `lsc_context_audit_flags.csv`, and manual-sample review handoff |
| 3 | Frame classification sample prep | `notebooks/01_classification/01_prepare_annotation_samples.ipynb` | hierarchical v2 schema | shared context table | protected pilot/validation XLSX handoffs, LLM-training pool, and full target-context pool |
| 4 | Human annotation ingest | `notebooks/01_classification/02_ingest_human_annotations.ipynb` | hierarchical v2 schema; pending human labels | completed pilot and validation XLSX workbooks | clean human label tables with derived frames |
| 5 | LLM annotation batching | `notebooks/01_classification/03_llm_annotation_batches.ipynb` | hierarchical v2 schema; pending prompt pilot | LLM-training pool; annotator prompt | annotator batches and parsed LLM labels |
| 6 | LLM criticism and correction | `notebooks/01_classification/04_llm_criticism_and_correction.ipynb` | hierarchical v2 schema; pending LLM labels | parsed annotator labels; critic prompt | critic batches, critic scores, and correction sheet |
| 7 | Frame classifier training | `notebooks/01_classification/05_train_frame_classifier.ipynb` | hierarchical v2 schema; pending corrected labels | human labels; ACT-corrected LLM labels | hierarchical classifier, validation metrics, and validation predictions |
| 8 | Frame classifier application | `notebooks/01_classification/06_apply_frame_classifier.ipynb` | scaffolded; pending trained classifier | trained frame classifier; shared context table | `data/processed/lsc/classification/` frame-label outputs |
| 9 | Sentiment | `notebooks/02_sentiment/01_vad_valence.ipynb` | complete; frame-aware rerun pending | shared context table; NRC-VAD v2.1; frame labels | `data/processed/lsc/sentiment/` |
| 10 | Intensity | `notebooks/03_intensity/01_vad_arousal.ipynb` | complete; frame-aware rerun pending | reusable VAD collocate handoff; frame labels | `data/processed/lsc/intensity/` |
| 11 | Severity/intensifier check | `notebooks/03_intensity/02_severity_intensifier_check.ipynb` | pending | shared context table; dependency parses; frame labels | supplementary modifier diagnostics |
| 12 | Salience audit | `notebooks/05_salience/01_salience_trend_audit.ipynb` | complete; frame-composition extension pending | completed processed trend output | annual Salience tables, validation/publication-year diagnostics, and report figures |
| 13 | Breadth | `notebooks/04_breadth/01_xl_lexeme_breadth.ipynb` | implemented; execution pending; frame-aware rerun pending | shared context table; XL-LEXEME; frame labels | breadth scores and saved embeddings |
| 14 | Breadth diagnostics | `notebooks/04_breadth/02_breadth_diagnostics.ipynb` | pending | breadth outputs; frame labels | raw-form, frame, sample-size, and context diagnostics |
| 15 | Thematic evolution | `notebooks/06_thematic_content/01_bertopic_thematic_evolution.ipynb` | pending | target-centred passages; frame labels | topic inventory and topic-over-time outputs |
| 16 | Integrated synthesis | `notebooks/07_integrated_synthesis/01_integrated_lsc_results.ipynb` | pending | all processed LSC outputs, including frame labels | integrated figures and tables |

## Decision Log

| date | decision | reason | recorded in |
|---|---|---|---|
| 2026-06-02 | Shared context preprocessing uses capped mention-level rows. | A mention-level contract supports VAD windows, severity checks, breadth sampling, and examples while limiting document dominance. | `reports/lsc_analysis_design_decisions.md` |
| 2026-06-02 | Same-sentence acronym/expansion pairs are collapsed before capping. | Prevents double-counting one conceptual mention as two semantic contexts while preserving collapse diagnostics. | `reports/lsc_analysis_design_decisions.md` |
| 2026-06-02 | Shared semantic contexts use publication year as `lsc_year`. | Semantic analyses need document publication time rather than Common Crawl source/capture time; excluded publication-date cases are written as diagnostics. | `reports/lsc_analysis_design_decisions.md` |
| 2026-06-02 | Sentiment uses a reusable NRC-VAD collocate handoff. | The same lemmatised local collocates can support Sentiment now and Intensity later without duplicating preprocessing. | `reports/lsc_analysis_design_decisions.md` |
| 2026-06-03 | Intensity uses the reusable NRC-VAD collocate handoff. | Arousal and valence are parallel VAD dimensions, so using the same collocate table keeps preprocessing fixed across Sentiment and Intensity. | `reports/lsc_analysis_design_decisions.md` |
| 2026-06-03 | Breadth uses local XL-LEXEME target-token embeddings. | Target-aware embeddings and a fixed sampling contract make the breadth stage scalable to future larger corpus runs. | `reports/lsc_analysis_design_decisions.md` |
| 2026-06-04 | Frame classification inserted before LSC measures. | Clinical/lived-experience composition may explain apparent semantic trajectories, so target contexts need frame labels before downstream interpretation. | `reports/lsc_analysis_design_decisions.md` |
| 2026-06-04 | Classification notebooks live in `notebooks/01_classification/`. | Keeps shared context prep separate from annotation, LLM criticism, classifier training, and frame-label deployment. | `reports/lsc_analysis_design_decisions.md` |
| 2026-06-05 | Frame classification uses a hierarchical substantive-discourse gate. | Non-substantive boilerplate, lists, resource blurbs, and noisy extraction should not be treated as ordinary negative examples for clinical/lived framing. | `reports/lsc_analysis_design_decisions.md` |
| 2026-06-09 | Salience uses source-year WARC-validated hits per million minimum-length WET tokens. | This is the denominator supported consistently by the completed trend run; WET/candidate rates and publication-year composition remain diagnostics. | `reports/lsc_analysis_design_decisions.md` |

## Stage Closeout Checklist

- Outputs created in the expected `data/interim/lsc/` or `data/processed/lsc/` path.
- Diagnostics reviewed for sample size, domain concentration, raw-form balance, and obvious extraction errors.
- Relevant design decisions recorded.
- `AGENTS.md` notebook and data rules followed.
- Next stage status updated.
