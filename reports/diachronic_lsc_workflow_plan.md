# Diachronic LSC Workflow Plan

This document tracks the practical analysis workflow. Methodological detail belongs in `reports/diachronic_lsc_analysis_plan.md`; consequential design decisions belong in `reports/lsc_analysis_design_decisions.md`.

## Current Stage

| Field | Value |
|---|---|
| Stage | Intensity |
| Status | ready to plan |
| Blocking dependency | none |
| Next action | Plan `notebooks/02_intensity/01_vad_arousal.ipynb` using the reusable VAD collocate handoff |

## Stage Order

| order | stage | notebook | status | required input | expected output |
|---:|---|---|---|---|---|
| 1 | Shared LSC context preprocessing | `notebooks/00_lsc_data_prep/01_build_lsc_contexts.ipynb` | complete | `data/processed/corpus/corpus_documents.parquet` | `data/interim/lsc/contexts/lsc_mention_contexts.parquet` |
| 2 | Shared context audit | `notebooks/00_lsc_data_prep/02_lsc_sample_audit.ipynb` | complete | shared context table | `lsc_context_audit_checks.csv`, `lsc_context_audit_flags.csv`, and manual-sample review handoff |
| 3 | Sentiment | `notebooks/01_sentiment/01_vad_valence.ipynb` | complete | shared context table; NRC-VAD v2.1 | `data/processed/lsc/sentiment/` |
| 4 | Intensity | `notebooks/02_intensity/01_vad_arousal.ipynb` | pending | shared context table; NRC-VAD v2.1 | `data/processed/lsc/intensity/` |
| 5 | Severity/intensifier check | `notebooks/02_intensity/02_severity_intensifier_check.ipynb` | pending | shared context table; dependency parses | supplementary modifier diagnostics |
| 6 | Salience audit | `notebooks/04_salience/01_salience_trend_audit.ipynb` | blocked | updated trend run synced locally | salience diagnostics from updated trend output |
| 7 | Breadth | `notebooks/03_breadth/01_xl_lexeme_breadth.ipynb` | pending | shared context table; XL-LEXEME | breadth scores and saved embeddings |
| 8 | Breadth diagnostics | `notebooks/03_breadth/02_breadth_diagnostics.ipynb` | pending | breadth outputs | raw-form, sample-size, and context diagnostics |
| 9 | Thematic evolution | `notebooks/05_thematic_content/01_bertopic_thematic_evolution.ipynb` | pending | target-centred passages | topic inventory and topic-over-time outputs |
| 10 | Integrated synthesis | `notebooks/06_integrated_synthesis/01_integrated_lsc_results.ipynb` | pending | all processed LSC outputs | integrated figures and tables |

## Decision Log

| date | decision | reason | recorded in |
|---|---|---|---|
| 2026-06-02 | Shared context preprocessing uses capped mention-level rows. | A mention-level contract supports VAD windows, severity checks, breadth sampling, and examples while limiting document dominance. | `reports/lsc_analysis_design_decisions.md` |
| 2026-06-02 | Same-sentence acronym/expansion pairs are collapsed before capping. | Prevents double-counting one conceptual mention as two semantic contexts while preserving collapse diagnostics. | `reports/lsc_analysis_design_decisions.md` |
| 2026-06-02 | Shared semantic contexts use publication year as `lsc_year`. | Semantic analyses need document publication time rather than Common Crawl source/capture time; excluded publication-date cases are written as diagnostics. | `reports/lsc_analysis_design_decisions.md` |
| 2026-06-02 | Sentiment uses a reusable NRC-VAD collocate handoff. | The same lemmatised local collocates can support Sentiment now and Intensity later without duplicating preprocessing. | `reports/lsc_analysis_design_decisions.md` |

## Stage Closeout Checklist

- Outputs created in the expected `data/interim/lsc/` or `data/processed/lsc/` path.
- Diagnostics reviewed for sample size, domain concentration, raw-form balance, and obvious extraction errors.
- Relevant design decisions recorded.
- `AGENTS.md` notebook and data rules followed.
- Next stage status updated.
