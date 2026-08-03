# msc-nlp-therapy-speak

MSc dissertation (Applied Social Data Science, Trinity College Dublin) on whether
**ADHD** and **autism** discourse on the open web shows the lexical semantic change
predicted by *concept creep* theory — horizontal broadening and vertical softening of
harm-related concepts.

The project builds its own diachronic corpus from Common Crawl rather than relying on
curated academic text, then measures change along five dimensions of the SIBling
framework (Baes et al., 2024), separating change in *meaning* from change in *discourse
composition* via a validated framing classifier.

**Headline finding.** Neither hypothesised form of concept creep is supported. Intensity
does not decline, breadth contracts rather than broadens (most for ADHD), and the
thematic core stays diagnostic and child-centred. The clear change is compositional:
discourse shifts from clinical toward lived-experience framing. See `paper/main.pdf`.

The corpus spans thirteen annual Common Crawl snapshots (2014–2026) and yields 96,864
quality-filtered target contexts for `adhd` and `autism`, plus three baseline emotion
terms (`frustration`, `sadness`, `loneliness`) used as a comparison track.

---

## Repository map

```text
configs/        Collection config (commoncrawl_collection.yaml) + untracked local overrides
src/            Common Crawl acquisition/scan/resolve/WARC/quality code + CLI
notebooks/      Staged analysis notebooks (00 data prep → 08 measurement invariance)
scripts/        Runnable tooling: annotation runners, CL short-paper figure/table build
data/           raw / interim / processed corpus and analysis outputs (mostly git-ignored)
reports/        Figures, tables, runbooks, provenance docs, interim presentation
paper/          Dissertation LaTeX source, sections, bibliography, and compiled PDF
Makefile        Collection, lint, and paper-build targets
environment.yml Conda environment (msc-nlp)
```

The work has two halves: a **corpus-collection pipeline** (`src/`, driven by the
`Makefile` and runbooks) and a **discourse-analysis workflow** (`notebooks/`, reading the
collected corpus). Remote collection runs upload artifacts to S3 and local machines sync them back.

---

## Part 1 — Corpus collection pipeline (`src/`)

The pipeline starts from yearly Common Crawl WET files, finds candidate term hits,
resolves their corresponding WARC HTML records, extracts readable document text, and
writes two downstream products:

- **`trend`** — fixed-size yearly samples for source-year diachronic rate estimates.
- **`corpus`** — larger yearly samples with document-quality gates for target-term
  semantic and contextual analysis.

```text
Common Crawl crawl map → sample yearly WET files → download → scan for target/baseline hits
  → export candidate URLs → resolve WARC pointers (local CC index server) → fetch WARC HTML
  → extract main text + candidate publication dates → quality gates, English filter, dedup
  → build processed trend and corpus outputs
```

The WET stage is a fast coarse search; the WARC stage is the substantive validation point,
where the original HTML is retrieved and document text re-extracted before final filtering.
The pipeline does **not** classify documents by page or genre type — the aim is a
quality-gated corpus of substantive web documents, not a genre-controlled sample.

Key modules live in `src/data_sources/commoncrawl/` (`cc_acquire`, `cc_scan`,
`cc_resolve`, `cc_warc`, `cc_document_quality`, `cc_collection`), with `src/cli.py` as the
entry point and `src/pathing.py` for shared output paths.

**Operating the pipeline** — use the runbooks, not this README, for execution:

- [Single-year runbook](reports/commoncrawl_single_year_runbook.md) — smoke tests,
  targeted reruns, corpus-expansion batches.
- [All-years runbook](reports/commoncrawl_all_years_runbook.md) — the full run, after the
  single-year path is validated.

These cover EC2 setup, GitHub sync, local AWS config, preflight checks, tmux usage, S3
sync-back, output inspection, and failure recovery.

---

## Part 2 — Discourse analysis (`notebooks/`)

The notebooks are numbered in execution order and read the collected corpus. Stage `00`
builds one shared mention-level context table; stage `01` produces a framing classifier;
stages `02`–`06` estimate the five SIBling dimensions (overall and within frames); stage
`07` integrates the results into report-ready tables and figures.

| Stage | Notebook(s) | Purpose |
|-------|-------------|---------|
| `00_lsc_data_prep` | `01_build_lsc_contexts`, `02_lsc_sample_audit` | Build and audit the shared mention-level context table from the quality-gated corpus. |
| `01_classification` | `01`–`07` | Build a hierarchical **clinical vs lived-experience** frame classifier: prepare samples → ingest human annotations → LLM annotation batches → LLM criticism + human correction → train → apply → results. |
| `02_sentiment` | `01_vad_valence` (+ Warriner robustness) | Annual connotational **sentiment** via NRC–VAD valence over local collocates. |
| `03_intensity` | `01_vad_arousal` (+ Warriner robustness) | Annual affective **intensity** via NRC–VAD arousal. |
| `04_breadth` | `01_xl_lexeme_breadth` (+ MPNet robustness) | Annual semantic **breadth** as within-year contextual dispersion of XL-LEXEME target-aware embeddings. |
| `05_salience` | `01_salience_trend_audit` | Annual source-year **salience** (WARC-validated hits per unit) from the trend run. |
| `06_thematic_content` | `01_neighbour_similarity_thematic_evolution` | **Thematic** evolution via pairwise neighbour similarity (Vylomova & Haslam, 2021). |
| `07_integrated_synthesis` | `01`–`04` | Integrated regression tables, robustness figures, quadratic-trend diagnostics, and post-hoc contributor diagnostics. |

### The classification stage in more detail

Framing labels separate *clinical/disorder* from *lived-experience* discourse so that
semantic-change measures can be read within-frame. Labels were produced by LLM-assisted
annotation under human adjudication: a locked **Codex annotator** (`scripts/annotation/run_codex_annotation.py`,
prompts `annotator_v1`–`v4`, codebooks `v1`–`v4`), an independent **Gemini critic**
(`scripts/annotation/run_gemini_criticism.py`, `critic_v5`) that ranks likely errors, and a human coder who
holds final say — no LLM suggestion changes a training label without human review. A
held-out 200-case human validation set is never touched by the LLM workflow. The runbooks
`codex_annotation_runbook.md` and `gemini_criticism_runbook.md` document these external steps.

> Notebook `01_classification/01` intentionally shows no stored outputs: it orchestrates
> protected annotation handoffs whose execution is delegated to the `run_*.py` scripts and
> their runbooks.

---

## Where the outputs live

| What | Location |
|------|----------|
| Processed analysis datasets (per dimension, incl. robustness variants) | `data/processed/lsc/{salience,sentiment,intensity,breadth,thematic_evolution,classification,posthoc}/` |
| Processed corpus / trend outputs | `data/processed/{corpus,trend,manifests}/` |
| Figures (PDF + PNG) | `reports/figures/lsc/` (and `reports/figures/commoncrawl_pipeline/`) |
| Tables (CSV + LaTeX) | `reports/tables/lsc/{classification,regression,robustness,diagnostics,posthoc}/` |
| Collection reporting numbers | `reports/tables/summary/commoncrawl_collection_reporting.csv` |

Each analysis notebook writes its datasets to `data/processed/lsc/...` and its
report-ready figures/tables to `reports/`. Most of `data/` is git-ignored; the tracked
`data/processed/lsc/` CSVs are the analysis-ready outputs the paper draws on.

---

## Reproducibility

```bash
conda env create -f environment.yml
conda activate msc-nlp
make sanity     # CLI help + import check
make lint       # ruff
make paper      # build paper/main.pdf via latexmk
```

External resources not committed to the repo (see `.gitignore`): raw crawl data
(`data/raw/`), large model weights and lexicons (`data/external/`), locally cloned
reference code (`external/reference/`, incl. the Baes 2024 SIBling framework), and paper
PDFs (`paper/literature/`). Local credentials go in untracked `configs/local/*.yaml`
(template: `configs/local/aws.example.yaml`).

---

## Paper and provenance

- `paper/main.pdf` — the compiled dissertation; `paper/sections/` holds the source.
- [reports/commoncrawl_corpus_design_and_provenance.md](reports/commoncrawl_corpus_design_and_provenance.md)
  — crawl-window choice, crawl-map pinning, WET-first acquisition, WARC validation,
  publication-date recovery, quality filtering, dedup, and trend/corpus separation.
- `data/interim/pilot-dev/*/00_closeout_stage_*.md` — pilot-development closeout notes
  that shaped the final pipeline.
