# Data Collection Strategy — Common Crawl Therapy‑Speak (ADHD, Autism)

**Project:** MSc Dissertation (Diachronic NLP / Concept Creep)

**Purpose of this document**
1) A living “where we are / where we’re going” reference for the human researcher.
2) A foundational plan for the **MSc Dissertation** ChatGPT Project so future chats can ground decisions in an agreed data-collection strategy.

**Status:** Active / editable. Update as Stage 1b, Stage 1c, and Stage 2 mature.

---

## 0. Executive summary

We study the rise of “therapy‑speak” in general web discourse by tracking usage and contexts of **ADHD** and **Autism** across ~10 years using **Common Crawl**.

Core design:
- **WET-first scanning** for scale and denominators (fast, low cost).
- **One crawl per year** (consistent temporal anchor).
- **Hybrid outputs**:
  - **Option A (Trends):** lightweight repeated cross‑sectional samples to estimate diachronic mention rates.
  - **Option B (Modelling corpus):** a smaller, higher-quality corpus of substantive documents for downstream NLP.
- **Contextual disambiguation:** e.g., treat **ASD** as a hit only when “autism” occurs within ±200 characters.
- **Domain cap:** e.g., **≤ 50 documents per registered domain per year** to prevent dominance.
- **Publication date enrichment:** use **WARC retrieval only for hit documents** (or a selected subset) to infer `published_ts` from HTML metadata; add/store `capture_ts` (from `WARC-Date`) and store WARC pointers (implemented in Stage 1c; backfill earlier artifacts where feasible).

Stage structure:
- **Stage 1a:** pilot / discovery (**complete**) — end-to-end WET pipeline proven on `CC-MAIN-2016-44` and `CC-MAIN-2026-04` (K=2 WET files per crawl) with appendix-ready exports.
- **Stage 1b:** boilerplate mitigation micro-iteration (**in progress**) — 2–3 conservative iterations on the same 4 WET files using a repeatable CLI validation harness.
- **Stage 1c:** generalization + enrichment (**planned**) — test the Stage 1b rules on a new/larger WET sample with a hold-out evaluation; add `capture_ts`; store WARC pointers and infer `published_ts` for hits.
- **Stage 2 (provisional):** scale to full diachronic collection (one crawl per year; Option A + Option B), with compute location (local vs AWS) decided after Stage 1 throughput measurements.

---

## 1. Research goal and scope

### 1.1 Goal
Track how often and in what contexts the terms **ADHD** and **Autism** appear in general web discourse over ~10 years, using Common Crawl.

### 1.2 Scope decisions
- **Data source:** Common Crawl.
- **Primary file type:** **WET** for text scanning and snippet extraction.
- **Temporal unit:** **1 crawl per year**, anchored consistently (e.g., “closest crawl to June 1” or another fixed rule).
- **Language scope:** initial focus on **English** (if feasible; else “language-unknown” retained with flags).
- **Unit of analysis:** **document/page capture**.

### 1.3 Outcomes
- **Trend dataset (Option A):** counts / rates over time with stable denominators.
- **Modelling dataset (Option B):** substantive snippets/documents suitable for downstream NLP.

---

## 2. Key definitions (operational)

### 2.1 Timestamps
- `capture_ts`: timestamp of Common Crawl capture (from `WARC-Date`). **Present in the raw WET/WARC headers**, but **not yet extracted/stored** in current Stage 1a outputs. Planned: add in **Stage 1c** (and backfill Stage 1a artifacts where feasible).
- `published_ts`: inferred publication (or earliest credible) date from HTML metadata, obtained via WARC record retrieval. Often missing/ambiguous; store source + confidence.

**Rule:** Primary time axis for trends is **capture year**, unless robust evidence supports published-year alignment for the subset.

### 2.2 What counts as a “hit”
A **candidate hit** is a document whose extracted text contains at least one target term pattern.

A **validated/substantive hit** is a candidate hit that passes boilerplate mitigation and structural heuristics designed to remove “chrome” and non-content pages.

### 2.3 “Substantive” (working definition)
A document is “substantive” if the target term appears in **meaningful content** rather than navigation, boilerplate, category lists, tag pages, menus, or repeated site chrome.

Stage 1b will finalize a defensible operational definition and corresponding rules.

### 2.4 Domain cap
To avoid large sites dominating:
- **Cap:** ≤ 50 retained documents per **registered domain** per year (tunable; default 50).
- Apply cap on **validated hits** (not raw candidates) unless needed earlier.

---

## 3. Data sources and access model

### 3.1 Common Crawl objects
- **WET:** plaintext extracted from HTML (used for scanning).
- **WARC:** raw HTML + headers (used for publication date inference and (optionally) cleaner snippets if later needed).

### 3.2 Access patterns
- **Stage 1:** HTTP download of a small number of `.wet.gz` files selected from `wet.paths.gz` per crawl.
- **Stage 2 (provisional):** process in-region (AWS `us-east-1`) to avoid large downloads; store only compact outputs.

---

## 4. Sampling design (diachronic)

### 4.1 Crawl selection (one per year)
Select one `CC-MAIN-YYYY-WW` crawl per year using a consistent anchoring rule (documented in config):
- Example rule: **choose the crawl whose start date is closest to June 1** of that year.

Store the chosen crawl IDs in a versioned config and treat them as frozen once Stage 2 starts.

### 4.2 Option A (Trends): fixed-effort sampling
Per yearly crawl:
- Sample **K WET files** uniformly at random from that crawl’s `wet.paths.gz` using a fixed seed.
- Scan all documents in those files.
- Compute mention rates:
  - candidate hit rate per N docs
  - validated hit rate per N docs

**Key:** Always preserve denominators: `docs_scanned`.

### 4.3 Option B (Modelling corpus): targeted collection
Per yearly crawl:
- Continue scanning beyond the Option A sample until **M validated hits** are collected (or until a budget is reached).
- Apply domain caps and dedup rules.

**Key:** Keep Option B distinct from Option A so trend validity is not conflated with “collect lots of hits”.

---

## 5. Filtering and disambiguation rules

### 5.1 Term patterns (baseline)
**ADHD**
- `\badhd\b`
- spelled-out variants (optional): `attention[-\s]?deficit...`

**Autism**
- `\bautism\b`, `\bautistic\b`
- `autism[-\s]?spectrum...`

**ASD ambiguity rule**
- Treat `\bASD\b` as a hit only if `autism` appears within ±200 characters (window parameterized).

All patterns and parameters are stored in the config.

### 5.2 Boilerplate mitigation (Stage 1b)
Three-tier plan (see Stage 1b section):
1) low-risk chrome suppression
2) structural heuristics
3) optional platform-domain handling

---

## 6. Stage 1a — Pilot / discovery (complete)

### 6.1 Purpose
- Prove end-to-end feasibility: sample → download → scan WET → produce a tiny corpus.
- Lock the core methodological primitives: term matching, ASD disambiguation, and reproducible sampling.
- Generate dissertation-ready artifacts (tables/figures → LaTeX Appendix).
- Identify the dominant failure mode before scaling (boilerplate/chrome).

### 6.2 What was done (factual)
- **Pilot anchor crawls:** `CC-MAIN-2016-44` and `CC-MAIN-2026-04`.
- **Sampling:** `K = 2` WET files per crawl (total **4** WET files), deterministic with a seed.
- **Acquisition:** read `wet.paths.gz` per crawl and perform **reservoir sampling** to select K WET paths; write a manifest `data/manifests/cc_sample_<timestamp>.jsonl`; download selected `.wet.gz` to `data/raw/wet/`.
- **Scanning:** CLI scan over `data/raw/wet/*.wet.gz` parsing WET/WARC conversion records and applying:
  - minimum text length filter
  - ADHD/autism regex matching
  - **ASD disambiguation:** count `ASD` only if `autism` occurs within ±200 characters
  - registered domain extraction (e.g., `tldextract`)
  - **domain cap implemented** (≤50 per registered domain per crawl), but **not triggered** at pilot scale

### 6.3 Pilot counts (from the run)
- Documents scanned: **157,461**
- Documents above min length: **146,995**
- Hits total: **728** (~0.5% hit rate on above-min-length docs)
- Unique domains total: **73,793**
- Unique domains contributing hits: **384**
- ASD hits: **18**
- Domain caps removed: **0**

### 6.4 Key learning
A large fraction of candidate hits are not substantive discourse but **boilerplate/chrome** (menus, taxonomies, UI fragments, accessibility widgets, etc.). The main methodological risk is therefore **precision** (substantive yield), not raw term detection.

### 6.5 Stage 1a artifacts
- **Reproducible intake:** manifests in `data/manifests/` capturing crawl IDs, sampled paths, seed, timestamps, source URLs.
- **Run logs:** `reports/logs/` (untracked).
- **Scan outputs (untracked):** `data/interim/` tables including run summaries and a pilot corpus table (hits with context snippets).
- **LaTeX integration:** an export step produced LaTeX tables and a figure and inserted them into the dissertation **Appendix** (with a pointer from Methods), avoiding copy–paste errors.
- **Repo milestone:** Stage 1 pilot closed and tagged as **`stage1-pilot`**.

---

## 7. Stage 1b — Boilerplate mitigation micro-iteration (in progress)

### 7.1 Objective
Raise the proportion of hits that are **substantively meaningful** (suitable for downstream NLP), by adding **conservative, generic** boilerplate mitigation on top of WET scanning.

**Target:** substantive hit rate ≥ ~70% (validated hits), based on disciplined spot-checking.

### 7.2 Core approach
- Keep the dataset fixed: **the same 4 WET files** from Stage 1a.
- Run **2–3** small iterations, each adding a low-risk rule layer.
- Avoid overfitting by design:
  - no domain-specific blacklists
  - rely on generic signals (UI phrases, density, structure)
  - use **fresh random samples** each iteration for qualitative checks

### 7.3 Validation harness (already implemented; must-have)
Use `cc-validate` (and `make cc_pilot_validate`) to keep iterations disciplined. Per run it produces:
- `cc_val_sample25_<runid>.csv` (seeded random 25 hits)
- `cc_val_asd_<runid>.csv` (all ASD rows)
- top 10 domains + share printed
Plus counters: hits removed by each rule type (signature vs density vs structural, etc.).

### 7.4 Planned iterations
**Iteration 1: Generic “chrome suppression” (low risk, high return)**
- signature patterns: skip-to-content, cookie banners, accessibility widgets, e-commerce chrome
- gentle density checks: minimum snippet word count, alphabetic ratio, etc.
- counters: removals by signature vs density

**Iteration 2: Structural heuristics (only if needed)**
- a single structural rule for menu/list/taxonomy detection based on repetition/fragmentation/separators
- still generic; still conservative

**Iteration 3 (optional): Platform-domain nuance (only if indicated)**
- if platform domains (e.g., `blogspot.com`, `wordpress.com`) cause artefacts, apply host-level nuance so caps behave sensibly

### 7.5 Stage 1b deliverables
- Updated scan pipeline + config flags (boilerplate mitigation stack)
- Updated summary metrics showing removals by filter type
- Short validation note (before/after substantive precision estimate from spot-checks)
- Optional: refreshed Appendix artifacts (not required until Stage 2)

---

## 8. Stage 1c — Generalization test + timestamp enrichment (planned)

### 8.1 Purpose
- **Test generalization:** evaluate the Stage 1b filter stack on a **new and larger** WET sample (out-of-sample).
- **Add a hold-out evaluation stage:** final headline metrics come from a locked test set (separate from any tuning).
- **Add time signals + provenance:** extract/store `capture_ts`; store WARC pointers for each hit; retrieve WARC for `published_ts` inference.

### 8.2 Data and evaluation design
- Draw a larger sample of WET files (exact K/TBD) using the same deterministic sampling procedure (seeded).
- Split into:
  - **Dev set:** only for minor threshold adjustments if absolutely necessary.
  - **Hold-out test set:** locked; used only once to produce Stage 1c summary metrics.

### 8.3 New fields added (must-have)
- Extract and store **`capture_ts`** (`WARC-Date`) for all hit records.
- For each (validated) hit, store **WARC pointers** obtained via the crawl index lookup:
  - `warc_filename`, `warc_offset`, `warc_length`
  - plus `digest` and capture `timestamp` when available
- Retrieve the referenced WARC record for each hit (or a sampled subset if volume dictates) to infer **`published_ts`**, storing:
  - `published_ts` (nullable)
  - `published_ts_source` (jsonld/meta/time/url/none)
  - `published_ts_confidence` (high/med/low)

### 8.4 Acceptance criteria for finishing Stage 1
**Must-have**
- Stage 1b rules achieve the substantive target on the **hold-out** Stage 1c evaluation set (or a documented shortfall + decision).
- Candidate hit rate does not collapse relative to Stage 1a baseline.
- Timestamp enrichment success is measured and documented (share of hits with credible `published_ts`).
- Rules/config frozen for Stage 2; repo tagged (e.g., `v0.2-stage1c-freeze`).

---

## 9. Publication date strategy (WARC enrichment)

### 9.1 Rationale
WET provides extracted text but not reliable publication metadata. We therefore enrich **hit documents** by retrieving the corresponding **WARC** HTML record and extracting candidate publication dates.

### 9.2 Scope
- Default: apply to **validated hits**.
- If volume is large: apply to a stratified sample (by year/domain/term) and/or to the Option B corpus first.

### 9.3 Stored fields (must-have)
- `capture_ts` (from `WARC-Date`)
- WARC pointers: `warc_filename`, `warc_offset`, `warc_length`, plus `digest`/`timestamp` when available
- `published_ts` (nullable)
- `published_ts_source` and `published_ts_confidence`

### 9.4 Extraction hierarchy (published_ts)
Attempt in order:
1) JSON-LD (`schema.org`) fields: `datePublished`, `dateModified`
2) OpenGraph / meta: `article:published_time`, `pubdate`, `DC.date`, etc.
3) HTML `<time datetime=...>` near top
4) URL date pattern (weak; mark low confidence)
5) else missing

### 9.5 Validity checks
- `published_ts` must be ≤ `capture_ts` (allow small tolerance).
- Reject implausible years.

### 9.6 Use in analysis
- Primary trend time axis: **capture year**.
- Publication-year analysis: robustness check on subset with credible `published_ts`.

---

## 10. Provisional Stage 2 — Full-scale collection (TBC after Stage 1)

### 10.1 Stage 2 objectives
- Collect a diachronic dataset covering ~10 years with:
  - Option A trend estimates per year
  - Option B modelling corpus per year
  - WARC-based published_ts enrichment for (some) hits

### 10.2 Stage 2 plan (provisional)
**Step 1 — Freeze year-by-year crawl list**
- Finalize 10 crawl IDs using the anchor rule.

**Step 2 — Option A per year**
- Sample K WET files per year (fixed effort).
- Run scan + Stage 1b validated-hit rules.
- Produce per-year metrics tables.

**Step 3 — Option B per year**
- Continue scanning until M validated hits per year (budgeted).
- Apply domain caps and dedup.

**Step 4 — WARC enrichment**
- For Option B (and optionally a sample of Option A hits): infer published_ts.

**Step 5 — Freeze datasets**
- Write immutable outputs with checksums and version tags.

### 10.3 Compute model (local vs AWS)
Decision after Stage 1b:
- If local throughput is sufficient: keep local.
- If not: use AWS in-region compute (us-east-1), store only compact outputs.

Stage 2 cost principles:
- Avoid downloading large WET/WARC volumes to local.
- Store only compact outputs in S3.
- Terminate compute immediately after runs.

---

## 11. Data outputs and schemas

### 11.1 Core tables (recommended)
1) **Scan log (per run)**
- `run_id`, `crawl_id`, `wet_files`, `seed`, `rules_version`, `started_at`, `finished_at`
- `docs_scanned`, `candidate_hits`, `validated_hits`, `domains_seen`, `domains_retained`

2) **Candidate hits** (WET)
- `crawl_id`, `capture_ts`, `url`, `registered_domain`
- `term_flags` (adhd/autism/asd_disambiguated)
- `snippet_wet`
- `doc_char_len`
- `hash_text_norm` (optional)

3) **Validated hits** (after Stage 1b rules)
- all of the above plus:
  - `validation_flags` (which rules fired)
  - `link_density`, `listiness`, `lexical_diversity` (optional)

4) **Enriched hits** (WARC)
- `published_ts`, `published_ts_source`, `published_ts_confidence`
- `warc_pointer_fields` (filename/offset/length/digest as available)

### 11.2 File formats
- Prefer **Parquet** for tables, CSV for small summaries, JSONL for manifests.

---

## 12. Reproducibility and governance

### 12.1 Repository and project layout (current state)
- **Repo:** `msc-nlp-therapy-speak`.
- **Layout principle:** Common Crawl is a **data-source component** (e.g., `src/data_sources/commoncrawl/`), not the whole project identity.
- **Tracked vs untracked:**
  - Track: `src/`, `configs/`, `paper/`, small `reports/{figures,tables}/`, and `data/manifests/`.
  - Do not track: `data/raw/`, `data/interim/`, `data/processed/`, `reports/logs/`, caches, notebook checkpoints, secrets.

### 12.2 Immutable raw data rule
- Anything in `data/raw/` is treated as immutable input; never edited in place.

### 12.3 Reproducible data intake (must-have)
- Each sampling action writes a **manifest** (JSONL) recording crawl IDs, sampled WET paths, seed, timestamp, and source URLs.
- Manifests are tracked in Git; raw WET/WARC data is not.

### 12.4 Execution discipline (current state)
- Stage 1 runs via repeatable CLI commands (sampling, scanning, validation, export).
- A lightweight **Makefile** provides one-command entrypoints (sanity/lint/test, pilot validate/export, paper build).

### 12.5 Environment (current state)
- **Conda environment:** `nlp-therapy` (Python 3.11) with a registered Jupyter kernel.
- Lock files exist (conda + pip) to support reruns.

### 12.6 LaTeX integration (current state)
- No copy–paste: LaTeX includes generated outputs from `reports/figures/` and `reports/tables/`.
- Pilot artifacts are placed in the **Appendix** and referenced from **Methods**.

### 12.7 Milestones and freezing
- Stage 1a was closed and tagged as **`stage1-pilot`**.
- Stage 1b ends with a frozen “filter stack” config (still in-sample).
- Stage 1c produces out-of-sample headline metrics + timestamp enrichment and is the gate to Stage 2.
- Planned tag after Stage 1c: **`v0.2-stage1c-freeze`**.

---

## 13. Risks and mitigations (living list)

### 13.1 Boilerplate mitigation overfitting
Mitigation: keep Stage 1b iterations conservative and generic; perform the **out-of-sample hold-out evaluation in Stage 1c**; document every rule change and its measured effect.

### 13.2 Recall loss (true positives removed)
Mitigation: track candidate hit rate and analyze deltas per iteration.

### 13.3 Cross-year comparability
Mitigation: test rules on older vs recent crawls; keep thresholds conservative.

### 13.4 Publication date missingness and ambiguity
Mitigation: store capture_ts always; treat published_ts as optional; record source/confidence.

---

## 14. Open decisions / TODO (update as work progresses)

- Final anchor rule for yearly crawl selection.
- Final language detection strategy (if any) and whether to restrict to English.
- Final dedup strategy (within-year / across-year).
- Whether to store WARC-clean snippets opportunistically during published_ts enrichment.
- Stage 2 compute choice (local vs AWS) based on measured throughput.

---

## Appendix A — Stage 1b rule registry (placeholder)

Maintain a table here once rules are frozen:

| Rule ID | Description | Thresholds | Risk | Added in iteration |
|---|---|---|---|---|
| R1 | Link density filter | … | low/med | 1 |
| R2 | Cookie/chrome signature removal | … | low | 1 |
| R3 | Listiness/menu detection | … | med | 2 |

---

## Appendix B — Term matching registry (placeholder)

Maintain the exact regex patterns (versioned) and examples of tricky cases.

---

## Appendix C — Stage completion checklist

### Stage 1a (done)
- [x] WET sampling + scan works end-to-end
- [x] Baseline noise assessment

### Stage 1b (in progress)
- [ ] Iteration 1 metrics (chrome suppression)
- [ ] Iteration 2 metrics (structural heuristics, if needed)
- [ ] Optional iteration 3 (platform nuance, only if indicated)
- [ ] Freeze Stage 1b filter stack config + short validation note

### Stage 1c (planned)
- [ ] New/larger WET sample drawn with deterministic sampling
- [ ] Hold-out evaluation completed (headline substantive estimate)
- [ ] `capture_ts` extracted and stored
- [ ] WARC pointers stored for hits
- [ ] `published_ts` extraction run + missingness documented
- [ ] Freeze rules/config for Stage 2 + tag (`v0.2-stage1c-freeze`)

### Stage 2 (provisional)
- [ ] Finalize yearly crawl list
- [ ] Run Option A for all years
- [ ] Build Option B per year
- [ ] WARC enrichment for published_ts
- [ ] Freeze datasets + checksums

