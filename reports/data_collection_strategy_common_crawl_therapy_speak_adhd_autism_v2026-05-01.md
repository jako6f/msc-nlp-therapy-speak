# Data Collection Strategy — Common Crawl Therapy‑Speak (ADHD, Autism + Baseline Terms)

**Project:** MSc Dissertation (Diachronic NLP / Concept Creep)

## Purpose of this document
1) A living “North Star” for the end‑to‑end data collection pipeline (design → implementation → outputs).
2) The shared reference for this ChatGPT project so future implementation decisions stay aligned.

**Status:** Active / editable (updated through the frozen Stage 1e document-quality cleanup pass; Stage 2 is the next scale-up step).

---

## 0. Executive summary

We study the rise of “therapy‑speak” in general web discourse by tracking usage and contexts of **ADHD** and **Autism** across ~10–15 years using **Common Crawl**. To provide a baseline for semantic drift, the design includes a **matched non‑clinical negative baseline**. Stage 1c evaluated five pilot candidates (**worry**, **sadness**, **tiredness**, **frustration**, and **loneliness**) and froze the pilot-dev baseline set as **frustration**, **sadness**, and **loneliness**. The baseline module will help assess whether observed ADHD/autism semantic drift exceeds ordinary drift in broadly comparable negative but non‑clinical terms.

### Core architecture
- **WET‑first scanning** for scale and stable denominators (fast, low cost).
- **WARC‑on‑demand** only for documents/contexts selected after WET triage (slower, but DOM‑aware extraction + metadata).
- **One crawl per year** (fixed temporal anchoring rule) for diachronic comparability.
- **Matched baseline terms:** processed through the same WET scan + boilerplate triage stack as ADHD/autism; Stage 1c pilot-dev freeze selected **frustration**, **sadness**, and **loneliness**.

### Two-track design (Stage 2 outputs)
- **Trend Track:** fixed‑effort samples per year for **rates over time** (candidate + validated rates with denominators).
- **Corpus Track:** targeted collection of a **higher‑quality modelling corpus** (precision‑leaning).

### Key constraints / guardrails
- **ASD disambiguation:** count `ASD` only if `autism` appears within **±200 characters**.
- **Per‑domain cap:** default **≤ 50 documents per registered domain per year** to prevent dominance.
- **Cross‑year comparability:** prefer conservative, stable rules; avoid year‑specific tuning.
- **Baseline comparability:** target and baseline terms must be processed with the same scan, sampling frame, boilerplate triage, context extraction, and reporting conventions wherever feasible.

### Stage roadmap
- **Stage 1a (pilot/discovery):** complete — end‑to‑end WET pipeline proven on `CC-MAIN-2016-44` and `CC-MAIN-2026-04`.
- **Stage 1b (WET target-terms scan + boilerplate triage):** frozen — conservative WET boilerplate mitigation reduced to a minimal, stable rule stack.
- **Stage 1c (WET baseline-terms scan + boilerplate triage):** complete / frozen — baseline candidates evaluated and final pilot-dev baseline set selected.
- **Stage 1d (generalisation + WARC validation/enrichment + English-only dedup filtering):** frozen.
- **Stage 1e (document-quality cleanup on WARC-validated documents):** frozen gate to Stage 2.
- **Stage 2 (scale):** one crawl per year across ~10 years, producing Trend Track + Corpus Track outputs.

---

## 1. Research goal and scope

### 1.1 Goal
Track how often and in what contexts **ADHD** and **Autism** appear in general web discourse over ~10–15 years, and evaluate their semantic drift relative to a matched baseline of non‑clinical negative terms. Stage 1c selected **frustration**, **sadness**, and **loneliness** as the current pilot-dev baseline set.

### 1.2 Scope decisions
- **Data source:** Common Crawl.
- **Primary scan artifact:** **WET** (plaintext).
- **Authoritative content artifact:** **WARC HTML** (only for selected survivors/contexts) + DOM‑aware extraction.
- **Temporal unit:** **1 crawl per year**, anchored consistently (rule stored in config).
- **Language scope:** English‑only for Corpus Track (i.e., modelling) and semantic-drift contexts when feasible; Trend Track can retain language flags and should be reported with/without English gating when feasible.
- **Unit of analysis:** page capture/document; for semantic-change analyses, extracted target/baseline contexts may become the analytic unit.

### 1.3 Outcomes
- **Trend Track dataset:** yearly candidate/validated mention rates with stable denominators.
- **Corpus Track dataset:** precision‑leaning modelling corpus for downstream NLP (semantic drift, framing, sentiment/valence, etc.).
- **Baseline-term dataset:** matched non‑clinical negative contexts used to estimate background semantic drift and contextual movement in the same Common Crawl environment.

---

## 2. Key definitions (operational)

### 2.1 Term roles
- **Target terms:** ADHD/autism terms that operationalise the therapy‑speak / concept creep case study.
- **Baseline terms:** matched non‑clinical negative terms used as a comparator for generic drift. Stage 1c evaluated `worry`, `sadness`, `tiredness`, `frustration`, and `loneliness`, and froze the pilot-dev baseline set as `frustration`, `sadness`, and `loneliness`.

### 2.2 Hits and validation levels
- **Candidate hit (WET):** target or baseline term matched in WET text after basic sanity filters.
- **WET‑validated hit:** candidate hit that survives the frozen Stage 1b WET triage stack.

**Metrics naming**
- `candidate_hits` is the raw WET match count after basic sanity filters.
- `validated_hits_wet` is the canonical WET validation count after Stage 1b triage.
- `validated_hits_warc` is reserved for WARC/Trafilatura validation in Stage 1d and Stage 2.

### 2.3 “Substantive” (working definition)
A document/context is “substantive” if the term appears in meaningful content (article/body) rather than navigation, taxonomies, tag pages, menus, footers, UI widgets, or other repeated chrome.

**Principle:** WET triage removes obvious junk; **Trafilatura on WARC** is the authoritative arbiter for marginal cases where WARC validation is run.

### 2.4 Timestamps
- `capture_ts`: capture timestamp from WARC headers (`WARC-Date`) — robust and always available for WARC records.
- `published_ts`: best‑effort inferred publication date from WARC HTML via `htmldate` — often missing/ambiguous.

**Rule:** Trend analyses use **capture year** as the primary time axis; publication dates are a robustness layer on the subset with credible signals.

### 2.5 Domain cap
To prevent a few large sites dominating:
- **Cap:** ≤ 50 retained documents per **registered domain** per year (default 50; stored in config).
- Apply cap on the retained set for the relevant track or module (Trend Track: WET‑validated hits; Corpus Track: post‑WARC / post‑filter survivors; Baseline module: WET‑validated baseline contexts unless otherwise specified).

---

## 3. Common Crawl objects and access model

### 3.1 Objects
- **WET:** plaintext extracted from HTML (cheap to scan).
- **WARC:** raw HTML + headers (required for DOM‑aware extraction and timestamp enrichment).

### 3.2 Access patterns
- **Stage 1:** download a small number of `.wet.gz` files per crawl; only fetch WARC records for selected survivors/contexts.
- **Stage 2 (provisional):** process in‑region (AWS `us-east-1`) if local throughput is insufficient; store only compact outputs.

---

## 4. Sampling design (diachronic)

### 4.1 Crawl selection (one per year)
Select one `CC-MAIN-YYYY-WW` crawl per year using a consistent anchoring rule (documented in config), e.g.:
- choose the crawl whose start date is closest to a fixed reference date (e.g., June 1).

Once Stage 2 starts, the year→crawl mapping is treated as frozen.

### 4.2 Trend Track (fixed-effort sampling)
Per yearly crawl:
- Sample **K WET files** uniformly at random from `wet.paths.gz` with a fixed seed.
- Scan all documents in those files for target and baseline terms.
- Report denominators + rates:
  - `docs_scanned`
  - `candidate_hits`
  - `validated_hits_wet` (and rate per N docs)

**Key:** Trend Track is designed for comparability, not maximum yield. Baseline terms must be included in the same fixed-effort scan when used for drift comparisons.

### 4.3 Corpus Track (targeted modelling corpus)
Per yearly crawl:
- Continue scanning beyond the Trend Track sample until **M** high‑quality target-term survivors are collected (or a budget is reached).
- Apply domain caps and dedup rules.
- Validate using WARC + Trafilatura; then apply English-only filtering gates (language ID, dedup).

**Key:** Corpus Track is designed for modelling quality while staying consistent with the Trend Track triage logic.

### 4.4 Baseline-term module (semantic drift comparator)
The baseline module is not a third Stage 2 production track. It is a comparator module that supports semantic drift inference.

Minimum viable design:
- scan the same WET samples used for target terms;
- apply the exact same WET boilerplate triage stack;
- retain term role fields (`term_role = target | baseline`) and `term_group` / `term_pattern` metadata;
- audit baseline term frequencies and removal rates;
- freeze a small final baseline set with sufficient annual coverage and manageable noise.

---

## 5. Matching, disambiguation, and triage rules

### 5.1 Target term patterns
**ADHD**
- `\badhd\b`
- (optional) spelled‑out variants: `attention[-\s]?deficit...`

**Autism**
- `\bautism\b`, `\bautistic\b`
- `autism[-\s]?spectrum...`

**ASD ambiguity rule**
- Treat `\bASD\b` as a hit only if `autism` appears within **±200 characters** (parameterised).

### 5.2 Baseline term patterns
Initial candidate baseline terms:
- `\bworry\b`
- `\bsadness\b`
- `\btiredness\b`
- `\bfrustration\b`
- `\bloneliness\b`

Stage 1c evaluated frequency, noise, and coverage on the pilot-dev slice. The current frozen pilot-dev baseline set is:
- `\bfrustration\b`
- `\bsadness\b`
- `\bloneliness\b`

Excluded after Stage 1c pilot-dev diagnostics:
- `\bworry\b` — strong residual generic / commercial contamination despite high coverage
- `\btiredness\b` — too sparse and too symptom-oriented for the intended comparator role

### 5.3 Stage 1b frozen WET triage stack (cheap, conservative)
Stage 1b freezes a minimal, stable triage layer intended to remove only obvious junk while preserving cross‑year comparability.
- Rule family: **signature_hard** (hard boilerplate signatures)
- Rule family: **directory_index** (directory/index page detection)

Key config assumptions:
- `boilerplate.check_window_chars = 2000`
- `filters.context_window_chars = 200`

Stage 1b rules are **not tuned** during Stage 1c or Stage 1d.

### 5.4 Baseline triage policy
Baseline terms will receive the **same WET boilerplate triage** as target terms, including the full frozen Stage 1b rule stack. This intentionally prioritises a simple and defensible like-for-like pipeline over a more complex distinction between generic and term-specific boilerplate rules.

---

## 6. Stage 1a — Pilot / discovery (complete)

### 6.1 Purpose
- Prove end‑to‑end feasibility: sample → download → scan WET → produce a tiny corpus.
- Lock the methodological primitives: term matching, ASD disambiguation, reproducible sampling.
- Identify dominant failure modes before scaling.

### 6.2 What was done
- **Pilot anchor crawls:** `CC-MAIN-2016-44` and `CC-MAIN-2026-04`.
- **Sampling:** `K = 2` WET files per crawl (total 4 WET files), deterministic with a seed.
- **Acquisition:** read `wet.paths.gz` per crawl and select WET paths via reservoir sampling; write a manifest in `data/manifests/`; download selected `.wet.gz`.
- **Scanning:** parse WET records and apply:
  - minimum text length filter
  - ADHD/autism matching
  - ASD disambiguation (±200 chars)
  - registered domain extraction (e.g., `tldextract`)
  - per‑domain cap (not triggered at pilot scale)

### 6.3 Pilot counts (from the run)
- Documents scanned: **157,461**
- Documents above min length: **146,995**
- Hits total: **728** (~0.5% on above‑min‑length docs)
- Unique domains total: **73,793**
- Unique domains contributing hits: **384**
- ASD hits: **18**
- Domain caps removed: **0**

### 6.4 Key learning
The main methodological risk is **precision**: many candidate hits reflect boilerplate/chrome rather than substantive discourse.

### 6.5 Stage 1a artifacts (where they live)
- **Stage outputs:** `data/interim/stage1_pilot-dev/stage1a/…` (untracked).
- **Report artifacts (figures/tables):** `reports/{figures,tables}/stage1_pilot-dev/…` (track only compact final artifacts).
- **Manifests:** `data/manifests/…` (tracked).
- Raw data stays untracked under `data/raw/…`.

---

## 7. Stage 1b — WET boilerplate mitigation (frozen)

### 7.1 Purpose
Freeze a conservative, cross‑year‑stable WET triage layer that removes only obvious junk.

### 7.2 Outcome
Stage 1b triage is reduced to two rule families:
- `signature_hard`
- `directory_index`

Stage 1b outputs live under:
- `data/interim/stage1_pilot-dev/stage1b/…`

---

## 8. Stage 1c — Baseline-terms scan + boilerplate triage (complete / frozen)

### 8.1 Purpose
Implement and evaluate the matched non‑clinical negative baseline module before WARC validation/enrichment. This stage answered whether the proposed baseline terms produced enough usable, WET‑validated contexts to serve as a semantic drift comparator in Stage 2.

### 8.2 Completed objectives
1) Added baseline term patterns to config and scan logic.
2) Ran the baseline candidates through the **same WET acquisition, scanning, and Stage 1b triage** as ADHD/autism.
3) Produced frequency, removal, and quality diagnostics per candidate term.
4) Selected a final pilot-dev baseline set for Stage 1d/Stage 2.

### 8.3 Candidate baseline terms
Initial set:
- `worry`
- `sadness`
- `tiredness`
- `frustration`
- `loneliness`

The final set was prioritised using:
- sufficient hit counts in both old and recent crawls;
- manageable boilerplate/noise after Stage 1b triage;
- roughly comparable negative valence;
- minimal clinical/diagnostic ambiguity.

### 8.4 What was done
- Use the existing Stage 1 WET files first (2016 + 2026) as a smoke test.
- Run scan with both `term_role = target` and `term_role = baseline`.
- Apply the frozen WET triage stack identically to target and baseline terms.
- Persist row-level Stage 1c diagnostics, including:
  - `candidate_hits` table;
  - `validated_hits_wet` table;
  - per-term summary diagnostics;
  - per-term top-domain diagnostics;
  - removed-audit samples;
  - retained validation samples.
- Export diagnostics:
  - candidate and WET‑validated counts per term;
  - removal rates by triage rule family;
  - top domains per term;
  - random validation samples per baseline term;
  - old vs recent coverage comparison.

### 8.5 Outcome
Stage 1c completed successfully on the pilot-dev smoke run using the Stage 1 anchor WET files (`CC-MAIN-2016-44` and `CC-MAIN-2026-04`).

Headline Stage 1c smoke-run counts:
- total `candidate_hits`: **3,199**
- total `validated_hits_wet`: **2,705**
- baseline `candidate_hits`: **2,471**
- baseline `validated_hits_wet`: **2,126**

Final pilot-dev baseline decision:
- **Selected:** `frustration`, `sadness`, `loneliness`
- **Excluded:** `worry`, `tiredness`

Reason for exclusions (short form):
- `worry`: too generic and commercially contaminated despite strong coverage
- `tiredness`: too sparse and too symptom-oriented for the intended comparator role

### 8.6 Stage 1c outputs
- Active Stage 1c freeze config:
  - `configs/stage1c_freeze.yaml`
- Baseline candidate scan tables under:
  - `data/interim/stage1_pilot-dev/stage1c/…`
- A short baseline decision note documenting:
  - final selected baseline terms;
  - excluded candidates and reasons;
  - any limitations (frequency mismatch, topic volatility, residual noise).
  - current file: `data/interim/stage1_pilot-dev/stage1c/README_stage1c_baseline_decision.md`

### 8.7 Stage 1c acceptance check
- Pipeline can scan and triage target + baseline terms in one run: **met**
- Each retained baseline term has adequate old/recent coverage for Stage 1d smoke testing: **met for `frustration`, `sadness`, `loneliness`**
- Removal rates are inspectable and not obviously pathological for the retained baseline terms: **met**
- Final baseline set is documented before Stage 1d: **met**

---

## 9. Stage 1d — Generalisation + WARC validation/enrichment + English-only dedup filtering (frozen)

### 9.1 Objectives
1) **Hold‑out generalisation test (WET):** sample WET files from a new year not used in Stage 1b/1c and apply frozen rules unchanged.
2) **Authoritative content validation (WARC):** resolve WARC pointers in bulk on an EC2 worker by querying a local Common Crawl index server backed by the raw CDXJ secondary indexes, then fetch WARC records only for selected survivors and run main-content extraction.
3) **Enrichment:** populate `capture_ts`, WARC pointers, and attempt `published_ts`.
4) **Filtering:** English‑only gating (language ID) and de‑duplication on extracted main content.
5) **Baseline smoke test:** ensure the selected baseline terms (`frustration`, `sadness`, `loneliness`) remain usable under the Stage 1d validation/filtering workflow, at least on a controlled sample.

### 9.2 Method (conceptual)
**1d‑A — Hold‑out WET generalisation**
- Sample **4 WET files** from an unseen crawl year.
- Compute and compare (vs 2016/2026 baseline):
  - candidate hit rate
  - `validated_hits_wet` rate
  - boilerplate removal rate
- Report separately for target and selected baseline terms.

**1d‑B — Remote local index-server pointer resolution**
- Export unique `(crawl_id, url)` survivors from the frozen Stage 1c anchor set plus the Stage 1d hold-out.
- Upload the URL table to S3.
- On a small **EC2** worker in `us-east-1`, install the per-crawl Common Crawl secondary indexes locally and query them through a local pywb/Common Crawl index server, producing:
  - `warc_filename`
  - `warc_record_offset`
  - `warc_record_length`
  - `fetch_time`
  - `fetch_status`
  - MIME/language metadata where available
- Treat the resulting pointer cache as the authoritative Stage 1d lookup surface.

**1d‑C — WARC extraction on EC2**
- Run the expensive WARC fetch and HTML extraction step on the same **EC2** worker in `us-east-1`.
- Fetch WARC byte ranges from Common Crawl using the resolver-produced pointer cache.
- Extract main content with:
  1. `Trafilatura` (`favor_recall=False` / precision-leaning)
  2. fallback extractor when Trafilatura returns empty output
- Retain only if the relevant term still occurs in extracted main content.

**1d‑D — Enrichment**
- For WET‑validated survivors selected for enrichment:
  - fetch the corresponding WARC record
  - extract main content
  - retain only if the relevant term occurs in extracted main content
- Populate `validated_hits_warc` for WARC‑validated records.

For WARC‑validated hits:
- `capture_ts` from WARC headers (`WARC-Date`)
- WARC pointers: `warc_filename`, `warc_offset`, `warc_length`
- attempt `published_ts` with `htmldate`, then normalize and filter it against capture-time and sanity constraints

**1d‑E — English-only dedup filtering**
- **Language identification:** enforce **English‑only** gating for modelling outputs (record decision thresholds).
- **Deduplication:** remove exact and near‑duplicate texts (record dedup strategy and % removed).

### 9.3 Stage 1d outputs
- Hold‑out metrics table + cross‑year stability assessment.
- Remote pointer cache (`cc_pointer_cache_*`) for the selected Stage 1d pilot URLs.
- Trafilatura‑filtered hit records (`validated_hits_warc`) for selected target/baseline records.
- Enriched hit records (`capture_ts`, WARC pointers, attempted `published_ts`).
- English‑only + deduped modelling texts/snippets.
- Frozen config/tag: **`v0.2-stage1d-freeze`**.

Stage 1d outputs live under:
- `data/interim/stage1_pilot-dev/stage1d/…`
- freeze README: `data/interim/stage1_pilot-dev/stage1d/README_stage1d_freeze.md`

### 9.4 Stage 1e — WARC-validated document-quality cleanup pass (frozen)
Stage 1e reran the pilot-dev corpus-quality pass on the same WET input slice as Stage 1d, allowing direct comparison of the cleanup strategy while avoiding a new hold-out sample.

Purpose:
- tighten the WARC-validated corpus quality before Stage 2;
- remove non-substantive documents, repeated/list-like pages, low-quality extracted text, schema-detected page types, and residual junk;
- avoid target-term centrality filters that would bias the ADHD/autism discourse analysis.

Final approach:
- keep the WET-stage URL denylist as a cheap pre-WARC cost-control filter;
- keep WARC extraction/enrichment unchanged from Stage 1e extraction outputs;
- replace the hand-rolled post-WARC text-shape rule stack with a DataTrove-backed document-quality gate;
- use DataTrove `GopherRepetitionFilter`, `GopherQualityFilter`, and `FineWebQualityFilter`;
- disable FineWeb's line-punctuation threshold with `line_punct_thr: 0.0` because the default rule over-filtered plausible Trafilatura-extracted prose;
- keep local English gating and local exact/near-dedup rather than adopting DataTrove MinHash at pilot scale.

Final Stage 1e freeze run IDs:
- WET scan: `20260511_092845`
- URL export: `20260511_093546`
- WARC extraction: `20260511_100811`
- Document quality: `20260511_131035`

Headline final counts:
- documents scanned: **310,082**
- WET candidate hits: **2,682**
- WET-validated hits: **1,942**
- WARC-validated row-level hits: **862**
- WARC-validated documents entering document quality: **733**
- documents kept after document quality: **507**
- English documents: **492**
- final deduplicated corpus documents: **489**
- final target hits: **165**
- final baseline hits: **403**

Manual validation:
- final deterministic sample: `data/interim/stage1_pilot-dev/stage1e/document_quality/cc_val_sample30_20260511_131035.csv`
- the output is the best pilot-dev corpus candidate so far, but remaining known failures include prose-like spam, service directories, commercial listings, archive pages, and isolated incidental target mentions.

Stage 1e outputs live under:
- `data/interim/stage1_pilot-dev/stage1e/…`
- freeze README: `data/interim/stage1_pilot-dev/stage1e/README_stage1e_freeze.md`

---

## 10. Publication date and provenance strategy (WARC enrichment)

### 10.1 Rationale
WET text is not reliable for publication metadata. We enrich selected hits by retrieving the corresponding WARC HTML record and extracting best-effort publication dates.

### 10.2 Stored fields (must-have)
- `capture_ts` (from `WARC-Date`)
- WARC pointers: `warc_filename`, `warc_offset`, `warc_length` (plus digest if available)
- `published_ts` (nullable)

### 10.3 Extraction hierarchy (published_ts)
Use `htmldate` on the fetched WARC HTML with original-date mode enabled. The returned date is then:
1) normalized to UTC ISO format for storage
2) rejected if it is later than `capture_ts` beyond a small tolerance
3) rejected if it matches obvious defaults (e.g. `1970-01-01`, year `0001`)
4) rejected if it predates the configurable pre-web lower bound
5) otherwise stored as `published_ts`; else missing

### 10.4 Validity checks
- `published_ts` should be ≤ `capture_ts` (allow small tolerance for timezone/clock skews).
- Reject implausible years (configurable bounds).

---

## 11. Stage 2 — Full-scale diachronic collection (next)

### 11.1 Stage 2 objectives
- Collect ~10 years of data with:
  - Trend Track yearly rates (stable denominators)
  - Corpus Track modelling corpus per year (precision‑leaning)
  - Baseline-term contexts for semantic drift comparison
  - WARC enrichment for provenance and timestamps (at least for Corpus Track and selected baseline/target contexts used in semantic analyses)
  - Stage 1e document-quality filtering for WARC-validated corpus outputs

### 11.2 Stage 2 plan (provisional)
1) Freeze the year→crawl mapping (anchoring rule + chosen crawl IDs).
2) Run Trend Track for each year (fixed K WET files; scan target + final baseline terms; produce per‑year metrics).
3) Build Corpus Track per year (collect M WARC‑validated, English‑only, deduped target-term documents; apply domain caps).
4) Collect matched baseline contexts per year for semantic drift comparison (fixed contexts per term-year; same triage and sampling principles).
5) Freeze datasets with checksums and version tags.

### 11.3 Compute model (local vs AWS)
Decision after Stage 1e throughput measurements:
- Stage 2 should assume **AWS `us-east-1`** for the WARC stage.
- Local execution remains appropriate for WET scanning, validation, and downstream analysis.
- Store only compact outputs locally; keep the expensive WARC fetch/extraction step in-region.
- Observed Stage 1e throughput:
  - WET scan: ~**1,478 docs/sec**
  - WARC extraction: ~**3.14 docs/sec**
  - document quality: ~**15.59 docs/sec**
  - WARC extraction is the bottleneck.

---

## 12. Data outputs and schemas (recommended)

### 12.1 Run summary (per run)
- `run_id`, `stage`, `track`, `crawl_id`, `wet_files`, `seed`, `rules_version`, `started_at`, `finished_at`
- `docs_scanned`, `candidate_hits`, `validated_hits_wet`, `validated_hits_warc`
- `term_role` summary (`target` vs `baseline`)
- `domains_seen`, `domains_retained`, `%removed_by_triage`, `%removed_by_lang`, `%removed_by_dedup`

### 12.2 Candidate hits (WET)
- `crawl_id`, `url`, `registered_domain`
- `term_role` (`target` / `baseline`)
- `term_group` (e.g., `adhd`, `autism`, `baseline_negative`)
- `term_pattern` or `matched_term`
- term flags (adhd/autism/asd_disambiguated/baseline term)
- `snippet_wet` (context window)
- `doc_char_len`
- optional: `hash_text_norm`

### 12.3 WET‑validated hits (post Stage 1b triage)
- all of the above plus:
  - `triage_flags` (which rule families fired / passed)
  - `validated_hits_wet` membership

### 12.4 WARC‑validated + enriched hits (post Stage 1d)
- `validated_hits_warc` membership
- `capture_ts`
- WARC pointers: `warc_filename`, `warc_offset`, `warc_length`
- `published_ts`
- `text_main` (Trafilatura main content)
- `lang`, `lang_confidence` (or equivalent)
- `dedup_cluster_id` / `is_duplicate` (as applicable)

### 12.5 Document-quality corpus outputs (post Stage 1e)
- `stage1e_passes_document_quality`
- `stage1e_document_quality_reason`
- DataTrove filter diagnostics:
  - `passes_gopher_repetition`
  - `passes_gopher_quality`
  - `passes_fineweb_quality`
  - `stage1e_quality_filter_name`
  - `stage1e_quality_filter_reason`
- `context_snippet_sentence_200`
- final English/dedup fields:
  - `language_code`, `language_score`, `is_english`
  - `dedup_cluster_id`, `dedup_reason`, `is_dedup_representative`

### 12.6 File formats
- Prefer **Parquet** for large tables; CSV for small summaries; JSONL for manifests.

---

## 13. Reproducibility and governance

### 13.1 Repo layout and naming (single repo; Common Crawl only at component level)
Repo: `msc-nlp-therapy-speak`.

Layout:
- `src/`
  - `data_sources/commoncrawl/` — WET sampling/scanning/triage + (Stage 1d+) WARC pointers/enrichment.
  - `analysis/` — downstream analyses consuming processed outputs.
  - `cli.py` — stable CLI entrypoints (sample/download/scan/validate/resolve/extract/document-quality).
- `configs/` — versioned run configs (crawl IDs, seed, K/M, regex, baseline terms, ASD window, domain cap, stage flags).
  - Stage-scoped freeze configs should follow:
    - `configs/stage1b_freeze.yaml`
    - `configs/stage1c_freeze.yaml`
    - `configs/stage1d_freeze.yaml`
    - `configs/stage1e.yaml`
  - Reserve `configs/commoncrawl_collection.yaml` for the post-Stage-1 collection freeze that will drive Stage 2.
- `data/`
  - `raw/` — immutable inputs (e.g., `.wet.gz` downloaded in Stage 1). **Untracked.**
  - `manifests/` — JSONL sampling provenance (seed, crawl ID, sampled paths, timestamps). **Tracked.**
  - `interim/` — working outputs (untracked; stage-namespaced).
  - `processed/` — production datasets (untracked; track only small “release” subsets if needed).
- `reports/`
  - strategy notes and supporting report materials.
  - `logs/` — run logs (untracked).
- `paper/` — Trinity template + dissertation content.
- `notebooks/` — exploration only; stable logic is promoted into `src/` to avoid notebook sprawl.

### 13.2 Execution workflow (repeatable “daily loop”)
Runs are executed via CLI entrypoints (not ad-hoc notebooks):
1) Set run parameters in the relevant stage-scoped `configs/*.yaml` file (crawl IDs, seed, K/M, regex, baseline terms, caps).
2) **Sample** WET paths → write manifest JSONL to `data/manifests/` (tracked).
3) **Acquire** WET files (Stage 1: download to `data/raw/`; Stage 2: stream in-region).
4) **Scan** WET → doc denominators + candidate hits/snippets (`data/interim/…`).
5) **Validate (Stage 1b/1c)** → `validated_hits_wet` + removal diagnostics.
6) **Export URLs (Stage 1d freeze)** → write unique `(crawl_id, url)` survivors for remote pointer resolution.
7) **Upload URLs (Stage 1d freeze)** → upload the latest URL export to S3.
8) **Remote resolve (Stage 1d freeze)** → resolve WARC pointers in bulk on EC2 via the local Common Crawl index server in `us-east-1`.
9) **Remote extract (Stage 1d freeze)** → fetch WARC byte ranges on EC2 and extract main content.
10) **Document quality (Stage 1e freeze)** → DataTrove/Gopher quality gate + English gating + dedup annotations / corpus-ready texts.
11) Compile `paper/` and commit code/config/manifests.

### 13.3 Directory conventions (Stage 1)
- Stage outputs (untracked):
  - `data/interim/stage1_pilot-dev/stage1a/…`
  - `data/interim/stage1_pilot-dev/stage1b/…`
  - `data/interim/stage1_pilot-dev/stage1c/…`
  - `data/interim/stage1_pilot-dev/stage1d/…`
  - Stage 1d commonly uses:
    - `url_exports/`
    - `pointer_cache/`
    - `warc/`
    - `filter_en_dedup/`
  - Stage 1e commonly uses:
    - `wet_scan/`
    - `url_exports/`
    - `pointer_cache/`
    - `warc/`
    - `document_quality/`
    - `metrics/`

### 13.4 Tracked vs untracked
**Tracked:** `src/`, `configs/`, `paper/`, `data/manifests/`, and selected report notes/materials as needed.
**Untracked:** `data/raw/`, `data/interim/`, `data/processed/`, `reports/logs/`, caches/notebook checkpoints, and secrets (e.g., `.env`).

### 13.5 Immutable raw data rule
Anything under `data/raw/` is treated as immutable input (never edited in place).

### 13.6 Manifests (must-have)
Every sampling action writes a manifest (JSONL) capturing crawl IDs, sampled paths, seed, timestamp, and source URLs. Manifests are tracked.

### 13.7 Makefile discipline
- A minimal `Makefile` provides one-command entrypoints for routine pipeline actions (`sample`, `download`, `acquire`, `scan`, `validate`, `process`, `run`) and paper builds (reduces run divergence and errors).
- Stage-scoped freeze targets should mirror the stage config name:
  - `cc_stage1b_freeze_*` ↔ `configs/stage1b_freeze.yaml`
  - `cc_stage1c_freeze_*` ↔ `configs/stage1c_freeze.yaml`
  - `cc_stage1d_freeze_*` ↔ `configs/stage1d_freeze.yaml`
  - `cc_stage1e_*` ↔ `configs/stage1e.yaml`
- Reserve `cc_collection_*` for the post-Stage-1 collection workflow that will pair with `configs/commoncrawl_collection.yaml`.

---

## 14. Risks and mitigations (living list)

### 14.1 Overfitting triage rules
Mitigation: keep Stage 1b triage minimal and frozen; use Stage 1c only to add baseline-term scanning, not to tune the triage stack; test on hold‑out year in Stage 1d; document deltas.

### 14.2 Recall loss
Mitigation: report candidate hit rates alongside validated rates; track where losses occur (WET triage vs WARC validation); compare target and baseline removal rates.

### 14.3 Cross-year comparability
Mitigation: fixed anchoring rule; conservative thresholds; consistent reporting of denominators and removal rates.

### 14.4 Baseline mismatch
Mitigation: use several baseline terms, audit frequency and noise in Stage 1c, freeze a small final set, and report any mismatch or volatility transparently.

### 14.5 Publication date missingness
Mitigation: always store `capture_ts`; treat `published_ts` as optional and best-effort.

---

## 15. Open decisions / TODO (update as work progresses)
- Final anchoring rule for yearly crawl selection (and the frozen year→crawl list).
- Whether Stage 2 keeps the local Stage 1e dedup or adopts a larger-scale MinHash strategy.
- Corpus Track targets per year (M) and compute budget constraints.
- Whether to backfill `capture_ts` / WARC pointers for Stage 1a/1b/1c artifacts (optional).

Resolved in Stage 1c:
- Final pilot-dev baseline term set: `frustration`, `sadness`, `loneliness`

---

## Appendix A — Stage 1b rule registry (frozen)
| Rule family | Purpose | Key params | Notes |
|---|---|---|---|
| `signature_hard` | remove obvious boilerplate signatures | `boilerplate.check_window_chars=2000` | conservative, cross-year |
| `directory_index` | detect directory/index-like pages | `filters.context_window_chars=200` | conservative, cross-year |

---

## Appendix B — Stage completion checklist

### Stage 1a (done)
- [x] WET sampling + scan works end-to-end
- [x] Baseline noise assessment and pilot exports

### Stage 1b (frozen)
- [x] Minimal WET triage stack frozen for comparability

### Stage 1c (done / frozen)
- [x] Add baseline term patterns to config
- [x] Scan target + baseline terms through same WET pipeline
- [x] Apply frozen Stage 1b triage to baseline terms
- [x] Export baseline diagnostics (counts, removal rates, top domains, random samples)
- [x] Select final baseline terms and document decision

### Stage 1d (frozen)
- [x] Hold‑out WET generalisation completed (4 WET files, unseen year)
- [x] Populate `validated_hits_warc` via WARC + Trafilatura (`favor_recall=False`) for selected target/baseline records
- [x] Enrichment: `capture_ts`, WARC pointers, attempted `published_ts`
- [x] English-only dedup filtering: report % removed
- [x] Freeze config + tag: `v0.2-stage1d-freeze`

### Stage 1e (frozen)
- [x] Rerun Stage 1e on the same pilot-dev WET input slice for direct comparison
- [x] Apply WET-stage URL denylist as pre-WARC cost control
- [x] Resolve pointers and extract WARC records on EC2
- [x] Apply DataTrove/Gopher document-quality gate
- [x] Disable FineWeb line-punctuation threshold after it proved too destructive for Trafilatura output
- [x] Preserve local English gating and local dedup
- [x] Export final document-quality corpus, summaries, throughput metrics, and sample30
- [x] Freeze README: `data/interim/stage1_pilot-dev/stage1e/README_stage1e_freeze.md`
- [ ] Validate the cleaned corpus manually before any Stage 2 expansion

### Stage 2 (provisional)
- [ ] Freeze year→crawl mapping
- [ ] Run Trend Track for all years (target + final baseline terms)
- [ ] Build Corpus Track per year
- [ ] Collect matched baseline contexts for semantic-drift comparison
- [ ] Freeze datasets + checksums + version tags
