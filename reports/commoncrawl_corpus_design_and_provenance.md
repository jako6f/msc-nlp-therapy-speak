# Common Crawl Corpus Design and Provenance

**Project:** MSc dissertation corpus collection for diachronic NLP analysis of ADHD/autism discourse
**Status:** Final collection design after pilot-dev validation
**Active config:** `configs/commoncrawl_collection.yaml`
**Operational runbook:** `reports/commoncrawl_collection_runbook.md`
**Pilot-dev archive:** `data/interim/pilot-dev/`
**Pilot-dev checkpoint tag:** `v0.3-pilot-dev`

## 1. Purpose and Scope

This document records the design, configuration, and methodological decisions behind the Common Crawl collection pipeline used in this repository. It is intended to be public-facing: a reader should be able to understand what the corpus is designed to measure, how documents enter or leave the pipeline, which key parameters are frozen in config, and which limitations remain.

The document is a provenance and design record, not an execution manual. Step-by-step commands are maintained separately in `reports/commoncrawl_collection_runbook.md`.

The research objective is to study how ADHD and autism language appears in general web discourse over time, and whether usage contexts shift in ways that exceed broader background drift in non-clinical negative language.

The final collection has two linked aims:

- estimate diachronic mention rates for ADHD/autism and matched baseline terms in Common Crawl;
- build a high-quality WARC-extracted document corpus for downstream semantic and discourse analysis.

## 2. Source Data

The source data are Common Crawl WET and WARC files.

- WET files provide extracted plaintext and are used for cheap first-pass scanning.
- WARC files provide the archived HTML records and are used for exact document validation, main-text extraction, and publication-date enrichment.
- Common Crawl index metadata are used to resolve selected URLs to WARC filenames, byte offsets, and byte lengths.

The pipeline deliberately uses WET first and WARC second. Scanning all WARC HTML directly would be more expensive and unnecessary for the initial denominator and candidate discovery step. WET-first scanning provides a stable, cheap denominator; WARC retrieval is reserved for selected candidate URLs.

## 3. Target and Comparator Terms

The target terms are ADHD and autism expressions:

- `adhd`: `\badhd\b`
- `attention_deficit`: `attention[-\s]?deficit`
- `autism`: `\bautism\b`
- `autistic`: `\bautistic\b`
- `autism_spectrum`: `autism[-\s]?spectrum`
- `asd`: `\bASD\b`, retained only when autism appears nearby

`ASD` is treated separately because it is an ambiguous acronym. The active config uses a `200` character disambiguation window.

The frozen comparator terms are:

- `frustration`
- `sadness`
- `loneliness`

These comparator terms were selected during pilot development because they provided usable coverage while remaining more semantically interpretable and less commercially contaminated than rejected alternatives such as `worry`. `tiredness` was rejected because volume was low and surviving contexts skewed toward symptom lists and clinical/somatic material.

The comparator terms are not intended to be perfect semantic controls. They provide a non-target reference track for assessing whether ADHD/autism usage differs from broader affective-language patterns in Common Crawl.

## 4. Temporal Frame and Crawl Selection

The primary temporal window is `2014-2026`.

The conservative fallback window is `2016-2026`.

The 2014 start is used because, by then, Common Crawl had large WARC/WET/WAT crawls with a format suitable for the WET-first and WARC-enrichment workflow. Older Common Crawl data exist, including a 2012 corpus, but early crawls used legacy ARC formats and are less convenient for this pipeline.

One crawl is selected per year. The selection rule is deterministic:

- use the official Common Crawl `collinfo.json`;
- select the available `CC-MAIN` crawl whose crawl midpoint is closest to April 15 of that year;
- require WET path, WARC path, and index availability checks;
- freeze the explicit crawl map in config so future runs do not drift if Common Crawl metadata changes.

The auditable crawl-selection manifest is written to `reports/commoncrawl_collection_crawl_map.json`.

Frozen crawl map:

| Year | Crawl ID |
| --- | --- |
| 2014 | `CC-MAIN-2014-15` |
| 2015 | `CC-MAIN-2015-18` |
| 2016 | `CC-MAIN-2016-18` |
| 2017 | `CC-MAIN-2017-17` |
| 2018 | `CC-MAIN-2018-17` |
| 2019 | `CC-MAIN-2019-18` |
| 2020 | `CC-MAIN-2020-16` |
| 2021 | `CC-MAIN-2021-17` |
| 2022 | `CC-MAIN-2022-21` |
| 2023 | `CC-MAIN-2023-14` |
| 2024 | `CC-MAIN-2024-18` |
| 2025 | `CC-MAIN-2025-18` |
| 2026 | `CC-MAIN-2026-17` |

## 5. Output Tracks

The final pipeline has two output tracks.

### Trend Track

Purpose: estimate rates over time with fixed effort per year.

Configuration:

- fixed WET files per year: configured in `collection.trend.fixed_wet_files_per_year`
- corpus document-quality stage: disabled by default for trend

Trend outputs should report three rates:

- `validated_hits_wet / docs_scanned`
- `validated_hits_warc / docs_scanned`
- `validated_hits_warc / validated_hits_wet`

Rationale:

- WET rates are cheaper and have the cleanest denominator.
- WARC rates are cleaner but more expensive.
- Corpus-style document quality is reserved for the corpus track; trend rates use accepted
  WARC-validated summaries so the annual denominator remains the fixed WET sample.
- WARC/WET retention quantifies how much stricter validation changes the trend signal.

### Corpus Track

Purpose: build a larger high-quality modelling corpus for semantic and discourse analysis.

Configuration:

- desired target-term survivors per target group and year: `1000`
- soft minimum per target group and year: `500`
- initial WET batch size: `50`
- expansion WET batch size: `50`
- first-pass maximum WET files per year: `150`

The corpus track is intentionally iterative. Additional yearly batches can be run later if yield, time, and budget remain acceptable.

## 6. Pipeline Overview

The final pipeline has six substantive stages:

1. deterministic WET acquisition;
2. WET scan and conservative candidate triage;
3. remote URL upload, WARC pointer resolution, and WARC byte-range fetching;
4. WARC HTML extraction and publication-date enrichment;
5. post-WARC document-quality gate;
6. English filtering, near-deduplication, and processed-output construction.

Active Makefile targets use collection-oriented names:

- `collection_*` for individual steps;
- `trend_year` and `corpus_year` for yearly batches;
- `trend` and `corpus` for multi-year local preparation.

There is no separate baseline command. Target and comparator terms are processed together so the same denominator, scan logic, and quality gates apply to both.

## 7. WET Acquisition and Scan

The WET scan uses plaintext Common Crawl records. Documents shorter than `500` characters are excluded before term matching.

Key scan parameters:

| Parameter | Value | Rationale |
| --- | ---: | --- |
| `project.seed` | `123` | Deterministic sampling and reproducible validation samples. |
| `filters.min_chars` | `500` | Remove extremely short pages/fragments before matching. |
| `filters.domain_cap` | `50` | Limit domain concentration within a run. |
| `filters.context_window_chars` | `200` | Store compact match context for inspection. |
| `filters.asd_disambiguation_window_chars` | `200` | Retain `ASD` only when local context supports autism relevance. |

The WET stage stores candidate-hit rows, term summaries, domain summaries, removed-audit samples, and validation samples. This makes the cheap scan inspectable before WARC work. For the trend track, the WET scan also records document and whitespace-token denominators so salience can be reported both per scanned document and per scanned token. The processed trend output keeps the combined annual row but also includes separate rows for `term_role`, `term_group`, and `matched_term`, allowing target and baseline trajectories to be modelled separately.

## 8. WET Triage and Cost-Control Filters

Pilot development showed that raw WET term matching recovered relevant material but also retained page chrome, navigation fragments, directory pages, and other low-value text. The final pipeline therefore keeps conservative WET-stage triage.

The WET triage has three components.

### Hard Signature Boilerplate

High-confidence regular expressions remove obvious UI/chrome fragments such as cookie banners, accessibility widgets, ecommerce checkout language, and skip-navigation text.

Examples include:

- `skip\s+to\s+content`
- `cookie\s+consent`
- `add\s+to\s+cart`
- `open\s+accessibility\s+menu`
- `adhd[-\s]?friendly\s+(mode|profile)`

### Directory/Index Heuristic

The directory/index detector targets navigation-heavy pages using a small lexicon plus structural features.

Important configured signals:

- lexicon examples: `all conditions`, `conditions a-z`, `health a-z`, `browse conditions`, `a-z index`;
- `score_threshold = 4`;
- separator density;
- short-fragment ratio;
- low sentence-terminator rate;
- title-case token ratio;
- single-letter token count.

This heuristic is intentionally conservative. Broader WET-only rules were tested during pilot development and dropped because they risked overfitting and recall loss.

### Negative Page-Type URL Denylist

The URL denylist removes obvious low-value page types before WARC extraction to reduce cost. It is not treated as the substantive quality gate.

Configured patterns include:

- tag/category/search pages;
- playlist/forum/thread/viewtopic pages;
- quote and flashcard pages;
- album and pin pages;
- product/product-category/job pages;
- `trackAsin=` URLs;
- obvious casino/pharma/spam domains.

The core document-quality decision is deferred to the post-WARC gate because WET text alone lacks DOM and page-structure evidence.

## 9. WARC Pointer Resolution and Remote Execution

Selected WET candidates are exported as unique `(crawl_id, url)` rows and uploaded to S3. WARC pointers are resolved on EC2 using a local Common Crawl index server.

Final resolver backend:

- provider: `local_index_server`
- server host: `127.0.0.1`
- server port: `8080`
- URL workers: `16`
- query limit: `25`
- request timeout: `120` seconds

This approach was selected after pilot experimentation because it provided reliable WARC filename/offset/length resolution once Common Crawl secondary indexes were installed correctly and served with the correct local path configuration.

Remote execution is preferred for pointer resolution and WARC extraction because Common Crawl data are in-region and WARC byte-range fetching is network-bound. The committed config includes generic AWS defaults only. Personal AWS account values, bucket names, SSH key names, instance profiles, and `.pem` paths belong in ignored local config or local shell/SSH configuration.

PySpark is not part of the initial final implementation. The current Python pipeline is easier to audit and was cheap enough during pilot development. PySpark remains a fallback only if full-year collection proves too slow.

## 10. WARC HTML Extraction

For each resolved candidate, the pipeline fetches the relevant WARC byte range and extracts main HTML content.

Configured extraction settings:

- `favor_precision = true`
- `favor_recall = false`
- `include_tables = false`
- `resiliparse_comments = false`
- `resiliparse_post_meta = false`

Rationale:

- WARC extraction validates that the matched term appears in extracted main content, not only in WET plaintext.
- The extraction settings favor substantive prose over recall of marginal page fragments.
- Tables, comments, and post metadata are excluded because the target corpus is intended for discourse analysis over document prose.

Pilot validation showed that WARC extraction was the runtime bottleneck but operationally feasible at this scale.

## 11. Publication-Date Enrichment

Publication-date enrichment uses `htmldate` rather than a custom provenance/confidence hierarchy.

The wrapper applies:

- normalization;
- capture-time plausibility filtering;
- rejection of obvious invalid/default dates;
- minimum date: `1995-01-01`;
- capture-time tolerance: `48` hours.

Rationale:

- `htmldate` is purpose-built for publication-date recovery from HTML.
- The project does not need downstream provenance/confidence labels for publication-date candidates.
- Wrapper-level sanity checks prevent obvious defaults such as pre-web dates or invalid calendar artifacts from entering the dataset.

In the final pilot-dev document-quality run, publication timestamp coverage among fetch-success documents was `97.75%`.

## 12. Document-Quality Gate

The main corpus-quality threat is systematic page-type contamination: tag pages, directory pages, product/job pages, list pages, forum/listing artifacts, generated spam, and documents where the matched term is present but not used in substantive prose.

The final substantive gate is post-WARC document quality, not WET URL filtering.

Configured schema negative types:

- `SearchResultsPage`
- `CollectionPage`
- `ItemList`
- `OfferCatalog`
- `MusicAlbum`
- `MusicRecording`
- `Product`
- `JobPosting`
- `QAPage`

Configured text/substantiveness thresholds:

| Parameter | Value |
| --- | ---: |
| `sentence_min_tokens` | `6` |
| `sentence_min_chars` | `40` |
| `substantive_sentence_min_tokens` | `8` |
| `doc_min_substantive_sentences` | `3` |
| `min_extracted_text_chars` | `500` |
| `snippet_target_chars` | `200` |
| `snippet_max_chars` | `600` |
| `validation_sample_n` | `30` |

Configured DataTrove filters:

- `GopherRepetitionFilter`
- `GopherQualityFilter`
- `FineWebQualityFilter`

The FineWeb `line_punct_thr` parameter is set to `0.0`. During pilot validation, the default line-punctuation threshold was too sensitive to Trafilatura line segmentation and removed many plausible prose documents. Disabling only this threshold preserved the other Gopher/FineWeb checks while avoiding excessive recall loss.

The final choice to use DataTrove/Gopher/FineWeb-style filters replaced an earlier trajectory of adding increasingly specific hand-written page-type rules. This makes the pipeline more auditable and less overfit to individual validation samples.

## 13. English Filtering and Deduplication

After document-quality filtering, the pipeline retains English documents and deduplicates locally.

Configured language filter:

- provider: `py3langid`
- keep: `en`

Configured near-deduplication:

- n-gram size: `5`
- Jaccard threshold: `0.9`

DataTrove MinHash deduplication was considered but not adopted for the initial final collection. Local near-deduplication is simpler, sufficient for the pilot scale, and easier to audit. This decision can be revisited if full collection scale makes stronger deduplication necessary.

## 14. Output Layout and Reproducibility Artifacts

Working outputs:

- `data/interim/collection/trend/{year}/batch_001/`
- `data/interim/collection/corpus/{year}/batch_{NNN}/`

Final processed outputs:

- `data/processed/trend/`
- `data/processed/corpus/`
- `data/processed/manifests/`

Archived pilot-dev outputs:

- `data/interim/pilot-dev/`

For every final run, the following should be retained:

- config commit hash or config snapshot;
- year, crawl ID, track, and batch;
- WET manifest;
- URL export manifest;
- pointer-cache manifest;
- WARC extraction manifest;
- document-quality summary for corpus runs;
- throughput summary;
- processed-output manifest.

The final pipeline should be reproducible from:

- `configs/commoncrawl_collection.yaml`;
- committed code under `src/`;
- Makefile targets and CLI commands;
- saved manifests and run summaries.

## 15. Pilot-Dev Provenance

Pilot development is archived under `data/interim/pilot-dev/`. The purpose of the archive is not to reproduce every exploratory step forever, but to preserve the evidence behind the final design.

### 01: WET Smoke Test

Scope:

- `CC-MAIN-2016-44` and `CC-MAIN-2026-04`;
- `2` WET files per crawl;
- `157,461` scanned documents.

Outcome:

- WET-first scanning was technically viable and cheap.
- Raw term matching worked, including initial ASD disambiguation.
- Many retained hits were page chrome, navigation text, or low-value boilerplate.

Design consequence:

- Keep WET-first architecture, but add conservative WET triage.

### 02: WET Triage

Accepted design:

- keep only `signature_hard` and `directory_index`;
- drop broader soft boilerplate/listiness/commerce/topic-hub rules.

Evidence:

- `596` retained WET hits from `728` candidates;
- `132` removals;
- about `37.85` retained hits per `10,000` scanned documents.

Design consequence:

- WET triage is useful for obvious junk and cost control, but WET-only filtering should not be the final quality gate.

### 03: Baseline-Term Selection

Accepted comparator terms:

- `frustration`
- `sadness`
- `loneliness`

Rejected candidates:

- `worry`, because it was high-volume but commercially and generically contaminated;
- `tiredness`, because it was sparse and skewed toward symptom-list material.

Design consequence:

- Comparator terms were frozen for subsequent pilot and final collection work.

### 04: WARC Validation

Accepted workflow:

- export unique candidate URLs;
- resolve WARC pointers through local Common Crawl index server on EC2;
- fetch WARC byte ranges;
- extract main HTML content;
- keep hits whose matched term survives main-text extraction;
- apply English filtering and local deduplication.

Evidence:

- `1,895 / 1,899` URLs resolved;
- `1,895` fetch-success documents;
- `938` extraction-success documents;
- `1,126` WARC-validated row-level hits;
- `818` final English deduplicated documents before the later document-quality gate.

Design consequence:

- WARC-backed validation was feasible and cheap enough, but page-type contamination remained.

### 05: Document-Quality Gate

Accepted workflow:

- rerun the WET-to-WARC workflow on the same pilot-dev input frame;
- use URL denylist only as pre-WARC cost control;
- use `htmldate` for publication-date extraction;
- use schema guardrails plus DataTrove Gopher/FineWeb quality filters;
- apply English filtering and near-deduplication.

Evidence:

- `310,082` scanned documents;
- `2,682` WET candidate hits;
- `1,942` WET-validated hits;
- `862` WARC-validated row-level hits;
- `733` WARC-validated documents entering quality filtering;
- `507` documents kept after document quality;
- `492` English documents;
- `489` final deduplicated corpus documents;
- `165` final target hits;
- `403` final baseline hits;
- `97.75%` publication timestamp coverage among fetch-success documents.

Design consequence:

- The final collection pipeline uses the document-quality gate established here and accepts a small amount of residual junk rather than continuing to overfit sample-specific page-type rules.

## 16. Known Limitations and Residual Risks

The corpus is derived from public web crawl data. It inherits Common Crawl's coverage biases, crawl timing irregularities, domain skew, language-detection errors, and archival limitations.

Known limitations:

- Some prose-like spam, commercial listings, archive pages, and incidental mentions will survive.
- WET-stage URL filtering is conservative and intentionally not complete.
- Document-quality filters may remove some genuine prose.
- Retained documents are quality-gated for substantive web discourse but are not classified or balanced by page genre.
- Publication dates are inferred from archived HTML and may reflect page metadata rather than original authorship date.
- Common Crawl captures are not a representative sample of the whole web or of all communities discussing ADHD/autism.
- The final corpus is designed for research on web discourse patterns, not clinical prevalence, diagnosis, lived experience prevalence, or population-level health inference.

The pipeline intentionally avoids filtering by target-term centrality. Such filtering might raise apparent precision but would bias the very language around ADHD/autism that the project aims to study.

## 17. Intended and Non-Intended Uses

Intended uses:

- diachronic analysis of ADHD/autism mentions in general web discourse;
- comparison against broader affective-language baselines;
- semantic/discourse modelling over WARC-extracted document text;
- transparent reporting of collection rates and retention at WET and WARC stages.

Non-intended uses:

- estimating clinical prevalence;
- inferring individual diagnoses or user-level attributes;
- treating Common Crawl as demographically representative;
- deploying models trained on this corpus for high-stakes decisions;
- making claims that require complete capture of online discourse.

## 18. Config Decision Reference

| Area | Config value | Decision |
| --- | --- | --- |
| Randomness | `project.seed = 123` | Deterministic sampling and validation samples. |
| Active interim root | `data/interim/collection` | Working outputs for final collection runs. |
| Active processed root | `data/processed` | Final trend/corpus/manifests output root. |
| Primary window | `2014-2026` | Earliest practical full WET/WARC window chosen for this project. |
| Fallback window | `2016-2026` | Conservative fallback if early years underperform. |
| Crawl anchor | `04-15` | Stable yearly crawl selection near April. |
| Trend sample | `50` WET files/year | Fixed-effort denominator for trend rates. |
| Corpus target | `1000` target survivors/group/year | Ambitious modelling-corpus target. |
| Corpus soft minimum | `500` target survivors/group/year | Minimum acceptable yearly target volume. |
| Minimum text length | `500` chars | Remove very short fragments. |
| Domain cap | `50` | Limit domain concentration. |
| ASD window | `200` chars | Disambiguate acronym matches. |
| Context snippet | `200` chars | Compact WET context for inspection. |
| WET boilerplate window | `2000` chars | Limit boilerplate checks to local text window. |
| Resolver provider | `local_index_server` | Reliable WARC pointer resolution via local CC index server. |
| Resolver URL workers | `16` | Parallel URL lookup during pointer resolution. |
| WARC extraction | `favor_precision = true` | Prefer substantive prose extraction. |
| Tables | `include_tables = false` | Exclude table-heavy/non-prose content from extraction. |
| Published date minimum | `1995-01-01` | Reject implausible pre-web/default dates. |
| Published date tolerance | `48` hours | Plausibility window against capture time. |
| Schema negative types | search/list/product/job/music/Q&A types | Remove high-confidence non-document pages. |
| Minimum extracted text | `500` chars | Keep substantive WARC-extracted documents. |
| Substantive sentences | `3` | Require enough prose-like content. |
| Sentence snippet target | `200` chars | Secondary snippet export for manual inspection. |
| Sentence snippet max | `600` chars | Prevent oversized snippets. |
| DataTrove repetition | enabled | Remove heavily repeated/templated text. |
| DataTrove Gopher quality | enabled | Apply established web-corpus quality checks. |
| DataTrove FineWeb quality | enabled | Apply additional web-quality checks. |
| FineWeb line punctuation | `0.0` | Disabled because Trafilatura line segmentation made the default over-aggressive. |
| Language | `py3langid`, keep `en` | English-only corpus. |
| Near-dedup | 5-gram Jaccard `0.9` | Local, auditable near-duplicate removal. |

## 19. Maintenance

This document should be updated when any of the following change:

- active config values;
- term inventory;
- crawl-selection logic;
- WET or WARC filtering rules;
- document-quality filters;
- output layout;
- intended uses or known limitations.

Operational instructions belong in `reports/commoncrawl_collection_runbook.md`. This document should remain the canonical methodological and provenance record.
