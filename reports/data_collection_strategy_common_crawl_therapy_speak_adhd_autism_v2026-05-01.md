# Data Collection Strategy - Common Crawl Therapy-Speak Corpus

**Project:** MSc dissertation, diachronic NLP / concept creep
**Status:** Final collection design after pilot-dev validation
**Pilot-dev archival tag:** `v0.3-pilot-dev`

## 1. Research Objective

The project studies how ADHD and autism language appears in general web discourse over time, and whether the usage/context of these target terms shifts in ways that exceed broader background drift in non-clinical negative language.

The final collection therefore has two linked aims:

1. Estimate diachronic mention rates for ADHD/autism and matched baseline terms in Common Crawl.
2. Build a high-quality modelling corpus of substantive WARC-extracted documents for downstream semantic and discourse analysis.

The frozen pilot-dev work selected the baseline comparator terms:

- `frustration`
- `sadness`
- `loneliness`

The target terms remain:

- ADHD and robust ADHD variants
- autism, autistic, autism-spectrum variants
- ASD only when autism appears nearby, to avoid ambiguous acronym matches

## 2. Core Pipeline

The final pipeline keeps the pilot-dev architecture because it proved efficient and sufficiently auditable:

1. **WET-first scan:** scan plaintext WET records cheaply and keep stable denominators.
2. **Conservative WET triage:** remove obvious negative page types before WARC work to reduce cost.
3. **WARC pointer resolution:** resolve selected WET hits to WARC offsets using the local Common Crawl index server on EC2.
4. **WARC HTML extraction:** fetch only selected WARC records and extract main text with the precision-leaning extraction stack.
5. **Publication-date enrichment:** infer publication dates from HTML using `htmldate`, with capture-time plausibility and sanity filters.
6. **Document-quality gate:** apply local, auditable Gopher/FineWeb-style quality metrics via DataTrove plus project-specific text-shape safeguards.
7. **English + dedup:** retain English documents and deduplicate within the final working corpus.
8. **Processed outputs:** write final trend and corpus outputs under `data/processed/`.

The old pilot-dev outputs remain only as archived evidence under `data/interim/pilot-dev/`.

## 3. Time Window And Crawl Selection

Primary window:

- 2014-2026
- Most recent crawl in scope: `CC-MAIN-2026-17`

Fallback window:

- 2016-2026

Rationale:

By 2014, Common Crawl had multi-billion-page crawls with WARC/WET/WAT file lists and WET sizes of several TiB per crawl. Common Crawl has older data, including a 2012 corpus with 3.8 billion documents, but early crawls used legacy formats and are less convenient for this WET-first/WARC-enrichment pipeline. Common Crawl notes that early crawls used ARC rather than WARC, and the 2012 release was in ARC format.

The crawl map is frozen in `configs/commoncrawl_collection.yaml`. One crawl is selected per year using the official Common Crawl index collection list and a stable April 15 anchor. For each year, the selected crawl is the available `CC-MAIN` crawl whose `from`/`to` midpoint is closest to April 15, with WET path, WARC path, and CDX index availability checked before selection. The explicit map is stored in config so later collection runs do not drift if Common Crawl metadata changes. The auditable selection manifest is versioned at `reports/commoncrawl_collection_crawl_map.json`.

## 4. Output Tracks

### Trend

Purpose:

- Estimate rates over time with fixed effort per year.

Method:

- Use a fixed deterministic WET-file sample per year.
- Process target and baseline terms through the same WET scan, WARC enrichment, and document-quality rules.
- Report all three trend rates:
  - `validated_hits_wet / docs_scanned`
  - `validated_hits_warc / docs_scanned`
  - `validated_hits_warc / validated_hits_wet`

Interpretation:

- WET rates are cheaper and have the cleanest denominator.
- WARC rates are cleaner but more expensive.
- WARC/WET retention quantifies how much the stricter document-quality layer changes the trend signal.

### Corpus

Purpose:

- Build a larger high-quality modelling corpus for semantic/discourse analysis.

Method:

- Run yearly crawl batches iteratively.
- Keep scanning until the target-term survivor goal is met or AWS Budget alerts/runtime limits indicate that collection should pause.
- Use the same core filters as the Trend output, but continue in additional WET batches when yield is insufficient.

Target:

- Desired target-term survivors per year: `M = 1000`
- Soft minimum per year: `500`
- Cost control: monitor spend with AWS Budgets rather than an in-pipeline dollar-denominated stop condition.

The pipeline is intentionally iterative: if cost and time remain acceptable, additional yearly corpus batches can be run later to raise `M`.

## 5. Scale And Execution Policy

The final collection should run primarily on EC2 in `us-east-1`.

Reasons:

- Common Crawl data are in-region.
- Prior pilot-dev WARC extraction was cheap.
- Running acquire, scan, resolve, extract, and quality steps in one environment is simpler to reproduce than splitting local and remote execution.

PySpark is not part of the first final implementation.

Reason:

- The expected bottleneck is not large distributed WET throughput yet.
- The current Python pipeline is easier to audit and has already produced workable costs.
- PySpark remains a fallback if full-year batches prove too slow.

## 6. Quality Philosophy

The main threat to the corpus is not a few residual junk pages. The core threat is systematic page-type contamination: tag pages, directory pages, list pages, product/job pages, forum/listing artifacts, generated spam, and other documents where the matched term is present but not used in substantive prose.

The final collection therefore uses two layers:

- A cheap URL/page-type denylist during WET triage, mainly to avoid wasting WARC extraction on obvious non-documents.
- A substantive post-WARC document-quality gate based on Gopher/FineWeb-style metrics, DataTrove filters, and compact project-specific text-shape checks.

This is intentionally more robust than repeatedly adding sample-specific URL patterns.

## 7. Naming And Repository Layout

Final active pipeline names avoid pilot-stage terminology.

Active config:

- `configs/commoncrawl_collection.yaml`

Active Makefile targets:

- `collection_*` for individual steps
- `trend_year` / `corpus_year` for yearly batches
- `trend` / `corpus` for local multi-year preparation

Working outputs:

- `data/interim/collection/trend_working/`
- `data/interim/collection/corpus_working/`

Final outputs:

- `data/processed/trend/`
- `data/processed/corpus/`
- `data/processed/manifests/`

Archived pilot-dev outputs:

- `data/interim/pilot-dev/`

## 8. Reproducibility Requirements

For every final run, retain:

- config snapshot or config commit hash
- year, crawl ID, track, and batch
- WET manifest
- URL export manifest
- pointer-cache manifest
- WARC extraction manifest
- document-quality summary
- throughput summary
- processed-output manifest

The final pipeline should be reproducible from `configs/commoncrawl_collection.yaml`, the committed code, and the saved WET manifests.

## 9. Current Decision Record

- Use 2014-2026 as the primary window.
- Use 2016-2026 as the conservative fallback window if early years underperform.
- Run by yearly crawl batches.
- Report both WET and WARC trend rates.
- Do not add a separate baseline command; baseline terms are processed within the same Trend and Corpus scans.
- Do not switch deduplication to DataTrove MinHash for the initial full collection.
- Do not add PySpark unless the Python pipeline becomes a demonstrated throughput bottleneck.
- Keep full extracted text in the working corpus and include sentence-based context snippets as secondary columns.
- Keep personal AWS account details in untracked local config, not in the committed collection config.
