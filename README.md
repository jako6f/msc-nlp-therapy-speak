# msc-nlp-therapy-speak

This repository builds a Common Crawl corpus for diachronic NLP analysis of public web
discourse around ADHD and autism, with matched baseline emotion terms for comparison.

The repository is organized around a final collection pipeline. Earlier pilot-dev work is
kept as archived evidence under `data/interim/pilot-dev/`, but the active code, config,
and documentation are for the full collection run.

## What This Pipeline Does

The pipeline starts from yearly Common Crawl WET files, finds candidate term hits, resolves
their corresponding WARC HTML records, extracts readable document text, applies document
quality gates, and writes two downstream products:

- `trend`: fixed-size yearly samples for diachronic rate estimates.
- `corpus`: larger yearly samples for target-term semantic and contextual analysis.

The target groups are `adhd` and `autism`. The current baseline group contains negative
emotion terms used as a comparison track.

## What This Pipeline Does NOT Do

The pipeline does not attempt to classify retained documents by web page or genre type.
Its focus is a quality-gated corpus of substantive web documents rather than a
genre-controlled sample of articles, forums, blogs, or other page classes.

## Pipeline Overview

```text
Common Crawl crawl map
        |
        v
Sample yearly WET files
        |
        v
Download WET files
        |
        v
Scan WET text for target/baseline hits
        |
        v
Export candidate URLs
        |
        v
Resolve WARC pointers with local Common Crawl index server
        |
        v
Fetch WARC HTML records
        |
        v
Extract main text and candidate publication dates
        |
        v
Apply document quality gates, English filtering, and local deduplication
        |
        v
Build processed trend and corpus outputs
```

The WET stage is used for efficient coarse search. The WARC stage is the substantive
validation point because it retrieves the original HTML record and re-extracts document
text before final quality filtering.

## Key Files

- `configs/commoncrawl_collection.yaml`: active collection configuration. It defines the
  crawl map, sampling settings, term patterns, WET filters, WARC extraction settings,
  document-quality gates, deduplication settings, output paths, and generic AWS/EC2
  defaults.
- `configs/local/aws.example.yaml`: template for private AWS/S3 settings.
- `configs/local/aws.yaml`: untracked local override file. This is where account-specific
  bucket, prefix, SSH key, or EC2 details belong.
- `reports/commoncrawl_corpus_design_and_provenance.md`: public design and provenance
  record. Read this for the rationale behind corpus scope, crawl selection, filtering,
  validation, and known tradeoffs.
- `reports/commoncrawl_single_year_runbook.md`: operational guide for one year, one track,
  and one batch.
- `reports/commoncrawl_all_years_runbook.md`: operational guide for all configured years.
- `reports/commoncrawl_collection_crawl_map.json`: generated record of the frozen crawl map.
- `environment.yml`: conda environment specification.
- `AGENTS.md`: repository-level working instructions for Codex-style coding agents.

## Repository Structure

```text
configs/
  commoncrawl_collection.yaml      Active public collection config
  local/                           Untracked private local overrides

data/
  interim/
    collection/                    Working and S3-synced collection artifacts
    pilot-dev/                     Archived pilot-dev outputs and closeout notes
  processed/
    trend/                         Processed trend outputs
    corpus/                        Processed corpus outputs
    manifests/                     Processed run manifests

notebooks/                         Lightweight inspection and sanity notebooks
paper/                             Thesis/proposal LaTeX materials
reports/                           Runbooks, provenance docs, and crawl-map records
src/
  cli.py                           Command-line entry point
  pathing.py                       Shared output path helpers
  data_sources/commoncrawl/        Common Crawl acquisition, scan, WARC, and quality code
```

Generated data products are not moved through GitHub. Remote collection runs upload data
artifacts to S3, and local machines sync those artifacts back from S3.

## How To Operate The Pipeline

Use the runbooks rather than this README for execution details:

- Start with [the single-year runbook](reports/commoncrawl_single_year_runbook.md) for
  smoke tests, targeted reruns, or corpus expansion batches.
- Use [the all-years runbook](reports/commoncrawl_all_years_runbook.md) after the
  single-year path has been validated.

Those runbooks cover EC2 setup, GitHub synchronization, local AWS config, preflight checks,
tmux usage, S3 sync-back, output inspection, and failure recovery.

## Design And Provenance

For methodological details, see
[reports/commoncrawl_corpus_design_and_provenance.md](reports/commoncrawl_corpus_design_and_provenance.md).

That document records the important design and configuration decisions: crawl-window
choice, crawl-map pinning, WET-first acquisition, WARC validation, publication-date
recovery, page-quality filtering, deduplication, trend/corpus separation, and pilot-dev
lessons that shaped the final pipeline.
