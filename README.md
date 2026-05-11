# msc-nlp-therapy-speak

Research repository for collecting a Common Crawl corpus of ADHD/autism discourse and matched baseline terms for diachronic NLP analysis.

**Status:** final collection pipeline after pilot-dev validation. Pilot-dev artifacts are archived under `data/interim/pilot-dev/` and the pilot-dev code/data checkpoint is tagged as `v0.3-pilot-dev`.

## Quickstart

```bash
conda env create -f environment.yml
conda activate msc-nlp
make sanity
make collection_select_crawls
```

Process one yearly Trend batch:

```bash
make trend_year YEAR=2020
```

Process one yearly Corpus batch:

```bash
make corpus_year YEAR=2020 BATCH=1
```

See [reports/commoncrawl_collection_runbook.md](/Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak/reports/commoncrawl_collection_runbook.md) for the full local/EC2 execution sequence.

## Active Configuration

The active collection config is:

- `configs/commoncrawl_collection.yaml`

It defines:

- the frozen 2014-2026 year-to-crawl map
- the 2016-2026 conservative fallback window
- Trend and Corpus sampling settings
- target and baseline term patterns
- WET triage rules
- WARC extraction settings
- publication-date extraction settings
- document-quality, English filtering, and dedup settings
- generic AWS/S3/EC2 resolver defaults; personal account values belong in untracked `configs/local/aws.yaml`

Optional local override:

```bash
cp configs/local/aws.example.yaml configs/local/aws.yaml
```

`configs/local/aws.yaml` is ignored by git and is merged automatically when present. You can also point to another local override with `MSC_NLP_LOCAL_CONFIG=/path/to/local.yaml`.

## Active Commands

Individual steps:

- `make collection_sample`
- `make collection_download`
- `make collection_scan`
- `make collection_export_urls`
- `make collection_upload_urls`
- `make collection_install_indexes`
- `make collection_start_index_server`
- `make collection_resolve`
- `make collection_extract`
- `make collection_quality`
- `make collection_build_processed`

Convenience targets:

- `make trend_year YEAR=YYYY`
- `make corpus_year YEAR=YYYY BATCH=N`
- `make corpus_expand YEAR=YYYY BATCH=N`
- `make trend`
- `make corpus`

Remote pointer resolution and WARC extraction require explicit S3 URIs and should be run on the EC2 host in `us-east-1`; see the runbook.

## Data Layout

Working outputs:

- `data/interim/collection/trend_working/`
- `data/interim/collection/corpus_working/`

Final processed outputs:

- `data/processed/trend/`
- `data/processed/corpus/`
- `data/processed/manifests/`

Archived pilot-dev outputs:

- `data/interim/pilot-dev/`

## Strategy

The collection strategy is documented in:

- [reports/data_collection_strategy_common_crawl_therapy_speak_adhd_autism_v2026-05-01.md](/Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak/reports/data_collection_strategy_common_crawl_therapy_speak_adhd_autism_v2026-05-01.md)
