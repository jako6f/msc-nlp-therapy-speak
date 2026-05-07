# msc-nlp-therapy-speak

Research repo for building a pilot NLP pipeline that samples and filters Common Crawl text related to mental‑health terms (e.g., ADHD/autism) for downstream analysis. The active workflow now covers Stage 1 WET scanning plus a rebooted Stage 1d WARC pipeline that uses a remote EC2 local-index-server resolver and remote WARC extraction in `us-east-1`.

**Status**: early pilot / scaffolding.

## Quickstart

```bash
conda env create -f environment.yml
conda activate msc-nlp
python -m src.cli cc-scan --config configs/stage1c_freeze.yaml
python -m src.cli cc-stage1d-export-urls --config configs/stage1d_freeze.yaml
python -m src.cli cc-stage1d-upload-urls --config configs/stage1d_freeze.yaml
```

For the frozen Stage 1d workflow summary, see:

- [data/interim/stage1_pilot-dev/stage1d/README_stage1d_freeze.md](/Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak/data/interim/stage1_pilot-dev/stage1d/README_stage1d_freeze.md)

## Configuration

Stage-scoped configs are kept separately. The current repo state includes:

- `configs/stage1b_freeze.yaml` — historical Stage 1b freeze reproduction
- `configs/stage1c_freeze.yaml` — active Stage 1c freeze workflow
- `configs/stage1d_freeze.yaml` — active Stage 1d freeze workflow

All stage configs define:

- Common Crawl IDs to sample (e.g., `CC-MAIN-YYYY-WW`)
- Sampling volume (WET files per crawl)
- Simple filters (minimum chars, per-domain cap, disambiguation window)
- Term patterns for ADHD/autism matching
- Baseline term patterns for the current selected baseline set (`frustration`, `sadness`, `loneliness`)
- Output routing for Stage-1 pilot/dev artifacts via:
  - `paths.interim_base`
  - `paths.stage1_base`
  - `run_context.stage` / `run_context.track`

## Project Layout

- `src/cli.py` — stage-scoped CLI entrypoints for sampling, download, scan, validation, remote URL export/upload, remote index setup, remote pointer resolution, remote extraction, and English-only dedup filtering
- `src/data_sources/commoncrawl/` — Common Crawl sampling, scanning, remote pointer-resolution helpers, and Stage 1d WARC-processing logic
- `configs/` — stage-scoped YAML configs plus small historical notes
- `notebooks/` — exploratory notebooks
- `data/` — data outputs (not committed)

Stage-1 outputs now live under `data/interim/stage1_pilot-dev/{stage1a,stage1b,stage1c,stage1d}`.
Within scan summaries, `validated_hits_wet` and `validated_hits_wet_per_10k` are the canonical WET-validated metrics. Historical Stage 1b summaries may still contain legacy aliases such as `final_hits`, `hits_total`, and `final_per_10k`, but new runs do not emit them.
Stage 1c scan outputs also persist row-level `candidate_hits` and `validated_hits_wet` parquet tables plus a per-term diagnostics CSV.
Stage 1d adds:

- URL-export and pointer-cache artifacts under `data/interim/stage1_pilot-dev/stage1d/{url_exports,pointer_cache}/`
- row-level `cc_enriched_hits`, `cc_validated_hits_warc`, `cc_filtered_hits_en_dedup`, and `cc_corpus_texts_en_dedup` parquet outputs
- post-filter `cc_val_sample30` validation sample under `data/interim/stage1_pilot-dev/stage1d/filter_en_dedup/`
- Stage 1d summary CSVs

## Notes

- This is a research prototype. Expect breaking changes.
- No code behavior has been altered by this README.
