# msc-nlp-therapy-speak

Research repo for building a pilot NLP pipeline that samples and filters Common Crawl text related to mental‑health terms (e.g., ADHD/autism) for downstream analysis. This repo currently contains a minimal CLI and configuration to validate data access, sampling, and Stage 1 scan diagnostics.

**Status**: early pilot / scaffolding.

## Quickstart

```bash
conda env create -f environment.yml
conda activate msc-nlp
python -m src.cli cc-scan --config configs/stage1c_freeze.yaml
```

## Configuration

Stage-scoped configs are kept separately. The current repo state includes:

- `configs/stage1b_freeze.yaml` — historical Stage 1b freeze reproduction
- `configs/stage1c_freeze.yaml` — active Stage 1c freeze workflow

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

- `src/cli.py` — minimal CLI that loads a config and prints key fields
- `src/data_sources/commoncrawl/` — placeholder package for Common Crawl logic
- `configs/` — stage-scoped YAML configs plus small historical notes
- `notebooks/` — exploratory notebooks
- `data/` — data outputs (not committed)

Stage-1 outputs now live under `data/interim/stage1_pilot-dev/{stage1a,stage1b,stage1c}`.
Within scan summaries, `validated_hits_wet` and `validated_hits_wet_per_10k` are the canonical WET-validated metrics. Historical Stage 1b summaries may still contain legacy aliases such as `final_hits`, `hits_total`, and `final_per_10k`, but new runs do not emit them.
Stage 1c scan outputs also persist row-level `candidate_hits` and `validated_hits_wet` parquet tables plus a per-term diagnostics CSV.
WARC validation metrics (`validated_hits_warc`, `validated_hits_warc_per_10k`, `warc_validation_attempted`, `warc_validation_notes`) are placeholder schema fields until Stage 1c/Stage 2 WARC validation is enabled.

## Notes

- This is a research prototype. Expect breaking changes.
- No code behavior has been altered by this README.
