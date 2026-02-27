# msc-nlp-therapy-speak

Research repo for building a pilot NLP pipeline that samples and filters Common Crawl text related to mental‑health terms (e.g., ADHD/autism) for downstream analysis. This repo currently contains a minimal CLI and configuration to validate data access, sampling, and pilot scan exports for the Appendix.

**Status**: early pilot / scaffolding.

## Quickstart

```bash
conda env create -f environment.yml
conda activate msc-nlp
python src/cli.py --config configs/pilot.yaml
```

## Configuration

The default config is `configs/pilot.yaml`. It defines:

- Common Crawl IDs to sample (e.g., `CC-MAIN-YYYY-WW`)
- Sampling volume (WET files per crawl)
- Simple filters (minimum chars, per-domain cap, disambiguation window)
- Term patterns for ADHD/autism matching
- Output routing for Stage-1 pilot/dev artifacts via:
  - `paths.interim_base`
  - `paths.stage1_base`
  - `paths.reports_figures_base`
  - `paths.reports_tables_base`
  - `run_context.stage` / `run_context.track`

## Project Layout

- `src/cli.py` — minimal CLI that loads a config and prints key fields
- `src/data_sources/commoncrawl/` — placeholder package for Common Crawl logic
- `configs/` — YAML configs for pilot runs
- `notebooks/` — exploratory notebooks
- `data/` — data outputs (not committed)
- `tests/` — test scaffolding (empty)

Stage-1 outputs now live under `data/interim/stage1_pilot-dev/{stage1a,stage1b,stage1c}`.
Generated report artifacts are namespaced under `reports/figures/{stage1_pilot-dev,trend,corpus}` and `reports/tables/{stage1_pilot-dev,trend,corpus}`.
Within scan summaries, `validated_hits_wet` is the canonical WET-validated hit metric; `final_hits` is retained as a deprecated alias for backward compatibility.
WARC validation metrics (`validated_hits_warc`, `validated_hits_warc_per_10k`, `warc_validation_attempted`, `warc_validation_notes`) are placeholder schema fields until Stage 1c/Stage 2 WARC validation is enabled.

## Notes

- This is a research prototype. Expect breaking changes.
- No code behavior has been altered by this README.
