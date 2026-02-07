# msc-nlp-therapy-speak

Research repo for building a pilot NLP pipeline that samples and filters Common Crawl text related to mental‑health terms (e.g., ADHD/autism) for downstream analysis. This repo currently contains a minimal CLI and configuration to validate data access and sampling.

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

## Project Layout

- `src/cli.py` — minimal CLI that loads a config and prints key fields
- `src/data_sources/commoncrawl/` — placeholder package for Common Crawl logic
- `configs/` — YAML configs for pilot runs
- `notebooks/` — exploratory notebooks
- `data/` — data outputs (not committed)
- `tests/` — test scaffolding (empty)

## Notes

- This is a research prototype. Expect breaking changes.
- No code behavior has been altered by this README.
