# Stage 1b Freeze (Final after Iter_08 acceptance)

## Purpose of Stage 1b
Stage 1b is a **WET-based coarse triage** step for Common Crawl candidate extraction.
It is intentionally not a precision-grade boilerplate filter; it is a pragmatic pre-filter to cut obvious noise before downstream validation.

## Final Frozen Rule Stack
Only 2 boilerplate rules are kept:
1. `signature_hard`
  - **What it is:** a list of regular expressions that represent high-confidence UI/chrome fragments (cookie banners, accessibility widgets, ecommerce chrome, skip-links, etc.)
2. `directory_index`
  - **What it is:** 
    - a structural heuristic that tries to detect directory/index pages (e.g., category listings, navigation-heavy medical indexes)
    - besides lexicon hits it used signals such as separator density, low sentence-terminator rate (.!?), lots of title-case words, etc. 
    - each satisfied condition increments a score; the default score_threshold is 4

All other Stage 1b boilerplate rules were removed at freeze time. Removed filters included:
- signature_soft_* (soft UI tokens with min-hit threshold)
- Density rule (any “low-content density” heuristics)
- Generic listiness/taxonomy detection (listiness_*, “short fragments / separator density” as a standalone rule)
- Explicit condition-index detector (condition_index_* markers/thresholds)
- Navigation-lexicon density scoring (nav_lexicon_*)
- Commerce-page detection (commerce_*)
- Topic-hub/archive/category cleanup (topic_hub_*)

## Final Key Parameters
- `boilerplate.check_window_chars = 2000` (window used only for boilerplate checks)
- `filters.context_window_chars = 200` (stored context snippet window)
- `filters.min_chars = 500`
- `filters.domain_cap = 50`
- `filters.asd_disambiguation_window_chars = 200`

## Final Acceptance Artefacts (iter_08)
- `data/interim/stage1_pilot-dev/stage1b/iter_08/cc_scan_summary_20260220_135152.csv`
- `data/interim/stage1_pilot-dev/stage1b/iter_08/cc_scan_top_domains_20260220_135152.csv`
- `data/interim/stage1_pilot-dev/stage1b/iter_08/cc_removed_audit_20260220_135152.csv`
- `data/interim/stage1_pilot-dev/stage1b/iter_08/cc_val_sample50_20260220_135152.csv`
- `data/interim/stage1_pilot-dev/stage1b/iter_08/cc_val_asd_20260220_135152.csv`

## Repro Steps
Use the standard make targets:

```bash
make cc_stage1b_freeze_scan
make cc_stage1b_freeze_validate
```

## Stop-Tuning Rationale
Stop tuning Stage 1b here.
WET-only filtering has reached its practical precision ceiling for this stage (~50%); additional heuristic tuning adds complexity and hampers recall with diminishing precision returns.
Stage 1c will use **WARC + Trafilatura** as the authoritative boilerplate/content filter (more on that below)

## Known Limitations
- WET strips DOM/layout signals, limiting robust boilerplate detection.
- Some residual boilerplate/noise can remain in retained hits.
- Precision is bounded in this stage by text-only heuristics and coarse windows.

========================================================
# Stage 1c Plan
We are now launching **Stage 1c: WARC-based Generalisation, Validation & Enrichment**.

## Stage 1c — Objectives
1) **Hold-out generalisation test**
- Sample 4 WET files from a **new crawl year not used in Stage 1b** (we used 2016 and 2026 in Stage 1b).
- Apply frozen Stage 1b rules unchanged.
- Report candidate hit rate, validated hit rate, boilerplate removal rate vs the 2016/2026 baseline.

2) **Trafilatura as authoritative filter (WARC-based)**
- Architecture:
  - Stage 1: WET triage (cheap) removes only obvious junk
  - Stage 2: Fetch corresponding **WARC records** only for surviving candidates
  - Run **Trafilatura** with `favor_recall=False` to extract main content (DOM-aware)
  - Keep hits only if target terms occur in extracted main content

Principle: Trafilatura replaces WET for marginal boilerplate decisions.

3) **Data enrichment post WARC extraction**
For validated hits in the hold-out sample:
- Extract timestamps
    - Extract `capture_ts` from WARC-Date
    - Attempt `published_ts` extraction using hierarchy:
        - JSON-LD, OpenGraph/meta, <time> tags, URL patterns
        - Store source + confidence
- Store WARC pointers (possibly useful downstream):
  - `warc_filename`, `warc_offset`, `warc_length`

4) **Data polishing post WARC extraction**
- Language identification (English-only gating)
- De-duplication

## Stage 1c — Outputs
- Hold-out metrics table
- Cross-year stability assessment
- Trafilatura-filtered hit records
- Enriched hit records (capture_ts, WARC pointers, published_ts)
- Deduped and english-only text snippets
- Frozen config tagged: `v0.2-stage1c-freeze` (gate to Stage 2)
