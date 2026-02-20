# Stage 1b Freeze (Final after Iter_08 acceptance)

## Purpose of Stage 1b
Stage 1b is a **WET-based coarse triage** step for Common Crawl candidate extraction.
It is intentionally not a precision-grade boilerplate filter; it is a pragmatic pre-filter to cut obvious noise before downstream validation.

## Final Frozen Rule Stack
Only 2 boilerplate rules are kept:
1. `signature_hard`
2. `directory_index`

All other Stage 1b boilerplate rules were removed at freeze time.

## Final Key Parameters
- `boilerplate.check_window_chars = 2000` (window used only for boilerplate checks)
- `filters.context_window_chars = 200` (stored context snippet window)
- `filters.min_chars = 500`
- `filters.domain_cap = 50`
- `filters.asd_disambiguation_window_chars = 200`

## What Iter_08 Demonstrates
Final acceptance is stable under the frozen stack.
Removing `topic_hub` did not change acceptance outcomes versus the immediately prior acceptance candidate (formerly `iter_08b` before folder rename).

## Final Acceptance Artefacts (iter_08)
- `data/interim/cc_pilot_stage_01b/iter_08/cc_scan_summary_20260220_135152.csv`
- `data/interim/cc_pilot_stage_01b/iter_08/cc_scan_top_domains_20260220_135152.csv`
- `data/interim/cc_pilot_stage_01b/iter_08/cc_removed_audit_20260220_135152.csv`
- `data/interim/cc_pilot_stage_01b/iter_08/cc_val_sample50_20260220_135152.csv`
- `data/interim/cc_pilot_stage_01b/iter_08/cc_val_asd_20260220_135152.csv`

## Repro Steps
Use the standard make targets:

```bash
make cc_pilot_scan
make cc_pilot_validate
```

## Stop-Tuning Rationale
Stop tuning Stage 1b here.
WET-only filtering has reached its practical precision ceiling for this stage; additional heuristic tuning adds complexity with diminishing returns.
Stage 1c will use **WARC + Trafilatura** as the authoritative boilerplate/content filter.

## Known Limitations
- WET strips DOM/layout signals, limiting robust boilerplate detection.
- Some residual boilerplate/noise can remain in retained hits.
- Precision is bounded in this stage by text-only heuristics and coarse windows.
