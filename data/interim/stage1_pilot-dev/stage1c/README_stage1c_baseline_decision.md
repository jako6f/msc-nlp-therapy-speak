# Stage 1c Baseline Decision Note

## Purpose
Freeze the final baseline-term choice after the Stage 1c WET-only scan and frozen Stage 1b boilerplate triage pass.

## Evidence base
Decision based on the Stage 1c smoke run outputs:
- `cc_scan_summary_20260501_135747.csv`
- `cc_term_summary_20260501_135747.csv`
- `cc_scan_top_domains_by_term_20260501_135747.csv`
- `cc_removed_audit_20260501_135747.csv`
- `cc_val_sample50_20260501_135747.csv`
- `cc_candidate_hits_20260501_135747.parquet`
- `cc_validated_hits_wet_20260501_135747.parquet`

Pilot input scope remained the Stage 1 anchor WET files only:
- `CC-MAIN-2016-44`
- `CC-MAIN-2026-04`

Frozen WET triage remained unchanged from Stage 1b:
- `signature_hard`
- `directory_index`

No WARC validation was run in Stage 1c.

## Selected baseline terms
Use these three baseline terms going forward:
1. `frustration`
2. `sadness`
3. `loneliness`

Selected config:
- `configs/commoncrawl_collection.yaml`

## Why these three
### `frustration`
- Strongest overall candidate.
- Coverage is solid in both slices: `215` validated hits in `CC-MAIN-2016-44`, `69` in `CC-MAIN-2026-04`.
- Combined counts: `309` candidate, `284` validated.
- Removal rate is low: `8.09%`.
- Surviving examples are mostly substantive discourse uses rather than generic UI or boilerplate phrasing.
- The validated rate is in a reasonable range relative to the target terms, so it is usable as a comparator without overwhelming the sample.

### `sadness`
- Adequate old and recent coverage: `128` validated hits in `CC-MAIN-2016-44`, `31` in `CC-MAIN-2026-04`.
- Combined counts: `177` candidate, `159` validated.
- Removal rate is still acceptable: `10.17%`.
- Semantically it behaves like a usable negative-affect comparator.
- Caveat: some residual noise remains from song titles, lyrics, and a small amount of low-quality adult-content spillover.

### `loneliness`
- Smaller than `frustration` and `sadness`, but still present in both slices: `52` validated hits in `CC-MAIN-2016-44`, `31` in `CC-MAIN-2026-04`.
- Combined counts: `91` candidate, `83` validated.
- Removal rate is low: `8.79%`.
- Qualitatively it is the cleanest term in the set: surviving contexts are more concept-like and less UI/commercial than `worry`.
- Domain concentration is modest, which supports use as a comparator despite lower volume.

## Excluded candidates
### `worry`
- Excluded from the core final set despite very high coverage.
- Combined counts: `1857` candidate, `1566` validated.
- Removal rate is the highest among baseline candidates: `15.67%`.
- Strong residual mismatch after triage:
  - many surviving uses are generic reassurance or commerce phrasing (`don't worry`, `worry-free`, signup / booking / product reassurance);
  - top validated domains include `pandora.com`, `booking.com`, `cheaperthandirt.com`, and `cbssports.com`;
  - domain-cap removals are much higher than for the other baseline terms (`40` removed by domain cap).
- Conclusion: the term is too generic and commercially contaminated to serve as the main semantic-drift baseline.

### `tiredness`
- Excluded due to weak volume and weaker comparator quality.
- Combined counts: `37` candidate, `34` validated.
- Coverage is low in both slices: `24` validated hits in `CC-MAIN-2016-44`, `10` in `CC-MAIN-2026-04`.
- Surviving contexts skew toward symptom lists, health Q&A, and medicalized somatic discourse.
- Conclusion: too sparse and too clinically/symptom oriented for the intended baseline role.

## Practical implication for Stage 1d
- Stage 1d should use `frustration`, `sadness`, and `loneliness` as the baseline set for smoke testing.
- `validated_hits_warc` remains reserved for Stage 1d onward.
- This Stage 1c decision does not change the frozen Stage 1b WET triage stack.

## Residual limitations
- The evidence base is still only the 4-file Stage 1 pilot slice.
- `sadness` retains some title/tag noise.
- `loneliness` is viable but lower-volume than `frustration` and `sadness`.
- The final decision should still be treated as a Stage 1 pilot-dev freeze, not full Stage 2 proof.
