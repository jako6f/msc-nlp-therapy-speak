# Document-Quality Gate Closeout

## Purpose
This pass reran the WET-to-WARC workflow on the same pilot-dev input frame and added a final document-quality gate. The aim was to improve corpus precision without filtering by target-term centrality, because the research design requires observing language around ADHD/autism rather than preselecting only documents that discuss those terms centrally.

## Input Scope
- Same WET input frame as the WARC-validation pass, enabling direct comparison.
- Final WET scan run: `20260511_092845`
- Final WARC extraction run: `20260511_100811`
- Final document-quality run: `20260511_131035`

## Final Workflow
1. Rerun WET scanning with the frozen target and comparator terms.
2. Use WET-stage URL denylist rules only as a cheap pre-WARC cost-control measure.
3. Export unique WARC URL candidates and resolve pointers on EC2.
4. Fetch WARC byte ranges and extract main HTML content.
5. Extract candidate publication dates with `htmldate`, wrapped with capture-time plausibility and sanity checks.
6. Apply a document-quality gate using schema guardrails plus DataTrove Gopher/FineWeb quality filters.
7. Apply English-only filtering and local near-deduplication.
8. Write summaries, throughput metrics, final text outputs, and a deterministic manual-validation sample.

## Final Quality Gate
The final gate uses:

- JSON-LD/schema guardrails for high-confidence non-document page types.
- DataTrove `GopherRepetitionFilter` to remove heavily repeated or templated text.
- DataTrove `GopherQualityFilter` to remove very short, malformed, low-stopword, bullet-heavy, or symbol-heavy documents.
- DataTrove `FineWebQualityFilter`, with `line_punct_thr: 0.0`.

The FineWeb line-punctuation threshold was disabled because Trafilatura line segmentation made the default threshold too aggressive for this corpus. Disabling only that threshold preserved the other established Gopher/FineWeb checks while avoiding large recall loss.

## Key Results
- Documents scanned: `310,082`
- WET candidate hits: `2,682`
- WET-validated hits: `1,942`
- WARC-validated row-level hits: `862`
- WARC-validated documents entering document quality: `733`
- Documents kept after document quality: `507`
- English documents: `492`
- Final deduplicated corpus documents: `489`
- Final target hits: `165`
- Final baseline hits: `403`
- Publication timestamp coverage among fetch-success documents: `97.75%`
- WET scan throughput: about `1,478` documents per second
- WARC extraction throughput: about `3.14` documents per second
- Document-quality throughput: about `15.59` documents per second

## Design Rationale
The key methodological decision was to replace increasingly specific page-type heuristics with an auditable subset of established web-corpus quality filters. This reduced overfitting to individual validation samples and made the final collection design easier to justify externally.

The remaining URL denylist is retained only to reduce unnecessary WARC extraction cost. It is not treated as the substantive quality gate.

## Manual Validation
The final deterministic sample is:

```text
document_quality/cc_val_sample30_20260511_131035.csv
```

Manual inspection showed the best pilot-dev quality achieved so far. The output is still not perfectly clean, but the remaining noise level was accepted for moving from pilot development to the full collection run.

## Limitations
- Some prose-like spam, commercial listings, archive pages, and incidental term mentions still survive.
- The pipeline intentionally avoids centrality filtering because that would bias the linguistic evidence around the target terms.
- WARC extraction is the runtime bottleneck and should determine batch sizing for the full collection run.

## Outcome
This pass closed pilot development. The final collection pipeline reuses the WET-first scan, WARC-backed extraction, `htmldate` publication-date recovery, DataTrove document-quality gate, English filtering, and local near-deduplication established here.
