# Stage 1e Freeze

## Purpose
Stage 1e is the frozen Common Crawl pilot cleanup pass that reruns the Stage 1d WARC-validation workflow on the same WET input slice, then applies a stricter document-quality gate before English filtering and local deduplication.

The goal was to improve corpus precision without tuning on target-term centrality. Stage 1e therefore filters documents by page/text quality rather than by how centrally they discuss ADHD/autism or the baseline terms.

## Frozen Pipeline
1. Run the Stage 1e WET scan on the same pilot-dev WET files used for the Stage 1d comparison frame.
2. Apply the WET-stage URL denylist only as a cheap cost-control prefilter before WARC extraction.
3. Export unique WARC URL candidates and resolve WARC pointers on EC2 with the local Common Crawl index server.
4. Fetch WARC byte ranges on EC2 and extract main HTML content with the Stage 1e WARC extractor.
5. Apply the Stage 1e document-quality gate:
   - schema guardrails for high-confidence non-document page types;
   - DataTrove `GopherRepetitionFilter`;
   - DataTrove `GopherQualityFilter`;
   - DataTrove `FineWebQualityFilter` with `line_punct_thr: 0.0`.
6. Apply English-only filtering and the existing local exact/near-dedup step.
7. Export the final corpus, summaries, throughput metrics, and deterministic sample30 for manual validation.

## Final Configuration Decision
The final document-quality gate uses DataTrove/Gopher filters directly rather than a repo-local hand-rolled text-shape rule stack. This is methodologically cleaner and easier to justify for Stage 2.

The default FineWeb line-punctuation rule was disabled via:

```yaml
stage1e:
  document_quality:
    datatrove:
      fineweb_quality:
        enabled: true
        line_punct_thr: 0.0
```

This rule was too sensitive to Trafilatura line segmentation in this pipeline. With the default threshold it removed many plausible prose documents and reduced the final corpus from 489 to 301 documents. Disabling only this threshold retained the other Gopher/FineWeb checks while avoiding that over-filtering.

## Outputs
- `wet_scan/` - Stage 1e WET scan, removal audit, term summaries, and validation samples.
- `url_exports/` - Stage 1e URL exports uploaded for remote pointer resolution.
- `pointer_cache/` - local pointer-cache summaries if synced from the EC2 resolver.
- `warc/` - WARC extraction/enrichment outputs and WARC-stage summaries.
- `document_quality/` - final Stage 1e document-quality outputs, final corpus text table, summaries, and validation sample.
- `metrics/` - compact throughput summary for Stage 2 runtime planning.

Final freeze run IDs:
- WET scan: `20260511_092845`
- URL export: `20260511_093546`
- WARC extraction: `20260511_100811`
- Document quality: `20260511_131035`

## Final Results
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

Throughput summary:
- WET scan: `1,477.94 docs/sec`
- WARC extraction: `3.14 docs/sec`
- Document quality: `15.59 docs/sec`
- WARC extraction remains the Stage 2 bottleneck.

## Manual Validation
The final deterministic sample is:

```text
document_quality/cc_val_sample30_20260511_131035.csv
```

Manual inspection showed that the final Stage 1e output is the best pilot-dev corpus candidate so far, but not perfectly clean. It removes many repeated/list-like/low-quality pages while preserving substantially more target material than the default FineWeb run. Remaining known failure modes include prose-like spam, service directories, commercial listings, archive pages, and isolated incidental target mentions.

## Repro Commands
The Stage 1e flow is split across local and EC2 steps:

```bash
make cc_stage1e_freeze_scan
make cc_stage1e_freeze_validate
make cc_stage1e_freeze_export_urls
make cc_stage1e_freeze_upload_urls
make cc_stage1e_freeze_install_indexes_remote
make cc_stage1e_freeze_start_index_server
make cc_stage1e_freeze_resolve
make cc_stage1e_freeze_extract
make cc_stage1e_freeze_document_quality
```

See `reports/stage1e_execution_guide.md` for the full local/EC2 command sequence.

## Notes
- Stage 1e is a cleanup and corpus-quality freeze, not a new collection design.
- Deduplication deliberately remains local and simple; DataTrove MinHash was not adopted for this pilot scale.
- Stage 2 should reuse the Stage 1e document-quality gate but plan compute around WARC extraction throughput.
