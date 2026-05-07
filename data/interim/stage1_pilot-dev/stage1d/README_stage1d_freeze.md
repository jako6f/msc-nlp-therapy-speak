# Stage 1d Freeze

## Purpose
Stage 1d is the frozen Common Crawl pilot workflow that generalises the Stage 1b/1c WET pipeline into WARC-backed validation, remote pointer resolution, WARC extraction, and a final English-only dedup filter.

## Frozen Pipeline
1. Export unique `(crawl_id, url)` rows from the frozen Stage 1c anchor plus Stage 1d hold-out.
2. Resolve WARC pointers on the EC2 helper using the local Common Crawl index server.
3. Fetch WARC byte ranges and validate them with HTML extraction.
4. Apply the final English-only dedup filter to the WARC-validated hits.

## Retained Outputs
- `url_exports/` - frozen Stage 1d URL export used for remote pointer resolution.
- `pointer_cache/` - resolver-produced WARC pointers for the Stage 1d URL export.
- `warc/` - WARC-validated rows plus Stage 1d summary metrics.
- `filter_en_dedup/` - English-only dedup outputs and manual validation sample.
- `holdout/` - Stage 1d WET hold-out input artifacts kept for provenance.

## Final Results
- Pointer resolution: `1895 / 1899` URLs resolved.
- WARC validation: `1126` validated hits survived extraction.
- Final filtered corpus: `818` English deduplicated documents.
- Manual validation sample: `30` documents in `cc_val_sample30_20260507_123700.csv`.

## Repro Commands
The frozen Stage 1d flow is split across local and EC2 steps:

```bash
make cc_stage1d_freeze_export_urls
make cc_stage1d_freeze_upload_urls
make cc_stage1d_freeze_install_indexes_remote
make cc_stage1d_freeze_start_index_server
make cc_stage1d_freeze_resolve
make cc_stage1d_freeze_extract
make cc_stage1d_freeze_filter_en_dedup
```

## Notes
- `cc-stage1d-resolve-remote` and `cc-stage1d-extract-remote` are the active remote stages.
- The older reboot guide and discarded lookup experiments were removed from the freeze surface.
- Stage 1e is the next cleanup pass on the WARC-validated corpus.
