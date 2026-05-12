# WARC Validation Closeout

## Purpose
This pass connected the cheap WET candidate scan to WARC-backed HTML validation. The central question was whether selected WET hits could be resolved to exact Common Crawl WARC records, fetched efficiently on EC2, and validated against extracted main-page content rather than WET text alone.

## Input Scope
- WET anchor frame: the earlier two-crawl pilot slice
- Additional hold-out slice: a larger pilot-dev hold-out over `CC-MAIN-2016-44`, `CC-MAIN-2021-49`, and `CC-MAIN-2026-04`
- URL export run: `20260506_155406`
- WARC extraction run: `20260507_112625`
- English/dedup run: `20260507_123700`

## Final Workflow
1. Export unique `(crawl_id, url)` rows from WET-validated candidates.
2. Upload the URL export to S3.
3. On EC2, install the relevant Common Crawl secondary indexes and run a local `pywb` index server.
4. Resolve WARC filename, byte offset, and byte length for each URL.
5. Fetch the WARC byte ranges from Common Crawl.
6. Extract main HTML content with Trafilatura.
7. Keep rows where the matched term survives in extracted main content.
8. Apply English-only filtering and local near-deduplication.

## Key Design Decisions
- The local Common Crawl index-server route replaced earlier discarded lookup approaches because it gave reliable pointer resolution once index shard access and path configuration were corrected.
- WARC extraction was run remotely because byte-range fetching is network-bound and easier to perform close to S3/Common Crawl infrastructure.
- Trafilatura became the substantive content-extraction layer because WET text alone could not distinguish main article text from navigation, tags, or page furniture.
- English filtering and deduplication were kept as a separate final pass so their impact could be measured independently.

## Key Results
- URLs in pointer cache: `1,899`
- Resolved WARC pointers: `1,895`
- Fetch-success documents: `1,895`
- WARC extraction successes: `938`
- WARC-validated row-level hits: `1,126`
- Final English deduplicated documents: `818`
- Manual validation sample: `filter_en_dedup/cc_val_sample30_20260507_123700.csv`

## Outcome
The pass proved that WARC-backed validation was operationally feasible and inexpensive at pilot scale. It also showed that WARC validation plus English/dedup filtering was not sufficient on its own: manual inspection still found tag pages, directory pages, quote aggregators, playlist pages, and SEO-like junk.

## Limitations
- Page-type contamination remained the main quality threat.
- The initial publication-date extraction logic was custom and later replaced by a simpler `htmldate`-based approach.
- The output was a successful infrastructure proof and intermediate corpus, not the final quality gate.
