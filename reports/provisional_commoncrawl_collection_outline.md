# Provisional Collection Section Outline

Purpose: compact drafting anchors for `Materials / Common Crawl / Collection`. This is not final prose.

## 1. Introduce Common Crawl

- Common Crawl is a large, public archive of web crawl data released in recurring crawl snapshots.
- It is widely used as a web-scale source for language data, including search, corpus construction, NLP research, and large language model pretraining.
- For this project, its value is breadth: it provides repeated cross-sections of general web discourse rather than a single platform, newspaper archive, or curated clinical corpus.
- It is not a representative survey of the web or of public opinion; it reflects what Common Crawl crawled, retained, and made available.
- The relevant Common Crawl data layers are WET and WARC.
- WET files contain extracted plaintext from crawled pages; they are smaller and efficient for broad term scanning, but they lose HTML structure and much page-level metadata.
- WARC files contain the original archived web responses, including HTML and headers; they are larger and slower to process, but support stronger validation, main-text extraction, and metadata recovery.
- The pipeline therefore uses WET first for scale and stable denominators, then WARC for precision and document quality.

## 2. Overall Collection Pipeline Design

- We built a Common Crawl collection pipeline designed to extract high-quality general discourse to study diachronic lexical-semantic change in specific target terms, in this case ADHD and autism, against matched baseline terms.
- Target and baseline terms are processed together so that yearly denominators, sampling logic, and quality filters are comparable.
- WET scanning is the scalable discovery step; WARC extraction is the substantive validation step.
- The design is economical: expensive WARC processing is reserved for candidate documents rather than applied to all crawled pages.
- The design has two linked tracks.
- The trend track uses fixed-effort annual samples to estimate how often target and baseline terms appear over time.
- The corpus track builds a larger, quality-gated document corpus for downstream semantic, affective, and contextual analysis.
- The two-track design separates rate estimation from corpus construction: the trend track prioritises comparability, while the corpus track prioritises document quality and sufficient target-term coverage.
- Domain caps (set to 50 WET-validated hit rows per registered domain per Common Crawl crawl) reduce the risk that a few large websites dominate the corpus.
- Intermediate summaries and manifests make each year, crawl, track, and batch auditable.
- The pipeline is built to run on AWS infrastructure, using S3 for intermediate artifacts and EC2 for network-bound Common Crawl index lookup and WARC byte-range extraction.
- The project uses deterministic yearly crawl selection: one Common Crawl snapshot per year, chosen near a fixed annual anchor date and frozen in a crawl map.

## 3. Key Collection Numbers

- Temporal coverage: 13 annual Common Crawl snapshots, 2014-2026.
- Trend track: 27.7 million WET records scanned and 78,899 WARC-validated term hits retained (provisional, number will increase significantly).
- Corpus track: 28.1 million WET records scanned and 42,975 analysis-ready documents retained (provisional, number will increase significantly).
- Corpus target coverage: 10,998 target documents, including 3,907 ADHD documents and 8,558 autism documents (provisional, number will increase significantly).
- Current compute reference: the collection was run on an AWS `m7i-flex.large` instance, featuring 2 vCPUs, 8 GiB of RAM, and up to 12.5 Gbps network bandwidth (AWS, 2026)
- Current corpus throughput: roughly 1.0 hour per million scanned WET records on the current instance type.

## 4. Walk Through The Pipeline

- The collection workflow is summarised visually in `commoncrawl_pipeline/commoncrawl_collection_pipeline.pdf`; in the paper, point readers to Figure~\ref{fig:commoncrawl-pipeline}.
- Stage 1: select one Common Crawl snapshot for each year and deterministically sample WET files.
- Stage 2: scan WET plaintext for target and baseline term matches, storing document counts as the trend denominator.
- Stage 3: apply conservative WET-stage triage to remove obvious page chrome, directory pages, search/listing pages, product/job pages, and spam-like URL patterns.
- Stage 4: export unique candidate URLs and resolve them to WARC filenames, offsets, and byte ranges using Common Crawl index metadata.
- Stage 5: fetch the corresponding WARC records and re-extract main text from the archived HTML.
- Stage 6: retain only documents where the matched terms survive WARC-based main-text extraction.
- Stage 7: enrich documents with candidate publication dates where available, while treating crawl capture year as the primary temporal anchor.
- Stage 8: apply post-WARC document-quality filters to remove non-substantive or structurally unsuitable pages.
- Stage 9: retain English documents and deduplicate near-identical texts within the collection.
- Stage 10: write final processed outputs separately for the trend track and corpus track.

## 5. GitHub Pointer

- The collection pipeline is available in the project repository.\footnote{\url{https://github.com/jako6f/msc-nlp-therapy-speak}}
