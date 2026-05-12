# Wet Smoke Test Closeout

## Purpose
This first pilot pass checked whether a WET-first Common Crawl workflow was viable for finding ADHD, autism, and comparator-language candidates at low cost. It was deliberately small: the goal was to validate parsing, term matching, context snippets, domain summaries, and manual-inspection exports before investing in heavier filtering or WARC retrieval.

## Input Scope
- Crawls: `CC-MAIN-2016-44`, `CC-MAIN-2026-04`
- Sampling: `2` WET files per crawl
- Documents scanned: `157,461`
- Minimum text length: `500` characters

## Main Design Decisions
- Start from WET files rather than WARC files because WET text is cheap to scan at scale.
- Store row-level context snippets around each match so candidate quality could be inspected manually.
- Track registered domains early to identify domain concentration and potential spam sources.
- Treat `ASD` separately from other autism patterns because the acronym is ambiguous and requires local-context disambiguation.

## Key Results
- Candidate hits: `728`
- Hit rate: about `46.23` candidates per `10,000` scanned documents
- Domain cap was not triggered at this small scale.
- Manual inspection showed that core term matching worked, including the initial ASD disambiguation, but many retained hits were page chrome, navigation text, or other low-value boilerplate.

## Outcome
The smoke test established that WET-first triage was technically feasible and inexpensive, but that raw term matching was not precise enough. The next pilot pass therefore focused on conservative WET-stage boilerplate and directory-page filtering.

## Limitations
- The sample was too small to support substantive corpus claims.
- WET text lacks DOM structure, so page-type and boilerplate decisions could only use plain-text heuristics.
- The output should be read as a pipeline feasibility check, not as an analysis-ready corpus.
