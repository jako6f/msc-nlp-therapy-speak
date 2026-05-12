# WET Triage Closeout

## Purpose
This pass developed the conservative WET-stage triage used later in the pipeline. The objective was to remove obvious junk before WARC retrieval while avoiding aggressive filters that could bias the language surrounding ADHD, autism, or the comparator terms.

## Input Scope
- Crawls: `CC-MAIN-2016-44`, `CC-MAIN-2026-04`
- Sampling: same WET smoke-test frame
- Accepted run: `iter_08`
- Documents scanned: `157,461`
- Candidate hits: `728`
- Retained WET hits after triage: `596`

## Final Rule Stack
Only two WET-stage boilerplate rules were retained:

- `signature_hard`: high-confidence regular expressions for UI and page-chrome fragments such as cookie banners, accessibility widgets, ecommerce chrome, and skip-navigation text.
- `directory_index`: a structural heuristic for navigation-heavy directory/index pages, using signals such as separator density, low sentence-terminator rate, title-case density, and a small directory lexicon.

All broader or softer rules were dropped before closeout. Removed candidates included generic listiness rules, commerce-page rules, topic-hub/archive detectors, soft navigation lexicons, and density-based filters. Those rules were too easy to overfit and risked deleting genuine prose.

## Key Parameters
- `filters.min_chars = 500`
- `filters.context_window_chars = 200`
- `filters.domain_cap = 50`
- `filters.asd_disambiguation_window_chars = 200`
- `boilerplate.check_window_chars = 2000`

## Key Results
- Removed by `signature_hard`: `103`
- Removed by `directory_index`: `29`
- Removed total: `132`
- Retained WET hit rate: about `37.85` hits per `10,000` scanned documents
- Scan throughput: about `2,314` documents per second

## Outcome
Manual inspection showed that conservative WET triage removed clear boilerplate but could not reliably identify all tag pages, directory pages, SEO spam, or playlist/list pages. This was an important methodological finding: WET filtering is useful as a cheap cost-control layer, but it should not be treated as the final document-quality gate.

## Limitations
- WET strips HTML structure and layout, so many page-type decisions remain underdetermined.
- Additional WET-only heuristics offered diminishing returns and increased the risk of recall loss.
- The downstream pipeline therefore moved substantive validation to WARC HTML extraction and post-extraction quality filtering.
