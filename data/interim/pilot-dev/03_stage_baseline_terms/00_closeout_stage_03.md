# Baseline-Term Selection Closeout

## Purpose
This pass fixed the non-target comparator terms used alongside ADHD/autism. The comparator set needed enough coverage for diachronic analysis while remaining semantically interpretable and not dominated by commercial boilerplate.

## Input Scope
- Crawls: `CC-MAIN-2016-44`, `CC-MAIN-2026-04`
- WET triage: frozen conservative rules from the previous pass
- WARC validation: not run in this pass
- Accepted run: `20260505_104532`
- Documents scanned: `157,461`
- WET-validated hits: `1,126`

## Selected Comparator Terms
The selected baseline terms were:

- `frustration`
- `sadness`
- `loneliness`

These terms were retained because they produced enough WET-validated material while remaining more semantically focused than broader alternatives.

## Evidence
- `frustration`: `288` WET-validated hits across the two crawls; strong coverage and relatively low removal.
- `sadness`: `162` WET-validated hits; useful negative-affect comparator, with some expected title/music noise.
- `loneliness`: `85` WET-validated hits; lower volume but comparatively clean and concept-like.

The target terms remained:

- ADHD patterns: `adhd`, `attention_deficit`
- Autism patterns: `autism`, `autistic`, `autism_spectrum`, context-disambiguated `ASD`

## Excluded Comparator Candidates
- `worry` was rejected despite high volume because many surviving uses were commercial or generic reassurance phrases such as product, booking, or signup language.
- `tiredness` was rejected because volume was low and surviving contexts skewed toward symptom lists and clinical/somatic material rather than a broad discourse comparator.

## Design Rationale
The comparator terms are not intended to be perfect semantic controls. They provide a broad non-target reference track for assessing whether ADHD/autism usage patterns differ from general affective-language patterns in Common Crawl. The chosen terms balance interpretability, volume, and contamination risk.

## Outcome
The baseline vocabulary was frozen for the remainder of pilot development and for the final collection design. Later passes focused on whether WARC extraction and document-quality gates could turn the WET candidates into a cleaner corpus.

## Limitations
- This decision was based on a small two-crawl pilot frame.
- Some comparator-specific noise remains, especially around `sadness` in media/music contexts.
- Final corpus quality still depends on WARC extraction and document-quality filtering.
