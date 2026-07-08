# LSC Figure Style Guide

This guide keeps figures from the diachronic LSC notebooks visually consistent and report-ready without introducing a plotting framework.

## General Principles

- Use clean white backgrounds, subtle grid lines, and no top/right spines.
- Keep titles descriptive but short. Use reader-facing wording rather than notebook shorthand or raw stratum identifiers.
- Use publication year on the x-axis for semantic measures and Common Crawl source year for Salience.
- Separate target groups and comparator terms into panels when a single overlay would be visually crowded.
- Show uncertainty only when the notebook has a defined uncertainty quantity. For Sentiment, Intensity, and Breadth this means annual bootstrap intervals; for Salience primary trend figures this means model-based OLS trend uncertainty.
- Save report figures as high-resolution PNG and vector PDF when feasible.
- Keep the report figure folder focused: each scalar measure should normally save one primary trajectory figure. Diagnostics should be retained as CSV tables and compact notebook summaries unless an appendix figure is explicitly needed.

## Frame-Aware Figure Hierarchy

- Main dissertation-facing scalar figures should use three equal-width horizontal panels: ADHD, Autism, and comparator terms.
- ADHD and Autism panels should foreground the substantive-core Overall trajectory with the full condition hue.
- Clinical/disorder and lived-experience frame trajectories should remain in the same panel as thinner contextual traces, using lighter shades of the same condition hue.
- Every series shown in the scalar trajectory figures should have its annual line, 95% uncertainty band, and dashed OLS trend summary where the corresponding table supports it.
- Mixed-frame target estimates should remain in tables and audit summaries, but should not appear in the compact main-text trajectory figure unless the written interpretation specifically needs them.
- Do not save all-frame or coverage diagnostic plots by default. Keep those diagnostics in annual score tables, coverage tables, sampling/raw-form diagnostics, and audit-flag outputs.
- Do not mark significant slopes directly in the figures. Report p-values, residual-autocorrelation flags, and sensitivity results in the trend tables and text.

## Classification Frame Balance Figure

- The primary classification composition figure should show the clinical/disorder versus lived-experience balance for ADHD and Autism in two equal-width horizontal panels.
- Plot frame shares over publication year using only clear clinical/disorder and lived-experience assignments as the denominator.
- Keep mixed, non-substantive, and sparse substantive-other labels in appendix tables and notebook summaries rather than in the main composition figure.
- Use the frame-category palette for the two plotted lines, because colour encodes frame category rather than analysis unit.

## Salience Figure

- Salience uses Common Crawl source year rather than publication year, so its figure should make the source-year axis explicit.
- The primary Salience figure should use two horizontal panels: absolute ADHD/autism WARC-validated hits per million WET tokens, and all target/comparator terms indexed to their own 2014 rate.
- The absolute panel is the Salience result. The indexed panel is comparator context for proportional movement, not a replacement measure.
- Publication-year status, WARC/WET retention, raw-form balance, and Salience frame-composition breakdowns should remain table diagnostics unless the dissertation text explicitly needs an appendix figure.

## Thematic Evolution Figure

- Thematic neighbour-similarity figures should use one target group per figure, with equal-width horizontal panels for Overall, Clinical/disorder, and Lived experience.
- Because line colour encodes neighbours rather than analysis units, use the frame-category/cloud palette for neighbour lines instead of assigning ADHD/Autism condition hues to individual neighbours.
- Do not force every thematic panel to contain five lines. Report-facing neighbour lines should be stable enough to read as trajectories rather than one-year top-neighbour artifacts.
- Do not add confidence bands or OLS trend lines to thematic neighbour figures. The annual top-neighbour table and notebook diagnostics provide the interpretive context.
- Keep mixed-frame thematic information in notebook diagnostics unless the text specifically needs an additional appendix figure.
- Exploratory thematic heatmaps may be saved as appendix candidates when annual top-neighbour churn is important to inspect, but they should not replace the compact stable-neighbour line figures.

## Cloud Palette

Use condition hue families for scalar trajectories.

| analysis unit | main colour | marker | role |
|---|---|---:|---|
| ADHD | `#2F6F9F` | `o` | target Overall |
| Autism | `#B66A4A` | `s` | target Overall |
| frustration | `#4F8F78` | `^` | baseline |
| loneliness | `#7FA68A` | `D` | baseline |
| sadness | `#9AA6A1` | `v` | baseline |

Use lighter condition shades for target frame traces.

| condition | Overall | Clinical/disorder | Lived experience | Mixed |
|---|---|---|---|---|
| ADHD | `#2F6F9F` | `#75A9C8` | `#AECFE0` | `#D4E4EC` |
| Autism | `#B66A4A` | `#CE8D70` | `#E1B49D` | `#F2D8CF` |

Use frame-category colours for frame-composition figures and other non-condition-specific frame displays.

| frame category | colour |
|---|---|
| Clinical/disorder | `#4F8DB3` |
| Lived experience | `#C98263` |
| Mixed | `#79A889` |
| Substantive other | `#A998C9` |
| Non-substantive | `#B8C0C5` |
| Comparator/unframed | `#7B8785` |

## Defaults

- Figure DPI: `300`
- Main uncertainty ribbon alpha: about `0.14`
- Frame-trace ribbon alpha: about `0.07`
- Main line width: approximately `2.7` to `2.8`
- Frame-trace line width: approximately `1.5` to `1.7`
- Font: bundled Matplotlib sans-serif font unless the paper later standardises a different font
- Legend: frameless, placed inside the panel only when it does not obscure the data

## Notebook Standard

Each LSC notebook should define the required local plotting constants near setup, reuse the colours and markers above, and briefly state any intentional deviation in nearby markdown.
