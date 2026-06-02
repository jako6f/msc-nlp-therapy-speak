# LSC Figure Style Guide

This guide keeps figures from the diachronic LSC notebooks visually consistent and report-ready without introducing a plotting framework.

## General Principles

- Use clean white backgrounds, subtle horizontal/vertical grid lines, and no top/right spines.
- Keep titles descriptive but short; use sentence case or compact title case consistently within a figure set.
- Use publication year on the x-axis for diachronic plots, with integer ticks and no categorical year labels.
- Separate target groups and comparator terms into panels when a single overlay would be visually crowded.
- Show uncertainty ribbons where bootstrap intervals exist; otherwise avoid implying precision.
- Save report figures as high-resolution PNG and vector PDF when feasible.

## Analysis Unit Styling

| analysis unit | colour | marker | role |
|---|---|---:|---|
| ADHD | `#0072B2` | `o` | target |
| Autism | `#D55E00` | `s` | target |
| frustration | `#009E73` | `^` | baseline |
| loneliness | `#CC79A7` | `D` | baseline |
| sadness | `#6E6E6E` | `v` | baseline |

## Defaults

- Figure DPI: `300`
- Confidence ribbon alpha: `0.15`
- Main line width: approximately `2.0` to `2.5`
- Font: bundled Matplotlib sans-serif font unless the paper later standardises a different font
- Legend: frameless, placed inside the panel only when it does not obscure the data

## Notebook Standard

Each LSC notebook should define the required local plotting constants near setup, reuse the colours and markers above, and briefly state any intentional deviation in nearby markdown.
