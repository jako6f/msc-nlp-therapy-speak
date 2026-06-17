---
name: frame-annotation
description: Annotate one supplied ADHD/autism discourse batch using the repository's locked hierarchical frame codebook and schema. Use only when explicitly asked to perform frame annotation for this repository.
---

# Frame Annotation

Annotate exactly one named batch.

1. Read `notebooks/01_classification/codebooks/codebook_v4.md`.
2. Read `notebooks/01_classification/prompts/annotator_v4.md`.
3. Read only the named JSONL batch.
4. Apply Stage 0 before the conditional clinical and lived-experience labels.
5. Treat rows independently and use no outside evidence or web search.
6. Return only the object required by the supplied output schema.
7. Before responding, verify exact ID coverage, input order, allowed values, and
   valid hierarchical combinations.

Do not edit files, annotate another batch, explain individual decisions, or
infer labels from other rows.
