# Classification Prompt Log

Use this file to record prompt revisions, model/interface choices, and batch-level notes for the frame classification workflow.

| date | prompt_version | model/interface | stage | change | reason |
|---|---|---|---|---|---|
| 2026-06-04 | annotator_v1 | Codex/OpenAI, exact model to record at run time | LLM annotation | Initial compact schema-constrained prompt. | Prepared before pilot prompt refinement. |
| 2026-06-04 | critic_v1 | Claude Code, exact model to record at run time | LLM criticism | Initial axis-specific error-probability prompt. | Prepared before pilot prompt refinement. |
| 2026-06-05 | annotator_v2 | Codex/OpenAI, exact model to record at run time | LLM annotation | Switched to hierarchical Stage-0 substantive-discourse gate with conditional clinical/lived labels and compact confidence. | Revised codebook separated non-substantive target mentions from meaningful negative frame examples. |
| 2026-06-05 | critic_v2 | Claude Code, exact model to record at run time | LLM criticism | Added separate error probabilities for substantive, clinical, and lived labels, with clinical/lived checks conditional on substantive discourse. | Criticism must respect the masked-loss logic used by the hierarchical classifier. |
