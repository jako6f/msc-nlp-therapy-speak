# Codex Frame Annotation Runbook

This runbook executes the locked Codex annotation workflow. It launches one
fresh, read-only `codex exec` context per batch and validates every result before
it becomes a canonical annotation handoff.

The human validation sample is excluded from this workflow. It remains untouched
until final classifier evaluation.

## Preconditions

From the repository root:

```bash
conda activate msc-nlp
codex login status
python scripts/annotation/run_codex_annotation.py self-test
```

The runner fixes the model to `gpt-5.5`, disables web search, uses a read-only
sandbox, and records input/prompt/codebook/schema/skill hashes plus available
token-usage metadata.

The active annotation contract is:

- `codebooks/codebook_v4.md`: sole authority for coding rules and boundary cases;
- `prompts/annotator_v4.md`: task order, operational safeguards, and output
  contract;
- `.agents/skills/frame-annotation/SKILL.md`: one-batch execution procedure.

## 1. Prepare and Inspect Pilot Batches

```bash
python scripts/annotation/run_codex_annotation.py prepare-pilot
python scripts/annotation/run_codex_annotation.py dry-run \
  --dataset pilot --reasoning high
```

This creates three label-free pilot batches of 75, 75, and 50 rows. It does not
alter the completed human pilot workbook.

## 2. Calibrate the Locked Prompt

Run the pilot with `high` reasoning:

```bash
python scripts/annotation/run_codex_annotation.py run \
  --dataset pilot --reasoning high
python scripts/annotation/run_codex_annotation.py validate \
  --dataset pilot --reasoning high
python scripts/annotation/run_codex_annotation.py evaluate-pilot \
  --reasoning high
```

Inspect:

- `data/interim/lsc/classification/llm_annotation/codex/pilot/evaluation/pilot_reasoning_metrics.csv`
- `data/interim/lsc/classification/llm_annotation/codex/pilot/evaluation/pilot_disagreement_review.csv`

When evaluation finds completed adjudication fields in an existing disagreement
review, it archives that file under a content-hashed
`pilot_disagreement_review_adjudicated_*.csv` name before writing the new review
sheet.

Use pilot errors to refine the prompt only when they reveal a systematic,
codebook-relevant failure. Permit at most two prompt-refinement iterations.
Record every prompt change in `prompts/prompt_log.md`.

Proceed to production only when the locked prompt reaches:

- 100% schema and hierarchical validity;
- Stage-0 macro-F1 of at least 0.90;
- conditional clinical and lived-experience macro-F1 of at least 0.85;
- hierarchical exact match of at least 0.85;
- no unresolved systematic error involving a major codebook boundary.

## 3. Locked Reasoning Effort

The pilot comparison selected `high` reasoning for production annotation.
Deprecated `xhigh` pilot artifacts have been removed. Use `high` consistently
for all further production extensions and validation.

## 4. Run Production Annotation

First run and inspect two batches:

```bash
python scripts/annotation/run_codex_annotation.py run \
  --dataset production --reasoning high --batch annotator_batch_001
python scripts/annotation/run_codex_annotation.py run \
  --dataset production --reasoning high --batch annotator_batch_002
python scripts/annotation/run_codex_annotation.py validate \
  --dataset production --reasoning high --batch annotator_batch_001
python scripts/annotation/run_codex_annotation.py validate \
  --dataset production --reasoning high --batch annotator_batch_002
```

If both outputs are valid and their label distributions are plausible, resume
the complete sequential run:

```bash
python scripts/annotation/run_codex_annotation.py run \
  --dataset production --reasoning high
python scripts/annotation/run_codex_annotation.py validate \
  --dataset production --reasoning high
python scripts/annotation/run_codex_annotation.py combine \
  --dataset production --reasoning high
```

The runner skips already valid batches only when their recorded input, prompt,
codebook, schema, skill, model, and reasoning hashes still match. It stops after
two invalid attempts. Failed attempts and Codex event logs are preserved under
`llm_annotation/codex/pilot/runs/` and `llm_annotation/codex/production/runs/`.

Because this workflow uses ChatGPT-authenticated Codex rather than API-key
billing, actual usage is governed by rolling Codex limits. Inspect
`llm_annotation/codex/run_manifest.csv` for recorded per-run usage.

## 5. Append Additional Production Batches

The production workflow is append-only. Do not rerun
`01_prepare_annotation_samples.ipynb` to increase the LLM sample: that notebook
defines the protected pilot, validation, and initial production samples.

To add a named, disjoint 1,000-row tranche after the completed initial 2,000
rows:

```bash
python scripts/annotation/run_codex_annotation.py \
  prepare-production-extension \
  --extension-name additional_1000_v1 \
  --additional-rows 1000 \
  --reasoning high
```

The `additional_1000_v1` extension has already been prepared in the current
repository state. Do not rerun that preparation command; begin with
`annotator_batch_028` below.

The command:

- requires every existing production batch to have a current valid annotation;
- excludes pilot, validation, and all previously sampled production contexts;
- samples the remaining context pool by target group and broad year band;
- continues annotation IDs and batch numbers without modifying existing
  batches or labels;
- records the extension in `llm_annotation/codex/production/extension_manifest.csv`;
- refuses to reuse an extension name.

For the first extension from 2,000 to 3,000 rows, this creates
`annotator_batch_028` through `annotator_batch_041`. Inspect the first two new
batches:

```bash
python scripts/annotation/run_codex_annotation.py run \
  --dataset production --reasoning high --batch annotator_batch_028
python scripts/annotation/run_codex_annotation.py run \
  --dataset production --reasoning high --batch annotator_batch_029
python scripts/annotation/run_codex_annotation.py validate \
  --dataset production --reasoning high --batch annotator_batch_028
python scripts/annotation/run_codex_annotation.py validate \
  --dataset production --reasoning high --batch annotator_batch_029
```

Then run the complete production command. The runner skips the already valid
initial and inspection batches and annotates only unfinished appended batches:

```bash
python scripts/annotation/run_codex_annotation.py run \
  --dataset production --reasoning high
python scripts/annotation/run_codex_annotation.py validate \
  --dataset production --reasoning high
python scripts/annotation/run_codex_annotation.py combine \
  --dataset production --reasoning high
```

Rerunning the `run` command after a transient failure is safe. Do not rerun the
`prepare-production-extension` command with a new extension name unless another
distinct tranche is intentionally required.

## 6. Continue to Cross-Model Criticism

After all 3,000 Codex labels pass validation:

1. Rerun `03_llm_annotation_batches.ipynb` to inspect complete-batch coverage,
   frame/confidence distributions, and token usage.
2. Follow `gemini_criticism_runbook.md` to calibrate and run the codebook-v4,
   critic-v5 Gemini criticism workflow.
3. Human-correct critic-ranked cases and complete the random residual audit.
4. Train the hierarchical classifier from the human pilot plus corrected
   LLM-assisted labels.
5. Evaluate once on the untouched 200-case human validation set.
