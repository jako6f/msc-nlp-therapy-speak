# Gemini Frame Criticism Runbook

This runbook executes the cross-model criticism and human-correction stage using
the Gemini CLI as an alternative to Claude. Gemini ranks likely annotation errors
and suggests alternative labels, but no Gemini suggestion changes a training label
without human review.

The held-out human validation sample is excluded from this workflow.

## Preconditions

From the repository root:

```bash
conda activate msc-nlp
gemini --version
python scripts/annotation/run_gemini_criticism.py self-test
```

The active criticism contract is:

- `codebooks/codebook_v4.md`: sole authority for coding rules;
- `prompts/critic_v5.md`: criticism procedure and safeguards;
- `prompts/critic_output_schema.json`: locally validated output contract;
- `prompts/gemini_critic_no_tools.toml`: policy excluding substantive tools.

Gemini runs use normal headless mode, a restrictive tool policy, and
`--output-format json`. The codebook, critic prompt, batch content, and a compact
output template are inlined directly into the task prompt. Gemini CLI does not
expose a custom structured-output schema flag, so every response must pass strict
local field, hierarchy, row-order, length, and runtime-safety validation.
Overlong review text and hierarchy-implied null fields are normalised
deterministically before validation; the raw response and normalisation counts
remain recorded.

## 0. Probe the JSON Output Format and Policy

Before any real run, inspect what the Gemini CLI actually emits with
`--output-format json` and confirm that the restrictive policy is active.

```bash
python scripts/annotation/run_gemini_criticism.py probe \
  --model gemini-3-flash-preview
```

The probe must report no file changes and no substantive tool calls. Gemini
CLI's harmless internal `update_topic` call is permitted; all other tools are
excluded by policy. A policy-blocked failed attempt remains recorded but does
not invalidate the response; any successful excluded tool use is rejected.

## 1. Synchronize Critic Inputs

Critic inputs are sourced from the same Codex outputs but partitioned
independently into chunks of at most 10 rows. The smaller Gemini-only chunks
avoid the field omissions and agent/tool fallback observed with 25-row outputs.

```bash
python scripts/annotation/run_gemini_criticism.py sync --dataset pilot
python scripts/annotation/run_gemini_criticism.py sync --dataset production
```

Inspect commands without calling Gemini:

```bash
python scripts/annotation/run_gemini_criticism.py dry-run \
  --dataset pilot --model gemini-3-flash-preview --batch critic_batch_001_01
```

## 2. Calibrate the Critic on the Pilot

Run Gemini 3 Flash on the complete pilot:

```bash
python scripts/annotation/run_gemini_criticism.py run \
  --dataset pilot --model gemini-3-flash-preview

python scripts/annotation/run_gemini_criticism.py validate \
  --dataset pilot --model gemini-3-flash-preview

python scripts/annotation/run_gemini_criticism.py evaluate-pilot \
  --model gemini-3-flash-preview
```

Inspect:

- `llm_annotation/gemini/pilot/evaluation/critic_pilot_model_metrics.csv`
- `llm_annotation/gemini/pilot/evaluation/critic_pilot_comparisons.csv`

Proceed only when the model has 100% valid outputs and recovers at least 60% of
known Codex errors in the top 40 pilot cases. Inspect the comparison table for
repeated codebook blind spots before production.

The runner defaults to one attempt because Gemini CLI may already retry quota
errors internally. Inspect invalid attempt metadata before rerunning; valid
completed or recoverable chunks are skipped.

The completed Gemini 3 Flash pilot produced 200/200 validated rows and recovered
11 of 17 known Codex errors in the top 40 cases (`64.7%` recall; average
precision `0.338`). Its suggested replacement hierarchy matched the human label
for only 3 of 17 known errors. Use Gemini as a ranking critic only; do not
automatically adopt its suggested labels.

The 21 valid pilot chunks required 47 API requests, 18.6 minutes of aggregate
API latency, and approximately 0.81 million CLI-reported total tokens. A
production run with 320 ten-row chunks is therefore projected to require roughly
4.7 hours of aggregate API latency and 12.3 million total tokens. Quota retries
and occasional invalid responses are expected; resume the same command rather
than increasing automatic attempts.

## 3. Run Production Criticism

Set the selected model for the current shell:

```bash
PINNED_MODEL="gemini-3-flash-preview"
```

First inspect two batches:

```bash
python scripts/annotation/run_gemini_criticism.py run \
  --dataset production --model "$PINNED_MODEL" --batch 001
python scripts/annotation/run_gemini_criticism.py run \
  --dataset production --model "$PINNED_MODEL" --batch 002
python scripts/annotation/run_gemini_criticism.py validate \
  --dataset production --model "$PINNED_MODEL" --batch 001
python scripts/annotation/run_gemini_criticism.py validate \
  --dataset production --model "$PINNED_MODEL" --batch 002
```

Then resume the complete run:

```bash
python scripts/annotation/run_gemini_criticism.py run \
  --dataset production --model "$PINNED_MODEL"
python scripts/annotation/run_gemini_criticism.py validate \
  --dataset production --model "$PINNED_MODEL"
python scripts/annotation/run_gemini_criticism.py combine \
  --dataset production --model "$PINNED_MODEL"
```

Valid completed batches are skipped only while their full recorded contract still
matches.

Do not leave the production run unattended initially. Run and validate the first
two source batches, inspect their metadata for successful excluded tools or
unexpected normalisation rates, then proceed with the resumable full run.

## 4. Critique Future Annotation Extensions

After Codex creates and annotates a new batch, the next `run` command
automatically synchronizes it. For example, after `annotator_batch_042`:

```bash
python scripts/annotation/run_gemini_criticism.py run \
  --dataset production --model "$PINNED_MODEL" --batch 042
```

Selecting a source batch number runs all critic chunks derived from it:

```text
--batch 042
--batch annotator_batch_042
```

Select one exact critic chunk with, for example, `--batch critic_batch_042_01`.

If a synchronized Codex batch was intentionally changed:

```bash
python scripts/annotation/run_gemini_criticism.py rebuild-batch \
  --dataset production --batch annotator_batch_042
```

## 5. Human Review

Create the protected ranked-review workbook:

```bash
python scripts/annotation/run_gemini_criticism.py prepare-ranked-review \
  --model "$PINNED_MODEL"
```

Review cases in descending score order and set `review_status = reviewed` for
every completed row. Edit the three `final_*` label columns when correction is
needed. Gemini suggestions are advisory.

Review in 100-case waves. Stop before the 550-case maximum only after two
consecutive completed 100-case waves each produce fewer than five corrections.

After the ranked stopping point is final, generate and complete the 50-case
random audit:

```bash
python scripts/annotation/run_gemini_criticism.py prepare-audit \
  --model "$PINNED_MODEL"
```

## 6. Finalize Corrected Labels

After completing the ranked review and residual audit:

```bash
python scripts/annotation/run_gemini_criticism.py finalize-corrections
```

This writes:

```text
data/interim/lsc/classification/human_correction/frame_llm_correction_completed.csv
```

Only rows explicitly marked `reviewed` can replace Codex labels. Unreviewed rows
retain their Codex labels.
