# Quality Bar — `msc-nlp-therapy-speak`

## Purpose
This document defines the quality bar for the dissertation codebase.

The goal is not industrial-grade software perfection. The goal is a clear, efficient, reproducible, and defensible research pipeline that is proportionate to a 30 ECTS MSc dissertation in the social sciences.

## Core principle
The repo should be:

- correct enough to trust
- simple enough to understand
- efficient enough to run
- documented enough to defend
- not over-engineered

## What this repo is
This repo is a research pipeline, not a general-purpose software product.

That means the quality bar prioritises:

- methodological clarity
- reproducibility of key stages
- stable naming and outputs
- computational efficiency where it matters
- minimal but sufficient engineering discipline

It does not prioritise:

- enterprise architecture
- exhaustive abstraction
- full automated test suites
- perfect API design
- future-proofing for every hypothetical extension

## Practical quality standard

### 1. Code should be understandable
A reasonably careful reader should be able to understand:

- what each script/module does
- what inputs it expects
- what outputs it writes
- what stage of the pipeline it belongs to

Code should favour straightforward logic over cleverness.

### 2. Naming should reflect pipeline reality
Names should be:

- explicit
- stage-scoped where needed
- consistent across config, Makefile, docs, and outputs

Current convention:

- `configs/stage1b_freeze.yaml`
- `configs/stage1c_freeze.yaml`
- `cc_stage1b_freeze_*`
- `cc_stage1c_freeze_*`

Reserve:

- `configs/commoncrawl_collection.yaml`
- `cc_collection_*`

for the post-Stage-1 final collection workflow.

### 3. Outputs should be reproducible
A run should be reproducible from:

- the relevant stage config
- the current code in `src/`
- the manifest where applicable
- the documented command used to run the stage

For active Stage 1c, the main command is:

```bash
make cc_stage1c_freeze_process
```

### 4. Raw data must remain immutable
Nothing under `data/raw/` should be edited in place.

Derived outputs belong in stage-appropriate `data/interim/...` locations.

### 5. Validation should be proportionate
The repo does not need a heavy formal test framework.

The acceptable bar is:

- smoke checks on the actual pipeline
- manual review of key outputs
- targeted sanity checks when logic changes
- clear logging and inspectable artifacts

For this project, a good validation habit is stronger than a large testing surface.

### 6. Performance matters where it actually matters
Performance work is justified when it affects:

- WET scanning time
- repeated I/O
- regex matching over large corpora
- unnecessary reruns of expensive stages

Performance work is not justified when it only improves tiny startup costs or makes the code harder to understand.

Rule of thumb:

- optimise the scan path
- do not micro-optimise everything else

### 7. Documentation should support defence, not bureaucracy
Documentation should be sufficient for:

- you to resume work later
- your supervisor/examiners to understand the pipeline at a high level
- future stage work to remain consistent

Good documentation for this repo means:

- current strategy document aligned with implementation
- stage decisions recorded
- config and command naming explained
- important output conventions documented

It does not require extensive internal developer documentation.

## Change quality bar
A code change is good enough when it:

- solves a real pipeline need
- preserves or improves clarity
- does not silently break stage conventions
- keeps outputs inspectable
- avoids unnecessary complexity
- is reflected in the relevant docs if the workflow changed

## Definition of done
A pipeline change is "done" when:

- the implementation works in the intended environment
- the correct stage config is used explicitly
- outputs land in the expected directories
- naming is aligned across code, config, Makefile, and docs
- no obsolete commands or files are left behind
- the change can be explained simply

## Preferred engineering style
Prefer:

- small modules
- explicit configs
- simple Makefile entrypoints
- direct CLI commands
- stage-specific naming
- minimal dependencies
- clear file outputs

Avoid:

- abstraction for its own sake
- premature generalisation
- duplicate workflows
- dead files and placeholder folders
- hidden defaults that make stage selection ambiguous
- feature creep beyond dissertation needs

## Dissertation-specific standard
This repo should look like the work of a careful MSc researcher who can justify their pipeline design.

That means the code should be:

- serious
- tidy
- reproducible
- computationally sensible

It does not need to look like a production system built by a software engineering team.

## Final rule
When choosing between two implementations, prefer the one that is:

- easier to explain
- easier to rerun
- easier to verify
- less fragile
- adequate for the dissertation scope
