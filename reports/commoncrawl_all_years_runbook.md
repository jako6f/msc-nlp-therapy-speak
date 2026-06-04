# Common Crawl All-Years Collection Runbook

This runbook is the operational guide for running the configured Common Crawl collection
across all years in `configs/commoncrawl_collection.yaml`.

Use it after a successful single-year test run.

## Tracks

- `trend`: fixed-size annual samples used for diachronic rate estimates.
- `corpus`: larger annual samples used for the target-term analysis corpus.

## 1. Prepare EC2

Before connecting to EC2, commit and push local code changes:

```bash
cd /Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak
git status --short
git push origin main
```

Connect to EC2:

```bash
ssh -i /path/to/commoncrawl-collection-key.pem ec2-user@YOUR_EC2_HOSTNAME
```

Set up the repo and environment:

```bash
cd ~/msc-nlp-therapy-speak
source ~/miniconda3/etc/profile.d/conda.sh
conda activate msc-nlp
git pull origin main
git status --short
```

Confirm the local AWS config exists:

```bash
test -f configs/local/aws.yaml
```

## 2. Preflight

Run:

```bash
make sanity
make collection_preflight
```

If preflight reports an old collection index server, stop it:

```bash
make collection_stop_index_server
```

If port `8080` is occupied by something unexpected, inspect it before killing anything:

```bash
ss -ltnp | grep ':8080' || true
```

## 3. Use Tmux

Long collection runs should run inside `tmux`:

```bash
tmux new -s collection
```

Optional but useful: keep failed panes visible for diagnosis.

```bash
tmux set-option remain-on-exit on
```

Detach with `Ctrl-b`, then `d`. Reattach with:

```bash
tmux attach -t collection
```

## 4. Run Trend

Run all configured trend years:

```bash
make trend CONFIG=configs/commoncrawl_collection.yaml
```

This writes the final trend output:

```text
data/processed/trend/trend_rates.csv
```

The trend track estimates annual term rates from accepted WARC-validated hit summaries.
It does not run the heavier corpus document-quality stage by default.

## 5. Run Corpus First Pass

Run corpus batch 1 for all configured years:

```bash
make corpus CONFIG=configs/commoncrawl_collection.yaml
```

This builds the processed corpus once after all configured first-pass years complete.

Inspect annual target-group counts:

```bash
find data/interim/collection/corpus -path '*/quality/cc_collection_summary_*.csv' | sort
```

For each year, check at least:

```text
final.term_group.adhd.doc_count
final.term_group.autism.doc_count
```

## 6. Expand Corpus Years

If a year needs more target-group documents, run deterministic expansion batches:

```bash
make corpus_expand YEAR=2024 BATCH=2 CONFIG=configs/commoncrawl_collection.yaml
make corpus_expand YEAR=2024 BATCH=3 CONFIG=configs/commoncrawl_collection.yaml
```

Batch sizing is controlled by:

```text
collection.corpus.initial_wet_batch_size
collection.corpus.expansion_wet_batch_size
```

`make corpus_expand` does not rebuild the final processed corpus. After all expansion
batches are complete, build it once and upload it to the configured S3 collection
prefix:

```bash
make corpus_build_processed CONFIG=configs/commoncrawl_collection.yaml
```

## 7. Monitor Progress

In another SSH session:

```bash
cd ~/msc-nlp-therapy-speak
tail -f reports/logs/*.log
```

List completed quality summaries:

```bash
find data/interim/collection -path '*/quality/cc_collection_summary_*.csv' | sort
```

List completed run manifests:

```bash
find data/interim/collection -path '*/metrics/cc_collection_run_manifest_*.json' | sort
```

Check disk space during long runs:

```bash
df -h .
```

## 8. Sync Outputs Locally

Do not use GitHub for collection outputs. Sync data artifacts from S3 to your local
machine:

```bash
cd /Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak
aws s3 sync s3://msc-nlp-therapy-speak-823916751170-us-east-1-an/msc-nlp-therapy-speak/collection/ data/interim/collection/ --exclude "processed/*" --exclude "*.parquet"
aws s3 sync s3://msc-nlp-therapy-speak-823916751170-us-east-1-an/msc-nlp-therapy-speak/collection/processed/ data/processed/
```

Use a narrower S3 prefix if you only need one track, year, or batch.

## 9. Basic Recovery

If a run fails, first inspect the latest logs:

```bash
ls -lt reports/logs | head -30
tail -120 "$(ls -t reports/logs/*.log | head -1)"
```

Check whether any collection process is still running:

```bash
ps aux | grep -E 'python -m src.cli cc-collection-run-year|make corpus_expand|make collection_year' | grep -v grep
```

Rerun the affected year or batch with the same command. Examples:

```bash
make trend_year YEAR=2024 CONFIG=configs/commoncrawl_collection.yaml
make corpus_year YEAR=2024 CONFIG=configs/commoncrawl_collection.yaml
make corpus_expand YEAR=2024 BATCH=2 CONFIG=configs/commoncrawl_collection.yaml
```

For failed corpus expansion batches, only delete partial local files if you have confirmed
the batch did not produce a valid `quality/cc_collection_summary_*.csv`.
