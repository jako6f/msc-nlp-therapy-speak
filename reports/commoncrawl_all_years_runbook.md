# Common Crawl All-Years Collection Runbook

This runbook explains how to run the configured Common Crawl collection across all
years in `configs/commoncrawl_collection.yaml`.

Use this after a successful single-year test run.

## Scope

There are two collection tracks:

- `trend`: fixed-size annual samples for diachronic rate estimates.
- `corpus`: larger annual samples for the target-term analysis corpus.

The all-years commands run the complete pipeline for each configured year:

```text
sample WET files
download WET files
scan WET text
export and upload URL candidates
install Common Crawl secondary indexes
start the local index server
resolve WARC pointers
fetch WARC HTML and extract main text
run document quality, English filtering, and deduplication
build processed outputs
```

For each year, the orchestrator installs the relevant Common Crawl index files and
restarts only the index-server process recorded in the configured pidfile before WARC
pointer resolution. This avoids stale server state when moving between crawls.

## Prerequisites

Run on the EC2 collection host.

From your local machine:

```bash
ssh -i /path/to/commoncrawl-collection-key.pem ec2-user@YOUR_EC2_HOSTNAME
```

On EC2:

```bash
cd ~/msc-nlp-therapy-speak
source ~/miniconda3/etc/profile.d/conda.sh
conda activate msc-nlp
git pull origin main
```

Confirm the local AWS config exists:

```bash
test -f configs/local/aws.yaml
```

## Preflight

Run:

```bash
make sanity
make collection_preflight
```

Preflight checks the merged config, AWS identity, S3 write/delete access, configured years,
and index-server state.

If the configured pidfile points to an old collection index server, stop it:

```bash
make collection_stop_index_server
```

Do not blindly kill arbitrary processes on port `8080`. If needed, inspect first:

```bash
ss -ltnp | grep ':8080' || true
```

## Run In Tmux

All-years collection should run inside `tmux`:

```bash
tmux new -s collection
```

Reattach after disconnecting:

```bash
tmux attach -t collection
```

## Run All Trend Years

The trend track uses the fixed annual WET sample size configured at:

```text
collection.trend.fixed_wet_files_per_year
```

Run:

```bash
make trend CONFIG=configs/commoncrawl_collection.yaml
```

This runs every configured year once and refreshes:

```text
data/processed/trend/trend_rates.csv
```

## Run All Corpus Years

The corpus command runs batch 1 for every configured year:

```bash
make corpus CONFIG=configs/commoncrawl_collection.yaml
```

This is the corpus first pass. After it completes, inspect annual target-group counts:

```bash
find data/interim/collection/corpus_working -path "*/quality/cc_collection_summary_*.csv" | sort
```

For each year, check:

```text
final.term_group.adhd.doc_count
final.term_group.autism.doc_count
```

The configured target is:

```text
collection.corpus.target_docs_per_target_group_year
```

The configured soft minimum is:

```text
collection.corpus.soft_min_docs_per_target_group_year
```

## Expand Corpus Years

If a year is below target after batch 1, run additional deterministic batches:

```bash
make corpus_expand YEAR=2024 BATCH=2 CONFIG=configs/commoncrawl_collection.yaml
make corpus_expand YEAR=2024 BATCH=3 CONFIG=configs/commoncrawl_collection.yaml
```

Each corpus batch uses:

```text
collection.corpus.initial_wet_batch_size
```

The current first-pass cap is:

```text
collection.corpus.max_wet_files_per_year_first_pass
```

With 50-WET batches and a 250-WET cap, the practical first-pass maximum is five batches
per year. Each successful `corpus_expand` run refreshes the processed corpus output.

## Output Layout

Trend working outputs:

```text
data/interim/collection/trend_working/<YEAR>/
```

Corpus working outputs:

```text
data/interim/collection/corpus_working/<YEAR>/batch_<BATCH>/
```

Processed outputs:

```text
data/processed/trend/trend_rates.csv
data/processed/corpus/corpus_documents.parquet
```

Important per-run files:

```text
quality/cc_collection_summary_<runid>.csv
quality/cc_collection_term_summary_<runid>.csv
quality/cc_val_sample30_<runid>.csv
metrics/cc_collection_throughput_summary_<runid>.csv
metrics/cc_collection_run_manifest_<runid>.json
```

## Monitoring

In another SSH session:

```bash
cd ~/msc-nlp-therapy-speak
tail -f reports/logs/*.log
```

Useful high-level checks:

```bash
find data/interim/collection -path "*/quality/cc_collection_summary_*.csv" | sort
find data/interim/collection -path "*/metrics/cc_collection_throughput_summary_*.csv" | sort
```

## Runtime Expectations

Runtime depends on EC2 instance type, network throughput, and WET/WARC yield. Based on
2024 smoke tests on the project EC2 instance, a 50-WET run took roughly 70 minutes.
Larger corpus batches scale approximately linearly.

The limiting factor is usually wall-clock runtime, not S3 cost.

## Failure Recovery

The all-years command runs years sequentially. If a run fails, inspect:

```text
reports/logs/
data/interim/collection/.../metrics/cc_collection_run_manifest_<runid>.json
```

Then either rerun the all-years command or rerun the affected year explicitly:

```bash
make collection_year YEAR=2024 TRACK=trend BATCH=1 CONFIG=configs/commoncrawl_collection.yaml
```

Existing WET downloads are skipped. Later steps write new timestamped outputs rather than
overwriting old files.
