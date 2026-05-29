# Common Crawl Single-Year Collection Runbook

This runbook is the operational guide for running one year, track, and batch. Use it for
smoke tests, targeted reruns, failure recovery, and corpus expansion batches.

## 1. Prepare EC2

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

Use `tmux` for long runs:

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

## 4. Run One Year Or Batch

Set the run parameters:

```bash
YEAR=2024
TRACK=corpus
BATCH=1
CONFIG=configs/commoncrawl_collection.yaml
```

Run the configured year/track/batch:

```bash
make collection_year YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
```

Convenience aliases:

```bash
make trend_year YEAR=2024 CONFIG=configs/commoncrawl_collection.yaml
make corpus_year YEAR=2024 CONFIG=configs/commoncrawl_collection.yaml
make corpus_expand YEAR=2024 BATCH=2 CONFIG=configs/commoncrawl_collection.yaml
```

Use:

- `trend_year` for a fixed annual trend sample.
- `corpus_year` for corpus batch 1.
- `corpus_expand` for additional deterministic corpus batches.

Single-year and expansion runs do not rebuild final processed outputs by default. After
all required corpus expansion batches are complete, build the processed corpus once and
upload it to the configured S3 collection prefix:

```bash
make corpus_build_processed CONFIG=configs/commoncrawl_collection.yaml
```

## 5. Inspect Results

For trend:

```bash
BASE=data/interim/collection/trend_working/$YEAR
find "$BASE/quality" -maxdepth 1 -name 'cc_collection_summary_*.csv' | sort | tail -n1 | xargs cat
find "$BASE/quality" -maxdepth 1 -name 'cc_collection_term_summary_*.csv' | sort | tail -n1 | xargs cat
find "$BASE/metrics" -maxdepth 1 -name 'cc_collection_throughput_summary_*.csv' | sort | tail -n1 | xargs cat
```

For corpus:

```bash
BPAD=$(printf "%03d" "$BATCH")
BASE=data/interim/collection/corpus_working/$YEAR/batch_$BPAD
find "$BASE/quality" -maxdepth 1 -name 'cc_collection_summary_*.csv' | sort | tail -n1 | xargs cat
find "$BASE/quality" -maxdepth 1 -name 'cc_collection_term_summary_*.csv' | sort | tail -n1 | xargs cat
find "$BASE/metrics" -maxdepth 1 -name 'cc_collection_throughput_summary_*.csv' | sort | tail -n1 | xargs cat
```

Validation samples are written as:

```text
quality/cc_val_sample30_<runid>.csv
```

## 6. Sync Outputs Locally

Do not use GitHub for collection outputs. Sync data artifacts from S3 to your local
machine:

```bash
cd /Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak
aws s3 sync s3://msc-nlp-therapy-speak-823916751170-us-east-1-an/msc-nlp-therapy-speak/collection/ data/interim/collection/ --exclude "processed/*" --exclude "*.parquet"
aws s3 sync s3://msc-nlp-therapy-speak-823916751170-us-east-1-an/msc-nlp-therapy-speak/collection/processed/ data/processed/
```

Use a narrower S3 prefix if you only need one track, year, or batch.

## 7. Basic Recovery

If a run fails, inspect the latest logs:

```bash
ls -lt reports/logs | head -30
tail -120 "$(ls -t reports/logs/*.log | head -1)"
```

Check whether any collection process is still running:

```bash
ps aux | grep -E 'python -m src.cli cc-collection-run-year|make corpus_expand|make collection_year' | grep -v grep
```

Rerun the same year or batch with the same command. Examples:

```bash
make trend_year YEAR=2024 CONFIG=configs/commoncrawl_collection.yaml
make corpus_year YEAR=2024 CONFIG=configs/commoncrawl_collection.yaml
make corpus_expand YEAR=2024 BATCH=2 CONFIG=configs/commoncrawl_collection.yaml
```

For failed corpus expansion batches, only delete partial local files if you have confirmed
the batch did not produce a valid `quality/cc_collection_summary_*.csv`.
