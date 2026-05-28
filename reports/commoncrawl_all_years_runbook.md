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

Before connecting to EC2, make sure your local code changes are committed and pushed:

```bash
cd /Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak
git status --short
git push origin main
```

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

Confirm EC2 is on the expected code:

```bash
git log --oneline -3
git status --short
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
raw-WET disk headroom, and index-server state.

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

If the session already exists, attach to it rather than creating a second collection
session.

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

Run trend first. It is the lower-risk full-history pass and gives the diachronic rate
denominators before the larger corpus pass.

## Run All Corpus Years

The corpus command runs batch 1 for every configured year:

```bash
make corpus CONFIG=configs/commoncrawl_collection.yaml
```

This is the corpus first pass. After it completes, inspect annual target-group counts:

```bash
find data/interim/collection/corpus_working \
  -path "*/quality/cc_collection_summary_*.csv" | sort
```

For each year, check:

```text
final.term_group.adhd.doc_count
final.term_group.autism.doc_count
```

## Expand Corpus Years

If a year needs more target-group documents after batch 1, run additional
deterministic batches:

```bash
make corpus_expand YEAR=2024 BATCH=2 CONFIG=configs/commoncrawl_collection.yaml
make corpus_expand YEAR=2024 BATCH=3 CONFIG=configs/commoncrawl_collection.yaml
```

Batch 1 uses:

```text
collection.corpus.initial_wet_batch_size
```

Additional expansion batches use:

```text
collection.corpus.expansion_wet_batch_size
```

Track cumulative WET files per year against your external collection policy. Each
successful `corpus_expand` run writes that batch's collection artifacts only. Build the
processed corpus once after the full expansion plan completes:

```bash
python -m src.cli cc-collection-build-corpus --config configs/commoncrawl_collection.yaml
```

Use expansion only for years that need additional target-group documents under that
external policy.

Raw WET files and detailed parquet handoff files are transient working inputs. After a
year completes successfully, the runner removes that year's local WET batch and local
WARC parquets automatically. Corpus text parquets are kept only until they have been
absorbed into `data/processed/corpus/corpus_documents.parquet`, then removed locally.
The CSV/JSON summaries, manifests, validation samples, and final processed outputs remain
durable.

## Output Layout

On EC2, working outputs are written under track-specific working directories.

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

The high-level runners also upload key outputs to S3. By default, sync only lightweight
CSV/JSON summaries, manifests, validation samples, and final processed outputs locally.
Detailed WARC and per-batch quality parquets are not uploaded by the high-level runner;
they are local handoff files that are cleaned after successful downstream handoff.

After the lightweight S3 sync, the local interim mirror has this layout:

```text
data/interim/collection/url_exports/<TRACK>/<YEAR>/batch_<BATCH>/<url_export_runid>/
data/interim/collection/pointer_cache/<TRACK>/<YEAR>/batch_<BATCH>/<url_export_runid>/
data/interim/collection/warc_output/<TRACK>/<YEAR>/batch_<BATCH>/<url_export_runid>/
data/interim/collection/quality/<TRACK>/<YEAR>/batch_<BATCH>/<quality_runid>/
data/interim/collection/metrics/<TRACK>/<YEAR>/batch_<BATCH>/<runid>/
```

The final document-quality gate summaries, term summaries, validation samples, and
WARC summaries are in the synced `warc_output/.../<url_export_runid>/` and
`quality/.../<quality_runid>/` folders. Throughput summaries and run manifests are in the
synced `metrics/.../<runid>/` folders. Detailed WARC and per-batch quality parquets are
not retained by the high-level runner.

Final processed outputs are not part of the interim mirror. Sync them separately into:

```text
data/processed/trend/trend_rates.csv
data/processed/corpus/corpus_documents.parquet
```

## Sync Outputs To Local Machine

Do not push collection outputs through GitHub. GitHub is only for tracked code, config,
and documentation. After EC2 collection runs, sync data artifacts from S3 on your local
machine:

```bash
cd /Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak
aws s3 sync s3://msc-nlp-therapy-speak-823916751170-us-east-1-an/msc-nlp-therapy-speak/collection/ data/interim/collection/ --exclude "processed/*" --exclude "*.parquet"
aws s3 sync s3://msc-nlp-therapy-speak-823916751170-us-east-1-an/msc-nlp-therapy-speak/collection/processed/ data/processed/
```

Use a narrower S3 prefix if you only need a specific track, year, or batch.

If a diagnostic question requires detailed parquet artifacts, rerun the affected
year/batch with the lower-level extraction or quality command and inspect the local files
before returning to the high-level runner. The standard S3 mirror intentionally keeps only
lightweight summaries/manifests and final processed outputs.

After syncing, inspect synced document-quality gate outputs locally with:

```bash
TRACK=trend
YEAR=2024
BATCH=1
BPAD=$(printf "%03d" "$BATCH")

find "data/interim/collection/quality/$TRACK/$YEAR/batch_$BPAD" \
  -name "cc_collection_summary_*.csv" | sort | tail -n1 | xargs cat
find "data/interim/collection/quality/$TRACK/$YEAR/batch_$BPAD" \
  -name "cc_collection_term_summary_*.csv" | sort | tail -n1 | xargs cat
find "data/interim/collection/quality/$TRACK/$YEAR/batch_$BPAD" \
  -name "cc_val_sample30_*.csv" | sort | tail -n1
find "data/interim/collection/metrics/$TRACK/$YEAR/batch_$BPAD" \
  -name "cc_collection_throughput_summary_*.csv" | sort | tail -n1 | xargs cat
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

Each successful WARC stage is checked against the configured acceptance thresholds:

```text
collection.warc_validation.acceptance.warn_fetch_success_rate_pct
collection.warc_validation.acceptance.min_fetch_success_rate_pct
collection.warc_validation.acceptance.warn_extract_success_rate_pct
collection.warc_validation.acceptance.min_extract_success_rate_pct
```

Inspect the latest throughput summary for `warc.fetch_success_rate_pct` and
`warc.extract_success_rate_pct` when a year warns or fails before proceeding.

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
make collection_year YEAR=2024 TRACK=corpus BATCH=1 CONFIG=configs/commoncrawl_collection.yaml
```

Existing WET downloads are skipped only while the raw files are still present. After a
successful year, local raw WET files are cleaned up automatically and a later rerun will
redownload them from the saved deterministic manifest. Later steps write new timestamped
outputs rather than overwriting old files.

If you run an ad hoc recovery loop manually, make it fail fast:

```bash
for YEAR in 2016 2017 2018; do
  make trend_year YEAR=$YEAR CONFIG=configs/commoncrawl_collection.yaml || break
done

for YEAR in 2016 2017 2018; do
  make corpus_year YEAR=$YEAR CONFIG=configs/commoncrawl_collection.yaml || break
done
```

For a failed expansion batch, rerun the same deterministic batch number:

```bash
make corpus_expand YEAR=2024 BATCH=2 CONFIG=configs/commoncrawl_collection.yaml
```

If the failed batch left partial local working files and raw WET downloads, inspect them
first and then remove only that failed batch before rerunning:

```bash
find data/interim/collection/corpus_working/2024/batch_002 -type f | sort
ls data/raw/wet/*corpus_2024_b002*.wet.gz 2>/dev/null
rm -rf data/interim/collection/corpus_working/2024/batch_002
rm -f data/raw/wet/*corpus_2024_b002*.wet.gz data/manifests/cc_collection_wet_corpus_2024_b002_*.jsonl
```
