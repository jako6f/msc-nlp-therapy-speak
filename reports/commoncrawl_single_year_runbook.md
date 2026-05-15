# Common Crawl Single-Year Collection Runbook

This runbook explains how to run the full pipeline for one year, track, and batch.
Use it for smoke tests, targeted reruns, failure recovery, and corpus expansion batches.

## Scope

The single-year command runs the complete pipeline:

```text
sample WET files
download WET files
scan WET text for candidate hits
export and upload URL candidates
install Common Crawl secondary indexes
start the local index server
resolve WARC pointers
fetch WARC HTML and extract main text
run document quality, English filtering, and deduplication
refresh processed trend/corpus outputs
```

## Prerequisites

Run on the EC2 collection host unless you are deliberately testing local-only code.

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

The local AWS config must exist on the EC2 host:

```bash
test -f configs/local/aws.yaml
```

That file is intentionally not tracked by Git. It supplies the private S3 bucket/prefix
and any local AWS details needed by `configs/commoncrawl_collection.yaml`.

## Preflight

Run preflight before a collection run:

```bash
make sanity
make collection_preflight
```

`make collection_preflight` checks:

- The configured collection years.
- The configured S3 bucket and prefix.
- AWS caller identity.
- S3 write/delete access under the configured prefix.
- Available disk headroom before local raw-WET download.
- The local index-server port and pidfile state.

If port `8080` is already occupied by an old collection index server, stop the recorded
pidfile process:

```bash
make collection_stop_index_server
```

If another unrelated process is using port `8080`, inspect it manually before killing it:

```bash
ss -ltnp | grep ':8080' || true
```

## Run In Tmux

Use `tmux` for long runs:

```bash
tmux new -s collection
```

If your SSH connection drops:

```bash
tmux attach -t collection
```

## Run One Year

Set the year, track, and batch explicitly:

```bash
YEAR=2024
TRACK=trend
BATCH=1
CONFIG=configs/commoncrawl_collection.yaml
```

Run the complete single-year pipeline:

```bash
make collection_year YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
```

After installing the relevant Common Crawl index files, this command restarts only the
index-server process recorded in the configured pidfile. This avoids stale server state
when moving between crawls.

Convenience aliases are available:

```bash
make trend_year YEAR=2024 CONFIG=configs/commoncrawl_collection.yaml
make corpus_year YEAR=2024 CONFIG=configs/commoncrawl_collection.yaml
make corpus_expand YEAR=2024 BATCH=2 CONFIG=configs/commoncrawl_collection.yaml
```

Use `trend_year` for a fixed annual trend sample. Use `corpus_year` for corpus batch 1.
Use `corpus_expand` for additional deterministic corpus batches after inspecting whether
the year has reached the target document counts.

Raw WET files are transient working inputs. After the year completes successfully, the
runner removes that year's local WET batch automatically. The deterministic manifest and
downstream outputs remain durable; if the year fails before completion, its WET files are
kept for immediate recovery.

## Outputs

On EC2, working outputs are written under the track-specific working directory.

For `TRACK=trend`, outputs are written under:

```text
data/interim/collection/trend_working/<YEAR>/
```

For `TRACK=corpus`, outputs are written under:

```text
data/interim/collection/corpus_working/<YEAR>/batch_<BATCH>/
```

The most useful files are:

```text
wet_scan/cc_scan_summary_<runid>.csv
url_exports/cc_collection_url_upload_manifest_<runid>.json
pointer_cache/cc_pointer_cache_summary_<runid>.csv
warc/cc_collection_summary_<runid>.csv
quality/cc_collection_summary_<runid>.csv
quality/cc_collection_term_summary_<runid>.csv
quality/cc_val_sample30_<runid>.csv
metrics/cc_collection_throughput_summary_<runid>.csv
metrics/cc_collection_run_manifest_<runid>.json
```

Processed outputs are refreshed after a successful run:

```text
data/processed/trend/trend_rates.csv
data/processed/corpus/corpus_documents.parquet
```

The high-level runner also uploads the key outputs to S3 so they can be synced back to
your local machine. The S3 mirror uses top-level collection folders, not the EC2 working
folder names:

```text
url_exports/<TRACK>/<YEAR>/batch_<BATCH>/<url_export_runid>/
pointer_cache/<TRACK>/<YEAR>/batch_<BATCH>/<url_export_runid>/
warc_output/<TRACK>/<YEAR>/batch_<BATCH>/<url_export_runid>/
quality/<TRACK>/<YEAR>/batch_<BATCH>/<quality_runid>/
metrics/<TRACK>/<YEAR>/batch_<BATCH>/<runid>/
processed/<TRACK>/
```

The final document-quality gate summaries, term summaries, validation samples, and
filtered parquet outputs are in the synced `quality/.../<quality_runid>/` folder. The
throughput summary and run manifest are in the synced `metrics/.../<runid>/` folder.

## Sync Outputs To Local Machine

Do not use GitHub to move collection outputs from EC2 to your local machine. GitHub is
only for tracked code, config, and documentation. After a successful EC2 run, sync data
artifacts from S3 on your local machine:

```bash
cd /Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak
aws s3 sync \
  s3://msc-nlp-therapy-speak-823916751170-us-east-1-an/msc-nlp-therapy-speak/collection/ \
  data/interim/collection/
```

Use a narrower S3 prefix if you only need one year, track, or batch.

After syncing, the local S3-mirrored quality outputs are under:

```text
data/interim/collection/quality/<TRACK>/<YEAR>/batch_<BATCH>/<quality_runid>/
```

The synced throughput summary and run manifest are under:

```text
data/interim/collection/metrics/<TRACK>/<YEAR>/batch_<BATCH>/<runid>/
```

For example:

```bash
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

## Inspect Latest Results

For trend:

```bash
BASE=data/interim/collection/trend_working/$YEAR
find "$BASE/quality" -maxdepth 1 -type f -name "cc_collection_summary_*.csv" | sort | tail -n1 | xargs cat
find "$BASE/quality" -maxdepth 1 -type f -name "cc_collection_term_summary_*.csv" | sort | tail -n1 | xargs cat
find "$BASE/metrics" -maxdepth 1 -type f -name "cc_collection_throughput_summary_*.csv" | sort | tail -n1 | xargs cat
find "$BASE/quality" -maxdepth 1 -type f -name "cc_val_sample30_*.csv" | sort | tail -n1
```

For corpus:

```bash
BPAD=$(printf "%03d" "$BATCH")
BASE=data/interim/collection/corpus_working/$YEAR/batch_$BPAD
find "$BASE/quality" -maxdepth 1 -type f -name "cc_collection_summary_*.csv" | sort | tail -n1 | xargs cat
find "$BASE/quality" -maxdepth 1 -type f -name "cc_collection_term_summary_*.csv" | sort | tail -n1 | xargs cat
find "$BASE/metrics" -maxdepth 1 -type f -name "cc_collection_throughput_summary_*.csv" | sort | tail -n1 | xargs cat
find "$BASE/quality" -maxdepth 1 -type f -name "cc_val_sample30_*.csv" | sort | tail -n1
```

## Failure Recovery

The high-level command is the default interface. If a run fails mid-way, inspect:

```text
reports/logs/
data/interim/collection/.../metrics/cc_collection_run_manifest_<runid>.json
```

If the failure happened before WARC resolution, rerunning the same high-level command is
usually acceptable. Existing WET downloads are skipped while the raw files are still
present; after a successful year, a later rerun redownloads them from the saved manifest.

If the failure happened after URL upload and you need specialist manual recovery, inspect
the relevant log and manifest first, then call the underlying CLI command directly with the
exact S3 URI or local path you want to resume from. These recovery commands are intentionally
not exposed as public Make targets because the high-level command is the supported interface.
