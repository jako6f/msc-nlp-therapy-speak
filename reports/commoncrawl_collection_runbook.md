# Common Crawl Collection Runbook

This guide executes the final Common Crawl collection pipeline. The canonical execution environment is EC2. Use the Mac only to connect to EC2, push/pull code through GitHub, and optionally download final outputs for inspection or analysis.

## 1. Execution Model

Run the full pipeline on EC2:

- WET file sampling and download
- WET scan and URL export
- S3 URL upload
- Common Crawl index installation
- local index-server startup
- WARC pointer resolution
- WARC HTML retrieval and extraction
- document-quality filtering
- English filtering and deduplication
- processed trend/corpus output construction

This avoids manual local-to-remote code syncing and keeps the data-producing workflow in one environment. The repo should be updated on EC2 with `git pull`.

Run long collection commands inside `tmux`. This keeps the pipeline running if SSH drops, the Mac sleeps, or the terminal closes.

## 2. Connect To EC2

Start the EC2 instance from the AWS console if it is stopped. Then connect from the Mac:

```bash
ssh -i /Users/jakoblutkemeier/Desktop/aws/commoncrawl-collection-key.pem ec2-user@ec2-3-88-136-254.compute-1.amazonaws.com
```

On EC2:

```bash
cd ~/msc-nlp-therapy-speak
source ~/miniconda3/etc/profile.d/conda.sh
conda activate msc-nlp
git pull --ff-only
```

If the repo is missing or is not a Git clone, clone it from GitHub first:

```bash
cd ~
git clone git@github.com:jako6f/msc-nlp-therapy-speak.git
cd ~/msc-nlp-therapy-speak
```

If GitHub SSH authentication fails, configure the EC2 instance's SSH key in GitHub before continuing. Do not manually copy the repo from the Mac for collection runs.

## 3. Run Inside tmux

Use one persistent `tmux` session for collection work. All long-running commands in this runbook should be run inside that session.

Start a new session:

```bash
tmux new -s collection
```

If the session already exists, attach to it instead:

```bash
tmux attach -t collection
```

Inside `tmux`, set up the repo and environment:

```bash
cd ~/msc-nlp-therapy-speak
source ~/miniconda3/etc/profile.d/conda.sh
conda activate msc-nlp
git pull --ff-only
```

Detach from `tmux` before closing the laptop:

```text
Ctrl-b
d
```

That means press `Ctrl-b`, release both keys, then press `d`. The command keeps running on EC2 after detaching.

Reconnect later:

```bash
ssh -i /Users/jakoblutkemeier/Desktop/aws/commoncrawl-collection-key.pem ec2-user@ec2-3-88-136-254.compute-1.amazonaws.com
tmux attach -t collection
```

Useful `tmux` commands:

```bash
tmux ls
```

Lists active sessions.

```bash
tmux attach -t collection
```

Reopens the collection session.

```bash
tmux new -s collection
```

Creates the session if it does not already exist.

```bash
exit
```

Closes the current shell. Only use this when no pipeline command is running and you intend to close the session.

Do not start the same year/track/batch twice in different terminals. If unsure, attach to the existing session and inspect what is running before launching another command.

## 4. Required Local Config On EC2

Personal AWS values must live in the ignored local config:

```bash
configs/local/aws.yaml
```

Create it from the example if needed:

```bash
cp configs/local/aws.example.yaml configs/local/aws.yaml
```

Then edit the copied file with EC2-local AWS values. The committed config intentionally does not contain personal bucket names, SSH key names, instance profiles, or local key paths.

The pipeline automatically merges `configs/local/aws.yaml` into `configs/commoncrawl_collection.yaml`. Alternatively, point to another local override:

```bash
export MSC_NLP_LOCAL_CONFIG=/path/to/local/aws.yaml
```

## 5. Preflight Checks

Run these on EC2 from the repo root, preferably inside `tmux`:

```bash
git status --short
git log --oneline -5
test -f configs/local/aws.yaml && echo "local aws config found"
make sanity
aws sts get-caller-identity
```

Verify that the merged config contains a bucket and prefix:

```bash
python -c 'from pathlib import Path; from src.cli import load_config; cfg=load_config(Path("configs/commoncrawl_collection.yaml")); s3=cfg["collection"]["aws"]["s3"]; print("bucket:", s3.get("bucket")); print("prefix:", s3.get("prefix")); assert s3.get("bucket"), "Missing collection.aws.s3.bucket"'
```

Test S3 write access:

```bash
BUCKET=$(python -c 'from pathlib import Path; from src.cli import load_config; cfg=load_config(Path("configs/commoncrawl_collection.yaml")); s3=cfg["collection"]["aws"]["s3"]; print("s3://" + s3["bucket"] + "/" + str(s3["prefix"]).strip("/"))')

echo "BUCKET=$BUCKET"
echo "test" > /tmp/msc_nlp_s3_test.txt
aws s3 cp /tmp/msc_nlp_s3_test.txt "$BUCKET/_smoke/msc_nlp_s3_test.txt"
aws s3 rm "$BUCKET/_smoke/msc_nlp_s3_test.txt"
```

## 6. Select And Audit Crawls

Run once after config changes, or when validating the crawl map:

```bash
CONFIG=configs/commoncrawl_collection.yaml
make collection_select_crawls CONFIG=$CONFIG
```

The selection step uses Common Crawl's official `collinfo.json`, selects the available crawl whose crawl midpoint is closest to April 15 of each year, and records WET/WARC/index availability evidence.

Outputs:

- `data/processed/manifests/`
- `reports/commoncrawl_collection_crawl_map.json`

## 7. Set Run Variables

Set these before each year/track/batch:

```bash
YEAR=2024
TRACK=trend
BATCH=1
CONFIG=configs/commoncrawl_collection.yaml
BPAD=$(printf "%03d" "$BATCH")
BUCKET=$(python -c 'from pathlib import Path; from src.cli import load_config; cfg=load_config(Path("configs/commoncrawl_collection.yaml")); s3=cfg["collection"]["aws"]["s3"]; print("s3://" + s3["bucket"] + "/" + str(s3["prefix"]).strip("/"))')
```

Use:

- `TRACK=trend` for the fixed yearly trend denominator/numerator track.
- `TRACK=corpus` for iterative target-corpus collection.
- `BATCH=1`, then `BATCH=2`, etc. for corpus expansion if more target documents are needed.

## 8. WET Acquisition, Scan, URL Export, And Upload

Run on EC2 inside `tmux`:

```bash
make collection_sample YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
make collection_download YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
make collection_scan YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
make collection_export_urls YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
make collection_upload_urls YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
```

Convenience targets:

```bash
make trend_year YEAR=$YEAR CONFIG=$CONFIG
make corpus_year YEAR=$YEAR BATCH=$BATCH CONFIG=$CONFIG
```

Find the URL-export run ID for this year/track/batch:

```bash
RUNID=$(find data/interim/collection -path "*/url_exports/cc_collection_urls_*.csv" \
  ! -name "*summary*" \
  | grep "/${YEAR}/" \
  | sort \
  | tail -n1 \
  | sed -E 's#.*cc_collection_urls_([0-9_]+)\.csv#\1#')

echo "RUNID=$RUNID"
```

For `corpus`, verify that the path includes `batch_$BPAD` if several batches exist:

```bash
find data/interim/collection -path "*/url_exports/cc_collection_urls_${RUNID}.csv" -print
```

## 9. Start Or Restart The Local Index Server

Before starting the index server, check whether port `8080` is occupied:

```bash
ss -ltnp | grep ':8080' || true
```

If a stale Python process is listening, kill the numeric PID shown by `ss`:

```bash
kill PID_FROM_SS_OUTPUT
sleep 2
ss -ltnp | grep ':8080' || true
```

Do not type `kill <PID>` literally. Replace `PID_FROM_SS_OUTPUT` with the numeric PID, for example `kill 7544`.

Install Common Crawl secondary indexes for the URLs in this run:

```bash
make collection_install_indexes \
  CONFIG=$CONFIG \
  URL_EXPORT_URI=$BUCKET/url_exports/$TRACK/$YEAR/batch_$BPAD/$RUNID/cc_collection_urls_$RUNID.csv
```

Start the local index server:

```bash
make collection_start_index_server CONFIG=$CONFIG
```

## 10. Resolve WARC Pointers

Run inside `tmux`:

```bash
make collection_resolve \
  YEAR=$YEAR \
  TRACK=$TRACK \
  BATCH=$BATCH \
  CONFIG=$CONFIG \
  URL_EXPORT_URI=$BUCKET/url_exports/$TRACK/$YEAR/batch_$BPAD/$RUNID/cc_collection_urls_$RUNID.csv \
  RESOLVE_OUTPUT_PREFIX=$BUCKET/pointer_cache/$TRACK/$YEAR/batch_$BPAD/$RUNID/
```

Inspect the pointer-cache summary:

```bash
find data/interim/collection -path "*/pointer_cache/cc_pointer_cache_summary_*.csv" \
  | grep "/${YEAR}/" \
  | sort \
  | tail -n1 \
  | xargs cat
```

Proceed only if `pointer_cache_resolved_rows` is high and `failed_query_count` is zero or explainable.

## 11. Extract WARC HTML

Run on EC2 inside `tmux`:

```bash
make collection_extract \
  YEAR=$YEAR \
  TRACK=$TRACK \
  BATCH=$BATCH \
  CONFIG=$CONFIG \
  POINTER_CACHE_URI=$BUCKET/pointer_cache/$TRACK/$YEAR/batch_$BPAD/$RUNID/cc_pointer_cache_$RUNID.parquet \
  WARC_OUTPUT_PREFIX=$BUCKET/warc_output/$TRACK/$YEAR/batch_$BPAD/$RUNID/
```

This fetches WARC HTML records, extracts main text, enriches publication dates with `htmldate`, writes local WARC outputs under `data/interim/collection`, and uploads the WARC outputs to S3.

Inspect:

```bash
find data/interim/collection -path "*/warc/cc_collection_summary_*.csv" \
  | grep "/${YEAR}/" \
  | sort \
  | tail -n1 \
  | xargs cat
```

## 12. Run Document Quality, English Filter, And Deduplication

Run on EC2 inside `tmux`:

```bash
make collection_quality YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
```

Inspect:

```bash
find data/interim/collection -path "*/quality/cc_collection_summary_*.csv" \
  | grep "/${YEAR}/" \
  | sort \
  | tail -n1 \
  | xargs cat

find data/interim/collection -path "*/quality/cc_collection_term_summary_*.csv" \
  | grep "/${YEAR}/" \
  | sort \
  | tail -n1 \
  | xargs cat

find data/interim/collection -path "*/quality/cc_val_sample30_*.csv" \
  | grep "/${YEAR}/" \
  | sort \
  | tail -n1

find data/interim/collection -path "*/metrics/cc_collection_throughput_summary_*.csv" \
  | grep "/${YEAR}/" \
  | sort \
  | tail -n1 \
  | xargs cat
```

Read the sample before scaling up if this is a new configuration, new environment, or first run after code changes.

## 13. Build Processed Outputs

Trend:

```bash
make collection_build_processed TRACK=trend CONFIG=$CONFIG
```

Corpus:

```bash
make collection_build_processed TRACK=corpus CONFIG=$CONFIG
```

Outputs:

- `data/processed/trend/trend_rates.csv`
- `data/processed/corpus/corpus_documents.parquet`
- `data/processed/manifests/`

## 14. Optional Sync Back To Mac

The pipeline does not require local syncing during collection. If you want to inspect outputs on the Mac, sync only the relevant outputs after EC2 processing:

```bash
aws s3 sync "$BUCKET/warc_output/$TRACK/$YEAR/batch_$BPAD/$RUNID/" \
  "data/interim/collection/${TRACK}_${YEAR}_batch_${BPAD}_warc_from_s3/"
```

For final outputs, prefer committing code/config only. Do not commit large generated data unless explicitly intended.

## 15. Multi-Year Execution Strategy

Trend:

- Run one fixed WET sample per configured year.
- Build `data/processed/trend/trend_rates.csv`.
- Report both WET and WARC rates:
  - `validated_hits_wet / docs_scanned`
  - `validated_hits_warc / docs_scanned`
  - `validated_hits_warc / validated_hits_wet`

Corpus:

- Run `BATCH=1` for each year first.
- Inspect survivor counts and validation samples.
- Add `BATCH=2`, `BATCH=3`, etc. only for years below the target.
- Aim for 1000 high-quality target-term survivors per year where feasible.
- Accept 500 as a soft minimum for sparse or expensive years.

Convenience commands for local-preparation phases across all configured years:

```bash
make trend CONFIG=$CONFIG
make corpus CONFIG=$CONFIG
```

Use these only when ready for a longer multi-year run. Pointer resolution, WARC extraction, quality filtering, and processed output construction should still be monitored year by year.

## 16. Budget And Stop Criteria

The pipeline records throughput metrics, but it does not enforce a dollar-denominated stop condition internally. Configure AWS Budgets separately for account-level alerts.

Stop or pause when:

- AWS budget alerts indicate spend is approaching the allocated cap.
- WARC extraction throughput becomes unexpectedly slow or expensive.
- validation samples show a material quality regression.
- a year reaches the target corpus size, or further batches show poor marginal yield.

Do not keep tuning filters against individual samples unless a repeated, corpus-threatening failure mode appears.
