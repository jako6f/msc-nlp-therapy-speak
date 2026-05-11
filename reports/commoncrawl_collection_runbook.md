# Common Crawl Collection Runbook

This guide executes the final collection pipeline. Commands are split by where they should run.

## 1. Local Setup

Run from the repository root on your Mac unless the section says EC2.

```bash
conda activate msc-nlp
git status --short
```

Personal AWS values are loaded from `configs/local/aws.yaml` if present. This file is ignored by git. Create it from the example if needed:

```bash
cp configs/local/aws.example.yaml configs/local/aws.yaml
```

Set the year, track, and batch you want to process.

```bash
YEAR=2020
TRACK=trend
BATCH=1
CONFIG=configs/commoncrawl_collection.yaml
```

Use `TRACK=corpus` for the iterative corpus collection.

## 2. Select Crawls

Run locally once after code/config changes.

```bash
make collection_select_crawls CONFIG=$CONFIG
```

This writes the frozen crawl map to `data/processed/manifests/`.

The selection step uses Common Crawl's official `collinfo.json`, selects the available crawl whose crawl midpoint is closest to April 15 of each year, and records WET/WARC/index availability evidence in the manifest. A versioned audit copy is also written to `reports/commoncrawl_collection_crawl_map.json`.

## 3. Local WET Preparation

Run locally or on EC2. EC2 is preferred for the full collection.

```bash
make collection_sample YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
make collection_download YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
make collection_scan YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
make collection_export_urls YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
make collection_upload_urls YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
```

Find the URL-export run ID.

```bash
RUNID=$(find data/interim/collection -path "*/url_exports/cc_collection_urls_*.csv" \
  ! -name "*summary*" \
  | sed -E 's#.*cc_collection_urls_([0-9_]+)\.csv#\1#' \
  | sort \
  | tail -n1)
echo "$RUNID"
```

## 4. Launch Or Reconnect To EC2

Start the EC2 instance from the AWS console if it is stopped. Then SSH from your Mac.

```bash
ssh -i /PATH/TO/YOUR_EC2_KEY.pem \
  ec2-user@YOUR_EC2_PUBLIC_DNS
```

On EC2:

```bash
cd ~/msc-nlp-therapy-speak
conda activate msc-nlp
git pull
```

If port 8080 is occupied by an old local index server, kill the PID shown by `ss`.

```bash
ss -ltnp | grep ':8080' || true
kill PID_FROM_SS_OUTPUT
sleep 2
ss -ltnp | grep ':8080' || true
```

Do not type `kill <PID>` literally. Replace `PID_FROM_SS_OUTPUT` with the numeric PID, for example `kill 7544`.

## 5. Resolve WARC Pointers On EC2

Set the same year, track, batch, and run ID on EC2.

```bash
YEAR=2020
TRACK=trend
BATCH=1
BPAD=$(printf "%03d" "$BATCH")
RUNID=YYYYMMDD_HHMMSS
CONFIG=configs/commoncrawl_collection.yaml
BUCKET=$(python - <<'PY'
from pathlib import Path
from src.cli import load_config

cfg = load_config(Path("configs/commoncrawl_collection.yaml"))
s3 = cfg["collection"]["aws"]["s3"]
print(f"s3://{s3['bucket']}/{str(s3['prefix']).strip('/')}")
PY
)
```

Install the relevant Common Crawl secondary indexes.

```bash
make collection_install_indexes \
  CONFIG=$CONFIG \
  URL_EXPORT_URI=$BUCKET/url_exports/$TRACK/$YEAR/batch_$BPAD/$RUNID/cc_collection_urls_$RUNID.csv
```

Start the local index server.

```bash
make collection_start_index_server CONFIG=$CONFIG
```

Resolve pointers.

```bash
make collection_resolve \
  YEAR=$YEAR \
  TRACK=$TRACK \
  BATCH=$BATCH \
  CONFIG=$CONFIG \
  URL_EXPORT_URI=$BUCKET/url_exports/$TRACK/$YEAR/batch_$BPAD/$RUNID/cc_collection_urls_$RUNID.csv \
  RESOLVE_OUTPUT_PREFIX=$BUCKET/pointer_cache/$TRACK/$YEAR/batch_$BPAD/$RUNID/
```

Sanity check the pointer-cache summary on EC2.

```bash
find data/interim/collection -path "*/pointer_cache/cc_pointer_cache_summary_*.csv" | sort | tail -n1
```

## 6. Extract WARC HTML On EC2

```bash
make collection_extract \
  YEAR=$YEAR \
  TRACK=$TRACK \
  BATCH=$BATCH \
  CONFIG=$CONFIG \
  POINTER_CACHE_URI=$BUCKET/pointer_cache/$TRACK/$YEAR/batch_$BPAD/$RUNID/cc_pointer_cache_$RUNID.parquet \
  WARC_OUTPUT_PREFIX=$BUCKET/warc_output/$TRACK/$YEAR/batch_$BPAD/$RUNID/
```

This writes WARC outputs locally on EC2 and uploads them to S3.

## 7. Sync WARC Outputs Back To Local

Run on your Mac from the repository root.

```bash
YEAR=2020
TRACK=trend
BATCH=1
BPAD=$(printf "%03d" "$BATCH")
RUNID=YYYYMMDD_HHMMSS
BUCKET=$(python - <<'PY'
from pathlib import Path
from src.cli import load_config

cfg = load_config(Path("configs/commoncrawl_collection.yaml"))
s3 = cfg["collection"]["aws"]["s3"]
print(f"s3://{s3['bucket']}/{str(s3['prefix']).strip('/')}")
PY
)

mkdir -p data/interim/collection/${TRACK}_working/$YEAR
aws s3 sync \
  $BUCKET/warc_output/$TRACK/$YEAR/batch_$BPAD/$RUNID/ \
  data/interim/collection/${TRACK}_working/$YEAR/warc/
```

For `TRACK=corpus`, use the batch subfolder:

```bash
mkdir -p data/interim/collection/corpus_working/$YEAR/batch_$BPAD
aws s3 sync \
  $BUCKET/warc_output/$TRACK/$YEAR/batch_$BPAD/$RUNID/ \
  data/interim/collection/corpus_working/$YEAR/batch_$BPAD/warc/
```

## 8. Run Document Quality

Run locally or on EC2 after WARC outputs exist in the working directory.

```bash
make collection_quality YEAR=$YEAR TRACK=$TRACK BATCH=$BATCH CONFIG=$CONFIG
```

Inspect:

```bash
find data/interim/collection -path "*/quality/cc_collection_summary_*.csv" | sort | tail -n1
find data/interim/collection -path "*/quality/cc_val_sample30_*.csv" | sort | tail -n1
find data/interim/collection -path "*/metrics/cc_collection_throughput_summary_*.csv" | sort | tail -n1
```

## 9. Build Processed Outputs

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

## 10. Yearly Convenience Targets

These run the local preparation steps for one year.

```bash
make trend_year YEAR=2020 CONFIG=$CONFIG
make corpus_year YEAR=2020 BATCH=1 CONFIG=$CONFIG
```

The remote pointer-resolution and WARC-extraction steps still need the explicit EC2 commands above because they depend on S3 run IDs and the running local index server.

## 11. Multi-Year Local Preparation

These run acquire, scan, URL export, and URL upload for every configured year.

```bash
make trend CONFIG=$CONFIG
make corpus CONFIG=$CONFIG
```

Use these only when you are ready for a longer multi-year local preparation run.

## 12. Stop Criteria

Trend:

- Process the fixed WET sample for every configured year.
- Keep both WET and WARC trend rates.

Corpus:

- Aim for 1000 high-quality target-term survivors per year.
- Accept 500 as a soft minimum if a year is sparse or expensive.
- Add corpus batches iteratively while AWS Budget alerts and runtime remain acceptable.
- The pipeline does not enforce a dollar-denominated stop condition internally; configure AWS Budgets separately for account-level spend alerts.
