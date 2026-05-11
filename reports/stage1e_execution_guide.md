# Stage 1e Execution Guide

This guide runs the full Stage 1e second pass on the same raw WET files used for Stage 1d. Stage 1e is a parallel rerun and does not overwrite Stage 1d.

Command blocks are marked either:

- **Copy-paste:** run exactly as shown.
- **Edit first:** replace the indicated value before running.

## 1. Local Pre-Flight

Run on your **local Mac**.

**Copy-paste:**

```bash
cd /Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak
conda activate msc-nlp
```

Stage 1e document-quality filtering now depends on DataTrove. If this import check fails,
update the environment from `environment.yml` or install the package directly.

**Copy-paste:**

```bash
python - <<'PY'
from datatrove.data import Document
from datatrove.pipeline.filters.fineweb_quality_filter import FineWebQualityFilter
from datatrove.pipeline.filters.gopher_quality_filter import GopherQualityFilter
from datatrove.pipeline.filters.gopher_repetition_filter import GopherRepetitionFilter
print("DataTrove quality filters available")
PY
```

If needed:

**Copy-paste:**

```bash
python -m pip install 'datatrove==0.9.0'
```

If `conda activate` is not initialized in that shell, use this instead.

**Copy-paste:**

```bash
cd /Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak
source /opt/anaconda3/etc/profile.d/conda.sh
conda activate msc-nlp
```

Confirm the eight Stage 1e raw WET files are present.

**Copy-paste:**

```bash
ls data/raw/wet/*.wet.gz
```

Expected filenames:

```text
CC-MAIN-2016-44_001.wet.gz
CC-MAIN-2016-44_002.wet.gz
CC-MAIN-2021-49_001.wet.gz
CC-MAIN-2021-49_002.wet.gz
CC-MAIN-2021-49_003.wet.gz
CC-MAIN-2021-49_004.wet.gz
CC-MAIN-2026-04_001.wet.gz
CC-MAIN-2026-04_002.wet.gz
```

## 2. Local WET Scan, URL Export, And Upload

Run on your **local Mac**.

**Copy-paste:**

```bash
make cc_stage1e_scan
make cc_stage1e_validate
make cc_stage1e_export_urls
make cc_stage1e_upload_urls
```

This writes WET scan outputs to:

```text
data/interim/stage1_pilot-dev/stage1e/wet_scan/
```

It writes URL export outputs to:

```text
data/interim/stage1_pilot-dev/stage1e/url_exports/
```

Set the Stage 1e URL-export runid and S3 prefix.

**Copy-paste:**

```bash
RUNID=$(find data/interim/stage1_pilot-dev/stage1e/url_exports \
  -maxdepth 1 \
  -type f \
  -name 'cc_stage1e_urls_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_[0-9][0-9][0-9][0-9][0-9][0-9].csv' \
  | sed -E 's#.*cc_stage1e_urls_([0-9_]+)\.csv#\1#' \
  | sort \
  | tail -n1)

BUCKET=s3://msc-nlp-therapy-speak-823916751170-us-east-1-an/msc-nlp-therapy-speak/stage1e

echo "$RUNID"
echo "$BUCKET"
```

The printed `RUNID` should look like `20260508_124713`. Use that same `RUNID` for all remote pointer-resolution and WARC-extraction commands below.

## 3. Start Or Reopen EC2

Start the EC2 resolver instance in the AWS Console if it is stopped.

The commands below assume this EC2 DNS name:

```text
ec2-174-129-171-95.compute-1.amazonaws.com
```

If AWS gives you a different Public IPv4 DNS, replace `ec2-174-129-171-95.compute-1.amazonaws.com` in the SSH and rsync commands before running them.

## 4. Sync Code And Stage 1e Artifacts To EC2

Run on your **local Mac**.

### 4.1 Sync Code

Use this exact command if the EC2 DNS above is still correct.

**Copy-paste unless EC2 DNS changed:**

```bash
rsync -av \
  --exclude '.git/' \
  --exclude 'data/' \
  --exclude '__pycache__/' \
  -e "ssh -i /Users/jakoblutkemeier/Desktop/aws/athena-extraction-key.pem" \
  /Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak/ \
  ec2-user@ec2-174-129-171-95.compute-1.amazonaws.com:~/msc-nlp-therapy-speak/
```

### 4.2 Sync Stage 1e Data Artifacts

This is required because remote WARC extraction reads the latest Stage 1e WET-validated artifacts from the repo on EC2.

**Copy-paste unless EC2 DNS changed:**

```bash
rsync -av \
  -e "ssh -i /Users/jakoblutkemeier/Desktop/aws/athena-extraction-key.pem" \
  /Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak/data/interim/stage1_pilot-dev/stage1e/ \
  ec2-user@ec2-174-129-171-95.compute-1.amazonaws.com:~/msc-nlp-therapy-speak/data/interim/stage1_pilot-dev/stage1e/
```

Repeat this artifact sync if you rerun `make cc_stage1e_scan` and produce a newer WET scan runid.

## 5. SSH Into EC2

Run on your **local Mac**.

**Copy-paste unless EC2 DNS changed:**

```bash
ssh -i /Users/jakoblutkemeier/Desktop/aws/athena-extraction-key.pem \
  ec2-user@ec2-174-129-171-95.compute-1.amazonaws.com
```

Once connected, run on the **remote EC2 shell**.

**Copy-paste:**

```bash
cd ~/msc-nlp-therapy-speak
source ~/miniconda3/etc/profile.d/conda.sh
conda activate msc-nlp
```

Confirm Stage 1e artifacts are present on EC2.

**Copy-paste:**

```bash
find ~/msc-nlp-therapy-speak/data/interim/stage1_pilot-dev/stage1e -maxdepth 2 -type f | sort
```

You should see files under at least:

```text
wet_scan/
url_exports/
```

## 6. Clear Any Stale EC2 Index Server

Run on the **remote EC2 shell**.

**Copy-paste:**

```bash
PIDFILE=~/.cache/msc-nlp-therapy-speak/stage1e_index_server/index_server.pid
RUNTIMEDIR=~/.cache/msc-nlp-therapy-speak/stage1e_index_server

ss -ltnp | grep ':8080' || true
cat "$PIDFILE" 2>/dev/null || true
```

If a process is listening on `8080`, this command kills it safely.

**Copy-paste:**

```bash
PID=$(ss -ltnp | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' | head -n1)
if [ -n "$PID" ]; then
  kill "$PID" 2>/dev/null || true
  sleep 2
fi
ss -ltnp | grep ':8080' || true
rm -f "$PIDFILE"
```

If port `8080` is still occupied after that, force-kill the remaining listener.

**Copy-paste only if needed:**

```bash
PID=$(ss -ltnp | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' | head -n1)
if [ -n "$PID" ]; then
  kill -9 "$PID" 2>/dev/null || true
  sleep 2
fi
ss -ltnp | grep ':8080' || true
rm -f "$PIDFILE"
```

Optional clean reinstall of Stage 1e index-server collections.

**Copy-paste only if you want a clean index cache:**

```bash
rm -rf "$RUNTIMEDIR/collections"
```

## 7. Remote Pointer Resolution On EC2

Run on the **remote EC2 shell**.

Set `RUNID` and `BUCKET`.

**Edit first:** replace `PASTE_LOCAL_RUNID_HERE` with the `RUNID` printed in Section 2.

```bash
RUNID=20260511_093546
BUCKET=s3://msc-nlp-therapy-speak-823916751170-us-east-1-an/msc-nlp-therapy-speak/stage1e

echo "$RUNID"
echo "$BUCKET"
```

Install Common Crawl secondary indexes.

**Copy-paste after setting `RUNID` and `BUCKET`:**

```bash
make cc_stage1e_install_indexes_remote \
  URL_EXPORT_URI=$BUCKET/url_exports/$RUNID/cc_stage1e_urls_$RUNID.csv
```

Optional sanity check.

**Copy-paste:**

```bash
find ~/.cache/msc-nlp-therapy-speak/stage1e_index_server/collections -maxdepth 3 -type f | sort
```

Start the local index server.

**Copy-paste:**

```bash
make cc_stage1e_start_index_server
```

If startup fails with “Address already in use”, repeat Section 6 and retry.

Optional health check. A healthy server returns a real NDJSON capture row, not a mode listing or `500`.

**Copy-paste:**

```bash
curl -i -sS --get "http://127.0.0.1:8080/CC-MAIN-2016-44/index" \
  --data-urlencode "url=http://1meh8.deviantart.com/art/Swedish-Stereotypes-349856832" \
  --data-urlencode "output=json" \
  --data-urlencode "limit=1"
```

Resolve WARC pointers.

**Copy-paste after setting `RUNID` and `BUCKET`:**

```bash
make cc_stage1e_resolve \
  URL_EXPORT_URI=$BUCKET/url_exports/$RUNID/cc_stage1e_urls_$RUNID.csv \
  RESOLVE_OUTPUT_PREFIX=$BUCKET/pointer_cache/$RUNID/
```

Inspect pointer-cache summary.

**Copy-paste:**

```bash
cat data/interim/stage1_pilot-dev/stage1e/pointer_cache/cc_pointer_cache_summary_${RUNID}.csv
```

## 8. Remote WARC Extraction On EC2

Run on the **remote EC2 shell**.

**Copy-paste after setting `RUNID` and `BUCKET`:**

```bash
make cc_stage1e_extract \
  POINTER_CACHE_URI=$BUCKET/pointer_cache/$RUNID/cc_pointer_cache_$RUNID.parquet \
  WARC_OUTPUT_PREFIX=$BUCKET/warc_output/$RUNID/
```

The WARC extraction step creates its own output runid, but S3 output is still stored under the URL-export `RUNID` prefix:

```text
s3://msc-nlp-therapy-speak-823916751170-us-east-1-an/msc-nlp-therapy-speak/stage1e/warc_output/$RUNID/
```

Inspect the latest WARC summary on EC2.

**Copy-paste:**

```bash
LATEST_WARC_SUMMARY=$(ls -t data/interim/stage1_pilot-dev/stage1e/warc/cc_stage1e_summary_*.csv | head -n1)
echo "$LATEST_WARC_SUMMARY"
cat "$LATEST_WARC_SUMMARY"
```

## 9. Sync WARC Outputs Back To Local

Return to your **local Mac**.

**Copy-paste:**

```bash
cd /Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak
conda activate msc-nlp
```

Set the same `RUNID` used on EC2.

**Edit first:** replace `PASTE_LOCAL_RUNID_HERE` with the `RUNID` from Section 2.

```bash
RUNID=PASTE_LOCAL_RUNID_HERE
```

Sync WARC outputs from S3.

**Copy-paste after setting `RUNID`:**

```bash
aws s3 sync \
  s3://msc-nlp-therapy-speak-823916751170-us-east-1-an/msc-nlp-therapy-speak/stage1e/warc_output/$RUNID/ \
  /Users/jakoblutkemeier/Documents/msc-nlp-therapy-speak/data/interim/stage1_pilot-dev/stage1e/warc/
```

Sanity check.

**Copy-paste:**

```bash
find data/interim/stage1_pilot-dev/stage1e/warc -maxdepth 1 -type f | sort
```

## 10. Local Document-Quality Filter And Final Outputs

Run on your **local Mac**.

**Copy-paste:**

```bash
make cc_stage1e_document_quality
```

This writes final Stage 1e outputs to:

```text
data/interim/stage1_pilot-dev/stage1e/document_quality/
data/interim/stage1_pilot-dev/stage1e/metrics/
```

Key files:

```text
document_quality/cc_document_quality_hits_RUNID.parquet
document_quality/cc_corpus_texts_document_quality_RUNID.parquet
document_quality/cc_val_sample30_RUNID.csv
document_quality/cc_stage1e_summary_RUNID.csv
document_quality/cc_stage1e_term_summary_RUNID.csv
metrics/cc_stage1e_throughput_summary_RUNID.csv
```

Inspect summaries.

**Copy-paste:**

```bash
LATEST_STAGE1E_SUMMARY=$(ls -t data/interim/stage1_pilot-dev/stage1e/document_quality/cc_stage1e_summary_*.csv | head -n1)
LATEST_STAGE1E_THROUGHPUT=$(ls -t data/interim/stage1_pilot-dev/stage1e/metrics/cc_stage1e_throughput_summary_*.csv | head -n1)

echo "$LATEST_STAGE1E_SUMMARY"
cat "$LATEST_STAGE1E_SUMMARY"

echo
echo "$LATEST_STAGE1E_THROUGHPUT"
cat "$LATEST_STAGE1E_THROUGHPUT"
```

Inspect the validation sample path.

**Copy-paste:**

```bash
LATEST_STAGE1E_SAMPLE=$(ls -t data/interim/stage1_pilot-dev/stage1e/document_quality/cc_val_sample30_*.csv | head -n1)
echo "$LATEST_STAGE1E_SAMPLE"
```

## 11. Stop The EC2 Index Server

Run on the **remote EC2 shell** when finished.

**Copy-paste:**

```bash
ss -ltnp | grep ':8080' || true
PID=$(ss -ltnp | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' | head -n1)
if [ -n "$PID" ]; then
  kill "$PID" 2>/dev/null || true
  sleep 2
fi
ss -ltnp | grep ':8080' || true
rm -f ~/.cache/msc-nlp-therapy-speak/stage1e_index_server/index_server.pid
```

Then stop the EC2 instance in the AWS Console if you no longer need it.

## Minimal Command Split

Local only:

```bash
make cc_stage1e_scan
make cc_stage1e_validate
make cc_stage1e_export_urls
make cc_stage1e_upload_urls
aws s3 sync s3://msc-nlp-therapy-speak-823916751170-us-east-1-an/msc-nlp-therapy-speak/stage1e/warc_output/$RUNID/ data/interim/stage1_pilot-dev/stage1e/warc/
make cc_stage1e_document_quality
```

Remote EC2 only:

```bash
make cc_stage1e_install_indexes_remote URL_EXPORT_URI=$BUCKET/url_exports/$RUNID/cc_stage1e_urls_$RUNID.csv
make cc_stage1e_start_index_server
make cc_stage1e_resolve URL_EXPORT_URI=$BUCKET/url_exports/$RUNID/cc_stage1e_urls_$RUNID.csv RESOLVE_OUTPUT_PREFIX=$BUCKET/pointer_cache/$RUNID/
make cc_stage1e_extract POINTER_CACHE_URI=$BUCKET/pointer_cache/$RUNID/cc_pointer_cache_$RUNID.parquet WARC_OUTPUT_PREFIX=$BUCKET/warc_output/$RUNID/
```

Important:

- `PASTE_LOCAL_RUNID_HERE` is the only placeholder in this guide.
- If EC2 DNS changes, replace `ec2-98-92-8-228.compute-1.amazonaws.com` before running SSH or rsync commands.
- Stage 1e WARC extraction depends on the latest Stage 1e `wet_scan/` files being present on EC2.
- Stage 1d remains untouched.
