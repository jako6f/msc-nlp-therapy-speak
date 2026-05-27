#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

CONFIG="${CONFIG:-configs/commoncrawl_collection.yaml}"

if [ "$#" -eq 0 ]; then
  echo "Usage: CONFIG=configs/commoncrawl_collection.yaml $0 YEAR:FIRST_BATCH:LAST_BATCH [...]" >&2
  echo "Example: $0 2021:3:4 2022:2:4 2023:2:4" >&2
  exit 2
fi

mkdir -p reports/logs
driver_log="reports/logs/corpus_expansion_plan_$(date -u +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$driver_log") 2>&1

echo "driver_log=$driver_log"
echo "config=$CONFIG"
echo "plan=$*"

completed_summary() {
  local year="$1"
  local batch="$2"
  local batch_label
  batch_label="$(printf "%03d" "$batch")"
  find "data/interim/collection/corpus_working/$year/batch_$batch_label/quality" \
    -maxdepth 1 \
    -type f \
    -name "cc_collection_summary_*.csv" \
    -print \
    -quit 2>/dev/null
}

for spec in "$@"; do
  IFS=":" read -r year first_batch last_batch extra <<<"$spec"
  if [ -z "${year:-}" ] || [ -z "${first_batch:-}" ] || [ -z "${last_batch:-}" ] || [ -n "${extra:-}" ]; then
    echo "ERROR invalid plan item: $spec" >&2
    exit 2
  fi

  for batch in $(seq "$first_batch" "$last_batch"); do
    batch_label="$(printf "%03d" "$batch")"
    existing_summary="$(completed_summary "$year" "$batch")"
    if [ -n "$existing_summary" ]; then
      echo "SKIP existing year=$year batch=$batch_label summary=$existing_summary"
      continue
    fi

    echo "=== RUNNING CORPUS YEAR $year BATCH $batch_label ==="
    if make corpus_expand YEAR="$year" BATCH="$batch" CONFIG="$CONFIG"; then
      :
    else
      rc="$?"
      echo "ERROR corpus expansion failed year=$year batch=$batch_label rc=$rc" >&2
      exit "$rc"
    fi

    completed_summary_path="$(completed_summary "$year" "$batch")"
    if [ -z "$completed_summary_path" ]; then
      echo "ERROR missing quality summary after year=$year batch=$batch_label" >&2
      exit 1
    fi
    echo "DONE year=$year batch=$batch_label summary=$completed_summary_path"
  done
done

echo "corpus expansion plan complete"
