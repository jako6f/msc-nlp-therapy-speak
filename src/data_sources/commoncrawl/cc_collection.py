import copy
import datetime as dt
import gzip
import io
import json
import os
import random
import re
import shutil
import socket
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd

from src.pathing import (
    collection_interim_dir,
    collection_metrics_dir,
    collection_pointer_cache_dir,
    collection_quality_dir,
    collection_track_working_dir,
    collection_url_export_dir,
    collection_warc_dir,
    processed_corpus_dir,
    processed_manifest_dir,
)

from .cc_acquire import COMMONCRAWL_BASE_URL, _setup_logger, _stable_seed, read_manifest
from .cc_document_quality import document_quality_hits
from .cc_resolve import (
    export_urls,
    install_indexes_remote,
    resolve_urls_remote,
    start_index_server_remote,
    stop_index_server_remote,
    upload_urls_to_s3,
)
from .cc_scan import scan_wet_files
from .cc_warc import extract_pointer_cache

COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
COMMONCRAWL_DATA_BASE_URL = "https://data.commoncrawl.org"
CRAWL_ID_RE = re.compile(r"^CC-MAIN-(?P<year>\d{4})-(?P<week>\d{2})$")


def _utc_runid() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.gmtime())


def _collection_cfg(config: Dict) -> Dict:
    return config.get("collection", {})


def _year_cfg(config: Dict, year: int | str) -> Dict:
    year_key = str(year)
    for item in _collection_cfg(config).get("crawl_map", []):
        if str(item.get("year", "")) == year_key:
            return item
    raise ValueError(f"Year {year_key} is not present in collection.crawl_map")


def _crawl_id_for_year(config: Dict, year: int | str) -> str:
    crawl_id = str(_year_cfg(config, year).get("crawl_id", "")).strip()
    if not crawl_id:
        raise ValueError(f"collection.crawl_map entry for {year} is missing crawl_id")
    return crawl_id


def _collection_years(config: Dict) -> List[int]:
    years = [int(item["year"]) for item in _collection_cfg(config).get("crawl_map", [])]
    if not years:
        raise ValueError("collection.crawl_map must list the frozen collection years.")
    return sorted(years)


def _warc_validation_cfg(config: Dict) -> Dict:
    return _collection_cfg(config).get("warc_validation", {})


def _warc_acceptance_cfg(config: Dict) -> Dict:
    return _warc_validation_cfg(config).get("acceptance", {})


def _acquisition_cfg(config: Dict) -> Dict:
    return _collection_cfg(config).get("acquisition", {})


def _metric_float(summary: Dict[str, object], key: str, default: float = 0.0) -> float:
    value = summary.get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _warc_fetch_success_rate_pct(metrics: Dict[str, object]) -> float:
    docs = _metric_float(metrics, "doc_count_input")
    fetch_success = _metric_float(metrics, "doc_count_fetch_success")
    return (fetch_success / docs * 100.0) if docs else 0.0


def _warc_extract_success_rate_pct(metrics: Dict[str, object]) -> float:
    docs = _metric_float(metrics, "doc_count_input")
    extract_success = _metric_float(metrics, "doc_count_extract_success")
    return (extract_success / docs * 100.0) if docs else 0.0


def _warc_summary_metrics(path: Path) -> Dict[str, object]:
    frame = pd.read_csv(path)
    if "metric" not in frame.columns or "value" not in frame.columns:
        raise ValueError(f"Missing expected columns in {path}")
    return frame.set_index("metric")["value"].to_dict()


def _warc_summary_is_accepted(config: Dict, summary_path: Path) -> Tuple[bool, float, float]:
    metrics = _warc_summary_metrics(summary_path)
    fetch_success_rate_pct = _warc_fetch_success_rate_pct(metrics)
    extract_success_rate_pct = _warc_extract_success_rate_pct(metrics)
    acceptance_cfg = _warc_acceptance_cfg(config)
    min_fetch_rate_pct = float(acceptance_cfg.get("min_fetch_success_rate_pct", 95.0))
    min_extract_rate_pct = float(acceptance_cfg.get("min_extract_success_rate_pct", 35.0))
    return (
        fetch_success_rate_pct >= min_fetch_rate_pct
        and extract_success_rate_pct >= min_extract_rate_pct,
        fetch_success_rate_pct,
        extract_success_rate_pct,
    )


def _require_accepted_warc_summary(
    config: Dict,
    *,
    summary_path: Path,
    logger,
    year: int | str,
    track: str,
) -> None:
    accepted, fetch_success_rate_pct, extract_success_rate_pct = _warc_summary_is_accepted(
        config, summary_path
    )
    acceptance_cfg = _warc_acceptance_cfg(config)
    min_fetch_rate_pct = float(acceptance_cfg.get("min_fetch_success_rate_pct", 95.0))
    warn_fetch_rate_pct = float(acceptance_cfg.get("warn_fetch_success_rate_pct", 98.0))
    min_extract_rate_pct = float(acceptance_cfg.get("min_extract_success_rate_pct", 35.0))
    warn_extract_rate_pct = float(acceptance_cfg.get("warn_extract_success_rate_pct", 40.0))
    if fetch_success_rate_pct < warn_fetch_rate_pct:
        logger.warning(
            "WARC fetch success below warning threshold year=%s track=%s "
            "fetch_success_rate_pct=%.6f warn_below_pct=%.6f summary=%s",
            year,
            track,
            fetch_success_rate_pct,
            warn_fetch_rate_pct,
            summary_path,
        )
    if extract_success_rate_pct < warn_extract_rate_pct:
        logger.warning(
            "WARC extraction success below warning threshold year=%s track=%s "
            "extract_success_rate_pct=%.6f warn_below_pct=%.6f summary=%s",
            year,
            track,
            extract_success_rate_pct,
            warn_extract_rate_pct,
            summary_path,
        )
    if not accepted:
        raise RuntimeError(
            "Rejected collection year after WARC extraction: "
            f"year={year} track={track} fetch_success_rate_pct={fetch_success_rate_pct:.6f} "
            f"min_fetch_success_rate_pct={min_fetch_rate_pct:.6f} "
            f"extract_success_rate_pct={extract_success_rate_pct:.6f} "
            f"min_extract_success_rate_pct={min_extract_rate_pct:.6f}. "
            f"Inspect {summary_path} before rerunning."
        )


def _latest_corpus_quality_paths(config: Dict, *, require_all_years: bool = True) -> List[Path]:
    corpus_root = collection_interim_dir(config) / "corpus"
    selected_paths: List[Path] = []
    missing_years: List[str] = []
    for year in _collection_years(config):
        year_dir = corpus_root / str(year)
        selected_for_year = 0
        for batch_dir in sorted(year_dir.glob("batch_*")):
            if not batch_dir.is_dir():
                continue
            quality_paths = sorted(
                (batch_dir / "quality").rglob("cc_corpus_texts_document_quality_*.parquet")
            )
            if not quality_paths:
                continue
            selected_paths.append(quality_paths[-1])
            selected_for_year += 1
        if selected_for_year == 0:
            missing_years.append(str(year))
    if missing_years and require_all_years:
        raise FileNotFoundError(
            "No successful corpus quality output found for configured years: "
            + ", ".join(missing_years)
        )
    return selected_paths


def _corpus_quality_outputs_complete(config: Dict) -> bool:
    try:
        _latest_corpus_quality_paths(config, require_all_years=True)
    except FileNotFoundError:
        return False
    return True


def _corpus_source_parts(corpus_path: Path) -> Tuple[Optional[int], Optional[int]]:
    parts = corpus_path.parts
    candidate_roots = ("corpus", "corpus_working")
    for root in candidate_roots:
        try:
            root_idx = parts.index(root)
            year = int(parts[root_idx + 1])
            batch = int(str(parts[root_idx + 2]).removeprefix("batch_"))
            return year, batch
        except (ValueError, IndexError):
            continue

    # Legacy synced layout was stage-first:
    # data/interim/collection/{stage}/corpus/{year}/batch_NNN/...
    for idx, part in enumerate(parts):
        if part != "corpus":
            continue
        try:
            year = int(parts[idx + 1])
            batch = int(str(parts[idx + 2]).removeprefix("batch_"))
            return year, batch
        except (ValueError, IndexError):
            continue
    return None, None


def _corpus_source_parts_from_value(value: object) -> Tuple[Optional[int], Optional[int]]:
    value_str = str(value or "").strip()
    if not value_str or value_str.lower() == "nan":
        return None, None
    return _corpus_source_parts(Path(value_str))


def _with_corpus_source_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if "source_year" in frame.columns and "source_batch" in frame.columns:
        return frame
    if "source_corpus_path" not in frame.columns:
        raise ValueError(
            "Existing processed corpus is missing source_corpus_path; cannot safely "
            "replace rerun corpus batches."
        )
    frame = frame.copy()
    source_parts = frame["source_corpus_path"].map(_corpus_source_parts_from_value)
    frame["source_year"] = source_parts.map(lambda value: value[0])
    frame["source_batch"] = source_parts.map(lambda value: value[1])
    return frame


def _normalise_corpus_source_key(
    year: object, batch: object
) -> Tuple[Optional[int], Optional[int]]:
    try:
        if pd.isna(year) or pd.isna(batch):
            return None, None
        return int(year), int(batch)
    except (TypeError, ValueError):
        return None, None


def _processed_corpus_sources(path: Path) -> Tuple[set[Tuple[int, int]], set[str]]:
    import pyarrow.parquet as pq  # type: ignore

    parquet_file = pq.ParquetFile(path)
    names = set(parquet_file.schema_arrow.names)
    source_paths: set[str] = set()
    source_keys: set[Tuple[int, int]] = set()

    if "source_corpus_path" in names:
        frame = pd.read_parquet(path, columns=["source_corpus_path"])
        source_paths = {
            str(value).strip()
            for value in frame["source_corpus_path"].dropna().unique()
            if str(value).strip()
        }
        for source_path in source_paths:
            year, batch = _corpus_source_parts(Path(source_path))
            if year is not None and batch is not None:
                source_keys.add((year, batch))
        return source_keys, source_paths

    if {"source_year", "source_batch"}.issubset(names):
        frame = pd.read_parquet(path, columns=["source_year", "source_batch"])
        for year, batch in zip(frame["source_year"], frame["source_batch"]):
            source_key = _normalise_corpus_source_key(year, batch)
            if source_key[0] is not None and source_key[1] is not None:
                source_keys.add((source_key[0], source_key[1]))
        return source_keys, source_paths

    raise ValueError(
        "Existing processed corpus is missing source metadata; cannot safely update it."
    )


def _corpus_frame_source_keys(frame: pd.DataFrame) -> List[Tuple[Optional[int], Optional[int]]]:
    if "source_year" in frame.columns and "source_batch" in frame.columns:
        return [
            _normalise_corpus_source_key(year, batch)
            for year, batch in zip(frame["source_year"], frame["source_batch"])
        ]
    if "source_corpus_path" not in frame.columns:
        raise ValueError(
            "Processed corpus frame is missing source metadata; cannot safely update it."
        )
    return [
        _corpus_source_parts_from_value(source_path)
        for source_path in frame["source_corpus_path"]
    ]


def _normalise_track(track: Optional[str]) -> str:
    value = (track or "corpus").strip().lower()
    if value not in {"trend", "corpus"}:
        raise ValueError("track must be one of: trend, corpus")
    return value


def _batch_bounds(config: Dict, track: str, batch: int) -> Tuple[int, int]:
    collection = _collection_cfg(config)
    if track == "trend":
        raise ValueError(
            "The WET-first trend collection path has been retired. Use cc-trend-run-batch "
            "or `make trend` for the publication-year WARC trend track."
        )

    corpus_cfg = collection.get("corpus", {})
    initial_size = int(corpus_cfg.get("initial_wet_batch_size", 50))
    expansion_size = int(corpus_cfg.get("expansion_wet_batch_size", initial_size))
    if batch <= 1:
        return 0, initial_size
    start = initial_size + (batch - 2) * expansion_size
    return start, start + expansion_size


def _batch_label(batch: int | str) -> str:
    return f"batch_{int(batch):03d}"


def _raw_wet_cfg(config: Dict) -> Dict:
    return _collection_cfg(config).get("raw_wet", {})


def _raw_wet_dir() -> Path:
    return Path("data/raw/wet")


def _raw_wet_min_free_gb(config: Dict) -> float:
    return float(_raw_wet_cfg(config).get("min_free_gb_before_download", 10.0))


def _disk_free_gb(path: Path) -> float:
    path.mkdir(parents=True, exist_ok=True)
    return shutil.disk_usage(path).free / (1024**3)


def _require_raw_wet_download_space(config: Dict, output_dir: Path) -> None:
    min_free_gb = _raw_wet_min_free_gb(config)
    free_gb = _disk_free_gb(output_dir)
    if free_gb < min_free_gb:
        raise RuntimeError(
            "Insufficient free disk space before WET download: "
            f"{free_gb:.2f} GiB available in {output_dir}, "
            f"requires at least {min_free_gb:.2f} GiB."
        )


def _acquisition_retry_http_statuses(config: Dict) -> set[int]:
    return {
        int(status)
        for status in _acquisition_cfg(config).get(
            "retry_http_statuses",
            [403, 408, 425, 429, 500, 502, 503, 504],
        )
    }


def _acquisition_retry_backoff_s(config: Dict, attempt: int) -> float:
    acquisition_cfg = _acquisition_cfg(config)
    initial = max(0.0, float(acquisition_cfg.get("retry_initial_backoff_s", 2.0)))
    maximum = max(initial, float(acquisition_cfg.get("retry_max_backoff_s", 30.0)))
    return min(maximum, initial * (2 ** max(0, attempt - 1)))


def _acquisition_max_attempts(config: Dict) -> int:
    return max(1, int(_acquisition_cfg(config).get("max_attempts", 4)))


def _acquisition_timeout_s(config: Dict) -> int:
    return max(1, int(_acquisition_cfg(config).get("request_timeout_s", 120)))


def _acquisition_request(url: str) -> Request:
    return Request(url, headers={"User-Agent": "msc-nlp-therapy-speak/0.2"})


def _acquisition_error_is_retryable(config: Dict, exc: Exception) -> bool:
    if isinstance(exc, HTTPError):
        return exc.code in _acquisition_retry_http_statuses(config)
    return isinstance(exc, (URLError, TimeoutError, OSError))


def _log_acquisition_retry(
    logger, *, label: str, attempt: int, max_attempts: int, delay_s: float, exc
):
    if logger is None:
        return
    logger.warning(
        "%s attempt %d/%d failed with %s: %s; retrying in %.3fs",
        label,
        attempt,
        max_attempts,
        type(exc).__name__,
        exc,
        delay_s,
    )


def _read_url_bytes_with_retries(
    config: Dict,
    *,
    url: str,
    logger,
    label: str,
) -> bytes:
    max_attempts = _acquisition_max_attempts(config)
    for attempt in range(1, max_attempts + 1):
        try:
            with urlopen(
                _acquisition_request(url), timeout=_acquisition_timeout_s(config)
            ) as response:
                return response.read()
        except Exception as exc:
            if attempt >= max_attempts or not _acquisition_error_is_retryable(config, exc):
                raise
            delay_s = _acquisition_retry_backoff_s(config, attempt)
            _log_acquisition_retry(
                logger,
                label=label,
                attempt=attempt,
                max_attempts=max_attempts,
                delay_s=delay_s,
                exc=exc,
            )
            time.sleep(delay_s)
    raise RuntimeError(f"Failed to read {url}")


def _download_url_with_retries(
    config: Dict,
    *,
    url: str,
    dest: Path,
    logger,
    label: str,
) -> None:
    max_attempts = _acquisition_max_attempts(config)
    temp_dest = dest.with_name(f"{dest.name}.part")
    for attempt in range(1, max_attempts + 1):
        temp_dest.unlink(missing_ok=True)
        try:
            with (
                urlopen(
                    _acquisition_request(url), timeout=_acquisition_timeout_s(config)
                ) as response,
                temp_dest.open("wb") as handle,
            ):
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
            temp_dest.replace(dest)
            return
        except Exception as exc:
            temp_dest.unlink(missing_ok=True)
            if attempt >= max_attempts or not _acquisition_error_is_retryable(config, exc):
                raise
            delay_s = _acquisition_retry_backoff_s(config, attempt)
            _log_acquisition_retry(
                logger,
                label=label,
                attempt=attempt,
                max_attempts=max_attempts,
                delay_s=delay_s,
                exc=exc,
            )
            time.sleep(delay_s)
    raise RuntimeError(f"Failed to download {url}")


def _aws_s3_base(config: Dict) -> Tuple[str, str]:
    s3_cfg = _collection_cfg(config).get("aws", {}).get("s3", {})
    bucket = str(s3_cfg.get("bucket", "")).strip()
    prefix = str(s3_cfg.get("prefix", "")).strip("/")
    if not bucket:
        raise ValueError(
            "collection.aws.s3.bucket is required. Store it in configs/local/aws.yaml "
            "or the file pointed to by MSC_NLP_LOCAL_CONFIG."
        )
    return bucket, prefix


def _collection_s3_uri(config: Dict, *parts: str) -> str:
    bucket, prefix = _aws_s3_base(config)
    key_parts = [prefix, *[part.strip("/") for part in parts if part]]
    key = "/".join(part for part in key_parts if part)
    return f"s3://{bucket}/{key}" if key else f"s3://{bucket}"


def _processed_output_prefix(config: Dict, track: str) -> str:
    return _collection_s3_uri(config, "processed", track).rstrip("/") + "/"


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {uri}")
    without_scheme = uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def _upload_paths_to_s3(local_paths: Iterable[Path], s3_output_prefix: str) -> List[str]:
    import boto3  # type: ignore

    bucket, key_prefix = _parse_s3_uri(s3_output_prefix.rstrip("/") + "/")
    key_prefix = key_prefix.rstrip("/")
    s3_client = boto3.client("s3")
    uploaded_uris: List[str] = []
    for local_path in local_paths:
        if not local_path.exists():
            continue
        key = f"{key_prefix}/{local_path.name}" if key_prefix else local_path.name
        s3_client.upload_file(str(local_path), bucket, key)
        uploaded_uris.append(f"s3://{bucket}/{key}")
    return uploaded_uris


def upload_processed_output(config: Dict, *, track: str, path: Path) -> List[str]:
    track = _normalise_track(track)
    local_path = Path(path)
    if not local_path.exists():
        raise FileNotFoundError(f"Processed output not found: {local_path}")
    return _upload_paths_to_s3([local_path], _processed_output_prefix(config, track))


def _runid_from_path(path: Path, prefix: str, suffix: str) -> str:
    name = path.name
    if not name.startswith(prefix) or not name.endswith(suffix):
        raise ValueError(f"Unexpected file name format: {name}")
    return name[len(prefix) : -len(suffix)]


def _read_latest_url_upload_manifest(
    config: Dict, *, year: int | str, track: str, batch: int | str
) -> Dict[str, object]:
    upload_dir = collection_url_export_dir(config, track=track, year=year, batch=batch)
    matches = sorted(upload_dir.glob("cc_collection_url_upload_manifest_*.json"))
    if not matches:
        raise FileNotFoundError(f"No URL upload manifest found in {upload_dir}")
    return json.loads(matches[-1].read_text())


def _write_run_manifest(
    config: Dict,
    *,
    year: int | str,
    track: str,
    batch: int | str,
    payload: Dict[str, object],
) -> Path:
    metrics_dir = collection_metrics_dir(config, track=track, year=year, batch=batch)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    runid = str(payload.get("runid", _utc_runid()))
    manifest_path = metrics_dir / f"cc_collection_run_manifest_{runid}.json"
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return manifest_path


def _quality_output_paths(
    config: Dict, *, year: int | str, track: str, batch: int | str, runid: str
) -> List[Path]:
    quality_dir = collection_quality_dir(config, track=track, year=year, batch=batch)
    paths = [
        quality_dir / f"cc_collection_summary_{runid}.csv",
        quality_dir / f"cc_collection_term_summary_{runid}.csv",
    ]
    paths.extend(sorted(quality_dir.glob(f"cc_val_sample*_{runid}.csv")))
    return paths


def _warc_parquet_paths(
    config: Dict, *, year: int | str, track: str, batch: int | str, runid: str
) -> List[Path]:
    warc_dir = collection_warc_dir(config, track=track, year=year, batch=batch)
    return [
        warc_dir / f"cc_enriched_hits_{runid}.parquet",
        warc_dir / f"cc_validated_hits_warc_{runid}.parquet",
    ]


def _quality_parquet_paths(
    config: Dict, *, year: int | str, track: str, batch: int | str, runid: str
) -> List[Path]:
    quality_dir = collection_quality_dir(config, track=track, year=year, batch=batch)
    return [
        quality_dir / f"cc_document_quality_hits_{runid}.parquet",
        quality_dir / f"cc_corpus_texts_document_quality_{runid}.parquet",
    ]


def _delete_existing_paths(paths: Iterable[Path], *, logger, label: str) -> int:
    removed = 0
    for path in paths:
        if not path.exists():
            continue
        path.unlink()
        removed += 1
    if removed:
        logger.info("Removed %d local %s files", removed, label)
    return removed


def _cleanup_corpus_quality_sources(config: Dict, *, logger) -> int:
    corpus_paths = _latest_corpus_quality_paths(config, require_all_years=False)
    parquet_paths: List[Path] = []
    for corpus_path in corpus_paths:
        runid = _runid_from_path(
            corpus_path,
            "cc_corpus_texts_document_quality_",
            ".parquet",
        )
        parquet_paths.append(corpus_path)
        parquet_paths.append(corpus_path.with_name(f"cc_document_quality_hits_{runid}.parquet"))
    return _delete_existing_paths(
        parquet_paths,
        logger=logger,
        label="corpus quality parquet",
    )


def _throughput_summary_path(
    config: Dict, *, year: int | str, track: str, batch: int | str, runid: str
) -> Path:
    return (
        collection_metrics_dir(config, track=track, year=year, batch=batch)
        / f"cc_collection_throughput_summary_{runid}.csv"
    )


def _manifest_dir() -> Path:
    path = Path("data/manifests")
    path.mkdir(parents=True, exist_ok=True)
    return path


def _parse_crawl_id(crawl_id: str) -> Tuple[int, int]:
    match = CRAWL_ID_RE.match(crawl_id)
    if match is None:
        raise ValueError(f"Unsupported CC-MAIN crawl id format: {crawl_id}")
    return int(match.group("year")), int(match.group("week"))


def _parse_commoncrawl_datetime(value: object) -> Optional[dt.datetime]:
    value_str = str(value or "").strip()
    if not value_str:
        return None
    try:
        return dt.datetime.fromisoformat(value_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _crawl_reference_datetime(item: Dict[str, object]) -> Tuple[dt.datetime, str]:
    start = _parse_commoncrawl_datetime(item.get("from"))
    end = _parse_commoncrawl_datetime(item.get("to"))
    if start is not None and end is not None:
        return start + (end - start) / 2, "crawl_midpoint"
    if start is not None:
        return start, "crawl_start"

    year, week = _parse_crawl_id(str(item["id"]))
    return dt.datetime.fromisocalendar(year, week, 1), "id_iso_week_monday"


def _url_exists(url: str, timeout_s: int) -> bool:
    try:
        request = Request(url, method="HEAD")
        with urlopen(request, timeout=timeout_s):
            return True
    except (HTTPError, URLError, TimeoutError, OSError):
        try:
            request = Request(url, headers={"Range": "bytes=0-0"})
            with urlopen(request, timeout=timeout_s):
                return True
        except (HTTPError, URLError, TimeoutError, OSError):
            return False


def _availability_for_crawl(
    item: Dict[str, object], *, require_remote_checks: bool, timeout_s: int
) -> Dict[str, object]:
    crawl_id = str(item["id"])
    wet_url = f"{COMMONCRAWL_DATA_BASE_URL}/crawl-data/{crawl_id}/wet.paths.gz"
    warc_url = f"{COMMONCRAWL_DATA_BASE_URL}/crawl-data/{crawl_id}/warc.paths.gz"
    cdx_api = str(item.get("cdx-api", "")).strip()

    if not require_remote_checks:
        return {
            "wet_paths_url": wet_url,
            "warc_paths_url": warc_url,
            "index_api_url": cdx_api,
            "wet_paths_available": None,
            "warc_paths_available": None,
            "index_available": bool(cdx_api),
            "all_required_available": bool(cdx_api),
            "remote_checks_performed": False,
        }

    wet_available = _url_exists(wet_url, timeout_s)
    warc_available = _url_exists(warc_url, timeout_s)
    return {
        "wet_paths_url": wet_url,
        "warc_paths_url": warc_url,
        "index_api_url": cdx_api,
        "wet_paths_available": wet_available,
        "warc_paths_available": warc_available,
        "index_available": bool(cdx_api),
        "all_required_available": wet_available and warc_available and bool(cdx_api),
        "remote_checks_performed": True,
    }


def _load_collinfo(url: str, timeout_s: int) -> List[Dict[str, object]]:
    with urlopen(url, timeout=timeout_s) as response:
        payload = json.load(response)
    if not isinstance(payload, list):
        raise ValueError(f"Expected a list from {url}")
    return [item for item in payload if isinstance(item, dict)]


def _crawl_candidates_for_year(
    collinfo: List[Dict[str, object]], *, year: int, anchor_date: dt.datetime
) -> List[Dict[str, object]]:
    candidates: List[Dict[str, object]] = []
    for item in collinfo:
        crawl_id = str(item.get("id", "")).strip()
        if not CRAWL_ID_RE.match(crawl_id):
            continue
        crawl_year, _ = _parse_crawl_id(crawl_id)
        if crawl_year != year:
            continue
        reference_dt, reference_source = _crawl_reference_datetime(item)
        distance_days = abs((reference_dt - anchor_date).total_seconds()) / 86_400
        candidates.append(
            {
                "year": year,
                "crawl_id": crawl_id,
                "name": str(item.get("name", "")).strip(),
                "from": str(item.get("from", "")).strip(),
                "to": str(item.get("to", "")).strip(),
                "reference_datetime": reference_dt.isoformat(timespec="seconds"),
                "reference_date_source": reference_source,
                "distance_days": round(distance_days, 6),
                "cdx_api": str(item.get("cdx-api", "")).strip(),
            }
        )
    return sorted(
        candidates,
        key=lambda item: (
            float(item["distance_days"]),
            str(item["reference_datetime"]),
            str(item["crawl_id"]),
        ),
    )


def _latest_collection_manifest(year: int | str, track: str, batch: int | str) -> Path:
    pattern = f"cc_collection_wet_{track}_{year}_b{int(batch):03d}_*.jsonl"
    matches = sorted(_manifest_dir().glob(pattern))
    if not matches:
        raise FileNotFoundError(
            f"No collection WET manifest found for {track} {year} batch {batch}"
        )
    return matches[-1]


def _iter_wet_paths(config: Dict, crawl_id: str, logger=None) -> Iterable[str]:
    url = f"{COMMONCRAWL_BASE_URL}/crawl-data/{crawl_id}/wet.paths.gz"
    payload = _read_url_bytes_with_retries(
        config,
        url=url,
        logger=logger,
        label=f"wet.paths.gz {crawl_id}",
    )
    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as gz:
        for line in gz:
            path = line.decode("utf-8").strip()
            if path:
                yield path


def _deterministic_wet_order(
    config: Dict,
    year: int | str,
    track: str,
    logger=None,
) -> List[str]:
    crawl_id = _crawl_id_for_year(config, year)
    paths = list(_iter_wet_paths(config, crawl_id, logger=logger))
    seed = _stable_seed(int(config.get("project", {}).get("seed", 123)), f"{crawl_id}:{track}")
    rng = random.Random(seed)
    rng.shuffle(paths)
    return paths


def _selected_paths(
    config: Dict,
    year: int | str,
    track: str,
    batch: int,
    logger=None,
) -> List[str]:
    ordered_paths = _deterministic_wet_order(config, year, track, logger=logger)
    start, end = _batch_bounds(config, track, batch)
    return ordered_paths[start:end]


def _local_wet_filename(crawl_id: str, year: int | str, track: str, batch: int, idx: int) -> str:
    return f"{crawl_id}_{track}_{year}_b{batch:03d}_{idx:04d}.wet.gz"


def select_collection_crawls(config: Dict) -> Path:
    out_dir = processed_manifest_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "collection_crawl_map.json"
    collection = _collection_cfg(config)
    window = collection.get("window", {})
    selection_cfg = collection.get("crawl_selection", {})
    start_year = int(window.get("primary_start_year", 2014))
    end_year = int(window.get("end_year", 2026))
    anchor_month_day = str(selection_cfg.get("anchor_month_day", "04-15")).strip()
    collinfo_url = str(selection_cfg.get("collinfo_url", COLLINFO_URL)).strip() or COLLINFO_URL
    timeout_s = max(5, int(selection_cfg.get("availability_timeout_s", 20)))
    require_remote_checks = bool(selection_cfg.get("require_availability_checks", True))

    try:
        anchor_month, anchor_day = [int(part) for part in anchor_month_day.split("-", 1)]
    except ValueError as exc:
        raise ValueError("collection.crawl_selection.anchor_month_day must be MM-DD") from exc

    collinfo = _load_collinfo(collinfo_url, timeout_s=timeout_s)
    selected: List[Dict[str, object]] = []
    rejected_unavailable: List[Dict[str, object]] = []

    for year in range(start_year, end_year + 1):
        anchor_date = dt.datetime(year, anchor_month, anchor_day)
        candidates = _crawl_candidates_for_year(collinfo, year=year, anchor_date=anchor_date)
        if not candidates:
            raise ValueError(f"No CC-MAIN candidates found for {year} in {collinfo_url}")

        selected_candidate: Optional[Dict[str, object]] = None
        for candidate in candidates:
            source_item = next(item for item in collinfo if item.get("id") == candidate["crawl_id"])
            availability = _availability_for_crawl(
                source_item,
                require_remote_checks=require_remote_checks,
                timeout_s=timeout_s,
            )
            candidate = {
                **candidate,
                "anchor_date": anchor_date.date().isoformat(),
                "selection_rank": len(rejected_unavailable) + 1,
                "availability": availability,
            }
            if availability["all_required_available"]:
                selected_candidate = candidate
                break
            rejected_unavailable.append(candidate)

        if selected_candidate is None:
            raise ValueError(f"No available WET/WARC/index crawl found for {year}")
        selected_candidate["selection_reason"] = (
            "nearest available CC-MAIN crawl to annual April anchor by crawl midpoint"
        )
        selected.append(selected_candidate)

    payload = {
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source_url": collinfo_url,
        "anchor_month_day": anchor_month_day,
        "selection_rule": (
            "For each year, select the available CC-MAIN crawl whose from/to midpoint "
            "is closest to the configured annual anchor date."
        ),
        "availability_required": {
            "wet_paths_gz": True,
            "warc_paths_gz": True,
            "cdx_index_api": True,
            "remote_checks_performed": require_remote_checks,
        },
        "primary_window": {
            "start_year": start_year,
            "end_year": end_year,
        },
        "fallback_window": {
            "start_year": int(window.get("fallback_start_year", 2016)),
            "end_year": end_year,
        },
        "crawl_map": [{"year": item["year"], "crawl_id": item["crawl_id"]} for item in selected],
        "selected_crawls": selected,
        "rejected_unavailable_candidates": rejected_unavailable,
    }
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    versioned_manifest = str(selection_cfg.get("versioned_manifest_path", "")).strip()
    if versioned_manifest:
        versioned_path = Path(versioned_manifest)
        versioned_path.parent.mkdir(parents=True, exist_ok=True)
        versioned_path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    return out_path


def sample_collection_wet(
    config: Dict,
    *,
    year: int | str,
    track: Optional[str] = None,
    batch: int | str = 1,
) -> Path:
    track = _normalise_track(track)
    batch_int = int(batch)
    if track == "trend":
        raise ValueError(
            "The WET-first trend collection path has been retired. Use cc-trend-run-batch "
            "or `make trend` for the publication-year WARC trend track."
        )
    runid = _utc_runid()
    logger = _setup_logger(Path("reports/logs"), f"cc-collection-sample-{track}")
    crawl_id = _crawl_id_for_year(config, year)
    selected = _selected_paths(config, year, track, batch_int, logger=logger)
    if not selected:
        raise ValueError(f"No WET paths selected for {track} {year} batch {batch_int}")

    manifest_path = (
        _manifest_dir() / f"cc_collection_wet_{track}_{year}_b{batch_int:03d}_{runid}.jsonl"
    )
    with manifest_path.open("w", encoding="utf-8") as handle:
        for idx, wet_path in enumerate(selected, start=1):
            source_url = f"{COMMONCRAWL_BASE_URL}/{wet_path}"
            handle.write(
                json.dumps(
                    {
                        "year": str(year),
                        "track": track,
                        "batch": str(batch_int),
                        "crawl_id": crawl_id,
                        "sampled_wet_path": wet_path,
                        "source_url": source_url,
                        "local_filename": _local_wet_filename(
                            crawl_id, year, track, batch_int, idx
                        ),
                        "sample_index": str(idx),
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    },
                    sort_keys=True,
                )
                + "\n"
            )
    logger.info("Wrote collection WET manifest %s", manifest_path)
    return manifest_path


def download_collection_wet(
    config: Dict,
    *,
    year: int | str,
    track: Optional[str] = None,
    batch: int | str = 1,
    manifest: Optional[str] = None,
) -> int:
    track = _normalise_track(track)
    manifest_path = Path(manifest) if manifest else _latest_collection_manifest(year, track, batch)
    entries = read_manifest(manifest_path)
    output_dir = _raw_wet_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    _require_raw_wet_download_space(config, output_dir)
    logger = _setup_logger(Path("reports/logs"), f"cc-collection-download-{track}")
    downloaded = 0
    for entry in entries:
        source_url = str(entry["source_url"])
        local_filename = str(entry["local_filename"])
        dest = output_dir / local_filename
        if dest.exists():
            logger.info("Skipping existing %s", dest)
            downloaded += 1
            continue
        logger.info("Downloading %s -> %s", source_url, dest)
        _download_url_with_retries(
            config,
            url=source_url,
            dest=dest,
            logger=logger,
            label=f"WET download {local_filename}",
        )
        downloaded += 1
    logger.info("Downloaded or confirmed %d WET files", downloaded)
    return downloaded


def cleanup_collection_wet(
    config: Dict,
    *,
    year: int | str,
    track: Optional[str] = None,
    batch: int | str = 1,
    manifest: Optional[str] = None,
) -> int:
    del config
    track = _normalise_track(track)
    manifest_path = Path(manifest) if manifest else _latest_collection_manifest(year, track, batch)
    entries = read_manifest(manifest_path)
    output_dir = _raw_wet_dir()
    removed = 0
    for entry in entries:
        wet_path = output_dir / str(entry["local_filename"])
        if wet_path.exists():
            wet_path.unlink()
            removed += 1
        partial_path = wet_path.with_name(f"{wet_path.name}.part")
        partial_path.unlink(missing_ok=True)
    return removed


def _collection_stage_config(
    config: Dict,
    *,
    year: int | str,
    track: Optional[str] = None,
    batch: int | str = 1,
) -> Dict:
    track = _normalise_track(track)
    batch_int = int(batch)
    adapted = copy.deepcopy(config)
    collection = _collection_cfg(adapted)
    working_dir = collection_track_working_dir(adapted, track=track, year=year, batch=batch_int)
    adapted["run_context"] = {
        "stage": "collection",
        "label": "collection",
        "track": track,
        "collection_year": str(year),
        "batch": str(batch_int),
    }
    adapted["project"] = {**adapted.get("project", {}), "out_dir": str(working_dir / "wet_scan")}
    adapted["crawl"] = {"crawl_ids": [_crawl_id_for_year(adapted, year)]}
    adapted["terms"] = collection.get("terms", adapted.get("terms", {}))
    adapted["filters"] = collection.get("filters", adapted.get("filters", {}))
    adapted["boilerplate"] = collection.get("boilerplate", adapted.get("boilerplate", {}))
    try:
        manifest_path = _latest_collection_manifest(year, track, batch_int)
        entries = read_manifest(manifest_path)
        adapted["inputs"] = {"wet_filenames": [str(entry["local_filename"]) for entry in entries]}
    except FileNotFoundError:
        adapted["inputs"] = adapted.get("inputs", {})

    stage_cfg = {
        **collection,
        "url_export_dir": str(
            collection_url_export_dir(adapted, track=track, year=year, batch=batch_int)
        ),
        "pointer_cache_dir": str(
            collection_pointer_cache_dir(adapted, track=track, year=year, batch=batch_int)
        ),
        "warc_out_dir": str(collection_warc_dir(adapted, track=track, year=year, batch=batch_int)),
        "document_quality_out_dir": str(
            collection_quality_dir(adapted, track=track, year=year, batch=batch_int)
        ),
        "metrics_out_dir": str(
            collection_metrics_dir(adapted, track=track, year=year, batch=batch_int)
        ),
    }
    adapted["collection_stage"] = stage_cfg
    return adapted


def scan_collection(
    config: Dict, *, year: int | str, track: Optional[str], batch: int | str
) -> Path:
    adapted = _collection_stage_config(config, year=year, track=track, batch=batch)
    return scan_wet_files(adapted, Path("configs/commoncrawl_collection.yaml"))


def export_collection_urls(
    config: Dict, *, year: int | str, track: Optional[str], batch: int | str
) -> Path:
    return export_urls(_collection_stage_config(config, year=year, track=track, batch=batch))


def upload_collection_urls(
    config: Dict, *, year: int | str, track: Optional[str], batch: int | str
) -> Path:
    return upload_urls_to_s3(_collection_stage_config(config, year=year, track=track, batch=batch))


def install_collection_indexes(config: Dict, url_export_uri: Optional[str] = None) -> Path:
    return install_indexes_remote(
        _collection_stage_config(config, year=_collection_years(config)[0], track="corpus"),
        url_export_uri=url_export_uri,
    )


def start_collection_index_server(config: Dict) -> Path:
    return start_index_server_remote(
        _collection_stage_config(config, year=_collection_years(config)[0], track="corpus")
    )


def resolve_collection_urls(
    config: Dict,
    *,
    year: int | str,
    track: Optional[str],
    batch: int | str,
    url_export_uri: Optional[str] = None,
    s3_output_prefix: Optional[str] = None,
) -> Path:
    return resolve_urls_remote(
        _collection_stage_config(config, year=year, track=track, batch=batch),
        url_export_uri=url_export_uri,
        s3_output_prefix=s3_output_prefix,
    )


def extract_collection(
    config: Dict,
    *,
    year: int | str,
    track: Optional[str],
    batch: int | str,
    pointer_cache_uri: Optional[str] = None,
    s3_output_prefix: Optional[str] = None,
) -> Path:
    return extract_pointer_cache(
        _collection_stage_config(config, year=year, track=track, batch=batch),
        pointer_cache_uri=pointer_cache_uri,
        s3_output_prefix=s3_output_prefix,
    )


def quality_collection(
    config: Dict, *, year: int | str, track: Optional[str], batch: int | str
) -> Path:
    return document_quality_hits(
        _collection_stage_config(config, year=year, track=track, batch=batch)
    )


def stop_collection_index_server(config: Dict) -> Optional[int]:
    return stop_index_server_remote(
        _collection_stage_config(config, year=_collection_years(config)[0], track="corpus")
    )


def preflight_collection(config: Dict) -> Path:
    runid = _utc_runid()
    logger = _setup_logger(Path("reports/logs"), "cc-collection-preflight")
    payload: Dict[str, object] = {
        "runid": runid,
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "checks": {},
    }
    checks = payload["checks"]
    assert isinstance(checks, dict)

    checks["configured_years"] = _collection_years(config)
    bucket, prefix = _aws_s3_base(config)
    checks["s3_bucket"] = bucket
    checks["s3_prefix"] = prefix
    raw_wet_dir = _raw_wet_dir()
    raw_wet_free_gb = _disk_free_gb(raw_wet_dir)
    raw_wet_min_free_gb = _raw_wet_min_free_gb(config)
    checks["raw_wet_dir"] = str(raw_wet_dir)
    checks["raw_wet_free_gb"] = round(raw_wet_free_gb, 3)
    checks["raw_wet_min_free_gb_before_download"] = raw_wet_min_free_gb
    if raw_wet_free_gb < raw_wet_min_free_gb:
        raise RuntimeError(
            "Insufficient free disk space before collection: "
            f"{raw_wet_free_gb:.2f} GiB available in {raw_wet_dir}, "
            f"requires at least {raw_wet_min_free_gb:.2f} GiB."
        )

    import boto3  # type: ignore

    sts_client = boto3.client("sts")
    identity = sts_client.get_caller_identity()
    checks["aws_identity_arn"] = identity.get("Arn", "")

    s3_client = boto3.client("s3")
    smoke_key = "/".join(part for part in [prefix, "_preflight", f"{runid}.txt"] if part)
    s3_client.put_object(Bucket=bucket, Key=smoke_key, Body=b"ok\n")
    s3_client.delete_object(Bucket=bucket, Key=smoke_key)
    checks["s3_write_delete_ok"] = True

    stage_cfg = _collection_stage_config(config, year=_collection_years(config)[0], track="corpus")
    resolve_cfg = stage_cfg["collection_stage"].get("resolve_remote", {})
    host = str(resolve_cfg.get("server_host", "127.0.0.1"))
    port = int(resolve_cfg.get("server_port", 8080))
    try:
        with socket.create_connection((host, port), timeout=2):
            port_open = True
    except OSError:
        port_open = False
    checks["index_server_host"] = host
    checks["index_server_port"] = port
    checks["index_server_port_open"] = port_open

    pid_path = Path(
        str(
            resolve_cfg.get(
                "server_pid_path",
                "~/.cache/msc-nlp-therapy-speak/collection_index_server/index_server.pid",
            )
        )
    ).expanduser()
    checks["index_server_pid_path"] = str(pid_path)
    if pid_path.exists():
        try:
            pid = int(pid_path.read_text().strip())
            os.kill(pid, 0)
            checks["index_server_pid_alive"] = True
            checks["index_server_pid"] = pid
        except (OSError, ValueError):
            checks["index_server_pid_alive"] = False
    else:
        checks["index_server_pid_alive"] = False

    out_dir = processed_manifest_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / f"collection_preflight_{runid}.json"
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    logger.info("Wrote collection preflight manifest %s", manifest_path)
    return manifest_path


def migrate_collection_interim_layout(config: Dict, *, apply: bool = False) -> Path:
    """Move legacy stage-first corpus artifacts into the track-first corpus layout."""
    runid = _utc_runid()
    logger = _setup_logger(Path("reports/logs"), "cc-collection-migrate-interim-layout")
    root = collection_interim_dir(config)
    stage_map = {
        "metrics": "metrics",
        "pointer_cache": "pointer_cache",
        "quality": "quality",
        "url_exports": "url_exports",
        "warc_output": "warc",
        "wet_scan": "wet_scan",
    }
    rows: List[Dict[str, object]] = []

    for source_stage, target_stage in stage_map.items():
        source_stage_root = root / source_stage / "corpus"
        if not source_stage_root.exists():
            continue
        for source_dir in sorted(source_stage_root.glob("*/batch_*")):
            if not source_dir.is_dir():
                continue
            year = source_dir.parent.name
            batch_label = source_dir.name
            target_dir = root / "corpus" / year / batch_label / target_stage
            row: Dict[str, object] = {
                "source": str(source_dir),
                "target": str(target_dir),
                "source_stage": source_stage,
                "target_stage": target_stage,
                "year": year,
                "batch": batch_label,
                "status": "planned",
            }
            if target_dir.exists():
                row["status"] = "conflict_target_exists"
                rows.append(row)
                continue
            rows.append(row)

    conflict_count = sum(1 for row in rows if str(row["status"]).startswith("conflict"))
    if apply and conflict_count:
        raise FileExistsError(
            "Cannot apply corpus interim layout migration because target paths already exist. "
            "Run the dry-run command and resolve conflicts first."
        )

    if apply:
        for row in rows:
            if row["status"] != "planned":
                continue
            source = Path(str(row["source"]))
            target = Path(str(row["target"]))
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(target))
            row["status"] = "moved"
            logger.info("Moved %s -> %s", source, target)
    else:
        logger.info("Dry-run only; pass --apply to move corpus interim artifacts.")

    summary_path = Path("reports/logs") / f"cc-collection-migrate-interim-layout_{runid}.csv"
    pd.DataFrame(rows).to_csv(summary_path, index=False)
    logger.info(
        "Wrote migration summary %s | rows=%d conflicts=%d apply=%s",
        summary_path,
        len(rows),
        conflict_count,
        apply,
    )
    return summary_path


def run_collection_year(
    config: Dict,
    *,
    year: int | str,
    track: Optional[str],
    batch: int | str = 1,
    build_processed: bool = False,
) -> Path:
    track = _normalise_track(track)
    if track == "trend":
        raise ValueError(
            "The WET-first trend collection path has been retired. Use cc-trend-run-batch "
            "or `make trend` for the publication-year WARC trend track."
        )
    batch_int = int(batch)
    runid = _utc_runid()
    logger = _setup_logger(Path("reports/logs"), f"cc-collection-run-year-{track}")
    steps: List[Dict[str, object]] = []

    def _record_step(name: str, output: object) -> None:
        steps.append(
            {
                "step": name,
                "output": str(output),
                "completed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
        )

    logger.info("Running collection year=%s track=%s batch=%03d", year, track, batch_int)
    wet_manifest_path = sample_collection_wet(config, year=year, track=track, batch=batch_int)
    _record_step("sample_wet", wet_manifest_path)
    _record_step(
        "download_wet",
        download_collection_wet(
            config,
            year=year,
            track=track,
            batch=batch_int,
            manifest=str(wet_manifest_path),
        ),
    )
    _record_step("scan_wet", scan_collection(config, year=year, track=track, batch=batch_int))
    _record_step(
        "export_urls",
        export_collection_urls(config, year=year, track=track, batch=batch_int),
    )
    upload_manifest_path = upload_collection_urls(config, year=year, track=track, batch=batch_int)
    _record_step("upload_urls", upload_manifest_path)

    upload_manifest = _read_latest_url_upload_manifest(
        config, year=year, track=track, batch=batch_int
    )
    url_export_uri = str(upload_manifest["csv_s3_uri"])
    export_runid = str(upload_manifest["runid"])
    batch_label = _batch_label(batch_int)

    _record_step(
        "install_indexes",
        install_collection_indexes(config, url_export_uri=url_export_uri),
    )
    stopped_pid = stop_collection_index_server(config)
    if stopped_pid is not None:
        _record_step("stop_existing_index_server", stopped_pid)
    _record_step("start_index_server", start_collection_index_server(config))

    resolve_output_prefix = _collection_s3_uri(
        config, "pointer_cache", track, str(year), batch_label, export_runid
    )
    resolve_output_prefix = resolve_output_prefix.rstrip("/") + "/"
    pointer_cache_path = resolve_collection_urls(
        config,
        year=year,
        track=track,
        batch=batch_int,
        url_export_uri=url_export_uri,
        s3_output_prefix=resolve_output_prefix,
    )
    _record_step("resolve", pointer_cache_path)

    pointer_cache_uri = f"{resolve_output_prefix}cc_pointer_cache_{export_runid}.parquet"
    warc_output_prefix = _collection_s3_uri(
        config, "warc_output", track, str(year), batch_label, export_runid
    )
    warc_output_prefix = warc_output_prefix.rstrip("/") + "/"
    extract_path = extract_collection(
        config,
        year=year,
        track=track,
        batch=batch_int,
        pointer_cache_uri=pointer_cache_uri,
        s3_output_prefix=warc_output_prefix,
    )
    _record_step("extract", extract_path)
    extract_runid = _runid_from_path(extract_path, "cc_enriched_hits_", ".parquet")
    warc_summary_path = (
        collection_warc_dir(
            config,
            track=track,
            year=year,
            batch=batch_int,
        )
        / f"cc_collection_summary_{extract_runid}.csv"
    )
    _require_accepted_warc_summary(
        config,
        summary_path=warc_summary_path,
        logger=logger,
        year=year,
        track=track,
    )
    _record_step("accept_warc", warc_summary_path)
    quality_runid = ""
    quality_output_prefix = ""
    quality_uploaded_uris: List[str] = []
    throughput_runid = extract_runid
    throughput_summary_path = _throughput_summary_path(
        config,
        year=year,
        track=track,
        batch=batch_int,
        runid=throughput_runid,
    )
    quality_summary_path = quality_collection(config, year=year, track=track, batch=batch_int)
    _record_step("quality", quality_summary_path)

    quality_runid = _runid_from_path(
        quality_summary_path,
        "cc_collection_summary_",
        ".csv",
    )
    throughput_runid = quality_runid
    throughput_summary_path = _throughput_summary_path(
        config,
        year=year,
        track=track,
        batch=batch_int,
        runid=throughput_runid,
    )
    quality_output_prefix = (
        _collection_s3_uri(
            config, "quality", track, str(year), batch_label, quality_runid
        ).rstrip("/")
        + "/"
    )
    quality_uploaded_uris = _upload_paths_to_s3(
        _quality_output_paths(
            config,
            year=year,
            track=track,
            batch=batch_int,
            runid=quality_runid,
        ),
        quality_output_prefix,
    )
    _record_step("upload_quality", quality_output_prefix)
    _record_step(
        "cleanup_warc_parquet",
        _delete_existing_paths(
            _warc_parquet_paths(
                config,
                year=year,
                track=track,
                batch=batch_int,
                runid=extract_runid,
            ),
            logger=logger,
            label="WARC parquet",
        ),
    )
    if track == "trend":
        _record_step(
            "cleanup_quality_parquet",
            _delete_existing_paths(
                _quality_parquet_paths(
                    config,
                    year=year,
                    track=track,
                    batch=batch_int,
                    runid=quality_runid,
                ),
                logger=logger,
                label="trend quality parquet",
            ),
        )
    else:
        detailed_quality_path = (
            collection_quality_dir(config, track=track, year=year, batch=batch_int)
            / f"cc_document_quality_hits_{quality_runid}.parquet"
        )
        _record_step(
            "cleanup_quality_detail_parquet",
            _delete_existing_paths(
                [detailed_quality_path],
                logger=logger,
                label="corpus quality detail parquet",
            ),
        )
    if bool(_raw_wet_cfg(config).get("cleanup_after_successful_year", True)):
        removed_wet_files = cleanup_collection_wet(
            config,
            year=year,
            track=track,
            batch=batch_int,
            manifest=str(wet_manifest_path),
        )
        logger.info("Removed %d raw WET files after successful year", removed_wet_files)
        _record_step("cleanup_raw_wet", removed_wet_files)

    processed_path = ""
    processed_output_prefix = ""
    if build_processed:
        corpus_processed_exists = (
            processed_corpus_dir(config) / "corpus_documents.parquet"
        ).exists()
        if _corpus_quality_outputs_complete(config) or corpus_processed_exists:
            processed_path = str(build_processed_corpus(config))
        else:
            logger.info(
                "Skipping processed corpus build until every configured year has "
                "at least one successful quality output."
            )
        if processed_path:
            _record_step("build_processed", processed_path)
            processed_output_prefix = _processed_output_prefix(config, track)
            upload_processed_output(config, track=track, path=Path(processed_path))
            _record_step("upload_processed", processed_output_prefix)
            _record_step(
                "cleanup_corpus_quality_parquet",
                _cleanup_corpus_quality_sources(config, logger=logger),
            )
    else:
        logger.info(
            "Skipping processed %s build for single year/batch run. "
            "Run the track-level collection or explicit processed builder to finalize once.",
            track,
        )

    metrics_output_prefix = (
        _collection_s3_uri(config, "metrics", track, str(year), batch_label, runid).rstrip("/")
        + "/"
    )
    _record_step("upload_metrics", metrics_output_prefix)
    manifest_payload = {
        "runid": runid,
        "quality_runid": quality_runid,
        "url_export_runid": export_runid,
        "year": str(year),
        "track": track,
        "batch": str(batch_int),
        "url_export_uri": url_export_uri,
        "resolve_output_prefix": resolve_output_prefix,
        "pointer_cache_uri": pointer_cache_uri,
        "warc_output_prefix": warc_output_prefix,
        "quality_output_prefix": quality_output_prefix,
        "quality_uploaded_uris": quality_uploaded_uris,
        "metrics_output_prefix": metrics_output_prefix,
        "throughput_summary_path": str(throughput_summary_path),
        "processed_path": processed_path,
        "processed_output_prefix": processed_output_prefix,
        "steps": steps,
    }
    manifest_path = _write_run_manifest(
        config,
        year=year,
        track=track,
        batch=batch_int,
        payload=manifest_payload,
    )
    _upload_paths_to_s3([manifest_path], metrics_output_prefix)
    _upload_paths_to_s3([throughput_summary_path], metrics_output_prefix)
    logger.info("Wrote collection run manifest %s", manifest_path)
    return manifest_path


def build_processed_corpus(config: Dict) -> Path:
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore

    out_dir = processed_corpus_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "corpus_documents.parquet"
    if out_path.exists():
        corpus_paths = _latest_corpus_quality_paths(config, require_all_years=False)
        if not corpus_paths:
            return out_path
    else:
        corpus_paths = _latest_corpus_quality_paths(config, require_all_years=True)

    source_paths = {str(path) for path in corpus_paths}
    source_keys = {_corpus_source_parts(corpus_path) for corpus_path in corpus_paths}
    source_keys = {
        (year, batch)
        for year, batch in source_keys
        if year is not None and batch is not None
    }

    if out_path.exists():
        existing_keys, existing_source_paths = _processed_corpus_sources(out_path)
        if existing_source_paths and source_paths.issubset(existing_source_paths):
            return out_path
        if not existing_source_paths and source_keys.issubset(existing_keys):
            return out_path

    temp_path = out_dir / f".{out_path.name}.tmp"
    temp_path.unlink(missing_ok=True)
    writer: Optional[pq.ParquetWriter] = None
    schema: Optional[pa.Schema] = None

    def _write_frame(frame: pd.DataFrame) -> None:
        nonlocal writer, schema
        if frame.empty:
            return
        table = pa.Table.from_pandas(frame, preserve_index=False)
        if writer is None:
            schema = table.schema
            writer = pq.ParquetWriter(temp_path, schema)
        else:
            assert schema is not None
            table = table.select(schema.names).cast(schema, safe=False)
        writer.write_table(table)

    try:
        if out_path.exists():
            existing_keys, _ = _processed_corpus_sources(out_path)
            retained_keys = existing_keys - source_keys
            if retained_keys:
                parquet_file = pq.ParquetFile(out_path)
                for batch in parquet_file.iter_batches(batch_size=5000):
                    frame = pa.Table.from_batches([batch]).to_pandas()
                    keep_mask = [
                        source_key in retained_keys
                        for source_key in _corpus_frame_source_keys(frame)
                    ]
                    if any(keep_mask):
                        _write_frame(frame.loc[keep_mask].copy())

        for corpus_path in corpus_paths:
            frame = pd.read_parquet(corpus_path)
            frame["source_corpus_path"] = str(corpus_path)
            source_year, source_batch = _corpus_source_parts(corpus_path)
            frame["source_year"] = source_year
            frame["source_batch"] = source_batch
            _write_frame(frame)

        if writer is None:
            raise FileNotFoundError("No corpus quality parquet inputs or processed corpus found.")
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    finally:
        if writer is not None:
            writer.close()

    temp_path.replace(out_path)
    return out_path


def run_collection_track(config: Dict, *, track: str) -> None:
    track = _normalise_track(track)
    if track == "trend":
        raise ValueError(
            "The WET-first trend collection path has been retired. Use cc-trend-run-batch "
            "or `make trend` for the publication-year WARC trend track."
        )
    logger = _setup_logger(Path("reports/logs"), f"cc-collection-run-{track}")
    for year in _collection_years(config):
        run_collection_year(config, year=year, track=track, batch=1, build_processed=False)
    processed_path = build_processed_corpus(config)
    upload_processed_output(config, track=track, path=processed_path)
    _cleanup_corpus_quality_sources(config, logger=logger)
