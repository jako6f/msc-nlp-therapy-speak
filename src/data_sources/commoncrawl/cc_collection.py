import copy
import datetime as dt
import gzip
import json
import os
import random
import re
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
    processed_trend_dir,
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


def _normalise_track(track: Optional[str]) -> str:
    value = (track or "trend").strip().lower()
    if value not in {"trend", "corpus"}:
        raise ValueError("track must be one of: trend, corpus")
    return value


def _batch_size(config: Dict, track: str) -> int:
    collection = _collection_cfg(config)
    if track == "trend":
        return int(collection.get("trend", {}).get("fixed_wet_files_per_year", 50))
    return int(collection.get("corpus", {}).get("initial_wet_batch_size", 50))


def _batch_label(batch: int | str) -> str:
    return f"batch_{int(batch):03d}"


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
        quality_dir / f"cc_document_quality_hits_{runid}.parquet",
        quality_dir / f"cc_corpus_texts_document_quality_{runid}.parquet",
        quality_dir / f"cc_collection_summary_{runid}.csv",
        quality_dir / f"cc_collection_term_summary_{runid}.csv",
    ]
    paths.extend(sorted(quality_dir.glob(f"cc_val_sample*_{runid}.csv")))
    return paths


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


def _iter_wet_paths(crawl_id: str) -> Iterable[str]:
    url = f"{COMMONCRAWL_BASE_URL}/crawl-data/{crawl_id}/wet.paths.gz"
    with urlopen(url) as response:
        with gzip.GzipFile(fileobj=response) as gz:
            for line in gz:
                path = line.decode("utf-8").strip()
                if path:
                    yield path


def _deterministic_wet_order(config: Dict, year: int | str, track: str) -> List[str]:
    crawl_id = _crawl_id_for_year(config, year)
    paths = list(_iter_wet_paths(crawl_id))
    seed = _stable_seed(int(config.get("project", {}).get("seed", 123)), f"{crawl_id}:{track}")
    rng = random.Random(seed)
    rng.shuffle(paths)
    return paths


def _selected_paths(config: Dict, year: int | str, track: str, batch: int) -> List[str]:
    ordered_paths = _deterministic_wet_order(config, year, track)
    size = _batch_size(config, track)
    start = 0 if track == "trend" else (batch - 1) * size
    end = start + size
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
        "crawl_map": [
            {"year": item["year"], "crawl_id": item["crawl_id"]} for item in selected
        ],
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
    runid = _utc_runid()
    logger = _setup_logger(Path("reports/logs"), f"cc-collection-sample-{track}")
    crawl_id = _crawl_id_for_year(config, year)
    selected = _selected_paths(config, year, track, batch_int)
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
    del config
    track = _normalise_track(track)
    manifest_path = Path(manifest) if manifest else _latest_collection_manifest(year, track, batch)
    entries = read_manifest(manifest_path)
    output_dir = Path("data/raw/wet")
    output_dir.mkdir(parents=True, exist_ok=True)
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
        with urlopen(source_url) as response, dest.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
        downloaded += 1
    logger.info("Downloaded or confirmed %d WET files", downloaded)
    return downloaded


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
        adapted["inputs"] = {
            "wet_filenames": [str(entry["local_filename"]) for entry in entries]
        }
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
    return export_urls(
        _collection_stage_config(config, year=year, track=track, batch=batch)
    )


def upload_collection_urls(
    config: Dict, *, year: int | str, track: Optional[str], batch: int | str
) -> Path:
    return upload_urls_to_s3(
        _collection_stage_config(config, year=year, track=track, batch=batch)
    )


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


def run_collection_year(
    config: Dict,
    *,
    year: int | str,
    track: Optional[str],
    batch: int | str = 1,
    build_processed: bool = True,
) -> Path:
    track = _normalise_track(track)
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
    _record_step(
        "sample_wet",
        sample_collection_wet(config, year=year, track=track, batch=batch_int),
    )
    _record_step(
        "download_wet",
        download_collection_wet(config, year=year, track=track, batch=batch_int),
    )
    _record_step("scan_wet", scan_collection(config, year=year, track=track, batch=batch_int))
    _record_step(
        "export_urls",
        export_collection_urls(config, year=year, track=track, batch=batch_int),
    )
    upload_manifest_path = upload_collection_urls(
        config, year=year, track=track, batch=batch_int
    )
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
    _record_step(
        "extract",
        extract_collection(
            config,
            year=year,
            track=track,
            batch=batch_int,
            pointer_cache_uri=pointer_cache_uri,
            s3_output_prefix=warc_output_prefix,
        ),
    )
    quality_summary_path = quality_collection(config, year=year, track=track, batch=batch_int)
    _record_step("quality", quality_summary_path)

    quality_runid = _runid_from_path(
        quality_summary_path,
        "cc_collection_summary_",
        ".csv",
    )
    quality_output_prefix = (
        _collection_s3_uri(config, "quality", track, str(year), batch_label, quality_runid).rstrip(
            "/"
        )
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

    processed_path = ""
    processed_output_prefix = ""
    if build_processed:
        if track == "trend":
            processed_path = str(build_processed_trend(config))
        else:
            processed_path = str(build_processed_corpus(config))
        _record_step("build_processed", processed_path)
        processed_output_prefix = (
            _collection_s3_uri(config, "processed", track).rstrip("/") + "/"
        )
        _upload_paths_to_s3([Path(processed_path)], processed_output_prefix)
        _record_step("upload_processed", processed_output_prefix)

    metrics_output_prefix = (
        _collection_s3_uri(config, "metrics", track, str(year), batch_label, runid).rstrip("/")
        + "/"
    )
    throughput_summary_path = _throughput_summary_path(
        config,
        year=year,
        track=track,
        batch=batch_int,
        runid=quality_runid,
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


def build_processed_trend(config: Dict) -> Path:
    out_dir = processed_trend_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, object]] = []
    trend_root = collection_interim_dir(config) / "trend_working"
    for summary_path in trend_root.glob("*/warc/cc_collection_summary_*.csv"):
        year = summary_path.parent.parent.name
        metrics = pd.read_csv(summary_path).set_index("metric")["value"].to_dict()
        docs_scanned = float(metrics.get("docs_scanned", 0) or 0)
        wet = float(metrics.get("validated_hits_wet", 0) or 0)
        warc = float(metrics.get("validated_hits_warc", 0) or 0)
        rows.append(
            {
                "year": year,
                "docs_scanned": docs_scanned,
                "validated_hits_wet": wet,
                "validated_hits_warc": warc,
                "validated_hits_wet_per_doc": wet / docs_scanned if docs_scanned else 0.0,
                "validated_hits_warc_per_doc": warc / docs_scanned if docs_scanned else 0.0,
                "warc_over_wet_validation_rate": warc / wet if wet else 0.0,
                "summary_path": str(summary_path),
            }
        )
    out_path = out_dir / "trend_rates.csv"
    pd.DataFrame(rows).sort_values("year").to_csv(out_path, index=False)
    return out_path


def build_processed_corpus(config: Dict) -> Path:
    out_dir = processed_corpus_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    frames: List[pd.DataFrame] = []
    corpus_root = collection_interim_dir(config) / "corpus_working"
    for corpus_path in corpus_root.glob(
        "*/batch_*/quality/cc_corpus_texts_document_quality_*.parquet"
    ):
        frame = pd.read_parquet(corpus_path)
        frame["source_corpus_path"] = str(corpus_path)
        frames.append(frame)
    out_path = out_dir / "corpus_documents.parquet"
    if frames:
        pd.concat(frames, ignore_index=True).to_parquet(out_path, index=False)
    else:
        pd.DataFrame().to_parquet(out_path, index=False)
    return out_path


def run_collection_track(config: Dict, *, track: str) -> None:
    track = _normalise_track(track)
    for year in _collection_years(config):
        run_collection_year(config, year=year, track=track, batch=1, build_processed=False)
    if track == "trend":
        processed_path = build_processed_trend(config)
    else:
        processed_path = build_processed_corpus(config)
    processed_output_prefix = _collection_s3_uri(config, "processed", track).rstrip("/") + "/"
    _upload_paths_to_s3([processed_path], processed_output_prefix)
