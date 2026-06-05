import csv
import gzip
import io
import json
import os
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd
import yaml

from src.pathing import (
    pointer_cache_output_dir,
    url_export_output_dir,
    working_output_dir,
)

from .cc_warc import (
    DOC_KEY_COLUMNS,
    LookupRecord,
    _http_get_bytes,
    _latest_validated_hits_wet,
    _lookup_outcome_from_candidates,
    _mime_is_html,
    _require_stage,
    _setup_logger,
    _utc_runid,
)


@dataclass(frozen=True)
class AwsConfig:
    region: str
    s3_bucket: str
    s3_prefix: str
    ec2_instance_type: str
    ec2_ssh_key_name: str
    ec2_instance_profile: str


@dataclass(frozen=True)
class ResolveRemoteConfig:
    provider: str
    index_manifest_base_url: str
    index_file_base_url: str
    request_timeout_s: int
    max_workers: int
    progress_log_every_shards: int
    server_host: str
    server_port: int
    server_runtime_dir: Path
    server_collections_dir: Path
    server_log_path: Path
    server_pid_path: Path
    startup_timeout_s: int
    url_workers: int
    progress_log_every_urls: int
    query_limit: int


def _load_boto3():
    try:
        import boto3  # type: ignore
    except ImportError as exc:  # pragma: no cover - dependency dependent
        raise RuntimeError(
            "Missing dependency 'boto3'. Install the collection AWS dependencies "
            "before using S3 upload or remote resolver commands."
        ) from exc
    return boto3


def _expand_path(value: str) -> Path:
    return Path(value).expanduser()


def _aws_config(config: Dict, stage_key: str = "collection_stage") -> AwsConfig:
    aws_cfg = config.get(stage_key, {}).get("aws", {})
    s3_cfg = aws_cfg.get("s3", {})
    ec2_cfg = aws_cfg.get("ec2", {})
    bucket = str(s3_cfg.get("bucket", "")).strip()
    if not bucket:
        raise ValueError(
            f"{stage_key}.aws.s3.bucket is required. Create an S3 bucket in us-east-1 "
            "and add its name to configs/local/aws.yaml or MSC_NLP_LOCAL_CONFIG."
        )
    return AwsConfig(
        region=str(aws_cfg.get("region", "us-east-1")).strip() or "us-east-1",
        s3_bucket=bucket,
        s3_prefix=str(s3_cfg.get("prefix", f"msc-nlp-therapy-speak/{stage_key}")).strip("/")
        or f"msc-nlp-therapy-speak/{stage_key}",
        ec2_instance_type=str(ec2_cfg.get("instance_type", "m7i-flex.large")).strip()
        or "m7i-flex.large",
        ec2_ssh_key_name=str(ec2_cfg.get("ssh_key_name", "")).strip(),
        ec2_instance_profile=str(ec2_cfg.get("instance_profile", "")).strip(),
    )


def _resolve_remote_config(
    config: Dict, stage_key: str = "collection_stage"
) -> ResolveRemoteConfig:
    raw_cfg = config.get(stage_key, {}).get("resolve_remote", {})
    runtime_dir = _expand_path(
        str(
            raw_cfg.get(
                "server_runtime_dir",
                f"~/.cache/msc-nlp-therapy-speak/{stage_key}_index_server",
            )
        )
    )
    collections_dir_raw = str(raw_cfg.get("server_collections_dir", "")).strip()
    collections_dir = (
        _expand_path(collections_dir_raw) if collections_dir_raw else runtime_dir / "collections"
    )
    server_log_raw = str(raw_cfg.get("server_log_path", "")).strip()
    server_pid_raw = str(raw_cfg.get("server_pid_path", "")).strip()

    return ResolveRemoteConfig(
        provider=str(raw_cfg.get("provider", "local_index_server")).strip() or "local_index_server",
        index_manifest_base_url=str(
            raw_cfg.get("index_manifest_base_url", "https://data.commoncrawl.org")
        ).rstrip("/"),
        index_file_base_url=str(
            raw_cfg.get("index_file_base_url", "https://data.commoncrawl.org")
        ).rstrip("/"),
        request_timeout_s=max(30, int(raw_cfg.get("request_timeout_s", 120))),
        max_workers=max(1, int(raw_cfg.get("max_workers", 4))),
        progress_log_every_shards=max(1, int(raw_cfg.get("progress_log_every_shards", 10))),
        server_host=str(raw_cfg.get("server_host", "127.0.0.1")).strip() or "127.0.0.1",
        server_port=max(1, int(raw_cfg.get("server_port", 8080))),
        server_runtime_dir=runtime_dir,
        server_collections_dir=collections_dir,
        server_log_path=(
            _expand_path(server_log_raw) if server_log_raw else runtime_dir / "index_server.log"
        ),
        server_pid_path=(
            _expand_path(server_pid_raw) if server_pid_raw else runtime_dir / "index_server.pid"
        ),
        startup_timeout_s=max(10, int(raw_cfg.get("startup_timeout_s", 60))),
        url_workers=max(1, int(raw_cfg.get("url_workers", 16))),
        progress_log_every_urls=max(1, int(raw_cfg.get("progress_log_every_urls", 50))),
        query_limit=max(1, int(raw_cfg.get("query_limit", 25))),
    )


def _csv_join(values: Iterable[str]) -> str:
    unique_values = sorted({str(value).strip() for value in values if str(value).strip()})
    return "|".join(unique_values)


def _manifest_path(directory: Path, prefix: str, runid: str) -> Path:
    return directory / f"{prefix}_{runid}.json"


def _latest_manifest(directory: Path, prefix: str) -> Tuple[str, Path]:
    latest_runid = ""
    latest_path: Optional[Path] = None
    for path in directory.glob(f"{prefix}_*.json"):
        runid = path.stem.removeprefix(f"{prefix}_")
        if runid > latest_runid:
            latest_runid = runid
            latest_path = path
    if latest_path is None:
        raise FileNotFoundError(f"No {prefix}_*.json found in {directory}")
    return latest_runid, latest_path


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {uri}")
    without_scheme = uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def _s3_uri(bucket: str, key: str) -> str:
    key = key.lstrip("/")
    return f"s3://{bucket}/{key}" if key else f"s3://{bucket}"


def _s3_key_prefix(aws_cfg: AwsConfig, *parts: str) -> str:
    joined = "/".join(part.strip("/") for part in parts if part)
    return f"{aws_cfg.s3_prefix}/{joined}".strip("/")


def _collection_s3_prefix(config: Dict, aws_cfg: AwsConfig, artifact: str, runid: str) -> str:
    run_context = config.get("run_context", {})
    label = str(run_context.get("label", "")).strip()
    if label != "collection":
        return _s3_key_prefix(aws_cfg, artifact, runid)

    track = str(run_context.get("track", "corpus")).strip()
    year = str(run_context.get("collection_year", "")).strip()
    batch = f"batch_{int(run_context.get('batch', 1)):03d}"
    return _s3_key_prefix(aws_cfg, track, year, batch, artifact, runid)


def _load_json(path: Path) -> Dict[str, object]:
    return json.loads(path.read_text())


def _write_json(path: Path, payload: Dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def _write_summary_csv(path: Path, rows: List[List[object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value"])
        writer.writerows([row[:2] for row in rows])


def _build_url_export_df(hits_df: pd.DataFrame) -> pd.DataFrame:
    return (
        hits_df.groupby(DOC_KEY_COLUMNS, dropna=False)
        .agg(
            registered_domain=("registered_domain", "first"),
            term_roles=("term_role", _csv_join),
            term_groups=("term_group", _csv_join),
            matched_terms=("matched_term", _csv_join),
            source_stages=("source_stage", _csv_join),
            source_scopes=("source_scope", _csv_join),
            source_runids=("source_runid", _csv_join),
        )
        .reset_index()
        .sort_values(DOC_KEY_COLUMNS)
        .reset_index(drop=True)
    )


def export_urls(config: Dict) -> Path:
    _require_stage(config, "collection")
    runid = _utc_runid()
    label = str(config.get("run_context", {}).get("label", "collection")).strip() or "collection"
    display_label = label
    file_prefix = f"cc_{label}_urls"
    logger = _setup_logger(Path("reports/logs"), f"cc-{label}-export-urls", runid)
    input_dir = working_output_dir(config, default_name="wet_scan")
    source_runid, hits_path = _latest_validated_hits_wet(input_dir)
    hits_df = pd.read_parquet(hits_path).copy()
    if hits_df.empty:
        raise ValueError("No validated WET hits available for URL export.")
    hits_df["source_stage"] = label
    hits_df["source_scope"] = "combined"
    hits_df["source_runid"] = source_runid
    export_df = _build_url_export_df(hits_df)

    out_dir = url_export_output_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = out_dir / f"{file_prefix}_{runid}.parquet"
    csv_path = out_dir / f"{file_prefix}_{runid}.csv"
    summary_path = out_dir / f"{file_prefix}_summary_{runid}.csv"
    manifest_path = _manifest_path(out_dir, f"{file_prefix}_manifest", runid)

    export_df.to_parquet(parquet_path, index=False)
    export_df.to_csv(csv_path, index=False)

    crawl_counts = export_df.groupby("crawl_id").size().to_dict()
    _write_summary_csv(
        summary_path,
        [
            ["doc_count_total", int(len(export_df)), f"Unique {display_label} URL candidates."],
            [
                "crawl_count",
                int(len(crawl_counts)),
                "Number of crawl partitions represented in the export.",
            ],
            *[
                [
                    f"crawl.{crawl_id}.doc_count",
                    int(count),
                    "Unique URL candidates exported for this crawl.",
                ]
                for crawl_id, count in sorted(crawl_counts.items())
            ],
        ],
    )

    manifest = {
        "runid": runid,
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "doc_count": int(len(export_df)),
        "crawl_ids": sorted(crawl_counts),
        "source_runid": source_runid,
        "csv_path": str(csv_path),
        "parquet_path": str(parquet_path),
        "summary_path": str(summary_path),
    }
    _write_json(manifest_path, manifest)
    logger.info("Wrote %s URL export %s", display_label, parquet_path)
    logger.info("Wrote %s URL CSV %s", display_label, csv_path)
    logger.info("Wrote %s URL manifest %s", display_label, manifest_path)
    return parquet_path


def _upload_file_to_s3(client, local_path: Path, bucket: str, key: str) -> str:
    client.upload_file(str(local_path), bucket, key)
    return _s3_uri(bucket, key)


def upload_urls_to_s3(config: Dict) -> Path:
    _require_stage(config, "collection")
    label = str(config.get("run_context", {}).get("label", "collection")).strip() or "collection"
    display_label = label
    file_prefix = f"cc_{label}_urls"
    runid, manifest_path = _latest_manifest(
        url_export_output_dir(config), f"{file_prefix}_manifest"
    )
    manifest = _load_json(manifest_path)
    aws_cfg = _aws_config(config)
    logger = _setup_logger(Path("reports/logs"), f"cc-{label}-upload-urls", runid)
    boto3 = _load_boto3()
    s3_client = boto3.client("s3", region_name=aws_cfg.region)

    csv_path = Path(str(manifest["csv_path"]))
    upload_prefix = _collection_s3_prefix(config, aws_cfg, "url_exports", runid)
    csv_key = f"{upload_prefix}/{csv_path.name}"
    manifest_key = f"{upload_prefix}/{manifest_path.name}"
    csv_uri = _upload_file_to_s3(s3_client, csv_path, aws_cfg.s3_bucket, csv_key)
    manifest_uri = _upload_file_to_s3(s3_client, manifest_path, aws_cfg.s3_bucket, manifest_key)

    upload_manifest = {
        "runid": runid,
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "bucket": aws_cfg.s3_bucket,
        "region": aws_cfg.region,
        "csv_s3_uri": csv_uri,
        "csv_s3_key": csv_key,
        "manifest_s3_uri": manifest_uri,
        "manifest_s3_key": manifest_key,
        "doc_count": int(manifest["doc_count"]),
        "crawl_ids": manifest["crawl_ids"],
    }
    upload_manifest_path = _manifest_path(
        url_export_output_dir(config), f"cc_{label}_url_upload_manifest", runid
    )
    _write_json(upload_manifest_path, upload_manifest)
    logger.info("Uploaded %s URL CSV to %s", display_label, csv_uri)
    logger.info("Wrote URL upload manifest %s", upload_manifest_path)
    return upload_manifest_path


def _load_export_df_from_s3(uri: str) -> pd.DataFrame:
    boto3 = _load_boto3()
    s3_client = boto3.client("s3")
    bucket, key = _parse_s3_uri(uri)
    buffer = io.BytesIO()
    s3_client.download_fileobj(bucket, key, buffer)
    buffer.seek(0)
    if key.endswith(".parquet"):
        return pd.read_parquet(buffer)
    if key.endswith(".csv"):
        return pd.read_csv(buffer)
    if key.endswith(".json"):
        manifest = json.loads(buffer.getvalue().decode("utf-8"))
        csv_uri = str(manifest.get("csv_s3_uri", "")).strip()
        if not csv_uri:
            raise ValueError(f"Upload manifest {uri} does not contain csv_s3_uri")
        return _load_export_df_from_s3(csv_uri)
    raise ValueError(f"Unsupported URL export URI: {uri}")


def _load_export_df(
    *,
    url_export_dir: Path,
    url_prefix: str,
    upload_manifest_prefix: str,
    url_export_uri: Optional[str],
) -> Tuple[str, pd.DataFrame, str]:
    if url_export_uri:
        if url_export_uri.startswith("s3://"):
            df = _load_export_df_from_s3(url_export_uri)
            runid = Path(url_export_uri).stem.removeprefix(url_prefix)
            return runid, df, url_export_uri
        export_path = Path(url_export_uri)
        if export_path.suffix == ".json":
            manifest = _load_json(export_path)
            csv_path = Path(str(manifest["csv_path"]))
            return str(manifest["runid"]), pd.read_csv(csv_path), str(csv_path)
        if export_path.suffix == ".parquet":
            return (
                export_path.stem.removeprefix(url_prefix),
                pd.read_parquet(export_path),
                str(export_path),
            )
        if export_path.suffix == ".csv":
            return (
                export_path.stem.removeprefix(url_prefix),
                pd.read_csv(export_path),
                str(export_path),
            )
        raise ValueError(f"Unsupported export path: {export_path}")

    runid, upload_manifest_path = _latest_manifest(url_export_dir, upload_manifest_prefix)
    upload_manifest = _load_json(upload_manifest_path)
    csv_uri = str(upload_manifest["csv_s3_uri"])
    return runid, _load_export_df_from_s3(csv_uri), csv_uri


def _collection_file_candidates(crawl_id: str, filename: str) -> List[Tuple[str, str]]:
    if filename == "cluster.idx":
        return [
            (
                f"commoncrawl/cc-index/collections/{crawl_id}/indexes/cluster.idx",
                f"{crawl_id}/indexes/cluster.idx",
            ),
            (
                f"cc-index/collections/{crawl_id}/indexes/cluster.idx",
                f"{crawl_id}/indexes/cluster.idx",
            ),
        ]
    if filename == "metadata.yaml":
        return [
            (
                f"commoncrawl/cc-index/collections/{crawl_id}/metadata.yaml",
                f"{crawl_id}/metadata.yaml",
            ),
            (
                f"cc-index/collections/{crawl_id}/metadata.yaml",
                f"{crawl_id}/metadata.yaml",
            ),
        ]
    raise ValueError(f"Unsupported collection filename: {filename}")


def _download_collection_file_via_https(
    crawl_id: str, filename: str, collections_dir: Path, timeout_s: int
) -> Path:
    last_error = ""
    for remote_path, local_rel_path in _collection_file_candidates(crawl_id, filename):
        local_path = collections_dir / local_rel_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            payload = _http_get_bytes(
                f"https://data.commoncrawl.org/{remote_path}", timeout=timeout_s
            )
            local_path.write_bytes(payload)
            return local_path
        except Exception as exc:  # pragma: no cover - network dependent
            last_error = f"{type(exc).__name__}: {exc}"
    raise FileNotFoundError(
        f"Unable to download {filename} for {crawl_id} from the Common Crawl HTTPS mirror. "
        f"Last error: {last_error}"
    )


def _ensure_collection_metadata(crawl_id: str, collections_dir: Path, timeout_s: int) -> Path:
    try:
        return _download_collection_file_via_https(
            crawl_id, "metadata.yaml", collections_dir, timeout_s
        )
    except FileNotFoundError:
        metadata_path = collections_dir / crawl_id / "metadata.yaml"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(
            yaml.safe_dump(
                {
                    "id": crawl_id,
                    "title": crawl_id,
                    "source": "commoncrawl",
                    "note": (
                        "Synthesized locally because metadata.yaml was not "
                        "present on the HTTPS mirror."
                    ),
                },
                sort_keys=False,
            )
        )
        return metadata_path


def install_indexes_remote(config: Dict, url_export_uri: Optional[str] = None) -> Path:
    _require_stage(config, "collection")
    label = str(config.get("run_context", {}).get("label", "collection")).strip() or "collection"
    display_label = label
    runid, export_df, export_source = _load_export_df(
        url_export_dir=url_export_output_dir(config),
        url_prefix=f"cc_{label}_urls_",
        upload_manifest_prefix=f"cc_{label}_url_upload_manifest",
        url_export_uri=url_export_uri,
    )
    if export_df.empty:
        raise ValueError(f"{display_label} URL export is empty.")

    resolve_cfg = _resolve_remote_config(config)
    logger = _setup_logger(Path("reports/logs"), f"cc-{label}-install-indexes", runid)

    crawl_ids = sorted({str(crawl_id) for crawl_id in export_df["crawl_id"].tolist()})
    resolve_cfg.server_collections_dir.mkdir(parents=True, exist_ok=True)

    downloaded_paths: List[str] = []
    for crawl_id in crawl_ids:
        logger.info("Installing Common Crawl secondary indexes for %s", crawl_id)
        cluster_path = _download_collection_file_via_https(
            crawl_id,
            "cluster.idx",
            resolve_cfg.server_collections_dir,
            resolve_cfg.request_timeout_s,
        )
        metadata_path = _ensure_collection_metadata(
            crawl_id,
            resolve_cfg.server_collections_dir,
            resolve_cfg.request_timeout_s,
        )
        downloaded_paths.extend([str(cluster_path), str(metadata_path)])
        logger.info("Installed %s", cluster_path)
        logger.info("Installed %s", metadata_path)

    manifest_path = resolve_cfg.server_runtime_dir / f"install_manifest_{runid}.json"
    resolve_cfg.server_runtime_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        manifest_path,
        {
            "runid": runid,
            "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "url_export_source": export_source,
            "collections_dir": str(resolve_cfg.server_collections_dir),
            "crawl_ids": crawl_ids,
            "downloaded_paths": downloaded_paths,
        },
    )
    logger.info("Wrote local index install manifest %s", manifest_path)
    return manifest_path


def _server_command() -> List[str]:
    # pywb 2.x documents `warcserver` and `wayback` as the supported server
    # CLIs. Starting through the current interpreter avoids stale/broken
    # console-script wrappers such as `cdx-server`.
    return [
        sys.executable,
        "-c",
        ("import sys; from pywb.apps.cli import warcserver; warcserver(sys.argv[1:])"),
    ]


def _installed_crawl_ids(resolve_cfg: ResolveRemoteConfig) -> List[str]:
    if not resolve_cfg.server_collections_dir.exists():
        return []
    crawl_ids: List[str] = []
    for path in sorted(resolve_cfg.server_collections_dir.iterdir()):
        if not path.is_dir():
            continue
        has_cluster = (path / "indexes" / "cluster.idx").exists() or (path / "cluster.idx").exists()
        has_metadata = (path / "metadata.yaml").exists() or (
            path / "indexes" / "metadata.yaml"
        ).exists()
        if has_cluster and has_metadata:
            crawl_ids.append(path.name)
    return crawl_ids


def _index_endpoint_base(crawl_id: str, resolve_cfg: ResolveRemoteConfig) -> str:
    return f"http://{resolve_cfg.server_host}:{resolve_cfg.server_port}/{crawl_id}/index"


def _index_endpoint_fallback(crawl_id: str, resolve_cfg: ResolveRemoteConfig) -> str:
    return f"http://{resolve_cfg.server_host}:{resolve_cfg.server_port}/{crawl_id}-index"


def _local_index_server_healthcheck(resolve_cfg: ResolveRemoteConfig, crawl_ids: List[str]) -> bool:
    if not crawl_ids:
        return False
    query = urlencode(
        {
            "url": "http://example.com/",
            "output": "json",
            "limit": 1,
        }
    )
    urls = [
        _index_endpoint_base(crawl_ids[0], resolve_cfg) + "?" + query,
        _index_endpoint_fallback(crawl_ids[0], resolve_cfg) + "?" + query,
    ]
    for health_url in urls:
        try:
            with urlopen(health_url, timeout=10) as response:
                if response.status == 200:
                    return True
        except HTTPError as exc:
            # The canonical /<coll>/index route can return 500 for probe URLs
            # with no captures even when the server is otherwise healthy.
            if exc.code == 500 and health_url.startswith(
                _index_endpoint_base(crawl_ids[0], resolve_cfg)
            ):
                continue
        except Exception:
            continue
    return False


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _write_index_server_config(resolve_cfg: ResolveRemoteConfig, crawl_ids: List[str]) -> Path:
    resolve_cfg.server_runtime_dir.mkdir(parents=True, exist_ok=True)
    resolve_cfg.server_collections_dir.mkdir(parents=True, exist_ok=True)
    config_path = resolve_cfg.server_runtime_dir / "config.yaml"
    payload = {
        "collections_root": str(resolve_cfg.server_collections_dir),
        "archive_paths": "s3://commoncrawl/",
        "enable_cdx_api": "-index",
        "cdx_api_endpoint": "-index",
        "enable_memento": True,
        "shard_index_loc": {
            "match": r".*(collections/[^/]+/)",
            "replace": r"s3://commoncrawl/cc-index/\1",
        },
        "max_blocks": 5,
        "framed_replay": False,
        "enable_coll_info": True,
    }
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False))
    return config_path


def _read_log_tail(path: Path, max_lines: int = 40) -> str:
    if not path.exists():
        return ""
    lines = path.read_text(errors="ignore").splitlines()
    return "\n".join(lines[-max_lines:])


def start_index_server_remote(config: Dict) -> Path:
    _require_stage(config, "collection")
    runid = _utc_runid()
    label = str(config.get("run_context", {}).get("label", "collection")).strip() or "collection"
    display_label = label
    resolve_cfg = _resolve_remote_config(config)
    logger = _setup_logger(Path("reports/logs"), f"cc-{label}-start-index-server", runid)

    crawl_ids = _installed_crawl_ids(resolve_cfg)
    if not crawl_ids:
        raise FileNotFoundError(
            "No local Common Crawl secondary indexes found. Run "
            f"`cc-{label}-install-indexes` first."
        )

    config_path = _write_index_server_config(resolve_cfg, crawl_ids)
    logger.info("Wrote local index server config %s", config_path)

    if _local_index_server_healthcheck(resolve_cfg, crawl_ids):
        existing_pid = ""
        if resolve_cfg.server_pid_path.exists():
            existing_pid = resolve_cfg.server_pid_path.read_text().strip()
        logger.info(
            "%s local index server already responding on %s:%d%s",
            display_label,
            resolve_cfg.server_host,
            resolve_cfg.server_port,
            f" (pid={existing_pid})" if existing_pid else "",
        )
        return config_path

    if resolve_cfg.server_pid_path.exists():
        try:
            pid = int(resolve_cfg.server_pid_path.read_text().strip())
        except ValueError:
            pid = 0
        if pid and _pid_is_alive(pid) and _local_index_server_healthcheck(resolve_cfg, crawl_ids):
            logger.info(
                "%s local index server already running on %s:%d (pid=%d)",
                display_label,
                resolve_cfg.server_host,
                resolve_cfg.server_port,
                pid,
            )
            return config_path
        resolve_cfg.server_pid_path.unlink(missing_ok=True)

    command = _server_command() + ["-p", str(resolve_cfg.server_port)]
    resolve_cfg.server_log_path.parent.mkdir(parents=True, exist_ok=True)
    resolve_cfg.server_log_path.unlink(missing_ok=True)
    with resolve_cfg.server_log_path.open("ab") as log_handle:
        proc = subprocess.Popen(
            command,
            cwd=resolve_cfg.server_runtime_dir,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    resolve_cfg.server_pid_path.write_text(str(proc.pid))

    start_deadline = time.time() + resolve_cfg.startup_timeout_s
    while time.time() < start_deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                "Local index server exited during startup.\n"
                + _read_log_tail(resolve_cfg.server_log_path)
            )
        if _local_index_server_healthcheck(resolve_cfg, crawl_ids):
            logger.info(
                "%s local index server ready on %s:%d (pid=%d)",
                display_label,
                resolve_cfg.server_host,
                resolve_cfg.server_port,
                proc.pid,
            )
            logger.info("Local index server log: %s", resolve_cfg.server_log_path)
            return config_path
        time.sleep(2)

    try:
        proc.terminate()
    except OSError:
        pass
    raise TimeoutError(
        "Timed out waiting for the local index server to become ready.\n"
        + _read_log_tail(resolve_cfg.server_log_path)
    )


def stop_index_server_remote(config: Dict) -> Optional[int]:
    _require_stage(config, "collection")
    resolve_cfg = _resolve_remote_config(config)
    if not resolve_cfg.server_pid_path.exists():
        return None

    try:
        pid = int(resolve_cfg.server_pid_path.read_text().strip())
    except ValueError:
        resolve_cfg.server_pid_path.unlink(missing_ok=True)
        return None

    if pid and _pid_is_alive(pid):
        os.kill(pid, 15)
        time.sleep(2)
    resolve_cfg.server_pid_path.unlink(missing_ok=True)
    return pid


def _cdxj_manifest_url(crawl_id: str, resolve_cfg: ResolveRemoteConfig) -> str:
    return f"{resolve_cfg.index_manifest_base_url}/crawl-data/{crawl_id}/cc-index.paths.gz"


def _cdxj_shard_urls_for_crawl(crawl_id: str, resolve_cfg: ResolveRemoteConfig) -> List[str]:
    manifest_url = _cdxj_manifest_url(crawl_id, resolve_cfg)
    raw_bytes = _http_get_bytes(manifest_url, timeout=resolve_cfg.request_timeout_s)
    with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes)) as gz:
        manifest_text = gz.read().decode("utf-8", errors="ignore")

    shard_urls: List[str] = []
    for raw_line in manifest_text.splitlines():
        line = raw_line.strip()
        if not line or not line.endswith(".gz"):
            continue
        if line.startswith("http://") or line.startswith("https://"):
            shard_urls.append(line)
        else:
            shard_urls.append(f"{resolve_cfg.index_file_base_url}/{line.lstrip('/')}")
    if not shard_urls:
        raise FileNotFoundError(
            f"No CDXJ shard paths found for {crawl_id} in manifest {manifest_url}"
        )
    return shard_urls


def _parse_cdxj_line(raw_line: bytes) -> Optional[dict]:
    line = raw_line.decode("utf-8", errors="ignore").strip()
    if not line:
        return None
    if line.startswith("{"):
        json_payload = line
    else:
        parts = line.split(" ", 2)
        if len(parts) < 3:
            return None
        json_payload = parts[2]
    try:
        parsed = json.loads(json_payload)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _record_from_cdxj_json(crawl_id: str, capture: dict) -> Optional[LookupRecord]:
    filename = str(capture.get("filename", "") or "").strip()
    offset = capture.get("offset", "")
    length = capture.get("length", "")
    url = str(capture.get("url", "") or "").strip()
    if not filename or not url:
        return None
    try:
        offset_int = int(str(offset))
        length_int = int(str(length))
    except (TypeError, ValueError):
        return None
    return LookupRecord(
        url=url,
        crawl_id=crawl_id,
        filename=filename,
        offset=offset_int,
        length=length_int,
        status=str(capture.get("status", "") or ""),
        mime=str(capture.get("mime", "") or capture.get("mime-detected", "") or ""),
        timestamp=str(capture.get("timestamp", "") or ""),
    )


def _candidate_is_better(candidate: LookupRecord, current: LookupRecord) -> bool:
    candidate_rank = (
        1 if candidate.status == "200" else 0,
        1 if _mime_is_html(candidate.mime) or candidate.mime == "" else 0,
        candidate.timestamp,
    )
    current_rank = (
        1 if current.status == "200" else 0,
        1 if _mime_is_html(current.mime) or current.mime == "" else 0,
        current.timestamp,
    )
    return candidate_rank > current_rank


def _scan_cdxj_shard(
    shard_url: str,
    crawl_id: str,
    target_urls: set[str],
    timeout_s: int,
) -> Tuple[Dict[str, LookupRecord], Counter, str]:
    shard_best: Dict[str, LookupRecord] = {}
    shard_counts: Counter = Counter()
    raw_bytes = _http_get_bytes(shard_url, timeout=timeout_s)
    with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes)) as gz:
        for raw_line in gz:
            capture = _parse_cdxj_line(raw_line)
            if not capture:
                continue
            url = str(capture.get("url", "") or "").strip()
            if url not in target_urls:
                continue
            record = _record_from_cdxj_json(crawl_id, capture)
            if record is None:
                continue
            shard_counts[url] += 1
            current = shard_best.get(url)
            if current is None or _candidate_is_better(record, current):
                shard_best[url] = record
    return shard_best, shard_counts, shard_url


def _resolve_crawl_via_raw_cdxj(
    crawl_id: str,
    crawl_df: pd.DataFrame,
    resolve_cfg: ResolveRemoteConfig,
    logger,
) -> Tuple[pd.DataFrame, Dict[str, int], List[str]]:
    shard_urls = _cdxj_shard_urls_for_crawl(crawl_id, resolve_cfg)
    target_urls = {str(url) for url in crawl_df["url"].tolist()}
    best_by_url: Dict[str, LookupRecord] = {}
    count_by_url: Counter = Counter()
    failed_shards: List[str] = []
    processed_shards = 0

    with ThreadPoolExecutor(max_workers=resolve_cfg.max_workers) as executor:
        futures = {
            executor.submit(
                _scan_cdxj_shard,
                shard_url,
                crawl_id,
                target_urls,
                resolve_cfg.request_timeout_s,
            ): shard_url
            for shard_url in shard_urls
        }
        for future in as_completed(futures):
            shard_url = futures[future]
            processed_shards += 1
            try:
                shard_best, shard_counts, _ = future.result()
            except Exception as exc:  # pragma: no cover - runtime/network dependent
                failed_shards.append(f"{shard_url} | {type(exc).__name__}: {exc}")
                if len(failed_shards) <= 5:
                    logger.warning(
                        "CDXJ shard scan failed for %s shard %s: %s",
                        crawl_id,
                        shard_url,
                        exc,
                    )
                continue

            count_by_url.update(shard_counts)
            for url, candidate in shard_best.items():
                current = best_by_url.get(url)
                if current is None or _candidate_is_better(candidate, current):
                    best_by_url[url] = candidate

            if (
                processed_shards % resolve_cfg.progress_log_every_shards == 0
                or processed_shards == len(shard_urls)
            ):
                logger.info(
                    "Raw CDXJ resolve %s progress "
                    "shards=%d/%d matched_docs=%d/%d failed_shards=%d",
                    crawl_id,
                    processed_shards,
                    len(shard_urls),
                    len(best_by_url),
                    len(crawl_df),
                    len(failed_shards),
                )

    records: List[Dict[str, object]] = []
    for _, row in crawl_df.sort_values("url").iterrows():
        url = str(row["url"])
        matched = best_by_url.get(url)
        output_row = row.to_dict()
        output_row.update(
            {
                "warc_filename": matched.filename if matched else "",
                "warc_record_offset": matched.offset if matched else pd.NA,
                "warc_record_length": matched.length if matched else pd.NA,
                "fetch_time": matched.timestamp if matched else "",
                "fetch_status": matched.status if matched else "",
                "content_mime_type": matched.mime if matched else "",
                "lookup_match_count": int(count_by_url.get(url, 0)),
                "lookup_note": "raw_cdxj_no_match",
                "lookup_provider": "raw_cdxj",
            }
        )
        if matched is not None:
            outcome = _lookup_outcome_from_candidates(
                [matched],
                "ok",
                "ok_non_200_or_non_html",
                attempts=1,
                last_error="",
                retry_delay_total_s=0.0,
                provider="raw_cdxj",
            )
            output_row["lookup_note"] = outcome.note
        records.append(output_row)

    metrics = {
        "doc_count": int(len(crawl_df)),
        "shard_count": int(len(shard_urls)),
        "failed_shard_count": int(len(failed_shards)),
        "matched_doc_count": int(sum(1 for url in crawl_df["url"] if url in best_by_url)),
        "unresolved_doc_count": int(sum(1 for url in crawl_df["url"] if url not in best_by_url)),
    }
    return pd.DataFrame(records), metrics, failed_shards[:5]


def _local_index_server_query(
    crawl_id: str, url: str, resolve_cfg: ResolveRemoteConfig
) -> Tuple[Optional[LookupRecord], int, str, str]:
    query_string = urlencode(
        {
            "url": url,
            "output": "json",
            "limit": resolve_cfg.query_limit,
        }
    )

    endpoint_errors: List[str] = []
    payload = ""
    content_type = ""
    for endpoint_base in (
        _index_endpoint_base(crawl_id, resolve_cfg),
        _index_endpoint_fallback(crawl_id, resolve_cfg),
    ):
        endpoint = endpoint_base + "?" + query_string
        try:
            with urlopen(Request(endpoint), timeout=resolve_cfg.request_timeout_s) as response:
                payload = response.read().decode("utf-8", errors="ignore")
                content_type = response.headers.get("Content-Type", "")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            endpoint_errors.append(f"{endpoint_base}: {type(exc).__name__}: {exc}")
            continue

        if "json" not in content_type.lower():
            endpoint_errors.append(
                f"{endpoint_base}: non-json response {content_type or '<missing>'}"
            )
            continue

        stripped = payload.strip()
        if stripped == '{"modes": ["list_sources", "index", "resource"]}':
            endpoint_errors.append(f"{endpoint_base}: returned modes listing")
            continue

        break
    else:
        joined = " | ".join(endpoint_errors)[:500]
        return None, 0, "local_index_server_endpoint_error", joined

    candidates: List[LookupRecord] = []
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            capture = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(capture, dict):
            continue
        if str(capture.get("url", "")).strip() != url:
            continue
        record = _record_from_cdxj_json(crawl_id, capture)
        if record is not None:
            candidates.append(record)

    if not candidates:
        return None, 0, "local_index_server_no_match", ""

    outcome = _lookup_outcome_from_candidates(
        candidates,
        "ok",
        "ok_non_200_or_non_html",
        attempts=1,
        last_error="",
        retry_delay_total_s=0.0,
        provider="local_index_server",
    )
    return outcome.record, outcome.match_count, outcome.note, outcome.last_error


def _resolve_crawl_via_local_index_server(
    crawl_id: str,
    crawl_df: pd.DataFrame,
    resolve_cfg: ResolveRemoteConfig,
    logger,
) -> Tuple[pd.DataFrame, Dict[str, int], List[str]]:
    rows: List[Dict[str, object]] = []
    failure_examples: List[str] = []
    resolved_count = 0
    failed_query_count = 0
    completed = 0
    total = len(crawl_df)

    with ThreadPoolExecutor(max_workers=resolve_cfg.url_workers) as executor:
        futures = {
            executor.submit(
                _local_index_server_query,
                crawl_id,
                str(row["url"]),
                resolve_cfg,
            ): row.to_dict()
            for _, row in crawl_df.sort_values("url").iterrows()
        }

        for future in as_completed(futures):
            row = futures[future]
            completed += 1
            url = str(row["url"])
            record, match_count, note, last_error = future.result()
            output_row = dict(row)
            output_row.update(
                {
                    "warc_filename": record.filename if record else "",
                    "warc_record_offset": record.offset if record else pd.NA,
                    "warc_record_length": record.length if record else pd.NA,
                    "fetch_time": record.timestamp if record else "",
                    "fetch_status": record.status if record else "",
                    "content_mime_type": record.mime if record else "",
                    "lookup_match_count": int(match_count),
                    "lookup_note": note,
                    "lookup_provider": "local_index_server",
                }
            )
            rows.append(output_row)

            if record is not None:
                resolved_count += 1
            elif note.startswith("local_index_server_error:") or note.startswith(
                "local_index_server_endpoint_error"
            ):
                failed_query_count += 1
                if len(failure_examples) < 5:
                    failure_examples.append(f"{url} | {note} | {last_error}")

            if completed % resolve_cfg.progress_log_every_urls == 0 or completed == total:
                logger.info(
                    "Local index resolve %s progress "
                    "urls=%d/%d matched_docs=%d failed_queries=%d",
                    crawl_id,
                    completed,
                    total,
                    resolved_count,
                    failed_query_count,
                )

    metrics = {
        "doc_count": total,
        "matched_doc_count": resolved_count,
        "unresolved_doc_count": total - resolved_count,
        "failed_query_count": failed_query_count,
        "url_worker_count": resolve_cfg.url_workers,
    }
    return pd.DataFrame(rows), metrics, failure_examples


def _upload_outputs_to_s3(local_paths: List[Path], s3_output_prefix: str) -> List[str]:
    boto3 = _load_boto3()
    s3_client = boto3.client("s3")
    bucket, key_prefix = _parse_s3_uri(s3_output_prefix)
    key_prefix = key_prefix.rstrip("/")
    uploaded_uris: List[str] = []
    for local_path in local_paths:
        key = f"{key_prefix}/{local_path.name}" if key_prefix else local_path.name
        s3_client.upload_file(str(local_path), bucket, key)
        uploaded_uris.append(f"s3://{bucket}/{key}")
    return uploaded_uris


def resolve_urls_remote(
    config: Dict,
    url_export_uri: Optional[str] = None,
    s3_output_prefix: Optional[str] = None,
) -> Path:
    _require_stage(config, "collection")
    resolve_start = time.perf_counter()
    label = str(config.get("run_context", {}).get("label", "collection")).strip() or "collection"
    display_label = label
    runid, export_df, export_source = _load_export_df(
        url_export_dir=url_export_output_dir(config),
        url_prefix=f"cc_{label}_urls_",
        upload_manifest_prefix=f"cc_{label}_url_upload_manifest",
        url_export_uri=url_export_uri,
    )
    if export_df.empty:
        raise ValueError(f"{display_label} URL export is empty.")

    resolve_cfg = _resolve_remote_config(config)
    logger = _setup_logger(Path("reports/logs"), f"cc-{label}-resolve", runid)

    export_df = export_df.copy()
    export_df["crawl_id"] = export_df["crawl_id"].astype(str)
    export_df["url"] = export_df["url"].astype(str)
    export_df = export_df.drop_duplicates(subset=DOC_KEY_COLUMNS, keep="first").reset_index(
        drop=True
    )
    crawl_ids = sorted(export_df["crawl_id"].unique().tolist())

    logger.info("Loaded %s URL export rows=%d", display_label, len(export_df))
    logger.info("URL export source: %s", export_source)
    logger.info(
        "Remote resolve config: provider=%s host=%s port=%d url_workers=%d timeout_s=%d",
        resolve_cfg.provider,
        resolve_cfg.server_host,
        resolve_cfg.server_port,
        resolve_cfg.url_workers,
        resolve_cfg.request_timeout_s,
    )

    if resolve_cfg.provider == "local_index_server":
        if not _local_index_server_healthcheck(resolve_cfg, crawl_ids):
            raise RuntimeError(
                f"Local {display_label} index server is not reachable. Run "
                f"`cc-{label}-install-indexes` and "
                f"`cc-{label}-start-index-server` on the EC2 instance first."
            )

    crawl_frames: List[pd.DataFrame] = []
    summary_rows: List[List[object]] = [
        ["doc_count_total", int(len(export_df)), f"Unique {display_label} URL candidates."],
        [
            "crawl_count",
            int(export_df["crawl_id"].nunique()),
            "Number of crawl partitions represented in the resolver input.",
        ],
        [
            "lookup_provider",
            resolve_cfg.provider,
            f"Active {display_label} pointer-resolution backend.",
        ],
    ]
    failure_examples: Dict[str, List[str]] = {}

    for crawl_id, crawl_df in export_df.groupby("crawl_id", sort=True):
        logger.info("Resolving WARC pointers for %s (%d docs)", crawl_id, len(crawl_df))
        if resolve_cfg.provider == "local_index_server":
            resolved_df, crawl_metrics, failures = _resolve_crawl_via_local_index_server(
                crawl_id=crawl_id,
                crawl_df=crawl_df.reset_index(drop=True),
                resolve_cfg=resolve_cfg,
                logger=logger,
            )
            summary_rows.extend(
                [
                    [
                        f"crawl.{crawl_id}.doc_count",
                        crawl_metrics["doc_count"],
                        f"Unique URLs sent to the local {display_label} index server.",
                    ],
                    [
                        f"crawl.{crawl_id}.url_worker_count",
                        crawl_metrics["url_worker_count"],
                        "URL-level worker threads used for this crawl.",
                    ],
                    [
                        f"crawl.{crawl_id}.matched_doc_count",
                        crawl_metrics["matched_doc_count"],
                        "URLs resolved to a WARC pointer for this crawl.",
                    ],
                    [
                        f"crawl.{crawl_id}.unresolved_doc_count",
                        crawl_metrics["unresolved_doc_count"],
                        "URLs with no matching WARC pointer for this crawl.",
                    ],
                    [
                        f"crawl.{crawl_id}.failed_query_count",
                        crawl_metrics["failed_query_count"],
                        "Local index-server queries that failed for this crawl.",
                    ],
                ]
            )
        elif resolve_cfg.provider == "raw_cdxj":
            resolved_df, crawl_metrics, failures = _resolve_crawl_via_raw_cdxj(
                crawl_id=crawl_id,
                crawl_df=crawl_df.reset_index(drop=True),
                resolve_cfg=resolve_cfg,
                logger=logger,
            )
            summary_rows.extend(
                [
                    [
                        f"crawl.{crawl_id}.doc_count",
                        crawl_metrics["doc_count"],
                        "Unique URLs sent to the raw CDXJ resolver for this crawl.",
                    ],
                    [
                        f"crawl.{crawl_id}.shard_count",
                        crawl_metrics["shard_count"],
                        "Number of CDXJ shards enumerated for this crawl.",
                    ],
                    [
                        f"crawl.{crawl_id}.failed_shard_count",
                        crawl_metrics["failed_shard_count"],
                        "Raw CDXJ shard downloads or scans that failed for this crawl.",
                    ],
                    [
                        f"crawl.{crawl_id}.matched_doc_count",
                        crawl_metrics["matched_doc_count"],
                        "URLs resolved to a WARC pointer for this crawl.",
                    ],
                    [
                        f"crawl.{crawl_id}.unresolved_doc_count",
                        crawl_metrics["unresolved_doc_count"],
                        "URLs with no matching raw CDXJ pointer for this crawl.",
                    ],
                ]
            )
        else:
            raise ValueError(
                f"Unsupported collection.resolve_remote.provider value: {resolve_cfg.provider}"
            )

        crawl_frames.append(resolved_df)
        failure_examples[crawl_id] = failures

    pointer_df = (
        pd.concat(crawl_frames, ignore_index=True)
        .sort_values(DOC_KEY_COLUMNS)
        .reset_index(drop=True)
    )
    resolved_count = int(pointer_df["warc_filename"].fillna("").astype(str).ne("").sum())
    total_elapsed_sec = time.perf_counter() - resolve_start

    summary_rows.extend(
        [
            [
                "pointer_cache_rows",
                int(len(pointer_df)),
                f"Rows in the {display_label} pointer cache.",
            ],
            [
                "pointer_cache_resolved_rows",
                resolved_count,
                f"Rows with non-empty WARC pointer fields after {display_label} resolution.",
            ],
            [
                "pointer_cache_unresolved_rows",
                int(len(pointer_df) - resolved_count),
                "Rows without a matching WARC pointer.",
            ],
            [
                "total_elapsed_sec",
                round(total_elapsed_sec, 6),
                f"Total wall-clock time for {display_label} pointer resolution in seconds.",
            ],
            [
                "urls_per_sec",
                round(len(pointer_df) / total_elapsed_sec, 6) if total_elapsed_sec > 0 else 0.0,
                f"{display_label} pointer-resolution throughput in URLs per second.",
            ],
        ]
    )

    out_dir = pointer_cache_output_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = out_dir / f"cc_pointer_cache_{runid}.parquet"
    summary_path = out_dir / f"cc_pointer_cache_summary_{runid}.csv"
    manifest_path = out_dir / f"cc_{label}_resolve_manifest_{runid}.json"

    pointer_df.to_parquet(parquet_path, index=False)
    _write_summary_csv(summary_path, summary_rows)

    manifest_payload: Dict[str, object] = {
        "runid": runid,
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "url_export_source": export_source,
        "pointer_cache_path": str(parquet_path),
        "summary_path": str(summary_path),
        "failure_examples": failure_examples,
        "resolve_config": {
            "provider": resolve_cfg.provider,
            "server_host": resolve_cfg.server_host,
            "server_port": resolve_cfg.server_port,
            "url_workers": resolve_cfg.url_workers,
            "request_timeout_s": resolve_cfg.request_timeout_s,
        },
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True))

    if s3_output_prefix:
        uploaded_uris = _upload_outputs_to_s3(
            [parquet_path, summary_path, manifest_path],
            s3_output_prefix,
        )
        logger.info("Uploaded %s pointer cache outputs to %s", display_label, s3_output_prefix)
        for uri in uploaded_uris:
            logger.info("Uploaded %s", uri)

    logger.info("Wrote %s pointer cache %s", display_label, parquet_path)
    logger.info(
        "%s remote resolve complete: pointer_cache_rows=%d resolved_rows=%d",
        display_label,
        len(pointer_df),
        resolved_count,
    )
    return parquet_path
