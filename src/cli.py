import argparse
import os
import sys
from pathlib import Path
from typing import Any

import yaml

from src.data_sources.commoncrawl import (
    build_processed_corpus,
    build_processed_trend,
    download_collection_wet,
    export_collection_urls,
    extract_collection,
    install_collection_indexes,
    quality_collection,
    resolve_collection_urls,
    run_collection_track,
    sample_collection_wet,
    scan_collection,
    select_collection_crawls,
    start_collection_index_server,
    upload_collection_urls,
)
from src.pathing import (
    collection_interim_dir,
    processed_corpus_dir,
    processed_manifest_dir,
    processed_trend_dir,
)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, override_value in override.items():
        base_value = merged.get(key)
        if isinstance(base_value, dict) and isinstance(override_value, dict):
            merged[key] = _deep_merge(base_value, override_value)
        else:
            merged[key] = override_value
    return merged


def load_config(path: Path) -> dict:
    config = yaml.safe_load(path.read_text()) or {}
    local_config_path = Path(os.environ.get("MSC_NLP_LOCAL_CONFIG", "configs/local/aws.yaml"))
    if local_config_path.exists():
        local_config = yaml.safe_load(local_config_path.read_text()) or {}
        config = _deep_merge(config, local_config)
    return config


def _add_config_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        default="configs/commoncrawl_collection.yaml",
        help="Path to the final Common Crawl collection config.",
    )


def _add_year_track_batch_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--year", required=True, help="Collection year, e.g. 2020.")
    parser.add_argument(
        "--track",
        required=True,
        choices=["trend", "corpus"],
        help="Collection track to process.",
    )
    parser.add_argument("--batch", default="1", help="Deterministic corpus/trend batch number.")


def _ensure_collection_dirs(cfg: dict) -> None:
    collection_interim_dir(cfg).mkdir(parents=True, exist_ok=True)
    processed_trend_dir(cfg).mkdir(parents=True, exist_ok=True)
    processed_corpus_dir(cfg).mkdir(parents=True, exist_ok=True)
    processed_manifest_dir(cfg).mkdir(parents=True, exist_ok=True)
    Path("reports/logs").mkdir(parents=True, exist_ok=True)
    Path("data/manifests").mkdir(parents=True, exist_ok=True)
    Path("data/raw/wet").mkdir(parents=True, exist_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    p_select = sub.add_parser(
        "cc-collection-select-crawls",
        help="Write the frozen year-to-crawl map manifest for the final collection.",
    )
    _add_config_arg(p_select)

    p_sample = sub.add_parser(
        "cc-collection-sample-wet",
        help="Sample deterministic WET file batches for one collection year.",
    )
    _add_config_arg(p_sample)
    _add_year_track_batch_args(p_sample)

    p_download = sub.add_parser(
        "cc-collection-download-wet",
        help="Download the latest sampled WET batch for one collection year.",
    )
    _add_config_arg(p_download)
    _add_year_track_batch_args(p_download)
    p_download.add_argument("--manifest", default=None, help="Optional explicit WET manifest path.")

    p_scan = sub.add_parser(
        "cc-collection-scan",
        help="Scan downloaded WET files for one collection year/track/batch.",
    )
    _add_config_arg(p_scan)
    _add_year_track_batch_args(p_scan)

    p_export = sub.add_parser(
        "cc-collection-export-urls",
        help="Export unique WARC URL candidates for one collection year/track/batch.",
    )
    _add_config_arg(p_export)
    _add_year_track_batch_args(p_export)

    p_upload = sub.add_parser(
        "cc-collection-upload-urls",
        help="Upload the latest collection URL export for remote resolution.",
    )
    _add_config_arg(p_upload)
    _add_year_track_batch_args(p_upload)

    p_install = sub.add_parser(
        "cc-collection-install-indexes",
        help="Install Common Crawl secondary indexes on the EC2 resolver host.",
    )
    _add_config_arg(p_install)
    p_install.add_argument("--url-export-uri", default=None)

    p_start = sub.add_parser(
        "cc-collection-start-index-server",
        help="Start the local Common Crawl index server on the EC2 resolver host.",
    )
    _add_config_arg(p_start)

    p_resolve = sub.add_parser(
        "cc-collection-resolve",
        help="Resolve WARC pointers for one collection year/track/batch.",
    )
    _add_config_arg(p_resolve)
    _add_year_track_batch_args(p_resolve)
    p_resolve.add_argument("--url-export-uri", default=None)
    p_resolve.add_argument("--s3-output-prefix", default=None)

    p_extract = sub.add_parser(
        "cc-collection-extract",
        help="Fetch WARC records and extract main text for one collection year/track/batch.",
    )
    _add_config_arg(p_extract)
    _add_year_track_batch_args(p_extract)
    p_extract.add_argument("--pointer-cache-uri", default=None)
    p_extract.add_argument("--s3-output-prefix", default=None)

    p_quality = sub.add_parser(
        "cc-collection-quality",
        help="Run document quality, English filtering, dedup, and sample export.",
    )
    _add_config_arg(p_quality)
    _add_year_track_batch_args(p_quality)

    p_build_trend = sub.add_parser(
        "cc-collection-build-trend",
        help="Build processed trend outputs from collection working artifacts.",
    )
    _add_config_arg(p_build_trend)

    p_build_corpus = sub.add_parser(
        "cc-collection-build-corpus",
        help="Build processed corpus outputs from collection working artifacts.",
    )
    _add_config_arg(p_build_corpus)

    p_run_track = sub.add_parser(
        "cc-collection-run",
        help="Run acquire, scan, URL export, and URL upload for all configured years in one track.",
    )
    _add_config_arg(p_run_track)
    p_run_track.add_argument("--track", required=True, choices=["trend", "corpus"])

    args = parser.parse_args()
    cfg_path = Path(args.config)
    if not cfg_path.exists():
        print(f"Config not found: {cfg_path}", file=sys.stderr)
        sys.exit(1)
    cfg = load_config(cfg_path)
    _ensure_collection_dirs(cfg)

    if args.command == "cc-collection-select-crawls":
        select_collection_crawls(cfg)
        return
    if args.command == "cc-collection-sample-wet":
        sample_collection_wet(cfg, year=args.year, track=args.track, batch=args.batch)
        return
    if args.command == "cc-collection-download-wet":
        download_collection_wet(
            cfg,
            year=args.year,
            track=args.track,
            batch=args.batch,
            manifest=args.manifest,
        )
        return
    if args.command == "cc-collection-scan":
        scan_collection(cfg, year=args.year, track=args.track, batch=args.batch)
        return
    if args.command == "cc-collection-export-urls":
        export_collection_urls(cfg, year=args.year, track=args.track, batch=args.batch)
        return
    if args.command == "cc-collection-upload-urls":
        upload_collection_urls(cfg, year=args.year, track=args.track, batch=args.batch)
        return
    if args.command == "cc-collection-install-indexes":
        install_collection_indexes(cfg, url_export_uri=args.url_export_uri)
        return
    if args.command == "cc-collection-start-index-server":
        start_collection_index_server(cfg)
        return
    if args.command == "cc-collection-resolve":
        resolve_collection_urls(
            cfg,
            year=args.year,
            track=args.track,
            batch=args.batch,
            url_export_uri=args.url_export_uri,
            s3_output_prefix=args.s3_output_prefix,
        )
        return
    if args.command == "cc-collection-extract":
        extract_collection(
            cfg,
            year=args.year,
            track=args.track,
            batch=args.batch,
            pointer_cache_uri=args.pointer_cache_uri,
            s3_output_prefix=args.s3_output_prefix,
        )
        return
    if args.command == "cc-collection-quality":
        quality_collection(cfg, year=args.year, track=args.track, batch=args.batch)
        return
    if args.command == "cc-collection-build-trend":
        build_processed_trend(cfg)
        return
    if args.command == "cc-collection-build-corpus":
        build_processed_corpus(cfg)
        return
    if args.command == "cc-collection-run":
        run_collection_track(cfg, track=args.track)
        return


if __name__ == "__main__":
    main()
