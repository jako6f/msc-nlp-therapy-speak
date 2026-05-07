import argparse
import sys
from pathlib import Path

import yaml

from src.analysis.cc_validate import validate_corpus_outputs
from src.data_sources.commoncrawl import (
    download_from_manifest,
    filter_en_dedup_hits,
    export_stage1d_urls,
    extract_stage1d_pointer_cache,
    find_latest_manifest,
    install_stage1d_indexes_remote,
    resolve_stage1d_urls_remote,
    sample_and_write_manifest,
    scan_wet_files,
    start_stage1d_index_server_remote,
    upload_stage1d_urls_to_s3,
    validate_counts,
)
from src.pathing import (
    stage1_base,
    stage1d_filter_en_dedup_dir,
    stage1d_pointer_cache_dir,
    stage1d_url_export_dir,
    stage1d_warc_dir,
)


def load_config(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def _ensure_stage1_dirs(cfg: dict) -> None:
    base = stage1_base(cfg)
    for stage in ("stage1a", "stage1b", "stage1c", "stage1d"):
        (base / stage).mkdir(parents=True, exist_ok=True)
    if cfg.get("run_context", {}).get("stage") == "stage1d":
        stage1d_url_export_dir(cfg).mkdir(parents=True, exist_ok=True)
        stage1d_pointer_cache_dir(cfg).mkdir(parents=True, exist_ok=True)
        stage1d_warc_dir(cfg).mkdir(parents=True, exist_ok=True)
        stage1d_filter_en_dedup_dir(cfg).mkdir(parents=True, exist_ok=True)


def _add_config_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the stage-scoped YAML config for this run.",
    )


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command", required=True)

    p_sample = sub.add_parser("cc-sample", help="Sample WET paths per crawl")
    _add_config_arg(p_sample)

    p_download = sub.add_parser("cc-download", help="Download sampled WET files")
    _add_config_arg(p_download)
    p_download.add_argument(
        "--manifest",
        help="Path to manifest JSONL (defaults to latest in data/manifests)",
        default=None,
    )

    p_scan = sub.add_parser("cc-scan", help="Scan downloaded WET files")
    _add_config_arg(p_scan)

    p_validate = sub.add_parser("cc-validate", help="Validate latest scan outputs")
    _add_config_arg(p_validate)

    p_filter_en_dedup = sub.add_parser(
        "cc-filter-en-dedup",
        help="Run English-only gating plus deduplication for WARC-validated hits",
    )
    _add_config_arg(p_filter_en_dedup)

    p_export_urls = sub.add_parser(
        "cc-stage1d-export-urls",
        help=(
            "Export unique Stage 1d WARC URL candidates for remote "
            "Common Crawl pointer resolution"
        ),
    )
    _add_config_arg(p_export_urls)

    p_upload_urls = sub.add_parser(
        "cc-stage1d-upload-urls",
        help="Upload the latest Stage 1d URL export to S3 for the remote resolver",
    )
    _add_config_arg(p_upload_urls)

    p_install_indexes = sub.add_parser(
        "cc-stage1d-install-indexes-remote",
        help="Install Common Crawl secondary indexes locally on the remote EC2 resolver host",
    )
    _add_config_arg(p_install_indexes)
    p_install_indexes.add_argument(
        "--url-export-uri",
        default=None,
        help="Optional local path or s3:// URI for the Stage 1d URL export CSV/parquet/manifest.",
    )

    p_start_index_server = sub.add_parser(
        "cc-stage1d-start-index-server",
        help="Start the local pywb/Common Crawl index server on the remote EC2 resolver host",
    )
    _add_config_arg(p_start_index_server)

    p_resolve_remote = sub.add_parser(
        "cc-stage1d-resolve-remote",
        help=(
            "Resolve Stage 1d WARC pointers by querying the local Common Crawl "
            "index server on a remote EC2 instance"
        ),
    )
    _add_config_arg(p_resolve_remote)
    p_resolve_remote.add_argument(
        "--url-export-uri",
        default=None,
        help="Optional local path or s3:// URI for the Stage 1d URL export CSV/parquet/manifest.",
    )
    p_resolve_remote.add_argument(
        "--s3-output-prefix",
        default=None,
        help="Optional s3:// prefix to upload the pointer cache outputs to after the run finishes.",
    )

    p_extract_remote = sub.add_parser(
        "cc-stage1d-extract-remote",
        help="Run Stage 1d WARC extraction from a resolver-produced pointer cache",
    )
    _add_config_arg(p_extract_remote)
    p_extract_remote.add_argument(
        "--pointer-cache-uri",
        default=None,
        help="Optional local parquet path, local directory, or s3:// URI for the pointer cache.",
    )
    p_extract_remote.add_argument(
        "--s3-output-prefix",
        default=None,
        help="Optional s3:// prefix to upload the extraction outputs to after the run finishes.",
    )

    args = p.parse_args()
    cfg_path = Path(args.config)
    cfg = load_config(cfg_path)
    _ensure_stage1_dirs(cfg)

    if args.command == "cc-sample":
        sample_and_write_manifest(cfg, cfg_path)
        return

    if args.command == "cc-download":
        manifest_path = Path(args.manifest) if args.manifest else None
        if manifest_path is None:
            manifest_path = find_latest_manifest(Path("data/manifests"))
        if manifest_path is None:
            print("No manifest found in data/manifests. Run cc-sample first.")
            sys.exit(1)
        downloaded = download_from_manifest(manifest_path)
        validate_counts(manifest_path, downloaded)
        return

    if args.command == "cc-scan":
        scan_wet_files(cfg, cfg_path)
        return

    if args.command == "cc-validate":
        validate_corpus_outputs(cfg)
        return

    if args.command == "cc-filter-en-dedup":
        filter_en_dedup_hits(cfg)
        return

    if args.command == "cc-stage1d-export-urls":
        export_stage1d_urls(cfg)
        return

    if args.command == "cc-stage1d-upload-urls":
        upload_stage1d_urls_to_s3(cfg)
        return

    if args.command == "cc-stage1d-install-indexes-remote":
        install_stage1d_indexes_remote(cfg, url_export_uri=args.url_export_uri)
        return

    if args.command == "cc-stage1d-start-index-server":
        start_stage1d_index_server_remote(cfg)
        return

    if args.command == "cc-stage1d-resolve-remote":
        resolve_stage1d_urls_remote(
            cfg,
            url_export_uri=args.url_export_uri,
            s3_output_prefix=args.s3_output_prefix,
        )
        return

    if args.command == "cc-stage1d-extract-remote":
        extract_stage1d_pointer_cache(
            cfg,
            pointer_cache_uri=args.pointer_cache_uri,
            s3_output_prefix=args.s3_output_prefix,
        )
        return


if __name__ == "__main__":
    main()
