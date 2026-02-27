import argparse
import sys
from pathlib import Path

import yaml

from src.analysis.cc_validate import validate_corpus_outputs
from src.analysis.pilot_exports import export_tables_and_figures
from src.data_sources.commoncrawl import (
    download_from_manifest,
    find_latest_manifest,
    sample_and_write_manifest,
    scan_wet_files,
    validate_counts,
)
from src.pathing import stage1_base, stage1_output_dir


def load_config(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def _ensure_stage1_dirs(cfg: dict) -> None:
    base = stage1_base(cfg)
    for stage in ("stage1a", "stage1b", "stage1c"):
        (base / stage).mkdir(parents=True, exist_ok=True)


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command", required=True)

    p_sample = sub.add_parser("cc-sample", help="Sample WET paths per crawl")
    p_sample.add_argument("--config", default="configs/pilot.yaml")

    p_download = sub.add_parser("cc-download", help="Download sampled WET files")
    p_download.add_argument("--config", default="configs/pilot.yaml")
    p_download.add_argument(
        "--manifest",
        help="Path to manifest JSONL (defaults to latest in data/manifests)",
        default=None,
    )

    p_scan = sub.add_parser("cc-scan", help="Scan downloaded WET files")
    p_scan.add_argument("--config", default="configs/pilot.yaml")

    p_export = sub.add_parser("cc-export", help="Export pilot scan tables/figures")
    p_export.add_argument("--config", default="configs/pilot.yaml")

    p_validate = sub.add_parser("cc-validate", help="Validate latest pilot outputs")
    p_validate.add_argument("--config", default="configs/pilot.yaml")

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

    if args.command == "cc-export":
        interim_dir = stage1_output_dir(cfg, default_stage="stage1b")
        export_tables_and_figures(cfg, interim_dir)
        return

    if args.command == "cc-validate":
        validate_corpus_outputs(cfg)
        return


if __name__ == "__main__":
    main()
