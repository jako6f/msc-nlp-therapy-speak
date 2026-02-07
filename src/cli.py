import argparse
from pathlib import Path
import sys
import yaml

from src.data_sources.commoncrawl import (
    download_from_manifest,
    find_latest_manifest,
    sample_and_write_manifest,
    validate_counts,
)


def load_config(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


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

    args = p.parse_args()
    cfg_path = Path(args.config)
    cfg = load_config(cfg_path)

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


if __name__ == "__main__":
    main()
