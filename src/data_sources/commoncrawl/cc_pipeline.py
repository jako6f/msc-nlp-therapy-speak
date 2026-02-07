import gzip
import hashlib
import json
import logging
from pathlib import Path
import random
import time
from typing import Dict, Iterable, List, Optional
from urllib.request import urlopen

COMMONCRAWL_BASE_URL = "https://data.commoncrawl.org"


def _utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _stable_seed(base_seed: int, crawl_id: str) -> int:
    digest = hashlib.sha256(crawl_id.encode("utf-8")).hexdigest()[:8]
    crawl_seed = int(digest, 16)
    return base_seed ^ crawl_seed


def _setup_logger(log_dir: Path, label: str) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    log_path = log_dir / f"{label}_{timestamp}.log"

    logger = logging.getLogger(label)
    logger.setLevel(logging.INFO)
    logger.handlers = []

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger.info("Logging to %s", log_path)
    return logger


def _iter_wet_paths(crawl_id: str, logger: logging.Logger) -> Iterable[str]:
    url = f"{COMMONCRAWL_BASE_URL}/crawl-data/{crawl_id}/wet.paths.gz"
    logger.info("Streaming %s", url)
    with urlopen(url) as response:
        with gzip.GzipFile(fileobj=response) as gz:
            for line in gz:
                path = line.decode("utf-8").strip()
                if path:
                    yield path


def sample_wet_paths(
    crawl_id: str, k: int, base_seed: int, logger: logging.Logger
) -> List[str]:
    seed = _stable_seed(base_seed, crawl_id)
    rng = random.Random(seed)
    reservoir: List[str] = []
    count = 0

    for path in _iter_wet_paths(crawl_id, logger):
        if count < k:
            reservoir.append(path)
        else:
            j = rng.randint(0, count)
            if j < k:
                reservoir[j] = path
        count += 1

    logger.info("%s: sampled %d of %d paths", crawl_id, len(reservoir), count)
    return reservoir


def write_manifest(
    manifest_path: Path,
    entries: List[Dict[str, str]],
    logger: logging.Logger,
) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
    logger.info("Wrote manifest %s", manifest_path)


def read_manifest(manifest_path: Path) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    with manifest_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entries.append(json.loads(line))
    return entries


def find_latest_manifest(manifest_dir: Path) -> Optional[Path]:
    if not manifest_dir.exists():
        return None
    manifests = sorted(manifest_dir.glob("cc_sample_*.jsonl"))
    if not manifests:
        return None
    return manifests[-1]


def sample_and_write_manifest(config: Dict, config_path: Path) -> Path:
    log_dir = Path("reports/logs")
    logger = _setup_logger(log_dir, "cc-sample")

    crawl_ids = config["pilot"]["crawl_ids"]
    k = int(config["sampling"]["wet_files_per_crawl"])
    base_seed = int(config["project"]["seed"])

    logger.info("Loaded config %s", config_path)
    logger.info("Crawl IDs: %s", crawl_ids)
    logger.info("Per-crawl sample size: %d", k)
    logger.info("Seed: %d", base_seed)

    timestamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    manifest_path = Path("data/manifests") / f"cc_sample_{timestamp}.jsonl"
    manifest_entries: List[Dict[str, str]] = []
    run_ts = _utc_timestamp()

    for crawl_id in crawl_ids:
        logger.info("Sampling crawl %s", crawl_id)
        sample = sample_wet_paths(crawl_id, k, base_seed, logger)
        seed = _stable_seed(base_seed, crawl_id)
        for path in sample:
            source_url = f"{COMMONCRAWL_BASE_URL}/{path}"
            manifest_entries.append(
                {
                    "crawl_id": crawl_id,
                    "sampled_wet_path": path,
                    "source_url": source_url,
                    "seed": str(seed),
                    "timestamp": run_ts,
                }
            )

    write_manifest(manifest_path, manifest_entries, logger)
    print(
        f"Sampled {len(manifest_entries)} WET files across {len(crawl_ids)} crawls."
    )
    return manifest_path


def download_wet_files(
    entries: List[Dict[str, str]],
    output_dir: Path,
    logger: logging.Logger,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    counts: Dict[str, int] = {}
    downloaded = 0

    for entry in entries:
        crawl_id = entry["crawl_id"]
        source_url = entry["source_url"]
        counts.setdefault(crawl_id, 0)
        counts[crawl_id] += 1
        idx = counts[crawl_id]
        filename = f"{crawl_id}_{idx:03d}.wet.gz"
        dest = output_dir / filename

        logger.info("Downloading %s -> %s", source_url, dest)
        with urlopen(source_url) as response:
            with dest.open("wb") as f:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
        downloaded += 1

    logger.info("Downloaded %d files", downloaded)
    return downloaded


def download_from_manifest(manifest_path: Path) -> int:
    log_dir = Path("reports/logs")
    logger = _setup_logger(log_dir, "cc-download")

    logger.info("Loading manifest %s", manifest_path)
    entries = read_manifest(manifest_path)
    output_dir = Path("data/raw/wet")

    downloaded = download_wet_files(entries, output_dir, logger)
    print(f"Downloaded {downloaded} WET files.")
    return downloaded


def validate_counts(manifest_path: Path, downloaded: int) -> None:
    entries = read_manifest(manifest_path)
    print(
        f"Validation: sampled {len(entries)} files; downloaded {downloaded} files."
    )
