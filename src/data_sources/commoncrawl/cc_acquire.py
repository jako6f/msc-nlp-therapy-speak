import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Dict, List

COMMONCRAWL_BASE_URL = "https://data.commoncrawl.org"


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


def read_manifest(manifest_path: Path) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    with manifest_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries
