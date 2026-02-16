import hashlib
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

REQUIRED_COLUMNS = [
    "matched_term",
    "registered_domain",
    "url",
    "context_snippet",
    "crawl_id",
    "source_wet",
]


def _setup_logger(log_dir: Path, runid: str) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"cc-validate_{runid}.log"

    logger = logging.getLogger(f"cc-validate-{runid}")
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


def _extract_runid(path: Path, prefix: str, suffix: str) -> str:
    name = path.name
    if not name.startswith(prefix) or not name.endswith(suffix):
        raise ValueError(f"Unexpected file name format: {name}")
    return name[len(prefix) : -len(suffix)]


def _latest_by_runid(interim_dir: Path, prefix: str, suffix: str) -> Tuple[str, Path]:
    latest_runid = ""
    latest_path: Optional[Path] = None

    for path in interim_dir.glob(f"{prefix}*{suffix}"):
        runid = _extract_runid(path, prefix, suffix)
        if runid > latest_runid:
            latest_runid = runid
            latest_path = path

    if latest_path is None:
        raise FileNotFoundError(f"No {prefix}*{suffix} found in {interim_dir}")

    return latest_runid, latest_path


def _seed_for_run(project_seed: int, runid: str) -> int:
    digest = hashlib.sha256(f"{project_seed}:{runid}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def _validate_columns(df: pd.DataFrame, path: Path) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in {path}: {missing}")


def validate_corpus_outputs(config: Dict) -> Tuple[Path, Path]:
    interim_dir = Path(config.get("project", {}).get("out_dir", "data/interim"))
    project_seed = int(config.get("project", {}).get("seed", 0))

    corpus_runid, corpus_path = _latest_by_runid(
        interim_dir, "cc_pilot_corpus_", ".parquet"
    )
    domains_runid, top_domains_path = _latest_by_runid(
        interim_dir, "cc_scan_top_domains_", ".csv"
    )

    logger = _setup_logger(Path("reports/logs"), corpus_runid)
    logger.info("Using corpus: %s", corpus_path)
    logger.info("Using top domains: %s", top_domains_path)
    if domains_runid != corpus_runid:
        logger.warning(
            "Run IDs differ: corpus=%s, top_domains=%s; continuing with latest files",
            corpus_runid,
            domains_runid,
        )

    corpus_df = pd.read_parquet(corpus_path)
    _validate_columns(corpus_df, corpus_path)

    top_domains_df = pd.read_csv(top_domains_path)
    if (
        "registered_domain" not in top_domains_df.columns
        or "hits" not in top_domains_df.columns
    ):
        raise ValueError(
            f"Missing expected columns in {top_domains_path}: ['registered_domain', 'hits']"
        )

    sample_seed = _seed_for_run(project_seed, corpus_runid)
    sample_n = min(25, len(corpus_df))
    sample_df = corpus_df[REQUIRED_COLUMNS].sample(n=sample_n, random_state=sample_seed)

    asd_mask = corpus_df["matched_term"].fillna("").str.lower().str.contains("asd")
    asd_df = corpus_df.loc[asd_mask, REQUIRED_COLUMNS]

    sample_path = interim_dir / f"cc_val_sample25_{corpus_runid}.csv"
    asd_path = interim_dir / f"cc_val_asd_{corpus_runid}.csv"
    sample_df.to_csv(sample_path, index=False)
    asd_df.to_csv(asd_path, index=False)

    hits_total = int(len(corpus_df))
    hits_by_term = corpus_df["matched_term"].value_counts().to_dict()
    unique_domains_hits = int(
        corpus_df["registered_domain"].fillna("").replace("", pd.NA).dropna().nunique()
    )

    print(f"hits_total: {hits_total}")
    print(f"hits_by_term: {hits_by_term}")
    print(f"unique_domains_hits: {unique_domains_hits}")
    print("top_domains:")

    for _, row in top_domains_df.head(10).iterrows():
        domain = str(row["registered_domain"])
        count = int(row["hits"])
        share = (count / hits_total) if hits_total else 0.0
        print(f"  {domain}: count={count}, share={share:.4f}")

    logger.info("Wrote sample validation CSV: %s", sample_path)
    logger.info("Wrote ASD validation CSV: %s", asd_path)
    logger.info("hits_total=%d", hits_total)
    logger.info("hits_by_term=%s", hits_by_term)
    logger.info("unique_domains_hits=%d", unique_domains_hits)

    return sample_path, asd_path
