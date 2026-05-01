import hashlib
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from src.pathing import stage1_output_dir
from src.analysis.summary_schema import with_warc_metric_defaults

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


def _latest_validated_hits_wet(interim_dir: Path) -> Tuple[str, Path]:
    try:
        return _latest_by_runid(interim_dir, "cc_validated_hits_wet_", ".parquet")
    except FileNotFoundError:
        return _latest_by_runid(interim_dir, "cc_pilot_corpus_", ".parquet")


def _seed_for_run(project_seed: int, runid: str) -> int:
    digest = hashlib.sha256(f"{project_seed}:{runid}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def _load_metric_map(path: Path) -> Dict[str, object]:
    df = pd.read_csv(path)
    if "metric" not in df.columns or "value" not in df.columns:
        raise ValueError(f"Missing expected columns in {path}: ['metric', 'value']")
    # Supports both legacy (metric,value) and current (metric,value,description).
    return with_warc_metric_defaults(dict(zip(df["metric"], df["value"])))


def _metric_int(summary: Dict[str, object], key: str, default: int = 0) -> int:
    value = summary.get(key, default)
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _metric_float(summary: Dict[str, object], key: str, default: float = 0.0) -> float:
    value = summary.get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _metric_optional_float(summary: Dict[str, object], key: str) -> Optional[float]:
    value = summary.get(key)
    if value is None:
        return None
    value_str = str(value).strip()
    if value_str == "" or value_str.lower() in {"na", "none", "nan", "null"}:
        return None
    try:
        return float(value_str)
    except (TypeError, ValueError):
        return None


def _metric_bool(summary: Dict[str, object], key: str, default: bool = False) -> bool:
    value = summary.get(key, default)
    if isinstance(value, bool):
        return value
    value_str = str(value).strip().lower()
    if value_str in {"1", "true", "yes", "y"}:
        return True
    if value_str in {"0", "false", "no", "n", ""}:
        return False
    return default


def _rate_per(numer: int, denom: int, scale: int) -> float:
    if denom <= 0:
        return 0.0
    return (numer / denom) * scale


def _print_slice_metrics(
    label: str,
    docs_scanned: int,
    candidate_hits: int,
    validated_hits_wet: int,
    validated_hits_warc: Optional[float],
    warc_validation_attempted: bool,
    docs_per_sec: float,
    timings: Dict[str, float],
) -> None:
    cand_10k = _rate_per(candidate_hits, docs_scanned, 10_000)
    wet_10k = _rate_per(validated_hits_wet, docs_scanned, 10_000)
    warc_10k = (
        _rate_per(int(validated_hits_warc), docs_scanned, 10_000)
        if validated_hits_warc is not None
        else None
    )
    delta_abs = candidate_hits - validated_hits_wet
    delta_pct = (delta_abs / candidate_hits * 100.0) if candidate_hits > 0 else 0.0
    warc_hits_label = (
        str(int(validated_hits_warc)) if validated_hits_warc is not None else "NA"
    )
    warc_rate_label = f"{warc_10k:.3f}" if warc_10k is not None else "NA"

    print(
        f"{label}: docs_scanned={docs_scanned}, "
        f"candidate_hits={candidate_hits}, validated_hits_wet={validated_hits_wet}, "
        f"validated_hits_warc={warc_hits_label}"
    )
    print(
        f"  rates: candidate_per_10k={cand_10k:.3f}, "
        f"validated_hits_wet_per_10k={wet_10k:.3f}, "
        f"validated_hits_warc_per_10k={warc_rate_label}"
    )
    print(f"  warc_validation_attempted={warc_validation_attempted}")
    print(
        f"  candidate_to_validated_hits_wet_delta: abs={delta_abs}, pct={delta_pct:.2f}%"
    )
    print(
        f"  throughput: docs_per_sec={docs_per_sec:.3f}, "
        f"input={float(timings.get('time_input_read_sec', 0.0)):.3f}s, "
        f"parse={float(timings.get('time_parse_sec', 0.0)):.3f}s, "
        f"match={float(timings.get('time_term_match_sec', 0.0)):.3f}s, "
        f"domain={float(timings.get('time_domain_extract_sec', 0.0)):.3f}s, "
        f"write={float(timings.get('time_write_sec', 0.0)):.3f}s, "
        f"total={float(timings.get('total_elapsed_sec', 0.0)):.3f}s"
    )


def _validate_columns(df: pd.DataFrame, path: Path) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in {path}: {missing}")


def _slice_ids(summary: Dict[str, object]) -> list[str]:
    prefix = "slice."
    slice_ids = set()
    for key in summary:
        if not key.startswith(prefix):
            continue
        remainder = key[len(prefix) :]
        if "." not in remainder:
            continue
        slice_ids.add(remainder.split(".", 1)[0])
    return sorted(slice_ids)


def _slice_metric_int(summary: Dict[str, object], slice_id: str, metric: str) -> int:
    return _metric_int(summary, f"slice.{slice_id}.{metric}", 0)


def _slice_metric_float(summary: Dict[str, object], slice_id: str, metric: str) -> float:
    return _metric_float(summary, f"slice.{slice_id}.{metric}", 0.0)


def _slice_metric_optional_float(
    summary: Dict[str, object], slice_id: str, metric: str
) -> Optional[float]:
    return _metric_optional_float(summary, f"slice.{slice_id}.{metric}")


def _slice_metric_bool(
    summary: Dict[str, object], slice_id: str, metric: str, default: bool = False
) -> bool:
    return _metric_bool(summary, f"slice.{slice_id}.{metric}", default=default)


def validate_corpus_outputs(config: Dict) -> Tuple[Path, Path]:
    interim_dir = stage1_output_dir(config, default_stage="stage1b")
    project_seed = int(config.get("project", {}).get("seed", 0))

    corpus_runid, corpus_path = _latest_validated_hits_wet(interim_dir)
    top_domains_path = interim_dir / f"cc_scan_top_domains_{corpus_runid}.csv"
    if not top_domains_path.exists():
        domains_runid, top_domains_path = _latest_by_runid(
            interim_dir, "cc_scan_top_domains_", ".csv"
        )
    else:
        domains_runid = corpus_runid
    summary_path = interim_dir / f"cc_scan_summary_{corpus_runid}.csv"
    if not summary_path.exists():
        raise FileNotFoundError(f"No matching scan summary found for runid {corpus_runid}")

    logger = _setup_logger(Path("reports/logs"), corpus_runid)
    logger.info("Using corpus: %s", corpus_path)
    logger.info("Using top domains: %s", top_domains_path)
    logger.info("Using scan summary: %s", summary_path)
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

    summary = _load_metric_map(summary_path)
    docs_scanned = _metric_int(summary, "docs_scanned", 0)
    candidate_hits = _metric_int(
        summary, "candidate_hits", _metric_int(summary, "hits_total", 0)
    )
    validated_hits_wet = _metric_int(
        summary,
        "validated_hits_wet",
        _metric_int(summary, "final_hits", _metric_int(summary, "hits_total", 0)),
    )
    validated_hits_warc = _metric_optional_float(summary, "validated_hits_warc")
    warc_validation_attempted = _metric_bool(
        summary, "warc_validation_attempted", default=False
    )
    timings_combined = {
        "time_input_read_sec": _metric_float(summary, "time_input_read_sec", 0.0),
        "time_parse_sec": _metric_float(summary, "time_parse_sec", 0.0),
        "time_term_match_sec": _metric_float(summary, "time_term_match_sec", 0.0),
        "time_domain_extract_sec": _metric_float(summary, "time_domain_extract_sec", 0.0),
        "time_write_sec": _metric_float(summary, "time_write_sec", 0.0),
        "total_elapsed_sec": _metric_float(summary, "total_elapsed_sec", 0.0),
    }

    sample_seed = _seed_for_run(project_seed, corpus_runid)
    sample_n = min(50, len(corpus_df))
    sample_df = corpus_df[REQUIRED_COLUMNS].sample(n=sample_n, random_state=sample_seed)

    asd_mask = corpus_df["matched_term"].fillna("").str.lower().str.contains("asd")
    asd_df = corpus_df.loc[asd_mask, REQUIRED_COLUMNS]

    sample_path = interim_dir / f"cc_val_sample50_{corpus_runid}.csv"
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
    print("scan_stage1b_instrumentation:")

    slice_ids = _slice_ids(summary)
    for slice_id in slice_ids:
        _print_slice_metrics(
            label=f"slice[{slice_id}]",
            docs_scanned=_slice_metric_int(summary, slice_id, "docs_scanned"),
            candidate_hits=_slice_metric_int(summary, slice_id, "candidate_hits"),
            validated_hits_wet=_slice_metric_int(
                summary, slice_id, "validated_hits_wet"
            )
            or _slice_metric_int(summary, slice_id, "final_hits"),
            validated_hits_warc=_slice_metric_optional_float(
                summary, slice_id, "validated_hits_warc"
            ),
            warc_validation_attempted=_slice_metric_bool(
                summary, slice_id, "warc_validation_attempted", default=False
            ),
            docs_per_sec=_slice_metric_float(summary, slice_id, "docs_per_sec"),
            timings={
                "total_elapsed_sec": _slice_metric_float(
                    summary, slice_id, "total_elapsed_sec"
                )
            },
        )

    _print_slice_metrics(
        label="combined",
        docs_scanned=docs_scanned,
        candidate_hits=candidate_hits,
        validated_hits_wet=validated_hits_wet,
        validated_hits_warc=validated_hits_warc,
        warc_validation_attempted=warc_validation_attempted,
        docs_per_sec=_metric_float(summary, "docs_per_sec", 0.0),
        timings=timings_combined,
    )
    print("  removal_by_filter (combined):")
    combined_removal_keys = sorted(
        [
            key
            for key in summary
            if key.startswith("removed_boilerplate_")
            and not key.startswith("slice.")
        ]
    )
    for key in combined_removal_keys:
        print(f"    {key}={_metric_int(summary, key, 0)}")
    print(f"    removed_domaincap={_metric_int(summary, 'removed_domaincap', 0)}")
    print(f"    removed_total={_metric_int(summary, 'removed_total', 0)}")
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
    logger.info(
        "validated_hits_warc=%s",
        "NA" if validated_hits_warc is None else int(validated_hits_warc),
    )
    logger.info("warc_validation_attempted=%s", warc_validation_attempted)

    return sample_path, asd_path
