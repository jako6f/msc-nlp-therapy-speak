import csv
import gzip
import io
import json
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd
from warcio.archiveiterator import ArchiveIterator

from src.pathing import (
    stage1_output_dir,
    stage1_stage_dir,
    stage1d_pointer_cache_dir,
    stage1d_warc_dir,
)

COMMONCRAWL_DATA_BASE_URL = "https://data.commoncrawl.org"
DOC_KEY_COLUMNS = ["crawl_id", "url"]


@dataclass(frozen=True)
class StageSource:
    stage_name: str
    scope_name: str
    runid: str
    output_dir: Path
    hits_path: Path
    summary_path: Path
    term_summary_path: Path
    hits_df: pd.DataFrame
    summary: Dict[str, object]


@dataclass(frozen=True)
class LookupRecord:
    url: str
    crawl_id: str
    filename: str
    offset: int
    length: int
    status: str
    mime: str
    timestamp: str


@dataclass(frozen=True)
class LookupOutcome:
    record: Optional[LookupRecord]
    note: str
    attempts: int
    last_error: str
    retry_delay_total_s: float
    match_count: int
    provider: str


def _utc_runid() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.gmtime())


def _setup_logger(log_dir: Path, label: str, runid: str) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{label}_{runid}.log"

    logger = logging.getLogger(f"{label}-{runid}")
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


def _latest_by_runid(directory: Path, prefix: str, suffix: str) -> Tuple[str, Path]:
    latest_runid = ""
    latest_path: Optional[Path] = None
    for path in directory.glob(f"{prefix}*{suffix}"):
        runid = _extract_runid(path, prefix, suffix)
        if runid > latest_runid:
            latest_runid = runid
            latest_path = path
    if latest_path is None:
        raise FileNotFoundError(f"No {prefix}*{suffix} found in {directory}")
    return latest_runid, latest_path


def _latest_validated_hits_wet(directory: Path) -> Tuple[str, Path]:
    try:
        return _latest_by_runid(directory, "cc_validated_hits_wet_", ".parquet")
    except FileNotFoundError:
        return _latest_by_runid(directory, "cc_pilot_corpus_", ".parquet")


def _latest_enriched_hits(directory: Path) -> Tuple[str, Path]:
    return _latest_by_runid(directory, "cc_enriched_hits_", ".parquet")


def _metric_int(summary: Dict[str, object], key: str, default: int = 0) -> int:
    value = summary.get(key, default)
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _rate_per(numer: int, denom: int, scale: int) -> float:
    if denom <= 0:
        return 0.0
    return (numer / denom) * scale


def _summary_row(metric: str, value: object, description: str) -> List[object]:
    return [metric, value, description]


def _load_metric_map(path: Path) -> Dict[str, object]:
    df = pd.read_csv(path)
    if "metric" not in df.columns or "value" not in df.columns:
        raise ValueError(f"Missing expected columns in {path}")
    return dict(zip(df["metric"], df["value"]))


def _load_stage_source(config: Dict, stage_name: str, scope_name: str) -> StageSource:
    if stage_name == "stage1d":
        output_dir = stage1_output_dir(config, default_stage="stage1d")
    else:
        output_dir = stage1_stage_dir(config, stage_name)
    runid, hits_path = _latest_validated_hits_wet(output_dir)
    summary_path = output_dir / f"cc_scan_summary_{runid}.csv"
    term_summary_path = output_dir / f"cc_term_summary_{runid}.csv"
    if not summary_path.exists():
        raise FileNotFoundError(f"Missing scan summary for {stage_name}: {summary_path}")
    if not term_summary_path.exists():
        raise FileNotFoundError(
            f"Missing term summary for {stage_name}: {term_summary_path}"
        )
    hits_df = pd.read_parquet(hits_path)
    hits_df = hits_df.copy()
    hits_df["source_stage"] = stage_name
    hits_df["source_scope"] = scope_name
    hits_df["source_runid"] = runid
    return StageSource(
        stage_name=stage_name,
        scope_name=scope_name,
        runid=runid,
        output_dir=output_dir,
        hits_path=hits_path,
        summary_path=summary_path,
        term_summary_path=term_summary_path,
        hits_df=hits_df,
        summary=_load_metric_map(summary_path),
    )


def _combined_stage1d_inputs(config: Dict) -> Tuple[pd.DataFrame, List[StageSource]]:
    anchor_stage = str(config.get("stage1d", {}).get("anchor_stage", "stage1c"))
    sources = [
        _load_stage_source(config, anchor_stage, "anchor"),
        _load_stage_source(config, "stage1d", "holdout"),
    ]
    combined_df = pd.concat([source.hits_df for source in sources], ignore_index=True)
    return combined_df, sources


def _sample_enrich_inputs(
    combined_hits_df: pd.DataFrame, config: Dict
) -> Tuple[pd.DataFrame, Dict[str, int | bool]]:
    sample_cfg = config.get("stage1d", {}).get("warc_validation", {}).get("sample", {})
    max_docs_per_crawl = int(sample_cfg.get("max_docs_per_crawl", 0) or 0)
    if max_docs_per_crawl <= 0 or combined_hits_df.empty:
        return combined_hits_df, {"doc_sample_enabled": False}

    base_seed = int(sample_cfg.get("seed", config.get("project", {}).get("seed", 123)))
    unique_docs = combined_hits_df[DOC_KEY_COLUMNS].drop_duplicates().reset_index(drop=True)
    sampled_groups: List[pd.DataFrame] = []
    sampled_doc_total = 0

    for crawl_idx, (_, crawl_docs) in enumerate(
        unique_docs.groupby("crawl_id", sort=True), start=1
    ):
        sample_n = min(max_docs_per_crawl, len(crawl_docs))
        sampled = crawl_docs.sample(
            n=sample_n,
            random_state=base_seed + crawl_idx,
            replace=False,
        )
        sampled_groups.append(sampled)
        sampled_doc_total += sample_n

    sampled_keys = pd.concat(sampled_groups, ignore_index=True)
    sampled_hits_df = combined_hits_df.merge(sampled_keys, on=DOC_KEY_COLUMNS, how="inner")
    metadata: Dict[str, int | bool] = {
        "doc_sample_enabled": True,
        "doc_sample_max_docs_per_crawl": max_docs_per_crawl,
        "doc_sampled_input_docs": sampled_doc_total,
        "doc_sampled_input_hits": int(len(sampled_hits_df)),
    }
    return sampled_hits_df, metadata


def _require_stage1d(config: Dict) -> None:
    stage = str(config.get("run_context", {}).get("stage", "")).strip()
    if stage != "stage1d":
        raise ValueError("Stage 1d commands require run_context.stage=stage1d")
def _fetch_warc_payload(record: LookupRecord) -> Tuple[Optional[dict], str]:
    end_offset = record.offset + record.length - 1
    warc_url = f"{COMMONCRAWL_DATA_BASE_URL}/{record.filename}"
    request = Request(
        warc_url,
        headers={
            "Range": f"bytes={record.offset}-{end_offset}",
            "User-Agent": "msc-nlp-therapy-speak/0.2",
        },
    )

    try:
        with urlopen(request, timeout=120) as response:
            payload = response.read()
    except HTTPError as exc:
        return None, f"warc_http_error:{exc.code}"
    except URLError as exc:
        return None, f"warc_url_error:{exc.reason}"
    except Exception as exc:  # pragma: no cover - network/runtime dependent
        return None, f"warc_error:{type(exc).__name__}"

    try:
        with gzip.GzipFile(fileobj=io.BytesIO(payload)) as gz:
            for warc_record in ArchiveIterator(gz):
                content = warc_record.content_stream().read()
                return (
                    {
                        "warc_type": warc_record.rec_type,
                        "capture_ts": warc_record.rec_headers.get_header("WARC-Date") or "",
                        "warc_target_uri": warc_record.rec_headers.get_header(
                            "WARC-Target-URI"
                        )
                        or "",
                        "http_status": (
                            warc_record.http_headers.get_statuscode()
                            if warc_record.http_headers
                            else ""
                        ),
                        "html_bytes": len(content),
                        "html_text": content.decode("utf-8", errors="ignore"),
                    },
                    "ok",
                )
    except Exception as exc:  # pragma: no cover - parsing/runtime dependent
        return None, f"warc_parse_error:{type(exc).__name__}"

    return None, "warc_empty_record"


def _load_boto3():
    try:
        import boto3  # type: ignore
    except ImportError as exc:  # pragma: no cover - dependency dependent
        raise RuntimeError(
            "Missing dependency 'boto3'. Install the Stage 1d AWS dependencies "
            "before using the Stage 1d remote resolver/extraction workflow."
        ) from exc
    return boto3


def _latest_pointer_cache_path(config: Dict) -> Tuple[str, Path]:
    out_dir = stage1d_pointer_cache_dir(config)
    latest_runid = ""
    latest_path: Optional[Path] = None
    for path in out_dir.glob("cc_pointer_cache_*.parquet"):
        runid = path.stem.removeprefix("cc_pointer_cache_")
        if runid > latest_runid:
            latest_runid = runid
            latest_path = path
    if latest_path is None:
        raise FileNotFoundError(
            f"No cc_pointer_cache_*.parquet found in {out_dir}. Run the remote resolver step first "
            "or pass --pointer-cache-uri explicitly."
        )
    return latest_runid, latest_path


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {uri}")
    without_scheme = uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def _load_pointer_cache_s3(uri: str) -> pd.DataFrame:
    boto3 = _load_boto3()
    s3_client = boto3.client("s3")
    bucket, key = _parse_s3_uri(uri)

    parquet_keys: List[str] = []
    if key.endswith(".parquet"):
        parquet_keys = [key]
    else:
        paginator = s3_client.get_paginator("list_objects_v2")
        prefix = key.rstrip("/") + "/"
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                candidate_key = str(obj["Key"])
                if candidate_key.endswith(".parquet"):
                    parquet_keys.append(candidate_key)
    if not parquet_keys:
        raise FileNotFoundError(f"No parquet objects found under {uri}")

    frames = []
    for parquet_key in sorted(parquet_keys):
        buffer = io.BytesIO()
        s3_client.download_fileobj(bucket, parquet_key, buffer)
        buffer.seek(0)
        frames.append(pd.read_parquet(buffer))
    return pd.concat(frames, ignore_index=True)


def _load_pointer_cache_df(config: Dict, pointer_cache_uri: Optional[str]) -> pd.DataFrame:
    if pointer_cache_uri:
        if pointer_cache_uri.startswith("s3://"):
            return _load_pointer_cache_s3(pointer_cache_uri)
        pointer_path = Path(pointer_cache_uri)
        if pointer_path.is_dir():
            parquet_paths = sorted(pointer_path.glob("*.parquet"))
            if not parquet_paths:
                raise FileNotFoundError(f"No parquet files found in {pointer_path}")
            return pd.concat([pd.read_parquet(path) for path in parquet_paths], ignore_index=True)
        return pd.read_parquet(pointer_path)
    _, latest_path = _latest_pointer_cache_path(config)
    return pd.read_parquet(latest_path)


def _normalize_timestamp(raw_value: object) -> Optional[str]:
    if raw_value is None:
        return None
    raw_text = str(raw_value).strip()
    if not raw_text:
        return None
    ts = pd.to_datetime(raw_text, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.isoformat().replace("+00:00", "Z")


def _url_date_candidate(url: str) -> Optional[str]:
    patterns = [
        r"(20\d{2})/(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])",
        r"(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            year, month, day = match.groups()
            return f"{year}-{month}-{day}"
    return None


def _iter_jsonld_dates(node: object) -> Iterable[str]:
    target_keys = {"datepublished", "datecreated", "uploaddate"}
    if isinstance(node, dict):
        for key, value in node.items():
            if key.lower() in target_keys and isinstance(value, (str, int, float)):
                yield str(value)
            else:
                yield from _iter_jsonld_dates(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_jsonld_dates(item)


def _candidate_before_capture(
    candidate_ts: Optional[str],
    capture_ts: Optional[str],
    tolerance_hours: int,
) -> bool:
    if not candidate_ts:
        return False
    if not capture_ts:
        return True
    candidate_dt = pd.to_datetime(candidate_ts, utc=True, errors="coerce")
    capture_dt = pd.to_datetime(capture_ts, utc=True, errors="coerce")
    if pd.isna(candidate_dt) or pd.isna(capture_dt):
        return True
    return candidate_dt <= capture_dt + pd.Timedelta(hours=tolerance_hours)


def _extract_published_timestamp(
    html_text: str,
    url: str,
    capture_ts: Optional[str],
    tolerance_hours: int,
) -> Tuple[Optional[str], str, str]:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        url_candidate = _url_date_candidate(url)
        normalized = _normalize_timestamp(url_candidate)
        if _candidate_before_capture(normalized, capture_ts, tolerance_hours):
            return normalized, "url_date", "low"
        return None, "", "none"

    soup = BeautifulSoup(html_text, "html.parser")

    for script_tag in soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)}):
        raw_json = script_tag.string or script_tag.get_text(strip=True)
        if not raw_json:
            continue
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            continue
        for raw_candidate in _iter_jsonld_dates(parsed):
            normalized = _normalize_timestamp(raw_candidate)
            if _candidate_before_capture(normalized, capture_ts, tolerance_hours):
                return normalized, "json_ld", "high"

    meta_selectors = [
        ("property", "article:published_time"),
        ("property", "og:published_time"),
        ("name", "pubdate"),
        ("name", "publishdate"),
        ("name", "timestamp"),
        ("name", "date"),
        ("itemprop", "datePublished"),
        ("itemprop", "dateCreated"),
    ]
    for attr_name, attr_value in meta_selectors:
        tag = soup.find(
            "meta", attrs={attr_name: re.compile(f"^{re.escape(attr_value)}$", re.I)}
        )
        if not tag:
            continue
        normalized = _normalize_timestamp(tag.get("content"))
        if _candidate_before_capture(normalized, capture_ts, tolerance_hours):
            return normalized, "meta", "high"

    for time_tag in soup.find_all("time"):
        normalized = _normalize_timestamp(
            time_tag.get("datetime") or time_tag.get_text(" ", strip=True)
        )
        if _candidate_before_capture(normalized, capture_ts, tolerance_hours):
            return normalized, "time_tag", "medium"

    url_candidate = _url_date_candidate(url)
    normalized = _normalize_timestamp(url_candidate)
    if _candidate_before_capture(normalized, capture_ts, tolerance_hours):
        return normalized, "url_date", "low"

    return None, "", "none"


def _trafilatura_extract(html_text: str, favor_recall: bool) -> Tuple[Optional[str], str]:
    try:
        import trafilatura
    except ImportError as exc:  # pragma: no cover - dependency dependent
        raise RuntimeError(
            "Missing dependency 'trafilatura'. Install it before running Stage 1d WARC extraction."
        ) from exc

    extracted = trafilatura.extract(
        html_text,
        favor_recall=favor_recall,
        include_comments=False,
        include_tables=True,
    )
    if not extracted:
        return None, "trafilatura_empty"
    normalized = " ".join(extracted.split())
    if not normalized:
        return None, "trafilatura_whitespace_only"
    return normalized, "ok"


def _resiliparse_extract(html_text: str) -> Tuple[Optional[str], str]:
    try:
        from resiliparse.extract.html2text import extract_plain_text  # type: ignore
        from resiliparse.parse.html import HTMLTree  # type: ignore
    except ImportError:
        return None, "resiliparse_missing"

    try:
        tree = HTMLTree.parse(html_text)
        extracted = extract_plain_text(tree, main_content=True, preserve_formatting=False)
    except Exception as exc:  # pragma: no cover - dependency/runtime dependent
        return None, f"resiliparse_error:{type(exc).__name__}"
    normalized = " ".join(str(extracted or "").split())
    if not normalized:
        return None, "resiliparse_empty"
    return normalized, "ok"


def _extract_main_content(
    html_text: str, favor_recall: bool
) -> Tuple[Optional[str], str, str]:
    extracted_text, note = _trafilatura_extract(html_text, favor_recall=favor_recall)
    if extracted_text:
        return extracted_text, note, "trafilatura"

    fallback_text, fallback_note = _resiliparse_extract(html_text)
    if fallback_text:
        return fallback_text, fallback_note, "resiliparse"

    return None, note if note else fallback_note, ""


def _build_stage1d_summary_rows(
    docs_scanned: int,
    candidate_hits: int,
    validated_hits_wet: int,
    validated_hits_warc: int,
    role_counts: Dict[str, Dict[str, int]],
    doc_metrics: Dict[str, object],
    notes: str,
) -> List[List[object]]:
    rows = [
        _summary_row(
            "docs_scanned",
            docs_scanned,
            "Combined scanned documents across the frozen "
            "Stage 1c anchors and Stage 1d hold-out slice.",
        ),
        _summary_row(
            "candidate_hits",
            candidate_hits,
            "Combined candidate hits across the frozen "
            "Stage 1c anchors and Stage 1d hold-out slice.",
        ),
        _summary_row(
            "validated_hits_wet",
            validated_hits_wet,
            "Combined WET-validated hits used as input to Stage 1d WARC validation.",
        ),
        _summary_row(
            "validated_hits_warc",
            validated_hits_warc,
            "Combined row-level hits that survived WARC validation and Trafilatura extraction.",
        ),
        _summary_row(
            "validated_hits_wet_per_10k",
            round(_rate_per(validated_hits_wet, docs_scanned, 10_000), 6),
            "Combined WET-validated hit rate per 10,000 scanned documents.",
        ),
        _summary_row(
            "validated_hits_warc_per_10k",
            round(_rate_per(validated_hits_warc, docs_scanned, 10_000), 6),
            "Combined WARC-validated hit rate per 10,000 scanned documents.",
        ),
        _summary_row(
            "warc_validation_attempted",
            True,
            "Indicates that Stage 1d WARC validation ran for this summary.",
        ),
        _summary_row(
            "warc_validation_notes",
            notes,
            "Short note describing the Stage 1d WARC validation scope.",
        ),
    ]
    for role in ("target", "baseline"):
        role_values = role_counts.get(role, {})
        rows.extend(
            [
                _summary_row(
                    f"term_role.{role}.validated_hits_wet",
                    role_values.get("validated_hits_wet", 0),
                    f"Input WET-validated hits attributed to term_role={role}.",
                ),
                _summary_row(
                    f"term_role.{role}.validated_hits_warc",
                    role_values.get("validated_hits_warc", 0),
                    f"WARC-validated hits attributed to term_role={role}.",
                ),
            ]
        )
    for metric_name, metric_value in sorted(doc_metrics.items()):
        rows.append(
            _summary_row(
                metric_name,
                metric_value,
                "Stage 1d document-level lookup, fetch, extraction, or filtering metric.",
            )
        )
    return rows


def _write_summary_csv(path: Path, rows: List[List[object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value", "description"])
        writer.writerows(rows)


def _write_stage1d_term_summary(
    path: Path,
    df: pd.DataFrame,
    include_filter_fields: bool = False,
) -> None:
    if df.empty:
        columns = [
            "crawl_id",
            "term_role",
            "term_group",
            "matched_term",
            "validated_hits_wet",
            "validated_hits_warc",
        ]
        if include_filter_fields:
            columns.extend(["english_hits", "dedup_representative_hits"])
        pd.DataFrame(columns=columns).to_csv(path, index=False)
        return

    base = (
        df.groupby(["crawl_id", "term_role", "term_group", "matched_term"], dropna=False)
        .agg(
            validated_hits_wet=("matched_term", "size"),
            validated_hits_warc=("is_validated_hits_warc", "sum"),
        )
        .reset_index()
    )
    if include_filter_fields:
        extras = (
            df.groupby(
                ["crawl_id", "term_role", "term_group", "matched_term"], dropna=False
            )
            .agg(
                english_hits=("is_english", "sum"),
                dedup_representative_hits=("is_dedup_representative", "sum"),
            )
            .reset_index()
        )
        base = base.merge(
            extras,
            on=["crawl_id", "term_role", "term_group", "matched_term"],
            how="left",
    )
    base.to_csv(path, index=False)


def _upload_stage1d_outputs_to_s3(
    local_paths: List[Path], s3_output_prefix: str
) -> List[str]:
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


def extract_stage1d_pointer_cache(
    config: Dict,
    pointer_cache_uri: Optional[str] = None,
    s3_output_prefix: Optional[str] = None,
) -> Path:
    _require_stage1d(config)
    runid = _utc_runid()
    logger = _setup_logger(Path("reports/logs"), "cc-stage1d-extract-remote", runid)

    combined_hits_df, sources = _combined_stage1d_inputs(config)
    if combined_hits_df.empty:
        raise ValueError("No validated WET hits available for Stage 1d extraction.")
    combined_hits_df, sample_metadata = _sample_enrich_inputs(combined_hits_df, config)

    warc_cfg = config.get("stage1d", {}).get("warc_validation", {})
    favor_recall = bool(warc_cfg.get("favor_recall", False))
    tolerance_hours = int(config.get("stage1d", {}).get("published_ts_tolerance_hours", 48))

    pointer_df = _load_pointer_cache_df(config, pointer_cache_uri)
    if pointer_df.empty:
        raise ValueError("Pointer cache is empty.")
    pointer_df = pointer_df.copy()
    pointer_df["crawl_id"] = pointer_df["crawl_id"].astype(str)
    pointer_df["url"] = pointer_df["url"].astype(str)
    pointer_df = (
        pointer_df.sort_values(DOC_KEY_COLUMNS)
        .drop_duplicates(subset=DOC_KEY_COLUMNS, keep="first")
        .reset_index(drop=True)
    )

    logger.info("Loaded %d Stage 1d input hits", len(combined_hits_df))
    logger.info(
        "Source runids: %s",
        {source.scope_name: source.runid for source in sources},
    )
    logger.info("Loaded pointer cache rows=%d", len(pointer_df))
    if pointer_cache_uri:
        logger.info("Pointer cache source: %s", pointer_cache_uri)
    if bool(sample_metadata.get("doc_sample_enabled", False)):
        logger.info(
            "Sampling Stage 1d extraction input: max_docs_per_crawl=%d "
            "sampled_docs=%d sampled_hits=%d",
            int(sample_metadata.get("doc_sample_max_docs_per_crawl", 0)),
            int(sample_metadata.get("doc_sampled_input_docs", 0)),
            int(sample_metadata.get("doc_sampled_input_hits", 0)),
        )

    pointer_map = {
        (str(row["crawl_id"]), str(row["url"])): row
        for row in pointer_df.to_dict(orient="records")
    }
    unique_docs = combined_hits_df[DOC_KEY_COLUMNS].drop_duplicates().reset_index(drop=True)

    doc_records: List[Dict[str, object]] = []
    lookup_success_docs = 0
    fetch_success_docs = 0
    extract_success_docs = 0

    for doc_idx, (_, doc_row) in enumerate(unique_docs.iterrows(), start=1):
        crawl_id = str(doc_row["crawl_id"])
        url = str(doc_row["url"])
        pointer_row = pointer_map.get((crawl_id, url))
        lookup_note = "pointer_cache_no_match"
        lookup_match_count = 0
        record: Optional[LookupRecord] = None
        pointer_fetch_status = ""
        pointer_content_mime_type = ""
        if pointer_row is not None:
            lookup_note = str(pointer_row.get("lookup_note", "ok") or "ok")
            lookup_match_count = int(pointer_row.get("lookup_match_count", 0) or 0)
            pointer_fetch_status = str(pointer_row.get("fetch_status", "") or "")
            pointer_content_mime_type = str(pointer_row.get("content_mime_type", "") or "")
            filename = str(pointer_row.get("warc_filename", "") or "").strip()
            offset = pointer_row.get("warc_record_offset", pd.NA)
            length = pointer_row.get("warc_record_length", pd.NA)
            if filename and pd.notna(offset) and pd.notna(length):
                record = LookupRecord(
                    url=url,
                    crawl_id=crawl_id,
                    filename=filename,
                    offset=int(offset),
                    length=int(length),
                    status=pointer_fetch_status,
                    mime=pointer_content_mime_type,
                    timestamp=str(pointer_row.get("fetch_time", "") or ""),
                )

        row: Dict[str, object] = {
            "crawl_id": crawl_id,
            "url": url,
            "lookup_provider": "pointer_cache",
            "lookup_note": lookup_note,
            "lookup_match_count": lookup_match_count,
            "warc_lookup_success": record is not None,
            "warc_fetch_success": False,
            "trafilatura_success": False,
            "is_validated_hits_warc": False,
            "warc_filename": record.filename if record else "",
            "warc_offset": record.offset if record else pd.NA,
            "warc_length": record.length if record else pd.NA,
            "capture_ts": "",
            "warc_target_uri": "",
            "http_status": "",
            "html_bytes": 0,
            "extracted_text": "",
            "extracted_text_len": 0,
            "published_ts": "",
            "published_ts_source": "",
            "published_ts_confidence": "none",
            "cdx_attempts": 0,
            "cdx_last_error": "",
            "cdx_retry_delay_total_s": 0.0,
            "cdx_cooldown_triggered": False,
            "warc_validation_notes": lookup_note,
            "pointer_fetch_status": pointer_fetch_status,
            "pointer_content_mime_type": pointer_content_mime_type,
            "extractor_used": "",
        }
        if record is None:
            doc_records.append(row)
            continue

        lookup_success_docs += 1
        fetched_payload, fetch_note = _fetch_warc_payload(record)
        row["warc_validation_notes"] = fetch_note
        if fetched_payload is None:
            doc_records.append(row)
            continue

        fetch_success_docs += 1
        row["warc_fetch_success"] = True
        row["capture_ts"] = fetched_payload.get("capture_ts", "")
        row["warc_target_uri"] = fetched_payload.get("warc_target_uri", "")
        row["http_status"] = fetched_payload.get("http_status", "")
        row["html_bytes"] = int(fetched_payload.get("html_bytes", 0) or 0)

        html_text = str(fetched_payload.get("html_text", ""))
        extracted_text, extract_note, extractor_used = _extract_main_content(
            html_text, favor_recall=favor_recall
        )
        row["warc_validation_notes"] = extract_note
        row["extractor_used"] = extractor_used
        if extracted_text:
            term_rows = combined_hits_df.loc[
                (combined_hits_df["crawl_id"].astype(str) == crawl_id)
                & (combined_hits_df["url"].astype(str) == url)
            ]
            candidate_terms = {
                str(term).strip().lower()
                for term in term_rows["matched_term"].dropna().tolist()
                if str(term).strip()
            }
            extracted_lower = extracted_text.lower()
            if candidate_terms and not any(term in extracted_lower for term in candidate_terms):
                row["warc_validation_notes"] = "term_missing_after_extraction"
            else:
                extract_success_docs += 1
                row["trafilatura_success"] = extractor_used == "trafilatura"
                row["is_validated_hits_warc"] = True
                row["extracted_text"] = extracted_text
                row["extracted_text_len"] = len(extracted_text)
        published_ts, published_source, published_confidence = _extract_published_timestamp(
            html_text=html_text,
            url=url,
            capture_ts=str(row["capture_ts"]) or None,
            tolerance_hours=tolerance_hours,
        )
        row["published_ts"] = published_ts or ""
        row["published_ts_source"] = published_source
        row["published_ts_confidence"] = published_confidence
        doc_records.append(row)

        if doc_idx % 25 == 0:
            logger.info(
                "Stage 1d remote extraction progress %d/%d docs | lookup_success=%d "
                "fetch_success=%d extract_success=%d",
                doc_idx,
                len(unique_docs),
                lookup_success_docs,
                fetch_success_docs,
                extract_success_docs,
            )

    doc_df = pd.DataFrame(doc_records)
    enriched_df = combined_hits_df.merge(doc_df, on=DOC_KEY_COLUMNS, how="left")
    enriched_df["is_validated_hits_warc"] = (
        enriched_df["is_validated_hits_warc"].fillna(False).astype(bool)
    )
    enriched_df["trafilatura_success"] = (
        enriched_df["trafilatura_success"].fillna(False).astype(bool)
    )
    enriched_df["warc_lookup_success"] = (
        enriched_df["warc_lookup_success"].fillna(False).astype(bool)
    )
    enriched_df["warc_fetch_success"] = (
        enriched_df["warc_fetch_success"].fillna(False).astype(bool)
    )
    enriched_df["lookup_provider"] = enriched_df["lookup_provider"].fillna("pointer_cache")
    enriched_df["lookup_note"] = enriched_df["lookup_note"].fillna("pointer_cache_no_match")
    enriched_df["lookup_match_count"] = (
        pd.to_numeric(enriched_df["lookup_match_count"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    for column in (
        "capture_ts",
        "warc_validation_notes",
        "published_ts",
        "published_ts_source",
        "published_ts_confidence",
        "pointer_fetch_status",
        "pointer_content_mime_type",
        "extractor_used",
        "extracted_text",
    ):
        default_value = "none" if column == "published_ts_confidence" else ""
        enriched_df[column] = enriched_df[column].fillna(default_value)
    enriched_df["extracted_text_len"] = (
        pd.to_numeric(enriched_df["extracted_text_len"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    validated_warc_df = enriched_df.loc[
        enriched_df["is_validated_hits_warc"].fillna(False)
    ].copy()

    docs_scanned_total = sum(_metric_int(source.summary, "docs_scanned", 0) for source in sources)
    candidate_hits_total = sum(
        _metric_int(source.summary, "candidate_hits", 0) for source in sources
    )
    validated_hits_wet_total = len(enriched_df)
    validated_hits_warc_total = len(validated_warc_df)
    role_counts: Dict[str, Dict[str, int]] = {}
    for role in ("target", "baseline"):
        role_df = enriched_df.loc[enriched_df["term_role"] == role]
        role_counts[role] = {
            "validated_hits_wet": int(len(role_df)),
            "validated_hits_warc": int(role_df["is_validated_hits_warc"].sum()),
        }

    doc_metrics = {
        "doc_count_input": int(len(unique_docs)),
        "doc_count_lookup_success": int(lookup_success_docs),
        "doc_count_fetch_success": int(fetch_success_docs),
        "doc_count_extract_success": int(extract_success_docs),
        "doc_count_lookup_failed": int(len(unique_docs) - lookup_success_docs),
        "doc_count_fetch_failed": int(lookup_success_docs - fetch_success_docs),
        "doc_count_extract_failed": int(fetch_success_docs - extract_success_docs),
        "lookup_provider": "pointer_cache",
    }
    if bool(sample_metadata.get("doc_sample_enabled", False)):
        doc_metrics.update(
            {
                "doc_sample_enabled": True,
                "doc_sample_max_docs_per_crawl": int(
                    sample_metadata.get("doc_sample_max_docs_per_crawl", 0)
                ),
                "doc_sampled_input_docs": int(
                    sample_metadata.get("doc_sampled_input_docs", 0)
                ),
                "doc_sampled_input_hits": int(
                    sample_metadata.get("doc_sampled_input_hits", 0)
                ),
            }
        )

    notes = (
        "Stage 1d WARC validation over frozen Stage 1c anchors plus Stage 1d hold-out "
        "using a remote resolver-produced pointer cache."
    )
    if pointer_cache_uri:
        notes += f" pointer_cache_source={pointer_cache_uri}."

    out_dir = stage1d_warc_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    enriched_path = out_dir / f"cc_enriched_hits_{runid}.parquet"
    validated_warc_path = out_dir / f"cc_validated_hits_warc_{runid}.parquet"
    summary_path = out_dir / f"cc_stage1d_summary_{runid}.csv"
    term_summary_path = out_dir / f"cc_stage1d_term_summary_{runid}.csv"
    manifest_path = out_dir / f"cc_stage1d_remote_extract_manifest_{runid}.json"

    enriched_df.to_parquet(enriched_path, index=False)
    validated_warc_df.to_parquet(validated_warc_path, index=False)
    _write_stage1d_term_summary(term_summary_path, enriched_df, include_filter_fields=False)
    _write_summary_csv(
        summary_path,
        _build_stage1d_summary_rows(
            docs_scanned=docs_scanned_total,
            candidate_hits=candidate_hits_total,
            validated_hits_wet=validated_hits_wet_total,
            validated_hits_warc=validated_hits_warc_total,
            role_counts=role_counts,
            doc_metrics=doc_metrics,
            notes=notes,
        ),
    )

    manifest_payload = {
        "runid": runid,
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "pointer_cache_uri": pointer_cache_uri or str(_latest_pointer_cache_path(config)[1]),
        "s3_output_prefix": s3_output_prefix or "",
        "enriched_path": str(enriched_path),
        "validated_warc_path": str(validated_warc_path),
        "summary_path": str(summary_path),
        "term_summary_path": str(term_summary_path),
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True))

    if s3_output_prefix:
        uploaded_uris = _upload_stage1d_outputs_to_s3(
            [
                enriched_path,
                validated_warc_path,
                summary_path,
                term_summary_path,
                manifest_path,
            ],
            s3_output_prefix,
        )
        logger.info("Uploaded Stage 1d remote extraction outputs to %s", s3_output_prefix)
        for uri in uploaded_uris:
            logger.info("Uploaded %s", uri)

    logger.info("Wrote enriched hits %s", enriched_path)
    logger.info("Wrote WARC-validated hits %s", validated_warc_path)
    logger.info("Wrote Stage 1d summary %s", summary_path)
    return enriched_path
