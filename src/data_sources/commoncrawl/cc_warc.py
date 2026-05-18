import csv
import gzip
import io
import json
import logging
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd
from warcio.archiveiterator import ArchiveIterator

from src.pathing import pointer_cache_output_dir, warc_output_dir, working_output_dir

COMMONCRAWL_DATA_BASE_URL = "https://data.commoncrawl.org"
DOC_KEY_COLUMNS = ["crawl_id", "url"]
SCHEMA_NEGATIVE_TYPES = {
    "searchresultspage",
    "collectionpage",
    "itemlist",
    "offercatalog",
    "musicalbum",
    "musicrecording",
    "product",
    "jobposting",
    "qapage",
}


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


class _DocumentTimeoutError(TimeoutError):
    pass


class _DocumentTimeout:
    def __init__(self, timeout_s: int):
        self.timeout_s = max(0, int(timeout_s))
        self._previous_handler = None

    def __enter__(self):
        if self.timeout_s <= 0:
            return self
        self._previous_handler = signal.getsignal(signal.SIGALRM)
        signal.signal(signal.SIGALRM, self._raise_timeout)
        signal.setitimer(signal.ITIMER_REAL, self.timeout_s)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.timeout_s > 0:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, self._previous_handler)
        return False

    def _raise_timeout(self, signum, frame):  # noqa: ARG002
        raise _DocumentTimeoutError(f"document_timeout:{self.timeout_s}s")


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
    return _latest_by_runid(directory, "cc_validated_hits_wet_", ".parquet")


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
    return [metric, value]


def _load_metric_map(path: Path) -> Dict[str, object]:
    df = pd.read_csv(path)
    if "metric" not in df.columns or "value" not in df.columns:
        raise ValueError(f"Missing expected columns in {path}")
    return dict(zip(df["metric"], df["value"]))


def _warc_extraction_config(config: Dict, stage_key: str) -> Dict[str, object]:
    warc_cfg = config.get(stage_key, {}).get("warc_validation", {})
    return {
        "favor_recall": bool(warc_cfg.get("favor_recall", False)),
        "favor_precision": bool(warc_cfg.get("favor_precision", False)),
        "include_tables": bool(warc_cfg.get("include_tables", True)),
        "resiliparse_comments": bool(warc_cfg.get("resiliparse_comments", True)),
        "resiliparse_post_meta": bool(warc_cfg.get("resiliparse_post_meta", True)),
        "document_timeout_s": int(warc_cfg.get("document_timeout_s", 300)),
        "request_timeout_s": int(warc_cfg.get("request_timeout_s", 120)),
        "fetch_max_attempts": int(warc_cfg.get("fetch_max_attempts", 4)),
        "fetch_retry_initial_backoff_s": float(warc_cfg.get("fetch_retry_initial_backoff_s", 2.0)),
        "fetch_retry_max_backoff_s": float(warc_cfg.get("fetch_retry_max_backoff_s", 30.0)),
        "fetch_retry_http_statuses": {
            int(status)
            for status in warc_cfg.get(
                "fetch_retry_http_statuses",
                [403, 429, 500, 502, 503, 504],
            )
        },
        "fetch_cooldown_after_consecutive_failures": int(
            warc_cfg.get("fetch_cooldown_after_consecutive_failures", 25)
        ),
        "fetch_cooldown_s": float(warc_cfg.get("fetch_cooldown_s", 60.0)),
        "fetch_abort_after_consecutive_failures": int(
            warc_cfg.get("fetch_abort_after_consecutive_failures", 500)
        ),
    }


def _require_stage(config: Dict, expected_stage: str) -> None:
    stage = str(config.get("run_context", {}).get("stage", "")).strip()
    if stage != expected_stage:
        stage_label = expected_stage.replace("stage", "Stage ")
        raise ValueError(f"{stage_label} commands require run_context.stage={expected_stage}")


def _require_collection_stage(config: Dict) -> None:
    _require_stage(config, "collection")


def _http_get_bytes(url: str, timeout: int = 60) -> bytes:
    request = Request(url, headers={"User-Agent": "msc-nlp-therapy-speak/0.2"})
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def _mime_is_html(mime: str) -> bool:
    return "html" in (mime or "").lower()


def _lookup_outcome_from_candidates(
    candidates: List[LookupRecord],
    note_ok: str,
    note_fallback: str,
    *,
    attempts: int,
    last_error: str,
    retry_delay_total_s: float,
    provider: str,
) -> LookupOutcome:
    match_count = len(candidates)
    for candidate in candidates:
        if candidate.status == "200" and (_mime_is_html(candidate.mime) or candidate.mime == ""):
            return LookupOutcome(
                record=candidate,
                note=note_ok,
                attempts=attempts,
                last_error=last_error,
                retry_delay_total_s=round(retry_delay_total_s, 3),
                match_count=match_count,
                provider=provider,
            )
    return LookupOutcome(
        record=candidates[0],
        note=note_fallback,
        attempts=attempts,
        last_error=last_error,
        retry_delay_total_s=round(retry_delay_total_s, 3),
        match_count=match_count,
        provider=provider,
    )


def _retry_backoff_s(attempt: int, extraction_cfg: Dict[str, object]) -> float:
    initial = max(0.0, float(extraction_cfg.get("fetch_retry_initial_backoff_s", 2.0)))
    maximum = max(initial, float(extraction_cfg.get("fetch_retry_max_backoff_s", 30.0)))
    return min(maximum, initial * (2 ** max(0, attempt - 1)))


def _fetch_warc_payload(
    record: LookupRecord,
    *,
    extraction_cfg: Dict[str, object],
) -> Tuple[Optional[dict], str, int, str, float, bool]:
    end_offset = record.offset + record.length - 1
    warc_url = f"{COMMONCRAWL_DATA_BASE_URL}/{record.filename}"
    max_attempts = max(1, int(extraction_cfg.get("fetch_max_attempts", 4)))
    retry_http_statuses = {
        int(status) for status in extraction_cfg.get("fetch_retry_http_statuses", set())
    }
    request_timeout_s = int(extraction_cfg.get("request_timeout_s", 120))
    retry_delay_total_s = 0.0
    last_error = ""
    retryable_failure = False
    payload: Optional[bytes] = None
    attempt = 0

    for attempt in range(1, max_attempts + 1):
        request = Request(
            warc_url,
            headers={
                "Range": f"bytes={record.offset}-{end_offset}",
                "User-Agent": "msc-nlp-therapy-speak/0.2",
            },
        )
        try:
            with urlopen(request, timeout=request_timeout_s) as response:
                payload = response.read()
            break
        except HTTPError as exc:
            last_error = f"warc_http_error:{exc.code}"
            retryable_failure = exc.code in retry_http_statuses
        except URLError as exc:
            last_error = f"warc_url_error:{exc.reason}"
            retryable_failure = True
        except Exception as exc:  # pragma: no cover - network/runtime dependent
            last_error = f"warc_error:{type(exc).__name__}"
            retryable_failure = True

        if not retryable_failure or attempt >= max_attempts:
            return None, last_error, attempt, last_error, retry_delay_total_s, retryable_failure
        delay_s = _retry_backoff_s(attempt, extraction_cfg)
        retry_delay_total_s += delay_s
        if delay_s > 0:
            time.sleep(delay_s)

    if payload is None:
        return (
            None,
            last_error or "warc_error:empty_payload",
            attempt,
            last_error,
            retry_delay_total_s,
            retryable_failure,
        )

    try:
        with gzip.GzipFile(fileobj=io.BytesIO(payload)) as gz:
            for warc_record in ArchiveIterator(gz):
                content = warc_record.content_stream().read()
                return (
                    {
                        "warc_type": warc_record.rec_type,
                        "capture_ts": warc_record.rec_headers.get_header("WARC-Date") or "",
                        "warc_target_uri": warc_record.rec_headers.get_header("WARC-Target-URI")
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
                    attempt,
                    last_error,
                    retry_delay_total_s,
                    False,
                )
    except Exception as exc:  # pragma: no cover - parsing/runtime dependent
        return (
            None,
            f"warc_parse_error:{type(exc).__name__}",
            attempt,
            last_error,
            retry_delay_total_s,
            False,
        )

    return None, "warc_empty_record", attempt, last_error, retry_delay_total_s, False


def _load_boto3():
    try:
        import boto3  # type: ignore
    except ImportError as exc:  # pragma: no cover - dependency dependent
        raise RuntimeError(
            "Missing dependency 'boto3'. Install the collection AWS dependencies "
            "before using the remote resolver/extraction workflow."
        ) from exc
    return boto3


def _latest_pointer_cache_path(pointer_cache_dir: Path) -> Tuple[str, Path]:
    out_dir = pointer_cache_dir
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


def _load_pointer_cache_df(
    pointer_cache_dir: Path, pointer_cache_uri: Optional[str]
) -> pd.DataFrame:
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
    _, latest_path = _latest_pointer_cache_path(pointer_cache_dir)
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


def _capture_max_date(capture_ts: Optional[str]) -> Optional[str]:
    if not capture_ts:
        return None
    capture_dt = pd.to_datetime(capture_ts, utc=True, errors="coerce")
    if pd.isna(capture_dt):
        return None
    return capture_dt.strftime("%Y-%m-%d")


def _published_ts_passes_sanity(
    candidate_ts: Optional[str],
    capture_ts: Optional[str],
    tolerance_hours: int,
    min_date: Optional[str],
) -> bool:
    if not candidate_ts:
        return False
    candidate_dt = pd.to_datetime(candidate_ts, utc=True, errors="coerce")
    if pd.isna(candidate_dt):
        return False

    candidate_ymd = candidate_dt.strftime("%Y-%m-%d")
    if candidate_dt.year <= 1 or candidate_ymd == "1970-01-01":
        return False

    if min_date:
        min_dt = pd.to_datetime(min_date, utc=True, errors="coerce")
        if not pd.isna(min_dt) and candidate_dt < min_dt:
            return False

    return _candidate_before_capture(candidate_ts, capture_ts, tolerance_hours)


def _extract_published_timestamp(
    html_text: str,
    url: str,
    capture_ts: Optional[str],
    tolerance_hours: int,
    min_date: Optional[str],
) -> Optional[str]:
    try:
        from htmldate import find_date
    except ImportError as exc:  # pragma: no cover - dependency dependent
        raise RuntimeError(
            "Missing dependency 'htmldate'. Install it before running WARC extraction."
        ) from exc

    capture_max_date = _capture_max_date(capture_ts)
    for extensive_search in (False, True):
        try:
            raw_candidate = find_date(
                html_text,
                extensive_search=extensive_search,
                original_date=True,
                outputformat="%Y-%m-%d",
                url=url or None,
                min_date=min_date,
                max_date=capture_max_date,
                deferred_url_extractor=True,
            )
        except Exception:  # pragma: no cover - extractor/runtime dependent
            continue
        normalized = _normalize_timestamp(raw_candidate)
        if _published_ts_passes_sanity(
            normalized,
            capture_ts=capture_ts,
            tolerance_hours=tolerance_hours,
            min_date=min_date,
        ):
            return normalized

    return None


def _extract_schema_types(html_text: str, url: str) -> str:
    try:
        import extruct
    except ImportError as exc:  # pragma: no cover - dependency dependent
        raise RuntimeError(
            "Missing dependency 'extruct'. Install it before running WARC extraction."
        ) from exc

    try:
        metadata = extruct.extract(
            html_text,
            base_url=url or None,
            syntaxes=["json-ld", "microdata", "opengraph", "rdfa", "dublincore"],
            uniform=True,
        )
    except Exception:  # pragma: no cover - extractor/runtime dependent
        return ""

    def _normalize_type_name(raw_value: object) -> str:
        raw_text = str(raw_value or "").strip()
        if not raw_text:
            return ""
        normalized = raw_text.rstrip("/").split("/")[-1].split(":")[-1]
        return normalized.strip()

    def _walk_types(node: Any) -> Iterable[str]:
        if isinstance(node, dict):
            for key, value in node.items():
                lowered = key.lower()
                if lowered in {"@type", "type"}:
                    if isinstance(value, list):
                        for item in value:
                            normalized = _normalize_type_name(item)
                            if normalized:
                                yield normalized
                    else:
                        normalized = _normalize_type_name(value)
                        if normalized:
                            yield normalized
                yield from _walk_types(value)
        elif isinstance(node, list):
            for item in node:
                yield from _walk_types(item)

    schema_types = sorted({value for value in _walk_types(metadata) if value})
    return "|".join(schema_types)


def _trafilatura_extract(
    html_text: str,
    *,
    favor_recall: bool,
    favor_precision: bool,
    include_tables: bool,
) -> Tuple[Optional[str], str]:
    try:
        import trafilatura
    except ImportError as exc:  # pragma: no cover - dependency dependent
        raise RuntimeError(
            "Missing dependency 'trafilatura'. Install it before running WARC extraction."
        ) from exc

    extracted = trafilatura.extract(
        html_text,
        favor_recall=favor_recall,
        favor_precision=favor_precision,
        include_comments=False,
        include_tables=include_tables,
    )
    if not extracted:
        return None, "trafilatura_empty"
    normalized = " ".join(extracted.split())
    if not normalized:
        return None, "trafilatura_whitespace_only"
    return normalized, "ok"


def _resiliparse_extract(
    html_text: str, *, comments: bool, post_meta: bool
) -> Tuple[Optional[str], str]:
    try:
        from resiliparse.extract.html2text import extract_plain_text  # type: ignore
        from resiliparse.parse.html import HTMLTree  # type: ignore
    except ImportError:
        return None, "resiliparse_missing"

    try:
        tree = HTMLTree.parse(html_text)
        extracted = extract_plain_text(
            tree,
            main_content=True,
            preserve_formatting=False,
            comments=comments,
            post_meta=post_meta,
        )
    except Exception as exc:  # pragma: no cover - dependency/runtime dependent
        return None, f"resiliparse_error:{type(exc).__name__}"
    normalized = " ".join(str(extracted or "").split())
    if not normalized:
        return None, "resiliparse_empty"
    return normalized, "ok"


def _extract_main_content(
    html_text: str, extraction_cfg: Dict[str, object]
) -> Tuple[Optional[str], str, str]:
    extracted_text, note = _trafilatura_extract(
        html_text,
        favor_recall=bool(extraction_cfg.get("favor_recall", False)),
        favor_precision=bool(extraction_cfg.get("favor_precision", False)),
        include_tables=bool(extraction_cfg.get("include_tables", True)),
    )
    if extracted_text:
        return extracted_text, note, "trafilatura"

    fallback_text, fallback_note = _resiliparse_extract(
        html_text,
        comments=bool(extraction_cfg.get("resiliparse_comments", True)),
        post_meta=bool(extraction_cfg.get("resiliparse_post_meta", True)),
    )
    if fallback_text:
        return fallback_text, fallback_note, "resiliparse"

    return None, note if note else fallback_note, ""


def _compute_publication_date_metrics(doc_df: pd.DataFrame) -> Dict[str, object]:
    required_columns = [
        "warc_fetch_success",
        "is_validated_hits_warc",
        "published_ts",
    ]
    metrics: Dict[str, object] = {}
    if doc_df.empty or any(column not in doc_df.columns for column in required_columns):
        return metrics

    working_df = doc_df.copy()
    working_df["warc_fetch_success"] = working_df["warc_fetch_success"].fillna(False).astype(bool)
    working_df["is_validated_hits_warc"] = (
        working_df["is_validated_hits_warc"].fillna(False).astype(bool)
    )
    working_df["published_ts"] = working_df["published_ts"].fillna("").astype(str)

    fetch_success_df = working_df.loc[working_df["warc_fetch_success"]].copy()
    validated_df = working_df.loc[working_df["is_validated_hits_warc"]].copy()
    fetch_success_found_mask = fetch_success_df["published_ts"].str.strip() != ""
    validated_found_mask = validated_df["published_ts"].str.strip() != ""

    found_fetch_docs = int(fetch_success_found_mask.sum())
    metrics["doc_count_published_ts_found"] = found_fetch_docs
    metrics["doc_count_published_ts_missing"] = int(len(fetch_success_df) - found_fetch_docs)
    metrics["pct_fetch_success_docs_with_published_ts"] = round(
        (found_fetch_docs / len(fetch_success_df) * 100.0 if len(fetch_success_df) else 0.0),
        6,
    )
    metrics["doc_count_warc_validated_with_published_ts"] = int(validated_found_mask.sum())
    metrics["pct_warc_validated_docs_with_published_ts"] = round(
        (validated_found_mask.mean() * 100.0 if len(validated_df) else 0.0),
        6,
    )

    return metrics


def _build_publication_date_rows(publication_metrics: Dict[str, object]) -> List[List[object]]:
    if not publication_metrics:
        return []

    rows: List[List[object]] = []
    metric_descriptions = {
        "doc_count_published_ts_found": (
            "Fetch-success documents with a candidate published timestamp."
        ),
        "doc_count_published_ts_missing": (
            "Fetch-success documents without a candidate published timestamp."
        ),
        "pct_fetch_success_docs_with_published_ts": (
            "Share of fetch-success documents with a candidate published timestamp."
        ),
        "doc_count_warc_validated_with_published_ts": (
            "WARC-validated documents with a candidate published timestamp."
        ),
        "pct_warc_validated_docs_with_published_ts": (
            "Share of WARC-validated documents with a candidate published timestamp."
        ),
    }

    for metric_name, description in metric_descriptions.items():
        if metric_name in publication_metrics:
            rows.append(_summary_row(metric_name, publication_metrics[metric_name], description))

    return rows


def _build_warc_summary_rows(
    docs_scanned: int,
    candidate_hits: int,
    validated_hits_wet: int,
    validated_hits_warc: int,
    role_counts: Dict[str, Dict[str, int]],
    doc_metrics: Dict[str, object],
    publication_metrics: Optional[Dict[str, object]],
    notes: str,
) -> List[List[object]]:
    rows = [
        _summary_row(
            "docs_scanned",
            docs_scanned,
            "Documents scanned in the WET input files.",
        ),
        _summary_row(
            "candidate_hits",
            candidate_hits,
            "Candidate hits in the WET input files.",
        ),
        _summary_row(
            "validated_hits_wet",
            validated_hits_wet,
            "WET-validated hits used as input to WARC validation.",
        ),
        _summary_row(
            "validated_hits_warc",
            validated_hits_warc,
            "Row-level hits that survived WARC validation and extraction.",
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
    rows.extend(_build_publication_date_rows(publication_metrics or {}))
    omitted_metrics = {"docs_per_sec", "lookup_provider", "total_elapsed_sec"}
    for metric_name, metric_value in sorted(doc_metrics.items()):
        if metric_name in omitted_metrics:
            continue
        rows.append(
            _summary_row(
                metric_name,
                metric_value,
                "Document-level lookup, fetch, extraction, or filtering metric.",
            )
        )
    return rows


def _write_summary_csv(path: Path, rows: List[List[object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value"])
        writer.writerows([row[:2] for row in rows])


def _write_warc_term_summary(
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
            df.groupby(["crawl_id", "term_role", "term_group", "matched_term"], dropna=False)
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


def _upload_warc_outputs_to_s3(local_paths: List[Path], s3_output_prefix: str) -> List[str]:
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


def _latest_stage_hits_source(
    config: Dict, stage_name: str
) -> Tuple[str, Path, Path, pd.DataFrame]:
    source_label = str(config.get("run_context", {}).get("label", stage_name)).strip() or stage_name
    output_dir = working_output_dir(config, default_name="wet_scan")
    runid, hits_path = _latest_validated_hits_wet(output_dir)
    summary_path = output_dir / f"cc_scan_summary_{runid}.csv"
    if not summary_path.exists():
        raise FileNotFoundError(f"Missing scan summary for {stage_name}: {summary_path}")
    hits_df = pd.read_parquet(hits_path).copy()
    hits_df["source_stage"] = source_label
    hits_df["source_scope"] = "combined"
    hits_df["source_runid"] = runid
    return runid, hits_path, summary_path, hits_df


def extract_pointer_cache(
    config: Dict,
    pointer_cache_uri: Optional[str] = None,
    s3_output_prefix: Optional[str] = None,
) -> Path:
    _require_collection_stage(config)
    extraction_start = time.perf_counter()
    runid = _utc_runid()
    label = str(config.get("run_context", {}).get("label", "collection")).strip() or "collection"
    display_label = label
    logger = _setup_logger(Path("reports/logs"), f"cc-{label}-extract", runid)

    source_runid, _, input_summary_path, combined_hits_df = _latest_stage_hits_source(
        config, "collection"
    )
    if combined_hits_df.empty:
        raise ValueError(f"No validated WET hits available for {display_label} extraction.")

    extraction_cfg = _warc_extraction_config(config, "collection_stage")
    document_timeout_s = int(extraction_cfg.get("document_timeout_s", 300) or 0)
    tolerance_hours = int(
        config.get("collection_stage", {}).get("published_ts_tolerance_hours", 48)
    )
    min_published_ts = (
        str(config.get("collection_stage", {}).get("published_ts_min_date", "1995-01-01")).strip()
        or None
    )

    pointer_df = _load_pointer_cache_df(pointer_cache_output_dir(config), pointer_cache_uri)
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

    logger.info("Loaded %d %s input hits", len(combined_hits_df), display_label)
    logger.info("%s source runid: %s", display_label, source_runid)
    logger.info("Loaded pointer cache rows=%d", len(pointer_df))
    if pointer_cache_uri:
        logger.info("Pointer cache source: %s", pointer_cache_uri)

    pointer_map = {
        (str(row["crawl_id"]), str(row["url"])): row for row in pointer_df.to_dict(orient="records")
    }
    unique_docs = combined_hits_df[DOC_KEY_COLUMNS].drop_duplicates().reset_index(drop=True)

    doc_records: List[Dict[str, object]] = []
    lookup_success_docs = 0
    fetch_success_docs = 0
    extract_success_docs = 0
    consecutive_retryable_fetch_failures = 0
    cooldown_after_failures = int(
        extraction_cfg.get("fetch_cooldown_after_consecutive_failures", 25)
    )
    cooldown_s = float(extraction_cfg.get("fetch_cooldown_s", 60.0))
    abort_after_failures = int(extraction_cfg.get("fetch_abort_after_consecutive_failures", 500))

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
            "schema_types": "",
            "cdx_attempts": 0,
            "cdx_last_error": "",
            "cdx_retry_delay_total_s": 0.0,
            "cdx_cooldown_triggered": False,
            "warc_fetch_attempts": 0,
            "warc_fetch_last_error": "",
            "warc_retry_delay_total_s": 0.0,
            "warc_cooldown_triggered": False,
            "warc_validation_notes": lookup_note,
            "pointer_fetch_status": pointer_fetch_status,
            "pointer_content_mime_type": pointer_content_mime_type,
            "extractor_used": "",
        }
        if record is None:
            doc_records.append(row)
            continue

        lookup_success_docs += 1
        try:
            with _DocumentTimeout(document_timeout_s):
                (
                    fetched_payload,
                    fetch_note,
                    fetch_attempts,
                    fetch_last_error,
                    retry_delay_total_s,
                    retryable_fetch_failure,
                ) = _fetch_warc_payload(record, extraction_cfg=extraction_cfg)
                row["warc_fetch_attempts"] = fetch_attempts
                row["warc_fetch_last_error"] = fetch_last_error
                row["warc_retry_delay_total_s"] = round(retry_delay_total_s, 3)
                row["warc_validation_notes"] = fetch_note
                if fetched_payload is None:
                    if retryable_fetch_failure:
                        consecutive_retryable_fetch_failures += 1
                        if (
                            cooldown_after_failures > 0
                            and consecutive_retryable_fetch_failures % cooldown_after_failures == 0
                        ):
                            row["warc_cooldown_triggered"] = True
                            logger.warning(
                                "%s remote extraction cooling down after %d consecutive "
                                "retryable WARC fetch failures | cooldown_s=%.1f "
                                "last_note=%s",
                                display_label,
                                consecutive_retryable_fetch_failures,
                                cooldown_s,
                                fetch_note,
                            )
                            if cooldown_s > 0:
                                time.sleep(cooldown_s)
                        if (
                            abort_after_failures > 0
                            and consecutive_retryable_fetch_failures >= abort_after_failures
                        ):
                            raise RuntimeError(
                                "Aborting WARC extraction after "
                                f"{consecutive_retryable_fetch_failures} consecutive "
                                f"retryable fetch failures; latest note={fetch_note}."
                            )
                    else:
                        consecutive_retryable_fetch_failures = 0
                    doc_records.append(row)
                    continue

                consecutive_retryable_fetch_failures = 0
                fetch_success_docs += 1
                row["warc_fetch_success"] = True
                row["capture_ts"] = fetched_payload.get("capture_ts", "")
                row["warc_target_uri"] = fetched_payload.get("warc_target_uri", "")
                row["http_status"] = fetched_payload.get("http_status", "")
                row["html_bytes"] = int(fetched_payload.get("html_bytes", 0) or 0)

                html_text = str(fetched_payload.get("html_text", ""))
                extracted_text, extract_note, extractor_used = _extract_main_content(
                    html_text, extraction_cfg=extraction_cfg
                )
                row["warc_validation_notes"] = extract_note
                row["extractor_used"] = extractor_used
                row["schema_types"] = _extract_schema_types(html_text, url)
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
                    if candidate_terms and not any(
                        term in extracted_lower for term in candidate_terms
                    ):
                        row["warc_validation_notes"] = "term_missing_after_extraction"
                    else:
                        extract_success_docs += 1
                        row["trafilatura_success"] = extractor_used == "trafilatura"
                        row["is_validated_hits_warc"] = True
                        row["extracted_text"] = extracted_text
                        row["extracted_text_len"] = len(extracted_text)
                published_ts = _extract_published_timestamp(
                    html_text=html_text,
                    url=url,
                    capture_ts=str(row["capture_ts"]) or None,
                    tolerance_hours=tolerance_hours,
                    min_date=min_published_ts,
                )
                row["published_ts"] = published_ts or ""
        except _DocumentTimeoutError:
            row["warc_validation_notes"] = f"document_timeout:{document_timeout_s}s"
            logger.warning(
                "%s remote extraction skipped timed-out doc %d/%d | crawl_id=%s "
                "url=%s warc_filename=%s offset=%s length=%s timeout_s=%d",
                display_label,
                doc_idx,
                len(unique_docs),
                crawl_id,
                url,
                record.filename,
                record.offset,
                record.length,
                document_timeout_s,
            )
            doc_records.append(row)
            continue
        doc_records.append(row)

        if doc_idx % 25 == 0:
            logger.info(
                "%s remote extraction progress %d/%d docs | lookup_success=%d "
                "fetch_success=%d extract_success=%d",
                display_label,
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
    enriched_df["warc_fetch_success"] = enriched_df["warc_fetch_success"].fillna(False).astype(bool)
    enriched_df["lookup_provider"] = enriched_df["lookup_provider"].fillna("pointer_cache")
    enriched_df["lookup_note"] = enriched_df["lookup_note"].fillna("pointer_cache_no_match")
    enriched_df["lookup_match_count"] = (
        pd.to_numeric(enriched_df["lookup_match_count"], errors="coerce").fillna(0).astype(int)
    )
    for column in (
        "capture_ts",
        "warc_validation_notes",
        "published_ts",
        "pointer_fetch_status",
        "pointer_content_mime_type",
        "extractor_used",
        "extracted_text",
        "schema_types",
        "warc_fetch_last_error",
    ):
        enriched_df[column] = enriched_df[column].fillna("")
    enriched_df["extracted_text_len"] = (
        pd.to_numeric(enriched_df["extracted_text_len"], errors="coerce").fillna(0).astype(int)
    )
    enriched_df["warc_fetch_attempts"] = (
        pd.to_numeric(enriched_df["warc_fetch_attempts"], errors="coerce").fillna(0).astype(int)
    )
    enriched_df["warc_retry_delay_total_s"] = (
        pd.to_numeric(enriched_df["warc_retry_delay_total_s"], errors="coerce")
        .fillna(0.0)
        .astype(float)
    )
    enriched_df["warc_cooldown_triggered"] = (
        enriched_df["warc_cooldown_triggered"].fillna(False).astype(bool)
    )

    validated_warc_df = enriched_df.loc[enriched_df["is_validated_hits_warc"].fillna(False)].copy()
    publication_metrics = _compute_publication_date_metrics(doc_df)
    source_summary = _load_metric_map(input_summary_path)
    docs_scanned_total = _metric_int(source_summary, "docs_scanned", 0)
    candidate_hits_total = _metric_int(source_summary, "candidate_hits", 0)
    validated_hits_wet_total = len(enriched_df)
    validated_hits_warc_total = len(validated_warc_df)
    role_counts: Dict[str, Dict[str, int]] = {}
    for role in ("target", "baseline"):
        role_df = enriched_df.loc[enriched_df["term_role"] == role]
        role_counts[role] = {
            "validated_hits_wet": int(len(role_df)),
            "validated_hits_warc": int(role_df["is_validated_hits_warc"].sum()),
        }

    total_elapsed_sec = time.perf_counter() - extraction_start
    doc_metrics = {
        "doc_count_input": int(len(unique_docs)),
        "doc_count_lookup_success": int(lookup_success_docs),
        "doc_count_fetch_success": int(fetch_success_docs),
        "doc_count_extract_success": int(extract_success_docs),
        "doc_count_lookup_failed": int(len(unique_docs) - lookup_success_docs),
        "doc_count_fetch_failed": int(lookup_success_docs - fetch_success_docs),
        "doc_count_extract_failed": int(fetch_success_docs - extract_success_docs),
        "doc_count_fetch_retried": int((doc_df["warc_fetch_attempts"] > 1).sum()),
        "doc_count_fetch_cooldowns": int(doc_df["warc_cooldown_triggered"].sum()),
        "warc_retry_delay_total_s": round(float(doc_df["warc_retry_delay_total_s"].sum()), 6),
        "total_elapsed_sec": round(total_elapsed_sec, 6),
        "docs_per_sec": round(len(unique_docs) / max(total_elapsed_sec, 0.000001), 6),
        "lookup_provider": "pointer_cache",
    }

    notes = f"{display_label} WARC validation over the frozen collection input WET files."
    if pointer_cache_uri:
        notes += f" pointer_cache_source={pointer_cache_uri}."

    out_dir = warc_output_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    enriched_path = out_dir / f"cc_enriched_hits_{runid}.parquet"
    validated_warc_path = out_dir / f"cc_validated_hits_warc_{runid}.parquet"
    summary_path = out_dir / f"cc_{label}_summary_{runid}.csv"
    term_summary_path = out_dir / f"cc_{label}_term_summary_{runid}.csv"
    manifest_path = out_dir / f"cc_{label}_extract_manifest_{runid}.json"

    enriched_df.to_parquet(enriched_path, index=False)
    validated_warc_df.to_parquet(validated_warc_path, index=False)
    _write_warc_term_summary(term_summary_path, enriched_df, include_filter_fields=False)
    _write_summary_csv(
        summary_path,
        _build_warc_summary_rows(
            docs_scanned=docs_scanned_total,
            candidate_hits=candidate_hits_total,
            validated_hits_wet=validated_hits_wet_total,
            validated_hits_warc=validated_hits_warc_total,
            role_counts=role_counts,
            doc_metrics=doc_metrics,
            publication_metrics=publication_metrics,
            notes=notes,
        ),
    )

    manifest_payload = {
        "runid": runid,
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source_runid": source_runid,
        "pointer_cache_uri": pointer_cache_uri
        or str(_latest_pointer_cache_path(pointer_cache_output_dir(config))[1]),
        "s3_output_prefix": s3_output_prefix or "",
        "enriched_path": str(enriched_path),
        "validated_warc_path": str(validated_warc_path),
        "summary_path": str(summary_path),
        "term_summary_path": str(term_summary_path),
        "total_elapsed_sec": round(total_elapsed_sec, 6),
        "docs_per_sec": round(len(unique_docs) / max(total_elapsed_sec, 0.000001), 6),
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True))

    if s3_output_prefix:
        uploaded_uris = _upload_warc_outputs_to_s3(
            [
                enriched_path,
                validated_warc_path,
                summary_path,
                term_summary_path,
                manifest_path,
            ],
            s3_output_prefix,
        )
        logger.info("Uploaded %s remote extraction outputs to %s", display_label, s3_output_prefix)
        for uri in uploaded_uris:
            logger.info("Uploaded %s", uri)

    logger.info("Wrote enriched hits %s", enriched_path)
    logger.info("Wrote WARC-validated hits %s", validated_warc_path)
    logger.info("Wrote %s summary %s", display_label, summary_path)
    return enriched_path
