import csv
import gzip
import hashlib
import heapq
import json
import logging
import re
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import pandas as pd

from src.pathing import collection_interim_dir, processed_trend_dir

from .cc_collection import (
    _collection_cfg,
    _collection_years,
    _crawl_id_for_year,
    _processed_output_prefix,
    _upload_paths_to_s3,
)
from .cc_document_quality import (
    _classify_language_safely,
    _load_language_identifier,
    _normalize_text_for_dedup,
)
from .cc_resolve import (
    _cdxj_shard_urls_for_crawl,
    _parse_cdxj_line,
    _record_from_cdxj_json,
    _resolve_remote_config,
)
from .cc_scan import (
    asd_disambiguated,
    compile_asd_pattern,
    compile_boilerplate_patterns,
    compile_patterns,
    extract_registered_domain,
    is_negative_page_type_url,
)
from .cc_warc import (
    LookupRecord,
    _DocumentTimeout,
    _DocumentTimeoutError,
    _extract_main_content,
    _extract_published_timestamp,
    _fetch_warc_payload,
    _mime_is_html,
    _setup_logger,
    _utc_runid,
    _warc_extraction_config,
)


def _trend_cfg(config: Dict) -> Dict:
    return _collection_cfg(config).get("trend", {})


def _trend_root(config: Dict) -> Path:
    return collection_interim_dir(config) / "trend"


def _trend_batch_dir(config: Dict, batch: int | str) -> Path:
    return _trend_root(config) / f"batch_{int(batch):03d}"


def _trend_pilot_dir(config: Dict, runid: str) -> Path:
    return _trend_root(config) / "pilot" / runid


def _safe_int(value: object, default: Optional[int] = None) -> Optional[int]:
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except TypeError:
        pass
    text = str(value).strip()
    if not text:
        return default
    return int(float(text))


def _required_int(value: object, label: str) -> int:
    parsed = _safe_int(value)
    if parsed is None or parsed <= 0:
        raise ValueError(f"Set a positive integer for collection.trend.{label}.")
    return parsed


def _publication_window(config: Dict) -> Tuple[int, int]:
    trend_cfg = _trend_cfg(config)
    return (
        int(trend_cfg.get("publication_year_start", 2014)),
        int(trend_cfg.get("publication_year_end", 2026)),
    )


def _normalize_url(url: str) -> str:
    parts = urlsplit(str(url or "").strip())
    scheme = (parts.scheme or "http").lower()
    netloc = parts.netloc.lower()
    path = re.sub(r"/{2,}", "/", parts.path or "/")
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    query_pairs = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not key.lower().startswith("utm_")
    ]
    query = urlencode(sorted(query_pairs), doseq=True)
    return urlunsplit((scheme, netloc, path, query, ""))


def _stable_score(config: Dict, record: LookupRecord) -> int:
    seed = str(config.get("project", {}).get("seed", 123))
    key = "|".join(
        [
            seed,
            record.crawl_id,
            record.url,
            record.timestamp,
            record.filename,
            str(record.offset),
        ]
    )
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest(), 16)


def _record_sort_key(record: LookupRecord) -> Tuple[str, str, int, int]:
    return (record.crawl_id, record.filename, int(record.offset), int(record.length))


def _heap_tiebreaker(record: LookupRecord) -> str:
    crawl_id, filename, offset, length = _record_sort_key(record)
    return "|".join([crawl_id, filename, str(offset), str(length), record.url])


def _inverted_sort_key(record: LookupRecord) -> Tuple[str, str, int, int]:
    crawl_id, filename, offset, length = _record_sort_key(record)
    return (
        "".join(chr(0x10FFFF - ord(char)) for char in crawl_id),
        "".join(chr(0x10FFFF - ord(char)) for char in filename),
        -offset,
        -length,
    )


def _stable_shard_score(config: Dict, *, crawl_id: str, shard_url: str) -> int:
    seed = str(config.get("project", {}).get("seed", 123))
    key = "|".join([seed, crawl_id, shard_url])
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest(), 16)


def _selected_cdx_shard_urls(
    config: Dict,
    *,
    crawl_id: str,
    batch: int,
    max_shards: int,
) -> List[str]:
    resolve_cfg = _resolve_remote_config({"collection_stage": _collection_cfg(config)})
    shard_urls = _cdxj_shard_urls_for_crawl(crawl_id, resolve_cfg)
    ranked = sorted(
        shard_urls,
        key=lambda shard_url: (
            _stable_shard_score(config, crawl_id=crawl_id, shard_url=shard_url),
            shard_url,
        ),
    )
    start = (batch - 1) * max_shards
    selected = ranked[start : start + max_shards]
    if not selected:
        raise ValueError(
            f"Batch {batch} requests shards beyond the available CDXJ shard frame for {crawl_id}. "
            f"Available shards={len(shard_urls)}, max_shards_per_batch={max_shards}."
        )
    return selected


def _iter_cdx_records_for_shard(
    config: Dict,
    *,
    crawl_id: str,
    shard_url: str,
) -> Iterable[LookupRecord]:
    resolve_cfg = _resolve_remote_config({"collection_stage": _collection_cfg(config)})
    request = Request(shard_url, headers={"User-Agent": "msc-nlp-therapy-speak/0.2"})
    with urlopen(request, timeout=resolve_cfg.request_timeout_s) as response:
        with gzip.GzipFile(fileobj=response) as gz:
            for raw_line in gz:
                parsed = _parse_cdxj_line(raw_line)
                if not parsed:
                    continue
                record = _record_from_cdxj_json(crawl_id, parsed)
                if record is not None:
                    yield record


def _trend_retry_cfg(config: Dict) -> Tuple[int, float, float, set[int]]:
    acquisition_cfg = _collection_cfg(config).get("acquisition", {})
    return (
        max(1, int(acquisition_cfg.get("max_attempts", 4))),
        float(acquisition_cfg.get("retry_initial_backoff_s", 2.0)),
        float(acquisition_cfg.get("retry_max_backoff_s", 30.0)),
        {int(status) for status in acquisition_cfg.get("retry_http_statuses", [])},
    )


def _retryable_cdx_exception(config: Dict, exc: BaseException) -> bool:
    _, _, _, retry_statuses = _trend_retry_cfg(config)
    if isinstance(exc, HTTPError):
        return int(exc.code) in retry_statuses
    return isinstance(
        exc,
        (
            ConnectionError,
            ConnectionResetError,
            EOFError,
            TimeoutError,
            URLError,
            gzip.BadGzipFile,
            OSError,
        ),
    )


def _bounded_heap_add(
    heap: List[Tuple[int, Tuple[str, str, int, int], str, LookupRecord]],
    item: Tuple[int, Tuple[str, str, int, int], str, LookupRecord],
    limit: int,
) -> None:
    if len(heap) < limit:
        heapq.heappush(heap, item)
    elif item > heap[0]:
        heapq.heapreplace(heap, item)


def _sample_records_from_shard_once(
    config: Dict,
    *,
    source_year: int,
    crawl_id: str,
    shard_idx: int,
    shard_count: int,
    shard_url: str,
    sample_per_source_year: int,
    negative_enabled: bool,
    negative_patterns: List[re.Pattern],
    logger: logging.Logger,
) -> Tuple[int, int, List[Tuple[int, Tuple[str, str, int, int], str, LookupRecord]]]:
    seen = 0
    accepted = 0
    shard_heap: List[Tuple[int, Tuple[str, str, int, int], str, LookupRecord]] = []
    for record in _iter_cdx_records_for_shard(config, crawl_id=crawl_id, shard_url=shard_url):
        seen += 1
        if seen % 1_000_000 == 0:
            logger.info(
                "Sampling progress source_year=%s crawl_id=%s shard=%d/%d "
                "records_seen=%d accepted=%d selected_heap=%d",
                source_year,
                crawl_id,
                shard_idx,
                shard_count,
                seen,
                accepted,
                len(shard_heap),
            )
        if record.status != "200":
            continue
        if not (_mime_is_html(record.mime) or record.mime == ""):
            continue
        if negative_enabled and is_negative_page_type_url(record.url, negative_patterns):
            continue
        accepted += 1
        score = _stable_score(config, record)
        heap_item = (
            -score,
            _inverted_sort_key(record),
            _heap_tiebreaker(record),
            record,
        )
        _bounded_heap_add(shard_heap, heap_item, sample_per_source_year)
    return seen, accepted, shard_heap


def _sample_records_for_crawl(
    config: Dict,
    *,
    source_year: int,
    crawl_id: str,
    batch: int,
    sample_per_source_year: int,
    max_shards: int,
    logger: logging.Logger,
) -> List[LookupRecord]:
    selected_heap: List[Tuple[int, Tuple[str, str, int, int], str, LookupRecord]] = []
    candidate_count = 0
    negative_cfg = _collection_cfg(config).get("boilerplate", {}).get(
        "negative_page_type_url", {}
    )
    negative_patterns = compile_boilerplate_patterns(negative_cfg.get("patterns", []))
    negative_enabled = bool(negative_cfg.get("enabled", False))
    selected_shards = _selected_cdx_shard_urls(
        config,
        crawl_id=crawl_id,
        batch=batch,
        max_shards=max_shards,
    )

    max_attempts, initial_backoff_s, max_backoff_s, _ = _trend_retry_cfg(config)
    shard_count = len(selected_shards)
    for shard_idx, shard_url in enumerate(selected_shards, start=1):
        logger.info(
            "Sampling source_year=%s crawl_id=%s shard=%d/%d url=%s",
            source_year,
            crawl_id,
            shard_idx,
            shard_count,
            shard_url,
        )
        for attempt in range(1, max_attempts + 1):
            try:
                seen, accepted, shard_heap = _sample_records_from_shard_once(
                    config,
                    source_year=source_year,
                    crawl_id=crawl_id,
                    shard_idx=shard_idx,
                    shard_count=shard_count,
                    shard_url=shard_url,
                    sample_per_source_year=sample_per_source_year,
                    negative_enabled=negative_enabled,
                    negative_patterns=negative_patterns,
                    logger=logger,
                )
                candidate_count += accepted
                for heap_item in shard_heap:
                    _bounded_heap_add(selected_heap, heap_item, sample_per_source_year)
                break
            except Exception as exc:
                if attempt >= max_attempts or not _retryable_cdx_exception(config, exc):
                    logger.exception(
                        "Failed sampling source_year=%s crawl_id=%s shard=%d/%d "
                        "attempt=%d/%d url=%s",
                        source_year,
                        crawl_id,
                        shard_idx,
                        shard_count,
                        attempt,
                        max_attempts,
                        shard_url,
                    )
                    raise
                delay_s = min(max_backoff_s, initial_backoff_s * (2 ** (attempt - 1)))
                logger.warning(
                    "Retrying sampling source_year=%s crawl_id=%s shard=%d/%d "
                    "attempt=%d/%d delay_s=%.1f error=%s",
                    source_year,
                    crawl_id,
                    shard_idx,
                    shard_count,
                    attempt,
                    max_attempts,
                    delay_s,
                    repr(exc),
                )
                time.sleep(delay_s)
        logger.info(
            "Finished source_year=%s crawl_id=%s shard=%d/%d records_seen=%d accepted=%d",
            source_year,
            crawl_id,
            shard_idx,
            shard_count,
            seen,
            accepted,
        )

    selected = sorted(
        (item[3] for item in selected_heap),
        key=lambda record: (_stable_score(config, record), _record_sort_key(record)),
    )
    logger.info(
        "Selected source_year=%s crawl_id=%s records=%d candidates=%d shards=%d",
        source_year,
        crawl_id,
        len(selected),
        candidate_count,
        len(selected_shards),
    )
    return selected


def sample_trend_index(config: Dict, *, batch: int | str, pilot: bool = False) -> Path:
    runid = _utc_runid()
    logger = _setup_logger(Path("reports/logs"), "cc-trend-sample-index", runid)
    trend_cfg = _trend_cfg(config)
    batch_int = int(batch)
    if pilot:
        sample_count = _required_int(
            trend_cfg.get("pilot", {}).get("sample_records_per_source_year"),
            "pilot.sample_records_per_source_year",
        )
        max_shards = _required_int(
            trend_cfg.get("pilot", {}).get("max_index_shards_per_source_year"),
            "pilot.max_index_shards_per_source_year",
        )
        out_dir = _trend_pilot_dir(config, runid) / "sample"
    else:
        sample_count = _required_int(
            trend_cfg.get("full", {}).get("sample_records_per_source_year_batch"),
            "full.sample_records_per_source_year_batch",
        )
        max_shards = _required_int(
            trend_cfg.get("full", {}).get("max_index_shards_per_source_year_batch"),
            "full.max_index_shards_per_source_year_batch",
        )
        out_dir = _trend_batch_dir(config, batch_int) / "sample"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, object]] = []
    for source_year in _collection_years(config):
        crawl_id = _crawl_id_for_year(config, source_year)
        logger.info(
            "Sampling source_year=%s crawl_id=%s sample_records=%d max_shards=%d batch=%s",
            source_year,
            crawl_id,
            sample_count,
            max_shards,
            "pilot" if pilot else batch_int,
        )
        for record in _sample_records_for_crawl(
            config,
            source_year=int(source_year),
            crawl_id=crawl_id,
            batch=batch_int,
            sample_per_source_year=sample_count,
            max_shards=max_shards,
            logger=logger,
        ):
            rows.append(
                {
                    "source_year": int(source_year),
                    "crawl_id": record.crawl_id,
                    "url": record.url,
                    "normalized_url": _normalize_url(record.url),
                    "registered_domain": extract_registered_domain(record.url),
                    "warc_filename": record.filename,
                    "warc_offset": int(record.offset),
                    "warc_length": int(record.length),
                    "capture_ts": record.timestamp,
                    "fetch_status": record.status,
                    "content_mime_type": record.mime,
                    "sample_score": str(_stable_score(config, record)),
                    "batch": "pilot" if pilot else batch_int,
                }
            )

    sample_path = out_dir / f"cc_trend_warc_sample_{runid}.parquet"
    pd.DataFrame(rows).to_parquet(sample_path, index=False)
    logger.info("Wrote trend WARC sample %s rows=%d", sample_path, len(rows))
    return sample_path


def _latest_sample_path(config: Dict, *, batch: int | str, pilot: bool) -> Path:
    if pilot:
        roots = sorted((_trend_root(config) / "pilot").glob("*/sample"))
        paths = [path for root in roots for path in root.glob("cc_trend_warc_sample_*.parquet")]
    else:
        paths = list((_trend_batch_dir(config, batch) / "sample").glob("*.parquet"))
    if not paths:
        raise FileNotFoundError(
            "No trend WARC sample found. Run cc-trend-pilot or cc-trend-run-batch first."
        )
    return sorted(paths)[-1]


def _term_counts(config: Dict, text: str) -> Tuple[List[Dict[str, object]], int, int]:
    terms = _collection_cfg(config).get("terms", {})
    patterns = compile_patterns(terms)
    asd_pattern = compile_asd_pattern(terms)
    asd_window = int(
        _collection_cfg(config).get("filters", {}).get("asd_disambiguation_window_chars", 200)
    )
    rows: List[Dict[str, object]] = []
    target_total = 0
    baseline_total = 0

    for term in patterns:
        count = len(list(term.pattern.finditer(text)))
        if count <= 0:
            continue
        rows.append(
            {
                "term_role": term.term_role,
                "term_group": term.term_group,
                "matched_term": term.matched_term,
                "count": count,
            }
        )
        if term.term_role == "target":
            target_total += count
        elif term.term_role == "baseline":
            baseline_total += count

    if asd_pattern is not None:
        count = sum(
            1
            for match in asd_pattern.pattern.finditer(text)
            if asd_disambiguated(text, match.span(), asd_window)
        )
        if count > 0:
            rows.append(
                {
                    "term_role": asd_pattern.term_role,
                    "term_group": asd_pattern.term_group,
                    "matched_term": asd_pattern.matched_term,
                    "count": count,
                }
            )
            target_total += count
    return rows, target_total, baseline_total


def _publication_year(value: str) -> Optional[int]:
    parsed = pd.to_datetime(str(value or "").strip(), errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    return int(parsed.year)


def _limit_sample_by_source_year(frame: pd.DataFrame, max_docs: Optional[int]) -> pd.DataFrame:
    if max_docs is None or len(frame) <= max_docs:
        return frame.copy()
    if "source_year" not in frame.columns:
        return frame.head(max_docs).copy()

    groups = [
        (source_year, group)
        for source_year, group in frame.groupby("source_year", sort=True)
    ]
    if not groups:
        return frame.head(max_docs).copy()

    base = max_docs // len(groups)
    remainder = max_docs % len(groups)
    selected_frames: List[pd.DataFrame] = []
    for idx, (_, group) in enumerate(groups):
        limit = base + (1 if idx < remainder else 0)
        if limit <= 0:
            continue
        selected_frames.append(group.head(limit))
    if not selected_frames:
        return frame.head(max_docs).copy()
    return pd.concat(selected_frames, ignore_index=True)


def extract_trend_warc(config: Dict, *, batch: int | str, pilot: bool = False) -> Path:
    runid = _utc_runid()
    logger = _setup_logger(Path("reports/logs"), "cc-trend-extract-warc", runid)
    sample_path = _latest_sample_path(config, batch=batch, pilot=pilot)
    sample_df = pd.read_parquet(sample_path)
    trend_cfg = _trend_cfg(config)
    max_docs = (
        _safe_int(trend_cfg.get("pilot", {}).get("max_fetch_docs_total"))
        if pilot
        else _safe_int(trend_cfg.get("full", {}).get("max_fetch_docs_total"))
    )
    if max_docs is not None:
        before_counts = sample_df.groupby("source_year").size().to_dict()
        sample_df = _limit_sample_by_source_year(sample_df, max_docs)
        after_counts = sample_df.groupby("source_year").size().to_dict()
        logger.info(
            "Applied max_fetch_docs_total=%d to trend sample rows=%d source_year_counts=%s",
            max_docs,
            len(sample_df),
            {"before": before_counts, "after": after_counts},
        )

    extraction_cfg = _warc_extraction_config(
        {"collection_stage": _collection_cfg(config)}, "collection_stage"
    )
    tolerance_hours = int(_collection_cfg(config).get("published_ts_tolerance_hours", 48))
    min_date = str(_collection_cfg(config).get("published_ts_min_date", "1995-01-01")).strip()
    min_tokens = int(trend_cfg.get("min_extracted_tokens", 100))
    retain_text = bool(trend_cfg.get("retain_extracted_text", False))
    language_cfg = _collection_cfg(config).get("language", {})
    keep_languages = {
        str(value).strip()
        for value in trend_cfg.get("language_keep", language_cfg.get("keep", ["en"]))
    }
    language_max_chars = int(language_cfg.get("max_classification_chars", 50000))
    classify = _load_language_identifier()
    start_year, end_year = _publication_window(config)
    out_dir = (
        (_trend_pilot_dir(config, runid) if pilot else _trend_batch_dir(config, batch))
        / "extract"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, object]] = []
    for idx, row in enumerate(sample_df.to_dict(orient="records"), start=1):
        record = LookupRecord(
            url=str(row["url"]),
            crawl_id=str(row["crawl_id"]),
            filename=str(row["warc_filename"]),
            offset=int(row["warc_offset"]),
            length=int(row["warc_length"]),
            status=str(row.get("fetch_status", "")),
            mime=str(row.get("content_mime_type", "")),
            timestamp=str(row.get("capture_ts", "")),
        )
        out_row = dict(row)
        out_row.update(
            {
                "published_ts": "",
                "publication_year": pd.NA,
                "publication_date_status": "not_extracted",
                "extraction_status": "not_started",
                "extractor_used": "",
                "language_code": "",
                "language_score": 0.0,
                "language_error": "",
                "is_language_kept": False,
                "token_count": 0,
                "extracted_text_len": 0,
                "text_hash": "",
                "target_term_count": 0,
                "baseline_term_count": 0,
                "term_counts_json": "[]",
                "audit_snippet": "",
            }
        )
        try:
            with _DocumentTimeout(int(extraction_cfg.get("document_timeout_s", 300))):
                payload, note, *_ = _fetch_warc_payload(record, extraction_cfg=extraction_cfg)
                if payload is None:
                    out_row["extraction_status"] = note
                    rows.append(out_row)
                    continue
                html_text = str(payload.get("html_text", ""))
                capture_ts = str(payload.get("capture_ts", "") or row.get("capture_ts", ""))
                extracted_text, extract_note, extractor_used = _extract_main_content(
                    html_text, extraction_cfg=extraction_cfg
                )
                out_row["extractor_used"] = extractor_used
                if not extracted_text:
                    out_row["extraction_status"] = extract_note
                    rows.append(out_row)
                    continue
                published_ts = _extract_published_timestamp(
                    html_text=html_text,
                    url=record.url,
                    capture_ts=capture_ts,
                    tolerance_hours=tolerance_hours,
                    min_date=min_date,
                )
                publication_year = _publication_year(published_ts or "")
                if publication_year is None:
                    publication_status = "missing_or_unparseable_published_ts"
                elif publication_year < start_year:
                    publication_status = "published_before_window"
                elif publication_year > end_year:
                    publication_status = "published_after_window"
                else:
                    publication_status = "in_window"

                language = _classify_language_safely(
                    extracted_text,
                    classify,
                    max_chars=language_max_chars,
                )
                normalized_text = _normalize_text_for_dedup(extracted_text)
                token_count = len(re.findall(r"\b\w+\b", extracted_text))
                term_counts, target_count, baseline_count = _term_counts(config, extracted_text)
                out_row.update(
                    {
                        "capture_ts": capture_ts,
                        "published_ts": published_ts or "",
                        "publication_year": (
                            publication_year if publication_year is not None else pd.NA
                        ),
                        "publication_date_status": publication_status,
                        "extraction_status": (
                            "ok" if token_count >= min_tokens else "too_few_tokens"
                        ),
                        "language_code": str(language["language_code"]),
                        "language_score": float(language["language_score"]),
                        "language_error": str(language["language_error"]),
                        "is_language_kept": str(language["language_code"]) in keep_languages,
                        "token_count": token_count,
                        "extracted_text_len": len(extracted_text),
                        "text_hash": hashlib.sha256(normalized_text.encode("utf-8")).hexdigest(),
                        "target_term_count": target_count,
                        "baseline_term_count": baseline_count,
                        "term_counts_json": json.dumps(term_counts, sort_keys=True),
                        "audit_snippet": extracted_text[:500],
                    }
                )
                if retain_text:
                    out_row["extracted_text"] = extracted_text
        except _DocumentTimeoutError:
            out_row["extraction_status"] = "document_timeout"
        rows.append(out_row)
        if idx % 100 == 0 or idx == len(sample_df):
            logger.info("Trend WARC extraction progress %d/%d", idx, len(sample_df))

    out_path = out_dir / f"cc_trend_warc_documents_{runid}.parquet"
    pd.DataFrame(rows).to_parquet(out_path, index=False)
    logger.info("Wrote trend WARC extracted documents %s rows=%d", out_path, len(rows))
    _write_calibration(config, pd.DataFrame(rows))
    return out_path


def _trend_document_paths(config: Dict) -> List[Path]:
    return sorted((_trend_root(config)).glob("batch_*/extract/cc_trend_warc_documents_*.parquet"))


def _write_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_calibration(config: Dict, frame: pd.DataFrame) -> Path:
    out_dir = processed_trend_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, object]] = []
    total = len(frame)
    extracted = (
        frame["extraction_status"].eq("ok")
        if "extraction_status" in frame
        else pd.Series(dtype=bool)
    )
    in_window = frame.get("publication_date_status", pd.Series(dtype=str)).eq("in_window")
    kept_lang = frame.get("is_language_kept", pd.Series(dtype=bool)).fillna(False).astype(bool)
    retained = extracted & in_window & kept_lang
    rows.extend(
        [
            {"metric": "fetched_records", "value": total},
            {"metric": "extracted_ok_records", "value": int(extracted.sum())},
            {"metric": "in_window_publication_year_records", "value": int(in_window.sum())},
            {"metric": "language_kept_records", "value": int(kept_lang.sum())},
            {"metric": "retained_records_before_dedup", "value": int(retained.sum())},
            {
                "metric": "retained_tokens_before_dedup",
                "value": int(
                    pd.to_numeric(frame.loc[retained, "token_count"], errors="coerce")
                    .fillna(0)
                    .sum()
                ),
            },
        ]
    )
    if retained.any():
        token_series = pd.to_numeric(frame.loc[retained, "token_count"], errors="coerce").fillna(0)
        rows.extend(
            [
                {
                    "metric": "mean_retained_tokens_per_doc",
                    "value": round(float(token_series.mean()), 6),
                },
                {
                    "metric": "median_retained_tokens_per_doc",
                    "value": round(float(token_series.median()), 6),
                },
            ]
        )
        by_year = (
            frame.loc[retained]
            .assign(token_count=token_series)
            .groupby("publication_year", dropna=False)
            .agg(retained_docs=("url", "size"), retained_tokens=("token_count", "sum"))
            .reset_index()
        )
        for row in by_year.to_dict(orient="records"):
            rows.append(
                {
                    "metric": f"publication_year.{int(row['publication_year'])}.retained_docs",
                    "value": int(row["retained_docs"]),
                }
            )
            rows.append(
                {
                    "metric": f"publication_year.{int(row['publication_year'])}.retained_tokens",
                    "value": int(row["retained_tokens"]),
                }
            )
    out_path = out_dir / "trend_pilot_calibration.csv"
    _write_csv(out_path, rows)
    return out_path


def build_trend_warc(config: Dict) -> List[Path]:
    paths = _trend_document_paths(config)
    if not paths:
        raise FileNotFoundError("No trend WARC document shards found. Run make trend first.")
    frames = [pd.read_parquet(path).assign(source_shard=str(path)) for path in paths]
    frame = pd.concat(frames, ignore_index=True)
    start_year, end_year = _publication_window(config)
    min_tokens = int(_trend_cfg(config).get("min_extracted_tokens", 100))

    frame["publication_year"] = pd.to_numeric(frame["publication_year"], errors="coerce")
    frame["token_count"] = (
        pd.to_numeric(frame["token_count"], errors="coerce").fillna(0).astype(int)
    )
    frame["is_language_kept"] = frame["is_language_kept"].fillna(False).astype(bool)
    frame["keep_for_trend"] = (
        frame["extraction_status"].eq("ok")
        & frame["publication_year"].between(start_year, end_year)
        & frame["is_language_kept"]
        & frame["token_count"].ge(min_tokens)
    )

    exclusions = (
        frame.assign(
            exclusion_reason=frame.apply(
                lambda row: "kept"
                if bool(row["keep_for_trend"])
                else (
                    str(row["extraction_status"])
                    if str(row["extraction_status"]) != "ok"
                    else (
                        str(row["publication_date_status"])
                        if str(row["publication_date_status"]) != "in_window"
                        else (
                            "language_not_kept"
                            if not bool(row["is_language_kept"])
                            else "too_few_tokens"
                        )
                    )
                ),
                axis=1,
            )
        )
        .groupby("exclusion_reason", dropna=False)
        .agg(documents=("url", "size"), tokens=("token_count", "sum"))
        .reset_index()
    )

    kept = frame.loc[frame["keep_for_trend"]].copy()
    kept = kept.sort_values(
        ["normalized_url", "token_count", "capture_ts", "crawl_id", "warc_filename", "warc_offset"],
        ascending=[True, False, True, True, True, True],
    )
    before_url = len(kept)
    kept = kept.drop_duplicates(subset=["normalized_url"], keep="first")
    removed_by_url = before_url - len(kept)
    kept = kept.sort_values(
        ["text_hash", "token_count", "capture_ts", "crawl_id", "warc_filename", "warc_offset"],
        ascending=[True, False, True, True, True, True],
    )
    before_hash = len(kept)
    kept = kept.drop_duplicates(subset=["text_hash"], keep="first")
    removed_by_hash = before_hash - len(kept)

    out_dir = processed_trend_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    documents_path = out_dir / "trend_documents.parquet"
    kept.to_parquet(documents_path, index=False)

    term_rows: List[Dict[str, object]] = []
    for _, row in kept.iterrows():
        for term_count in json.loads(str(row.get("term_counts_json", "[]") or "[]")):
            term_rows.append(
                {
                    "publication_year": int(row["publication_year"]),
                    "term_role": str(term_count["term_role"]),
                    "term_group": str(term_count["term_group"]),
                    "matched_term": str(term_count["matched_term"]),
                    "count": int(term_count["count"]),
                }
            )
    term_df = pd.DataFrame(term_rows)
    denominator = (
        kept.groupby("publication_year", dropna=False)
        .agg(documents=("url", "size"), tokens=("token_count", "sum"))
        .reset_index()
    )
    rows: List[Dict[str, object]] = []
    rates_columns = [
        "year",
        "aggregation_level",
        "term_role",
        "term_group",
        "matched_term",
        "documents",
        "tokens",
        "term_count",
        "term_count_per_million_tokens",
    ]
    for den in denominator.to_dict(orient="records"):
        year = int(den["publication_year"])
        tokens = int(den["tokens"])
        docs = int(den["documents"])
        year_terms = (
            term_df.loc[term_df["publication_year"].eq(year)]
            if not term_df.empty
            else pd.DataFrame()
        )
        all_count = int(year_terms["count"].sum()) if not year_terms.empty else 0
        rows.append(
            {
                "year": year,
                "aggregation_level": "all",
                "term_role": "",
                "term_group": "",
                "matched_term": "",
                "documents": docs,
                "tokens": tokens,
                "term_count": all_count,
                "term_count_per_million_tokens": (
                    (all_count / tokens * 1_000_000) if tokens else 0.0
                ),
            }
        )
    if not term_df.empty:
        for level, columns in (
            ("term_role", ["publication_year", "term_role"]),
            ("term_group", ["publication_year", "term_role", "term_group"]),
            ("matched_term", ["publication_year", "term_role", "term_group", "matched_term"]),
        ):
            grouped = (
                term_df.groupby(columns, dropna=False)
                .agg(term_count=("count", "sum"))
                .reset_index()
            )
            grouped = grouped.merge(denominator, on="publication_year", how="left")
            for row in grouped.to_dict(orient="records"):
                tokens = int(row["tokens"])
                count = int(row["term_count"])
                rows.append(
                    {
                        "year": int(row["publication_year"]),
                        "aggregation_level": level,
                        "term_role": str(row.get("term_role", "") or ""),
                        "term_group": str(row.get("term_group", "") or ""),
                        "matched_term": str(row.get("matched_term", "") or ""),
                        "documents": int(row["documents"]),
                        "tokens": tokens,
                        "term_count": count,
                        "term_count_per_million_tokens": (
                            (count / tokens * 1_000_000) if tokens else 0.0
                        ),
                    }
                )
    rates_path = out_dir / "trend_rates.csv"
    pd.DataFrame(rows, columns=rates_columns).sort_values(
        ["year", "aggregation_level", "term_role", "term_group", "matched_term"]
    ).to_csv(rates_path, index=False)

    exclusions_path = out_dir / "trend_exclusions.csv"
    exclusions.to_csv(exclusions_path, index=False)
    dedup_path = out_dir / "trend_dedup_summary.csv"
    pd.DataFrame(
        [
            {"metric": "kept_before_url_dedup", "value": before_url},
            {"metric": "removed_by_normalized_url", "value": removed_by_url},
            {"metric": "removed_by_text_hash", "value": removed_by_hash},
            {"metric": "kept_after_dedup", "value": len(kept)},
        ]
    ).to_csv(dedup_path, index=False)
    diagnostics_path = out_dir / "trend_source_year_diagnostics.csv"
    (
        kept.groupby(["publication_year", "source_year"], dropna=False)
        .agg(documents=("url", "size"), tokens=("token_count", "sum"))
        .reset_index()
        .to_csv(diagnostics_path, index=False)
    )
    audit_path = out_dir / "trend_audit_sample.csv"
    sample_n = int(_trend_cfg(config).get("audit_sample_n_per_publication_year", 30))
    (
        kept.groupby("publication_year", group_keys=False)
        .apply(lambda group: group.head(sample_n))
        .reset_index(drop=True)
        .to_csv(audit_path, index=False)
    )
    _write_calibration(config, frame)
    return [
        documents_path,
        rates_path,
        exclusions_path,
        dedup_path,
        diagnostics_path,
        audit_path,
        out_dir / "trend_pilot_calibration.csv",
    ]


def run_trend_pilot(config: Dict) -> List[Path]:
    sample_trend_index(config, batch=1, pilot=True)
    extract_trend_warc(config, batch=1, pilot=True)
    return [processed_trend_dir(config) / "trend_pilot_calibration.csv"]


def run_trend_batch(config: Dict, *, batch: int | str) -> List[Path]:
    sample_trend_index(config, batch=batch, pilot=False)
    extract_trend_warc(config, batch=batch, pilot=False)
    return build_trend_warc(config)


def upload_trend_outputs(config: Dict, paths: Optional[List[Path]] = None) -> List[str]:
    if paths is None:
        out_dir = processed_trend_dir(config)
        paths = [
            out_dir / "trend_documents.parquet",
            out_dir / "trend_rates.csv",
            out_dir / "trend_exclusions.csv",
            out_dir / "trend_dedup_summary.csv",
            out_dir / "trend_source_year_diagnostics.csv",
            out_dir / "trend_audit_sample.csv",
            out_dir / "trend_pilot_calibration.csv",
        ]
    return _upload_paths_to_s3(paths, _processed_output_prefix(config, "trend"))
