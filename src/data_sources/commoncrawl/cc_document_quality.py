import hashlib
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from src.pathing import (
    stage1_output_dir,
    stage1e_document_quality_dir,
    stage1e_metrics_dir,
    stage1e_url_export_dir,
    stage1e_warc_dir,
)

from .cc_warc import (
    DOC_KEY_COLUMNS,
    STAGE1E_SCHEMA_NEGATIVE_TYPES,
    _build_stage1e_summary_rows,
    _compute_publication_date_metrics,
    _latest_enriched_hits,
    _load_metric_map,
    _metric_int,
    _require_stage1e,
    _setup_logger,
    _utc_runid,
    _write_summary_csv,
)


def _normalize_text_for_dedup(text: str) -> str:
    lowered = text.lower()
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def _token_ngrams(text: str, n: int) -> set[str]:
    tokens = re.findall(r"\b\w+\b", text.lower())
    if not tokens:
        return set()
    if len(tokens) < n:
        return {" ".join(tokens)}
    return {" ".join(tokens[idx : idx + n]) for idx in range(len(tokens) - n + 1)}


def _jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def _seed_for_run(project_seed: int, runid: str) -> int:
    digest = hashlib.sha256(f"{project_seed}:{runid}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def _load_language_identifier():
    try:
        import langid  # type: ignore

        return langid.classify
    except ImportError:
        try:
            from py3langid.langid import MODEL_FILE, LanguageIdentifier  # type: ignore
        except ImportError as exc:  # pragma: no cover - dependency dependent
            raise RuntimeError(
                "Missing language-ID dependency. Install 'langid' or 'py3langid' "
                "before running Stage 1e document-quality filtering."
            ) from exc
        identifier = LanguageIdentifier.from_pickled_model(MODEL_FILE, norm_probs=True)
        return identifier.classify


def _load_datatrove_quality_filters(datatrove_cfg: Dict):
    try:
        from datatrove.data import Document
        from datatrove.pipeline.filters.fineweb_quality_filter import FineWebQualityFilter
        from datatrove.pipeline.filters.gopher_quality_filter import GopherQualityFilter
        from datatrove.pipeline.filters.gopher_repetition_filter import GopherRepetitionFilter
    except ImportError as exc:  # pragma: no cover - dependency dependent
        raise RuntimeError(
            "Missing DataTrove dependency. Install 'datatrove==0.9.0' in the msc-nlp "
            "environment before running Stage 1e document quality filtering."
        ) from exc

    filter_specs = [
        (
            "gopher_repetition",
            GopherRepetitionFilter,
            datatrove_cfg.get("gopher_repetition", {}),
        ),
        ("gopher_quality", GopherQualityFilter, datatrove_cfg.get("gopher_quality", {})),
        ("fineweb_quality", FineWebQualityFilter, datatrove_cfg.get("fineweb_quality", {})),
    ]
    filters = []
    for name, cls, kwargs in filter_specs:
        enabled = bool(datatrove_cfg.get(name, {}).get("enabled", True))
        if not enabled:
            continue
        clean_kwargs = {
            key: value
            for key, value in dict(kwargs).items()
            if key != "enabled" and value is not None
        }
        filters.append((name, cls(**clean_kwargs)))
    return Document, filters


def _split_sentences_with_spans(text: str) -> List[Tuple[int, int, str]]:
    spans: List[Tuple[int, int, str]] = []
    for match in re.finditer(r"[^.!?]+(?:[.!?]+|$)", text):
        start, end = match.span()
        while start < end and text[start].isspace():
            start += 1
        while end > start and text[end - 1].isspace():
            end -= 1
        if start >= end:
            continue
        spans.append((start, end, text[start:end]))
    return spans


def _token_count(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


def _normalized_schema_types(raw_value: object) -> set[str]:
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        return set()
    return {token.strip().lower() for token in raw_text.split("|") if token and token.strip()}


def _compile_term_patterns(
    term_patterns: Sequence[str], matched_terms: Sequence[str]
) -> List[re.Pattern[str]]:
    compiled: List[re.Pattern[str]] = []
    for pattern_text in term_patterns:
        normalized = str(pattern_text).strip()
        if not normalized:
            continue
        try:
            compiled.append(re.compile(normalized, re.IGNORECASE))
        except re.error:
            continue
    if compiled:
        return compiled
    fallback_terms = [re.escape(str(term).strip()) for term in matched_terms if str(term).strip()]
    return [re.compile(term, re.IGNORECASE) for term in fallback_terms]


def _first_matching_sentence_index(
    sentences: Sequence[Tuple[int, int, str]], term_patterns: Sequence[re.Pattern[str]]
) -> Optional[int]:
    for idx, (_, _, sentence) in enumerate(sentences):
        if any(pattern.search(sentence) for pattern in term_patterns):
            return idx
    return None


def _build_sentence_window_snippet(
    text: str,
    term_patterns: Sequence[re.Pattern[str]],
    target_chars: int,
    max_chars: int,
) -> str:
    sentences = _split_sentences_with_spans(text)
    first_match = None
    for pattern in term_patterns:
        first_match = pattern.search(text)
        if first_match:
            break
    if not sentences:
        if not first_match:
            return ""
        return _centered_fallback_snippet(text, first_match.span(), max_chars)
    center_idx = _first_matching_sentence_index(sentences, term_patterns)
    if center_idx is None:
        if not first_match:
            return ""
        return _centered_fallback_snippet(text, first_match.span(), max_chars)

    selected = {center_idx}
    total_chars = len(sentences[center_idx][2])
    left = center_idx - 1
    right = center_idx + 1
    take_left = True

    while total_chars < target_chars and (left >= 0 or right < len(sentences)):
        if take_left and left >= 0:
            selected.add(left)
            total_chars += len(sentences[left][2])
            left -= 1
        elif right < len(sentences):
            selected.add(right)
            total_chars += len(sentences[right][2])
            right += 1
        elif left >= 0:
            selected.add(left)
            total_chars += len(sentences[left][2])
            left -= 1
        take_left = not take_left

    ordered = [sentences[idx][2].strip() for idx in sorted(selected)]
    snippet = " ".join(part for part in ordered if part)
    if max_chars > 0 and len(snippet) > max_chars and first_match:
        return _centered_fallback_snippet(text, first_match.span(), max_chars)
    return snippet


def _centered_fallback_snippet(text: str, span: Tuple[int, int], max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return " ".join(text.split())
    center = span[0] + ((span[1] - span[0]) // 2)
    start = max(0, center - (max_chars // 2))
    end = min(len(text), start + max_chars)
    start = max(0, end - max_chars)
    return " ".join(text[start:end].split())


def _doc_substantiveness_passes(
    extracted_text: str,
    term_patterns: Sequence[re.Pattern[str]],
    sentence_min_tokens: int,
    sentence_min_chars: int,
    substantive_sentence_min_tokens: int,
    doc_min_substantive_sentences: int,
    min_extracted_text_chars: int,
) -> bool:
    if not extracted_text:
        return False
    sentences = _split_sentences_with_spans(extracted_text)
    if not sentences:
        return False

    matched_sentence_ok = False
    substantive_sentence_count = 0
    for _, _, sentence in sentences:
        token_count = _token_count(sentence)
        if token_count >= substantive_sentence_min_tokens:
            substantive_sentence_count += 1
        if any(pattern.search(sentence) for pattern in term_patterns):
            if token_count >= sentence_min_tokens and len(sentence) >= sentence_min_chars:
                matched_sentence_ok = True

    doc_is_substantive = (
        substantive_sentence_count >= doc_min_substantive_sentences
        or len(extracted_text) >= min_extracted_text_chars
    )
    return matched_sentence_ok and doc_is_substantive


def _schema_negative_passes(schema_types: str, negative_schema_types: set[str]) -> bool:
    normalized_types = _normalized_schema_types(schema_types)
    if not normalized_types.intersection(negative_schema_types):
        return False

    # Avoid rejecting otherwise valid articles that contain nested item lists or products.
    article_like_types = {
        "article",
        "blogposting",
        "newsarticle",
        "report",
        "scholarlyarticle",
        "socialmediaposting",
        "techarticle",
    }
    if normalized_types.intersection(article_like_types):
        return False
    return True


def _latest_by_runid(directory: Path, prefix: str, suffix: str) -> Tuple[str, Path]:
    latest_runid = ""
    latest_path: Optional[Path] = None
    for path in directory.glob(f"{prefix}*{suffix}"):
        runid = path.stem.removeprefix(prefix).removesuffix(suffix.removeprefix("."))
        if runid > latest_runid:
            latest_runid = runid
            latest_path = path
    if latest_path is None:
        raise FileNotFoundError(f"No {prefix}*{suffix} found in {directory}")
    return latest_runid, latest_path


def _latest_metric_map(directory: Path, prefix: str, suffix: str) -> Tuple[str, Dict[str, object]]:
    runid, path = _latest_by_runid(directory, prefix, suffix)
    return runid, _load_metric_map(path)


def _latest_metric_map_or_empty(
    directory: Path, prefix: str, suffix: str
) -> Tuple[str, Dict[str, object]]:
    try:
        return _latest_metric_map(directory, prefix, suffix)
    except FileNotFoundError:
        return "", {}


def _write_stage1e_term_summary(path: Path, df: pd.DataFrame) -> None:
    if df.empty:
        pd.DataFrame(
            columns=[
                "crawl_id",
                "term_role",
                "term_group",
                "matched_term",
                "validated_hits_wet",
                "validated_hits_warc",
                "document_quality_kept_hits",
                "english_hits",
                "dedup_representative_hits",
            ]
        ).to_csv(path, index=False)
        return

    summary_df = (
        df.groupby(["crawl_id", "term_role", "term_group", "matched_term"], dropna=False)
        .agg(
            validated_hits_wet=("matched_term", "size"),
            validated_hits_warc=("is_validated_hits_warc", "sum"),
            document_quality_kept_hits=("stage1e_passes_document_quality", "sum"),
            english_hits=("is_english", "sum"),
            dedup_representative_hits=("is_dedup_representative", "sum"),
        )
        .reset_index()
    )
    summary_df.to_csv(path, index=False)


def _metric_float(metrics: Dict[str, object], name: str, default: float = 0.0) -> float:
    try:
        value = metrics.get(name, default)
        if value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_rate(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _write_stage1e_throughput_summary(
    path: Path,
    *,
    config: Dict,
    runid: str,
    document_quality_summary: Dict[str, object],
    document_quality_elapsed_sec: float,
) -> None:
    _, wet_summary = _latest_metric_map_or_empty(
        stage1_output_dir(config, default_stage="stage1e"), "cc_scan_summary_", ".csv"
    )
    _, url_summary = _latest_metric_map_or_empty(
        stage1e_url_export_dir(config), "cc_stage1e_urls_summary_", ".csv"
    )
    _, warc_summary = _latest_metric_map_or_empty(
        stage1e_warc_dir(config), "cc_stage1e_summary_", ".csv"
    )

    wet_docs = _metric_int(wet_summary, "docs_scanned", 0)
    wet_elapsed = _metric_float(wet_summary, "total_elapsed_sec")
    url_docs = _metric_int(url_summary, "doc_count_total", 0)
    warc_docs = _metric_int(warc_summary, "doc_count_input", 0)
    warc_fetch_success = _metric_int(warc_summary, "doc_count_fetch_success", 0)
    warc_extract_success = _metric_int(warc_summary, "doc_count_extract_success", 0)
    warc_elapsed = _metric_float(warc_summary, "total_elapsed_sec")
    final_docs = _metric_int(document_quality_summary, "doc_count_dedup_representatives", 0)
    document_quality_input = _metric_int(document_quality_summary, "doc_count_warc_validated", 0)
    total_observed_elapsed_sec = wet_elapsed + warc_elapsed + document_quality_elapsed_sec

    rows = [
        ["metric", "value"],
        ["runid", runid],
        ["wet.docs_scanned", wet_docs],
        ["wet.elapsed_sec", round(wet_elapsed, 6)],
        ["wet.docs_per_sec", round(_safe_rate(wet_docs, wet_elapsed), 6)],
        ["url_export.doc_count_total", url_docs],
        ["warc.doc_count_input", warc_docs],
        ["warc.elapsed_sec", round(warc_elapsed, 6)],
        ["warc.docs_per_sec", round(_safe_rate(warc_docs, warc_elapsed), 6)],
        [
            "warc.fetch_success_rate_pct",
            round(_safe_rate(warc_fetch_success, warc_docs) * 100.0, 6),
        ],
        [
            "warc.extract_success_rate_pct",
            round(_safe_rate(warc_extract_success, warc_docs) * 100.0, 6),
        ],
        ["document_quality.doc_count_input", document_quality_input],
        ["document_quality.elapsed_sec", round(document_quality_elapsed_sec, 6)],
        [
            "document_quality.docs_per_sec",
            round(_safe_rate(document_quality_input, document_quality_elapsed_sec), 6),
        ],
        ["final.doc_count", final_docs],
        ["total_observed_elapsed_sec", round(total_observed_elapsed_sec, 6)],
        [
            "total_observed_docs_per_sec",
            round(_safe_rate(wet_docs, total_observed_elapsed_sec), 6),
        ],
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        pd.DataFrame(rows[1:], columns=rows[0]).to_csv(handle, index=False)

def _datatrove_result_parts(result: object) -> Tuple[bool, str]:
    if isinstance(result, tuple):
        keep = bool(result[0])
        reason = str(result[1]) if len(result) > 1 and result[1] else ""
        return keep, reason
    return bool(result), ""


def _evaluate_datatrove_quality_filters(
    text: str, doc_id: str, url: str, filters: Sequence[Tuple[str, object]], document_cls
) -> Dict[str, object]:
    status: Dict[str, object] = {
        "stage1e_datatrove_filter_name": "",
        "stage1e_datatrove_filter_reason": "",
        "passes_gopher_repetition": True,
        "passes_gopher_quality": True,
        "passes_fineweb_quality": True,
    }
    doc = document_cls(text=text, id=doc_id, metadata={"url": url})
    for filter_name, quality_filter in filters:
        try:
            keep, reason = _datatrove_result_parts(quality_filter.filter(doc))
        except (AssertionError, ZeroDivisionError, ValueError) as exc:
            keep = False
            reason = f"filter_error_{type(exc).__name__.lower()}"
        status[f"passes_{filter_name}"] = keep
        if not keep and not status["stage1e_datatrove_filter_name"]:
            status["stage1e_datatrove_filter_name"] = filter_name
            status["stage1e_datatrove_filter_reason"] = reason or "failed"
    return status


def document_quality_stage1e_hits(config: Dict) -> Path:
    _require_stage1e(config)
    filter_start = time.perf_counter()
    runid = _utc_runid()
    logger = _setup_logger(Path("reports/logs"), "cc-stage1e-document-quality", runid)

    enrich_dir = stage1e_warc_dir(config)
    enrich_runid, enriched_path = _latest_enriched_hits(enrich_dir)
    enriched_df = pd.read_parquet(enriched_path)
    if enriched_df.empty:
        raise ValueError("Latest Stage 1e enriched hits table is empty.")

    quality_cfg = config.get("stage1e", {}).get("document_quality", {})
    negative_schema_types = {
        str(value).strip().lower()
        for value in quality_cfg.get("schema_negative_types", STAGE1E_SCHEMA_NEGATIVE_TYPES)
        if str(value).strip()
    }
    sentence_min_tokens = int(quality_cfg.get("sentence_min_tokens", 6))
    sentence_min_chars = int(quality_cfg.get("sentence_min_chars", 40))
    substantive_sentence_min_tokens = int(quality_cfg.get("substantive_sentence_min_tokens", 8))
    doc_min_substantive_sentences = int(quality_cfg.get("doc_min_substantive_sentences", 3))
    min_extracted_text_chars = int(quality_cfg.get("min_extracted_text_chars", 500))
    snippet_target_chars = int(quality_cfg.get("snippet_target_chars", 200))
    snippet_max_chars = int(quality_cfg.get("snippet_max_chars", 600))
    sample_n = int(quality_cfg.get("validation_sample_n", 30))
    Document, datatrove_filters = _load_datatrove_quality_filters(
        quality_cfg.get("datatrove", {})
    )

    validated_mask = enriched_df["is_validated_hits_warc"].fillna(False).astype(bool)
    validated_df = enriched_df.loc[validated_mask].copy()
    if validated_df.empty:
        raise ValueError(
            "No WARC-validated rows available for Stage 1e document-quality filtering."
        )

    grouped_docs = (
        validated_df.groupby(DOC_KEY_COLUMNS, dropna=False)
        .agg(
            registered_domain=("registered_domain", "first"),
            source_stage=("source_stage", "first"),
            source_scope=("source_scope", "first"),
            capture_ts=("capture_ts", "first"),
            published_ts=("published_ts", "first"),
            schema_types=("schema_types", "first"),
            extracted_text=("extracted_text", "first"),
            extracted_text_len=("extracted_text_len", "first"),
            matched_terms=(
                "matched_term",
                lambda series: sorted({str(v) for v in series if str(v).strip()}),
            ),
            term_roles=(
                "term_role",
                lambda series: sorted({str(v) for v in series if str(v).strip()}),
            ),
            term_patterns=(
                "term_pattern",
                lambda series: sorted({str(v) for v in series if str(v).strip()}),
            ),
        )
        .reset_index()
    )

    doc_rows: List[Dict[str, object]] = []
    for _, doc_row in grouped_docs.iterrows():
        matched_terms = list(doc_row["matched_terms"])
        term_roles = list(doc_row["term_roles"])
        term_patterns = list(doc_row["term_patterns"])
        extracted_text = str(doc_row["extracted_text"] or "")
        schema_types = str(doc_row["schema_types"] or "")
        compiled_term_patterns = _compile_term_patterns(term_patterns, matched_terms)

        schema_negative = _schema_negative_passes(schema_types, negative_schema_types)
        datatrove_status = _evaluate_datatrove_quality_filters(
            extracted_text,
            doc_id=f"{doc_row['crawl_id']}:{doc_row['url']}",
            url=str(doc_row["url"]),
            filters=datatrove_filters,
            document_cls=Document,
        )
        substantive_ok = _doc_substantiveness_passes(
            extracted_text=extracted_text,
            term_patterns=compiled_term_patterns,
            sentence_min_tokens=sentence_min_tokens,
            sentence_min_chars=sentence_min_chars,
            substantive_sentence_min_tokens=substantive_sentence_min_tokens,
            doc_min_substantive_sentences=doc_min_substantive_sentences,
            min_extracted_text_chars=min_extracted_text_chars,
        )
        datatrove_filter_name = str(datatrove_status["stage1e_datatrove_filter_name"])
        datatrove_filter_reason = str(datatrove_status["stage1e_datatrove_filter_reason"])
        document_quality_reason = ""
        passes_document_quality = True
        if schema_negative:
            document_quality_reason = "schema_page_type"
            passes_document_quality = False
        elif datatrove_filter_name:
            document_quality_reason = f"{datatrove_filter_name}_{datatrove_filter_reason}"
            passes_document_quality = False
        elif not substantive_ok:
            document_quality_reason = "substantive_context"
            passes_document_quality = False

        doc_rows.append(
            {
                "crawl_id": doc_row["crawl_id"],
                "url": doc_row["url"],
                "matched_terms": "|".join(matched_terms),
                "term_roles": "|".join(term_roles),
                "stage1e_schema_negative": schema_negative,
                "stage1e_substantive_context_ok": substantive_ok,
                "passes_gopher_repetition": bool(datatrove_status["passes_gopher_repetition"]),
                "passes_gopher_quality": bool(datatrove_status["passes_gopher_quality"]),
                "passes_fineweb_quality": bool(datatrove_status["passes_fineweb_quality"]),
                "stage1e_quality_filter_name": datatrove_filter_name,
                "stage1e_quality_filter_reason": datatrove_filter_reason,
                "stage1e_passes_document_quality": passes_document_quality,
                "stage1e_document_quality_reason": document_quality_reason,
                "doc_context_snippet_sentence_200": _build_sentence_window_snippet(
                    extracted_text,
                    compiled_term_patterns,
                    snippet_target_chars,
                    snippet_max_chars,
                ),
            }
        )

    doc_annotations = pd.DataFrame(doc_rows)
    filtered_df = enriched_df.merge(doc_annotations, on=DOC_KEY_COLUMNS, how="left")
    for column in (
        "matched_terms",
        "term_roles",
        "stage1e_quality_filter_name",
        "stage1e_quality_filter_reason",
        "stage1e_document_quality_reason",
        "doc_context_snippet_sentence_200",
    ):
        filtered_df[column] = filtered_df[column].fillna("")
    for column in (
        "stage1e_schema_negative",
        "stage1e_substantive_context_ok",
        "passes_gopher_repetition",
        "passes_gopher_quality",
        "passes_fineweb_quality",
        "stage1e_passes_document_quality",
    ):
        filtered_df[column] = filtered_df[column].fillna(False).astype(bool)

    row_snippets: List[str] = []
    for _, row in filtered_df.iterrows():
        if not bool(row.get("is_validated_hits_warc", False)):
            row_snippets.append("")
            continue
        extracted_text = str(row.get("extracted_text", "") or "")
        if not extracted_text:
            row_snippets.append("")
            continue
        compiled_term_patterns = _compile_term_patterns(
            [str(row.get("term_pattern", "") or "")],
            [str(row.get("matched_term", "") or "")],
        )
        row_snippets.append(
            _build_sentence_window_snippet(
                extracted_text,
                compiled_term_patterns,
                snippet_target_chars,
                snippet_max_chars,
            )
        )
    filtered_df["context_snippet_sentence_200"] = row_snippets

    cleanup_docs = filtered_df.loc[
        filtered_df["is_validated_hits_warc"].fillna(False)
        & filtered_df["stage1e_passes_document_quality"].fillna(False),
        DOC_KEY_COLUMNS + ["extracted_text"],
    ].drop_duplicates(subset=DOC_KEY_COLUMNS)
    if cleanup_docs.empty:
        raise ValueError("No Stage 1e documents survived document-quality filtering.")

    classify = _load_language_identifier()
    language_rows: List[Dict[str, object]] = []
    for _, doc_row in cleanup_docs.iterrows():
        extracted_text = str(doc_row["extracted_text"])
        language_code, language_score = classify(extracted_text)
        language_rows.append(
            {
                "crawl_id": doc_row["crawl_id"],
                "url": doc_row["url"],
                "language_code": language_code,
                "language_score": float(language_score),
                "is_english": language_code == "en",
                "normalized_text": _normalize_text_for_dedup(extracted_text),
            }
        )

    language_df = pd.DataFrame(language_rows)
    dedup_cfg = config.get("stage1e", {}).get("dedup", {})
    ngram_size = int(dedup_cfg.get("ngram_size", 5))
    jaccard_threshold = float(dedup_cfg.get("jaccard_threshold", 0.9))
    english_docs = language_df.loc[language_df["is_english"]].copy()
    representative_docs: List[Dict[str, object]] = []
    clusters: List[Dict[str, object]] = []

    for _, doc_row in english_docs.sort_values(
        by=["normalized_text"], key=lambda series: series.str.len(), ascending=False
    ).iterrows():
        normalized_text = str(doc_row["normalized_text"])
        cluster_id = ""
        dedup_reason = ""
        is_duplicate = False
        is_representative = False
        exact_hash = hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()
        ngrams = _token_ngrams(normalized_text, ngram_size)

        for cluster in clusters:
            if exact_hash == cluster["exact_hash"]:
                cluster_id = str(cluster["cluster_id"])
                dedup_reason = "exact"
                is_duplicate = True
                break
            similarity = _jaccard_similarity(ngrams, cluster["ngrams"])
            if similarity >= jaccard_threshold:
                cluster_id = str(cluster["cluster_id"])
                dedup_reason = "near"
                is_duplicate = True
                break

        if not cluster_id:
            cluster_id = f"cluster_{len(clusters) + 1:04d}"
            dedup_reason = "representative"
            is_representative = True
            clusters.append(
                {
                    "cluster_id": cluster_id,
                    "exact_hash": exact_hash,
                    "ngrams": ngrams,
                }
            )

        representative_docs.append(
            {
                "crawl_id": doc_row["crawl_id"],
                "url": doc_row["url"],
                "normalized_text_hash": exact_hash,
                "dedup_cluster_id": cluster_id,
                "dedup_reason": dedup_reason,
                "is_duplicate": is_duplicate,
                "is_dedup_representative": is_representative,
            }
        )

    dedup_df = pd.DataFrame(
        representative_docs,
        columns=[
            "crawl_id",
            "url",
            "normalized_text_hash",
            "dedup_cluster_id",
            "dedup_reason",
            "is_duplicate",
            "is_dedup_representative",
        ],
    )

    doc_annotations = language_df.merge(dedup_df, on=DOC_KEY_COLUMNS, how="left")
    filtered_df = filtered_df.merge(doc_annotations, on=DOC_KEY_COLUMNS, how="left")
    for column, fallback in (
        ("language_code", ""),
        ("normalized_text_hash", ""),
        ("dedup_cluster_id", ""),
        ("dedup_reason", ""),
    ):
        filtered_df[column] = filtered_df[column].fillna(fallback)
    filtered_df["language_score"] = pd.to_numeric(
        filtered_df["language_score"], errors="coerce"
    ).fillna(0.0)
    filtered_df["is_english"] = filtered_df["is_english"].fillna(False).astype(bool)
    filtered_df["is_duplicate"] = filtered_df["is_duplicate"].fillna(False).astype(bool)
    filtered_df["is_dedup_representative"] = (
        filtered_df["is_dedup_representative"].fillna(False).astype(bool)
    )

    corpus_df = (
        filtered_df.loc[
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["stage1e_passes_document_quality"].fillna(False)
            & filtered_df["is_english"]
            & filtered_df["is_dedup_representative"]
        ]
        .groupby(DOC_KEY_COLUMNS, dropna=False)
        .agg(
            registered_domain=("registered_domain", "first"),
            source_stage=("source_stage", "first"),
            source_scope=("source_scope", "first"),
            capture_ts=("capture_ts", "first"),
            published_ts=("published_ts", "first"),
            schema_types=("schema_types", "first"),
            extracted_text=("extracted_text", "first"),
            extracted_text_len=("extracted_text_len", "first"),
            matched_terms=("matched_terms", "first"),
            term_roles=("term_roles", "first"),
            dedup_cluster_id=("dedup_cluster_id", "first"),
            context_snippet_sentence_200=("doc_context_snippet_sentence_200", "first"),
        )
        .reset_index()
    )

    out_dir = stage1e_document_quality_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    filtered_path = out_dir / f"cc_document_quality_hits_{runid}.parquet"
    corpus_path = out_dir / f"cc_corpus_texts_document_quality_{runid}.parquet"
    sample_path = out_dir / f"cc_val_sample{sample_n}_{runid}.csv"
    summary_path = out_dir / f"cc_stage1e_summary_{runid}.csv"
    term_summary_path = out_dir / f"cc_stage1e_term_summary_{runid}.csv"

    filtered_df.to_parquet(filtered_path, index=False)
    corpus_df.to_parquet(corpus_path, index=False)

    project_seed = int(config.get("project", {}).get("seed", 123))
    sample_seed = _seed_for_run(project_seed, runid)
    sample_doc_n = min(sample_n, len(corpus_df))
    sample_columns = [
        "matched_terms",
        "term_roles",
        "registered_domain",
        "url",
        "crawl_id",
        "source_stage",
        "source_scope",
        "capture_ts",
        "published_ts",
        "schema_types",
        "extracted_text_len",
        "dedup_cluster_id",
        "context_snippet_sentence_200",
        "extracted_text",
    ]
    corpus_df[sample_columns].sample(n=sample_doc_n, random_state=sample_seed).to_csv(
        sample_path, index=False
    )

    _write_stage1e_term_summary(term_summary_path, filtered_df)

    validated_hits_wet_total = int(len(filtered_df))
    validated_hits_warc_total = int(filtered_df["is_validated_hits_warc"].sum())
    quality_kept_hits_total = int(
        (
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["stage1e_passes_document_quality"].fillna(False)
        ).sum()
    )
    english_hits_total = int(
        (
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["stage1e_passes_document_quality"].fillna(False)
            & filtered_df["is_english"].fillna(False)
        ).sum()
    )
    dedup_rep_hits_total = int(
        (
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["stage1e_passes_document_quality"].fillna(False)
            & filtered_df["is_english"].fillna(False)
            & filtered_df["is_dedup_representative"].fillna(False)
        ).sum()
    )

    role_counts: Dict[str, Dict[str, int]] = {}
    for role in ("target", "baseline"):
        role_df = filtered_df.loc[filtered_df["term_role"] == role]
        role_counts[role] = {
            "validated_hits_wet": int(len(role_df)),
            "validated_hits_warc": int(role_df["is_validated_hits_warc"].sum()),
        }

    summary_in_path = enrich_dir / f"cc_stage1e_summary_{enrich_runid}.csv"
    summary_in = _load_metric_map(summary_in_path) if summary_in_path.exists() else {}
    docs_scanned = _metric_int(summary_in, "docs_scanned", 0)
    candidate_hits = _metric_int(summary_in, "candidate_hits", 0)

    validated_docs = filtered_df.loc[
        filtered_df["is_validated_hits_warc"].fillna(False), DOC_KEY_COLUMNS
    ].drop_duplicates()
    quality_docs_count = int(
        filtered_df.loc[
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["stage1e_passes_document_quality"].fillna(False),
            DOC_KEY_COLUMNS,
        ]
        .drop_duplicates()
        .shape[0]
    )
    english_docs_count = int(
        filtered_df.loc[
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["stage1e_passes_document_quality"].fillna(False)
            & filtered_df["is_english"].fillna(False),
            DOC_KEY_COLUMNS,
        ]
        .drop_duplicates()
        .shape[0]
    )
    removed_schema_docs = int(
        filtered_df.loc[
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["stage1e_document_quality_reason"].eq("schema_page_type"),
            DOC_KEY_COLUMNS,
        ]
        .drop_duplicates()
        .shape[0]
    )
    removed_gopher_repetition_docs = int(
        filtered_df.loc[
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["stage1e_document_quality_reason"].str.startswith(
                "gopher_repetition_", na=False
            ),
            DOC_KEY_COLUMNS,
        ]
        .drop_duplicates()
        .shape[0]
    )
    removed_gopher_quality_docs = int(
        filtered_df.loc[
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["stage1e_document_quality_reason"].str.startswith(
                "gopher_quality_", na=False
            ),
            DOC_KEY_COLUMNS,
        ]
        .drop_duplicates()
        .shape[0]
    )
    removed_fineweb_quality_docs = int(
        filtered_df.loc[
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["stage1e_document_quality_reason"].str.startswith(
                "fineweb_quality_", na=False
            ),
            DOC_KEY_COLUMNS,
        ]
        .drop_duplicates()
        .shape[0]
    )
    removed_substantive_docs = int(
        filtered_df.loc[
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["stage1e_document_quality_reason"].eq("substantive_context"),
            DOC_KEY_COLUMNS,
        ]
        .drop_duplicates()
        .shape[0]
    )
    reason_doc_counts = (
        filtered_df.loc[
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["stage1e_document_quality_reason"].ne(""),
            DOC_KEY_COLUMNS + ["stage1e_document_quality_reason"],
        ]
        .drop_duplicates()
        .groupby("stage1e_document_quality_reason", dropna=False)
        .size()
        .to_dict()
    )
    reason_hit_counts = (
        filtered_df.loc[
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["stage1e_document_quality_reason"].ne(""),
            "stage1e_document_quality_reason",
        ]
        .value_counts()
        .to_dict()
    )

    doc_metrics = {
        "doc_count_warc_validated": int(validated_docs.shape[0]),
        "doc_count_document_quality_kept": quality_docs_count,
        "doc_count_english": english_docs_count,
        "doc_count_dedup_representatives": int(corpus_df.shape[0]),
        "term_role.target.dedup_representative_hits": int(
            (
                filtered_df["term_role"].eq("target")
                & filtered_df["is_validated_hits_warc"].fillna(False)
                & filtered_df["stage1e_passes_document_quality"].fillna(False)
                & filtered_df["is_english"].fillna(False)
                & filtered_df["is_dedup_representative"].fillna(False)
            ).sum()
        ),
        "term_role.baseline.dedup_representative_hits": int(
            (
                filtered_df["term_role"].eq("baseline")
                & filtered_df["is_validated_hits_warc"].fillna(False)
                & filtered_df["stage1e_passes_document_quality"].fillna(False)
                & filtered_df["is_english"].fillna(False)
                & filtered_df["is_dedup_representative"].fillna(False)
            ).sum()
        ),
        "total_elapsed_sec": round(time.perf_counter() - filter_start, 6),
        "removed_by_stage1e_schema_type_docs": removed_schema_docs,
        "removed_by_stage1e_gopher_repetition_docs": removed_gopher_repetition_docs,
        "removed_by_stage1e_gopher_quality_docs": removed_gopher_quality_docs,
        "removed_by_stage1e_fineweb_quality_docs": removed_fineweb_quality_docs,
        "removed_by_stage1e_substantive_context_docs": removed_substantive_docs,
        "removed_by_stage1e_schema_type_hits": int(
            (
                filtered_df["is_validated_hits_warc"].fillna(False)
                & filtered_df["stage1e_document_quality_reason"].eq("schema_page_type")
            ).sum()
        ),
        "removed_by_stage1e_gopher_repetition_hits": int(
            (
                filtered_df["is_validated_hits_warc"].fillna(False)
                & filtered_df["stage1e_document_quality_reason"].str.startswith(
                    "gopher_repetition_", na=False
                )
            ).sum()
        ),
        "removed_by_stage1e_gopher_quality_hits": int(
            (
                filtered_df["is_validated_hits_warc"].fillna(False)
                & filtered_df["stage1e_document_quality_reason"].str.startswith(
                    "gopher_quality_", na=False
                )
            ).sum()
        ),
        "removed_by_stage1e_fineweb_quality_hits": int(
            (
                filtered_df["is_validated_hits_warc"].fillna(False)
                & filtered_df["stage1e_document_quality_reason"].str.startswith(
                    "fineweb_quality_", na=False
                )
            ).sum()
        ),
        "removed_by_stage1e_substantive_context_hits": int(
            (
                filtered_df["is_validated_hits_warc"].fillna(False)
                & filtered_df["stage1e_document_quality_reason"].eq("substantive_context")
            ).sum()
        ),
        "removed_by_lang_hits": int(quality_kept_hits_total - english_hits_total),
        "removed_by_dedup_hits": int(english_hits_total - dedup_rep_hits_total),
        "removed_by_lang_docs": int(quality_docs_count - english_docs_count),
        "removed_by_dedup_docs": int(english_docs_count - len(corpus_df)),
    }
    for reason in sorted(reason_doc_counts):
        safe_reason = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(reason))
        doc_metrics[f"removed_by_stage1e_document_quality_reason.{safe_reason}.docs"] = int(
            reason_doc_counts[reason]
        )
        doc_metrics[f"removed_by_stage1e_document_quality_reason.{safe_reason}.hits"] = int(
            reason_hit_counts.get(reason, 0)
        )
    publication_doc_df = (
        filtered_df[
            DOC_KEY_COLUMNS + ["warc_fetch_success", "is_validated_hits_warc", "published_ts"]
        ]
        .drop_duplicates(subset=DOC_KEY_COLUMNS)
        .reset_index(drop=True)
    )
    publication_metrics = _compute_publication_date_metrics(publication_doc_df)

    stage1e_summary_rows = _build_stage1e_summary_rows(
        docs_scanned=docs_scanned,
        candidate_hits=candidate_hits,
        validated_hits_wet=validated_hits_wet_total,
        validated_hits_warc=validated_hits_warc_total,
        role_counts=role_counts,
        doc_metrics=doc_metrics,
        publication_metrics=publication_metrics,
        notes=(
            "Stage 1e document-quality outputs after schema guardrails, DataTrove "
            "Gopher/FineWeb quality filtering, substantiveness checks, English gating, "
            "and local dedup."
        ),
    )
    _write_summary_csv(summary_path, stage1e_summary_rows)
    document_quality_elapsed_sec = time.perf_counter() - filter_start
    metrics_dir = stage1e_metrics_dir(config)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    throughput_summary_path = metrics_dir / f"cc_stage1e_throughput_summary_{runid}.csv"
    _write_stage1e_throughput_summary(
        throughput_summary_path,
        config=config,
        runid=runid,
        document_quality_summary=_load_metric_map(summary_path),
        document_quality_elapsed_sec=document_quality_elapsed_sec,
    )

    logger.info("Wrote filtered hits %s", filtered_path)
    logger.info("Wrote corpus texts %s", corpus_path)
    logger.info("Wrote validation sample %s", sample_path)
    logger.info("Wrote Stage 1e summary %s", summary_path)
    logger.info("Wrote Stage 1e term summary %s", term_summary_path)
    logger.info("Wrote Stage 1e throughput summary %s", throughput_summary_path)
    print(
        "Stage 1e document quality complete: "
        f"validated_hits_warc={validated_hits_warc_total}, "
        f"document_quality_kept_hits={quality_kept_hits_total}, "
        f"english_hits={english_hits_total}, "
        f"dedup_representative_hits={dedup_rep_hits_total}, "
        f"corpus_docs={len(corpus_df)}, "
        f"sample_docs={sample_doc_n}"
    )
    return summary_path
