import hashlib
import re
from pathlib import Path
from typing import Dict, List

import pandas as pd

from src.pathing import stage1d_filter_en_dedup_dir, stage1d_warc_dir

from .cc_warc import (
    DOC_KEY_COLUMNS,
    _build_stage1d_summary_rows,
    _latest_enriched_hits,
    _load_metric_map,
    _metric_int,
    _require_stage1d,
    _setup_logger,
    _utc_runid,
    _write_stage1d_term_summary,
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
                "Missing language-ID dependency. Install 'langid' or 'py3langid' before running cc-filter-en-dedup."
            ) from exc
        # py3langid packages its default model as a compressed file path, not as the
        # legacy base64 model string expected by from_modelstring().
        identifier = LanguageIdentifier.from_pickled_model(
            MODEL_FILE, norm_probs=True
        )
        return identifier.classify


def filter_en_dedup_hits(config: Dict) -> Path:
    _require_stage1d(config)
    runid = _utc_runid()
    logger = _setup_logger(Path("reports/logs"), "cc-filter-en-dedup", runid)

    enrich_dir = stage1d_warc_dir(config)
    enrich_runid, enriched_path = _latest_enriched_hits(enrich_dir)
    enriched_df = pd.read_parquet(enriched_path)
    if enriched_df.empty:
        raise ValueError("Latest Stage 1d enriched hits table is empty.")

    classify = _load_language_identifier()
    dedup_cfg = config.get("stage1d", {}).get("dedup", {})
    ngram_size = int(dedup_cfg.get("ngram_size", 5))
    jaccard_threshold = float(dedup_cfg.get("jaccard_threshold", 0.9))

    filtered_df = enriched_df.copy()
    validated_mask = filtered_df["is_validated_hits_warc"].fillna(False).astype(bool)
    validated_docs = (
        filtered_df.loc[validated_mask, DOC_KEY_COLUMNS + ["extracted_text"]]
        .drop_duplicates(subset=DOC_KEY_COLUMNS)
        .reset_index(drop=True)
    )
    if validated_docs.empty:
        raise ValueError("No WARC-validated rows available for Stage 1d filtering.")

    language_rows: List[Dict[str, object]] = []
    for _, doc_row in validated_docs.iterrows():
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
    filtered_df["language_score"] = (
        pd.to_numeric(filtered_df["language_score"], errors="coerce").fillna(0.0)
    )
    filtered_df["is_english"] = filtered_df["is_english"].fillna(False).astype(bool)
    filtered_df["is_duplicate"] = filtered_df["is_duplicate"].fillna(False).astype(bool)
    filtered_df["is_dedup_representative"] = (
        filtered_df["is_dedup_representative"].fillna(False).astype(bool)
    )

    corpus_df = (
        filtered_df.loc[
            filtered_df["is_validated_hits_warc"].fillna(False)
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
            published_ts_source=("published_ts_source", "first"),
            published_ts_confidence=("published_ts_confidence", "first"),
            extracted_text=("extracted_text", "first"),
            extracted_text_len=("extracted_text_len", "first"),
            matched_terms=("matched_term", lambda series: "|".join(sorted(set(series)))),
            term_roles=("term_role", lambda series: "|".join(sorted(set(series)))),
            dedup_cluster_id=("dedup_cluster_id", "first"),
        )
        .reset_index()
    )

    out_dir = stage1d_filter_en_dedup_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    filtered_path = out_dir / f"cc_filtered_hits_en_dedup_{runid}.parquet"
    corpus_path = out_dir / f"cc_corpus_texts_en_dedup_{runid}.parquet"
    sample_cfg = config.get("stage1d", {}).get("filter_en_dedup", {})
    sample_n = int(sample_cfg.get("validation_sample_n", 30))
    sample_path = out_dir / f"cc_val_sample{sample_n}_{runid}.csv"
    summary_path = out_dir / f"cc_stage1d_summary_{runid}.csv"
    term_summary_path = out_dir / f"cc_stage1d_term_summary_{runid}.csv"

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
        "published_ts_source",
        "published_ts_confidence",
        "extracted_text_len",
        "dedup_cluster_id",
        "extracted_text",
    ]
    corpus_df[sample_columns].sample(
        n=sample_doc_n, random_state=sample_seed
    ).to_csv(sample_path, index=False)
    _write_stage1d_term_summary(term_summary_path, filtered_df, include_filter_fields=True)

    validated_hits_wet_total = int(len(filtered_df))
    validated_hits_warc_total = int(filtered_df["is_validated_hits_warc"].sum())
    english_hits_total = int(
        (
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["is_english"].fillna(False)
        ).sum()
    )
    dedup_rep_hits_total = int(
        (
            filtered_df["is_validated_hits_warc"].fillna(False)
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

    docs_scanned = 0
    candidate_hits = 0
    summary_in_path = enrich_dir / f"cc_stage1d_summary_{enrich_runid}.csv"
    if summary_in_path.exists():
        summary_in = _load_metric_map(summary_in_path)
        docs_scanned = _metric_int(summary_in, "docs_scanned", 0)
        candidate_hits = _metric_int(summary_in, "candidate_hits", 0)

    validated_docs = filtered_df.loc[
        filtered_df["is_validated_hits_warc"].fillna(False), DOC_KEY_COLUMNS
    ].drop_duplicates()
    english_docs_count = int(
        filtered_df.loc[
            filtered_df["is_validated_hits_warc"].fillna(False)
            & filtered_df["is_english"].fillna(False),
            DOC_KEY_COLUMNS,
        ]
        .drop_duplicates()
        .shape[0]
    )

    doc_metrics = {
        "doc_count_warc_validated": int(validated_docs.shape[0]),
        "doc_count_english": english_docs_count,
        "doc_count_dedup_representatives": int(corpus_df.shape[0]),
        "removed_by_lang_hits": int(validated_hits_warc_total - english_hits_total),
        "removed_by_dedup_hits": int(english_hits_total - dedup_rep_hits_total),
        "removed_by_lang_docs": int(validated_docs.shape[0] - english_docs_count),
        "removed_by_dedup_docs": int(english_docs_count - len(corpus_df)),
        "pct_removed_by_lang_hits": round(
            (
                (validated_hits_warc_total - english_hits_total)
                / validated_hits_warc_total
                * 100.0
            )
            if validated_hits_warc_total
            else 0.0,
            6,
        ),
        "pct_removed_by_dedup_hits": round(
            ((english_hits_total - dedup_rep_hits_total) / english_hits_total * 100.0)
            if english_hits_total
            else 0.0,
            6,
        ),
    }

    _write_summary_csv(
        summary_path,
        _build_stage1d_summary_rows(
            docs_scanned=docs_scanned,
            candidate_hits=candidate_hits,
            validated_hits_wet=validated_hits_wet_total,
            validated_hits_warc=validated_hits_warc_total,
            role_counts=role_counts,
            doc_metrics=doc_metrics,
            notes="Stage 1d filtered outputs after English gating and dedup.",
        ),
    )

    logger.info("Wrote filtered hits %s", filtered_path)
    logger.info("Wrote corpus texts %s", corpus_path)
    logger.info("Wrote validation sample %s", sample_path)
    logger.info("Wrote Stage 1d summary %s", summary_path)
    logger.info("Wrote Stage 1d term summary %s", term_summary_path)
    print(
        "Stage 1d filter complete: "
        f"validated_hits_warc={validated_hits_warc_total}, "
        f"english_hits={english_hits_total}, "
        f"dedup_representative_hits={dedup_rep_hits_total}, "
        f"corpus_docs={len(corpus_df)}, "
        f"sample_docs={sample_doc_n}"
    )
    return summary_path
