import csv
import gzip
import hashlib
import logging
import random
import re
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import tldextract
from warcio.archiveiterator import ArchiveIterator

from src.pathing import working_output_dir

TIMING_FIELDS = [
    "time_input_read_sec",
    "time_parse_sec",
    "time_term_match_sec",
    "time_domain_extract_sec",
    "time_write_sec",
]

DEFAULT_BOILERPLATE_SIGNATURE_PATTERNS = [
    r"skip\s+to\s+content",
    r"skip\s+navigation",
    r"privacy\s+policy.{0,80}cookies?",
    r"cookie\s+consent",
    r"cookie\s+settings",
    r"accept\s+cookies?",
    r"manage\s+cookies?",
    r"add\s+to\s+cart",
    r"\bcheckout\b",
    r"toggle\s+navigation",
    r"open\s+accessibility\s+menu",
    r"accessibility\s+widget",
    r"accessibility\s+profile",
    r"choose\s+accessibility\s+menu",
    r"cognitive\s+disability",
    r"seizure\s+safe",
    r"reading\s+mask",
    r"line\s+height",
    r"cursor\s+size",
    r"adhd[-\s]?friendly\s+(mode|profile)",
]

TERM_SECTION_DEFAULTS = {
    "adhd_patterns": {"term_role": "target", "term_group": "adhd"},
    "autism_patterns": {"term_role": "target", "term_group": "autism"},
    "baseline_patterns": {
        "term_role": "baseline",
        "term_group": "baseline_negative",
    },
}

DEPRECATED_SUMMARY_METRICS = (
    "final_hits",
    "hits_total",
    "final_per_10k",
)

CANDIDATE_HIT_COLUMNS = [
    "crawl_id",
    "source_wet",
    "url",
    "registered_domain",
    "warc_date",
    "term_role",
    "term_group",
    "matched_term",
    "term_pattern",
    "context_snippet",
    "doc_char_len",
    "doc_token_count",
    "text_len",
    "triage_rule_signature_hard",
    "triage_rule_directory_index",
    "triage_rule_negative_page_type_url",
    "triage_rule_negative_page_type_url_reason",
    "removed_by_domaincap",
    "is_validated_hits_wet",
    "triage_decision",
]

REMOVED_AUDIT_FIELDS = [
    "crawl_id",
    "source_wet",
    "url",
    "registered_domain",
    "term_role",
    "term_group",
    "matched_term",
    "term_pattern",
    "context_snippet",
    "triage_decision",
    "removal_reason",
]


@dataclass(frozen=True)
class CompiledTerm:
    matched_term: str
    term_pattern: str
    term_group: str
    term_role: str
    pattern: re.Pattern[str]


def _utc_runid() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.gmtime())


def _setup_logger(log_dir: Path, runid: str) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"cc-scan_{runid}.log"

    logger = logging.getLogger(f"cc-scan-{runid}")
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


def iter_wet_records(path: Path) -> Iterable[Tuple[Optional[str], Optional[str], str]]:
    with gzip.open(path, "rb") as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type != "conversion":
                continue
            url = record.rec_headers.get_header("WARC-Target-URI")
            warc_date = record.rec_headers.get_header("WARC-Date")
            payload = record.content_stream().read()
            text = payload.decode("utf-8", errors="ignore")
            yield url, warc_date, text


def _normalize_term_spec(
    raw_spec: Any,
    default_name: str,
    default_role: str,
    default_group: str,
) -> Tuple[str, str, str, str]:
    if isinstance(raw_spec, str):
        return default_name, raw_spec, default_role, default_group
    if not isinstance(raw_spec, dict):
        raise TypeError(f"Unsupported term spec type for {default_name}: {type(raw_spec).__name__}")

    matched_term = str(raw_spec.get("name", default_name)).strip() or default_name
    term_pattern = str(raw_spec.get("pattern", "")).strip()
    if not term_pattern:
        raise ValueError(f"Missing term pattern for {matched_term}")

    term_role = str(raw_spec.get("term_role", default_role)).strip() or default_role
    term_group = str(raw_spec.get("term_group", default_group)).strip() or default_group
    return matched_term, term_pattern, term_role, term_group


def compile_patterns(terms: Dict[str, Any]) -> List[CompiledTerm]:
    compiled: List[CompiledTerm] = []
    for section, defaults in TERM_SECTION_DEFAULTS.items():
        raw_specs = terms.get(section, [])
        for idx, raw_spec in enumerate(raw_specs):
            matched_term, term_pattern, term_role, term_group = _normalize_term_spec(
                raw_spec=raw_spec,
                default_name=f"{section}[{idx}]",
                default_role=str(defaults["term_role"]),
                default_group=str(defaults["term_group"]),
            )
            compiled.append(
                CompiledTerm(
                    matched_term=matched_term,
                    term_pattern=term_pattern,
                    term_group=term_group,
                    term_role=term_role,
                    pattern=re.compile(term_pattern, re.IGNORECASE),
                )
            )
    return compiled


def compile_asd_pattern(terms: Dict[str, Any]) -> Optional[CompiledTerm]:
    raw_spec = terms.get("asd_pattern")
    if not raw_spec:
        return None

    matched_term, term_pattern, term_role, term_group = _normalize_term_spec(
        raw_spec=raw_spec,
        default_name="asd_pattern",
        default_role="target",
        default_group="autism",
    )
    return CompiledTerm(
        matched_term=matched_term,
        term_pattern=term_pattern,
        term_group=term_group,
        term_role=term_role,
        pattern=re.compile(term_pattern, re.IGNORECASE),
    )


def asd_disambiguated(text: str, span: Tuple[int, int], window: int) -> bool:
    start = max(0, span[0] - window)
    end = min(len(text), span[1] + window)
    window_text = text[start:end].lower()
    return "autism" in window_text


def find_term_matches(
    text: str,
    patterns: List[CompiledTerm],
    asd_pattern: Optional[CompiledTerm],
    asd_window: int,
) -> List[Tuple[CompiledTerm, Tuple[int, int]]]:
    hits: List[Tuple[CompiledTerm, Tuple[int, int]]] = []
    for term in patterns:
        match = term.pattern.search(text)
        if match:
            hits.append((term, match.span()))

    if asd_pattern:
        match = asd_pattern.pattern.search(text)
        if match and asd_disambiguated(text, match.span(), asd_window):
            hits.append((asd_pattern, match.span()))

    return hits


_EXTRACTOR = tldextract.TLDExtract(
    suffix_list_urls=None,
    cache_dir=Path(tempfile.gettempdir()) / "tldextract",
)


def extract_registered_domain(url: Optional[str]) -> str:
    if not url:
        return ""
    extracted = _EXTRACTOR(url)
    return extracted.registered_domain or ""


def _context_snippet(text: str, span: Tuple[int, int], window: int) -> str:
    start = max(0, span[0] - window)
    end = min(len(text), span[1] + window)
    snippet = text[start:end].strip().replace("\n", " ")
    return " ".join(snippet.split())


def _crawl_id_from_wet_name(name: str) -> str:
    return name.split("_", 1)[0]


def _new_counter_bucket() -> Dict[str, int]:
    return {
        "docs_scanned": 0,
        "docs_minlen": 0,
        "tokens_scanned": 0,
        "tokens_minlen": 0,
        "candidate_hits": 0,
        "validated_hits_wet": 0,
        "removed_domaincap": 0,
        "removed_boilerplate_signature_hard": 0,
        "removed_boilerplate_directory_index": 0,
        "removed_boilerplate_negative_page_type_url": 0,
        "removed_boilerplate_total": 0,
    }


def _new_timing_bucket() -> Dict[str, float]:
    return {key: 0.0 for key in TIMING_FIELDS}


def _rate_per(numer: int, denom: int, scale: int) -> float:
    if denom <= 0:
        return 0.0
    return (numer / denom) * scale


def _summary_row(metric: str, value: object, description: str = "") -> List[object]:
    return [metric, value]


def compile_boilerplate_patterns(patterns: List[str]) -> List[re.Pattern]:
    return [re.compile(pattern, re.IGNORECASE) for pattern in patterns]


def is_boilerplate_signature(snippet: str, patterns: List[re.Pattern]) -> bool:
    return any(pattern.search(snippet) for pattern in patterns)


def is_negative_page_type_url(url: str, patterns: List[re.Pattern]) -> bool:
    if not url:
        return False
    return any(pattern.search(url) for pattern in patterns)


def _negative_page_type_url_reason(url: str, patterns: List[re.Pattern]) -> str:
    if not url:
        return ""
    for pattern in patterns:
        if not pattern.search(url):
            continue
        pattern_text = pattern.pattern.lower()
        if "flashcard" in pattern_text:
            return "flashcards"
        if "viewtopic" in pattern_text:
            return "viewtopic"
        if "thread" in pattern_text:
            return "threads"
        if "trackasin" in pattern_text:
            return "trackasin"
        if "album" in pattern_text:
            return "albums"
        if "product-category" in pattern_text:
            return "product_category"
        if "product" in pattern_text:
            return "products"
        if "list/job" in pattern_text:
            return "job_list"
        if "job" in pattern_text:
            return "jobs"
        if "/pin" in pattern_text:
            return "pin"
        if "playlist" in pattern_text:
            return "playlists"
        if "forum" in pattern_text:
            return "forums"
        if "quote" in pattern_text:
            return "quotes"
        if "category" in pattern_text:
            return "categories"
        if "search" in pattern_text or "[?&](s|q|search)=" in pattern_text:
            return "search"
        if "casino" in pattern_text or "viagra" in pattern_text or "pharma" in pattern_text:
            return "spam_domain"
        if "tag" in pattern_text:
            return "tags"
        return "other"
    return ""


def normalize_listiness_phrases(phrases: List[str]) -> List[str]:
    return [phrase.strip().lower() for phrase in phrases if phrase.strip()]


def likely_sentence_terminator_count(text: str) -> int:
    # Collapse ellipses/runs so menu fragments with "..." do not inflate counts.
    collapsed = re.sub(r"\.{2,}", ".", text)
    return len(re.findall(r"[.!?](?:\s|$)", collapsed))


def _count_short_fragments(snippet: str, short_fragment_max_words: int) -> int:
    fragments = [frag.strip() for frag in re.split(r"[|/>•]+", snippet) if frag.strip()]
    return sum(
        1 for fragment in fragments if len(fragment.split()) <= int(short_fragment_max_words)
    )


def is_boilerplate_directory_index(
    snippet: str,
    lexicon: List[str],
    thresholds: Dict[str, float],
) -> bool:
    if not snippet:
        return False
    char_count = max(1, len(snippet))
    lower_snippet = snippet.lower()
    fragments = [
        frag.strip() for frag in re.split(r"[\n\r,|;•»›/]+", snippet) if frag and frag.strip()
    ]
    short_fragment_count = sum(
        1
        for frag in fragments
        if int(thresholds["short_fragment_min_chars"])
        <= len(frag)
        <= int(thresholds["short_fragment_max_chars"])
    )
    short_fragment_ratio = short_fragment_count / max(1, len(fragments))

    separator_count = sum(snippet.count(ch) for ch in ",|;•»›/\n\r")
    separator_per_1k = (separator_count * 1000.0) / char_count
    sentence_per_1k = (likely_sentence_terminator_count(snippet) * 1000.0) / char_count
    lexicon_hits = sum(1 for phrase in lexicon if phrase in lower_snippet)

    words = re.findall(r"\b[A-Za-z][A-Za-z'-]*\b", snippet)
    capitalized_ratio = sum(1 for word in words if word[0].isupper()) / max(1, len(words))
    single_letter_tokens = set(re.findall(r"\b[a-z]\b", lower_snippet))

    score = 0
    if separator_per_1k >= float(thresholds["min_separator_per_1k"]):
        score += 1
    if short_fragment_count >= int(
        thresholds["min_short_fragments"]
    ) and short_fragment_ratio >= float(thresholds["min_short_fragment_ratio"]):
        score += 1
    if sentence_per_1k <= float(
        thresholds["max_sentence_terminators_per_1k"]
    ) and separator_per_1k >= float(thresholds["low_narrative_min_separator_per_1k"]):
        score += 1
    if lexicon_hits >= int(thresholds["min_lexicon_hits"]):
        score += 1
    if capitalized_ratio >= float(
        thresholds["min_capitalized_token_ratio"]
    ) and short_fragment_ratio >= float(thresholds["min_short_fragment_ratio"]):
        score += 1
    if len(single_letter_tokens) >= int(thresholds["min_single_letter_tokens"]):
        score += 1

    return score >= int(thresholds["score_threshold"])


def _seed_for_run(project_seed: int, runid: str) -> int:
    digest = hashlib.sha256(f"{project_seed}:{runid}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def _add_removed_audit_row(
    removed_rows_by_reason: Dict[str, List[Dict[str, str]]],
    reason: str,
    row: Dict[str, object],
    removal_reason: Optional[str] = None,
) -> None:
    audit_row = {field: row.get(field, "") for field in REMOVED_AUDIT_FIELDS}
    audit_row["removal_reason"] = removal_reason or reason
    removed_rows_by_reason[reason].append(
        {key: str(value) if value is not None else "" for key, value in audit_row.items()}
    )


def _write_removed_audit_csv(
    out_dir: Path,
    runid: str,
    project_seed: int,
    removed_rows_by_reason: Dict[str, List[Dict[str, str]]],
    reasons: List[str],
    logger: logging.Logger,
) -> Path:
    audit_path = out_dir / f"cc_removed_audit_{runid}.csv"
    sample_size = 7
    run_seed = _seed_for_run(project_seed, runid)
    sampled_rows: List[Dict[str, str]] = []

    for reason in sorted(reasons):
        candidates = removed_rows_by_reason.get(reason, [])
        reason_rng = random.Random(
            run_seed ^ int(hashlib.sha256(reason.encode("utf-8")).hexdigest()[:8], 16)
        )
        if not candidates:
            logger.warning("Audit rows for %s: requested=%d available=0", reason, sample_size)
            sampled = [
                {
                    "crawl_id": "",
                    "source_wet": "",
                    "url": "",
                    "registered_domain": "",
                    "term_role": "",
                    "term_group": "",
                    "matched_term": "",
                    "term_pattern": "",
                    "context_snippet": "",
                    "triage_decision": reason,
                    "removal_reason": reason,
                }
                for _ in range(sample_size)
            ]
        elif len(candidates) < sample_size:
            logger.warning(
                "Audit rows for %s: requested=%d available=%d (sampling with replacement)",
                reason,
                sample_size,
                len(candidates),
            )
            sampled = [reason_rng.choice(candidates) for _ in range(sample_size)]
        elif len(candidates) == sample_size:
            sampled = list(candidates)
        else:
            sampled = reason_rng.sample(candidates, sample_size)
        sampled_rows.extend(sampled)

    with audit_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=REMOVED_AUDIT_FIELDS,
        )
        writer.writeheader()
        writer.writerows(sampled_rows)

    return audit_path


def _record_boilerplate_removal(
    combined: Dict[str, int], crawl_counters: Dict[str, int], reason: str
) -> None:
    combined[reason] += 1
    crawl_counters[reason] += 1
    combined["removed_boilerplate_total"] += 1
    crawl_counters["removed_boilerplate_total"] += 1


def _build_hit_row(
    crawl_id: str,
    source_wet: str,
    url: str,
    registered_domain: str,
    warc_date: str,
    term: CompiledTerm,
    context_snippet: str,
    doc_char_len: int,
    doc_token_count: int,
    triage_rule_signature_hard: bool,
    triage_rule_directory_index: bool,
    triage_rule_negative_page_type_url: bool,
    triage_rule_negative_page_type_url_reason: str,
    removed_by_domaincap: bool,
    is_validated_hits_wet: bool,
    triage_decision: str,
) -> Dict[str, object]:
    return {
        "crawl_id": crawl_id,
        "source_wet": source_wet,
        "url": url,
        "registered_domain": registered_domain,
        "warc_date": warc_date,
        "term_role": term.term_role,
        "term_group": term.term_group,
        "matched_term": term.matched_term,
        "term_pattern": term.term_pattern,
        "context_snippet": context_snippet,
        "doc_char_len": doc_char_len,
        "doc_token_count": doc_token_count,
        "text_len": doc_char_len,
        "triage_rule_signature_hard": triage_rule_signature_hard,
        "triage_rule_directory_index": triage_rule_directory_index,
        "triage_rule_negative_page_type_url": triage_rule_negative_page_type_url,
        "triage_rule_negative_page_type_url_reason": triage_rule_negative_page_type_url_reason,
        "removed_by_domaincap": removed_by_domaincap,
        "is_validated_hits_wet": is_validated_hits_wet,
        "triage_decision": triage_decision,
    }


def _write_term_summary_csv(
    candidate_df: pd.DataFrame,
    out_dir: Path,
    runid: str,
) -> Path:
    summary_path = out_dir / f"cc_term_summary_{runid}.csv"
    if candidate_df.empty:
        summary_df = pd.DataFrame(
            columns=[
                "crawl_id",
                "term_role",
                "term_group",
                "matched_term",
                "candidate_hits",
                "validated_hits_wet",
                "removed_boilerplate_signature_hard",
                "removed_boilerplate_directory_index",
                "removed_boilerplate_negative_page_type_url",
                "removed_domaincap",
            ]
        )
        summary_df.to_csv(summary_path, index=False)
        return summary_path

    group_cols = ["crawl_id", "term_role", "term_group", "matched_term"]

    def _summarize(df: pd.DataFrame) -> pd.DataFrame:
        grouped = (
            df.groupby(group_cols, dropna=False)
            .agg(
                candidate_hits=("matched_term", "size"),
                validated_hits_wet=("is_validated_hits_wet", "sum"),
                removed_boilerplate_signature_hard=(
                    "triage_rule_signature_hard",
                    "sum",
                ),
                removed_boilerplate_directory_index=(
                    "triage_rule_directory_index",
                    "sum",
                ),
                removed_boilerplate_negative_page_type_url=(
                    "triage_rule_negative_page_type_url",
                    "sum",
                ),
                removed_domaincap=("removed_by_domaincap", "sum"),
            )
            .reset_index()
        )
        return grouped.sort_values(group_cols).reset_index(drop=True)

    per_crawl = _summarize(candidate_df)
    combined_input = candidate_df.copy()
    combined_input["crawl_id"] = "ALL"
    combined = _summarize(combined_input)
    summary_df = pd.concat([per_crawl, combined], ignore_index=True)
    summary_df.to_csv(summary_path, index=False)
    return summary_path


def _write_top_domains_by_term_csv(
    validated_df: pd.DataFrame,
    out_dir: Path,
    runid: str,
) -> Path:
    path = out_dir / f"cc_scan_top_domains_by_term_{runid}.csv"
    if validated_df.empty:
        pd.DataFrame(
            columns=[
                "term_role",
                "term_group",
                "matched_term",
                "registered_domain",
                "hits",
            ]
        ).to_csv(path, index=False)
        return path

    filtered = validated_df[validated_df["registered_domain"].fillna("") != ""].copy()
    top_domains_df = (
        filtered.groupby(
            ["term_role", "term_group", "matched_term", "registered_domain"],
            dropna=False,
        )
        .size()
        .reset_index(name="hits")
        .sort_values(
            ["term_role", "matched_term", "hits", "registered_domain"],
            ascending=[True, True, False, True],
        )
        .groupby(["term_role", "matched_term"], group_keys=False)
        .head(10)
        .reset_index(drop=True)
    )
    top_domains_df.to_csv(path, index=False)
    return path


def _build_scan_summary_rows(
    combined: Dict[str, int],
    docs_per_sec: float,
    total_elapsed_sec: float,
    candidate_hits_path: Path,
    validated_hits_wet_path: Path,
    term_summary_path: Path,
    top_domains_by_term_path: Path,
    candidate_hits_by_role: Dict[str, int],
    validated_hits_wet_by_role: Dict[str, int],
    combined_timings: Dict[str, float],
    boilerplate_reasons: List[str],
    counters_by_crawl: Dict[str, Dict[str, int]],
    elapsed_by_crawl: Dict[str, float],
    docs_per_sec_by_crawl: Dict[str, float],
) -> List[List[object]]:
    removed_total = combined["removed_boilerplate_total"] + combined["removed_domaincap"]

    summary_rows: List[List[object]] = [
        _summary_row(
            "docs_scanned",
            combined["docs_scanned"],
            "Total conversion documents scanned across all crawl slices.",
        ),
        _summary_row(
            "docs_minlen",
            combined["docs_minlen"],
            "Documents that passed the minimum character-length filter.",
        ),
        _summary_row(
            "tokens_scanned",
            combined["tokens_scanned"],
            "Whitespace-token count across all scanned conversion documents.",
        ),
        _summary_row(
            "tokens_minlen",
            combined["tokens_minlen"],
            "Whitespace-token count across documents that passed the minimum length filter.",
        ),
        _summary_row(
            "candidate_hits",
            combined["candidate_hits"],
            "Term matches before downstream filtering and domain caps.",
        ),
        _summary_row(
            "validated_hits_wet",
            combined["validated_hits_wet"],
            "Rows retained after WET filtering and domain-cap enforcement.",
        ),
        _summary_row(
            "candidate_per_10k",
            round(_rate_per(combined["candidate_hits"], combined["docs_scanned"], 10_000), 6),
            "Combined candidate hit rate per 10,000 scanned documents.",
        ),
        _summary_row(
            "validated_hits_wet_per_10k",
            round(
                _rate_per(combined["validated_hits_wet"], combined["docs_scanned"], 10_000),
                6,
            ),
            "Combined WET-validated hit rate per 10,000 scanned documents.",
        ),
        _summary_row(
            "removed_domaincap",
            combined["removed_domaincap"],
            "Candidate matches removed because the per-domain cap was reached.",
        ),
        _summary_row(
            "removed_total",
            removed_total,
            "Total removed candidates (domain-cap plus all boilerplate rules).",
        ),
        _summary_row(
            "docs_per_sec",
            round(docs_per_sec, 6),
            "Overall scan throughput in scanned documents per second.",
        ),
        _summary_row(
            "total_elapsed_sec",
            round(total_elapsed_sec, 6),
            "Total wall-clock time for the scan stage in seconds.",
        ),
    ]
    summary_rows.extend(
        [
            _summary_row(
                "candidate_hits_path",
                str(candidate_hits_path),
                "Parquet table containing row-level candidate hits before WET triage removal.",
            ),
            _summary_row(
                "validated_hits_wet_path",
                str(validated_hits_wet_path),
                "Parquet table containing row-level WET-validated hits after WET triage.",
            ),
            _summary_row(
                "term_summary_path",
                str(term_summary_path),
                "CSV table containing per-term candidate and removal diagnostics.",
            ),
            _summary_row(
                "top_domains_by_term_path",
                str(top_domains_by_term_path),
                "CSV table containing per-term top registered domains for validated WET hits.",
            ),
        ]
    )

    for term_role in ("target", "baseline"):
        summary_rows.extend(
            [
                _summary_row(
                    f"term_role.{term_role}.candidate_hits",
                    int(candidate_hits_by_role.get(term_role, 0)),
                    f"Candidate hits attributed to term_role={term_role}.",
                ),
                _summary_row(
                    f"term_role.{term_role}.validated_hits_wet",
                    int(validated_hits_wet_by_role.get(term_role, 0)),
                    f"WET-validated hits attributed to term_role={term_role}.",
                ),
            ]
        )

    for field in TIMING_FIELDS:
        summary_rows.append(
            _summary_row(
                field,
                round(combined_timings[field], 6),
                "Combined coarse timing for this processing stage in seconds.",
            )
        )

    for reason in boilerplate_reasons:
        summary_rows.append(
            _summary_row(
                reason,
                combined[reason],
                "Combined removals triggered by this specific boilerplate rule.",
            )
        )

    summary_rows.append(
        _summary_row(
            "removed_boilerplate_total",
            combined["removed_boilerplate_total"],
            "Combined total removals from all boilerplate rules.",
        )
    )

    for crawl_id in sorted(counters_by_crawl):
        slice_docs_scanned = counters_by_crawl[crawl_id]["docs_scanned"]
        slice_tokens_scanned = counters_by_crawl[crawl_id]["tokens_scanned"]
        slice_tokens_minlen = counters_by_crawl[crawl_id]["tokens_minlen"]
        slice_candidate_hits = counters_by_crawl[crawl_id]["candidate_hits"]
        slice_validated_hits_wet = counters_by_crawl[crawl_id]["validated_hits_wet"]
        prefix = f"slice.{crawl_id}"
        summary_rows.extend(
            [
                _summary_row(
                    f"{prefix}.docs_scanned",
                    slice_docs_scanned,
                    "Slice-level scanned documents for this crawl_id.",
                ),
                _summary_row(
                    f"{prefix}.tokens_scanned",
                    slice_tokens_scanned,
                    "Slice-level whitespace-token count across scanned documents.",
                ),
                _summary_row(
                    f"{prefix}.tokens_minlen",
                    slice_tokens_minlen,
                    "Slice-level whitespace-token count across minimum-length documents.",
                ),
                _summary_row(
                    f"{prefix}.candidate_hits",
                    slice_candidate_hits,
                    "Slice-level candidate matches before final filtering.",
                ),
                _summary_row(
                    f"{prefix}.validated_hits_wet",
                    slice_validated_hits_wet,
                    "Slice-level WET-validated retained hits after filtering.",
                ),
                _summary_row(
                    f"{prefix}.candidate_per_10k",
                    round(_rate_per(slice_candidate_hits, slice_docs_scanned, 10_000), 6),
                    "Slice-level candidate hit rate per 10,000 scanned documents.",
                ),
                _summary_row(
                    f"{prefix}.validated_hits_wet_per_10k",
                    round(
                        _rate_per(slice_validated_hits_wet, slice_docs_scanned, 10_000),
                        6,
                    ),
                    "Slice-level WET-validated hit rate per 10,000 scanned documents.",
                ),
                _summary_row(
                    f"{prefix}.total_elapsed_sec",
                    round(elapsed_by_crawl.get(crawl_id, 0.0), 6),
                    "Slice-level wall-clock elapsed time in seconds.",
                ),
                _summary_row(
                    f"{prefix}.docs_per_sec",
                    round(docs_per_sec_by_crawl.get(crawl_id, 0.0), 6),
                    "Slice-level throughput in scanned documents per second.",
                ),
            ]
        )
    return summary_rows


def scan_wet_files(config: Dict, config_path: Path) -> Path:
    scan_start = time.perf_counter()
    runid = _utc_runid()
    logger = _setup_logger(Path("reports/logs"), runid)
    project_seed = int(config.get("project", {}).get("seed", 0))

    min_chars = int(config["filters"]["min_chars"])
    domain_cap = int(config["filters"]["domain_cap"])
    asd_window = int(config["filters"]["asd_disambiguation_window_chars"])
    context_window_chars = int(config["filters"].get("context_window_chars", 200))
    boilerplate_cfg = config.get("boilerplate", {})
    boilerplate_enabled = bool(boilerplate_cfg.get("enabled", True))
    boilerplate_check_window_chars = int(boilerplate_cfg.get("check_window_chars", 2000))
    boilerplate_signature_hard_patterns = compile_boilerplate_patterns(
        boilerplate_cfg.get(
            "signature_hard_patterns",
            boilerplate_cfg.get("signature_patterns", DEFAULT_BOILERPLATE_SIGNATURE_PATTERNS),
        )
    )
    directory_index_cfg = boilerplate_cfg.get("directory_index", {})
    boilerplate_directory_index_enabled = bool(
        directory_index_cfg.get("enabled", boilerplate_cfg.get("directory_index_enabled", True))
    )
    boilerplate_directory_index_lexicon = normalize_listiness_phrases(
        directory_index_cfg.get(
            "lexicon",
            boilerplate_cfg.get(
                "directory_index_phrases",
                [
                    "all conditions",
                    "conditions a-z",
                    "health a-z",
                    "browse conditions",
                    "symptoms",
                    "diagnosis",
                    "treatment",
                    "diseases",
                    "disorders",
                    "a to z",
                    "a-z index",
                ],
            ),
        )
    )
    boilerplate_directory_index_thresholds = {
        "score_threshold": int(directory_index_cfg.get("score_threshold", 4)),
        "min_separator_per_1k": float(directory_index_cfg.get("min_separator_per_1k", 10.0)),
        "min_short_fragment_ratio": float(
            directory_index_cfg.get("min_short_fragment_ratio", 0.45)
        ),
        "min_short_fragments": int(directory_index_cfg.get("min_short_fragments", 10)),
        "short_fragment_min_chars": int(directory_index_cfg.get("short_fragment_min_chars", 3)),
        "short_fragment_max_chars": int(directory_index_cfg.get("short_fragment_max_chars", 35)),
        "max_sentence_terminators_per_1k": float(
            directory_index_cfg.get("max_sentence_terminators_per_1k", 2.5)
        ),
        "low_narrative_min_separator_per_1k": float(
            directory_index_cfg.get("low_narrative_min_separator_per_1k", 8.0)
        ),
        "min_lexicon_hits": int(directory_index_cfg.get("min_lexicon_hits", 3)),
        "min_capitalized_token_ratio": float(
            directory_index_cfg.get("min_capitalized_token_ratio", 0.3)
        ),
        "min_single_letter_tokens": int(directory_index_cfg.get("min_single_letter_tokens", 6)),
    }
    negative_page_type_url_cfg = boilerplate_cfg.get("negative_page_type_url", {})
    negative_page_type_url_enabled = bool(negative_page_type_url_cfg.get("enabled", False))
    negative_page_type_url_patterns = compile_boilerplate_patterns(
        negative_page_type_url_cfg.get("patterns", [])
    )

    terms = config["terms"]
    patterns = compile_patterns(terms)
    asd_pattern = compile_asd_pattern(terms)

    logger.info("Loaded config %s", config_path)
    logger.info("Min chars: %d", min_chars)
    logger.info("Domain cap: %d", domain_cap)
    logger.info("ASD window: %d", asd_window)
    logger.info("Context window: %d", context_window_chars)
    logger.info("Boilerplate enabled: %s", boilerplate_enabled)
    logger.info("Boilerplate check window: %d", boilerplate_check_window_chars)
    logger.info("Boilerplate filters: signature_hard and directory_index.")
    logger.info("Boilerplate signature hard patterns: %d", len(boilerplate_signature_hard_patterns))
    logger.info("Boilerplate directory index enabled: %s", boilerplate_directory_index_enabled)

    wet_dir = Path("data/raw/wet")
    configured_wet_files = {
        str(name).strip()
        for name in config.get("inputs", {}).get("wet_filenames", [])
        if str(name).strip()
    }
    configured_crawl_ids = {
        str(crawl_id).strip()
        for crawl_id in config.get("crawl", {}).get("crawl_ids", [])
        if str(crawl_id).strip()
    }
    wet_files = [
        wet_path
        for wet_path in sorted(wet_dir.glob("*.wet.gz"))
        if (
            (not configured_wet_files or wet_path.name in configured_wet_files)
            and (
                not configured_crawl_ids
                or _crawl_id_from_wet_name(wet_path.name) in configured_crawl_ids
            )
        )
    ]
    if not wet_files:
        raise FileNotFoundError("No .wet.gz files found in data/raw/wet")
    logger.info("Configured crawl IDs: %s", sorted(configured_crawl_ids) or "ALL")
    logger.info(
        "Configured WET filenames: %s",
        sorted(configured_wet_files) or "ALL",
    )
    logger.info("Matched WET files: %d", len(wet_files))

    combined = _new_counter_bucket()
    counters_by_crawl: Dict[str, Dict[str, int]] = defaultdict(_new_counter_bucket)
    removed_rows_by_reason: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    combined_timings = _new_timing_bucket()
    timings_by_crawl: Dict[str, Dict[str, float]] = defaultdict(_new_timing_bucket)
    elapsed_by_crawl: Dict[str, float] = defaultdict(float)
    domains_hits: set[str] = set()
    domain_hit_counts: Dict[Tuple[str, str], int] = defaultdict(int)
    top_domains_counter: Counter[str] = Counter()

    candidate_rows: List[Dict[str, object]] = []
    validated_rows: List[Dict[str, object]] = []

    for wet_path in wet_files:
        source_wet = wet_path.name
        crawl_id = _crawl_id_from_wet_name(source_wet)
        logger.info("Scanning %s", wet_path)
        file_start = time.perf_counter()
        crawl_counters = counters_by_crawl[crawl_id]
        crawl_timings = timings_by_crawl[crawl_id]

        with gzip.open(wet_path, "rb") as stream:
            iterator = ArchiveIterator(stream)
            while True:
                parse_start = time.perf_counter()
                try:
                    record = next(iterator)
                except StopIteration:
                    break
                parse_elapsed = time.perf_counter() - parse_start
                combined_timings["time_parse_sec"] += parse_elapsed
                crawl_timings["time_parse_sec"] += parse_elapsed

                if record.rec_type != "conversion":
                    continue

                read_start = time.perf_counter()
                url = record.rec_headers.get_header("WARC-Target-URI")
                warc_date = record.rec_headers.get_header("WARC-Date")
                payload = record.content_stream().read()
                read_elapsed = time.perf_counter() - read_start
                combined_timings["time_input_read_sec"] += read_elapsed
                crawl_timings["time_input_read_sec"] += read_elapsed

                parse_start = time.perf_counter()
                text = payload.decode("utf-8", errors="ignore")
                parse_elapsed = time.perf_counter() - parse_start
                combined_timings["time_parse_sec"] += parse_elapsed
                crawl_timings["time_parse_sec"] += parse_elapsed

                combined["docs_scanned"] += 1
                crawl_counters["docs_scanned"] += 1
                doc_token_count = len(text.split())
                combined["tokens_scanned"] += doc_token_count
                crawl_counters["tokens_scanned"] += doc_token_count
                text_len = len(text)
                if text_len < min_chars:
                    continue
                combined["docs_minlen"] += 1
                crawl_counters["docs_minlen"] += 1
                combined["tokens_minlen"] += doc_token_count
                crawl_counters["tokens_minlen"] += doc_token_count

                domain_start = time.perf_counter()
                domain = extract_registered_domain(url)
                domain_elapsed = time.perf_counter() - domain_start
                combined_timings["time_domain_extract_sec"] += domain_elapsed
                crawl_timings["time_domain_extract_sec"] += domain_elapsed
                match_start = time.perf_counter()
                matches = find_term_matches(text, patterns, asd_pattern, asd_window)
                match_elapsed = time.perf_counter() - match_start
                combined_timings["time_term_match_sec"] += match_elapsed
                crawl_timings["time_term_match_sec"] += match_elapsed
                if not matches:
                    continue
                combined["candidate_hits"] += len(matches)
                crawl_counters["candidate_hits"] += len(matches)

                for term, span in matches:
                    stored_snippet = _context_snippet(text, span, context_window_chars)
                    check_text = _context_snippet(text, span, boilerplate_check_window_chars)
                    triage_rule_signature_hard = False
                    triage_rule_directory_index = False
                    triage_rule_negative_page_type_url = False
                    triage_rule_negative_page_type_url_reason = ""
                    removed_by_domaincap = False
                    is_validated_hits_wet = False
                    triage_decision = "validated_hits_wet"

                    if boilerplate_enabled:
                        if is_boilerplate_signature(
                            check_text, boilerplate_signature_hard_patterns
                        ):
                            triage_rule_signature_hard = True
                            triage_decision = "removed_boilerplate_signature_hard"
                            _record_boilerplate_removal(
                                combined,
                                crawl_counters,
                                "removed_boilerplate_signature_hard",
                            )
                        elif boilerplate_directory_index_enabled and is_boilerplate_directory_index(
                            check_text,
                            boilerplate_directory_index_lexicon,
                            boilerplate_directory_index_thresholds,
                        ):
                            triage_rule_directory_index = True
                            triage_decision = "removed_boilerplate_directory_index"
                            _record_boilerplate_removal(
                                combined,
                                crawl_counters,
                                "removed_boilerplate_directory_index",
                            )
                        elif negative_page_type_url_enabled and (
                            triage_rule_negative_page_type_url_reason
                            := _negative_page_type_url_reason(
                                url or "",
                                negative_page_type_url_patterns,
                            )
                        ):
                            triage_rule_negative_page_type_url = True
                            triage_decision = "removed_boilerplate_negative_page_type_url"
                            _record_boilerplate_removal(
                                combined,
                                crawl_counters,
                                "removed_boilerplate_negative_page_type_url",
                            )
                            specific_reason = (
                                "removed_boilerplate_negative_page_type_url."
                                f"{triage_rule_negative_page_type_url_reason}"
                            )
                            combined[specific_reason] = combined.get(specific_reason, 0) + 1
                            crawl_counters[specific_reason] = (
                                crawl_counters.get(specific_reason, 0) + 1
                            )

                    if triage_decision == "validated_hits_wet" and domain:
                        key = (crawl_id, domain)
                        if domain_hit_counts[key] >= domain_cap:
                            removed_by_domaincap = True
                            triage_decision = "removed_domaincap"
                            combined["removed_domaincap"] += 1
                            crawl_counters["removed_domaincap"] += 1
                        else:
                            domain_hit_counts[key] += 1

                    if triage_decision == "validated_hits_wet":
                        is_validated_hits_wet = True
                        combined["validated_hits_wet"] += 1
                        crawl_counters["validated_hits_wet"] += 1
                        if domain:
                            domains_hits.add(domain)
                            top_domains_counter[domain] += 1

                    write_start = time.perf_counter()
                    row = _build_hit_row(
                        crawl_id=crawl_id,
                        source_wet=source_wet,
                        url=url or "",
                        registered_domain=domain,
                        warc_date=warc_date or "",
                        term=term,
                        context_snippet=stored_snippet,
                        doc_char_len=text_len,
                        doc_token_count=doc_token_count,
                        triage_rule_signature_hard=triage_rule_signature_hard,
                        triage_rule_directory_index=triage_rule_directory_index,
                        triage_rule_negative_page_type_url=triage_rule_negative_page_type_url,
                        triage_rule_negative_page_type_url_reason=(
                            triage_rule_negative_page_type_url_reason
                        ),
                        removed_by_domaincap=removed_by_domaincap,
                        is_validated_hits_wet=is_validated_hits_wet,
                        triage_decision=triage_decision,
                    )
                    candidate_rows.append(row)
                    if is_validated_hits_wet:
                        validated_rows.append(row)
                    elif triage_decision.startswith("removed_boilerplate_"):
                        removal_reason = (
                            "signature_hard"
                            if triage_decision == "removed_boilerplate_signature_hard"
                            else (
                                "directory_index"
                                if triage_decision == "removed_boilerplate_directory_index"
                                else (
                                    "negative_page_type_url."
                                    f"{triage_rule_negative_page_type_url_reason or 'other'}"
                                )
                            )
                        )
                        audit_bucket = (
                            triage_decision
                            if triage_decision != "removed_boilerplate_negative_page_type_url"
                            else (
                                "removed_boilerplate_negative_page_type_url."
                                f"{triage_rule_negative_page_type_url_reason or 'other'}"
                            )
                        )
                        _add_removed_audit_row(
                            removed_rows_by_reason,
                            audit_bucket,
                            row,
                            removal_reason=removal_reason,
                        )
                    write_elapsed = time.perf_counter() - write_start
                    combined_timings["time_write_sec"] += write_elapsed
                    crawl_timings["time_write_sec"] += write_elapsed

        elapsed_by_crawl[crawl_id] += time.perf_counter() - file_start

    out_dir = working_output_dir(config, default_name="wet_scan")
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_path = out_dir / f"cc_scan_summary_{runid}.csv"
    top_domains_path = out_dir / f"cc_scan_top_domains_{runid}.csv"
    candidate_hits_path = out_dir / f"cc_candidate_hits_{runid}.parquet"
    validated_hits_wet_path = out_dir / f"cc_validated_hits_wet_{runid}.parquet"

    write_start = time.perf_counter()
    with top_domains_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["registered_domain", "hits"])
        for domain, count in top_domains_counter.most_common(10):
            writer.writerow([domain, count])
    write_elapsed = time.perf_counter() - write_start
    combined_timings["time_write_sec"] += write_elapsed

    candidate_df = pd.DataFrame(candidate_rows, columns=CANDIDATE_HIT_COLUMNS)
    validated_df = pd.DataFrame(validated_rows, columns=CANDIDATE_HIT_COLUMNS)
    write_start = time.perf_counter()
    candidate_df.to_parquet(candidate_hits_path, index=False)
    validated_df.to_parquet(validated_hits_wet_path, index=False)
    write_elapsed = time.perf_counter() - write_start
    combined_timings["time_write_sec"] += write_elapsed

    write_start = time.perf_counter()
    term_summary_path = _write_term_summary_csv(candidate_df, out_dir, runid)
    top_domains_by_term_path = _write_top_domains_by_term_csv(validated_df, out_dir, runid)
    write_elapsed = time.perf_counter() - write_start
    combined_timings["time_write_sec"] += write_elapsed

    candidate_hits_by_role = (
        candidate_df.groupby("term_role").size().to_dict() if not candidate_df.empty else {}
    )
    validated_hits_wet_by_role = (
        validated_df.groupby("term_role").size().to_dict() if not validated_df.empty else {}
    )

    total_elapsed_sec = time.perf_counter() - scan_start
    docs_per_sec = combined["docs_scanned"] / total_elapsed_sec if total_elapsed_sec > 0 else 0.0
    docs_per_sec_by_crawl = {
        crawl_id: (
            counters["docs_scanned"] / elapsed_by_crawl[crawl_id]
            if elapsed_by_crawl[crawl_id] > 0
            else 0.0
        )
        for crawl_id, counters in counters_by_crawl.items()
    }
    boilerplate_reasons = sorted(
        [
            key
            for key in combined
            if key.startswith("removed_boilerplate_") and key != "removed_boilerplate_total"
        ]
    )
    summary_rows = _build_scan_summary_rows(
        combined=combined,
        docs_per_sec=docs_per_sec,
        total_elapsed_sec=total_elapsed_sec,
        candidate_hits_path=candidate_hits_path,
        validated_hits_wet_path=validated_hits_wet_path,
        term_summary_path=term_summary_path,
        top_domains_by_term_path=top_domains_by_term_path,
        candidate_hits_by_role=candidate_hits_by_role,
        validated_hits_wet_by_role=validated_hits_wet_by_role,
        combined_timings=combined_timings,
        boilerplate_reasons=boilerplate_reasons,
        counters_by_crawl=counters_by_crawl,
        elapsed_by_crawl=elapsed_by_crawl,
        docs_per_sec_by_crawl=docs_per_sec_by_crawl,
    )

    write_start = time.perf_counter()
    with summary_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value"])
        writer.writerows([row[:2] for row in summary_rows])
    write_elapsed = time.perf_counter() - write_start
    combined_timings["time_write_sec"] += write_elapsed

    removed_audit_path = _write_removed_audit_csv(
        out_dir=out_dir,
        runid=runid,
        project_seed=project_seed,
        removed_rows_by_reason=removed_rows_by_reason,
        reasons=sorted(removed_rows_by_reason.keys()),
        logger=logger,
    )

    logger.info("Wrote summary %s", summary_path)
    logger.info("Wrote top domains %s", top_domains_path)
    logger.info("Wrote candidate hits %s", candidate_hits_path)
    logger.info("Wrote validated hits %s", validated_hits_wet_path)
    logger.info("Wrote term summary %s", term_summary_path)
    logger.info("Wrote top domains by term %s", top_domains_by_term_path)
    logger.info("Wrote removed-audit sample %s", removed_audit_path)

    print(
        "Scan complete: "
        f"docs_scanned={combined['docs_scanned']}, "
        f"docs_minlen={combined['docs_minlen']}, "
        f"candidate_hits={combined['candidate_hits']}, "
        f"validated_hits_wet={combined['validated_hits_wet']}, "
        f"unique_domains_hits={len(domains_hits)}"
    )
    return summary_path
