import csv
import gzip
import hashlib
import logging
import random
import re
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import tldextract
from warcio.archiveiterator import ArchiveIterator

TIMING_FIELDS = [
    "time_input_read_sec",
    "time_parse_sec",
    "time_term_match_sec",
    "time_domain_extract_sec",
    "time_write_sec",
]

DEFAULT_BOILERPLATE_SIGNATURE_PATTERNS = [
    r"skip\s+to\s+content",
    r"cookie\s+consent",
    r"accept\s+cookies?",
    r"manage\s+cookies?",
    r"toggle\s+navigation",
    r"accessibility\s+widget",
    r"open\s+accessibility\s+menu",
    r"add\s+to\s+cart",
    r"\bcheckout\b",
    r"privacy\s+policy.{0,80}cookies?",
]


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


def compile_patterns(terms: Dict[str, List[str]]) -> List[Tuple[str, re.Pattern]]:
    compiled: List[Tuple[str, re.Pattern]] = []
    for idx, pattern in enumerate(terms.get("adhd_patterns", [])):
        compiled.append((f"adhd_patterns[{idx}]", re.compile(pattern, re.IGNORECASE)))
    for idx, pattern in enumerate(terms.get("autism_patterns", [])):
        compiled.append((f"autism_patterns[{idx}]", re.compile(pattern, re.IGNORECASE)))
    return compiled


def asd_disambiguated(text: str, span: Tuple[int, int], window: int) -> bool:
    start = max(0, span[0] - window)
    end = min(len(text), span[1] + window)
    window_text = text[start:end].lower()
    return "autism" in window_text


def find_term_matches(
    text: str,
    patterns: List[Tuple[str, re.Pattern]],
    asd_pattern: Optional[re.Pattern],
    asd_window: int,
) -> List[Tuple[str, Tuple[int, int]]]:
    hits: List[Tuple[str, Tuple[int, int]]] = []
    for label, pattern in patterns:
        match = pattern.search(text)
        if match:
            hits.append((label, match.span()))

    if asd_pattern:
        match = asd_pattern.search(text)
        if match and asd_disambiguated(text, match.span(), asd_window):
            hits.append(("asd_pattern", match.span()))

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


def _new_counter_bucket() -> Dict[str, int]:
    return {
        "docs_scanned": 0,
        "docs_minlen": 0,
        "candidate_hits": 0,
        "final_hits": 0,
        "removed_domaincap": 0,
        "removed_boilerplate_signature": 0,
        "removed_boilerplate_density": 0,
        "removed_boilerplate_listiness": 0,
        "removed_boilerplate_az_index": 0,
        "removed_boilerplate_topic_hub": 0,
        "removed_boilerplate_commerce": 0,
        "removed_boilerplate_navlex": 0,
        "removed_boilerplate_directory_index": 0,
        "removed_boilerplate_total": 0,
    }


def _new_timing_bucket() -> Dict[str, float]:
    return {key: 0.0 for key in TIMING_FIELDS}


def _rate_per(numer: int, denom: int, scale: int) -> float:
    if denom <= 0:
        return 0.0
    return (numer / denom) * scale


def _summary_row(metric: str, value: object, description: str) -> List[object]:
    return [metric, value, description]


def compile_boilerplate_patterns(patterns: List[str]) -> List[re.Pattern]:
    return [re.compile(pattern, re.IGNORECASE) for pattern in patterns]


def is_boilerplate_signature(snippet: str, patterns: List[re.Pattern]) -> bool:
    return any(pattern.search(snippet) for pattern in patterns)


def is_boilerplate_density(
    snippet: str, min_snippet_words: int, min_alpha_ratio: float
) -> bool:
    words = snippet.split()
    if len(words) < min_snippet_words:
        return True
    if not snippet:
        return True
    alpha_count = sum(ch.isalpha() for ch in snippet)
    alpha_ratio = alpha_count / len(snippet)
    return alpha_ratio < min_alpha_ratio


def normalize_listiness_phrases(phrases: List[str]) -> List[str]:
    return [phrase.strip().lower() for phrase in phrases if phrase.strip()]


def likely_sentence_terminator_count(text: str) -> int:
    # Collapse ellipses/runs so menu fragments with "..." do not inflate counts.
    collapsed = re.sub(r"\.{2,}", ".", text)
    return len(re.findall(r"[.!?](?:\s|$)", collapsed))


def _count_short_fragments(snippet: str, short_fragment_max_words: int) -> int:
    fragments = [frag.strip() for frag in re.split(r"[|/>•]+", snippet) if frag.strip()]
    return sum(
        1
        for fragment in fragments
        if len(fragment.split()) <= int(short_fragment_max_words)
    )


def is_boilerplate_listiness(
    snippet: str, phrases: List[str], thresholds: Dict[str, float]
) -> bool:
    if not snippet:
        return False

    lower_snippet = snippet.lower()
    if not any(phrase in lower_snippet for phrase in phrases):
        return False

    words = snippet.split()
    separator_count = sum(snippet.count(ch) for ch in "|/>•")
    separator_density = separator_count / max(1, len(words))
    sentence_punct_count = likely_sentence_terminator_count(snippet)
    short_fragment_count = _count_short_fragments(
        snippet, int(thresholds["short_fragment_max_words"])
    )

    return (
        separator_density >= float(thresholds["min_separator_density"])
        or sentence_punct_count <= int(thresholds["max_sentence_punct"])
        or short_fragment_count >= int(thresholds["min_short_fragments"])
    )


def is_boilerplate_az_index(snippet: str, thresholds: Dict[str, float]) -> bool:
    if not snippet:
        return False
    lower_snippet = snippet.lower()
    has_az_marker = "a-z" in lower_snippet or "a to z" in lower_snippet
    letter_tokens = re.findall(r"\b[a-z]\b", lower_snippet)
    letter_token_count = len(set(letter_tokens))
    short_fragment_count = _count_short_fragments(
        snippet, int(thresholds["short_fragment_max_words"])
    )
    sentence_punct_count = likely_sentence_terminator_count(snippet)
    return (
        (has_az_marker or letter_token_count >= int(thresholds["min_letter_tokens"]))
        and short_fragment_count >= int(thresholds["min_short_fragments"])
        and sentence_punct_count <= int(thresholds["max_sentence_punct"])
    )


def _phrase_hit_count(snippet: str, phrases: List[str]) -> int:
    lower_snippet = snippet.lower()
    return sum(1 for phrase in phrases if phrase in lower_snippet)


def is_boilerplate_topic_hub(
    snippet: str, phrases: List[str], min_phrase_hits: int, max_sentence_punct: int
) -> bool:
    if not snippet:
        return False
    phrase_hits = _phrase_hit_count(snippet, phrases)
    sentence_punct_count = likely_sentence_terminator_count(snippet)
    short_fragment_count = _count_short_fragments(snippet, short_fragment_max_words=3)
    ellipsis_count = snippet.count("...")
    return phrase_hits >= min_phrase_hits and (
        sentence_punct_count <= max_sentence_punct
        or short_fragment_count >= 4
        or ellipsis_count >= 2
    )


def is_boilerplate_commerce(snippet: str, phrases: List[str], min_phrase_hits: int) -> bool:
    if not snippet:
        return False
    phrase_hits = _phrase_hit_count(snippet, phrases)
    return phrase_hits >= min_phrase_hits


def _tokenize_for_navlex(text: str) -> List[str]:
    normalized = re.sub(r"[^a-z0-9\s]", " ", text.lower())
    return [token for token in normalized.split() if token]


def nav_lexicon_hit_count(text: str, lexicon: List[str]) -> int:
    lower_text = text.lower()
    tokens = _tokenize_for_navlex(text)
    token_set = set(tokens)
    hits = 0
    for entry in lexicon:
        if " " in entry:
            if entry in lower_text:
                hits += 1
        elif entry in token_set:
            hits += 1
    return hits


def is_boilerplate_navlex(
    snippet: str,
    nav_lexicon: List[str],
    min_hits: int,
    max_sentence_terminators: int,
    min_short_fragments: int,
) -> bool:
    if not snippet:
        return False
    nav_hits = nav_lexicon_hit_count(snippet, nav_lexicon)
    if nav_hits < min_hits:
        return False
    sentence_terminators = likely_sentence_terminator_count(snippet)
    short_fragments = _count_short_fragments(snippet, short_fragment_max_words=3)
    low_prose = (
        sentence_terminators <= max_sentence_terminators
        or short_fragments >= min_short_fragments
    )
    return low_prose


def is_boilerplate_directory_index(
    snippet: str,
    phrases: List[str],
    min_phrase_hits: int,
    min_short_fragments: int,
    max_sentence_terminators: int,
) -> bool:
    if not snippet:
        return False
    phrase_hits = _phrase_hit_count(snippet, phrases)
    if phrase_hits < min_phrase_hits:
        return False
    sentence_terminators = likely_sentence_terminator_count(snippet)
    short_fragments = _count_short_fragments(snippet, short_fragment_max_words=4)
    condition_like_tokens = re.findall(r"\b[a-z][a-z\-]{3,}\b", snippet.lower())
    separator_count = sum(snippet.count(ch) for ch in "|/>•")
    separator_density = separator_count / max(1, len(snippet.split()))
    return (
        short_fragments >= min_short_fragments
        or (
            len(condition_like_tokens) >= 18
            and separator_density >= 0.08
            and sentence_terminators <= max_sentence_terminators
        )
    )


def _seed_for_run(project_seed: int, runid: str) -> int:
    digest = hashlib.sha256(f"{project_seed}:{runid}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def _add_removed_audit_row(
    removed_rows_by_reason: Dict[str, List[Dict[str, str]]],
    reason: str,
    crawl_id: str,
    url: str,
    registered_domain: str,
    matched_term: str,
    context_snippet: str,
) -> None:
    removed_rows_by_reason[reason].append(
        {
            "crawl_id": crawl_id,
            "url": url,
            "registered_domain": registered_domain,
            "matched_term": matched_term,
            "context_snippet": context_snippet,
            "removal_reason": reason,
        }
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
        if len(candidates) < sample_size:
            logger.warning(
                "Audit rows for %s: requested=%d available=%d",
                reason,
                sample_size,
                len(candidates),
            )
        reason_rng = random.Random(
            run_seed
            ^ int(hashlib.sha256(reason.encode("utf-8")).hexdigest()[:8], 16)
        )
        if len(candidates) <= sample_size:
            sampled = list(candidates)
        else:
            sampled = reason_rng.sample(candidates, sample_size)
        sampled_rows.extend(sampled)

    with audit_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "crawl_id",
                "url",
                "registered_domain",
                "matched_term",
                "context_snippet",
                "removal_reason",
            ],
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
    boilerplate_signature_patterns = compile_boilerplate_patterns(
        boilerplate_cfg.get(
            "signature_patterns", DEFAULT_BOILERPLATE_SIGNATURE_PATTERNS
        )
    )
    boilerplate_min_snippet_words = int(boilerplate_cfg.get("min_snippet_words", 8))
    boilerplate_min_alpha_ratio = float(boilerplate_cfg.get("min_alpha_ratio", 0.45))
    boilerplate_listiness_enabled = bool(boilerplate_cfg.get("listiness_enabled", True))
    boilerplate_listiness_phrases = normalize_listiness_phrases(
        boilerplate_cfg.get(
            "listiness_phrases",
            [
                "more topics",
                "browse all listings",
                "resource center",
                "categories",
                "tags",
                "topics",
                "a-z",
                "all conditions",
            ],
        )
    )
    boilerplate_listiness_thresholds = {
        "min_separator_density": float(
            boilerplate_cfg.get("listiness_thresholds", {}).get(
                "min_separator_density", 0.12
            )
        ),
        "max_sentence_punct": int(
            boilerplate_cfg.get("listiness_thresholds", {}).get(
                "max_sentence_punct", 1
            )
        ),
        "min_short_fragments": int(
            boilerplate_cfg.get("listiness_thresholds", {}).get(
                "min_short_fragments", 4
            )
        ),
        "short_fragment_max_words": int(
            boilerplate_cfg.get("listiness_thresholds", {}).get(
                "short_fragment_max_words", 3
            )
        ),
    }
    boilerplate_az_index_enabled = bool(boilerplate_cfg.get("az_index_enabled", True))
    boilerplate_az_index_thresholds = {
        "min_letter_tokens": int(
            boilerplate_cfg.get("az_index_thresholds", {}).get("min_letter_tokens", 8)
        ),
        "min_short_fragments": int(
            boilerplate_cfg.get("az_index_thresholds", {}).get("min_short_fragments", 5)
        ),
        "short_fragment_max_words": int(
            boilerplate_cfg.get("az_index_thresholds", {}).get(
                "short_fragment_max_words", 2
            )
        ),
        "max_sentence_punct": int(
            boilerplate_cfg.get("az_index_thresholds", {}).get("max_sentence_punct", 1)
        ),
    }
    boilerplate_topic_hub_enabled = bool(boilerplate_cfg.get("topic_hub_enabled", True))
    boilerplate_topic_hub_phrases = normalize_listiness_phrases(
        boilerplate_cfg.get(
            "topic_hub_phrases",
            [
                "topics",
                "browse",
                "subscribe",
                "rss",
                "no articles match",
                "all conditions",
                "a-z",
            ],
        )
    )
    boilerplate_topic_hub_min_phrase_hits = int(
        boilerplate_cfg.get("topic_hub_min_phrase_hits", 2)
    )
    boilerplate_topic_hub_max_sentence_punct = int(
        boilerplate_cfg.get("topic_hub_max_sentence_punct", 1)
    )
    boilerplate_directory_index_enabled = bool(
        boilerplate_cfg.get("directory_index_enabled", True)
    )
    boilerplate_directory_index_phrases = normalize_listiness_phrases(
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
        )
    )
    boilerplate_directory_index_min_phrase_hits = int(
        boilerplate_cfg.get("directory_index_min_phrase_hits", 2)
    )
    boilerplate_directory_index_min_short_fragments = int(
        boilerplate_cfg.get("directory_index_min_short_fragments", 4)
    )
    boilerplate_directory_index_max_sentence_terminators = int(
        boilerplate_cfg.get("directory_index_max_sentence_terminators", 2)
    )
    boilerplate_nav_lexicon = normalize_listiness_phrases(
        boilerplate_cfg.get(
            "nav_lexicon",
            [
                "home",
                "about",
                "contact",
                "privacy",
                "terms",
                "cookie",
                "subscribe",
                "rss",
                "login",
                "sign in",
                "register",
                "sitemap",
                "search",
                "menu",
                "toggle",
                "schedule",
                "newsletter",
            ],
        )
    )
    boilerplate_nav_lexicon_min_hits = int(
        boilerplate_cfg.get("nav_lexicon_min_hits", 4)
    )
    boilerplate_nav_lexicon_max_sentence_terminators = int(
        boilerplate_cfg.get("nav_lexicon_max_sentence_terminators", 1)
    )
    boilerplate_nav_lexicon_min_short_fragments = int(
        boilerplate_cfg.get("nav_lexicon_min_short_fragments", 4)
    )
    boilerplate_commerce_enabled = bool(boilerplate_cfg.get("commerce_enabled", True))
    boilerplate_commerce_phrases = normalize_listiness_phrases(
        boilerplate_cfg.get(
            "commerce_phrases",
            [
                "add to cart",
                "quick view",
                "select options",
                "free shipping",
                "in stock",
            ],
        )
    )
    boilerplate_commerce_min_phrase_hits = int(
        boilerplate_cfg.get("commerce_min_phrase_hits", 2)
    )

    terms = config["terms"]
    patterns = compile_patterns(terms)
    asd_pattern_str = terms.get("asd_pattern")
    asd_pattern = (
        re.compile(asd_pattern_str, re.IGNORECASE) if asd_pattern_str else None
    )

    logger.info("Loaded config %s", config_path)
    logger.info("Min chars: %d", min_chars)
    logger.info("Domain cap: %d", domain_cap)
    logger.info("ASD window: %d", asd_window)
    logger.info("Context window: %d", context_window_chars)
    logger.info("Boilerplate enabled: %s", boilerplate_enabled)
    logger.info("Boilerplate check window: %d", boilerplate_check_window_chars)
    logger.info("Boilerplate listiness enabled: %s", boilerplate_listiness_enabled)
    logger.info("Boilerplate A-Z index enabled: %s", boilerplate_az_index_enabled)
    logger.info("Boilerplate topic hub enabled: %s", boilerplate_topic_hub_enabled)
    logger.info(
        "Boilerplate directory index enabled: %s", boilerplate_directory_index_enabled
    )
    logger.info("Boilerplate commerce enabled: %s", boilerplate_commerce_enabled)

    wet_dir = Path("data/raw/wet")
    wet_files = sorted(wet_dir.glob("*.wet.gz"))
    if not wet_files:
        raise FileNotFoundError("No .wet.gz files found in data/raw/wet")

    combined = _new_counter_bucket()
    counters_by_crawl: Dict[str, Dict[str, int]] = defaultdict(_new_counter_bucket)
    boilerplate_reasons = sorted(
        [
            key
            for key in combined
            if key.startswith("removed_boilerplate_")
            and key != "removed_boilerplate_total"
        ]
    )
    removed_rows_by_reason: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    combined_timings = _new_timing_bucket()
    timings_by_crawl: Dict[str, Dict[str, float]] = defaultdict(_new_timing_bucket)
    elapsed_by_crawl: Dict[str, float] = defaultdict(float)
    domains_hits: set[str] = set()
    domain_hit_counts: Dict[Tuple[str, str], int] = defaultdict(int)
    top_domains_counter: Counter = Counter()

    hit_rows: List[Dict[str, str]] = []

    for wet_path in wet_files:
        source_wet = wet_path.name
        crawl_id = source_wet.split("_")[0]
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
                text_len = len(text)
                if text_len < min_chars:
                    continue
                combined["docs_minlen"] += 1
                crawl_counters["docs_minlen"] += 1

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

                for label, span in matches:
                    stored_snippet = _context_snippet(text, span, context_window_chars)
                    check_text = _context_snippet(
                        text, span, boilerplate_check_window_chars
                    )
                    if boilerplate_enabled:
                        if is_boilerplate_signature(
                            check_text, boilerplate_signature_patterns
                        ):
                            _record_boilerplate_removal(
                                combined,
                                crawl_counters,
                                "removed_boilerplate_signature",
                            )
                            _add_removed_audit_row(
                                removed_rows_by_reason,
                                "removed_boilerplate_signature",
                                crawl_id,
                                url or "",
                                domain,
                                label,
                                stored_snippet,
                            )
                            continue
                        if is_boilerplate_density(
                            check_text,
                            boilerplate_min_snippet_words,
                            boilerplate_min_alpha_ratio,
                        ):
                            _record_boilerplate_removal(
                                combined,
                                crawl_counters,
                                "removed_boilerplate_density",
                            )
                            _add_removed_audit_row(
                                removed_rows_by_reason,
                                "removed_boilerplate_density",
                                crawl_id,
                                url or "",
                                domain,
                                label,
                                stored_snippet,
                            )
                            continue
                        if boilerplate_listiness_enabled and is_boilerplate_listiness(
                            check_text,
                            boilerplate_listiness_phrases,
                            boilerplate_listiness_thresholds,
                        ):
                            _record_boilerplate_removal(
                                combined,
                                crawl_counters,
                                "removed_boilerplate_listiness",
                            )
                            _add_removed_audit_row(
                                removed_rows_by_reason,
                                "removed_boilerplate_listiness",
                                crawl_id,
                                url or "",
                                domain,
                                label,
                                stored_snippet,
                            )
                            continue
                        if boilerplate_az_index_enabled and is_boilerplate_az_index(
                            check_text, boilerplate_az_index_thresholds
                        ):
                            _record_boilerplate_removal(
                                combined,
                                crawl_counters,
                                "removed_boilerplate_az_index",
                            )
                            _add_removed_audit_row(
                                removed_rows_by_reason,
                                "removed_boilerplate_az_index",
                                crawl_id,
                                url or "",
                                domain,
                                label,
                                stored_snippet,
                            )
                            continue
                        if boilerplate_topic_hub_enabled and is_boilerplate_topic_hub(
                            check_text,
                            boilerplate_topic_hub_phrases,
                            boilerplate_topic_hub_min_phrase_hits,
                            boilerplate_topic_hub_max_sentence_punct,
                        ):
                            _record_boilerplate_removal(
                                combined,
                                crawl_counters,
                                "removed_boilerplate_topic_hub",
                            )
                            _add_removed_audit_row(
                                removed_rows_by_reason,
                                "removed_boilerplate_topic_hub",
                                crawl_id,
                                url or "",
                                domain,
                                label,
                                stored_snippet,
                            )
                            continue
                        if is_boilerplate_navlex(
                            check_text,
                            boilerplate_nav_lexicon,
                            boilerplate_nav_lexicon_min_hits,
                            boilerplate_nav_lexicon_max_sentence_terminators,
                            boilerplate_nav_lexicon_min_short_fragments,
                        ):
                            _record_boilerplate_removal(
                                combined,
                                crawl_counters,
                                "removed_boilerplate_navlex",
                            )
                            _add_removed_audit_row(
                                removed_rows_by_reason,
                                "removed_boilerplate_navlex",
                                crawl_id,
                                url or "",
                                domain,
                                label,
                                stored_snippet,
                            )
                            continue
                        if (
                            boilerplate_directory_index_enabled
                            and is_boilerplate_directory_index(
                                check_text,
                                boilerplate_directory_index_phrases,
                                boilerplate_directory_index_min_phrase_hits,
                                boilerplate_directory_index_min_short_fragments,
                                boilerplate_directory_index_max_sentence_terminators,
                            )
                        ):
                            _record_boilerplate_removal(
                                combined,
                                crawl_counters,
                                "removed_boilerplate_directory_index",
                            )
                            _add_removed_audit_row(
                                removed_rows_by_reason,
                                "removed_boilerplate_directory_index",
                                crawl_id,
                                url or "",
                                domain,
                                label,
                                stored_snippet,
                            )
                            continue
                        if boilerplate_commerce_enabled and is_boilerplate_commerce(
                            check_text,
                            boilerplate_commerce_phrases,
                            boilerplate_commerce_min_phrase_hits,
                        ):
                            _record_boilerplate_removal(
                                combined,
                                crawl_counters,
                                "removed_boilerplate_commerce",
                            )
                            _add_removed_audit_row(
                                removed_rows_by_reason,
                                "removed_boilerplate_commerce",
                                crawl_id,
                                url or "",
                                domain,
                                label,
                                stored_snippet,
                            )
                            continue

                    if domain:
                        key = (crawl_id, domain)
                        if domain_hit_counts[key] >= domain_cap:
                            combined["removed_domaincap"] += 1
                            crawl_counters["removed_domaincap"] += 1
                            continue
                        domain_hit_counts[key] += 1

                    combined["final_hits"] += 1
                    crawl_counters["final_hits"] += 1
                    if domain:
                        domains_hits.add(domain)
                        top_domains_counter[domain] += 1

                    write_start = time.perf_counter()
                    hit_rows.append(
                        {
                            "crawl_id": crawl_id,
                            "source_wet": source_wet,
                            "url": url or "",
                            "registered_domain": domain,
                            "warc_date": warc_date or "",
                            "matched_term": label,
                            "context_snippet": stored_snippet,
                            "text_len": text_len,
                        }
                    )
                    write_elapsed = time.perf_counter() - write_start
                    combined_timings["time_write_sec"] += write_elapsed
                    crawl_timings["time_write_sec"] += write_elapsed

        elapsed_by_crawl[crawl_id] += time.perf_counter() - file_start

    out_dir = Path(config.get("project", {}).get("out_dir", "data/interim"))
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_path = out_dir / f"cc_scan_summary_{runid}.csv"
    top_domains_path = out_dir / f"cc_scan_top_domains_{runid}.csv"
    parquet_path = out_dir / f"cc_pilot_corpus_{runid}.parquet"

    write_start = time.perf_counter()
    with top_domains_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["registered_domain", "hits"])
        for domain, count in top_domains_counter.most_common(10):
            writer.writerow([domain, count])
    write_elapsed = time.perf_counter() - write_start
    combined_timings["time_write_sec"] += write_elapsed

    df = pd.DataFrame(
        hit_rows,
        columns=[
            "crawl_id",
            "source_wet",
            "url",
            "registered_domain",
            "warc_date",
            "matched_term",
            "context_snippet",
            "text_len",
        ],
    )
    write_start = time.perf_counter()
    df.to_parquet(parquet_path, index=False)
    write_elapsed = time.perf_counter() - write_start
    combined_timings["time_write_sec"] += write_elapsed

    total_elapsed_sec = time.perf_counter() - scan_start
    docs_per_sec = (
        combined["docs_scanned"] / total_elapsed_sec if total_elapsed_sec > 0 else 0.0
    )
    docs_per_sec_by_crawl = {
        crawl_id: (
            counters["docs_scanned"] / elapsed_by_crawl[crawl_id]
            if elapsed_by_crawl[crawl_id] > 0
            else 0.0
        )
        for crawl_id, counters in counters_by_crawl.items()
    }
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
            "candidate_hits",
            combined["candidate_hits"],
            "Term matches before downstream filtering and domain caps.",
        ),
        _summary_row(
            "final_hits",
            combined["final_hits"],
            "Rows retained after filtering and domain-cap enforcement.",
        ),
        _summary_row(
            "candidate_per_10k",
            round(_rate_per(combined["candidate_hits"], combined["docs_scanned"], 10_000), 6),
            "Combined candidate hit rate per 10,000 scanned documents.",
        ),
        _summary_row(
            "final_per_10k",
            round(_rate_per(combined["final_hits"], combined["docs_scanned"], 10_000), 6),
            "Combined final hit rate per 10,000 scanned documents.",
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
        slice_candidate_hits = counters_by_crawl[crawl_id]["candidate_hits"]
        slice_final_hits = counters_by_crawl[crawl_id]["final_hits"]
        prefix = f"slice.{crawl_id}"
        summary_rows.extend(
            [
                _summary_row(
                    f"{prefix}.docs_scanned",
                    slice_docs_scanned,
                    "Slice-level scanned documents for this crawl_id.",
                ),
                _summary_row(
                    f"{prefix}.candidate_hits",
                    slice_candidate_hits,
                    "Slice-level candidate matches before final filtering.",
                ),
                _summary_row(
                    f"{prefix}.final_hits",
                    slice_final_hits,
                    "Slice-level final retained hits after filtering.",
                ),
                _summary_row(
                    f"{prefix}.candidate_per_10k",
                    round(_rate_per(slice_candidate_hits, slice_docs_scanned, 10_000), 6),
                    "Slice-level candidate hit rate per 10,000 scanned documents.",
                ),
                _summary_row(
                    f"{prefix}.final_per_10k",
                    round(_rate_per(slice_final_hits, slice_docs_scanned, 10_000), 6),
                    "Slice-level final hit rate per 10,000 scanned documents.",
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

    write_start = time.perf_counter()
    with summary_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value", "description"])
        writer.writerows(summary_rows)
    write_elapsed = time.perf_counter() - write_start
    combined_timings["time_write_sec"] += write_elapsed

    removed_audit_path = _write_removed_audit_csv(
        out_dir=out_dir,
        runid=runid,
        project_seed=project_seed,
        removed_rows_by_reason=removed_rows_by_reason,
        reasons=boilerplate_reasons,
        logger=logger,
    )

    logger.info("Wrote summary %s", summary_path)
    logger.info("Wrote top domains %s", top_domains_path)
    logger.info("Wrote corpus %s", parquet_path)
    logger.info("Wrote removed-audit sample %s", removed_audit_path)

    print(
        "Scan complete: "
        f"docs_scanned={combined['docs_scanned']}, "
        f"docs_minlen={combined['docs_minlen']}, "
        f"candidate_hits={combined['candidate_hits']}, "
        f"final_hits={combined['final_hits']}, "
        f"unique_domains_hits={len(domains_hits)}"
    )
    return summary_path
