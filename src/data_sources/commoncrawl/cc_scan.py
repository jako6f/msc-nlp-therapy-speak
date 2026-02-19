import csv
import gzip
import json
import logging
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
    sentence_punct_count = sum(snippet.count(ch) for ch in ".!?")
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
    sentence_punct_count = sum(snippet.count(ch) for ch in ".!?")
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
    sentence_punct_count = sum(snippet.count(ch) for ch in ".!?")
    return phrase_hits >= min_phrase_hits and sentence_punct_count <= max_sentence_punct


def is_boilerplate_commerce(snippet: str, phrases: List[str], min_phrase_hits: int) -> bool:
    if not snippet:
        return False
    phrase_hits = _phrase_hit_count(snippet, phrases)
    return phrase_hits >= min_phrase_hits


def scan_wet_files(config: Dict, config_path: Path) -> Path:
    scan_start = time.perf_counter()
    runid = _utc_runid()
    logger = _setup_logger(Path("reports/logs"), runid)

    min_chars = int(config["filters"]["min_chars"])
    domain_cap = int(config["filters"]["domain_cap"])
    asd_window = int(config["filters"]["asd_disambiguation_window_chars"])
    context_window_chars = int(config["filters"].get("context_window_chars", 200))
    boilerplate_cfg = config.get("boilerplate", {})
    boilerplate_enabled = bool(boilerplate_cfg.get("enabled", True))
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
    logger.info("Boilerplate listiness enabled: %s", boilerplate_listiness_enabled)
    logger.info("Boilerplate A-Z index enabled: %s", boilerplate_az_index_enabled)
    logger.info("Boilerplate topic hub enabled: %s", boilerplate_topic_hub_enabled)
    logger.info("Boilerplate commerce enabled: %s", boilerplate_commerce_enabled)

    wet_dir = Path("data/raw/wet")
    wet_files = sorted(wet_dir.glob("*.wet.gz"))
    if not wet_files:
        raise FileNotFoundError("No .wet.gz files found in data/raw/wet")

    combined = _new_counter_bucket()
    counters_by_crawl: Dict[str, Dict[str, int]] = defaultdict(_new_counter_bucket)
    combined_timings = _new_timing_bucket()
    timings_by_crawl: Dict[str, Dict[str, float]] = defaultdict(_new_timing_bucket)
    elapsed_by_crawl: Dict[str, float] = defaultdict(float)
    hits_by_term: Counter = Counter()
    domains_total: set[str] = set()
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
                if domain:
                    domains_total.add(domain)

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
                    snippet = _context_snippet(text, span, context_window_chars)
                    if boilerplate_enabled:
                        if is_boilerplate_signature(
                            snippet, boilerplate_signature_patterns
                        ):
                            combined["removed_boilerplate_signature"] += 1
                            crawl_counters["removed_boilerplate_signature"] += 1
                            combined["removed_boilerplate_total"] += 1
                            crawl_counters["removed_boilerplate_total"] += 1
                            continue
                        if is_boilerplate_density(
                            snippet,
                            boilerplate_min_snippet_words,
                            boilerplate_min_alpha_ratio,
                        ):
                            combined["removed_boilerplate_density"] += 1
                            crawl_counters["removed_boilerplate_density"] += 1
                            combined["removed_boilerplate_total"] += 1
                            crawl_counters["removed_boilerplate_total"] += 1
                            continue
                        if boilerplate_listiness_enabled and is_boilerplate_listiness(
                            snippet,
                            boilerplate_listiness_phrases,
                            boilerplate_listiness_thresholds,
                        ):
                            combined["removed_boilerplate_listiness"] += 1
                            crawl_counters["removed_boilerplate_listiness"] += 1
                            combined["removed_boilerplate_total"] += 1
                            crawl_counters["removed_boilerplate_total"] += 1
                            continue
                        if boilerplate_az_index_enabled and is_boilerplate_az_index(
                            snippet, boilerplate_az_index_thresholds
                        ):
                            combined["removed_boilerplate_az_index"] += 1
                            crawl_counters["removed_boilerplate_az_index"] += 1
                            combined["removed_boilerplate_total"] += 1
                            crawl_counters["removed_boilerplate_total"] += 1
                            continue
                        if boilerplate_topic_hub_enabled and is_boilerplate_topic_hub(
                            snippet,
                            boilerplate_topic_hub_phrases,
                            boilerplate_topic_hub_min_phrase_hits,
                            boilerplate_topic_hub_max_sentence_punct,
                        ):
                            combined["removed_boilerplate_topic_hub"] += 1
                            crawl_counters["removed_boilerplate_topic_hub"] += 1
                            combined["removed_boilerplate_total"] += 1
                            crawl_counters["removed_boilerplate_total"] += 1
                            continue
                        if boilerplate_commerce_enabled and is_boilerplate_commerce(
                            snippet,
                            boilerplate_commerce_phrases,
                            boilerplate_commerce_min_phrase_hits,
                        ):
                            combined["removed_boilerplate_commerce"] += 1
                            crawl_counters["removed_boilerplate_commerce"] += 1
                            combined["removed_boilerplate_total"] += 1
                            crawl_counters["removed_boilerplate_total"] += 1
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
                    hits_by_term[label] += 1
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
                            "context_snippet": snippet,
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
        for domain, count in top_domains_counter.most_common(25):
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
    docs_scanned_by_crawl = {
        crawl_id: counters["docs_scanned"]
        for crawl_id, counters in sorted(counters_by_crawl.items())
    }
    docs_minlen_by_crawl = {
        crawl_id: counters["docs_minlen"]
        for crawl_id, counters in sorted(counters_by_crawl.items())
    }
    candidate_hits_by_crawl = {
        crawl_id: counters["candidate_hits"]
        for crawl_id, counters in sorted(counters_by_crawl.items())
    }
    final_hits_by_crawl = {
        crawl_id: counters["final_hits"]
        for crawl_id, counters in sorted(counters_by_crawl.items())
    }
    removed_domaincap_by_crawl = {
        crawl_id: counters["removed_domaincap"]
        for crawl_id, counters in sorted(counters_by_crawl.items())
    }
    removed_boilerplate_signature_by_crawl = {
        crawl_id: counters["removed_boilerplate_signature"]
        for crawl_id, counters in sorted(counters_by_crawl.items())
    }
    removed_boilerplate_density_by_crawl = {
        crawl_id: counters["removed_boilerplate_density"]
        for crawl_id, counters in sorted(counters_by_crawl.items())
    }
    removed_boilerplate_listiness_by_crawl = {
        crawl_id: counters["removed_boilerplate_listiness"]
        for crawl_id, counters in sorted(counters_by_crawl.items())
    }
    removed_boilerplate_az_index_by_crawl = {
        crawl_id: counters["removed_boilerplate_az_index"]
        for crawl_id, counters in sorted(counters_by_crawl.items())
    }
    removed_boilerplate_topic_hub_by_crawl = {
        crawl_id: counters["removed_boilerplate_topic_hub"]
        for crawl_id, counters in sorted(counters_by_crawl.items())
    }
    removed_boilerplate_commerce_by_crawl = {
        crawl_id: counters["removed_boilerplate_commerce"]
        for crawl_id, counters in sorted(counters_by_crawl.items())
    }
    removed_boilerplate_total_by_crawl = {
        crawl_id: counters["removed_boilerplate_total"]
        for crawl_id, counters in sorted(counters_by_crawl.items())
    }
    timings_by_crawl_json = {
        crawl_id: {
            **{field: round(crawl_timings[field], 6) for field in TIMING_FIELDS},
            "total_elapsed_sec": round(elapsed_by_crawl.get(crawl_id, 0.0), 6),
        }
        for crawl_id, crawl_timings in timings_by_crawl.items()
    }
    combined_timings_with_total = {
        **{field: round(combined_timings[field], 6) for field in TIMING_FIELDS},
        "total_elapsed_sec": round(total_elapsed_sec, 6),
    }

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
            "hits_total",
            combined["final_hits"],
            "Legacy alias for final_hits to preserve downstream compatibility.",
        ),
        _summary_row(
            "hits_by_term",
            json.dumps(hits_by_term),
            "JSON map from matched term label to retained hit count.",
        ),
        _summary_row(
            "unique_domains_total",
            len(domains_total),
            "Unique registered domains seen in all scanned documents.",
        ),
        _summary_row(
            "unique_domains_hits",
            len(domains_hits),
            "Unique registered domains represented in final retained hits.",
        ),
        _summary_row(
            "removed_domaincap",
            combined["removed_domaincap"],
            "Candidate matches removed because the per-domain cap was reached.",
        ),
        _summary_row(
            "capped_removed",
            combined["removed_domaincap"],
            "Legacy alias for removed_domaincap to preserve compatibility.",
        ),
        _summary_row(
            "removed_boilerplate_signature",
            combined["removed_boilerplate_signature"],
            "Count of hits removed by boilerplate signature patterns.",
        ),
        _summary_row(
            "removed_boilerplate_density",
            combined["removed_boilerplate_density"],
            "Count of hits removed by boilerplate density thresholds.",
        ),
        _summary_row(
            "removed_boilerplate_listiness",
            combined["removed_boilerplate_listiness"],
            "Count of hits removed by boilerplate listiness/taxonomy heuristics.",
        ),
        _summary_row(
            "removed_boilerplate_az_index",
            combined["removed_boilerplate_az_index"],
            "Count of hits removed by A-Z/index-style snippet heuristics.",
        ),
        _summary_row(
            "removed_boilerplate_topic_hub",
            combined["removed_boilerplate_topic_hub"],
            "Count of hits removed by topic-hub snippet heuristics.",
        ),
        _summary_row(
            "removed_boilerplate_commerce",
            combined["removed_boilerplate_commerce"],
            "Count of hits removed by commerce-listing snippet heuristics.",
        ),
        _summary_row(
            "removed_boilerplate_total",
            combined["removed_boilerplate_total"],
            "Total hits removed by boilerplate filtering before final output.",
        ),
        _summary_row(
            "top_domains_csv",
            str(top_domains_path),
            "Path to the CSV containing top retained domains for this run.",
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

    summary_rows.extend(
        [
            _summary_row(
                "docs_scanned_by_crawl",
                json.dumps(docs_scanned_by_crawl),
                "JSON map from crawl_id to scanned document count.",
            ),
            _summary_row(
                "docs_minlen_by_crawl",
                json.dumps(docs_minlen_by_crawl),
                "JSON map from crawl_id to documents passing min length.",
            ),
            _summary_row(
                "candidate_hits_by_crawl",
                json.dumps(candidate_hits_by_crawl),
                "JSON map from crawl_id to candidate term matches.",
            ),
            _summary_row(
                "final_hits_by_crawl",
                json.dumps(final_hits_by_crawl),
                "JSON map from crawl_id to final retained hits.",
            ),
            _summary_row(
                "removed_domaincap_by_crawl",
                json.dumps(removed_domaincap_by_crawl),
                "JSON map from crawl_id to matches removed by domain cap.",
            ),
            _summary_row(
                "removed_boilerplate_signature_by_crawl",
                json.dumps(removed_boilerplate_signature_by_crawl),
                "JSON map from crawl_id to signature-based boilerplate removals.",
            ),
            _summary_row(
                "removed_boilerplate_density_by_crawl",
                json.dumps(removed_boilerplate_density_by_crawl),
                "JSON map from crawl_id to density-based boilerplate removals.",
            ),
            _summary_row(
                "removed_boilerplate_listiness_by_crawl",
                json.dumps(removed_boilerplate_listiness_by_crawl),
                "JSON map from crawl_id to listiness/taxonomy boilerplate removals.",
            ),
            _summary_row(
                "removed_boilerplate_az_index_by_crawl",
                json.dumps(removed_boilerplate_az_index_by_crawl),
                "JSON map from crawl_id to A-Z/index boilerplate removals.",
            ),
            _summary_row(
                "removed_boilerplate_topic_hub_by_crawl",
                json.dumps(removed_boilerplate_topic_hub_by_crawl),
                "JSON map from crawl_id to topic-hub boilerplate removals.",
            ),
            _summary_row(
                "removed_boilerplate_commerce_by_crawl",
                json.dumps(removed_boilerplate_commerce_by_crawl),
                "JSON map from crawl_id to commerce boilerplate removals.",
            ),
            _summary_row(
                "removed_boilerplate_total_by_crawl",
                json.dumps(removed_boilerplate_total_by_crawl),
                "JSON map from crawl_id to total boilerplate removals.",
            ),
            _summary_row(
                "docs_per_sec_by_crawl",
                json.dumps(
                    {
                        crawl_id: round(rate, 6)
                        for crawl_id, rate in sorted(docs_per_sec_by_crawl.items())
                    }
                ),
                "JSON map from crawl_id to scan throughput in docs per second.",
            ),
            _summary_row(
                "timings_sec",
                json.dumps(combined_timings_with_total),
                "JSON object of combined timing breakdown in seconds.",
            ),
            _summary_row(
                "timings_sec_by_crawl",
                json.dumps(timings_by_crawl_json),
                "JSON map from crawl_id to timing breakdown in seconds.",
            ),
        ]
    )

    summary_rows.extend(
        [
            _summary_row(
                "combined.docs_scanned",
                combined["docs_scanned"],
                "Combined scanned documents across all slices.",
            ),
            _summary_row(
                "combined.candidate_hits",
                combined["candidate_hits"],
                "Combined candidate term matches across all slices.",
            ),
            _summary_row(
                "combined.final_hits",
                combined["final_hits"],
                "Combined retained final hits across all slices.",
            ),
            _summary_row(
                "combined.removed_boilerplate_signature",
                combined["removed_boilerplate_signature"],
                "Combined signature-based boilerplate removals across slices.",
            ),
            _summary_row(
                "combined.removed_boilerplate_density",
                combined["removed_boilerplate_density"],
                "Combined density-based boilerplate removals across slices.",
            ),
            _summary_row(
                "combined.removed_boilerplate_listiness",
                combined["removed_boilerplate_listiness"],
                "Combined listiness/taxonomy boilerplate removals across slices.",
            ),
            _summary_row(
                "combined.removed_boilerplate_az_index",
                combined["removed_boilerplate_az_index"],
                "Combined A-Z/index boilerplate removals across slices.",
            ),
            _summary_row(
                "combined.removed_boilerplate_topic_hub",
                combined["removed_boilerplate_topic_hub"],
                "Combined topic-hub boilerplate removals across slices.",
            ),
            _summary_row(
                "combined.removed_boilerplate_commerce",
                combined["removed_boilerplate_commerce"],
                "Combined commerce boilerplate removals across slices.",
            ),
            _summary_row(
                "combined.removed_boilerplate_total",
                combined["removed_boilerplate_total"],
                "Combined total boilerplate removals across slices.",
            ),
            _summary_row(
                "combined.candidate_per_10k",
                round(_rate_per(combined["candidate_hits"], combined["docs_scanned"], 10_000), 6),
                "Combined candidate hit rate per 10,000 scanned documents.",
            ),
            _summary_row(
                "combined.final_per_10k",
                round(_rate_per(combined["final_hits"], combined["docs_scanned"], 10_000), 6),
                "Combined final hit rate per 10,000 scanned documents.",
            ),
        ]
    )

    for crawl_id in sorted(counters_by_crawl):
        slice_docs_scanned = docs_scanned_by_crawl.get(crawl_id, 0)
        slice_candidate_hits = candidate_hits_by_crawl.get(crawl_id, 0)
        slice_final_hits = final_hits_by_crawl.get(crawl_id, 0)
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
                    f"{prefix}.removed_boilerplate_signature",
                    counters_by_crawl[crawl_id]["removed_boilerplate_signature"],
                    "Slice-level signature-based boilerplate removals.",
                ),
                _summary_row(
                    f"{prefix}.removed_boilerplate_density",
                    counters_by_crawl[crawl_id]["removed_boilerplate_density"],
                    "Slice-level density-based boilerplate removals.",
                ),
                _summary_row(
                    f"{prefix}.removed_boilerplate_listiness",
                    counters_by_crawl[crawl_id]["removed_boilerplate_listiness"],
                    "Slice-level listiness/taxonomy boilerplate removals.",
                ),
                _summary_row(
                    f"{prefix}.removed_boilerplate_az_index",
                    counters_by_crawl[crawl_id]["removed_boilerplate_az_index"],
                    "Slice-level A-Z/index boilerplate removals.",
                ),
                _summary_row(
                    f"{prefix}.removed_boilerplate_topic_hub",
                    counters_by_crawl[crawl_id]["removed_boilerplate_topic_hub"],
                    "Slice-level topic-hub boilerplate removals.",
                ),
                _summary_row(
                    f"{prefix}.removed_boilerplate_commerce",
                    counters_by_crawl[crawl_id]["removed_boilerplate_commerce"],
                    "Slice-level commerce boilerplate removals.",
                ),
                _summary_row(
                    f"{prefix}.removed_boilerplate_total",
                    counters_by_crawl[crawl_id]["removed_boilerplate_total"],
                    "Slice-level total boilerplate removals.",
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
            ]
        )

    write_start = time.perf_counter()
    with summary_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value", "description"])
        writer.writerows(summary_rows)
    write_elapsed = time.perf_counter() - write_start
    combined_timings["time_write_sec"] += write_elapsed

    logger.info("Wrote summary %s", summary_path)
    logger.info("Wrote top domains %s", top_domains_path)
    logger.info("Wrote corpus %s", parquet_path)

    print(
        "Scan complete: "
        f"docs_scanned={combined['docs_scanned']}, "
        f"docs_minlen={combined['docs_minlen']}, "
        f"candidate_hits={combined['candidate_hits']}, "
        f"final_hits={combined['final_hits']}, "
        f"unique_domains_hits={len(domains_hits)}"
    )
    return summary_path
