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
        "removed_boilerplate_total": 0,
    }


def _new_timing_bucket() -> Dict[str, float]:
    return {key: 0.0 for key in TIMING_FIELDS}


def scan_wet_files(config: Dict, config_path: Path) -> Path:
    scan_start = time.perf_counter()
    runid = _utc_runid()
    logger = _setup_logger(Path("reports/logs"), runid)

    min_chars = int(config["filters"]["min_chars"])
    domain_cap = int(config["filters"]["domain_cap"])
    asd_window = int(config["filters"]["asd_disambiguation_window_chars"])

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
                            "context_snippet": _context_snippet(
                                text, span, asd_window
                            ),
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
    with summary_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value"])
        writer.writerow(["docs_scanned", combined["docs_scanned"]])
        writer.writerow(["docs_minlen", combined["docs_minlen"]])
        writer.writerow(["candidate_hits", combined["candidate_hits"]])
        writer.writerow(["final_hits", combined["final_hits"]])
        writer.writerow(["hits_total", combined["final_hits"]])
        writer.writerow(["hits_by_term", json.dumps(hits_by_term)])
        writer.writerow(["unique_domains_total", len(domains_total)])
        writer.writerow(["unique_domains_hits", len(domains_hits)])
        writer.writerow(["removed_domaincap", combined["removed_domaincap"]])
        writer.writerow(["capped_removed", combined["removed_domaincap"]])
        writer.writerow(
            ["removed_boilerplate_signature", combined["removed_boilerplate_signature"]]
        )
        writer.writerow(
            ["removed_boilerplate_density", combined["removed_boilerplate_density"]]
        )
        writer.writerow(
            ["removed_boilerplate_total", combined["removed_boilerplate_total"]]
        )
        writer.writerow(["top_domains_csv", str(top_domains_path)])
    write_elapsed = time.perf_counter() - write_start
    combined_timings["time_write_sec"] += write_elapsed

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

    with summary_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["docs_per_sec", round(docs_per_sec, 6)])
        writer.writerow(["total_elapsed_sec", round(total_elapsed_sec, 6)])
        for field in TIMING_FIELDS:
            writer.writerow([field, round(combined_timings[field], 6)])
        writer.writerow(
            [
                "docs_scanned_by_crawl",
                json.dumps(docs_scanned_by_crawl),
            ]
        )
        writer.writerow(
            [
                "docs_minlen_by_crawl",
                json.dumps(docs_minlen_by_crawl),
            ]
        )
        writer.writerow(
            [
                "candidate_hits_by_crawl",
                json.dumps(candidate_hits_by_crawl),
            ]
        )
        writer.writerow(
            [
                "final_hits_by_crawl",
                json.dumps(final_hits_by_crawl),
            ]
        )
        writer.writerow(
            [
                "removed_domaincap_by_crawl",
                json.dumps(removed_domaincap_by_crawl),
            ]
        )
        writer.writerow(
            [
                "docs_per_sec_by_crawl",
                json.dumps(
                    {
                        crawl_id: round(rate, 6)
                        for crawl_id, rate in sorted(docs_per_sec_by_crawl.items())
                    }
                ),
            ]
        )
        writer.writerow(["timings_sec", json.dumps(combined_timings_with_total)])
        writer.writerow(["timings_sec_by_crawl", json.dumps(timings_by_crawl_json)])

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
