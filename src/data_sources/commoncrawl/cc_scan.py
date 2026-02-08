import csv
import gzip
import json
import logging
import re
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import tempfile
import tldextract
from warcio.archiveiterator import ArchiveIterator


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


def scan_wet_files(config: Dict, config_path: Path) -> Path:
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

    docs_scanned = 0
    docs_minlen = 0
    hits_total = 0
    capped_removed = 0
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

        for url, warc_date, text in iter_wet_records(wet_path):
            docs_scanned += 1
            text_len = len(text)
            if text_len < min_chars:
                continue
            docs_minlen += 1

            domain = extract_registered_domain(url)
            if domain:
                domains_total.add(domain)

            matches = find_term_matches(text, patterns, asd_pattern, asd_window)
            if not matches:
                continue

            for label, span in matches:
                if domain:
                    key = (crawl_id, domain)
                    if domain_hit_counts[key] >= domain_cap:
                        capped_removed += 1
                        continue
                    domain_hit_counts[key] += 1

                hits_total += 1
                hits_by_term[label] += 1
                if domain:
                    domains_hits.add(domain)
                    top_domains_counter[domain] += 1

                hit_rows.append(
                    {
                        "crawl_id": crawl_id,
                        "source_wet": source_wet,
                        "url": url or "",
                        "registered_domain": domain,
                        "warc_date": warc_date or "",
                        "matched_term": label,
                        "context_snippet": _context_snippet(text, span, asd_window),
                        "text_len": text_len,
                    }
                )

    out_dir = Path(config.get("project", {}).get("out_dir", "data/interim"))
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_path = out_dir / f"cc_scan_summary_{runid}.csv"
    top_domains_path = out_dir / f"cc_scan_top_domains_{runid}.csv"
    parquet_path = out_dir / f"cc_pilot_corpus_{runid}.parquet"

    with summary_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value"])
        writer.writerow(["docs_scanned", docs_scanned])
        writer.writerow(["docs_minlen", docs_minlen])
        writer.writerow(["hits_total", hits_total])
        writer.writerow(["hits_by_term", json.dumps(hits_by_term)])
        writer.writerow(["unique_domains_total", len(domains_total)])
        writer.writerow(["unique_domains_hits", len(domains_hits)])
        writer.writerow(["capped_removed", capped_removed])
        writer.writerow(["top_domains_csv", str(top_domains_path)])

    with top_domains_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["registered_domain", "hits"])
        for domain, count in top_domains_counter.most_common(25):
            writer.writerow([domain, count])

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
    df.to_parquet(parquet_path, index=False)

    logger.info("Wrote summary %s", summary_path)
    logger.info("Wrote top domains %s", top_domains_path)
    logger.info("Wrote corpus %s", parquet_path)

    print(
        "Scan complete: "
        f"docs_scanned={docs_scanned}, "
        f"docs_minlen={docs_minlen}, "
        f"hits_total={hits_total}, "
        f"unique_domains_hits={len(domains_hits)}"
    )
    return summary_path
