import json
import re
from pathlib import Path
from typing import Dict, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd

from src.pathing import reports_figures_base, reports_namespace, reports_tables_base
from src.analysis.summary_schema import with_warc_metric_defaults

RUNID_PATTERN = re.compile(r".*_(\d{8}_\d{6})\.(csv|parquet)$")


def _latest_runid(interim_dir: Path) -> str:
    candidates = []
    for path in interim_dir.glob("cc_scan_summary_*.csv"):
        match = RUNID_PATTERN.match(path.name)
        if match:
            candidates.append(match.group(1))
    if not candidates:
        raise FileNotFoundError("No cc_scan_summary_*.csv files found in data/interim")
    return sorted(candidates)[-1]


def _load_summary(summary_path: Path) -> Dict[str, object]:
    df = pd.read_csv(summary_path)
    return with_warc_metric_defaults(dict(zip(df["metric"], df["value"])))


def _validated_hits_wet_path(interim_dir: Path, runid: str) -> Path:
    canonical = interim_dir / f"cc_validated_hits_wet_{runid}.parquet"
    if canonical.exists():
        return canonical
    return interim_dir / f"cc_pilot_corpus_{runid}.parquet"


def _write_latex_table(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def _format_int(value: str) -> str:
    return f"{int(value):,}"


def _format_rate(numer: int, denom: int) -> str:
    if denom == 0:
        return "0.000"
    return f"{numer / denom:.3f}"


def _format_optional_rate(value: Optional[float]) -> str:
    if value is None:
        return "NA"
    return f"{value:.3f}"


def export_tables_and_figures(config: Dict, interim_dir: Path) -> Tuple[Path, Path, Path]:
    runid = _latest_runid(interim_dir)
    summary_path = interim_dir / f"cc_scan_summary_{runid}.csv"
    top_domains_path = interim_dir / f"cc_scan_top_domains_{runid}.csv"
    corpus_path = _validated_hits_wet_path(interim_dir, runid)

    summary = _load_summary(summary_path)
    validated_hits_wet = int(
        summary.get("validated_hits_wet", summary.get("final_hits", summary.get("hits_total", 0)))
    )
    validated_hits_warc = summary.get("validated_hits_warc")
    validated_hits_warc_per_10k = summary.get("validated_hits_warc_per_10k")
    warc_validation_attempted = bool(summary.get("warc_validation_attempted", False))
    warc_validation_notes = str(summary.get("warc_validation_notes", "")).strip()
    docs_minlen = int(summary.get("docs_minlen", 0))
    hit_rate = _format_rate(validated_hits_wet, docs_minlen)
    validated_hits_warc_label = (
        f"{int(validated_hits_warc):,}" if validated_hits_warc is not None else "NA"
    )

    report_ns = reports_namespace(config)
    summary_table_path = (
        reports_tables_base(config) / report_ns / "TAB_stage1_pilot_summary.tex"
    )
    top_domains_table_path = (
        reports_tables_base(config) / report_ns / "TAB_stage1_top_domains.tex"
    )
    fig_path = reports_figures_base(config) / report_ns / "FIG_stage1_hits_by_term.pdf"

    summary_lines = [
        "\\begin{tabular}{lr}",
        "\\toprule",
        "Metric & Value \\\\",
        "\\midrule",
        f"Docs scanned & {_format_int(summary.get('docs_scanned', '0'))} \\\\",
        f"Docs >= min chars & {_format_int(summary.get('docs_minlen', '0'))} \\\\",
        f"Validated hits (WET) & {validated_hits_wet:,} \\\\",
        f"Validated hit rate (WET / docs >= min chars) & {hit_rate} \\\\",
        f"Validated hits (WARC) & {validated_hits_warc_label} \\\\",
        "WARC validation attempted & "
        + ("Yes" if warc_validation_attempted else "No")
        + " \\\\",
        "WARC validated hit rate (/10k scanned) & "
        + _format_optional_rate(validated_hits_warc_per_10k)
        + " \\\\",
        f"Removed by domain cap & {_format_int(summary.get('removed_domaincap', '0'))} \\\\",
        f"Removed total & {_format_int(summary.get('removed_total', '0'))} \\\\",
        "WARC validation notes & "
        + (warc_validation_notes if warc_validation_notes else "not run (placeholder)")
        + " \\\\",
        "\\bottomrule",
        "\\end{tabular}",
    ]
    _write_latex_table(summary_table_path, summary_lines)

    top_domains = pd.read_csv(top_domains_path)
    top_domains = top_domains.head(10).copy()
    if validated_hits_wet > 0:
        top_domains["share"] = (top_domains["hits"] / validated_hits_wet).round(4)
    else:
        top_domains["share"] = 0.0

    top_lines = [
        "\\begin{tabular}{lrr}",
        "\\toprule",
        "Domain & Hits & Share \\\\",
        "\\midrule",
    ]
    for _, row in top_domains.iterrows():
        top_lines.append(
            f"{row['registered_domain']} & {int(row['hits'])} & {row['share']:.4f} \\\\"  # noqa: E501
        )
    top_lines += ["\\bottomrule", "\\end{tabular}"]
    _write_latex_table(top_domains_table_path, top_lines)

    hits_by_term = {}
    if corpus_path.exists():
        corpus_df = pd.read_parquet(corpus_path, columns=["matched_term"])
        hits_by_term = corpus_df["matched_term"].value_counts().to_dict()
    elif "hits_by_term" in summary:
        try:
            hits_by_term = json.loads(summary.get("hits_by_term", "{}"))
        except json.JSONDecodeError:
            hits_by_term = {}

    labels = list(hits_by_term.keys())
    values = [hits_by_term[label] for label in labels]

    fig_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(6, 3))
    plt.bar(labels, values)
    plt.xticks(rotation=30, ha="right")
    plt.ylabel("Hits")
    plt.title("Hits by term")
    plt.tight_layout()
    plt.savefig(fig_path)
    plt.close()

    return summary_table_path, top_domains_table_path, fig_path
