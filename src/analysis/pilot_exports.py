import json
import re
from pathlib import Path
from typing import Dict, Tuple

import matplotlib.pyplot as plt
import pandas as pd

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


def _load_summary(summary_path: Path) -> Dict[str, str]:
    df = pd.read_csv(summary_path)
    return dict(zip(df["metric"], df["value"]))


def _write_latex_table(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def _format_int(value: str) -> str:
    return f"{int(value):,}"


def _format_rate(numer: int, denom: int) -> str:
    if denom == 0:
        return "0.000"
    return f"{numer / denom:.3f}"


def export_tables_and_figures(interim_dir: Path, reports_dir: Path) -> Tuple[Path, Path, Path]:
    runid = _latest_runid(interim_dir)
    summary_path = interim_dir / f"cc_scan_summary_{runid}.csv"
    top_domains_path = interim_dir / f"cc_scan_top_domains_{runid}.csv"

    summary = _load_summary(summary_path)
    hits_total = int(summary.get("hits_total", 0))
    docs_minlen = int(summary.get("docs_minlen", 0))
    hit_rate = _format_rate(hits_total, docs_minlen)

    summary_table_path = reports_dir / "tables" / "TAB_stage1_pilot_summary.tex"
    top_domains_table_path = reports_dir / "tables" / "TAB_stage1_top_domains.tex"
    fig_path = reports_dir / "figures" / "FIG_stage1_hits_by_term.pdf"

    summary_lines = [
        "\\begin{tabular}{lr}",
        "\\toprule",
        "Metric & Value \\\\",
        "\\midrule",
        f"Docs scanned & {_format_int(summary.get('docs_scanned', '0'))} \\\\",
        f"Docs >= min chars & {_format_int(summary.get('docs_minlen', '0'))} \\\\",
        f"Hits total & {_format_int(summary.get('hits_total', '0'))} \\\\",
        f"Hit rate (hits / docs >= min chars) & {hit_rate} \\\\",
        f"Unique domains (all) & {_format_int(summary.get('unique_domains_total', '0'))} \\\\",
        f"Unique domains (hits) & {_format_int(summary.get('unique_domains_hits', '0'))} \\\\",
        f"Capped removed & {_format_int(summary.get('capped_removed', '0'))} \\\\",
        "\\bottomrule",
        "\\end{tabular}",
    ]
    _write_latex_table(summary_table_path, summary_lines)

    top_domains = pd.read_csv(top_domains_path)
    top_domains = top_domains.head(10).copy()
    if hits_total > 0:
        top_domains["share"] = (top_domains["hits"] / hits_total).round(4)
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

    hits_by_term = summary.get("hits_by_term", "{}")
    try:
        hits_by_term = json.loads(hits_by_term)
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
