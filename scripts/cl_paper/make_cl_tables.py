#!/usr/bin/env python3
"""Build the four in-text LaTeX tables for the Computational Linguistics short paper.

Every number is read from a tracked artefact under ``data/processed/lsc/`` or
``reports/tables/``; nothing is recomputed from the raw corpus and nothing is typed by
hand. Run from the repository root:

    python scripts/cl_paper/make_cl_tables.py

Outputs go to ``reports/tables/cl/``.
"""

from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "data/processed/lsc"
REPORTS = ROOT / "reports/tables/lsc"
OUT = ROOT / "reports/tables/cl"

FRAME_LABEL = {
    "substantive_core_overall": "Overall",
    "clinical_only": "Clinical",
    "lived_only": "Lived experience",
    "unframed_baseline": "Baseline",
}
# Strata reported in the paper, in reading order.
TARGET_STRATA = ["substantive_core_overall", "clinical_only", "lived_only"]


def read(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def marks(p: float, flagged: bool = False) -> str:
    """Combined significance and autocorrelation superscript, for use inside math mode.

    Emitted as a single ``^{...}`` group: TeX rejects two adjacent superscripts.
    """
    sig = "***" if p < 0.001 else "**" if p < 0.01 else "*" if p < 0.05 else ""
    body = sig + (r"\dagger" if flagged else "")
    return f"^{{{body}}}" if body else ""


def stars(p: float) -> str:
    return marks(p)


def num(x: float, nd: int, signed: bool = False) -> str:
    """APA-style number with the leading zero stripped, for use inside math mode."""
    s = f"{x:+.{nd}f}" if signed else f"{x:.{nd}f}"
    return s.replace("0.", ".", 1) if abs(x) < 1 else s


def maths(*parts: str) -> str:
    """Wrap a run of math fragments in a single math group."""
    return "$" + "".join(parts) + "$"


def fmt_beta(row: dict[str, str]) -> str:
    return maths(
        num(float(row["standardized_beta_year"]), 2, signed=True),
        stars(float(row["linear_p_value"])),
    )


def write(name: str, body: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_text(body)
    print(f"wrote {(OUT / name).relative_to(ROOT)}")


# --------------------------------------------------------------------------------------
# Table 1: annotation reliability and held-out classifier validation
# --------------------------------------------------------------------------------------

AXES = [
    ("substantive_target_discourse", "Substantive target discourse"),
    ("clinical_frame_present", "Clinical framing"),
    ("lived_experience_frame_present", "Lived-experience framing"),
]


def cohens_kappa(pairs: list[tuple[str, str]]) -> tuple[float, float, int]:
    """Return (kappa, observed agreement, n) for a list of (rater_a, rater_b) labels."""
    n = len(pairs)
    p_o = sum(a == b for a, b in pairs) / n
    ca, cb = Counter(a for a, _ in pairs), Counter(b for _, b in pairs)
    p_e = sum((ca[k] / n) * (cb.get(k, 0) / n) for k in ca)
    return (p_o - p_e) / (1 - p_e), p_o, n


def table1() -> None:
    comp = read(
        ROOT
        / "data/interim/lsc/classification/llm_annotation/codex/pilot/evaluation"
        / "pilot_comparison_high.csv"
    )
    val = {
        r["head"]: r for r in read(PROC / "classification/frame_classifier_validation_metrics.csv")
    }

    rows = []
    for axis, label in AXES:
        pairs = [
            (r[f"{axis}_human"], r[f"{axis}_codex"])
            for r in comp
            if r[f"{axis}_human"] not in ("", "None") and r[f"{axis}_codex"] not in ("", "None")
        ]
        kappa, agree, n = cohens_kappa(pairs)
        v = val[axis]
        rows.append(
            f"{label} & {n} & .{round(agree * 1000):03d} & .{round(kappa * 1000):03d} & "
            f"{int(v['support_positive'])}/{int(v['support_total'])} & "
            f"{float(v['precision']):.3f}".replace("0.", ".")
            + f" & {float(v['recall']):.3f}".replace("0.", ".")
            + f" & {float(v['f1']):.3f}".replace("0.", ".")
            + r" \\"
        )

    body = (
        r"""\begin{table}[t!]
\caption{Annotation reliability and held-out classifier validation for the three
hierarchical frame decisions. LLM--human agreement compares the locked Codex annotator
against the human coder on the 200-passage pilot; classifier performance is measured on
the disjoint 200-passage validation set, which was withheld from codebook development,
LLM annotation, criticism, correction, and training.}
\label{tab:annotation}
\small
\begin{tabular}{@{}lccccccc@{}}
\toprule
& \multicolumn{3}{c}{LLM--human agreement} & \multicolumn{4}{c}{Held-out classifier} \\
\cmidrule(r){2-4}\cmidrule(l){5-8}
Decision & $n$ & Agr. & $\kappa$ & Support & P & R & F1 \\
\midrule
"""
        + "\n".join(rows)
        + r"""
\bottomrule
\end{tabular}
\par\vspace{3pt}
\parbox{\linewidth}{\footnotesize Note. The clinical and lived-experience rows are
evaluated among passages the human coder judged substantive. All human labels come from a
single coder, so no inter-annotator agreement is reportable.}
\end{table}
"""
    )
    write("table1_annotation.tex", body)


# --------------------------------------------------------------------------------------
# Table 2: frame-composition trend models
# --------------------------------------------------------------------------------------


def table2() -> None:
    rows = []
    for r in read(REPORTS / "classification/lsc_classification_frame_time_trends.csv"):
        p = float(r["linear_p_value"])
        flagged = r["autocorrelation_flag"] == "True"
        ar1 = (
            maths(num(float(r["ar1_sensitivity_slope_pp_per_year"]), 2))
            + f" ($p = {num(float(r['ar1_sensitivity_p_value']), 3)}$)"
            if flagged
            else "---"
        )
        rows.append(
            f"{r['analysis_unit']} & "
            + maths(f"{float(r['linear_slope_pp_per_year']):.2f}", marks(p, flagged))
            + f" ({float(r['linear_slope_se_pp']):.2f}) & "
            + maths(num(float(r["standardized_beta_year"]), 2, signed=True))
            + " & "
            + maths(num(float(r["linear_adj_r_squared"]), 2))
            + f" & {float(r['fitted_lived_share_2014']) * 100:.1f}\\%"
            + f" & {float(r['fitted_lived_share_2026']) * 100:.1f}\\% & {ar1}"
            + r" \\"
        )

    body = (
        r"""\begin{table}[t!]
\caption{Annual trend models for the lived-experience share of clearly framed target
contexts, defined as lived-only divided by lived-only plus clinical-only.}
\label{tab:composition}
\small
\begin{tabular}{@{}lcccccc@{}}
\toprule
Target & $B$ (SE), pp/yr & $\beta$ & Adj. $R^2$ & Fitted 2014 & Fitted 2026 & AR(1) $B$ ($p$) \\
\midrule
"""
        + "\n".join(rows)
        + r"""
\bottomrule
\end{tabular}
\par\vspace{3pt}
\parbox{\linewidth}{\footnotesize Note. $B$ is the annual OLS slope in percentage points;
$\beta$ is the standardised year coefficient. $^{\dagger}$ marks a residual-autocorrelation
flag, for which the AR(1) sensitivity slope is reported in the final column.
$^{*}p<.05$, $^{**}p<.01$, $^{***}p<.001$; $p$-values are descriptive and uncorrected.}
\end{table}
"""
    )
    write("table2_composition.tex", body)


# --------------------------------------------------------------------------------------
# Table 3: target x frame trend models for the three semantic measures
# --------------------------------------------------------------------------------------


def table3() -> None:
    rows, prev = [], None
    for r in read(REPORTS / "regression/lsc_regression_target_frames.csv"):
        if r["measure"] == "Salience":  # deferred to the appendix
            continue
        p = float(r["linear_p_value"])
        flagged = r["autocorrelation_flag"] == "True"
        measure = r["measure"] if r["measure"] != prev else ""
        prev = r["measure"]
        rows.append(
            f"{measure} & {r['target_display']} & {r['frame_display']} & "
            + maths(num(float(r["linear_slope_per_year"]), 4), marks(p, flagged))
            + f" ({num(float(r['linear_slope_se']), 4)}) & "
            + maths(num(float(r["standardized_beta_year"]), 2, signed=True))
            + " & "
            + maths(num(float(r["linear_adj_r_squared"]), 2))
            + r" \\"
        )
    body = (
        r"""\begin{table}[t!]
\caption{Annual trend models for the three semantic measures, by target and frame.
Sentiment and intensity are NRC--VAD valence and arousal over local collocates; breadth is
the mean pairwise cosine distance among XL-LEXEME target-use embeddings.}
\label{tab:trends}
\small
\begin{tabular}{@{}lllccc@{}}
\toprule
Measure & Target & Frame & $B$ (SE) & $\beta$ & Adj. $R^2$ \\
\midrule
"""
        + "\n".join(rows)
        + r"""
\bottomrule
\end{tabular}
\par\vspace{3pt}
\parbox{\linewidth}{\footnotesize Note. $B$ is the unstandardised annual OLS slope over 13
publication years; $\beta$ is the standardised year coefficient. $^{\dagger}$ marks a
residual-autocorrelation flag; AR(1) sensitivity estimates for flagged series are reported
in Appendix~\ref{app:ar1}. $^{*}p<.05$, $^{**}p<.01$, $^{***}p<.001$; $p$-values are
descriptive and uncorrected across 34 trend tests.}
\end{table}
"""
    )
    write("table3_trends.tex", body)


# --------------------------------------------------------------------------------------
# Table 4: conclusion concordance across operationalisations
# --------------------------------------------------------------------------------------

SPECS = [
    (
        "Sentiment",
        "NRC--VAD",
        "Warriner",
        [PROC / "sentiment/lsc_sentiment_trend_models.csv"],
        [PROC / "sentiment/robustness_warriner/lsc_sentiment_warriner_trend_models.csv"],
        lambda r: r["index_name"] == "valence_mean",
        lambda r: "scaled" in r["index_name"],
    ),
    (
        "Intensity",
        "NRC--VAD",
        "Warriner",
        [PROC / "intensity/lsc_intensity_trend_models.csv"],
        [PROC / "intensity/robustness_warriner/lsc_intensity_warriner_trend_models.csv"],
        lambda r: r["index_name"] == "arousal_mean",
        lambda r: "scaled" in r["index_name"],
    ),
    (
        "Breadth",
        "XL-LEXEME",
        "MPNet",
        [PROC / "breadth/lsc_breadth_trend_models.csv"],
        # The comparator rows come from the extension run in
        # notebooks/08_measurement_invariance/run_mpnet_comparator_breadth.py, which the
        # dissertation's targets-only MPNet check did not cover.
        [
            PROC / "breadth/robustness_baes_mpnet/lsc_baes_mpnet_breadth_trend_models.csv",
            PROC
            / "breadth/robustness_baes_mpnet/lsc_baes_mpnet_breadth_trend_models_comparators.csv",
        ],
        lambda r: r["index_name"] == "breadth_mean_pairwise_cosine_distance",
        lambda r: True,
    ),
]


def verdict(a: dict[str, str], b: dict[str, str]) -> str:
    """Agree iff the two operationalisations license the same descriptive conclusion."""
    sig_a = float(a["linear_p_value"]) < 0.05
    sig_b = float(b["linear_p_value"]) < 0.05
    if sig_a != sig_b:
        return r"\textbf{differ}"
    if not sig_a:  # both null
        return "agree"
    same_sign = (float(a["linear_slope_per_year"]) > 0) == (float(b["linear_slope_per_year"]) > 0)
    return "agree" if same_sign else r"\textbf{differ}"


def table4() -> None:
    rows, n_differ, n_total = [], 0, 0
    per_measure: dict[str, list[int]] = {}
    for measure, prim_name, alt_name, prim_paths, alt_paths, prim_f, alt_f in SPECS:
        prim = [r for p in prim_paths for r in read(p) if prim_f(r)]
        alt = [r for p in alt_paths for r in read(p) if alt_f(r)]
        alt_idx = {(r["analysis_unit"], r["frame_stratum"]): r for r in alt}
        order = {s: i for i, s in enumerate(TARGET_STRATA + ["unframed_baseline"])}
        prim = sorted(
            (r for r in prim if r["frame_stratum"] in FRAME_LABEL),
            key=lambda r: (
                r["term_role"] != "target",
                r["analysis_unit"],
                order[r["frame_stratum"]],
            ),
        )
        first = True
        for p in prim:
            stratum = p["frame_stratum"]
            a = alt_idx.get((p["analysis_unit"], stratum))
            if a is None:
                continue  # no comparator counterpart under this operationalisation
            v = verdict(p, a)
            n_total += 1
            n_differ += v != "agree"
            bucket = per_measure.setdefault(measure, [0, 0])
            bucket[0] += v != "agree"
            bucket[1] += 1
            unit = p["analysis_unit"]
            label = FRAME_LABEL[stratum]
            if first:
                rows.append(
                    r"\multicolumn{5}{@{}l}{\itshape "
                    + f"{measure}: {prim_name} versus {alt_name}"
                    + r"} \\"
                )
                first = False
            rows.append(f"{unit} & {label} & {fmt_beta(p)} & {fmt_beta(a)} & {v}" + r" \\")
        rows.append(r"\addlinespace")
    rows = rows[:-1]

    body = (
        r"""\begin{table}[t!]
\caption{Conclusion concordance across operationalisations. Each row asks whether the two
resources license the same descriptive conclusion for the same series: agreement requires
the same significance verdict at $p<.05$ and, where significant, the same sign.}
\label{tab:concordance}
\small
\begin{tabular}{@{}llccl@{}}
\toprule
Series & Frame & Primary $\beta$ & Alternative $\beta$ & Conclusion \\
\midrule
"""
        + "\n".join(rows)
        + r"""
\bottomrule
\end{tabular}
\par\vspace{3pt}
\parbox{\linewidth}{\footnotesize Note. Cells report the standardised year coefficient,
which is comparable across resources on different native scales; raw slopes are not.
$^{*}p<.05$, $^{**}p<.01$, $^{***}p<.001$; $p$-values are descriptive and uncorrected. The
valence and arousal are scored on the same matched collocates within a lexicon, so the
sentiment and intensity blocks differ only in which rating is applied.}
\end{table}
"""
    )
    write("table4_concordance.tex", body)
    print(f"  concordance: {n_differ} of {n_total} series differ")
    for measure, counts in per_measure.items():
        print(f"    {measure}: {counts[0]} of {counts[1]} differ")


# --------------------------------------------------------------------------------------
# Appendix tables
# --------------------------------------------------------------------------------------

# The dissertation's table files carry their own labels and cross-references. Rewriting
# them to this paper's label scheme keeps every \ref resolvable, so the build stays free
# of undefined-reference warnings.
LABEL_MAP = {
    "tab:lsc-regression-target-frames": "tab:trends",
    "tab:lsc-classification-frame-time-trends": "tab:composition",
    "tab:lsc-regression-baseline-comparators": "tab:comparators",
    "tab:lsc-ar1-sensitivity-flagged": "tab:ar1",
    "tab:lsc-quadratic-diagnostics": "tab:quadratic",
    "tab:lsc-classification-temporal-stability": "tab:stability",
    "tab:lsc-posthoc-sentiment-contributors": "tab:posthoc-sentiment",
    "tab:lsc-posthoc-arousal-contributors": "tab:posthoc-arousal",
    "tab:lsc-posthoc-breadth-contributors": "tab:posthoc-breadth",
}

REUSED = [
    (
        "classification/temporal_stability/lsc_classification_temporal_stability.tex",
        "appx_stability.tex",
    ),
    ("regression/lsc_regression_baseline_comparators.tex", "appx_comparators.tex"),
    ("diagnostics/lsc_ar1_sensitivity_flagged.tex", "appx_ar1.tex"),
    ("diagnostics/lsc_quadratic_trend_diagnostics_flagged.tex", "appx_quadratic.tex"),
    ("posthoc/lsc_posthoc_sentiment_collocates.tex", "appx_posthoc_sentiment.tex"),
    ("posthoc/lsc_posthoc_arousal_collocates.tex", "appx_posthoc_arousal.tex"),
    ("posthoc/lsc_posthoc_breadth_content_words.tex", "appx_posthoc_breadth.tex"),
]


def appendix_reused() -> None:
    """Copy the analysis pipeline's table files across, rewriting labels and refs."""
    for src, dest in REUSED:
        text = (REPORTS / src).read_text()
        for old, new in LABEL_MAP.items():
            text = text.replace("{" + old + "}", "{" + new + "}")
        text = text.replace("[htbp]", "[t!]")
        write(dest, text)


# --------------------------------------------------------------------------------------
# Table 5: lexicon decomposition -- coverage versus ratings
# --------------------------------------------------------------------------------------

MEAS = ROOT / "data/processed/lsc/measurement"
DIM_LABEL = {"valence": "Valence (sentiment)", "arousal": "Arousal (intensity)"}


def table5() -> None:
    dec = read(MEAS / "lexicon_decomposition.csv")
    agree = {r["dimension"]: r for r in read(MEAS / "lexicon_rating_agreement.csv")}
    order = ["ADHD", "Autism", "frustration", "loneliness", "sadness"]

    rows = []
    for dim in ["valence", "arousal"]:
        block = sorted(
            (r for r in dec if r["dimension"] == dim), key=lambda r: order.index(r["analysis_unit"])
        )
        r_tok = float(agree[dim]["token_weighted_r"])
        rows.append(
            r"\multicolumn{6}{@{}l}{\itshape "
            + DIM_LABEL[dim]
            + ": NRC--VAD and Warriner ratings correlate"
            + f" $r = {num(r_tok, 2)}$ on shared vocabulary"
            + r"} \\"
        )
        for r in block:
            rows.append(
                f"{r['analysis_unit']} & "
                + maths(num(float(r["A_nrc_set_nrc_ratings_beta"]), 2, signed=True))
                + " & "
                + maths(num(float(r["B_warriner_set_warriner_ratings_beta"]), 2, signed=True))
                + " & "
                + maths(num(float(r["total_A_to_B"]), 2, signed=True))
                + " & "
                + maths(
                    num(
                        float(r["coverage_effect_A_to_C"]) + float(r["residual_coverage_D_to_B"]),
                        2,
                        signed=True,
                    )
                )
                + " & "
                + maths(num(float(r["rating_effect_C_to_D"]), 2, signed=True))
                + r" \\"
            )
        rows.append(r"\addlinespace")
    rows = rows[:-1]

    body = (
        r"""\begin{table}[t!]
\caption{Why the affective lexicons disagree. The shift from the primary to the alternative
estimate is split into the part attributable to scoring a different set of tokens and the
part attributable to scoring shared tokens differently.}
\label{tab:decomposition}
\small
\begin{tabular}{@{}lccccc@{}}
\toprule
& \multicolumn{2}{c}{Standardised $\beta$} & \multicolumn{3}{c}{Decomposition of the shift} \\
\cmidrule(r){2-3}\cmidrule(l){4-6}
Series & NRC--VAD & Warriner & Total & Coverage & Ratings \\
\midrule
"""
        + "\n".join(rows)
        + r"""
\bottomrule
\end{tabular}
\par\vspace{3pt}
\parbox{\linewidth}{\footnotesize Note. Both lexicons were applied to the same collocate
windows, so their matches can be crossed: the coverage column holds ratings fixed and varies
the token set, the ratings column holds the shared token set fixed and varies the rating.
Columns sum to the total up to a negligible interaction. Targets are the substantive
aggregate; comparators are unframed.}
\end{table}
"""
    )
    write("table5_decomposition.tex", body)


# --------------------------------------------------------------------------------------
# Table 6: what the encoders are sensitive to
# --------------------------------------------------------------------------------------


def table6() -> None:
    probe = read(MEAS / "encoder_target_swap_probe.csv")
    conc = read(MEAS / "encoder_annual_concordance.csv")
    mean_r = sum(float(r["pearson_r"]) for r in conc) / len(conc)

    rows = []
    for r in probe:
        rows.append(
            f"{r['encoder']} & "
            + maths(num(float(r["target_swap_distance_mean"]), 4))
            + " & "
            + maths(num(float(r["control_swap_distance_mean"]), 4))
            + " & "
            + maths(f"{float(r['target_over_control_ratio']):.0f}")
            + r"$\times$"
            + r" \\"
        )

    body = (
        r"""\begin{table}[t!]
\caption{What each breadth encoder responds to. Mean cosine distance between a context and
a minimally edited variant of it, over """
        + f"{int(probe[0]['n']):,}"
        + r""" target contexts.}
\label{tab:probe}
\small
\begin{tabular}{@{}lccc@{}}
\toprule
Encoder & Target swapped & Control word swapped & Ratio \\
\midrule
"""
        + "\n".join(rows)
        + r"""
\bottomrule
\end{tabular}
\par\vspace{3pt}
\parbox{\linewidth}{\footnotesize Note. The target swap replaces the target term with the
other concept's term; the control swap replaces an unrelated content word in the same
context. Both conditions use the same contexts. A higher ratio means the representation is
more specific to the target and less contaminated by the rest of the sentence. Across the
"""
        + f"{len(conc)}"
        + r""" annual series the two encoders' breadth estimates correlate at
mean $r = """
        + num(mean_r, 2)
        + r"""$.}
\end{table}
"""
    )
    write("table6_probe.tex", body)


if __name__ == "__main__":
    table1()
    table2()
    table3()
    table4()
    table5()
    table6()
    appendix_reused()
