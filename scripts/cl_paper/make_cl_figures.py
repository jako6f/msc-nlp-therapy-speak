#!/usr/bin/env python3
"""Recompose the in-text figures for the Computational Linguistics short paper.

The dissertation's figures were drawn for an A4 12pt page and are too wide for CL's 32pc
(~5.3 in) measure. This script rebuilds them from the same tracked annual-score CSVs under
``data/processed/lsc/`` and ``reports/tables/lsc/`` at CL proportions, with a serif face
and type sizes that stay legible at the journal's column width. No model is re-run and no
statistic is recomputed: every value plotted is read from a tracked artefact.

Run from the repository root with the project environment:

    python scripts/cl_paper/make_cl_figures.py

Outputs go to ``reports/figures/cl/``.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "data/processed/lsc"
REPORTS = ROOT / "reports/tables/lsc"
OUT = ROOT / "reports/figures/cl"

TEXTWIDTH_IN = 5.32  # clv2025 sets \textwidth to 32pc

# Colourblind-safe; keeps the dissertation's target/comparator associations.
ADHD = "#2b6ca3"
AUTISM = "#c1622f"
COMPARATOR = {"frustration": "#3f7d5a", "loneliness": "#7fa88c", "sadness": "#7a7a7a"}
FRAME_STYLE = {
    "substantive_core_overall": dict(lw=1.6, ls="-", marker="o", ms=2.8, alpha=1.0),
    "clinical_only": dict(lw=1.0, ls="--", marker="s", ms=2.4, alpha=0.85),
    "lived_only": dict(lw=1.0, ls=":", marker="^", ms=2.4, alpha=0.85),
}
FRAME_LABEL = {
    "substantive_core_overall": "Overall",
    "clinical_only": "Clinical/disorder",
    "lived_only": "Lived experience",
}


def style() -> None:
    mpl.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Palatino", "Palatino Linotype", "URW Palladio L", "DejaVu Serif"],
            "font.size": 7.5,
            "axes.labelsize": 7.5,
            "axes.titlesize": 8,
            "xtick.labelsize": 6.8,
            "ytick.labelsize": 6.8,
            "legend.fontsize": 6.6,
            "axes.linewidth": 0.6,
            "grid.linewidth": 0.4,
            "lines.linewidth": 1.2,
            "xtick.major.width": 0.6,
            "ytick.major.width": 0.6,
            "xtick.major.size": 2.5,
            "ytick.major.size": 2.5,
            "axes.grid": True,
            "grid.color": "#dddddd",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "figure.dpi": 150,
            "savefig.bbox": "tight",
            "savefig.pad_inches": 0.02,
        }
    )


def save(fig: plt.Figure, name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT / f"{name}.pdf")
    plt.close(fig)
    print(f"wrote {(OUT / f'{name}.pdf').relative_to(ROOT)}")


def trendline(ax, years, values, colour, **kw) -> None:
    """Overlay the OLS fit that the trend tables summarise."""
    b, a = np.polyfit(years, values, 1)
    ax.plot(
        years, a + b * np.asarray(years), color=colour, ls=(0, (4, 2)), lw=0.8, alpha=0.75, **kw
    )


# --------------------------------------------------------------------------------------
# Figure 2: discourse composition
# --------------------------------------------------------------------------------------


def figure_composition() -> None:
    periods = pd.read_csv(
        REPORTS / "classification/lsc_classification_full_composition_periods.csv"
    )
    balance = pd.read_csv(
        REPORTS / "classification/lsc_classification_frame_balance_figure_data.csv"
    )

    fig, (ax_a, ax_b) = plt.subplots(
        1, 2, figsize=(TEXTWIDTH_IN, 2.45), gridspec_kw={"width_ratios": [1.0, 1.25]}
    )

    # Panel A: full predicted composition, early vs late.
    order = periods.sort_values("frame_order")["frame_label"].drop_duplicates().tolist()
    palette = ["#2b6ca3", "#c1622f", "#3f7d5a", "#bcbcbc", "#8d7ab5"]
    cols = [(u, p) for u in ["ADHD", "Autism"] for p in ["2014-2017", "2022-2026"]]
    x = np.arange(len(cols))
    bottom = np.zeros(len(cols))
    for label, colour in zip(order, palette):
        vals = [
            periods.query("analysis_unit == @u and period == @p and frame_label == @label")[
                "pct_within_target_period"
            ].sum()
            * 100
            for u, p in cols
        ]
        ax_a.bar(x, vals, 0.66, bottom=bottom, color=colour, label=label, linewidth=0)
        for xi, (v, b0) in enumerate(zip(vals, bottom)):
            if label in ("Clinical/disorder", "Lived experience") and v > 4:
                ax_a.text(
                    xi,
                    b0 + v / 2,
                    f"{v:.0f}%",
                    ha="center",
                    va="center",
                    color="white",
                    fontsize=6.2,
                    fontweight="bold",
                )
        bottom += np.array(vals)

    ax_a.set_xticks(x)
    ax_a.set_xticklabels(["2014–17", "2022–26", "2014–17", "2022–26"], fontsize=6.2)
    ax_a.set_ylim(0, 100)
    ax_a.set_ylabel("Share of target contexts")
    ax_a.yaxis.set_major_formatter(mpl.ticker.PercentFormatter(decimals=0))
    ax_a.set_title("A. Predicted composition", fontsize=7.6, loc="left")
    ax_a.grid(axis="x", visible=False)
    for xi, unit in [(0.5, "ADHD"), (2.5, "Autism")]:
        ax_a.text(xi, -14, unit, ha="center", va="top", fontsize=7.2, transform=ax_a.transData)
    ax_a.legend(
        loc="upper center",
        bbox_to_anchor=(1.12, -0.20),
        ncol=3,
        frameon=False,
        handlelength=1.1,
        columnspacing=1.0,
        handletextpad=0.4,
    )

    # Panel B: lived-experience share among clearly framed contexts.
    lived = balance.query("predicted_derived_frame == 'lived_only'")
    for unit, colour in [("ADHD", ADHD), ("Autism", AUTISM)]:
        d = lived.query("analysis_unit == @unit").sort_values("lsc_year")
        yrs, share = d["lsc_year"].to_numpy(), d["pct_within_clear_frames"].to_numpy() * 100
        ax_b.fill_between(
            yrs, d["share_ci_low"] * 100, d["share_ci_high"] * 100, color=colour, alpha=0.16, lw=0
        )
        ax_b.plot(yrs, share, color=colour, marker="o", ms=2.6, lw=1.4, label=unit)
        trendline(ax_b, yrs, share, colour)
    ax_b.set_ylabel("Lived-experience share")
    ax_b.set_xlabel("Publication year")
    ax_b.set_title("B. Share among clearly framed contexts", fontsize=7.6, loc="left")
    ax_b.yaxis.set_major_formatter(mpl.ticker.PercentFormatter(decimals=0))
    ax_b.set_xticks([2014, 2017, 2020, 2023, 2026])
    ax_b.legend(loc="lower right", frameon=False, handlelength=1.4)

    fig.subplots_adjust(wspace=0.42)
    save(fig, "fig2_composition")


# --------------------------------------------------------------------------------------
# Figure 3: the two concept-creep tests
# --------------------------------------------------------------------------------------

PANELS = [
    (
        "Intensity (NRC–VAD arousal)",
        PROC / "intensity/lsc_intensity_annual_arousal.csv",
        "arousal_mean",
        "arousal_ci_low",
        "arousal_ci_high",
    ),
    (
        "Breadth (XL-LEXEME distance)",
        PROC / "breadth/lsc_breadth_annual_scores.csv",
        "breadth_mean_pairwise_cosine_distance",
        "breadth_ci_low",
        "breadth_ci_high",
    ),
]


def figure_no_creep() -> None:
    fig, axes = plt.subplots(2, 3, figsize=(TEXTWIDTH_IN, 3.9), sharex=True)

    for row, (row_label, path, col, lo, hi) in enumerate(PANELS):
        df = pd.read_csv(path)
        for j, (unit, colour) in enumerate([("ADHD", ADHD), ("Autism", AUTISM)]):
            ax = axes[row, j]
            d_unit = df.query("analysis_unit == @unit")
            for stratum, kw in FRAME_STYLE.items():
                d = d_unit.query("frame_stratum == @stratum").sort_values("lsc_year")
                if d.empty:
                    continue
                yrs = d["lsc_year"].to_numpy()
                ax.fill_between(yrs, d[lo], d[hi], color=colour, alpha=0.10, lw=0)
                ax.plot(
                    yrs,
                    d[col],
                    color=colour,
                    label=FRAME_LABEL[stratum],
                    **{k: v for k, v in kw.items()},
                )
            if row == 0:
                ax.set_title(unit, fontsize=7.8)

        ax = axes[row, 2]
        base = df.query("frame_stratum == 'unframed_baseline'")
        for term, colour in COMPARATOR.items():
            d = base.query("analysis_unit == @term").sort_values("lsc_year")
            ax.fill_between(d["lsc_year"], d[lo], d[hi], color=colour, alpha=0.10, lw=0)
            ax.plot(d["lsc_year"], d[col], color=colour, lw=1.1, marker="o", ms=2.2, label=term)
        if row == 0:
            ax.set_title("Comparator terms", fontsize=7.8)

        axes[row, 0].set_ylabel(row_label, fontsize=7.0)

    # Shared frame legend (left/middle panels) and comparator legend (right panels).
    handles = [
        plt.Line2D([], [], color="#555555", **{k: v for k, v in kw.items() if k != "alpha"})
        for kw in FRAME_STYLE.values()
    ]
    fig.legend(
        handles,
        list(FRAME_LABEL.values()),
        loc="lower center",
        bbox_to_anchor=(0.37, -0.055),
        ncol=3,
        frameon=False,
        handlelength=1.8,
        columnspacing=1.2,
        handletextpad=0.4,
    )
    axes[0, 2].legend(
        loc="upper left",
        frameon=False,
        handlelength=1.2,
        handletextpad=0.3,
        fontsize=6.3,
        borderpad=0.1,
        labelspacing=0.25,
    )

    for ax in axes[1, :]:
        ax.set_xlabel("Publication year")
        ax.set_xticks([2014, 2020, 2026])
    fig.subplots_adjust(hspace=0.28, wspace=0.38)
    save(fig, "fig3_no_creep")


# --------------------------------------------------------------------------------------
# Figure 4: operationalisation sensitivity
# --------------------------------------------------------------------------------------


def figure_sensitivity() -> None:
    """Primary against alternative operationalisation, standardised within series.

    The two affective lexicons live on different native scales (NRC--VAD spans -1 to 1,
    Warriner is a 1--9 scale rescaled to 0--1), so raw index changes are not comparable
    across methods. Each series is therefore centred and scaled by its own across-year
    standard deviation. Standardisation is a linear transform, so it leaves the sign,
    significance and standardised slope of every trend exactly as the trend tables report
    them; it only puts the panels on a common footing.
    """
    df = pd.read_csv(REPORTS / "robustness/lsc_method_robustness_figure_data.csv")
    fig, axes = plt.subplots(2, 2, figsize=(TEXTWIDTH_IN, 3.6), sharex=True)

    for row, measure in enumerate(["Intensity", "Breadth"]):
        sub = df.query("measure == @measure")
        labels = sorted(sub["method_label"].unique(), key=lambda s: not s.startswith("Present"))
        for col, unit in enumerate(["ADHD", "Autism"]):
            ax = axes[row, col]
            for label, colour, ls in zip(labels, ["#1f4e79", "#b5651d"], ["-", "--"]):
                d = sub.query("analysis_unit == @unit and method_label == @label").sort_values(
                    "lsc_year"
                )
                yrs = d["lsc_year"].to_numpy()
                mu, sd = d["score"].mean(), d["score"].std(ddof=0)
                z = (d["score"] - mu) / sd
                ax.fill_between(
                    yrs,
                    (d["ci_low_delta"] + d["score_2014"] - mu) / sd,
                    (d["ci_high_delta"] + d["score_2014"] - mu) / sd,
                    color=colour,
                    alpha=0.13,
                    lw=0,
                )
                ax.plot(
                    yrs,
                    z,
                    color=colour,
                    ls=ls,
                    lw=1.3,
                    marker="o",
                    ms=2.4,
                    label=label.replace("Original SIBling: ", "").replace("Present: ", ""),
                )
                trendline(ax, yrs, z.to_numpy(), colour)
            ax.axhline(0, color="#999999", lw=0.6, ls=(0, (1, 2)))
            if row == 0:
                ax.set_title(unit, fontsize=7.8)
            ax.legend(loc="best", frameon=False, handlelength=1.6, fontsize=6.3)
        axes[row, 0].set_ylabel(f"{measure}\n(SD within series)", fontsize=7.0)

    for ax in axes[1, :]:
        ax.set_xlabel("Publication year")
        ax.set_xticks([2014, 2020, 2026])
    fig.subplots_adjust(hspace=0.26, wspace=0.30)
    save(fig, "fig4_sensitivity")


# --------------------------------------------------------------------------------------
# Appendix F: neighbour similarity, as a check on the frame distinction
# --------------------------------------------------------------------------------------

NEIGHBOUR_FRAMES = [("clinical_only", "Clinical/disorder"), ("lived_only", "Lived experience")]


def figure_neighbours() -> None:
    """One compact 2x2 panel: targets by frame.

    The overall stratum is omitted deliberately. This figure exists to show that the two
    frames occupy different distributional neighbourhoods, and pooling them answers a
    different question.
    """
    df = pd.read_csv(
        ROOT
        / "data/processed/lsc/thematic_evolution"
        / "lsc_thematic_neighbour_similarity_trajectories.csv"
    )
    fig, axes = plt.subplots(2, 2, figsize=(TEXTWIDTH_IN, 4.3), sharex=True, sharey=True)

    for i, (unit, base) in enumerate([("ADHD", ADHD), ("Autism", AUTISM)]):
        for j, (stratum, frame_label) in enumerate(NEIGHBOUR_FRAMES):
            ax = axes[i, j]
            d = df[(df.analysis_unit == unit) & (df.frame_stratum == stratum)]
            # Rank by mean similarity so the legend reads top-to-bottom like the lines.
            order = d.groupby("neighbour").cosine_similarity.mean().sort_values(ascending=False)
            shades = _shades(base, len(order))
            for colour, neighbour in zip(shades, order.index):
                g = d[d.neighbour == neighbour].sort_values("lsc_year")
                ax.plot(g.lsc_year, g.cosine_similarity, color=colour, lw=1.3,
                        marker="o", ms=2.4, label=neighbour)
            ax.set_title(f"{unit} \u2014 {frame_label}", fontsize=7.6)
            # Panels are dense; let the legend find the free corner and sit on a light
            # backing so it never obscures a trajectory.
            ax.legend(loc="best", fontsize=6.2, handlelength=1.1, labelspacing=0.22,
                      handletextpad=0.4, borderpad=0.3, frameon=True, framealpha=0.88,
                      facecolor="white", edgecolor="none")
            ax.set_xticks([2014, 2018, 2022, 2026])
            if j == 0:
                ax.set_ylabel("Cosine similarity to target")
            if i == 1:
                ax.set_xlabel("Publication year")

    fig.subplots_adjust(hspace=0.30, wspace=0.14)
    save(fig, "figF1_neighbours")


def _shades(base: str, n: int) -> list[str]:
    """Evenly spaced tints of one hue, so each panel stays visually a single family."""
    rgb = np.array(mpl.colors.to_rgb(base))
    return [mpl.colors.to_hex(rgb + (1 - rgb) * t) for t in np.linspace(0.0, 0.62, max(n, 1))]


if __name__ == "__main__":
    style()
    figure_composition()
    figure_no_creep()
    figure_sensitivity()
    figure_neighbours()
