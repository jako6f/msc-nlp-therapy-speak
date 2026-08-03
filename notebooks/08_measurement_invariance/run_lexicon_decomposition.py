#!/usr/bin/env python3
"""Separate *which tokens are scored* from *how they are scored*.

Substituting NRC--VAD for the Warriner norms changes several trend conclusions, but the two
resources differ in two ways at once: they match different subsets of the same text, and
they assign different ratings to the words they share. This script crosses those two
factors so the divergence can be attributed.

Both lexicons were applied to the same preprocessed collocate windows, so their match
tables share a ``context_row_id`` space and can be joined per matched occurrence. That
allows four cells:

    A  NRC set      x  NRC ratings        (the paper's primary estimate)
    B  Warriner set x  Warriner ratings   (the alternative estimate)
    C  shared set   x  NRC ratings
    D  shared set   x  Warriner ratings

A->C isolates coverage holding NRC ratings fixed; C->D isolates ratings holding the token
set fixed; D->B is the residual coverage effect on the Warriner side.

A second, independent check needs no re-estimation at all. Valence and arousal are scored
on *identical* matched collocates within a lexicon, so comparing how well the two
dimensions survive the substitution holds coverage exactly constant and varies only which
rating is read.

Run from the repository root with the project environment:

    python notebooks/08_measurement_invariance/run_lexicon_decomposition.py
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parents[2]
NRC_PATH = ROOT / "data/interim/lsc/vad/lsc_vad_collocate_matches.parquet"
WAR_PATH = ROOT / "data/interim/lsc/warriner_vad/lsc_warriner_collocate_matches.parquet"
OUT_DIR = ROOT / "data/processed/lsc/measurement"

KEY = ["context_row_id", "lsc_year", "analysis_unit", "frame_stratum"]
REPORTED_STRATA = ["substantive_core_overall", "unframed_baseline"]
DIMENSIONS = {
    "valence": ("valence", "valence_scaled_0_1"),
    "arousal": ("arousal", "arousal_scaled_0_1"),
}


def fit(frame: pd.DataFrame, col: str) -> dict[str, float]:
    """Standardised annual trend, matching the convention used throughout the paper."""
    d = frame[["lsc_year", col]].dropna().sort_values("lsc_year")
    x = d["lsc_year"].to_numpy(float)
    y = d[col].to_numpy(float)
    x = x - x.mean()
    r = stats.linregress(x, y)
    beta = r.slope * np.std(x, ddof=1) / np.std(y, ddof=1) if np.std(y, ddof=1) else np.nan
    return {"slope": float(r.slope), "p": float(r.pvalue), "beta": float(beta)}


def verdict(a: dict[str, float], b: dict[str, float]) -> str:
    sa, sb = a["p"] < 0.05, b["p"] < 0.05
    if sa != sb:
        return "differ"
    if not sa:
        return "agree"
    return "agree" if (a["slope"] > 0) == (b["slope"] > 0) else "differ"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    nrc = pd.read_parquet(
        NRC_PATH,
        columns=KEY + ["doc_id", "term_role", "collocate", "collocate_type", "valence", "arousal"],
    )
    war = pd.read_parquet(
        WAR_PATH,
        columns=KEY + ["collocate", "collocate_type", "valence_scaled_0_1", "arousal_scaled_0_1"],
    )
    nrc = nrc[nrc.frame_stratum.isin(REPORTED_STRATA)].copy()
    war = war[war.frame_stratum.isin(REPORTED_STRATA)].copy()

    shared = nrc.merge(war, on=KEY + ["collocate"], how="inner", suffixes=("", "_w"))
    nrc_only = len(nrc) - len(shared)
    war_only = len(war) - len(shared)

    # ---- what the coverage gap is made of -------------------------------------------
    only_nrc = nrc.merge(
        war[KEY + ["collocate"]], on=KEY + ["collocate"], how="left", indicator=True
    )
    only_nrc = only_nrc[only_nrc["_merge"] == "left_only"]
    mwe_share = float((only_nrc["collocate_type"] != "unigram").mean())
    composition = pd.DataFrame(
        [
            {
                "nrc_matches": len(nrc),
                "warriner_matches": len(war),
                "shared_matches": len(shared),
                "nrc_only_matches": nrc_only,
                "warriner_only_matches": war_only,
                "nrc_only_share_of_nrc": nrc_only / len(nrc),
                "warriner_only_share_of_warriner": war_only / len(war),
                "mwe_share_of_nrc_only": mwe_share,
                "unique_nrc_only_types": only_nrc["collocate"].nunique(),
            }
        ]
    )
    composition.to_csv(OUT_DIR / "lexicon_coverage_composition.csv", index=False)
    print("Coverage composition")
    print(f"  NRC matched {len(nrc):,} occurrences, Warriner {len(war):,}, shared {len(shared):,}")
    print(
        f"  NRC-only {nrc_only:,} ({nrc_only / len(nrc):.1%} of NRC); "
        f"of these {mwe_share:.1%} are multi-word entries"
    )
    print(f"  Warriner-only {war_only:,} ({war_only / len(war):.1%} of Warriner)\n")

    # ---- ratings agreement on shared vocabulary --------------------------------------
    rating_rows = []
    for dim, (ncol, wcol) in DIMENSIONS.items():
        token = float(np.corrcoef(shared[ncol], shared[wcol])[0, 1])
        types = shared.groupby("collocate")[[ncol, wcol]].mean()
        type_r = float(np.corrcoef(types[ncol], types[wcol])[0, 1])
        rating_rows.append(
            {
                "dimension": dim,
                "token_weighted_r": token,
                "type_level_r": type_r,
                "n_tokens": len(shared),
                "n_types": len(types),
            }
        )
        print(
            f"Ratings agreement on shared vocabulary, {dim}: "
            f"token-weighted r = {token:.3f}, type-level r = {type_r:.3f}"
        )
    pd.DataFrame(rating_rows).to_csv(OUT_DIR / "lexicon_rating_agreement.csv", index=False)
    print()

    # ---- the four cells --------------------------------------------------------------
    records = []
    for dim, (ncol, wcol) in DIMENSIONS.items():
        cells = {
            "A_nrc_set_nrc_ratings": (nrc, ncol),
            "B_warriner_set_warriner_ratings": (war, wcol),
            "C_shared_set_nrc_ratings": (shared, ncol),
            "D_shared_set_warriner_ratings": (shared, wcol),
        }
        annual = {}
        for name, (frame, col) in cells.items():
            annual[name] = (
                frame.groupby(["analysis_unit", "frame_stratum", "lsc_year"], as_index=False)[col]
                .mean()
                .rename(columns={col: "value"})
            )
        grouped = annual["A_nrc_set_nrc_ratings"].groupby(["analysis_unit", "frame_stratum"])
        for (unit, stratum), _ in grouped:
            fits = {}
            for name, table in annual.items():
                sub = table[(table.analysis_unit == unit) & (table.frame_stratum == stratum)]
                fits[name] = fit(sub, "value")
            records.append(
                {
                    "dimension": dim,
                    "analysis_unit": unit,
                    "frame_stratum": stratum,
                    **{f"{k}_beta": v["beta"] for k, v in fits.items()},
                    **{f"{k}_p": v["p"] for k, v in fits.items()},
                    "verdict_A_vs_B": verdict(
                        fits["A_nrc_set_nrc_ratings"], fits["B_warriner_set_warriner_ratings"]
                    ),
                    "coverage_effect_A_to_C": fits["C_shared_set_nrc_ratings"]["beta"]
                    - fits["A_nrc_set_nrc_ratings"]["beta"],
                    "rating_effect_C_to_D": fits["D_shared_set_warriner_ratings"]["beta"]
                    - fits["C_shared_set_nrc_ratings"]["beta"],
                    "residual_coverage_D_to_B": fits["B_warriner_set_warriner_ratings"]["beta"]
                    - fits["D_shared_set_warriner_ratings"]["beta"],
                    "total_A_to_B": fits["B_warriner_set_warriner_ratings"]["beta"]
                    - fits["A_nrc_set_nrc_ratings"]["beta"],
                }
            )

    out = pd.DataFrame(records)
    out.to_csv(OUT_DIR / "lexicon_decomposition.csv", index=False)
    print(f"wrote {(OUT_DIR / 'lexicon_decomposition.csv').relative_to(ROOT)}\n")

    for dim in DIMENSIONS:
        d = out[out.dimension == dim]
        print(f"{dim.upper()}  (standardised beta; total shift A->B decomposed)")
        print(
            f"  {'series':<14}{'A':>7}{'B':>7}{'total':>8}"
            f"{'coverage':>10}{'ratings':>9}{'resid':>8}  verdict"
        )
        for _, r in d.iterrows():
            print(
                f"  {r.analysis_unit:<14}{r.A_nrc_set_nrc_ratings_beta:+7.2f}"
                f"{r.B_warriner_set_warriner_ratings_beta:+7.2f}{r.total_A_to_B:+8.2f}"
                f"{r.coverage_effect_A_to_C:+10.2f}{r.rating_effect_C_to_D:+9.2f}"
                f"{r.residual_coverage_D_to_B:+8.2f}  {r.verdict_A_vs_B}"
            )
        share = d[
            ["coverage_effect_A_to_C", "rating_effect_C_to_D", "residual_coverage_D_to_B"]
        ].abs()
        tot = share.sum(axis=1)
        print(
            f"  mean share of |movement|: "
            f"coverage {(share.coverage_effect_A_to_C / tot).mean():.0%}, "
            f"ratings {(share.rating_effect_C_to_D / tot).mean():.0%}, "
            f"residual coverage {(share.residual_coverage_D_to_B / tot).mean():.0%}\n"
        )


if __name__ == "__main__":
    main()
