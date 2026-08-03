#!/usr/bin/env python3
"""Split the change in aggregate affect into a within-frame and a composition component.

Písl et al. and Xiao et al. warn that an apparent semantic trend can be an artefact of a
changing mixture of contexts rather than a change in meaning. That warning is usually
raised in the abstract. Here it can be quantified, because the aggregate index is a
weighted mean over frame strata whose weights are themselves measured.

For year t the aggregate is A_t = sum_f w_ft a_ft, with w the frame share and a the
within-frame mean. The change from the first to the last year decomposes as

    dA = sum_f (w_f1 - w_f0) abar_f     <- composition: the mixture moved
       + sum_f wbar_f (a_f1 - a_f0)     <- within-frame: the frames themselves moved
       + interaction

using mean weights and mean levels, so the two main terms are symmetric.

The direction matters for how the paper's null should be read. If the composition term is
negative for arousal, an unstratified analysis of this corpus would have shown falling
intensity -- apparent vertical concept creep -- for reasons that have nothing to do with
the meaning of the targets.

Run from the repository root with the project environment:

    python notebooks/08_measurement_invariance/run_composition_decomposition.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "data/processed/lsc/measurement"
FRAMES = ["clinical_only", "lived_only", "mixed"]

MEASURES = {
    "arousal": (
        ROOT / "data/processed/lsc/intensity/lsc_intensity_annual_arousal.csv",
        "arousal_mean",
    ),
    "valence": (
        ROOT / "data/processed/lsc/sentiment/lsc_sentiment_annual_valence.csv",
        "valence_mean",
    ),
}


def frame_weights(scores: pd.DataFrame, unit: str) -> pd.DataFrame:
    """Annual share of each substantive frame, from the cell sizes the index was built on."""
    d = scores[(scores.analysis_unit == unit) & (scores.frame_stratum.isin(FRAMES))]
    counts = d.pivot_table(
        index="lsc_year", columns="frame_stratum", values="context_rows", aggfunc="sum"
    )
    return counts.div(counts.sum(axis=1), axis=0)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    records = []

    for measure, (path, col) in MEASURES.items():
        scores = pd.read_csv(path)
        for unit in ["ADHD", "Autism"]:
            w = frame_weights(scores, unit)
            a = scores[
                (scores.analysis_unit == unit) & (scores.frame_stratum.isin(FRAMES))
            ].pivot_table(index="lsc_year", columns="frame_stratum", values=col)
            years = sorted(set(w.index) & set(a.index))
            w, a = w.loc[years], a.loc[years]
            y0, y1 = years[0], years[-1]

            dw = w.loc[y1] - w.loc[y0]
            da = a.loc[y1] - a.loc[y0]
            wbar = (w.loc[y1] + w.loc[y0]) / 2
            abar = (a.loc[y1] + a.loc[y0]) / 2

            composition = float((dw * abar).sum())
            within = float((wbar * da).sum())
            total = float((w.loc[y1] * a.loc[y1]).sum() - (w.loc[y0] * a.loc[y0]).sum())
            records.append(
                {
                    "measure": measure,
                    "analysis_unit": unit,
                    "first_year": y0,
                    "last_year": y1,
                    "aggregate_first": float((w.loc[y0] * a.loc[y0]).sum()),
                    "aggregate_last": float((w.loc[y1] * a.loc[y1]).sum()),
                    "total_change": total,
                    "composition_component": composition,
                    "within_frame_component": within,
                    "interaction": total - composition - within,
                    "clinical_share_first": float(w.loc[y0, "clinical_only"]),
                    "clinical_share_last": float(w.loc[y1, "clinical_only"]),
                    "lived_share_first": float(w.loc[y0, "lived_only"]),
                    "lived_share_last": float(w.loc[y1, "lived_only"]),
                    "clinical_level": float(abar["clinical_only"]),
                    "lived_level": float(abar["lived_only"]),
                }
            )

    out = pd.DataFrame(records)
    out.to_csv(OUT_DIR / "composition_decomposition.csv", index=False)

    for measure in MEASURES:
        d = out[out.measure == measure]
        print(
            f"\n{measure.upper()}: change in the aggregate index, "
            f"{int(d.first_year.iloc[0])}-{int(d.last_year.iloc[0])}"
        )
        print(
            f"  {'unit':<8}{'clinical lvl':>13}{'lived lvl':>11}{'total':>10}"
            f"{'composition':>13}{'within':>10}{'interact':>10}"
        )
        for _, r in d.iterrows():
            print(
                f"  {r.analysis_unit:<8}{r.clinical_level:13.4f}{r.lived_level:11.4f}"
                f"{r.total_change:+10.4f}{r.composition_component:+13.4f}"
                f"{r.within_frame_component:+10.4f}{r.interaction:+10.4f}"
            )
        for _, r in d.iterrows():
            print(
                f"    {r.analysis_unit}: clinical share "
                f"{r.clinical_share_first:.1%} -> {r.clinical_share_last:.1%}; "
                f"lived {r.lived_share_first:.1%} -> {r.lived_share_last:.1%}"
            )
    print(f"\nwrote {(OUT_DIR / 'composition_decomposition.csv').relative_to(ROOT)}")


if __name__ == "__main__":
    main()
