#!/usr/bin/env python3
"""Extend the MPNet breadth track to the three comparator terms.

The dissertation's MPNet robustness check (``notebooks/04_breadth/02_...``) was scoped to
the two targets, so the alternative breadth operationalisation had no comparator coverage
while the primary XL-LEXEME track did. That asymmetry made it impossible to say whether
encoder-driven disagreement is specific to the diagnostic targets or a general property of
the instruments. This script closes the gap.

It is a port of the notebook's pipeline -- context selection, dedup, closed-form dispersion,
document bootstrap, and trend fitting are reproduced exactly -- applied to the baseline
units. The port is not taken on trust: ``--validate`` re-runs it over the *targets* and
checks that it reproduces the published target estimates before the comparator numbers are
believed.

The dissertation notebooks are frozen, so this writes to separate files rather than
overwriting the existing MPNet outputs.

Usage, from the repository root with the project environment:

    python notebooks/08_measurement_invariance/run_mpnet_comparator_breadth.py --validate
    python notebooks/08_measurement_invariance/run_mpnet_comparator_breadth.py --run
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

# --------------------------------------------------------------------------------------
# Configuration, mirroring notebooks/04_breadth/02_baes_mpnet_breadth_robustness_check.ipynb
# --------------------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
CONTEXT_PATH = ROOT / "data/interim/lsc/contexts/lsc_mention_contexts.parquet"
FRAME_LABEL_PATH = ROOT / "data/processed/lsc/classification/lsc_target_context_frame_labels.csv"
LOCAL_MODEL_PATH = ROOT / "data/external/models/all-mpnet-base-v2"
OUT_DIR = ROOT / "data/processed/lsc/breadth/robustness_baes_mpnet"
PUBLISHED_TARGET_SCORES = OUT_DIR / "lsc_baes_mpnet_breadth_annual_scores.csv"

TARGET_UNITS = ["ADHD", "Autism"]
BASELINE_UNITS = ["frustration", "loneliness", "sadness"]
BASELINE_FRAME_STRATUM = "unframed_baseline"
CORE_TARGET_FRAMES = ["clinical_only", "lived_only", "mixed"]
EXPECTED_YEARS = list(range(2014, 2027))

MIN_CONTEXT_TOKENS = 5
BOOTSTRAP_REPETITIONS = 500
RANDOM_SEED = 123
DW_LOW, DW_HIGH = 1.25, 2.75
ENCODE_BATCH_SIZE = 64
TOKEN_RE = re.compile(r"\b\w+\b", flags=re.UNICODE)

# The XL-LEXEME track capped comparator cells at 1,000 contexts per unit-year while leaving
# targets uncapped. Applying the same cap here keeps the two encoders comparable on the
# comparators, which is the whole point of the exercise.
BASELINE_MAX_CONTEXTS_PER_CELL = 1000

CONTEXT_COLUMNS = [
    "doc_id",
    "lsc_year",
    "published_year",
    "source_year",
    "analysis_unit",
    "term_role",
    "target_group",
    "raw_form",
    "matched_text",
    "mention_start_char",
    "mention_end_char",
    "collapsed_matched_texts",
    "registered_domain",
    "target_sentence",
    "target_sentence_plus_adjacent",
]


# --------------------------------------------------------------------------------------
# Context selection (ported verbatim in behaviour)
# --------------------------------------------------------------------------------------


def token_count(text: object) -> int:
    return len(TOKEN_RE.findall(str(text or "")))


def split_pipe_values(value: object) -> list[str]:
    if pd.isna(value):
        return []
    return [part.strip() for part in str(value).split("|") if part.strip()]


def whitespace_flexible_pattern(text: str) -> str:
    return re.escape(" ".join(str(text).split())).replace(r"\ ", r"\s+")


def raw_form_patterns(raw_form: str) -> list[str]:
    patterns = {
        "adhd": [r"\bADHD\b"],
        "attention_deficit": [r"\battention\s+deficit(?:\s+hyperactivity(?:\s+disorder)?)?\b"],
        "autism": [r"\bautism\b"],
        "autistic": [r"\bautistic\b"],
        "autism_spectrum": [r"\bautism\s+spectrum\b"],
        "asd_disambiguated": [r"\bASD\b"],
    }
    return patterns.get(raw_form, [r"\b" + re.escape(raw_form.replace("_", " ")) + r"\b"])


def candidate_patterns(row: pd.Series) -> list[str]:
    candidates: list[str] = []
    for value in [row.get("matched_text"), *split_pipe_values(row.get("collapsed_matched_texts"))]:
        if isinstance(value, str) and value.strip():
            candidates.append(whitespace_flexible_pattern(value))
    candidates.extend(raw_form_patterns(str(row.get("raw_form") or "")))
    seen: set[str] = set()
    return [p for p in candidates if not (p in seen or seen.add(p))]


def context_candidates(row: pd.Series) -> list[tuple[str, str]]:
    sentence = str(row.get("target_sentence") or "").strip()
    adjacent = str(row.get("target_sentence_plus_adjacent") or "").strip()
    out: list[tuple[str, str]] = []
    if token_count(sentence) >= MIN_CONTEXT_TOKENS:
        out.append(("target_sentence", sentence))
        if adjacent and adjacent != sentence:
            out.append(("target_sentence_plus_adjacent", adjacent))
    else:
        if adjacent:
            out.append(("target_sentence_plus_adjacent", adjacent))
        if sentence:
            out.append(("target_sentence", sentence))
    return out


def select_embedding_context(row: pd.Series) -> dict[str, object]:
    for context_source, text in context_candidates(row):
        for pattern in candidate_patterns(row):
            if re.search(pattern, text, flags=re.IGNORECASE):
                return {
                    "embedding_text": text,
                    "context_source": context_source,
                    "context_token_count": token_count(text),
                    "markable": True,
                }
    return {"embedding_text": "", "context_source": "", "context_token_count": 0, "markable": False}


def build_analysis_contexts(units: list[str], framed: bool) -> pd.DataFrame:
    """Assemble the unit/year/stratum cells the breadth estimate is computed over."""
    contexts = pd.read_parquet(CONTEXT_PATH, columns=CONTEXT_COLUMNS).reset_index(drop=True)
    contexts = contexts.loc[contexts["analysis_unit"].isin(units)].copy()
    contexts["registered_domain"] = contexts["registered_domain"].fillna("unknown_domain")

    if not framed:
        contexts["frame_stratum"] = BASELINE_FRAME_STRATUM
        return contexts.reset_index(drop=True)

    import hashlib

    def stable_context_id(row: pd.Series) -> str:
        value = "|".join(
            str(row.get(c, ""))
            for c in [
                "doc_id",
                "analysis_unit",
                "raw_form",
                "mention_start_char",
                "mention_end_char",
            ]
        )
        return hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]

    labels = pd.read_csv(FRAME_LABEL_PATH, usecols=["context_id", "predicted_derived_frame"])
    contexts["context_id"] = contexts.apply(stable_context_id, axis=1)
    contexts = contexts.merge(labels, on="context_id", how="left")
    core = contexts.loc[contexts["predicted_derived_frame"].isin(CORE_TARGET_FRAMES)].copy()
    by_frame = core.copy()
    by_frame["frame_stratum"] = by_frame["predicted_derived_frame"]
    overall = core.copy()
    overall["frame_stratum"] = "substantive_core_overall"
    return pd.concat([overall, by_frame], ignore_index=True, sort=False)


def prepare_sample(analysis_contexts: pd.DataFrame, cap: int | None) -> pd.DataFrame:
    marks = pd.DataFrame([select_embedding_context(r) for _, r in analysis_contexts.iterrows()])
    selected = pd.concat([analysis_contexts.reset_index(drop=True), marks], axis=1)
    markable = selected.loc[selected["markable"]].copy()
    markable = markable.drop_duplicates(
        subset=["analysis_unit", "lsc_year", "frame_stratum", "doc_id", "embedding_text"]
    ).reset_index(drop=True)
    if cap is not None:
        rng = np.random.default_rng(RANDOM_SEED)
        keep_positions: list[np.ndarray] = []
        for _, group in markable.groupby(["analysis_unit", "lsc_year", "frame_stratum"], sort=True):
            positions = group.index.to_numpy()
            if len(positions) > cap:
                positions = positions[np.sort(rng.choice(len(positions), size=cap, replace=False))]
            keep_positions.append(positions)
        markable = markable.loc[np.concatenate(keep_positions)].sort_index().reset_index(drop=True)
    return markable


# --------------------------------------------------------------------------------------
# Dispersion, bootstrap, trend (ported verbatim in behaviour)
# --------------------------------------------------------------------------------------


def mean_pairwise_cosine_distance(v: np.ndarray) -> float:
    n = v.shape[0]
    if n < 2:
        return float("nan")
    s = v.sum(axis=0)
    sum_sim = (float(np.dot(s, s)) - n) / 2.0
    return float(1.0 - sum_sim / (n * (n - 1) / 2.0))


def bootstrap_document_breadth(group: pd.DataFrame, vectors: np.ndarray, rng) -> dict[str, object]:
    doc_rows = [v.to_numpy(dtype=int) for _, v in group.groupby("doc_id")["embedding_row_id"]]
    if len(doc_rows) < 2:
        return {
            "bootstrap_repetitions": 0,
            "bootstrap_unit": "doc_id",
            "breadth_bootstrap_mean": np.nan,
            "breadth_ci_low": np.nan,
            "breadth_ci_high": np.nan,
        }
    vals = []
    for _ in range(BOOTSTRAP_REPETITIONS):
        picked = rng.integers(0, len(doc_rows), len(doc_rows))
        rows = np.concatenate([doc_rows[i] for i in picked])
        if len(rows) >= 2:
            vals.append(mean_pairwise_cosine_distance(vectors[rows]))
    return {
        "bootstrap_repetitions": BOOTSTRAP_REPETITIONS,
        "bootstrap_unit": "doc_id",
        "breadth_bootstrap_mean": float(np.mean(vals)),
        "breadth_ci_low": float(np.quantile(vals, 0.025)),
        "breadth_ci_high": float(np.quantile(vals, 0.975)),
    }


def fit_trend(frame: pd.DataFrame, value_column: str) -> dict[str, object]:
    data = frame[["lsc_year", value_column]].dropna().sort_values("lsc_year")
    if len(data) < 3 or data[value_column].nunique() < 2:
        return {"n_years": len(data)}
    years = data["lsc_year"].to_numpy(dtype=float)
    values = data[value_column].to_numpy(dtype=float)
    x = years - years.mean()
    res = stats.linregress(x, values)
    residuals = values - (res.intercept + res.slope * x)
    sse, sst = float(np.sum(residuals**2)), float(np.sum((values - values.mean()) ** 2))
    r2 = 1 - sse / sst if sst else np.nan
    n = len(values)
    adj = 1 - (1 - r2) * (n - 1) / (n - 2) if n > 2 else np.nan
    beta = (
        res.slope * np.std(x, ddof=1) / np.std(values, ddof=1) if np.std(values, ddof=1) else np.nan
    )
    dw = float(np.sum(np.diff(residuals) ** 2) / sse) if sse else np.nan
    lag1 = (
        float(np.corrcoef(residuals[:-1], residuals[1:])[0, 1])
        if n >= 4 and np.std(residuals)
        else np.nan
    )
    flag = bool(not np.isnan(dw) and (dw < DW_LOW or dw > DW_HIGH))
    ar1_slope = ar1_p = np.nan
    if flag and n >= 5 and not np.isnan(lag1) and abs(lag1) < 0.98:
        ar1 = stats.linregress(x[1:] - lag1 * x[:-1], values[1:] - lag1 * values[:-1])
        ar1_slope, ar1_p = float(ar1.slope), float(ar1.pvalue)
    return {
        "n_years": n,
        "year_center": float(years.mean()),
        "linear_intercept": float(res.intercept),
        "linear_slope_per_year": float(res.slope),
        "linear_slope_se": float(res.stderr),
        "linear_p_value": float(res.pvalue),
        "linear_r_squared": r2,
        "linear_adj_r_squared": adj,
        "standardized_beta_year": beta,
        "durbin_watson": dw,
        "lag1_residual_autocorrelation": lag1,
        "autocorrelation_flag": flag,
        "ar1_sensitivity_slope_per_year": ar1_slope,
        "ar1_sensitivity_p_value": ar1_p,
    }


def encode(texts: list[str], cache_tag: str) -> np.ndarray:
    """Encode and L2-normalise, caching on a fingerprint of the exact text list."""
    import hashlib

    from sentence_transformers import SentenceTransformer

    digest = hashlib.sha256()
    for t in texts:
        digest.update(t.encode("utf-8"))
        digest.update(b"\0")
    cache = (
        ROOT
        / "data/interim/lsc/breadth_baes_mpnet"
        / f"port_cache_{cache_tag}_{digest.hexdigest()[:12]}.npy"
    )
    if cache.exists():
        print(f"  using cached embeddings: {cache.name}")
        return np.load(cache)

    device = "mps" if _mps_available() else "cpu"
    model = SentenceTransformer(str(LOCAL_MODEL_PATH), device=device, local_files_only=True)
    vectors = model.encode(
        texts,
        batch_size=ENCODE_BATCH_SIZE,
        convert_to_numpy=True,
        normalize_embeddings=False,
        show_progress_bar=True,
    )
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    normalised = vectors / np.where(norms == 0, 1.0, norms)
    cache.parent.mkdir(parents=True, exist_ok=True)
    np.save(cache, normalised)
    return normalised


def _mps_available() -> bool:
    try:
        import torch

        return bool(torch.backends.mps.is_available())
    except Exception:
        return False


def compute(units: list[str], framed: bool, cap: int | None, cache_tag: str) -> pd.DataFrame:
    analysis = build_analysis_contexts(units, framed)
    sample = prepare_sample(analysis, cap)
    print(f"  {len(sample):,} contexts after markability and dedup")

    unique = sample[["embedding_text"]].drop_duplicates().reset_index(drop=True)
    unique["embedding_row_id"] = np.arange(len(unique), dtype=int)
    print(f"  {len(unique):,} unique texts to encode")
    vectors = encode(unique["embedding_text"].tolist(), cache_tag)

    index = sample.merge(unique, on="embedding_text", how="left")
    rng = np.random.default_rng(RANDOM_SEED)
    records = []
    for (unit, year, stratum), group in index.groupby(
        ["analysis_unit", "lsc_year", "frame_stratum"], sort=True
    ):
        rows = group["embedding_row_id"].to_numpy(dtype=int)
        meta = group.iloc[0]
        records.append(
            {
                "lsc_year": int(year),
                "analysis_unit": unit,
                "frame_stratum": stratum,
                "term_role": meta["term_role"],
                "target_group": meta["target_group"],
                "breadth_mean_pairwise_cosine_distance": mean_pairwise_cosine_distance(
                    vectors[rows]
                ),
                "sampled_contexts": int(len(group)),
                "sampled_documents": int(group["doc_id"].nunique()),
                "sampled_domains": int(group["registered_domain"].nunique()),
                **bootstrap_document_breadth(group, vectors, rng),
            }
        )
    return (
        pd.DataFrame(records)
        .sort_values(["analysis_unit", "frame_stratum", "lsc_year"])
        .reset_index(drop=True)
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--validate", action="store_true", help="re-derive the published target estimates"
    )
    ap.add_argument("--run", action="store_true", help="compute and write the comparator estimates")
    args = ap.parse_args()

    if args.validate:
        print("Validating the port against the published target estimates...")
        got = compute(TARGET_UNITS, framed=True, cap=None, cache_tag="targets")
        want = pd.read_csv(PUBLISHED_TARGET_SCORES)
        col = "breadth_mean_pairwise_cosine_distance"
        merged = got.merge(
            want[["analysis_unit", "lsc_year", "frame_stratum", col]],
            on=["analysis_unit", "lsc_year", "frame_stratum"],
            suffixes=("_port", "_published"),
        )
        merged["abs_diff"] = (merged[f"{col}_port"] - merged[f"{col}_published"]).abs()
        print(f"  matched {len(merged)} of {len(want)} published cells")
        print(f"  max |difference| = {merged['abs_diff'].max():.3e}")
        print(f"  cells differing by >1e-6: {(merged['abs_diff'] > 1e-6).sum()}")
        return

    if args.run:
        print("Computing MPNet breadth for the comparator terms...")
        scores = compute(
            BASELINE_UNITS,
            framed=False,
            cap=BASELINE_MAX_CONTEXTS_PER_CELL,
            cache_tag="comparators",
        )
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        scores_path = OUT_DIR / "lsc_baes_mpnet_breadth_annual_scores_comparators.csv"
        scores.to_csv(scores_path, index=False)
        print(f"wrote {scores_path.relative_to(ROOT)}  ({len(scores)} rows)")

        col = "breadth_mean_pairwise_cosine_distance"
        trends = []
        for (unit, stratum), group in scores.groupby(["analysis_unit", "frame_stratum"], sort=True):
            meta = group.iloc[0]
            trends.append(
                {
                    "analysis_unit": unit,
                    "frame_stratum": stratum,
                    "term_role": meta["term_role"],
                    "target_group": meta["target_group"],
                    "index_name": "baes_mpnet_breadth_mean_pairwise_cosine_distance",
                    **fit_trend(group, col),
                }
            )
        trends_df = pd.DataFrame(trends)
        trends_path = OUT_DIR / "lsc_baes_mpnet_breadth_trend_models_comparators.csv"
        trends_df.to_csv(trends_path, index=False)
        print(f"wrote {trends_path.relative_to(ROOT)}  ({len(trends_df)} rows)\n")
        print(
            trends_df[
                [
                    "analysis_unit",
                    "linear_slope_per_year",
                    "linear_p_value",
                    "standardized_beta_year",
                    "linear_adj_r_squared",
                ]
            ].to_string(index=False)
        )
        return

    ap.error("pass --validate or --run")


if __name__ == "__main__":
    main()
