#!/usr/bin/env python3
"""Ask what the two breadth encoders are actually measuring.

XL-LEXEME and MPNet produce annual breadth series that disagree about direction and
significance for almost every target and comparator. Unlike the lexicons, embeddings do not
expose which tokens they scored, so the divergence cannot be decomposed arithmetically.
Instead this script probes the instruments behaviourally.

1. Concordance. Correlate the two encoders' annual breadth estimates on the same cells.

2. Target-swap probe. Take target contexts and produce a minimally edited variant in which
   the target term is replaced by the *other* target's term -- "many autistic adults
   describe masking" becomes "many ADHD adults describe masking" -- leaving the rest of the
   sentence intact. A target-aware encoder should move substantially, because the word in
   context has changed. A sentence encoder that is really tracking topic should barely move,
   because the sentence is about almost exactly the same thing. The mean cosine shift per
   encoder is therefore a measure of how much of each instrument's signal is about the
   target rather than its surroundings.

   A control condition guards against reading tokenisation noise as target sensitivity: the
   same contexts are also perturbed by replacing a *non-target* content word, which should
   move a topic-sensitive encoder at least as much as the target swap does.

Run from the repository root with the project environment:

    python notebooks/08_measurement_invariance/run_encoder_divergence.py
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
CONTEXT_PATH = ROOT / "data/interim/lsc/breadth/lsc_breadth_sampled_contexts.parquet"
XL_MODEL = ROOT / "data/external/models/xl-lexeme"
MPNET_MODEL = ROOT / "data/external/models/all-mpnet-base-v2"
OUT_DIR = ROOT / "data/processed/lsc/measurement"

N_PROBE = 3000
RANDOM_SEED = 123
MAX_SEQUENCE_LENGTH = 128
XL_BATCH = 16
MPNET_BATCH = 64
TARGET_START, TARGET_END = "<t>", "</t>"

# Surface forms are swapped for the other concept's canonical form, preserving capitalisation
# style so the edit stays natural.
SWAP = {
    "ADHD": [
        ("adhd", "autism"),
        ("attention deficit hyperactivity disorder", "autism spectrum disorder"),
        ("attention deficit disorder", "autism spectrum disorder"),
        ("attention deficit", "autism"),
    ],
    "Autism": [
        ("autism spectrum disorder", "attention deficit hyperactivity disorder"),
        ("autism spectrum", "attention deficit"),
        ("autistic", "ADHD"),
        ("autism", "adhd"),
        ("asd", "adhd"),
    ],
}
# Frequent, affectively neutral content words used for the control perturbation.
CONTROL_SWAP = [
    ("people", "individuals"),
    ("children", "youngsters"),
    ("child", "youngster"),
    ("years", "seasons"),
    ("year", "season"),
    ("work", "labour"),
    ("school", "college"),
    ("time", "period"),
    ("study", "survey"),
    ("help", "assist"),
]


def _match_case(replacement: str, original: str) -> str:
    if original.isupper():
        return replacement.upper()
    if original[:1].isupper():
        return replacement[:1].upper() + replacement[1:]
    return replacement


def swap_once(text: str, pairs: list[tuple[str, str]]) -> tuple[str, str, str] | None:
    """Replace the first matching surface form; return (new_text, found, replacement)."""
    for needle, replacement in pairs:
        m = re.search(rf"\b{re.escape(needle)}\b", text, flags=re.IGNORECASE)
        if m:
            found = m.group(0)
            new = _match_case(replacement, found)
            return text[: m.start()] + new + text[m.end() :], found, new
    return None


def mark(text: str, span: str) -> str | None:
    m = re.search(rf"\b{re.escape(span)}\b", text, flags=re.IGNORECASE)
    if not m:
        return None
    return f"{text[: m.start()]}{TARGET_START} {m.group(0)} {TARGET_END}{text[m.end() :]}"


def encode_xl(marked: list[str]) -> np.ndarray:
    """Target-masked mean pooling, as in notebooks/04_breadth/01_xl_lexeme_breadth.ipynb."""
    import torch
    from transformers import AutoModel, AutoTokenizer

    device = "mps" if torch.backends.mps.is_available() else "cpu"
    tok = AutoTokenizer.from_pretrained(str(XL_MODEL), local_files_only=True)
    tok.model_max_length = 100_000
    model = AutoModel.from_pretrained(str(XL_MODEL), local_files_only=True).to(device).eval()
    start_id = tok.convert_tokens_to_ids(TARGET_START)
    end_id = tok.convert_tokens_to_ids(TARGET_END)

    def build(text: str) -> tuple[list[int], list[int]]:
        ids = tok.encode(text, add_special_tokens=False)
        s = ids.index(start_id)
        e = ids.index(end_id, s + 1)
        payload = MAX_SEQUENCE_LENGTH - 2
        if len(ids) > payload:
            centre = (s + e) // 2
            w0 = max(0, centre - payload // 2)
            w0 = min(w0, max(0, len(ids) - payload))
            if s < w0:
                w0 = max(0, s - 1)
            if e >= w0 + payload:
                w0 = max(0, e - payload + 1)
            ids = ids[w0 : min(len(ids), w0 + payload)]
            s = ids.index(start_id)
            e = ids.index(end_id, s + 1)
        input_ids = [tok.cls_token_id] + ids + [tok.sep_token_id]
        target_mask = [0] * len(input_ids)
        for pos in range(s + 2, e + 1):
            target_mask[pos] = 1
        return input_ids, target_mask

    out = []
    for i in range(0, len(marked), XL_BATCH):
        built = [build(t) for t in marked[i : i + XL_BATCH]]
        ids_batch, mask_batch = zip(*built)
        width = max(len(x) for x in ids_batch)
        padded = [list(x) + [tok.pad_token_id] * (width - len(x)) for x in ids_batch]
        attn = [[1] * len(x) + [0] * (width - len(x)) for x in ids_batch]
        tmask = [list(m) + [0] * (width - len(m)) for m in mask_batch]
        with torch.no_grad():
            hidden = model(
                input_ids=torch.tensor(padded, device=device),
                attention_mask=torch.tensor(attn, device=device),
            ).last_hidden_state
        tm = torch.tensor(tmask, dtype=torch.bool, device=device)
        for r in range(hidden.shape[0]):
            out.append(hidden[r][tm[r]].mean(dim=0).cpu().numpy().astype("float32"))
        if (i // XL_BATCH) % 20 == 0:
            print(f"    XL-LEXEME {i + len(built):,}/{len(marked):,}", flush=True)
    return np.vstack(out)


def encode_mpnet(texts: list[str]) -> np.ndarray:
    import torch
    from sentence_transformers import SentenceTransformer

    device = "mps" if torch.backends.mps.is_available() else "cpu"
    model = SentenceTransformer(str(MPNET_MODEL), device=device, local_files_only=True)
    return model.encode(
        texts, batch_size=MPNET_BATCH, convert_to_numpy=True, show_progress_bar=False
    )


def cos(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    a = a / np.linalg.norm(a, axis=1, keepdims=True)
    b = b / np.linalg.norm(b, axis=1, keepdims=True)
    return (a * b).sum(axis=1)


def concordance() -> pd.DataFrame:
    xl = pd.read_csv(ROOT / "data/processed/lsc/breadth/lsc_breadth_annual_scores.csv")
    mp = pd.read_csv(
        ROOT
        / "data/processed/lsc/breadth/robustness_baes_mpnet"
        / "lsc_baes_mpnet_breadth_annual_scores.csv"
    )
    mp_c = pd.read_csv(
        ROOT
        / "data/processed/lsc/breadth/robustness_baes_mpnet"
        / "lsc_baes_mpnet_breadth_annual_scores_comparators.csv"
    )
    mp = pd.concat([mp, mp_c], ignore_index=True)
    col = "breadth_mean_pairwise_cosine_distance"
    key = ["analysis_unit", "lsc_year", "frame_stratum"]
    d = xl[key + [col]].merge(mp[key + [col]], on=key, suffixes=("_xl", "_mpnet"))
    rows = []
    for (unit, stratum), g in d.groupby(["analysis_unit", "frame_stratum"]):
        if len(g) < 5:
            continue
        rows.append(
            {
                "analysis_unit": unit,
                "frame_stratum": stratum,
                "n_years": len(g),
                "pearson_r": float(np.corrcoef(g[f"{col}_xl"], g[f"{col}_mpnet"])[0, 1]),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    conc = concordance()
    conc.to_csv(OUT_DIR / "encoder_annual_concordance.csv", index=False)
    print("Annual breadth concordance between encoders (same cells):")
    for _, r in conc.iterrows():
        print(f"  {r.analysis_unit:<12} {r.frame_stratum:<26} r = {r.pearson_r:+.2f}")
    print(f"  mean r across {len(conc)} series = {conc.pearson_r.mean():+.2f}\n")

    ctx = pd.read_parquet(
        CONTEXT_PATH,
        columns=[
            "analysis_unit",
            "term_role",
            "frame_stratum",
            "lsc_year",
            "marked_context",
            "marked_text",
        ],
    )
    ctx["embedding_text"] = (
        ctx["marked_context"]
        .str.replace(TARGET_START, "", regex=False)
        .str.replace(TARGET_END, "", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    ctx = ctx[
        (ctx.term_role == "target") & (ctx.frame_stratum == "substantive_core_overall")
    ].copy()
    rng = np.random.default_rng(RANDOM_SEED)
    ctx = ctx.iloc[rng.choice(len(ctx), size=min(N_PROBE * 3, len(ctx)), replace=False)]

    rows = []
    for _, r in ctx.iterrows():
        text = str(r.embedding_text)
        span = str(r.marked_text)
        swapped = swap_once(text, SWAP[r.analysis_unit])
        control = swap_once(text, CONTROL_SWAP)
        if not swapped or not control:
            continue
        orig_marked = mark(text, span)
        swap_marked = mark(swapped[0], swapped[2])
        ctrl_marked = mark(control[0], span)
        if not (orig_marked and swap_marked and ctrl_marked):
            continue
        rows.append(
            {
                "analysis_unit": r.analysis_unit,
                "orig": text,
                "swap": swapped[0],
                "ctrl": control[0],
                "orig_marked": orig_marked,
                "swap_marked": swap_marked,
                "ctrl_marked": ctrl_marked,
            }
        )
        if len(rows) >= N_PROBE:
            break
    probe = pd.DataFrame(rows)
    print(f"Target-swap probe on {len(probe):,} contexts")
    print(f"  example original: {probe.orig.iloc[0][:110]}")
    print(f"  example swapped : {probe.swap.iloc[0][:110]}\n")

    print("  encoding MPNet...", flush=True)
    mp_o = encode_mpnet(probe.orig.tolist())
    mp_s = encode_mpnet(probe.swap.tolist())
    mp_c = encode_mpnet(probe.ctrl.tolist())
    print("  encoding XL-LEXEME...", flush=True)
    xl_o = encode_xl(probe.orig_marked.tolist())
    xl_s = encode_xl(probe.swap_marked.tolist())
    xl_c = encode_xl(probe.ctrl_marked.tolist())

    res = []
    for name, (o, s, c) in {"XL-LEXEME": (xl_o, xl_s, xl_c), "MPNet": (mp_o, mp_s, mp_c)}.items():
        d_target = 1 - cos(o, s)
        d_control = 1 - cos(o, c)
        res.append(
            {
                "encoder": name,
                "n": len(probe),
                "target_swap_distance_mean": float(d_target.mean()),
                "target_swap_distance_sd": float(d_target.std(ddof=1)),
                "control_swap_distance_mean": float(d_control.mean()),
                "control_swap_distance_sd": float(d_control.std(ddof=1)),
                "target_over_control_ratio": float(d_target.mean() / d_control.mean()),
            }
        )
    out = pd.DataFrame(res)
    out.to_csv(OUT_DIR / "encoder_target_swap_probe.csv", index=False)
    print("Mean cosine distance between a context and its perturbed variant:")
    print(f"  {'encoder':<12}{'target swap':>14}{'control swap':>15}{'ratio':>9}")
    for _, r in out.iterrows():
        print(
            f"  {r.encoder:<12}{r.target_swap_distance_mean:14.4f}"
            f"{r.control_swap_distance_mean:15.4f}{r.target_over_control_ratio:9.2f}"
        )
    print(f"\nwrote {(OUT_DIR / 'encoder_target_swap_probe.csv').relative_to(ROOT)}")


if __name__ == "__main__":
    main()
