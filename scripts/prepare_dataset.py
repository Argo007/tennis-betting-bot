#!/usr/bin/env python3
# scripts/prepare_dataset.py
import argparse
import pandas as pd
import sys
from pathlib import Path

def normalize_implied(oa, ob):
    # implied probabilities from decimal odds
    ia = 1.0 / oa
    ib = 1.0 / ob
    s = ia + ib
    return ia / s, ib / s

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="outputs/prob_enriched.csv", help="input CSV (optional)")
    p.add_argument("--out", default="results/prob_enriched.csv", help="output csv")
    args = p.parse_args()

    input_path = Path(args.input)
    fallback_paths = [
        Path("outputs/prob_enriched.csv"),
        Path("data/raw/vigfree_matches.csv"),
        Path("data/raw/odds/sample_odds.csv")
    ]
    df = None
    if input_path.exists():
        df = pd.read_csv(input_path)
    else:
        for fp in fallback_paths:
            if fp.exists():
                df = pd.read_csv(fp)
                break

    if df is None:
        print("ERROR: no input CSV found. looked for:", args.input, fallback_paths, file=sys.stderr)
        sys.exit(1)

    # Expect columns: either pa/pb exist, or oa/ob exist. We'll compute pa/pb from oa/ob if needed.
    if "oa" not in df.columns or "ob" not in df.columns:
        print("ERROR: need 'oa' and 'ob' columns to compute probabilities", file=sys.stderr)
        sys.exit(2)

    df = df.copy()
    df["oa"] = df["oa"].astype(float)
    df["ob"] = df["ob"].astype(float)
    pa_vals = []
    pb_vals = []
    for oa, ob in zip(df["oa"].values, df["ob"].values):
        pa, pb = normalize_implied(oa, ob)
        pa_vals.append(pa)
        pb_vals.append(pb)
    df["pa"] = pa_vals
    df["pb"] = pb_vals

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print("Wrote", out)

if __name__ == "__main__":
    main()
