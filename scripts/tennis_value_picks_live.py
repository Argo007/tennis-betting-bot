#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Make live value picks from a live odds CSV.

Accepts either:
  --out <file.csv>               (explicit output file)
or
  --outdir <folder>              (we'll write <outdir>/picks_live.csv)

Also accepts the canonical --min-edge (and keeps --edge as alias).
"""

import argparse
import os
import sys
import pandas as pd

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--odds", required=True, help="Input live odds CSV")
    # Backward/forward compatible edge flag
    p.add_argument("--min-edge", "--edge", dest="min_edge", type=float, default=0.05,
                   help="Minimum edge threshold (e.g., 0.05)")
    # Output options
    p.add_argument("--out", default=None, help="Output file (CSV)")
    p.add_argument("--outdir", default=None, help="Output directory (we write picks_live.csv inside)")
    return p.parse_args()

def coerce_prob(p):
    """Coerce probability column to 0-1 float if it looks like percentages."""
    if pd.isna(p):
        return p
    try:
        x = float(p)
    except Exception:
        return None
    # If looks like 54.9 (not 0.549), assume percent
    if x > 1.0:
        x = x / 100.0
    return x

def main():
    args = parse_args()

    # Resolve output path
    if args.out:
        out_path = args.out
    else:
        outdir = args.outdir or "live_results"
        out_path = os.path.join(outdir, "picks_live.csv")

    # Ensure output folder exists (never mkdir a file path)
    out_dirname = os.path.dirname(out_path) or "."
    os.makedirs(out_dirname, exist_ok=True)

    # Load odds
    try:
        df = pd.read_csv(args.odds)
    except FileNotFoundError:
        print(f"ERROR: odds file not found: {args.odds}", file=sys.stderr)
        sys.exit(2)

    if df.empty:
        # Write empty, exit 0 so workflow continues
        pd.DataFrame(columns=["match_id","sel","odds","p","edge"]).to_csv(out_path, index=False)
        print("No live odds; wrote empty picks file.")
        return 0

    # Normalize expected columns
    # Try to locate columns in a forgiving way
    cols = {c.lower(): c for c in df.columns}
    def pick_col(*cands):
        for c in cands:
            if c in cols: return cols[c]
        return None

    col_match = pick_col("match_id","match","id")
    col_sel   = pick_col("sel","selection","pick")
    col_odds  = pick_col("odds","price")
    col_p     = pick_col("p","prob","probability")

    missing = [name for name,val in
               [("match_id",col_match),("sel",col_sel),("odds",col_odds)]
               if val is None]
    if missing:
        print(f"ERROR: missing required columns in odds CSV: {missing}", file=sys.stderr)
        sys.exit(2)

    # If no prob column, can't compute edge safely: create empty picks
    if col_p is None:
        print("WARNING: no probability column found; emitting empty picks.")
        pd.DataFrame(columns=["match_id","sel","odds","p","edge"]).to_csv(out_path, index=False)
        return 0

    # Build working frame
    out = pd.DataFrame({
        "match_id": df[col_match].astype(str),
        "sel": df[col_sel].astype(str),
        "odds": pd.to_numeric(df[col_odds], errors="coerce"),
        "p": df[col_p].apply(coerce_prob)
    }, copy=False)

    # Compute edge if not present; if an 'edge' column exists, respect it
    col_edge = pick_col("edge")
    if col_edge and col_edge in df.columns:
        out["edge"] = pd.to_numeric(df[col_edge], errors="coerce")
    else:
        out["edge"] = out["p"] * out["odds"] - 1.0

    # Filter by edge threshold
    out = out.dropna(subset=["odds","p","edge"])
    out = out[out["edge"] >= float(args.min_edge)].copy()

    # Sort best first, write
    out = out.sort_values(["edge","p","odds"], ascending=[False, False, True])
    out.to_csv(out_path, index=False)
    print(f"Wrote {len(out)} picks to {out_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
