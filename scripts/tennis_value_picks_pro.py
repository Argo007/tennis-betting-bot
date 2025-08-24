#!/usr/bin/env python3
import argparse, os, pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--outdir", required=True)
ap.add_argument("--min-edge", type=float, default=0.08)
args = ap.parse_args()
os.makedirs(args.outdir, exist_ok=True)

df = pd.read_csv(args.input)
df["implied"] = 1.0 / df["odds"]
df["edge"] = df["p"] - df["implied"]
picks = df[df["edge"] >= args.min_edge].copy().sort_values("edge", ascending=False)

out = os.path.join(args.outdir, "picks_final.csv")
picks.to_csv(out, index=False)
print(f"Picks: {len(picks)} | Saved -> {out}") 
