#!/usr/bin/env python3
import argparse, os, pandas as pd, numpy as np

# Produces live value picks from odds using a simple model.
# Exports both "odds/p" and "price/p_model" columns.

ap = argparse.ArgumentParser()
ap.add_argument("--odds", required=True)
ap.add_argument("--outdir", required=True)
ap.add_argument("--min-edge", type=float, default=0.08)
args = ap.parse_args()

os.makedirs(args.outdir, exist_ok=True)

od = pd.read_csv(args.odds)
if od.empty:
    pd.DataFrame(columns=["match_id","sel","odds","p","edge","price","p_model"]).to_csv(
        os.path.join(args.outdir,"picks_live.csv"), index=False)
    print("No odds. Wrote empty picks_live.csv")
    raise SystemExit(0)

rng = np.random.default_rng(99)
od["implied"] = 1.0 / od["odds"]
# Toy probability model: implied ± noise (placeholder for your real model)
od["p"] = (od["implied"] + rng.normal(0, 0.03, len(od))).clip(0.05, 0.95)
od["edge"] = od["p"] - od["implied"]

picks = od[od["edge"] >= args.min_edge].copy().sort_values("edge", ascending=False)
picks["price"] = picks["odds"]
picks["p_model"] = picks["p"]

picks[["match_id","sel","odds","p","edge","price","p_model"]].to_csv(
    os.path.join(args.outdir,"picks_live.csv"), index=False)
print(f"Live picks: {len(picks)}")
