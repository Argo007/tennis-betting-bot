#!/usr/bin/env python3
import argparse, os, pandas as pd, numpy as np

ap = argparse.ArgumentParser()
ap.add_argument("--matches", required=True)
ap.add_argument("--out", required=True)
args = ap.parse_args()

os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

m = pd.read_csv(args.matches)
rng = np.random.default_rng(123)
rows = []
for _, r in m.iterrows():
    oa = round(float(rng.uniform(1.70, 2.60)), 2)
    ob = round(max(1.01, 1/(2 - 1/oa)), 2)  # rough complement
    rows.append({"match_id":r["match_id"],"book":"SimBook","market":"ML","sel":r["player_a"],"odds":oa,"ts":pd.Timestamp.utcnow().isoformat()})
    rows.append({"match_id":r["match_id"],"book":"SimBook","market":"ML","sel":r["player_b"],"odds":ob,"ts":pd.Timestamp.utcnow().isoformat()})
pd.DataFrame(rows).to_csv(args.out, index=False)
print(f"Wrote live odds -> {args.out}")
