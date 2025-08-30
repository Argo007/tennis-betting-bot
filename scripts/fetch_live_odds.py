#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, time, random
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--matches", required=True, help="CSV with match_id, player_a, player_b")
ap.add_argument("--out", required=True, help="Output odds CSV")
ap.add_argument("--overround", type=float, default=1.04)
args = ap.parse_args()

os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
m = pd.read_csv(args.matches)
need = {"match_id","player_a","player_b"}
miss = need - set(m.columns)
if miss: raise SystemExit(f"{args.matches} missing {sorted(miss)}")

rows, ts = [], int(time.time())
for _, r in m.iterrows():
    mid, a, b = str(r["match_id"]), str(r["player_a"]), str(r["player_b"])
    random.seed(f"{mid}-{int(ts/60)}")
    pA = random.uniform(0.35, 0.65); pB = 1.0 - pA
    scale = args.overround / (pA + pB)
    pA *= scale; pB *= scale
    to_odds = lambda p: max(1.01, round(1.0/max(min(0.99,p),1e-6), 3))
    rows += [
      {"match_id": mid, "book": "SimBook", "market":"ML", "sel": a, "odds": to_odds(pA), "ts": ts},
      {"match_id": mid, "book": "SimBook", "market":"ML", "sel": b, "odds": to_odds(pB), "ts": ts},
    ]

pd.DataFrame(rows).to_csv(args.out, index=False)
print(f"Wrote {len(rows)} live odds rows -> {args.out}")
