#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, time, random
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--matches", required=True, help="CSV with match_id, player_a, player_b")
ap.add_argument("--out", required=True, help="Output odds CSV")
ap.add_argument("--overround", type=float, default=1.04, help="Book margin factor (sum probs ~ overround)")
args = ap.parse_args()

os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

m = pd.read_csv(args.matches)
required_cols = {"match_id", "player_a", "player_b"}
missing = required_cols - set(map(str, m.columns))
if missing:
    raise SystemExit(f"fetch_live_odds.py: {args.matches} missing columns: {sorted(missing)}")

rows = []
ts = int(time.time())

for _, r in m.iterrows():
    mid = str(r["match_id"])
    a = str(r["player_a"])
    b = str(r["player_b"])

    # deterministic seed by match + minute bucket -> stable within a minute
    seed_base = f"{mid}-{int(ts/60)}"
    random.seed(seed_base)

    # pick a fair probability for A from [0.35..0.65], B = 1 - pA
    pA_fair = random.uniform(0.35, 0.65)
    pB_fair = 1.0 - pA_fair

    # apply overround: scale probabilities so they sum to > 1
    scale = args.overround / (pA_fair + pB_fair)  # denominator is 1, but keep formula explicit
    pA_book = pA_fair * scale
    pB_book = pB_fair * scale

    # convert to decimal odds; clamp to sensible bounds
    def to_odds(p):
        p = max(1e-6, min(0.99, p))
        return max(1.01, round(1.0 / p, 3))

    oa = to_odds(pA_book)
    ob = to_odds(pB_book)

    rows.append({"match_id": mid, "book": "SimBook", "market": "ML", "sel": a, "odds": oa, "ts": ts})
    rows.append({"match_id": mid, "book": "SimBook", "market": "ML", "sel": b, "odds": ob, "ts": ts})

out = pd.DataFrame(rows)
out.to_csv(args.out, index=False)
print(f"Wrote {len(out)} live odds rows -> {args.out}")
