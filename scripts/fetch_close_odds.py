#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Produce a 'close' odds snapshot for CLV calculation.

Inputs:
  --matches live_results/live_matches.csv
  --odds    live_results/live_odds.csv (optional; used to anchor close odds)
Output:
  live_results/close_odds.csv with columns: match_id, sel, close_odds, close_ts

Later you can replace the jitter with a real API pull.
"""
import argparse, os, time
import numpy as np
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--matches", required=True)
ap.add_argument("--odds", default="", help="optional: live odds snapshot to anchor from")
ap.add_argument("--out", required=True)
args = ap.parse_args()

os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

# read matches
m = pd.read_csv(args.matches) if os.path.isfile(args.matches) else pd.DataFrame()
if m.empty or "match_id" not in m.columns:
    pd.DataFrame(columns=["match_id","sel","close_odds","close_ts"]).to_csv(args.out, index=False)
    print("No matches -> wrote empty close_odds.csv")
    raise SystemExit(0)

# optional anchor
od = pd.read_csv(args.odds) if (args.odds and os.path.isfile(args.odds)) else pd.DataFrame()

rng = np.random.default_rng(777)
rows = []

def jitter_from(odds):
    # small random move ±3% with a slight drift toward 2.00 (markets sharpen)
    target = 2.00
    drift = 0.25*(target - odds)
    move = drift + rng.normal(0, 0.05*odds)
    new = max(1.01, round(odds + move, 2))
    return new

ts = pd.Timestamp.utcnow().isoformat()
for _, r in m.iterrows():
    mid = r["match_id"]
    # Build two selections if we have names; otherwise generic A/B
    sel_a = r.get("player_a", "Player A")
    sel_b = r.get("player_b", "Player B")

    # Anchor from current odds if available
    if not od.empty:
        cur = od[od["match_id"] == mid]
        if not cur.empty:
            for _, rr in cur.iterrows():
                rows.append({"match_id": mid,
                             "sel": rr.get("sel", sel_a),
                             "close_odds": jitter_from(float(rr["odds"])),
                             "close_ts": ts})
            continue

    # Fallback: synthesize complementary odds ~2.0–2.8
    oa = float(rng.uniform(1.7, 2.6))
    ob = max(1.01, 1/(2 - 1/oa))
    rows.append({"match_id": mid, "sel": sel_a, "close_odds": round(jitter_from(oa), 2), "close_ts": ts})
    rows.append({"match_id": mid, "sel": sel_b, "close_odds": round(jitter_from(ob), 2), "close_ts": ts})

pd.DataFrame(rows).to_csv(args.out, index=False)
print(f"Wrote close odds -> {args.out} ({len(rows)} rows)")
