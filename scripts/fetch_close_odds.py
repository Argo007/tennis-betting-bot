#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Create a closing-odds snapshot from live odds with a small deterministic drift.
This guarantees close_odds != entry odds so CLV is informative during tests.

Input:
  --matches  live_results/live_matches.csv   (optional; used only for sanity)
  --odds     live_results/live_odds.csv      (required)
Output:
  --out      live_results/close_odds.csv     (required)
Options:
  --max-drift 0.04   (4% drift magnitude; deterministic per match+sel+minute)
"""

import argparse, os, time, hashlib
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--matches", help="CSV with match_id, player_a, player_b", default=None)
ap.add_argument("--odds", required=True, help="Input live odds CSV")
ap.add_argument("--out", required=True, help="Output close odds CSV")
ap.add_argument("--max-drift", type=float, default=0.04)
args = ap.parse_args()

os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

# Load odds
odds = pd.read_csv(args.odds)
# Normalize column names we care about
if "selection" not in odds.columns and "sel" in odds.columns:
    odds = odds.rename(columns={"sel": "selection"})
required = {"match_id", "selection", "odds"}
missing = required - set(odds.columns)
if missing:
    raise SystemExit(f"fetch_close_odds.py: {args.odds} missing columns: {sorted(missing)}")

# Deterministic drift per minute bucket
minute_bucket = int(time.time() // 60)

def drift_for(match_id: str, selection: str, max_drift: float) -> float:
    """
    Deterministic in [-max_drift, +max_drift] based on (match_id, selection, minute_bucket).
    """
    seed = f"{match_id}::{selection}::{minute_bucket}".encode("utf-8")
    h = hashlib.sha256(seed).hexdigest()
    # map first 8 hex chars -> [0,1) then -> [-1,1)
    u = (int(h[:8], 16) % 10_000_000) / 10_000_000.0
    s = 2.0 * u - 1.0
    return max(-max_drift, min(max_drift, s * max_drift))

rows = []
for _, r in odds.iterrows():
    mid = str(r["match_id"])
    sel = str(r["selection"])
    o   = float(r["odds"])
    d   = drift_for(mid, sel, args.max_drift)
    close = max(1.01, round(o * (1.0 + d), 3))
    rows.append({"match_id": mid, "selection": sel, "close_odds": close})

out = pd.DataFrame(rows)
out.to_csv(args.out, index=False)
print(f"Wrote {len(out)} close odds rows -> {args.out} (±{int(args.max_drift*100)}% drift)")
