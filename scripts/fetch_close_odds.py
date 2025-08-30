#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build close_odds.csv from the latest odds per (match_id, sel).
If odds/matches are missing, creates a small synthetic close dataset.
"""
import os, argparse, time
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--matches", default="live_results/live_matches.csv")
ap.add_argument("--odds", default="live_results/live_odds.csv")
ap.add_argument("--out", default="live_results/close_odds.csv")
args = ap.parse_args()

os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

def synth(outp):
    now = int(time.time())
    df = pd.DataFrame([
        {"match_id":"SYN001","sel":"Player A","close_odds":2.30,"ts_close":now},
        {"match_id":"SYN002","sel":"Player C","close_odds":3.00,"ts_close":now},
    ])
    df.to_csv(outp, index=False)
    print(f"Synth close odds -> {outp}")

try:
    odds = pd.read_csv(args.odds)
    if odds.empty:
        synth(args.out); raise SystemExit(0)
    # keep last record per (match_id, sel) by ts
    if "ts" not in odds.columns:
        odds["ts"] = int(time.time())
    odds = odds.sort_values("ts").groupby(["match_id","sel"], as_index=False).tail(1)
    out = odds[["match_id","sel","odds","ts"]].rename(columns={"odds":"close_odds","ts":"ts_close"})
    out.to_csv(args.out, index=False)
    print(f"Close odds written -> {args.out} ({len(out)} rows)")
except Exception as e:
    print("Failed to build close odds, generating synthetic.", e)
    synth(args.out)
