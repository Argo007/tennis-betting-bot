#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Append live picks to a persistent trade log with Kelly-sized stakes.
Usage:
  python scripts/log_live_picks.py --picks live_results/picks_live.csv --state-dir state --kelly 0.5
"""
import argparse, os, json, time, pandas as pd, numpy as np

ap = argparse.ArgumentParser()
ap.add_argument("--picks", required=True)
ap.add_argument("--state-dir", default="state")
ap.add_argument("--kelly", type=float, default=0.5)
ap.add_argument("--max-stake-eur", type=float, default=200.0, help="Absolute stake cap")
ap.add_argument("--stake-cap", type=float, default=0.05, help="Max % of bankroll")
args = ap.parse_args()

os.makedirs(args.state_dir, exist_ok=True)
state_path = os.path.join(args.state_dir, "bankroll.json")
log_path = os.path.join(args.state_dir, "trade_log.csv")

# Read bankroll; default to 1000 if missing
bankroll = 1000.0
if os.path.isfile(state_path):
    with open(state_path, "r") as f:
        bankroll = float(json.load(f).get("bankroll", 1000.0))

picks = pd.read_csv(args.picks) if os.path.isfile(args.picks) else pd.DataFrame()
if picks.empty:
    print("No live picks to log.")
    raise SystemExit(0)

# Normalize columns
if "price" in picks.columns and "odds" not in picks.columns:
    picks["odds"] = picks["price"]
if "p_model" in picks.columns and "p" not in picks.columns:
    picks["p"] = picks["p_model"]

# Kelly sizing per row
def kelly_fraction(odds, p):
    b = odds - 1.0
    q = 1 - p
    if b <= 0: return 0.0
    return max(0.0, min(1.0, (b*p - q) / b))

rows = []
ts = int(time.time())
for _, r in picks.iterrows():
    odds = float(r["odds"])
    p = float(r["p"])
    f = kelly_fraction(odds, p) * args.kelly
    f = min(f, args.stake_cap)  # risk cap on fraction
    stake = min(bankroll * f, args.max_stake_eur)

    rows.append({
        "ts": ts,
        "match_id": r.get("match_id", ""),
        "selection": r.get("sel", f"{r.get('player_a','')} vs {r.get('player_b','')}"),
        "odds": round(odds, 2),
        "p": round(p, 4),
        "edge": round(p - 1.0/odds, 4),
        "kelly_fraction": round(f, 4),
        "stake_eur": round(stake, 2),
        "bankroll_snapshot": round(bankroll, 2),
        "status": "open"   # later we can settle and compute PnL/CLV
    })

new = pd.DataFrame(rows)

# Deduplicate: avoid adding same (ts, match_id, selection) twice
if os.path.isfile(log_path):
    log = pd.read_csv(log_path)
    combined = pd.concat([log, new], ignore_index=True).drop_duplicates(subset=["ts","match_id","selection"])
else:
    combined = new

combined.to_csv(log_path, index=False)
print(f"Logged {len(new)} picks -> {log_path} (total {len(combined)})")
