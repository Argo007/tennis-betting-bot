#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Log live picks to state/trade_log.csv with Kelly stake sizing and safety caps.

Inputs:
  --picks live_results/picks_live.csv
  --state-dir state
Options:
  --kelly 0.5
  --stake-cap 0.05           # max % bankroll per bet
  --max-stake-eur 200        # absolute cap in EUR
"""
import os, argparse, time, json
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--picks", required=True)
ap.add_argument("--state-dir", default="state")
ap.add_argument("--kelly", type=float, default=float(os.getenv("KELLY_SCALE", "0.5")))
ap.add_argument("--stake-cap", type=float, default=float(os.getenv("STAKE_CAP", "0.05")))
ap.add_argument("--max-stake-eur", type=float, default=float(os.getenv("MAX_STAKE_EUR", "200")))
args = ap.parse_args()

os.makedirs(args.state_dir, exist_ok=True)
log_path   = os.path.join(args.state_dir, "trade_log.csv")
bank_path  = os.path.join(args.state_dir, "bankroll.json")

def load_bankroll():
    if os.path.isfile(bank_path):
        try:
            return float(json.load(open(bank_path))["bankroll"])
        except Exception:
            pass
    return 1000.0

def save_bankroll(v):
    with open(bank_path, "w") as f:
        json.dump({"bankroll": float(v)}, f)

def kelly_fraction(p, b):
    # Kelly: f* = (bp - q)/b where b = odds-1, q = 1-p
    b = float(b)
    p = float(p)
    q = 1.0 - p
    f = (b*p - q) / max(b, 1e-9)
    return max(0.0, f)

picks = pd.read_csv(args.picks)
if picks.empty:
    print("No picks in input; nothing to log.")
    raise SystemExit(0)

bankroll = load_bankroll()
ts = int(time.time())

rows = []
for _, r in picks.iterrows():
    mid = r.get("match_id","")
    sel = r.get("sel", r.get("selection",""))
    odds = float(r.get("odds", 0))
    p    = float(r.get("p", 0))
    edge = float(r.get("edge", p - 1.0/max(odds,1e-9)))

    b = max(odds - 1.0, 1e-9)
    f_star = kelly_fraction(p, b) * float(args.kelly)
    stake = f_star * bankroll
    # safety caps
    stake = min(stake, args.stake_cap * bankroll)
    stake = min(stake, args.max_stake_eur)
    stake = max(0.0, round(stake, 2))
    if stake <= 0:
        continue

    rows.append({
        "ts": ts,
        "match_id": mid,
        "selection": sel,
        "odds": float(odds),
        "p": float(p),
        "edge": float(edge),
        "stake_eur": float(stake),
        "status": "open",
        "bankroll_snapshot": float(bankroll),
    })

if not rows:
    print("All stakes zero after caps; nothing to log.")
    raise SystemExit(0)

new = pd.DataFrame(rows)

if os.path.isfile(log_path):
    try:
        old = pd.read_csv(log_path)
        log = pd.concat([old, new], ignore_index=True)
    except Exception:
        log = new
else:
    log = new

log.to_csv(log_path, index=False)
print(f"Logged {len(new)} trades to {log_path} at bankroll €{bankroll:.2f}")
# Note: we don't deduct stakes from bankroll at logging time; settlement handles bankroll updates.
if not os.path.isfile(bank_path):
    save_bankroll(bankroll)
