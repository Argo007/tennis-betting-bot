#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Log live picks to state/trade_log.csv with Kelly sizing + safety caps.
Skips duplicates by (match_id, selection).

Inputs:
  --picks live_results/picks_live.csv
  --state-dir state
Options:
  --kelly 0.5
  --stake-cap 0.05       # max % bankroll per bet
  --max-stake-eur 200    # absolute cap in EUR
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
log_path  = os.path.join(args.state_dir, "trade_log.csv")
bank_path = os.path.join(args.state_dir, "bankroll.json")

def load_bankroll() -> float:
    if os.path.isfile(bank_path):
        try:
            return float(json.load(open(bank_path))["bankroll"])
        except Exception:
            pass
    return 1000.0

def save_bankroll(v: float):
    with open(bank_path, "w") as f:
        json.dump({"bankroll": float(v)}, f)

def kelly_fraction(p: float, odds: float) -> float:
    # Kelly: f* = (bp - q) / b, where b = odds - 1, q = 1 - p
    b = max(float(odds) - 1.0, 1e-9)
    p = float(p)
    q = 1.0 - p
    f = (b * p - q) / b
    return max(0.0, f)

# Load picks
picks = pd.read_csv(args.picks)
if picks.empty:
    print("No picks in input; nothing to log.")
    raise SystemExit(0)

# Ensure required columns exist / normalize
if "selection" not in picks.columns and "sel" in picks.columns:
    picks = picks.rename(columns={"sel": "selection"})
required = {"match_id", "selection", "odds"}
missing = required - set(picks.columns)
if missing:
    raise SystemExit(f"log_live_picks.py: {args.picks} missing columns: {sorted(missing)}")

# Optional p/edge handling
if "p" not in picks.columns:
    # fallback: implied prob from odds
    picks["p"] = 1.0 / picks["odds"].clip(lower=1e-9)
if "edge" not in picks.columns:
    picks["edge"] = picks["p"] - 1.0 / picks["odds"].clip(lower=1e-9)

# Load existing log for dedupe
existing = pd.DataFrame()
if os.path.isfile(log_path):
    try:
        existing = pd.read_csv(log_path)
    except Exception:
        existing = pd.DataFrame()

def uid(m, s): return f"{m}::{s}"

existing_uids = set()
if not existing.empty:
    # if any entry (open or settled) exists for this uid, skip re-bet
    if "selection" not in existing.columns and "sel" in existing.columns:
        existing = existing.rename(columns={"sel": "selection"})
    for _, r in existing.iterrows():
        existing_uids.add(uid(r.get("match_id",""), r.get("selection","")))

bankroll = load_bankroll()
ts = int(time.time())

new_rows = []
skipped = 0

for _, r in picks.iterrows():
    mid = r.get("match_id","")
    sel = r.get("selection","")
    odds = float(r.get("odds", 0.0))
    p    = float(r.get("p", 0.0))
    edge = float(r.get("edge", p - 1.0/max(odds,1e-9)))

    u = uid(mid, sel)
    if u in existing_uids:
        skipped += 1
        continue

    f_star = kelly_fraction(p, odds) * float(args.kelly)
    stake = f_star * bankroll
    # safety caps
    stake = min(stake, args.stake_cap * bankroll)
    stake = min(stake, args.max_stake_eur)
    stake = max(0.0, round(stake, 2))
    if stake <= 0:
        skipped += 1
        continue

    new_rows.append({
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

if not new_rows:
    print(f"Nothing to log (skipped={skipped}).")
    raise SystemExit(0)

new_df = pd.DataFrame(new_rows)

# Append to log
if not existing.empty:
    log = pd.concat([existing, new_df], ignore_index=True)
else:
    log = new_df

log.to_csv(log_path, index=False)
print(f"Logged {len(new_df)} trades (skipped {skipped}) -> {log_path} at bankroll €{bankroll:.2f}")

# initialize bankroll file if missing
if not os.path.isfile(bank_path):
    save_bankroll(bankroll)
