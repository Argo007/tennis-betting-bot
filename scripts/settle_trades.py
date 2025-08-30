#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Settle trades from state/trade_log.csv using live_results/close_odds.csv.
Computes:
    • CLV = log(close_odds / entry_odds)
    • PnL per trade
    • Bankroll updates and bankroll history

Usage:
    python scripts/settle_trades.py \
        --log state/trade_log.csv \
        --close-odds live_results/close_odds.csv \
        --state-dir state \
        --assume-random-if-missing
"""

import os
import argparse
import time
import json
import math
import random
import pandas as pd

# ------------------- ARGPARSE -------------------
ap = argparse.ArgumentParser()
ap.add_argument("--log", default="state/trade_log.csv")
ap.add_argument("--close-odds", default="live_results/close_odds.csv")
ap.add_argument("--state-dir", default="state")
ap.add_argument("--assume-random-if-missing", action="store_true")
args = ap.parse_args()

# ------------------- PATHS -------------------
os.makedirs(args.state_dir, exist_ok=True)
LOG_P   = args.log
CLOSE_P = args.close_odds
BANK_P  = os.path.join(args.state_dir, "bankroll.json")
HIST_P  = os.path.join(args.state_dir, "bankroll_history.csv")

# ------------------- BANKROLL UTILS -------------------
def load_bankroll(default=1000.0) -> float:
    """Load bankroll from bankroll.json, fallback to default if missing/corrupt."""
    try:
        if os.path.isfile(BANK_P):
            with open(BANK_P, "r", encoding="utf-8") as f:
                obj = json.load(f)
                return float(obj.get("bankroll", default))
    except Exception:
        pass
    return float(default)

def save_bankroll(v: float):
    """Persist bankroll to bankroll.json."""
    with open(BANK_P, "w", encoding="utf-8") as f:
        json.dump({"bankroll": float(v)}, f)

def append_history(ts: int, bankroll: float):
    """Append bankroll snapshot to bankroll_history.csv."""
    row = pd.DataFrame([{"ts": int(ts), "bankroll": float(bankroll)}])
    if os.path.isfile(HIST_P):
        try:
            old = pd.read_csv(HIST_P)
            pd.concat([old, row], ignore_index=True).to_csv(HIST_P, index=False)
            return
        except Exception:
            pass
    row.to_csv(HIST_P, index=False)

# ------------------- PRECHECKS -------------------
if not os.path.isfile(LOG_P):
    print("No trade_log.csv → nothing to settle.")
    raise SystemExit(0)

log = pd.read_csv(LOG_P)
if log.empty:
    print("trade_log.csv empty → nothing to settle.")
    raise SystemExit(0)

# ------------------- LOAD CLOSE ODDS -------------------
close_map = {}
if os.path.isfile(CLOSE_P):
    try:
        clos = pd.read_csv(CLOSE_P)
        # normalize column names
        if "odds" in clos.columns and "close_odds" not in clos.columns:
            clos = clos.rename(columns={"odds": "close_odds"})
        for _, r in clos.iterrows():
            mid = str(r.get("match_id", ""))
            sel = str(r.get("sel", r.get("selection", "")))
            co  = float(r.get("close_odds", float("nan")))
            if mid and sel and pd.notna(co):
                close_map[(mid, sel)] = co
    except Exception as e:
        print("WARN: could not read close odds:", e)

# ------------------- SETTLEMENT -------------------
bankroll = load_bankroll()
now = int(time.time())
settled_count = 0
pnl_sum = 0.0

def compute_pnl(win: bool, odds: float, stake: float) -> float:
    odds = float(odds)
    stake = float(stake)
    return stake * (odds - 1.0) if win else -stake

# filter open trades
status_s = log.get("status", pd.Series([""] * len(log))).astype(str).str.lower()
open_idx = status_s == "open"

for idx in log.index[open_idx]:
    r = log.loc[idx]
    mid = str(r.get("match_id", ""))
    sel = str(r.get("selection", r.get("sel", "")))
    odds = float(r.get("odds", 0.0))
    p    = float(r.get("p", 0.0))
    stake = float(r.get("stake_eur", 0.0))

    # CLV: log(close_odds / entry_odds)
    close_odds = close_map.get((mid, sel), odds)
    clv = math.log(max(close_odds, 1e-9) / max(odds, 1e-9))

    # outcome
    res = r.get("result", None)
    if pd.isna(res) or str(res).strip() == "" or str(res).lower() == "nan":
        if args.assume_random_if_missing:
            win = random.random() < p  # simulate outcome
        else:
            continue  # leave as open
    else:
        try:
            win = bool(int(res))
        except Exception:
            win = bool(res)

    # compute pnl and update bankroll
    trade_pnl = compute_pnl(win, odds, stake)
    bankroll += trade_pnl
    pnl_sum  += trade_pnl
    settled_count += 1

    # write back to log
    log.loc[idx, "status"]      = "settled"
    log.loc[idx, "close_odds"]  = float(close_odds)
    log.loc[idx, "clv"]         = float(clv)
    log.loc[idx, "pnl"]         = float(trade_pnl)
    log.loc[idx, "settled_ts"]  = now

# ------------------- SAVE EVERYTHING -------------------
log.to_csv(LOG_P, index=False)
save_bankroll(bankroll)
append_history(now, bankroll)

print(f"Settled {settled_count} trades | PnL {pnl_sum:+.2f} | Bankroll €{bankroll:.2f}")
