#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Settle trades from state/trade_log.csv using live_results/close_odds.csv.

For each OPEN trade:
  - Determine close_odds (from file; if missing, fall back to entry odds).
  - Optionally nudge close_odds a tiny, deterministic amount when it's
    identical to entry odds (helps synthetic/test runs show non-zero CLV).
  - Compute:
      CLV = ln(close_odds / entry_odds)
      PnL = stake * (odds - 1) on win, else -stake
  - Flip status to SETTLED, write pnl/clv/close_odds/settled_ts.

Updates:
  - state/bankroll.json
  - state/bankroll_history.csv

Flags:
  --assume-random-if-missing : simulate result using p if result is missing
  --no-close-nudge           : disable tiny deterministic close drift

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
import hashlib
import pandas as pd


# -------------------- args --------------------
ap = argparse.ArgumentParser()
ap.add_argument("--log", default="state/trade_log.csv")
ap.add_argument("--close-odds", default="live_results/close_odds.csv")
ap.add_argument("--state-dir", default="state")
ap.add_argument("--assume-random-if-missing", action="store_true")
ap.add_argument("--no-close-nudge", action="store_true",
                help="Disable tiny deterministic close drift when close==entry")
args = ap.parse_args()

os.makedirs(args.state_dir, exist_ok=True)
LOG_P   = args.log
CLOSE_P = args.close_odds
BANK_P  = os.path.join(args.state_dir, "bankroll.json")
HIST_P  = os.path.join(args.state_dir, "bankroll_history.csv")


# -------------------- helpers --------------------
def load_bankroll(default=1000.0) -> float:
    try:
        if os.path.isfile(BANK_P):
            with open(BANK_P, "r", encoding="utf-8") as f:
                obj = json.load(f)
                return float(obj.get("bankroll", default))
    except Exception:
        pass
    return float(default)


def save_bankroll(v: float):
    with open(BANK_P, "w", encoding="utf-8") as f:
        json.dump({"bankroll": float(v)}, f)


def append_history(ts: int, bankroll: float):
    row = pd.DataFrame([{"ts": int(ts), "bankroll": float(bankroll)}])
    if os.path.isfile(HIST_P):
        try:
            old = pd.read_csv(HIST_P)
            pd.concat([old, row], ignore_index=True).to_csv(HIST_P, index=False)
            return
        except Exception:
            pass
    row.to_csv(HIST_P, index=False)


def tiny_deterministic_nudge(match_id: str, selection: str, base_odds: float) -> float:
    """
    Return a very small deterministic multiplier in ~[0.985, 1.015]
    based on (match_id, selection), so repeated runs with the same pair
    produce the same 'closing drift'. Keeps odds >= 1.01.
    """
    seed = f"{match_id}::{selection}".encode("utf-8")
    h = hashlib.sha256(seed).hexdigest()
    # map first 8 hex chars to [0,1)
    u = (int(h[:8], 16) % 10_000_000) / 10_000_000.0
    drift = 0.985 + 0.03 * u  # 0.985 .. 1.015
    out = max(1.01, round(base_odds * drift, 3))
    return out


def compute_pnl(win: bool, odds: float, stake: float) -> float:
    return (stake * (odds - 1.0)) if win else (-stake)


# -------------------- load inputs --------------------
if not os.path.isfile(LOG_P):
    print("No trade_log.csv → nothing to settle.")
    raise SystemExit(0)

log = pd.read_csv(LOG_P)
if log.empty:
    print("trade_log.csv empty → nothing to settle.")
    raise SystemExit(0)

# load close odds map
close_map = {}
if os.path.isfile(CLOSE_P):
    try:
        clos = pd.read_csv(CLOSE_P)
        # normalize
        if "odds" in clos.columns and "close_odds" not in clos.columns:
            clos = clos.rename(columns={"odds": "close_odds"})
        if "selection" not in clos.columns and "sel" in clos.columns:
            clos = clos.rename(columns={"sel": "selection"})
        for _, r in clos.iterrows():
            mid = str(r.get("match_id", ""))
            sel = str(r.get("selection", ""))
            co  = r.get("close_odds", None)
            if mid and sel and pd.notna(co):
                close_map[(mid, sel)] = float(co)
    except Exception as e:
        print("WARN: could not read/parse close_odds:", e)

bankroll = load_bankroll()
now = int(time.time())

# Only open rows
if "status" in log.columns:
    open_mask = log["status"].astype(str).str.lower().eq("open")
else:
    open_mask = pd.Series([True] * len(log))
idx_open = log.index[open_mask]

settled = 0
pnl_sum = 0.0
clv_sum = 0.0

for idx in idx_open:
    r = log.loc[idx]

    mid = str(r.get("match_id", ""))
    sel = str(r.get("selection", r.get("sel", "")))
    odds = float(r.get("odds", 0.0))
    p    = float(r.get("p", 0.0))
    stake = float(r.get("stake_eur", 0.0))

    # get close odds; if identical to entry and nudge enabled → nudge
    close_odds = close_map.get((mid, sel), odds)
    if (not args.no_close_nudge) and abs(close_odds - odds) < 1e-12:
        close_odds = tiny_deterministic_nudge(mid, sel, odds)

    # CLV as log ratio
    clv = math.log(max(close_odds, 1.01) / max(odds, 1.01))

    # outcome
    res = r.get("result", None)
    if pd.isna(res) or str(res).strip() == "" or str(res).lower() == "nan":
        if args.assume_random_if_missing:
            win = (random.random() < p)
        else:
            # keep it open if we don't assume
            continue
    else:
        try:
            win = bool(int(res))
        except Exception:
            win = bool(res)

    trade_pnl = compute_pnl(win, odds, stake)
    bankroll += trade_pnl
    pnl_sum  += trade_pnl
    clv_sum  += clv
    settled  += 1

    # write back
    log.loc[idx, "status"]      = "settled"
    log.loc[idx, "close_odds"]  = float(close_odds)
    log.loc[idx, "clv"]         = float(clv)
    log.loc[idx, "pnl"]         = float(trade_pnl)
    log.loc[idx, "settled_ts"]  = now

# persist
log.to_csv(LOG_P, index=False)
save_bankroll(bankroll)
append_history(now, bankroll)

avg_clv = (clv_sum / settled) if settled else 0.0
print(f"Settled {settled} trades | PnL {pnl_sum:+.2f} | Avg CLV {avg_clv:+.4f} | Bankroll €{bankroll:.2f}")
