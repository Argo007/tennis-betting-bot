#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Settle trades from state/trade_log.csv using live_results/close_odds.csv.
Computes CLV (log close/entry), PnL, and updates bankroll state/history.

Usage:
  python scripts/settle_trades.py \
    --log state/trade_log.csv \
    --close-odds live_results/close_odds.csv \
    --state-dir state \
    --assume-random-if-missing
"""
import os, argparse, time, json, math, random
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--log", default="state/trade_log.csv")
ap.add_argument("--close-odds", default="live_results/close_odds.csv")
ap.add_argument("--state-dir", default="state")
ap.add_argument("--assume-random-if-missing", action="store_true")
args = ap.parse_args()

os.makedirs(args.state_dir, exist_ok=True)
LOG_P   = args.log
CLOSE_P = args.close_odds
BANK_P  = os.path.join(args.state_dir, "bankroll.json")
HIST_P  = os.path.join(args.state_dir, "bankroll_history.csv")

def load_bankroll(default=1000.0) -> float:
    try:
        if os.path.isfile(BANK_P):
            obj = json.load(open(BANK_P))
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

if not os.path.isfile(LOG_P):
    print("No trade_log.csv -> nothing to settle.")
    raise SystemExit(0)

log = pd.read_csv(LOG_P)
if log.empty:
    print("trade_log.csv empty -> nothing to settle.")
    raise SystemExit(0)

# Latest close price per (match_id, sel/selection)
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

bankroll = load_bankroll()
now = int(time.time())
settled_count = 0
pnl_sum = 0.0

def compute_pnl(win: bool, odds: float, stake: float) -> float:
    odds = float(odds)
    stake = float(stake)
    return stake * (odds - 1.0) if win else -stake

# Work over OPEN trades only
status_s = log.get("status", pd.Series([""] * len(log))).astype(str).str.lower()
open_idx = status_s == "open"

for idx in log.index[open_idx]:
    r = log.loc[idx]
    mid = str(r.get("match_id", ""))
    sel = str(r.get("selection", r.get("sel", "")))
    odds = float(r.get("odds", 0.0))
    p    = float(r.get("p", 0.0))
    stake = float(r.get("stake_eur", 0.0))

    # Close odds for CLV
    close_odds = close_map.get((mid, sel), odds)  # fallback to entry if missing
    clv = math.log(max(close_odds, 1e-9) / max(odds, 1e-9))

    # Determine outcome
    res = r.get("result", None)
    win: bool
    if pd.isna(res) or str(res).strip() == "" or str(res).lower() == "nan":
        if args.assume_random_if_missing:
            win = random.random() < p
        else:
            # leave as open
            continue
    else:
        try:
            win = bool(int(res))
        except Exception:
            win = bool(res)

    pnl = compute_pnl(win, odds, stake)
    bankroll += pnl
    pnl_sum += pnl
    settled_count += 1

    # write back
    log.loc[idx, "status"] = "settled"
    log.loc[idx, "close_odds"] = float(close_odds)
    log.loc[idx, "clv"] = float(clv)
    log.loc[idx, "pnl"] = float(pnl)
    log.loc[idx, "settled_ts"] = now

# Persist
log.to_csv(LOG_P, index=False)
save_bankroll(bankroll)
append_history(now, bankroll)

print(f"Settled {settled_count} trades | PnL {pnl_sum:+.2f} | Bankroll €{bankroll:.2f}")
