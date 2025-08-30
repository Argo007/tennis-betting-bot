#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Settle trades from state/trade_log.csv using close_odds.csv.
Computes CLV, PnL, and updates bankroll state/history.

Usage:
  python scripts/settle_trades.py \
    --log state/trade_log.csv \
    --close-odds live_results/close_odds.csv \
    --state-dir state \
    --assume-random-if-missing
"""
import os, argparse, time, json, random
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--log", default="state/trade_log.csv")
ap.add_argument("--close-odds", default="live_results/close_odds.csv")
ap.add_argument("--state-dir", default="state")
ap.add_argument("--assume-random-if-missing", action="store_true")
args = ap.parse_args()

os.makedirs(args.state_dir, exist_ok=True)
bank_p  = os.path.join(args.state_dir, "bankroll.json")
hist_p  = os.path.join(args.state_dir, "bankroll_history.csv")

def load_bankroll():
    if os.path.isfile(bank_p):
        try:
            return float(json.load(open(bank_p))["bankroll"])
        except Exception:
            pass
    return 1000.0

def save_bankroll(v):
    with open(bank_p, "w") as f:
        json.dump({"bankroll": float(v)}, f)

def append_hist(ts, bankroll):
    row = pd.DataFrame([{"ts": int(ts), "bankroll": float(bankroll)}])
    if os.path.isfile(hist_p):
        try:
            old = pd.read_csv(hist_p)
            pd.concat([old, row], ignore_index=True).to_csv(hist_p, index=False)
            return
        except Exception:
            pass
    row.to_csv(hist_p, index=False)

if not os.path.isfile(args.log):
    print("No trade_log.csv -> nothing to settle.")
    raise SystemExit(0)

log = pd.read_csv(args.log)
if log.empty:
    print("trade_log.csv empty -> nothing to settle.")
    raise SystemExit(0)

# Load close odds
close_map = {}
if os.path.isfile(args.close_odds):
    try:
        clos = pd.read_csv(args.close_odds)
        for _, r in clos.iterrows():
            close_map[(str(r.get("match_id","")), str(r.get("sel","")))] = float(r.get("close_odds", r.get("odds", 0)))
    except Exception:
        pass

bankroll = load_bankroll()
settled_any = False
now = int(time.time())

# Work only on OPEN trades
def compute_pnl(win, odds, stake):
    return float(stake) * (float(odds) - 1.0) if win else -float(stake)

for idx, r in log.iterrows():
    status = str(r.get("status","")).lower()
    if status == "settled":
        continue

    mid = str(r.get("match_id",""))
    sel = str(r.get("selection", r.get("sel","")))
    odds = float(r.get("odds", 0))
    p    = float(r.get("p", 0))
    stake = float(r.get("stake_eur", 0))

    # CLV
    close_odds = close_map.get((mid, sel), float("nan"))
    if not pd.notna(close_odds):
        close_odds = odds  # fallback: flat close

    clv = float(pd.np.log(close_odds / max(odds, 1e-9)))  # log(close/entry)

    # Outcome
    res = r.get("result", None)
    if pd.isna(res) or res == "" or str(res).lower() == "nan":
        if args.assume_random_if_missing:
            win = random.random() < p
        else:
            # leave open if we can't assume result
            continue
    else:
        win = bool(int(res))

    pnl = compute_pnl(win, odds, stake)
    bankroll += pnl
    log.loc[idx, "status"] = "settled"
    log.loc[idx, "close_odds"] = float(close_odds)
    log.loc[idx, "clv"] = float(clv)
    log.loc[idx, "pnl"] = float(pnl)
    log.loc[idx, "settled_ts"] = now
    settled_any = True

# Persist
log.to_csv(args.log, index=False)
save_bankroll(bankroll)
append_hist(now, bankroll)

print(f"Settled={settled_any} | New bankroll €{bankroll:.2f}")

