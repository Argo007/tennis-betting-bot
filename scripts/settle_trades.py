#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Settle open trades using close odds (and optional results), compute CLV and PnL,
and update bankroll state.

Inputs:
  --log         state/trade_log.csv (created by log_live_picks.py)
  --close-odds  live_results/close_odds.csv  (from fetch_close_odds.py)
  --results     <optional> CSV with columns: match_id, winner or result (1 for our sel)
  --state-dir   state

Outputs (updated in place):
  state/trade_log.csv        (status=open->settled, close_odds, clv, pnl)
  state/bankroll.json        (bankroll += Σ pnl of newly settled)
  state/bankroll_history.csv (append new snapshot)
"""
import argparse, os, json, time
import numpy as np
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--log", default="state/trade_log.csv")
ap.add_argument("--close-odds", default="live_results/close_odds.csv")
ap.add_argument("--results", default="", help="optional: CSV with match_id,[sel|winner|result]")
ap.add_argument("--state-dir", default="state")
ap.add_argument("--assume-random-if-missing", action="store_true",
                help="If no results provided, simulate outcome by p")
args = ap.parse_args()

os.makedirs(args.state_dir, exist_ok=True)
state_path = os.path.join(args.state_dir, "bankroll.json")
hist_path  = os.path.join(args.state_dir, "bankroll_history.csv")

if not os.path.isfile(args.log):
    print("No trade log; nothing to settle.")
    raise SystemExit(0)

log = pd.read_csv(args.log)
if log.empty or "status" not in log.columns:
    print("Empty or malformed trade log.")
    raise SystemExit(0)

open_mask = log["status"].astype(str).str.lower().eq("open")
to_settle = log[open_mask].copy()
if to_settle.empty:
    print("No open trades to settle.")
    raise SystemExit(0)

# close odds
co = pd.read_csv(args.close_odds) if os.path.isfile(args.close_odds) else pd.DataFrame()
if co.empty:
    print("No close odds; cannot compute CLV. Aborting.")
    raise SystemExit(0)

# normalize for join
for c in ("match_id","sel"):
    if c in co.columns:
        co[c] = co[c].astype(str)
    if c in to_settle.columns:
        to_settle[c] = to_settle[c].astype(str)

# bring selection name into log if missing
if "sel" not in to_settle.columns:
    to_settle["sel"] = to_settle["selection"]

# join to get close_odds
merged = pd.merge(to_settle, co[["match_id","sel","close_odds"]], on=["match_id","sel"], how="left")

# optional results
res = pd.read_csv(args.results) if (args.results and os.path.isfile(args.results)) else pd.DataFrame()
res_cols = [c for c in ["match_id","sel","winner","result"] if c in res.columns]
if not res.empty and res_cols:
    res = res.copy()
    for c in ("match_id","sel"):
        if c in res.columns: res[c] = res[c].astype(str)
    merged = pd.merge(merged, res, on=[c for c in ("match_id","sel") if c in merged.columns and c in res.columns], how="left")

# derive result: prefer explicit 'result' (1/0), else compare 'sel' to 'winner', else simulate by p
def derive_result(row):
    # explicit numeric result
    rv = row.get("result", "")
    if str(rv).strip() in ("0","1"):
        return int(rv)
    # winner name
    if "winner" in row and isinstance(row["winner"], str) and "sel" in row and isinstance(row["sel"], str):
        return int(row["winner"].strip() == row["sel"].strip())
    # simulate by p if allowed
    if args.assume_random_if_missing and "p" in row:
        return int(np.random.random() < float(row["p"]))
    return None  # unknown

merged["close_odds"] = merged["close_odds"].astype(float)
merged["clv"] = np.log(merged["close_odds"]) - np.log(merged["odds"].astype(float))
merged["derived_result"] = merged.apply(derive_result, axis=1)

# PnL calc
def pnl_row(row):
    stake = float(row.get("stake_eur", 0.0))
    if row.get("derived_result", None) == 1:
        return stake * (float(row["odds"]) - 1.0)
    elif row.get("derived_result", None) == 0:
        return -stake
    else:
        return 0.0

merged["pnl"] = merged.apply(pnl_row, axis=1)
merged["settled_ts"] = int(time.time())
merged["status"] = np.where(merged["derived_result"].isin([0,1]), "settled", "open")

# write back into log (only update rows we touched)
upd = log.copy()
key_cols = ["ts","match_id","selection"]
mkey = ["ts","match_id","selection"]
if "selection" not in merged.columns and "sel" in merged.columns:
    merged = merged.rename(columns={"sel":"selection"})
merged = merged.set_index(mkey)
upd = upd.set_index(mkey)

for col in ["close_odds","clv","pnl","settled_ts","status","derived_result"]:
    upd.loc[merged.index, col] = merged[col]

upd = upd.reset_index()
upd.to_csv(args.log, index=False)
print(f"Updated trade log -> {args.log}")

# update bankroll state with newly-settled PnL
# (sum only rows we just settled)
settled_now = merged[merged["status"] == "settled"]
sum_pnl = float(settled_now["pnl"].sum()) if not settled_now.empty else 0.0

# load state
bankroll = 1000.0
state = {"bankroll": bankroll}
if os.path.isfile(state_path):
    with open(state_path, "r") as f:
        try:
            state = json.load(f)
            bankroll = float(state.get("bankroll", 1000.0))
        except Exception:
            pass

bankroll = round(bankroll + sum_pnl, 2)
state["bankroll"] = bankroll
with open(state_path, "w") as f:
    json.dump(state, f, indent=2)

# append history
hist = pd.read_csv(hist_path) if os.path.isfile(hist_path) else pd.DataFrame(columns=["ts","bankroll"])
hist = pd.concat([hist, pd.DataFrame([{"ts": int(time.time()), "bankroll": bankroll}])], ignore_index=True)
hist.to_csv(hist_path, index=False)
print(f"Bankroll updated by {sum_pnl:+.2f} -> {bankroll:.2f}")
