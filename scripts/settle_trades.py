#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Settle trades:
- reads state/trade_log.csv (open bets)
- joins with close_odds to compute CLV
- simulates/uses result to compute PnL
- updates bankroll.json and appends to bankroll_history.csv
- writes settled_trades.csv (idempotent; won’t double-settle)

Usage (now YAML-safe):
  python scripts/settle_trades.py \
      --log state/trade_log.csv \
      --close-odds live_results/close_odds.csv \
      --state-dir state \
      --assume-random-if-missing true
"""
import argparse
import os
import sys
import json
import time
from datetime import datetime
import pandas as pd

def parse_bool(v: str) -> bool:
    s = str(v).strip().lower()
    if s in {"true","t","1","yes","y"}:
        return True
    if s in {"false","f","0","no","n"}:
        return False
    raise argparse.ArgumentTypeError(f"Expected true/false, got: {v}")

def load_df(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def save_df(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True, help="state/trade_log.csv")
    ap.add_argument("--close-odds", required=False, default="", help="live_results/close_odds.csv")
    ap.add_argument("--state-dir", default="state")
    # YAML-safe explicit value (matches the workflow style)
    ap.add_argument("--assume-random-if-missing", type=parse_bool, default=True,
                    help="true/false; simulate random outcome if result is missing")
    # optional output (not required by workflow, but supported)
    ap.add_argument("--out", default="", help="optional out CSV for settled rows")
    args = ap.parse_args()

    state = args.state_dir
    os.makedirs(state, exist_ok=True)

    log_df = load_df(args.log)
    if log_df.empty:
        print("No trades to settle.")
        return 0

    # Normalize columns we rely on
    for col in ["match_id","selection","odds","stake_eur","p","ts"]:
        if col not in log_df.columns:
            log_df[col] = None

    # Ensure numeric odds/stake/p
    for col in ["odds","stake_eur","p"]:
        log_df[col] = pd.to_numeric(log_df[col], errors="coerce")

    # Attach closing odds (for CLV) if provided
    clv = pd.Series(0.0, index=log_df.index, name="clv")
    if args.close_odds and os.path.isfile(args.close_odds):
        close_df = load_df(args.close_odds)
        # Expect columns: match_id, odds_close (or odds)
        close_df = close_df.copy()
        if "odds_close" not in close_df.columns:
            # fall back to "odds"
            if "odds" in close_df.columns:
                close_df = close_df.rename(columns={"odds": "odds_close"})
            else:
                close_df["odds_close"] = pd.NA

        merged = log_df.merge(close_df[["match_id","odds_close"]], on="match_id", how="left")
        # clv = (close_odds - open_odds) / open_odds
        with pd.option_context("mode.use_inf_as_na", True):
            clv = (pd.to_numeric(merged["odds_close"], errors="coerce") - merged["odds"]) / merged["odds"]
            clv = clv.fillna(0.0)
    log_df["clv"] = clv

    # Determine win/loss. If we don’t have a result, optionally simulate fair coin (biased by p)
    # Expected result column: "pr" (profit result) or "result" (W/L)
    if "result" not in log_df.columns:
        log_df["result"] = pd.NA

    have_result_mask = log_df["result"].astype(str).str.lower().isin({"w","l","win","loss","1","0","true","false"})
    if args.assume_random_if_missing:
        import numpy as np
        rng = np.random.default_rng(int(time.time()) % (2**32 - 1))
        miss = ~have_result_mask
        # use probability p (already 0..1) if available, else 0.5
        probs = pd.to_numeric(log_df.loc[miss, "p"], errors="coerce").fillna(0.5).clip(0,1)
        draws = rng.random(len(probs))
        sim_w = (draws < probs).map({True:"W", False:"L"})
        log_df.loc[miss, "result"] = sim_w.values
        have_result_mask = log_df["result"].astype(str).str.lower().isin({"w","l","win","loss","1","0","true","false"})
    else:
        # leave missing results; they won't be settled
        pass

    # Compute PnL only for rows with result now available
    settle = log_df.loc[have_result_mask].copy()
    if not settle.empty:
        res = settle["result"].astype(str).str.lower().map(
            {"w":1,"win":1,"1":1,"true":1,"l":0,"loss":0,"0":0,"false":0}
        ).fillna(0).astype(int)
        pnl = res * (settle["odds"] - 1.0) * settle["stake_eur"] - (1 - res) * settle["stake_eur"]
        settle["pnl"] = pnl
        # clv already attached

    # Update bankroll.json and append bankroll_history.csv
    bk_path = os.path.join(state, "bankroll.json")
    hist_path = os.path.join(state, "bankroll_history.csv")

    bankroll = float(os.environ.get("START_BANKROLL", "1000"))
    if os.path.isfile(bk_path):
        try:
            bankroll = float(json.load(open(bk_path)).get("bankroll", bankroll))
        except Exception:
            pass

    if not settle.empty:
        total_pnl = float(settle["pnl"].sum())
        bankroll += total_pnl

    # Persist
    json.dump({"bankroll": round(bankroll, 2)}, open(bk_path, "w"))
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    if os.path.isfile(hist_path):
        hist = pd.read_csv(hist_path)
    else:
        hist = pd.DataFrame(columns=["ts","bankroll"])
    hist = pd.concat([hist, pd.DataFrame([{"ts": now, "bankroll": round(bankroll, 2)}])], ignore_index=True)
    save_df(hist, hist_path)

    # Write settled report (optional file OR default inside state)
    out_path = args.out or os.path.join(state, "settled_trades.csv")
    if not settle.empty:
        # stamp settle time
        settle["settled_ts"] = now
        # append or create
        prev = load_df(out_path)
        all_rows = pd.concat([prev, settle], ignore_index=True) if not prev.empty else settle
        save_df(all_rows, out_path)

    print(f"Settled rows: {0 if settle.empty else len(settle)} | bankroll: €{bankroll:.2f}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
