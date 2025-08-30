#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Settle trades: attach close_odds, compute CLV and PnL, update bankroll state.
Idempotent: only rows with settled != 1 are processed.

Columns expected in state/trade_log.csv (extra columns are ok):
- ts, match_id, selection, odds, p, edge, stake_eur
Will add/update:
- close_odds, clv, result (0/1), pnl, settled (1), settled_ts

Close-odds file may have columns like:
- match_id, selection (or sel), close_odds (or odds)
We match by match_id + selection (when available) or by match_id alone.

If --assume-random-if-missing is provided and no real result exists,
we simulate a deterministic result using p and a hash-based RNG seeded
on match_id (so reruns produce identical results).
"""

from __future__ import annotations
import os, json, argparse, hashlib, time
from datetime import datetime, timezone
import pandas as pd
import numpy as np


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _read_df(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"WARNING: could not read {path}: {e}")
        return pd.DataFrame()


def _safe_col(df: pd.DataFrame, names: list[str], default=None):
    for n in names:
        if n in df.columns:
            return df[n]
    return default


def _to_float(x, default=np.nan):
    try:
        return float(x)
    except Exception:
        return default


def _stable_rng_for(match_id: str) -> np.random.Generator:
    # Deterministic RNG from match_id (stable across reruns)
    h = hashlib.sha256(str(match_id).encode("utf-8")).hexdigest()
    seed = int(h[:16], 16) % (2**32)
    return np.random.default_rng(seed)


def _clv(open_odds: float, close_odds: float) -> float:
    # Simple relative change: (close - open) / open
    # Positive means you beat the close if open < close for underdogs.
    if not np.isfinite(open_odds) or not np.isfinite(close_odds) or open_odds <= 0:
        return 0.0
    return (close_odds - open_odds) / open_odds


def _pnl(stake: float, odds: float, result: int) -> float:
    if not np.isfinite(stake) or not np.isfinite(odds):
        return 0.0
    return stake * (odds - 1.0) if result == 1 else -stake


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default="state/trade_log.csv", help="trade log CSV")
    ap.add_argument("--close-odds", default="live_results/close_odds.csv", help="close odds CSV")
    ap.add_argument("--state-dir", default="state", help="state directory")
    ap.add_argument("--assume-random-if-missing", action="store_true",
                    help="simulate result if no real result present (deterministic)")
    args = ap.parse_args()

    os.makedirs(args.state_dir, exist_ok=True)

    log_path = args.log
    close_path = args.close_odds

    trades = _read_df(log_path)
    if trades.empty:
        print("No trades to settle.")
        return 0

    # Normalize essential columns
    if "selection" not in trades.columns and "sel" in trades.columns:
        trades = trades.rename(columns={"sel": "selection"})
    for col in ["odds", "p", "stake_eur"]:
        if col in trades.columns:
            trades[col] = trades[col].apply(_to_float)

    # Ensure settlement columns exist
    for col, default in [
        ("close_odds", np.nan),
        ("clv", np.nan),
        ("result", np.nan),
        ("pnl", np.nan),
        ("settled", 0),
        ("settled_ts", "")
    ]:
        if col not in trades.columns:
            trades[col] = default

    # Load close odds
    close = _read_df(close_path)
    if not close.empty:
        if "selection" not in close.columns and "sel" in close.columns:
            close = close.rename(columns={"sel": "selection"})
        # normalise close odds column name
        if "close_odds" not in close.columns:
            if "odds" in close.columns:
                close = close.rename(columns={"odds": "close_odds"})
        # keep only the columns we need
        keep_cols = [c for c in ["match_id", "selection", "close_odds"] if c in close.columns]
        close = close[keep_cols].copy()

    # Work only on unsettled trades
    unsettled_mask = trades["settled"].fillna(0).astype(int) != 1
    unsettled = trades.loc[unsettled_mask].copy()
    if unsettled.empty:
        print("All trades already settled.")
        return 0

    # Attach close odds when possible
    if not close.empty:
        if "selection" in close.columns and "selection" in unsettled.columns:
            unsettled = unsettled.merge(
                close, on=["match_id", "selection"], how="left", suffixes=("", "_m")
            )
        else:
            unsettled = unsettled.merge(
                close.drop(columns=[c for c in ["selection"] if c in close.columns]),
                on=["match_id"], how="left", suffixes=("", "_m")
            )
        # prefer newly merged value if present
        if "close_odds_m" in unsettled.columns:
            unsettled["close_odds"] = np.where(
                unsettled["close_odds"].notna(), unsettled["close_odds"], unsettled["close_odds_m"]
            )
            unsettled = unsettled.drop(columns=["close_odds_m"])

    # Compute CLV for any row that has close_odds
    has_close = unsettled["close_odds"].apply(np.isfinite)
    unsettled.loc[has_close, "clv"] = [
        _clv(o, c) for o, c in zip(unsettled.loc[has_close, "odds"], unsettled.loc[has_close, "close_odds"])
    ]

    # Determine results:
    # If 'result' column exists with finite values, keep it.
    # Otherwise, if assume-random-if-missing -> simulate deterministically from p.
    res_missing = ~unsettled["result"].apply(np.isfinite)
    if args.assume_random_if_missing and res_missing.any():
        def sim_row(row):
            # p is probability of success (home pick wins)
            p = row.get("p", np.nan)
            p = p if np.isfinite(p) else 0.5
            rng = _stable_rng_for(row.get("match_id", "NA"))
            return int(rng.random() < p)

        unsettled.loc[res_missing, "result"] = unsettled.loc[res_missing].apply(sim_row, axis=1)

    # Compute PnL for rows that now have result
    can_settle = unsettled["result"].apply(np.isfinite)
    unsettled.loc[can_settle, "pnl"] = [
        _pnl(stk, od, int(r))
        for stk, od, r in zip(unsettled.loc[can_settle, "stake_eur"],
                              unsettled.loc[can_settle, "odds"],
                              unsettled.loc[can_settle, "result"])
    ]

    # Finalize settlement for rows with PnL computed
    settled_now = unsettled["pnl"].apply(np.isfinite)
    unsettled.loc[settled_now, "settled"] = 1
    unsettled.loc[settled_now, "settled_ts"] = _now_ts()

    # Write back: only update rows that we just settled (idempotent)
    trades.loc[unsettled.index, :] = unsettled
    # Make a backup before overwrite
    bkp = log_path + f".bak.{int(time.time())}"
    try:
        if os.path.isfile(log_path):
            os.replace(log_path, bkp)
            print(f"Backup created: {bkp}")
    except Exception as e:
        print(f"WARNING: backup failed: {e}")

    trades.to_csv(log_path, index=False)
    print(f"Updated log: {log_path}")

    # Aggregate metrics for the newly settled rows
    n_settled = int(settled_now.sum())
    pnl_sum = float(unsettled.loc[settled_now, "pnl"].sum()) if n_settled else 0.0
    clv_avg = float(unsettled.loc[settled_now, "clv"].mean()) if n_settled else 0.0
    print(f"Settled {n_settled} trades | PnL={pnl_sum:.2f} | Avg CLV={clv_avg:.4f}")

    # Update bankroll state
    bk_path = os.path.join(args.state_dir, "bankroll.json")
    hist_path = os.path.join(args.state_dir, "bankroll_history.csv")

    bankroll = 0.0
    if os.path.isfile(bk_path):
        try:
            bankroll = float(json.load(open(bk_path, "r")).get("bankroll", 0.0))
        except Exception:
            bankroll = 0.0

    bankroll += pnl_sum
    try:
        json.dump({"bankroll": round(bankroll, 2), "updated": _now_ts()}, open(bk_path, "w"))
    except Exception as e:
        print(f"WARNING: could not write {bk_path}: {e}")

    # Append to history (safe append)
    hist_cols = ["ts", "bankroll", "pnl", "avg_clv", "n_settled"]
    hist_row = pd.DataFrame([{
        "ts": _now_ts(),
        "bankroll": round(bankroll, 2),
        "pnl": round(pnl_sum, 2),
        "avg_clv": round(clv_avg, 6),
        "n_settled": n_settled,
    }], columns=hist_cols)

    if os.path.isfile(hist_path) and os.path.getsize(hist_path) > 0:
        try:
            old = pd.read_csv(hist_path)
        except Exception:
            old = pd.DataFrame(columns=hist_cols)
        new_hist = pd.concat([old, hist_row], ignore_index=True)
    else:
        new_hist = hist_row

    try:
        new_hist.to_csv(hist_path, index=False)
    except Exception as e:
        print(f"WARNING: could not write {hist_path}: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
