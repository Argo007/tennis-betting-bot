#!/usr/bin/env python3
"""
Simple odds-band backtester for tennis matches.

Input CSV expected columns (min):
  date, tournament, round, player1, player2, odds1, odds2, result
Optional:
  surface  (e.g., HARD/CLAY/GRASS/INDOOR)

Conventions:
- odds are decimal (European) odds
- result is "P1" if player1 won, "P2" if player2 won
- We bet flat 1 unit per selection (configurable)

Outputs:
- results.csv: per-bet rows with stake/return/won etc
- backtest_metrics.json: aggregate metrics (n_bets, hit_rate, roi, max_drawdown)
"""

from __future__ import annotations
import argparse, json, os
from datetime import datetime
from typing import List, Optional

import pandas as pd
import numpy as np


def parse_bands(bands_str: str | None, default_low: float, default_high: float) -> tuple[float, float]:
    """
    Accepts "low,high" or a longer comma list and uses the min/max.
    If blank/None, returns defaults.
    """
    if not bands_str:
        return default_low, default_high
    parts = [p.strip() for p in bands_str.split(",") if p.strip()]
    try:
        nums = [float(p) for p in parts]
        if len(nums) == 1:
            return min(nums[0], default_low), max(nums[0], default_high)
        return float(min(nums)), float(max(nums))
    except Exception:
        return default_low, default_high


def max_drawdown(series: pd.Series) -> float:
    """Max drawdown in units for a cumulative P&L series."""
    if series.empty:
        return 0.0
    roll_max = series.cummax()
    dd = roll_max - series
    return float(dd.max())


def decide_bet(row: pd.Series, strategy: str, low: float, high: float) -> tuple[str, float]:
    """
    Decide which side to bet ("P1"/"P2"/"NONE") and at what odds.

    strategy = "dog" -> bet the higher-odds side if within [low, high]
             = "fav" -> bet the lower-odds side if within [low, high]
    """
    o1 = float(row["odds1"]) if pd.notna(row["odds1"]) else np.nan
    o2 = float(row["odds2"]) if pd.notna(row["odds2"]) else np.nan
    if not np.isfinite(o1) or not np.isfinite(o2):
        return "NONE", np.nan

    if strategy == "fav":
        # favorite = lower odds
        if o1 <= o2:
            side, price = "P1", o1
        else:
            side, price = "P2", o2
    else:
        # underdog = higher odds
        if o1 >= o2:
            side, price = "P1", o1
        else:
            side, price = "P2", o2

    if low <= price <= high:
        return side, float(price)
    return "NONE", np.nan


def run_backtest(
    df: pd.DataFrame,
    start: str,
    end: str,
    grid: Optional[List[str]],
    bands: str | None,
    strategy: str,
    stake: float,
) -> tuple[pd.DataFrame, dict]:
    # normalize columns
    need_cols = ["date","tournament","round","player1","player2","odds1","odds2","result"]
    for c in need_cols:
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")

    # coerce types
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date","player1","player2","odds1","odds2","result"])
    df["odds1"] = pd.to_numeric(df["odds1"], errors="coerce")
    df["odds2"] = pd.to_numeric(df["odds2"], errors="coerce")
    df = df.dropna(subset=["odds1","odds2"])

    # filter dates
    start_dt = pd.to_datetime(start)
    end_dt   = pd.to_datetime(end)
    df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]

    # filter surfaces (if column present and grid provided)
    if "surface" in df.columns and grid:
        grid_upper = [g.strip().upper() for g in grid if g.strip()]
        df = df[df["surface"].astype(str).str.upper().isin(grid_upper)]

    # bands
    low, high = parse_bands(bands, default_low=1.8, default_high=3.2)

    # decide bets
    sel_side, sel_odds = [], []
    for _, r in df.iterrows():
        side, price = decide_bet(r, strategy=strategy, low=low, high=high)
        sel_side.append(side)
        sel_odds.append(price)

    out = df.copy()
    out["bet_on"] = sel_side
    out["selection_odds"] = sel_odds
    out = out[out["bet_on"] != "NONE"].reset_index(drop=True)

    if out.empty:
        metrics = {"n_bets": 0, "hit_rate": 0.0, "roi": 0.0, "max_drawdown": 0.0}
        return out, metrics

    # compute stakes/returns
    out["stake"] = float(stake)
    # won?
    out["won"] = ((out["bet_on"] == "P1") & (out["result"].astype(str).str.upper() == "P1")) | \
                 ((out["bet_on"] == "P2") & (out["result"].astype(str).str.upper() == "P2"))
    out["won"] = out["won"].astype(int)

    out["return"] = np.where(out["won"] == 1, out["stake"] * out["selection_odds"], 0.0)
    pnl = (out["return"] - out["stake"]).astype(float)
    cum_pnl = pnl.cumsum()

    # metrics
    n_bets = int(len(out))
    hit_rate = float(out["won"].mean())
    stake_sum = float(out["stake"].sum())
    roi = float(cum_pnl.iloc[-1] / stake_sum) if stake_sum else 0.0
    mdd = max_drawdown(cum_pnl)

    metrics = {
        "n_bets": n_bets,
        "hit_rate": hit_rate,
        "roi": roi,
        "max_drawdown": mdd,
        "band_low": low,
        "band_high": high,
        "strategy": strategy,
        "stake_unit": stake,
    }
    return out, metrics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/historical_matches.csv")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--grid", default="", help="comma list of surfaces, e.g. HARD,CLAY")
    ap.add_argument("--bands", default="", help="odds band 'low,high' (we’ll use min/max if more provided)")
    ap.add_argument("--strategy", default="dog", choices=["dog","fav"], help="bet underdog (dog) or favorite (fav)")
    ap.add_argument("--stake", type=float, default=1.0, help="flat stake size per bet")
    ap.add_argument("--out-csv", default="results.csv")
    args = ap.parse_args()

    if not os.path.exists(args.input):
        raise SystemExit(f"Input not found: {args.input}")

    df = pd.read_csv(args.input)
    grid = [g for g in args.grid.split(",")] if args.grid else None

    results, metrics = run_backtest(
        df=df,
        start=args.start,
        end=args.end,
        grid=grid,
        bands=args.bands,
        strategy=args.strategy,
        stake=args.stake,
    )

    # write outputs
    results.to_csv(args.out_csv, index=False)
    with open("backtest_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    # small console summary
    print(f"Bets: {metrics['n_bets']}, hit-rate: {metrics['hit_rate']:.2%}, ROI: {metrics['roi']:.2%}, MDD: {metrics['max_drawdown']:.2f}")


if __name__ == "__main__":
    main()
