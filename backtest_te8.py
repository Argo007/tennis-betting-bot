#!/usr/bin/env python3
"""
Tennis backtester with:
- odds-band filters (supports multiple bands via '|' separator)
- favorite/underdog selection
- flat or Kelly staking (with 'true edge' uplift)
- per-config metrics + overall best selection

Input CSV needs: date,tournament,round,player1,player2,odds1,odds2,result
Optional: surface

Outputs:
- results.csv                 (all bets with 'config' column)
- matrix_rankings.csv         (one row per config)
- backtest_metrics.json       (metrics for the *best* config)
"""

from __future__ import annotations
import argparse, json, os
from typing import List, Tuple, Optional
import pandas as pd
import numpy as np


# ---------- helpers ----------
def parse_date(s: str) -> pd.Timestamp:
    return pd.to_datetime(s, errors="raise")


def parse_bands_matrix(s: str | None, default_pair: Tuple[float,float]) -> List[Tuple[float,float]]:
    """
    Accepts forms like:
      "2.0,2.6|2.6,3.2|3.2,4.0"   or   "2.0-2.6|2.6-3.2"
    or a single "low,high". If blank, returns [default_pair].
    """
    if not s or not s.strip():
        return [default_pair]
    parts = [p.strip() for p in s.split("|") if p.strip()]
    out: List[Tuple[float,float]] = []
    for p in parts:
        if "-" in p:
            lo, hi = p.split("-", 1)
        else:
            lo, hi = p.split(",", 1)
        lo, hi = float(lo.strip()), float(hi.strip())
        if lo > hi:
            lo, hi = hi, lo
        out.append((lo, hi))
    return out


def max_drawdown(cum_pnl: pd.Series) -> float:
    if cum_pnl.empty:
        return 0.0
    dd = cum_pnl.cummax() - cum_pnl
    return float(dd.max())


def pick_side(odds1: float, odds2: float, strategy: str) -> Tuple[str, float]:
    """
    strategy: 'dog' → choose higher odds, 'fav' → choose lower odds
    returns (side, price)
    """
    if strategy == "fav":
        if odds1 <= odds2:
            return "P1", float(odds1)
        return "P2", float(odds2)
    # underdog
    if odds1 >= odds2:
        return "P1", float(odds1)
    return "P2", float(odds2)


def kelly_fraction(price: float, edge: float) -> float:
    """
    price: decimal odds
    edge: uplift applied to implied probability, e.g. 0.08 for 'true edge 8%'
    We set p = min(max( (1/price)*(1+edge), 0 ), 1)
    b = price - 1
    f* = (b*p - (1-p)) / b
    """
    if price <= 1.0:
        return 0.0
    p_implied = 1.0 / price
    p = min(max(p_implied * (1.0 + edge), 0.0), 1.0)
    b = price - 1.0
    f = (b * p - (1.0 - p)) / b
    return max(0.0, float(f))


# ---------- core ----------
def run_config(df: pd.DataFrame, band: Tuple[float,float], grid: Optional[List[str]],
               strategy: str, stake_mode: str, stake_unit: float,
               bankroll0: float, edge: float, kelly_scale: float,
               config_name: str) -> Tuple[pd.DataFrame, dict]:
    lo, hi = band
    work = df.copy()

    # surface filter
    if "surface" in work.columns and grid:
        grid_up = [g.strip().upper() for g in grid if g.strip()]
        work = work[work["surface"].astype(str).str.upper().isin(grid_up)]

    bets = []
    bankroll = float(bankroll0)

    for _, r in work.iterrows():
        o1 = float(r["odds1"])
        o2 = float(r["odds2"])
        side, price = pick_side(o1, o2, strategy=strategy)

        # band filter
        if not (lo <= price <= hi):
            continue

        # stake sizing
        if stake_mode == "kelly":
            f = kelly_fraction(price, edge=edge) * kelly_scale
            stake = max(0.0, bankroll * f)
        else:
            stake = float(stake_unit)

        if stake <= 0:
            continue

        won = int((side == "P1" and str(r["result"]).upper() == "P1") or
                  (side == "P2" and str(r["result"]).upper() == "P2"))
        ret = stake * price if won else 0.0
        pnl = ret - stake
        bankroll += pnl

        bets.append({
            "config": config_name,
            "date": r["date"].date().isoformat(),
            "tournament": r.get("tournament", ""),
            "round": r.get("round",""),
            "player1": r.get("player1",""),
            "player2": r.get("player2",""),
            "odds1": o1,
            "odds2": o2,
            "bet_on": side,
            "selection_odds": price,
            "stake": stake,
            "return": ret,
            "won": won,
            "bankroll": bankroll
        })

    out = pd.DataFrame(bets)
    if out.empty:
        return out, {
            "config": config_name,
            "n_bets": 0,
            "hit_rate": 0.0,
            "roi": 0.0,
            "max_drawdown": 0.0,
            "final_bankroll": bankroll0,
            "band_low": lo,
            "band_high": hi,
            "strategy": strategy,
            "stake_mode": stake_mode
        }

    pnl = (out["return"] - out["stake"]).astype(float)
    cum_pnl = pnl.cumsum()
    stake_sum = float(out["stake"].sum())

    metrics = {
        "config": config_name,
        "n_bets": int(len(out)),
        "hit_rate": float(out["won"].mean()),
        "roi": float(cum_pnl.iloc[-1] / stake_sum) if stake_sum else 0.0,
        "max_drawdown": max_drawdown(cum_pnl),
        "final_bankroll": float(out["bankroll"].iloc[-1]),
        "band_low": lo,
        "band_high": hi,
        "strategy": strategy,
        "stake_mode": stake_mode
    }
    return out, metrics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/historical_matches.csv")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--grid", default="", help="comma list of surfaces (optional)")
    ap.add_argument("--bands", default="", help="bands like '2.0,2.6|2.6,3.2' or single '2.2,3.0'")
    ap.add_argument("--strategy", default="dog", choices=["dog","fav"])
    ap.add_argument("--stake-mode", default="flat", choices=["flat","kelly"])
    ap.add_argument("--stake", type=float, default=1.0, help="flat units (flat mode)")
    ap.add_argument("--bankroll", type=float, default=100.0, help="starting bankroll (kelly uses this)")
    ap.add_argument("--edge", type=float, default=0.08, help="true edge uplift (e.g., 0.08 = +8%)")
    ap.add_argument("--kelly-scale", type=float, default=0.5, help="0..1; 0.5 = half-Kelly")
    ap.add_argument("--out-csv", default="results.csv")
    args = ap.parse_args()

    if not os.path.exists(args.input):
        raise SystemExit(f"Input not found: {args.input}")

    df = pd.read_csv(args.input)
    # normalize required columns/types
    need = ["date","player1","player2","odds1","odds2","result"]
    for c in need:
        if c not in df.columns:
            raise SystemExit(f"Missing required column: {c}")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["odds1"] = pd.to_numeric(df["odds1"], errors="coerce")
    df["odds2"] = pd.to_numeric(df["odds2"], errors="coerce")
    df = df.dropna(subset=["date","odds1","odds2","result"])

    start, end = parse_date(args.start), parse_date(args.end)
    df = df[(df["date"] >= start) & (df["date"] <= end)].reset_index(drop=True)

    grid = [g for g in args.grid.split(",") if g.strip()] if args.grid else None
    bands_list = parse_bands_matrix(args.bands, default_pair=(1.8, 3.2))

    all_rows = []
    all_metrics = []
    for idx, (lo, hi) in enumerate(bands_list, start=1):
        cfg = f"band_{lo:.2f}_{hi:.2f}_{args.strategy}_{args.stake_mode}"
        out, metrics = run_config(
            df=df, band=(lo,hi), grid=grid, strategy=args.strategy,
            stake_mode=args.stake_mode, stake_unit=args.stake,
            bankroll0=args.bankroll, edge=args.edge, kelly_scale=args.kelly_scale,
            config_name=cfg
        )
        if not out.empty:
            all_rows.append(out)
        all_metrics.append(metrics)

    # write results
    results = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame(
        columns=["config","date","tournament","round","player1","player2","odds1","odds2",
                 "bet_on","selection_odds","stake","return","won","bankroll"]
    )
    results.to_csv(args.out_csv, index=False)

    rankings = pd.DataFrame(all_metrics)
    rankings.sort_values(["roi","hit_rate"], ascending=[False, False], inplace=True)
    rankings.to_csv("matrix_rankings.csv", index=False)

    # best config metrics → backtest_metrics.json
    best = rankings.iloc[0].to_dict() if not rankings.empty else {
        "n_bets": 0, "hit_rate": 0.0, "roi": 0.0, "max_drawdown": 0.0
    }
    with open("backtest_metrics.json","w") as f:
        json.dump(best, f, indent=2)

    print("Best config:", best)


if __name__ == "__main__":
    main()
