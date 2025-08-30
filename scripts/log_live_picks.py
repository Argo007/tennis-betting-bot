#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Append today's live picks to state/trade_log.csv with calculated stakes.

Accepted flags (both hyphen and underscore styles work):
  --picks <CSV>                 input picks CSV (match_id, sel, odds, p[, edge])
  --state-dir <DIR>             state folder (default: state)
  --kelly                       enable Kelly sizing (default: True)
  --kelly-scale <float>         scale Kelly fraction (e.g., 0.5)
  --max-frac <float>            max fraction of bankroll per bet (e.g., 0.05)
  --abs-cap <float>             absolute € cap per bet (e.g., 200)
  --assume-random-if-missing    if picks missing/empty, exit 0 quietly

Workflow expectation:
- Reads state/bankroll.json {"bankroll": <float>} if present; else assumes 1000.
- Appends rows to state/trade_log.csv with columns:
  ts,match_id,selection,odds,p,edge,stake_eur,bankroll_snapshot
"""

import argparse
import json
import os
from datetime import datetime, timezone
import pandas as pd
import sys
from typing import Optional

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--picks", required=True, help="Input picks CSV")
    p.add_argument("--state-dir", "--state_dir", dest="state_dir", default="state")
    # sizing controls
    p.add_argument("--kelly", action="store_true", default=True, help="Use Kelly sizing")
    p.add_argument("--no-kelly", dest="kelly", action="store_false", help="Disable Kelly sizing")
    p.add_argument("--kelly-scale", "--kelly_scale", dest="kelly_scale", type=float, default=0.5)
    p.add_argument("--max-frac", "--max_frac", dest="max_frac", type=float, default=0.05)
    p.add_argument("--abs-cap", "--abs_cap", dest="abs_cap", type=float, default=200.0)
    p.add_argument("--assume-random-if-missing", "--assume_random_if_missing",
                   dest="assume_random_if_missing", type=lambda s: str(s).lower() in ("1","true","yes"),
                   default=False)
    return p.parse_args()

def read_bankroll(state_dir: str) -> float:
    bk_path = os.path.join(state_dir, "bankroll.json")
    if os.path.isfile(bk_path):
        try:
            with open(bk_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            b = float(data.get("bankroll", 1000.0))
            return max(b, 0.0)
        except Exception:
            return 1000.0
    return 1000.0

def coerce_prob(x) -> Optional[float]:
    if pd.isna(x):
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if v > 1.0:
        v = v / 100.0
    if v < 0 or v > 1:
        return None
    return v

def kelly_fraction(p: float, odds: float) -> float:
    # Decimal odds Kelly: f* = (p*(o-1) - (1-p)) / (o-1)
    b = max(odds - 1.0, 0.0)
    if b <= 0:
        return 0.0
    return (p * b - (1 - p)) / b

def main():
    args = parse_args()
    os.makedirs(args.state_dir, exist_ok=True)

    # Load picks
    if not os.path.isfile(args.picks):
        if args.assume_random_if_missing:
            print("No picks file; assuming none. Nothing to log.")
            return 0
        print(f"ERROR: picks file not found: {args.picks}", file=sys.stderr)
        return 2

    df = pd.read_csv(args.picks)
    if df.empty:
        if args.assume_random_if_missing:
            print("Empty picks; nothing to log.")
            return 0
        print("Empty picks; nothing to log.")
        return 0

    # Map columns flexibly
    cols = {c.lower(): c for c in df.columns}
    def get_col(*cands):
        for c in cands:
            if c in cols: return cols[c]
        return None

    col_match = get_col("match_id","match","id")
    col_sel   = get_col("sel","selection","pick")
    col_odds  = get_col("odds","price")
    col_p     = get_col("p","prob","probability")
    col_edge  = get_col("edge")

    required_missing = [name for name,val in
                        [("match_id",col_match),("sel",col_sel),("odds",col_odds)]
                        if val is None]
    if required_missing:
        print(f"ERROR: picks is missing required columns: {required_missing}", file=sys.stderr)
        return 2

    out = pd.DataFrame({
        "match_id": df[col_match].astype(str),
        "selection": df[col_sel].astype(str),
        "odds": pd.to_numeric(df[col_odds], errors="coerce"),
    })

    if col_p:
        out["p"] = df[col_p].apply(coerce_prob)
    else:
        out["p"] = None

    if col_edge:
        out["edge"] = pd.to_numeric(df[col_edge], errors="coerce")
    else:
        # compute if possible
        out["edge"] = out.apply(
            lambda r: (r["p"] * r["odds"] - 1.0) if pd.notna(r["p"]) and pd.notna(r["odds"]) else None,
            axis=1
        )

    out = out.dropna(subset=["odds"]).copy()
    if out.empty:
        print("No valid rows to log.")
        return 0

    # bankroll & stake sizing
    bankroll = read_bankroll(args.state_dir)
    max_frac = max(float(args.max_frac), 0.0)
    abs_cap  = max(float(args.abs_cap), 0.0)
    kscale   = max(float(args.kelly_scale), 0.0)

    def sized_stake(row):
        # default flat €0 if no p/edge
        if not args.kelly or pd.isna(row.get("p")) or pd.isna(row.get("odds")):
            return 0.0
        f = kelly_fraction(row["p"], row["odds"])
        f = max(0.0, f) * kscale
        # clamp
        f = min(f, max_frac) if max_frac > 0 else f
        stake = bankroll * f
        if abs_cap > 0:
            stake = min(stake, abs_cap)
        # nice rounding for reporting
        return round(stake, 2)

    out["stake_eur"] = out.apply(sized_stake, axis=1)
    out["bankroll_snapshot"] = bankroll
    out["ts"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

    # Order for log
    out = out[["ts","match_id","selection","odds","p","edge","stake_eur","bankroll_snapshot"]]

    # Append to state/trade_log.csv
    log_path = os.path.join(args.state_dir, "trade_log.csv")
    header = not os.path.isfile(log_path)
    out.to_csv(log_path, mode="a", header=header, index=False)
    print(f"Appended {len(out)} rows to {log_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
