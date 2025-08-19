#!/usr/bin/env python3
# run_matrix_backtest.py
from __future__ import annotations
import argparse
from typing import List, Dict, Tuple
from bet_math import KellyConfig
from backtest_core import read_rows, filter_by_band, simulate, parse_bands, write_results

def main():
    ap = argparse.ArgumentParser(description="Matrix backtester with Kelly staking and TE edge.")
    ap.add_argument("--input", "-i", default="data/market_history.csv", help="Input CSV with historical bets.")
    ap.add_argument("--outdir", "-o", default="outputs", help="Output directory.")
    ap.add_argument("--bands", required=True, help='Odds bands, e.g. "2.0,2.6|2.6,3.2|3.2,4.0"')
    ap.add_argument("--stake-mode", choices=["kelly","flat"], default="kelly")
    ap.add_argument("--edge", type=float, default=0.08, help="True edge booster, default 0.08 (TE8)")
    ap.add_argument("--kelly-scale", type=float, default=0.5, help="Safety scaler; 0.5 = half-Kelly.")
    ap.add_argument("--flat-stake", type=float, default=1.0, help="Units per bet when stake-mode=flat.")
    ap.add_argument("--bankroll", type=float, default=100.0, help="Starting bankroll in units.")
    args = ap.parse_args()

    cfg = KellyConfig(
        stake_mode=args.stake_mode,
        edge=args.edge,
        kelly_scale=args.kelly_scale,
        flat_stake=args.flat_stake,
        bankroll_init=args.bankroll
    )

    rows = read_rows(args.input)
    if not rows:
        raise SystemExit("No valid rows to backtest. Check your CSV columns: odds/price + probability + result.")

    bands = parse_bands(args.bands)

    all_bets: List[Dict] = []
    rank_rows: List[Dict] = []

    for bi, (lo, hi) in enumerate(bands):
        band_rows = filter_by_band(rows, lo, hi)
        label = f"{lo:.2f}-{hi:.2f}"
        config_id = f"band{bi+1}_{label}_mode{cfg.stake_mode}_TE{int(round(cfg.edge*100))}_K{cfg.kelly_scale}"
        stats = simulate(band_rows, cfg, config_id, all_bets)
        rank_rows.append({
            "config_id": config_id,
            "label": label,
            **stats
        })

    # sort by ROI descending
    rank_rows.sort(key=lambda r: r["roi"], reverse=True)

    paths = write_results(all_bets, rank_rows, args.outdir)
    print("Wrote:")
    for k, v in paths.items():
        print(f"  {k}: {v}")

if __name__ == "__main__":
    main()
