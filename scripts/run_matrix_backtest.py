#!/usr/bin/env python3
# run_matrix_backtest.py
from __future__ import annotations
import argparse, os, csv, json
from typing import List, Dict
from bet_math import KellyConfig
from backtest_core import read_rows, filter_by_band, simulate, parse_bands, write_results

def write_empty_outputs(outdir: str) -> None:
    os.makedirs(outdir, exist_ok=True)
    # Minimal empty results.csv
    res_path = os.path.join(outdir, "results.csv")
    with open(res_path, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=[
            "config_id","row_idx","price","p_model","p_used","kelly_f_raw",
            "stake","result","pnl","bankroll_before","bankroll_after",
            "cfg_stake_mode","cfg_edge","cfg_kelly_scale","cfg_flat_stake","cfg_bankroll_init"
        ])
        wr.writeheader()
    # Minimal empty rankings
    rank_path = os.path.join(outdir, "matrix_rankings.csv")
    with open(rank_path, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=[
            "config_id","label","bets","wins","hit_rate","avg_odds",
            "turnover","pnl","roi","end_bankroll","max_drawdown"
        ])
        wr.writeheader()
    # Metrics json with note
    with open(os.path.join(outdir, "backtest_metrics.json"), "w", encoding="utf-8") as f:
        json.dump({"best_by_roi": None, "n_configs": 0, "note": "no_valid_rows"}, f, indent=2)

def main():
    ap = argparse.ArgumentParser(description="Matrix backtester (Kelly + true-edge).")
    ap.add_argument("--input","-i", default="data/raw/odds/sample_odds.csv")
    ap.add_argument("--outdir","-o", default="outputs")
    ap.add_argument("--bands", required=True, help='e.g. "2.0,2.6|2.6,3.2|3.2,4.0"')
    ap.add_argument("--stake-mode", choices=["kelly","flat"], default="kelly")
    ap.add_argument("--edge", type=float, default=0.08)
    ap.add_argument("--kelly-scale", type=float, default=0.5)
    ap.add_argument("--flat-stake", type=float, default=1.0)
    ap.add_argument("--bankroll", type=float, default=100.0)
    args = ap.parse_args()

    # Kelly config
    cfg = KellyConfig(
        stake_mode=args.stake_mode,
        edge=args.edge,
        kelly_scale=args.kelly_scale,
        flat_stake=args.flat_stake,
        bankroll_init=args.bankroll,
    )

    # Load and validate rows
    rows = read_rows(args.input)
    if not rows:
        print("[matrix-backtest] No valid rows to backtest (need odds + probability + result). "
              "Writing empty outputs and continuing.")
        write_empty_outputs(args.outdir)
        return  # EXIT 0, not an error

    # Parse bands
    bands = parse_bands(args.bands)
    all_bets: List[Dict] = []
    rank_rows: List[Dict] = []

    for bi,(lo,hi) in enumerate(bands):
        br = filter_by_band(rows, lo, hi)
        label = f"{lo:.2f}-{hi:.2f}"
        config_id = f"band{bi+1}_{label}_mode{cfg.stake_mode}_TE{int(round(cfg.edge*100))}_K{cfg.kelly_scale}"
        stats = simulate(br, cfg, config_id, all_bets)
        rank_rows.append({"config_id": config_id, "label": label, **stats})

    # sort by ROI desc
    rank_rows.sort(key=lambda r: r["roi"], reverse=True)

    paths = write_results(all_bets, rank_rows, args.outdir)
    print("Wrote:")
    for k,v in paths.items():
        print(f"  {k}: {v}")

if __name__ == "__main__":
    main()
