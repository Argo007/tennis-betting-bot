#!/usr/bin/env python3
# scripts/run_matrix_backtest.py
import argparse, json
import pandas as pd
from pathlib import Path
import numpy as np

def parse_bands(s):
    # "1.0,2.0|2.0,3.0" -> list of (lo,hi)
    if not s:
        return []
    bands = []
    parts = s.split("|")
    for p in parts:
        a,b = p.split(",")
        bands.append((float(a), float(b)))
    return bands

def in_any_band(odds, bands):
    if not bands:
        return True
    for lo,hi in bands:
        if lo <= odds <= hi:
            return True
    return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--bands", default="")
    ap.add_argument("--staking", default="kelly")
    ap.add_argument("--kelly-scale", type=float, default=0.5)
    ap.add_argument("--bankroll", type=float, default=1000.0)
    ap.add_argument("--min-edge", type=float, default=0.02)
    ap.add_argument("--outdir", default="results/backtests")
    args = ap.parse_args()

    df = pd.read_csv(args.dataset)
    bands = parse_bands(args.bands)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    picks = []
    bankroll = args.bankroll
    start_bankroll = bankroll

    # deterministic "model": favorites (lower decimal odds) get slight positive bias
    for _, row in df.iterrows():
        oa = float(row["oa"])
        ob = float(row["ob"])
        implied_a = float(row.get("pa", 1.0/oa / (1.0/oa + 1.0/ob)))
        implied_b = float(row.get("pb", 1.0/ob / (1.0/oa + 1.0/ob)))
        # model probability: if A is favorite (oa < ob), add +0.05 else -0.05 (deterministic)
        if oa < ob:
            model_pa = min(0.99, implied_a + 0.05)
        else:
            model_pa = max(0.01, implied_a - 0.05)
        edge = model_pa - implied_a

        # choose candidate bet on A if edge >= min_edge and in bands
        if edge >= args.min_edge and in_any_band(oa, bands):
            # stake
            if args.staking == "kelly":
                # simple fractional rule: stake_fraction = kelly_scale * edge
                stake_fraction = max(0.0, args.kelly_scale * edge)
                stake = bankroll * stake_fraction
            else:
                stake = 1.0
            stake = float(stake)
            # outcome: assume actual result equals implied probability (i.e. random expectation), but to have deterministic PNL,
            # we'll treat "win if model_pa > 0.5" for deterministic behavior
            win = 1 if model_pa > 0.5 else 0
            pnl = stake * ( (oa - 1.0) if win == 1 else -1.0 )
            bankroll += pnl
            picks.append({
                "player_a": row.get("player_a", ""),
                "player_b": row.get("player_b", ""),
                "odds_a": oa,
                "odds_b": ob,
                "implied_a": implied_a,
                "model_pa": model_pa,
                "edge": edge,
                "stake": stake,
                "win": win,
                "pnl": pnl,
                "bankroll_after": bankroll
            })

    picks_df = pd.DataFrame(picks)
    picks_out = outdir / "logs" / "picks_cfg1.csv"
    picks_out.parent.mkdir(parents=True, exist_ok=True)
    picks_df.to_csv(picks_out, index=False)

    # summary
    summary = {
        "cfg_id": 1,
        "n_bets": int(len(picks_df)),
        "total_staked": float(picks_df["stake"].sum()) if not picks_df.empty else 0.0,
        "pnl": float(picks_df["pnl"].sum()) if not picks_df.empty else 0.0,
        "roi": (float(picks_df["pnl"].sum()) / float(picks_df["stake"].sum())) if (not picks_df.empty and picks_df["stake"].sum()>0) else 0.0,
        "hitrate": (float(picks_df["win"].sum()) / len(picks_df)) if not picks_df.empty else 0.0,
        "end_bankroll": bankroll
    }
    summary_df = pd.DataFrame([summary])
    summary_df.to_csv(outdir / "summary.csv", index=False)

    # params
    params = {
        "bands": args.bands,
        "staking": args.staking,
        "kelly_scale": args.kelly_scale,
        "bankroll": args.bankroll,
        "min_edge": args.min_edge,
        "source": str(args.dataset)
    }
    with open(outdir / "params_cfg1.json", "w") as f:
        json.dump(params, f, indent=2)

    print("Wrote picks:", picks_out)
    print("Wrote summary:", outdir / "summary.csv")
    print("Wrote params:", outdir / "params_cfg1.json")

if __name__ == "__main__":
    main()

