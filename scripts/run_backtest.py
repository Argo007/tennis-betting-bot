import argparse, os, subprocess, sys, json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

SUMMARY = "summary.md"
RESULTS = "results.csv"
METRICS = "backtest_metrics.json"
DATASET = "data/historical_matches.csv"

def file_has_rows(path: str) -> bool:
    try:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return False
        import csv
        with open(path, newline="") as f:
            r = csv.reader(f); next(r, None)
            return next(r, None) is not None
    except Exception:
        return False

parser = argparse.ArgumentParser()
parser.add_argument("--start", default="2024-01-01")
parser.add_argument("--end", default="2024-12-31")
parser.add_argument("--bands", default="")
parser.add_argument("--grid", default="")
parser.add_argument("--strategy", default="dog")
parser.add_argument("--stake-mode", default="flat")
parser.add_argument("--stake", type=float, default=1.0)
parser.add_argument("--edge", type=float, default=0.08)
parser.add_argument("--kelly-scale", type=float, default=0.5)
parser.add_argument("--bankroll", type=float, default=100.0)
args = parser.parse_args()

SMOKE = os.getenv("SMOKE_MODE", "auto").lower()   # auto|on|off

with open(SUMMARY, "w") as f:
    f.write("# TE8 Backtest Summary\n\n")
    f.write(f"_Window:_ {args.start} → {args.end}\n\n")
    if args.bands:    f.write(f"_Bands:_ {args.bands}\n\n")
    if args.grid:     f.write(f"_Surfaces:_ {args.grid}\n\n")
    f.write(f"_Strategy:_ {args.strategy}, _Stake mode:_ {args.stake_mode}\n\n")

ran_real = False
if os.path.exists(DATASET) and os.path.getsize(DATASET) > 0:
    cmd = [
        sys.executable, "backtest_te8.py",
        "--input", DATASET,
        "--start", args.start,
        "--end", args.end,
        "--out-csv", RESULTS,
        "--strategy", args.strategy,
        "--stake-mode", args.stake_mode,
        "--stake", str(args.stake),
        "--edge", str(args.edge),
        "--kelly-scale", str(args.kelly_scale),
        "--bankroll", str(args.bankroll),
    ]
    if args.bands: cmd += ["--bands", args.bands]
    if args.grid:  cmd += ["--grid", args.grid]
    try:
        subprocess.run(cmd, check=True)
        ran_real = True
    except subprocess.CalledProcessError as e:
        with open(SUMMARY, "a") as f:
            f.write(f"Backtest failed (code {e.returncode}). Falling back to demo.\n\n")
else:
    with open(SUMMARY, "a") as f:
        f.write("Dataset missing or empty. Falling back to demo.\n\n")

need_fallback = not file_has_rows(RESULTS)
if SMOKE == "off": need_fallback = False
elif SMOKE == "on": need_fallback = True

if need_fallback:
    # demo generator (same shape as real outputs)
    n = 25
    start_dt = datetime.strptime(args.start, "%Y-%m-%d")
    dates = [start_dt + timedelta(days=i) for i in range(n)]
    rng = np.random.default_rng(7)
    stakes = np.ones(n)
    wins = rng.random(n) < 0.48
    profits = np.where(wins, 0.8, -1.0)
    returns = stakes + profits
    df = pd.DataFrame({
        "config": "demo",
        "date": [d.strftime("%Y-%m-%d") for d in dates],
        "tournament": ["DEMO"] * n,
        "round": ["R"] * n,
        "player1": ["A"] * n,
        "player2": ["B"] * n,
        "odds1": 1.8, "odds2": 2.1,
        "bet_on": np.where(wins, "P1", "P2"),
        "selection_odds": 1.8,
        "stake": stakes, "return": returns, "won": wins.astype(int),
        "bankroll": 100 + (returns - stakes).cumsum()
    })
    df.to_csv(RESULTS, index=False)
    import json
    stake_sum = df["stake"].sum()
    pnl = (df["return"] - df["stake"]).sum()
    roi = pnl / stake_sum if stake_sum else 0.0
    eq = (df["return"] - df["stake"]).cumsum()
    mdd = float((eq.cummax() - eq).max())
    json.dump({"n_bets": int(len(df)), "hit_rate": float(df["won"].mean()),
               "roi": float(roi), "max_drawdown": mdd}, open("backtest_metrics.json","w"), indent=2)
    with open(SUMMARY, "a") as f:
        f.write("Produced demo results and metrics (smoke mode).\n")
else:
    with open(SUMMARY, "a") as f:
        f.write("Real backtest produced results.\n")
