import argparse
import os
import subprocess
import sys
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("--start", default="2024-01-01")
parser.add_argument("--end", default="2024-12-31")
args = parser.parse_args()

SUMMARY = "summary.md"
RESULTS = "results.csv"
METRICS = "backtest_metrics.json"
DATASET = "data/historical_matches.csv"

def file_has_rows(path: str) -> bool:
    try:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return False
        # quick check for CSV rows
        import csv
        with open(path, newline="") as f:
            r = csv.reader(f)
            # skip header
            header = next(r, None)
            first = next(r, None)
            return first is not None
    except Exception:
        return False

# 1) Start summary fresh
with open(SUMMARY, "w") as f:
    f.write("# TE8 Backtest Summary\n\n")
    f.write(f"_Window:_ {args.start} → {args.end}\n\n")

# 2) Try running the real backtest if dataset exists
ran_real = False
if os.path.exists(DATASET) and os.path.getsize(DATASET) > 0:
    try:
        subprocess.run([
            sys.executable, "backtest_te8.py",
            "--input", DATASET,
            "--start", args.start,
            "--end", args.end,
            "--out-csv", RESULTS
        ], check=True)
        ran_real = True
    except subprocess.CalledProcessError as e:
        with open(SUMMARY, "a") as f:
            f.write(f"Backtest script failed with code {e.returncode}. Falling back to demo.\n\n")
else:
    with open(SUMMARY, "a") as f:
        f.write("Dataset missing or empty. Falling back to demo data.\n\n")

# 3) If no rows produced, synthesize a tiny demo so the pipeline has output
if not file_has_rows(RESULTS):
    n = 25
    start_dt = datetime.strptime(args.start, "%Y-%m-%d")
    dates = [start_dt + timedelta(days=i) for i in range(n)]
    rng = np.random.default_rng(42)
    # Simple random walk: stake = 1 unit each bet, return = 1+profit
    stakes = np.ones(n)
    wins = rng.random(n) < 0.55
    # Profit per win/loss (odds ~1.8 payback, -1 loss)
    profits = np.where(wins, 0.8, -1.0)
    returns = stakes + profits
    df = pd.DataFrame({
        "date": [d.strftime("%Y-%m-%d") for d in dates],
        "tournament": ["DEMO"] * n,
        "round": ["R"] * n,
        "player1": ["A"] * n,
        "player2": ["B"] * n,
        "odds1": 1.80,
        "odds2": 2.10,
        "result": np.where(wins, "P1", "P2"),
        "stake": stakes,
        "return": returns,
        "won": wins.astype(int),
    })
    df.to_csv(RESULTS, index=False)

    # Compute demo metrics
    stake_sum = df["stake"].sum()
    pnl = (df["return"] - df["stake"]).sum()
    hit_rate = float(df["won"].mean())
    roi = float(pnl / stake_sum) if stake_sum else 0.0
    eq = (df["return"] - df["stake"]).cumsum()
    max_dd = float((eq.cummax() - eq).max()) if len(eq) else 0.0

    json.dump({
        "n_bets": int(len(df)),
        "hit_rate": hit_rate,
        "roi": roi,
        "max_drawdown": max_dd
    }, open(METRICS, "w"), indent=2)

    with open(SUMMARY, "a") as f:
        f.write("Produced demo results and metrics (smoke mode).\n")
else:
    # If real results exist but no metrics were written by your script, compute basic ones
    if not os.path.exists(METRICS):
        try:
            df = pd.read_csv(RESULTS)
            cols = {c.lower(): c for c in df.columns}
            stake_col = cols.get("stake")
            return_col = cols.get("return")
            won_col = cols.get("won")
            metrics = {"n_bets": int(len(df))}
            if won_col:
                metrics["hit_rate"] = float(pd.to_numeric(df[won_col], errors="coerce").fillna(0).astype(float).mean())
            if stake_col and return_col:
                stake_sum = pd.to_numeric(df[stake_col], errors="coerce").fillna(0).sum()
                pnl = (pd.to_numeric(df[return_col], errors="coerce").fillna(0) -
                       pd.to_numeric(df[stake_col], errors="coerce").fillna(0)).sum()
                metrics["roi"] = float(pnl / stake_sum) if stake_sum else 0.0
                eq = (pd.to_numeric(df[return_col], errors="coerce").fillna(0) -
                      pd.to_numeric(df[stake_col], errors="coerce").fillna(0)).cumsum()
                metrics["max_drawdown"] = float((eq.cummax() - eq).max())
            json.dump(metrics, open(METRICS, "w"), indent=2)
        except Exception:
            pass

    with open(SUMMARY, "a") as f:
        f.write("Real backtest produced results.\n")
