#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Update persistent bankroll state from the latest equity_curve.csv.
Writes:
  state/bankroll.json
  state/bankroll_history.csv
"""
import argparse, os, json, time
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--equity", default="results/equity_curve.csv", help="Path to equity curve CSV")
ap.add_argument("--state-dir", default="state", help="Directory to store bankroll state files")
ap.add_argument("--initial", type=float, default=1000.0, help="Initial bankroll if no state exists")
args = ap.parse_args()

os.makedirs(args.state_dir, exist_ok=True)
state_path = os.path.join(args.state_dir, "bankroll.json")
hist_path  = os.path.join(args.state_dir, "bankroll_history.csv")

# Load current bankroll
if os.path.isfile(state_path):
    with open(state_path, "r") as f:
        state = json.load(f)
else:
    state = {"bankroll": args.initial, "created_at": int(time.time())}

# Read last bankroll from equity curve if available
last_bankroll = None
if os.path.isfile(args.equity):
    try:
        eq = pd.read_csv(args.equity)
        if not eq.empty and "bankroll" in eq.columns:
            last_bankroll = float(eq.iloc[-1]["bankroll"])
    except Exception as e:
        print(f"Warning: failed reading equity curve: {e}")

if last_bankroll is not None:
    state["bankroll"] = round(last_bankroll, 2)

# Persist state
with open(state_path, "w") as f:
    json.dump(state, f, indent=2)

# Append to history
row = {"ts": int(time.time()), "bankroll": state["bankroll"]}
if os.path.isfile(hist_path):
    hist = pd.read_csv(hist_path)
    hist = pd.concat([hist, pd.DataFrame([row])], ignore_index=True)
else:
    hist = pd.DataFrame([row])

hist.to_csv(hist_path, index=False)

print("Updated bankroll state:", state)
print(f"History length: {len(hist)} rows -> {hist_path}")
