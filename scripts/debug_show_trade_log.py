#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Print a small summary of state/trade_log.csv:
- first 10 rows
- total rows
- SYN rows (match_id startswith 'SYN')
Designed for CI (no interactivity).
"""
import os
import argparse
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--state-dir", default="state")
ap.add_argument("--label", default="INFO")
args = ap.parse_args()

path = os.path.join(args.state_dir, "trade_log.csv")

print(f"--- {args.label} ---")
print(f"path: {path}")

if not os.path.isfile(path):
    print("No trade_log.csv present.")
    raise SystemExit(0)

try:
    df = pd.read_csv(path)
except Exception as e:
    print(f"Could not read trade_log.csv: {e}")
    raise SystemExit(0)

if df.empty:
    print("trade_log.csv is empty.")
    raise SystemExit(0)

syn = 0
if "match_id" in df.columns:
    syn = df["match_id"].astype(str).str.startswith("SYN").sum()

print(df.head(10).to_string(index=False))
print(f"rows={len(df)} syn_rows={syn}")
