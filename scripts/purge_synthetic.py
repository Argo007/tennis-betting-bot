#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Remove synthetic rows (match_id startswith 'SYN') from state/trade_log.csv.
Idempotent. Prints how many rows were purged.
"""
import os
import argparse
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-dir", default="state")
    ap.add_argument("--log", default=None, help="optional override path to trade_log.csv")
    args = ap.parse_args()

    state_dir = args.state_dir
    log_path = args.log or os.path.join(state_dir, "trade_log.csv")

    if not os.path.isfile(log_path):
        print("No trade_log.csv; nothing to purge.")
        return 0

    try:
        df = pd.read_csv(log_path)
    except Exception as e:
        print(f"Could not read {log_path}: {e}")
        return 0

    if df.empty or "match_id" not in df.columns:
        print("trade_log.csv present but empty or missing match_id.")
        return 0

    keep = ~df["match_id"].astype(str).str.startswith("SYN")
    n_purged = int((~keep).sum())

    if n_purged > 0:
        df.loc[keep].to_csv(log_path, index=False)
        print(f"Purged {n_purged} synthetic rows -> {log_path}")
    else:
        print("No synthetic rows to purge.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
