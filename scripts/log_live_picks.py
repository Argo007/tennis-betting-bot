#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Append live picks to state/trade_log.csv, with clear no-op logging.
Usage:
  python scripts/log_live_picks.py --picks live_results/picks_live.csv --state-dir state \
    --kelly 0.5 --stake-cap 200 --max-stake-eur 50 --assume-random-if-missing false
"""

import argparse
import os
import sys
import pandas as pd

def _safe_read_csv(path):
    if not os.path.isfile(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--picks", required=True, help="path to picks_live.csv")
    ap.add_argument("--state-dir", default="state")
    ap.add_argument("--kelly", type=float, default=0.5)
    ap.add_argument("--stake-cap", type=float, default=200.0)
    ap.add_argument("--max-stake-eur", type=float, default=50.0)
    ap.add_argument("--assume-random-if-missing", dest="assume_random_if_missing",
                    default="false", choices=["true","false"],
                    help="just plumbed-through flag for transparency; no effect here")
    args = ap.parse_args()

    os.makedirs(args.state_dir, exist_ok=True)
    picks = _safe_read_csv(args.picks)

    if picks.empty or "match_id" not in picks.columns:
        print(f"[log_live_picks] no picks to log (file missing/empty: {args.picks}).")
        sys.exit(0)

    # Heuristic: explain WHY no rows survive (if filters upstream left nothing)
    shown = picks.shape[0]
    if "edge" in picks.columns:
        viable = picks[picks["edge"].astype(float) > 0]
        if viable.empty:
            min_edge = os.environ.get("MIN_EDGE", "unknown")
            print(f"[log_live_picks] {shown} rows present but none pass edge > 0 "
                  f"(engine min_edge={min_edge}). No log entry written.")
            sys.exit(0)

    # Minimal trade record (don’t duplicate if already logged)
    log_path = os.path.join(args.state_dir, "trade_log.csv")
    existing = _safe_read_csv(log_path)

    cols = ["ts","match_id","selection","odds","p","edge","stake_eur"]
    out = picks.copy()
    # Add ts if missing
    if "ts" not in out.columns:
        out["ts"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M")

    if "stake_eur" not in out.columns:
        # simple constant stake respecting max-stake-eur and cap
        out["stake_eur"] = min(args.max_stake_eur, args.stake_cap)

    out = out[[c for c in cols if c in out.columns]]
    if out.empty:
        print("[log_live_picks] picks present but required columns missing; nothing logged.")
        sys.exit(0)

    # Deduplicate by (ts, match_id, selection, odds)
    if not existing.empty:
        key_cols = [c for c in ["ts","match_id","selection","odds"] if c in out.columns and c in existing.columns]
        if key_cols:
            before = len(out)
            merged = out.merge(existing[key_cols].drop_duplicates(), how="left", indicator=True, on=key_cols)
            out = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
            out = out[[c for c in cols if c in out.columns]]
            dropped = before - len(out)
            if dropped > 0:
                print(f"[log_live_picks] skipped {dropped} duplicate rows.")

    final = pd.concat([existing, out], ignore_index=True) if not existing.empty else out
    final.to_csv(log_path, index=False)
    print(f"[log_live_picks] appended {len(out)} row(s) to {log_path}")

if __name__ == "__main__":
    main()
