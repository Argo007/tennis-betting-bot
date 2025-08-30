#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
compute_max_stake.py
Reads bankroll from state/bankroll.json and prints the per-bet cap in EUR:
max(1.0, max_frac * bankroll)

Usage:
  python scripts/compute_max_stake.py \
    --state-dir state \
    --max-frac 0.05 \
    [--fallback 1000]

This prints ONLY the number so it can be captured in a shell var.
"""

import argparse
import json
import os
import sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-dir", default="state")
    ap.add_argument("--max-frac", type=float, default=0.05)
    ap.add_argument("--fallback", type=float, default=1000.0,
                    help="fallback bankroll if state file missing")
    args = ap.parse_args()

    bankroll = args.fallback
    path = os.path.join(args.state_dir, "bankroll.json")
    try:
        with open(path, "r") as f:
            data = json.load(f)
            bankroll = float(data.get("bankroll", bankroll))
    except Exception:
        pass

    cap = max(1.0, args.max_frac * bankroll)
    # print ONLY the value so the workflow can capture it
    print(f"{cap:.6f}")

if __name__ == "__main__":
    sys.exit(main())
