#!/usr/bin/env python3
# run_backtest.py
# Minimal, safe utility you can call in CI to sanity-check a history CSV.
# It DOES NOT change your matrix backtester; that's in run_matrix_backtest.py.

from __future__ import annotations
import argparse, csv
from bet_math import infer_odds, infer_prob, infer_result

def cmd_validate(path: str, n: int) -> int:
    with open(path, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        fields = rdr.fieldnames or []
        print("Columns:", fields)
        shown = 0
        for row in rdr:
            try:
                o = infer_odds(row, strict=False)
                p = infer_prob(row)
                try:
                    r = infer_result(row)
                except Exception:
                    r = "?"
                print({"odds": o, "prob": p, "result": r})
                shown += 1
                if shown >= n:
                    break
            except Exception as e:
                print("Row parse error:", e)
                continue
    return 0

def main():
    ap = argparse.ArgumentParser(description="Utility helpers (non-matrix backtest).")
    sub = ap.add_subparsers(dest="cmd")

    v = sub.add_parser("validate", help="Validate columns in a history CSV")
    v.add_argument("--input", "-i", required=True)
    v.add_argument("--n", type=int, default=5)

    args = ap.parse_args()
    if args.cmd == "validate":
        raise SystemExit(cmd_validate(args.input, args.n))

    # If no subcommand was provided, print help and exit 0 (safe default).
    ap.print_help()
    raise SystemExit(0)

if __name__ == "__main__":
    main()
