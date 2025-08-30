#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch (or just stage) match results in a minimal, deterministic way.

Usage examples
-------------
# 1) Read results from a CSV committed in the repo:
python scripts/fetch_results_stub.py \
  --in data/results/manual_results.csv \
  --out live_results/results.csv

# 2) Provide quick results inline:
python scripts/fetch_results_stub.py \
  --manual "L002=1,L003=0,SYN001=1" \
  --out live_results/results.csv

CSV format required (header row):
match_id,result
L002,1
L003,0

Notes
-----
- result must be 0 or 1.
- Last write wins if duplicates exist (dedup by match_id).
- Creates output directory if needed.
"""

from __future__ import annotations
import os
import argparse
import pandas as pd


def parse_manual(s: str) -> pd.DataFrame:
    rows = []
    s = s.strip()
    if not s:
        return pd.DataFrame(columns=["match_id", "result"])
    for item in s.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            print(f"WARNING: skip malformed token: '{item}' (expected match_id=0|1)")
            continue
        mid, val = item.split("=", 1)
        mid = mid.strip()
        try:
            r = int(val.strip())
        except Exception:
            print(f"WARNING: skip malformed result for '{mid}': '{val}'")
            continue
        if r not in (0, 1):
            print(f"WARNING: result must be 0 or 1 for '{mid}'; got {r}")
            continue
        rows.append({"match_id": mid, "result": r})
    df = pd.DataFrame(rows, columns=["match_id", "result"])
    return df


def read_input_csv(path: str) -> pd.DataFrame:
    if not path or not os.path.isfile(path):
        return pd.DataFrame(columns=["match_id", "result"])
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"WARNING: cannot read {path}: {e}")
        return pd.DataFrame(columns=["match_id", "result"])
    need = {"match_id", "result"}
    if not need.issubset(set(df.columns)):
        print(f"WARNING: {path} missing required columns {need}; got {list(df.columns)}")
        return pd.DataFrame(columns=["match_id", "result"])
    # coerce result to 0/1
    df["result"] = df["result"].apply(lambda x: 1 if str(x).strip() == "1" else 0)
    return df[["match_id", "result"]].copy()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_file", default="data/results/manual_results.csv",
                    help="Input CSV with match_id,result (optional).")
    ap.add_argument("--manual", default="", help='Inline results like "L002=1,L003=0" (optional).')
    ap.add_argument("--out", default="live_results/results.csv", help="Output CSV path.")
    args = ap.parse_args()

    df_csv = read_input_csv(args.in_file)
    df_manual = parse_manual(args.manual)

    # combine with "manual wins"
    df = pd.concat([df_csv, df_manual], ignore_index=True)
    if df.empty:
        print("No results provided. Nothing to write.")
        return 0

    # deduplicate by match_id, keep the last occurrence (manual overrides)
    df = df.drop_duplicates(subset=["match_id"], keep="last")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"Wrote results to: {args.out} ({len(df)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
