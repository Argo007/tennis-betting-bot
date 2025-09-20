#!/usr/bin/env python3
"""
Normalize a tennis odds CSV so it always contains:
  - oa, ob : decimal odds for A and B
  - pa, pb : implied win probabilities (vig-agnostic)

Input column flexibility:
  - Already has pa/pb  -> pass through
  - oa/ob              -> compute pa/pb
  - odds_a/odds_b      -> treated as oa/ob
  - a_odds/b_odds      -> treated as oa/ob
"""

import argparse
import sys
from pathlib import Path
import pandas as pd


CANDIDATES_A = ["oa", "odds_a", "a_odds"]
CANDIDATES_B = ["ob", "odds_b", "b_odds"]


def find_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def ensure_pa_pb(df: pd.DataFrame) -> pd.DataFrame:
    # If pa/pb already there, trust and return
    if "pa" in df.columns and "pb" in df.columns:
        return df

    # Map odds columns to oa/ob if needed
    col_oa = find_col(df, CANDIDATES_A)
    col_ob = find_col(df, CANDIDATES_B)

    if col_oa is None or col_ob is None:
        raise ValueError(
            "need oa/ob (or odds_a/odds_b or a_odds/b_odds) to compute pa/pb"
        )

    # Create canonical oa/ob if not present
    if "oa" not in df.columns:
        df["oa"] = df[col_oa]
    if "ob" not in df.columns:
        df["ob"] = df[col_ob]

    inv_a = 1.0 / df["oa"].astype(float)
    inv_b = 1.0 / df["ob"].astype(float)
    total = inv_a + inv_b
    df["pa"] = inv_a / total
    df["pb"] = inv_b / total
    return df


def main():
    ap = argparse.ArgumentParser(description="Prepare dataset with pa/pb.")
    ap.add_argument("--input", required=True, help="Input CSV path")
    ap.add_argument("--output", required=True, help="Output CSV path")
    args = ap.parse_args()

    inp = Path(args.input)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(inp)
    df = ensure_pa_pb(df)
    df.to_csv(out, index=False)
    print(f"[prepare_dataset] Saved -> {out}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[prepare_dataset][ERROR]: {e}", file=sys.stderr)
        sys.exit(1)
