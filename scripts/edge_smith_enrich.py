#!/usr/bin/env python3
"""
EdgeSmith — enrich probability file for backtesting.

Inputs
------
A CSV that *must* contain odds and probabilities for both sides with one of
these column name sets (case-insensitive):

- Required (canonical):
    oa, ob, pa, pb

- Accepted aliases (will be mapped to canonical):
    odds_a, odds_b, prob_a, prob_b
    implied_prob_a, implied_prob_b
    prob_a_vigfree, prob_b_vigfree

The script will:
- Normalize columns to (oa, ob, pa, pb)
- Compute edge_a = pa - 1/oa
         edge_b = pb - 1/ob
- true_edge  = max(edge_a, edge_b)
- sel        = 'A' if edge_a >= edge_b else 'B'
- sel_player = player_a or player_b if present
- fair_odds_a = 1/pa ; fair_odds_b = 1/pb

Output
------
Writes a fully enriched CSV to --output (can overwrite the input path).
Returns nonzero exit if no usable rows.

Usage
-----
python scripts/edge_smith_enrich.py \
  --input outputs/prob_enriched.csv \
  --output outputs/prob_enriched.csv
"""
from __future__ import annotations
import argparse
import sys
import math
from pathlib import Path
import pandas as pd

CANON = ("oa", "ob", "pa", "pb")

ALIASES = {
    "oa": ["oa", "odds_a", "oddsA", "odds_a_close", "odd_a", "o_a"],
    "ob": ["ob", "odds_b", "oddsB", "odds_b_close", "odd_b", "o_b"],
    "pa": ["pa", "prob_a", "probA", "implied_prob_a", "prob_a_vigfree", "p_a"],
    "pb": ["pb", "prob_b", "probB", "implied_prob_b", "prob_b_vigfree", "p_b"],
}

def _first_present(cols, options):
    for name in options:
        if name in cols:
            return name
        # allow case-insensitive match
        for c in cols:
            if c.lower() == name.lower():
                return c
    return None

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = set(df.columns)
    mapping = {}
    for tgt in CANON:
        found = _first_present(cols, ALIASES[tgt])
        if not found:
            raise KeyError(f"Missing required column for '{tgt}'. "
                           f"Looked for any of: {ALIASES[tgt]}")
        mapping[tgt] = found

    # Reassign canonical views without losing originals
    out = df.copy()
    for tgt, src in mapping.items():
        out[tgt] = pd.to_numeric(out[src], errors="coerce")

    return out

def compute_edges(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # sanity: positive odds and 0<prob<1
    valid = (
        (out["oa"] > 1e-9) &
        (out["ob"] > 1e-9) &
        (out["pa"] > 0) & (out["pa"] < 1) &
        (out["pb"] > 0) & (out["pb"] < 1)
    )
    before = len(out)
    out = out.loc[valid].copy()
    dropped = before - len(out)

    if len(out) == 0:
        raise RuntimeError("No usable rows after sanity checks (odds/prob bounds).")

    # compute
    out["fair_odds_a"] = 1.0 / out["pa"]
    out["fair_odds_b"] = 1.0 / out["pb"]

    out["edge_a"] = out["pa"] - (1.0 / out["oa"])
    out["edge_b"] = out["pb"] - (1.0 / out["ob"])

    # choose side with higher edge (ties -> A)
    out["true_edge"] = out[["edge_a", "edge_b"]].max(axis=1)
    out["sel"] = (out["edge_a"] >= out["edge_b"]).map({True: "A", False: "B"})

    # friendly selection label if players present
    if "player_a" in out.columns and "player_b" in out.columns:
        out["sel_player"] = out.apply(
            lambda r: r["player_a"] if r["sel"] == "A" else r["player_b"], axis=1
        )

    # deterministic column order: keep existing, then append our metrics at the end
    metric_cols = ["fair_odds_a", "fair_odds_b", "edge_a", "edge_b", "true_edge", "sel"]
    if "sel_player" in out.columns:
        metric_cols.append("sel_player")

    # ensure no duplicate columns in final order
    base_cols = [c for c in df.columns if c not in metric_cols]
    final_cols = base_cols + metric_cols

    # info line to stdout (visible in Actions logs)
    print(f"[enrich] input rows={before}, dropped_invalid={dropped}, kept={len(out)}")
    pos = (out["true_edge"] > 0).sum()
    print(f"[enrich] positive edges={pos} ({pos/len(out):.0%})")

    return out[final_cols]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to prob_enriched.csv (pre-enrichment).")
    ap.add_argument("--output", required=True, help="Path to write enriched CSV (may overwrite input).")
    args = ap.parse_args()

    inp = Path(args.input)
    outp = Path(args.output)
    if not inp.exists():
        print(f"[enrich] ERROR: input not found: {inp}", file=sys.stderr)
        sys.exit(2)

    df = pd.read_csv(inp)
    if df.shape[0] == 0:
        print(f"[enrich] WARNING: input has zero rows: {inp}", file=sys.stderr)
        # still write back the header + our fields for traceability
        df["oa"] = pd.NA; df["ob"] = pd.NA; df["pa"] = pd.NA; df["pb"] = pd.NA
        df["fair_odds_a"] = pd.NA; df["fair_odds_b"] = pd.NA
        df["edge_a"] = pd.NA; df["edge_b"] = pd.NA; df["true_edge"] = pd.NA; df["sel"] = pd.NA
        df.to_csv(outp, index=False)
        sys.exit(0)

    try:
        norm = normalize_columns(df)
        enr = compute_edges(norm)
    except Exception as e:
        print(f"[enrich] FATAL: {e}", file=sys.stderr)
        sys.exit(1)

    outp.parent.mkdir(parents=True, exist_ok=True)
    enr.to_csv(outp, index=False)
    print(f"[enrich] wrote -> {outp} with columns: {', '.join([c for c in enr.columns if c in ('oa','ob','pa','pb','edge_a','edge_b','true_edge','sel')])}")

if __name__ == "__main__":
    main()
