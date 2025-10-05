#!/usr/bin/env python3
"""
Prepare an enriched demo dataset from data/raw/odds/sample_odds.csv.

Input  (minimal): date,player_a,player_b,odds_a,odds_b
Output (enriched): + implied_prob_*, model_prob_*, edge_*, winner

This is a deterministic demo so that CI always places some bets.
Replace this later with your real model that emits model_prob_* and edge_*.
"""
from pathlib import Path
import pandas as pd

SRC = Path("data/raw/odds/sample_odds.csv")
DST = Path("data/raw/odds/sample_odds_enriched.csv")

def main():
    if not SRC.exists():
        raise FileNotFoundError(f"Missing input file: {SRC}")

    df = pd.read_csv(SRC)

    # Sanity
    required = {"date","player_a","player_b","odds_a","odds_b"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input is missing columns: {sorted(missing)}")

    implied_a = 1.0 / df["odds_a"]
    implied_b = 1.0 / df["odds_b"]

    # Deterministic nudge pattern so some bets have +edge
    nudges = [0.06, -0.03, -0.08, 0.02, 0.05, -0.07, -0.04, 0.09, 0.01, -0.05]
    nudges = (nudges * ((len(df) // len(nudges)) + 1))[:len(df)]

    model_prob_a = (implied_a + pd.Series(nudges)).clip(0.05, 0.95)
    model_prob_b = 1.0 - model_prob_a

    edge_a = model_prob_a - implied_a
    edge_b = model_prob_b - implied_b

    # Synthetic ground-truth winner (for realized PnL in the demo)
    winner = []
    for pa in model_prob_a:
        if pa >= 0.55:
            winner.append("A")
        elif pa <= 0.45:
            winner.append("B")
        else:
            winner.append("A")

    out = df.copy()
    out["implied_prob_a"] = implied_a.round(6)
    out["implied_prob_b"] = implied_b.round(6)
    out["model_prob_a"] = model_prob_a.round(6)
    out["model_prob_b"] = model_prob_b.round(6)
    out["edge_a"]        = edge_a.round(6)
    out["edge_b"]        = edge_b.round(6)
    out["winner"]        = winner

    DST.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST, index=False)
    print(f"Wrote {DST} ({len(out)} rows).")

if __name__ == "__main__":
    main()
