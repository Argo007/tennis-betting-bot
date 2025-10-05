#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

SRC = Path("data/raw/odds/sample_odds.csv")
DST = Path("data/raw/odds/sample_odds_enriched.csv")

def main():
    df = pd.read_csv(SRC)

    implied_a = 1.0 / df["odds_a"]
    implied_b = 1.0 / df["odds_b"]

    nudges = [0.06, -0.03, -0.08, 0.02, 0.05, -0.07, -0.04, 0.09, 0.01, -0.05]
    nudges = (nudges * ((len(df) // len(nudges)) + 1))[:len(df)]

    model_prob_a = (implied_a + pd.Series(nudges)).clip(0.05, 0.95)
    model_prob_b = 1.0 - model_prob_a

    edge_a = model_prob_a - implied_a
    edge_b = model_prob_b - implied_b

    # deterministic "winner" so we can compute realized PnL in a demo
    winner = []
    for pa in model_prob_a:
        if pa >= 0.55: winner.append("A")
        elif pa <= 0.45: winner.append("B")
        else: winner.append("A")

    out = df.copy()
    out["implied_prob_a"] = implied_a.round(6)
    out["implied_prob_b"] = implied_b.round(6)
    out["model_prob_a"] = model_prob_a.round(6)
    out["model_prob_b"] = model_prob_b.round(6)
    out["edge_a"] = edge_a.round(6)
    out["edge_b"] = edge_b.round(6)
    out["winner"] = winner

    DST.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST, index=False)
    print(f"Wrote {DST} with {len(out)} rows.")

if __name__ == "__main__":
    main()
