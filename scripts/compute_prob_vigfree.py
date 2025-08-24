#!/usr/bin/env python3
# scripts/compute_prob_vigfree.py
import argparse, sys
from pathlib import Path
import pandas as pd
import numpy as np

# -------- helpers
CANDIDATES = {
    "price": ["price", "odds", "decimal_odds", "o"],
    "player": ["player", "selection", "team", "runner", "home"],
    "opponent": ["opponent", "oppo", "away"],
    "tour": ["tour", "league", "comp"],
    "market": ["market", "mk", "bet_type"],
    "date": ["date", "match_date", "event_date", "dt"],
}

def first_col(df, names, required=False):
    for n in names:
        if n in df.columns:
            return n
        # tolerant: case-insensitive
        for c in df.columns:
            if c.lower() == n.lower():
                return c
    if required:
        raise ValueError(f"Missing required column; looked for any of: {names}")
    return None

def implied_prob_from_decimals(dec):
    dec = np.asarray(dec, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        p = 1.0 / dec
    return np.clip(p, 0.0, 1.0)

def remove_vig_pair(p_a, p_b):
    """
    Very simple no-frills de-vig on two-sided markets:
    Normalize so p_a + p_b = 1 (if both present).
    """
    s = p_a + p_b
    ok = (s > 0)
    p_a_out = np.where(ok, p_a / s, p_a)
    p_b_out = np.where(ok, p_b / s, p_b)
    return p_a_out, p_b_out

# -------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    inp = Path(args.input)
    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)

    if not inp.exists() or inp.stat().st_size == 0:
        raise SystemExit(f"Input not found or empty: {inp}")

    df = pd.read_csv(inp)
    # Soft standardization of column names
    price_col   = first_col(df, CANDIDATES["price"], required=True)
    player_col  = first_col(df, CANDIDATES["player"], required=True)
    opp_col     = first_col(df, CANDIDATES["opponent"], required=True)
    tour_col    = first_col(df, CANDIDATES["tour"]) or "tour"
    market_col  = first_col(df, CANDIDATES["market"]) or "market"
    date_col    = first_col(df, CANDIDATES["date"]) or "date"

    # Rename into canonical names the engine expects
    rename_map = {
        price_col: "price",
        player_col: "player",
        opp_col: "opponent",
    }
    if tour_col in df.columns:   rename_map[tour_col] = "tour"
    if market_col in df.columns: rename_map[market_col] = "market"
    if date_col in df.columns:   rename_map[date_col] = "date"
    df = df.rename(columns=rename_map)

    # Defaults if missing
    if "tour" not in df.columns: df["tour"] = "WTA/ATP"
    if "market" not in df.columns: df["market"] = "H2H"
    if "date" not in df.columns:
        # try to infer from index or fallback to today-like placeholder
        df["date"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d")

    # Build pair rows (player/opponent) if any duplicate fixtures exist:
    # We’ll compute both sides’ implied probs where possible.
    # For safety, just compute p_model as 1/price for now (single-sided),
    # then try to de-vig if the reverse price exists.
    df["p_model"] = implied_prob_from_decimals(df["price"])

    # Optional: attempt pairwise de-vig by (player, opponent, date)
    key = ["player", "opponent", "date"]
    rev = df.merge(df[key + ["price","p_model"]], left_on=key,
                   right_on=[ "opponent","player","date" ],
                   suffixes=("","_rev"), how="left")

    # where reverse exists, normalize
    mask = rev["p_model_rev"].notna()
    a = rev["p_model"].to_numpy()
    b = rev["p_model_rev"].fillna(0.0).to_numpy()
    a2, b2 = remove_vig_pair(a, b)
    rev.loc[mask, "p_model"] = a2[mask]

    # Keep canonical columns
    keep = ["player","opponent","price","tour","market","date","p_model"]
    rev[keep].to_csv(outp, index=False)

    # tiny log
    with open(outp.parent / "diag_prob.md", "w", encoding="utf-8") as f:
        ok = ((rev["p_model"] >= 0) & (rev["p_model"] <= 1)).mean()
        f.write(f"rows: {len(rev)}\n")
        f.write(f"p_model valid [0..1]: {ok:.2%}\n")
        f.write(f"mean p_model: {rev['p_model'].mean():.3f}\n")
        f.write(f"sample:\n{rev[keep].head(8).to_markdown(index=False)}\n")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
