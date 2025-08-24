#!/usr/bin/env python3
# scripts/compute_prob_vigfree.py
import argparse, sys
from pathlib import Path
import pandas as pd
import numpy as np

# ---------- helpers

def lc_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    # build a map from lower->original to keep exact names if needed
    lower_map = {}
    for c in df.columns:
        lower_map[c.lower()] = c
    df.columns = [c.lower() for c in df.columns]
    return df

def find_any(df, names):
    cols = set(df.columns)
    for n in names:
        if n in cols: return n
    return None

PRICE_CANDS   = ["price","odds","decimal_odds","o"]
PLAYER_CANDS  = ["player","selection","runner","team","home"]
OPP_CANDS     = ["opponent","oppo","opp","away"]
TOUR_CANDS    = ["tour","league","comp"]
MARKET_CANDS  = ["market","mk","bet_type"]
DATE_CANDS    = ["date","match_date","event_date","dt"]

# pairwise schema candidates
A_NAME_CANDS  = ["player_a","a_player","home","team_a","runner_a","a"]
B_NAME_CANDS  = ["player_b","b_player","away","team_b","runner_b","b"]
A_ODDS_CANDS  = ["odds_a","price_a","decimal_odds_a","a_odds","home_odds","odds1","a_price"]
B_ODDS_CANDS  = ["odds_b","price_b","decimal_odds_b","b_odds","away_odds","odds2","b_price"]

def implied_from_decimals(dec):
    dec = pd.to_numeric(dec, errors="coerce")
    with np.errstate(divide="ignore", invalid="ignore"):
        p = 1.0 / dec
    return p.clip(lower=0.0, upper=1.0)

def devig_pairwise(df: pd.DataFrame) -> pd.DataFrame:
    """
    If both sides of a matchup exist (same unordered pair & same date),
    renormalize so p_a + p_b = 1. Uses strings for key to keep vectorized.
    """
    if "date" not in df.columns:
        df["date"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d")

    a = df["player"].astype(str)
    b = df["opponent"].astype(str)
    x = a.where(a < b, b)  # min
    y = b.where(a < b, a)  # max
    key = x + "|" + y + "|" + df["date"].astype(str)
    df["_pair_key"] = key

    sums = df.groupby("_pair_key")["p_model"].transform("sum")
    mask = sums > 0
    df.loc[mask, "p_model"] = df.loc[mask, "p_model"] / sums[mask]
    return df.drop(columns=["_pair_key"])

def ensure_meta(df: pd.DataFrame) -> pd.DataFrame:
    if "tour" not in df.columns:
        df["tour"] = "WTA/ATP"
    if "market" not in df.columns:
        df["market"] = "H2H"
    if "date" not in df.columns:
        df["date"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    return df

# ---------- main

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

    raw = pd.read_csv(inp)
    df = lc_columns(raw)

    # ----- try single-sided first
    price_col  = find_any(df, PRICE_CANDS)
    player_col = find_any(df, PLAYER_CANDS)
    opp_col    = find_any(df, OPP_CANDS)

    tour_col   = find_any(df, TOUR_CANDS)
    market_col = find_any(df, MARKET_CANDS)
    date_col   = find_any(df, DATE_CANDS)

    if price_col and player_col and opp_col:
        long_df = df[[player_col, opp_col, price_col]].copy()
        long_df.columns = ["player","opponent","price"]
        if tour_col:   long_df["tour"] = df[tour_col]
        if market_col: long_df["market"] = df[market_col]
        if date_col:   long_df["date"] = df[date_col]

    else:
        # ----- fallback to pairwise schema
        a_name = find_any(df, A_NAME_CANDS)
        b_name = find_any(df, B_NAME_CANDS)
        a_odds = find_any(df, A_ODDS_CANDS)
        b_odds = find_any(df, B_ODDS_CANDS)

        if not all([a_name, b_name, a_odds, b_odds]):
            # dump quick debug + fail with clear message
            debug_path = outp.parent / "input_head_debug.csv"
            raw.head(20).to_csv(debug_path, index=False)
            cols = ", ".join(raw.columns.astype(str))
            raise SystemExit(
                "Input does not match expected schemas.\n"
                "Looked for either:\n"
                "  single-sided: player/opponent + price(odds/decimal_odds)\n"
                "  or pairwise:  player_a/player_b + odds_a/odds_b\n"
                f"Your columns: {cols}\n"
                f"Wrote sample to: {debug_path}"
            )

        base_cols = {}
        if tour_col:   base_cols["tour"] = df[tour_col]
        if market_col: base_cols["market"] = df[market_col]
        if date_col:   base_cols["date"] = df[date_col]

        a_rows = pd.DataFrame({
            "player":   df[a_name],
            "opponent": df[b_name],
            "price":    df[a_odds],
            **base_cols,
        })
        b_rows = pd.DataFrame({
            "player":   df[b_name],
            "opponent": df[a_name],
            "price":    df[b_odds],
            **base_cols,
        })
        long_df = pd.concat([a_rows, b_rows], ignore_index=True)

    # clean + compute
    long_df = long_df.dropna(subset=["player","opponent","price"]).copy()
    long_df["price"] = pd.to_numeric(long_df["price"], errors="coerce")
    long_df = long_df[long_df["price"] > 0].reset_index(drop=True)

    long_df = ensure_meta(long_df)
    long_df["p_model"] = implied_from_decimals(long_df["price"])
    long_df = devig_pairwise(long_df)

    # save enriched
    keep = ["player","opponent","price","tour","market","date","p_model"]
    long_df[keep].to_csv(outp, index=False)

    # small diagnostic
    with open(outp.parent / "diag_prob.md", "w", encoding="utf-8") as f:
        tot = len(long_df)
        valid = ((long_df["p_model"] >= 0) & (long_df["p_model"] <= 1)).sum()
        f.write(f"rows: {tot}\n")
        f.write(f"p_model valid [0..1]: {valid}/{tot} ({valid/max(tot,1):.1%})\n")
        f.write(f"mean p_model: {long_df['p_model'].mean():.3f}\n")
        f.write("sample:\n")
        f.write(long_df[keep].head(10).to_csv(index=False))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
