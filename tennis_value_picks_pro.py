#!/usr/bin/env python3
"""
Emit candidate picks from local odds (or synthetic fallback).

Looks for CSVs in:
  - data/raw/odds_live/*.csv
  - data/raw/odds/*.csv

Expected headers in any order (case-insensitive):
  date, player_a, player_b, odds_a, odds_b

Outputs:
  - value_picks_pro.csv with columns:
    date,player,opponent,odds,model_conf,book_count
"""
import argparse, glob
from pathlib import Path
import pandas as pd

def load_any(p):
    try:
        if p.lower().endswith((".xlsx",".xls")):
            return pd.read_excel(p)
        return pd.read_csv(p)
    except Exception:
        return pd.DataFrame()

def normalize_cols(df):
    lower={c.lower():c for c in df.columns}
    def pick(*opts):
        for o in opts:
            if o in lower: return lower[o]
        return None
    C = {
        "date": pick("date","match_date","event_date"),
        "a": pick("player_a","player","home","p1","selection"),
        "b": pick("player_b","opponent","away","p2"),
        "oa": pick("odds_a","price_a","decimal_odds_a","odds1","home_odds","price1"),
        "ob": pick("odds_b","price_b","decimal_odds_b","odds2","away_odds","price2"),
    }
    if None in C.values(): return pd.DataFrame()
    out = pd.DataFrame({
        "date": pd.to_datetime(df[C["date"]], errors="coerce").dt.normalize(),
        "player_a": df[C["a"]].astype(str),
        "player_b": df[C["b"]].astype(str),
        "odds_a": pd.to_numeric(df[C["oa"]], errors="coerce"),
        "odds_b": pd.to_numeric(df[C["ob"]], errors="coerce"),
    }).dropna(subset=["date","player_a","player_b","odds_a","odds_b"])
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookahead-h", default="6")
    ap.add_argument("--region", default="EU")
    ap.add_argument("--kelly", default="1.0")
    ap.add_argument("--min-books", type=int, default=3)
    ap.add_argument("--max-age-mins", type=int, default=60)
    ap.add_argument("--out", default="value_picks_pro.csv")
    args = ap.parse_args()

    paths = (glob.glob("data/raw/odds_live/*.*") +
             glob.glob("data/raw/odds/*.*"))
    frames=[normalize_cols(load_any(p)) for p in paths]
    df = pd.concat([f for f in frames if not f.empty], ignore_index=True) if frames else pd.DataFrame()

    if df.empty and Path("data/raw/odds/synthetic_odds.csv").exists():
        df = normalize_cols(pd.read_csv("data/raw/odds/synthetic_odds.csv"))

    if df.empty:
        Path(args.out).write_text("date,player,opponent,odds,model_conf,book_count\n")
        print("No odds found; wrote empty candidate CSV.")
        return

    # naive model_conf proxy: convert favorites/dogs to 0.60/0.40 +/- small drift
    # (real model should replace this; engine will override via Elo if missing)
    conf=[]
    for _,r in df.iterrows():
        if r["odds_a"]<=1.80:
            conf.append(0.60)
        elif r["odds_a"]>=2.50:
            conf.append(0.45)
        else:
            conf.append(0.52)
    out = pd.DataFrame({
        "date": df["date"].dt.strftime("%Y-%m-%d"),
        "player": df["player_a"],
        "opponent": df["player_b"],
        "odds": df["odds_a"].round(2),
        "model_conf": conf,
        "book_count": args.min_books  # placeholder; wire real book count if available
    })
    out.to_csv(args.out, index=False)
    print(f"Wrote {len(out)} candidate rows -> {args.out}")

if __name__ == "__main__":
    main()
