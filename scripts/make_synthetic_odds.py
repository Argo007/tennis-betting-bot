#!/usr/bin/env python3
"""
Make synthetic odds that line up with Jeff Sackmann results (CI-safe).
- Pulls ATP + WTA matches for the given years
- Writes data/raw/odds/synthetic_odds.csv with columns:
  date,player_a,player_b,odds_a,odds_b
- Odds are realistic (decimal), with a small overround.

Usage:
  python scripts/make_synthetic_odds.py --years "2021 2022 2023 2024" --rows 5000
"""
import io, os, random, argparse, math
from pathlib import Path
import numpy as np
import pandas as pd
import requests

def fetch_csv(url, retries=3, timeout=12):
    last=None
    for _ in range(retries):
        try:
            r=requests.get(url, timeout=timeout)
            r.raise_for_status()
            return pd.read_csv(io.StringIO(r.text))
        except Exception as e:
            last=e
    raise RuntimeError(f"Failed to fetch {url}: {last}")

def make_odds(p, overround=0.02):
    """
    Given fair win prob p for player_a, return (odds_a, odds_b) with small overround.
    """
    p = np.clip(p, 0.05, 0.95)
    q = 1.0 - p
    fair_a = 1.0/p
    fair_b = 1.0/q
    # apply overround by shrinking probs slightly
    scale = 1.0 + overround
    return round(fair_a*scale, 2), round(fair_b*scale, 2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default="2021 2022 2023 2024")
    ap.add_argument("--rows", type=int, default=6000, help="approx rows to sample (balanced ATP/WTA)")
    args = ap.parse_args()

    years = [int(y) for y in args.years.split() if y.strip().isdigit()]

    frames=[]
    pulled={}
    for tour, repo in [("ATP","tennis_atp"), ("WTA","tennis_wta")]:
        for y in years:
            url = f"https://raw.githubusercontent.com/JeffSackmann/{repo}/master/{tour.lower()}_matches_{y}.csv"
            df = fetch_csv(url)
            df["tour"] = tour
            # keep only necessary columns; drop rows with missing names/dates
            df = df[["tourney_date","winner_name","loser_name","surface","round","tourney_name","tour"]].copy()
            df = df.dropna(subset=["tourney_date","winner_name","loser_name"])
            df["date"] = pd.to_datetime(df["tourney_date"].astype(str), format="%Y%m%d", errors="coerce").dt.normalize()
            df = df.dropna(subset=["date"])
            frames.append(df)
            pulled[(tour,y)] = len(df)

    allm = pd.concat(frames, ignore_index=True)
    if allm.empty:
        raise SystemExit("No Sackmann rows pulled — can't synthesize odds.")

    # sample roughly half ATP / half WTA
    target = max(500, int(args.rows))
    per_tour = target // 2

    out_rows=[]
    rng = np.random.default_rng(42)

    def prob_from_round_surface(row):
        """rough prior for winner prob p_a (player_a = winner_name)"""
        base = 0.60  # winners win; duh
        # round: finals/semis have stronger favs
        rd = str(row.get("round","")).lower()
        if rd in ("f","sf"): base += 0.05
        if rd in ("qf","r16"): base += 0.02
        # surface flavor: slight variance
        surf = str(row.get("surface","")).lower()
        if surf == "clay": base += 0.02
        if surf == "grass": base -= 0.01
        # random noise
        base += float(rng.normal(0, 0.06))
        return float(np.clip(base, 0.52, 0.85))  # winner rarely <52% here

    for tour in ("ATP","WTA"):
        pool = allm[allm["tour"]==tour]
        if pool.empty: continue
        k = min(per_tour, len(pool))
        take = pool.sample(n=k, random_state=42, replace=False)
        for _,r in take.iterrows():
            p = prob_from_round_surface(r)
            oa, ob = make_odds(p, overround=0.02 + float(rng.uniform(0, 0.02)))
            out_rows.append([r["date"].strftime("%Y-%m-%d"),
                             r["winner_name"], r["loser_name"], oa, ob])

    out = pd.DataFrame(out_rows, columns=["date","player_a","player_b","odds_a","odds_b"])
    out = out.sort_values("date").reset_index(drop=True)

    outdir = Path("data/raw/odds"); outdir.mkdir(parents=True, exist_ok=True)
    dst = outdir / "synthetic_odds.csv"
    out.to_csv(dst, index=False)
    print(f"Wrote {len(out):,} rows -> {dst}")

if __name__ == "__main__":
    main()
