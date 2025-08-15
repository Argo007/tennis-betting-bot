#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TrueEdge8 + Kelly — Clean Summary (no tables)
- Inputs:
    data/atp_elo.csv, data/wta_elo.csv         (from your Elo step)
    value_picks_pro.csv                        (from your odds/model step)
    injuries.json   (optional; {"Player": 0.7, ...} 0..1 multiplier)
- Usage:
    python tennis_value_engine.py --lookahead-h 24 --min-conf 50 --bankroll 200 --diagnostics
"""

import os, json, math, argparse
import pandas as pd
from datetime import datetime, timezone, timedelta

# ---------------- CLI ----------------
def parse_args():
    p = argparse.ArgumentParser(description="TrueEdge8 + Kelly clean renderer")
    p.add_argument("--lookahead-h", type=int, default=24)
    p.add_argument("--min-conf", type=int, default=50)
    p.add_argument("--bankroll", type=float, default=0.0)   # € to show stake suggestion; 0 = hide
    p.add_argument("--diagnostics", action="store_true")
    return p.parse_args()

# -------------- Config --------------
BUF_MINUTES = 5
ODDS_DOG_MIN, ODDS_DOG_MAX = 1.90, 6.00
ODDS_FAV_MIN, ODDS_FAV_MAX = 1.15, 2.00
TE8_THRESHOLD_DOG, TE8_THRESHOLD_FAV = 0.60, 0.50
UNDERDOG_KELLY_CAP = 0.25  # stake = 0.25 * Kelly for dogs

# ---------- IO helpers ----------
def load_csv(path, req=None):
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    if req:
        miss = [c for c in req if c not in df.columns]
        if miss: raise RuntimeError(f"{path} missing columns: {miss}")
    return df

def load_injuries():
    try:
        with open("injuries.json","r",encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def write_summary(text: str):
    out_path = os.environ.get("GITHUB_STEP_SUMMARY", "summary.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text)
    print("Summary written to:", out_path)

# ---------- Elo ----------
def elo_rating(df, player):
    if df.empty: return 1500.0
    row = df[df["player"] == str(player)]
    return float(row["elo"].iloc[0]) if not row.empty else 1500.0

def elo_prob(df, p1, p2):
    e1 = elo_rating(df, p1); e2 = elo_rating(df, p2)
    return 1.0 / (1.0 + 10.0 ** ((e2 - e1) / 400.0))

# ---------- Kelly ----------
def kelly_fraction(p, odds):
    if not (0 <= p <= 1) or odds <= 1: return 0.0
    b = odds - 1.0
    return max((odds * p - 1.0) / b, 0.0)

# ---------- TrueEdge8 (light but sane) ----------
def trueedge8(r, injuries_map):
    # 1) Form via confidence (scaled 0.50..0.80)
    conf = float(r.get("confidence", 0.0))
    form = 0.50 + 0.30 * max(0, min(1, conf/100.0))
    # 2) Surface (placeholder conservative)
    surface = 0.60
    # 3) H2H (placeholder neutral+)
    h2h = 0.55
    # 4) Rest (reduce if starting soon)
    eta_min = float(r.get("eta_min", 240.0))
    rest = 0.55 if eta_min < 90 else (0.60 if eta_min < 240 else 0.65)
    # 5) Injury/news multiplier -> factor
    inj_mult = float(injuries_map.get(str(r.get("player","")), 1.0))
    injury = 0.40 + 0.50 * inj_mult   # 1.0 -> 0.90 ; 0.0 -> 0.40
    # 6) Tournament stage (neutral)
    stage = 0.55
    # 7) Mental/home
    mental = 0.70 if bool(r.get("home_adv", False)) else 0.60
    return round((form+surface+h2h+rest+injury+stage+mental)/7.0, 2)

# ---------- Formatting ----------
def eta_fmt(minutes: float) -> str:
    m = int(round(minutes)); h, mm = divmod(m, 60)
    return f"{h}h {mm:02d}m" if h else f"{mm}m"

def ts_fmt(ts) -> str:
    try:
        return pd.to_datetime(ts, utc=True).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(ts)

def badge(ok): return "🟢 **BET**" if ok else "🔴 **PASS**"

# ---------------- Main ----------------
def main():
    args = parse_args()
    injuries_map = load_injuries()

    # Elo ratings
    atp = load_csv("data/atp_elo.csv", ["player","elo"])
    wta = load_csv("data/wta_elo.csv", ["player","elo"])

    # Model output
    df = load_csv("value_picks_pro.csv")
    if df.empty:
        write_summary("_value_picks_pro.csv not found or empty._")
        return

    # Types
    for c in ["blended_prob","best_odds","confidence"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    if "confidence" not in df.columns: df["confidence"] = 0

    # Time parsing + filters
    if "commence_time_utc" in df.columns:
        df["start_dt"] = pd.to_datetime(df["commence_time_utc"], utc=True, errors="coerce")
    elif "commence_time" in df.columns:
        df["start_dt"] = pd.to_datetime(df["commence_time"], utc=True, errors="coerce")
    else:
        df["start_dt"] = pd.NaT

    now = datetime.now(timezone.utc)
    cut = now + timedelta(minutes=BUF_MINUTES)
    horizon = now + timedelta(hours=args.lookahead_h)

    # Remove live/in-play if columns exist
    if "is_live" in df.columns:
        df = df[~df["is_live"].fillna(False).astype(bool)]
    if "status" in df.columns:
        df = df[~df["status"].astype(str).str.contains("live|in ?play|started|progress", case=False, na=False)]

    df = df[df["start_dt"].notna() & (df["start_dt"] >= cut) & (df["start_dt"] <= horizon)]
    df = df[df["confidence"].fillna(0) >= args.min_conf]

    if df.empty:
        write_summary(f"_No eligible matches in ≤{args.lookahead_h}h window (min_conf={args.min_conf})._")
        return

    df["eta_min"] = (df["start_dt"] - now).dt.total_seconds()/60.0

    # Compute prob/kelly/te8
    rows = []
    for _, r in df.iterrows():
        tour = str(r.get("tour","")).upper()
        elo_df = atp if tour == "ATP" else wta
        player, opp = str(r.get("player","")), str(r.get("opponent",""))
        odds = float(r.get("best_odds", float("nan")))
        if not (odds > 1): continue

        prob = elo_prob(elo_df, player, opp)
        kelly = kelly_fraction(prob, odds)
        is_dog = (ODDS_DOG_MIN <= odds <= ODDS_DOG_MAX)
        if is_dog and kelly > 0:
            kelly = 0.25 * kelly  # micro-stake cap

        te8 = trueedge8(r, injuries_map)
        te_thresh = TE8_THRESHOLD_DOG if is_dog else TE8_THRESHOLD_FAV
        bet = (kelly > 0) and (te8 >= te_thresh)

        rows.append({
            "tour": tour, "player": player, "opponent": opp,
            "odds": round(odds, 2), "prob": round(prob, 3),
            "kelly": round(kelly, 3), "te8": round(te8, 2),
            "bet": bet, "start": r["start_dt"], "eta_min": float(r["eta_min"])
        })

    X = pd.DataFrame(rows)
    if X.empty:
        write_summary(f"_No qualified picks after TE8/Kelly filters in ≤{args.lookahead_h}h window._")
        return

    X = X.sort_values(["start","kelly"], ascending=[True, False])

    # Build clean summary
    lines = [
        f"_Filtered at {now.strftime('%Y-%m-%d %H:%M UTC')} · upcoming-only (≤{args.lookahead_h}h, buffer {BUF_MINUTES}m) · min_conf={args.min_conf}._",
        ""
    ]
    for tour in ["ATP","WTA"]:
        sub = X[X["tour"] == tour]
        if sub.empty: continue
        lines.append(f"## {tour} Picks")
        for _, r in sub.iterrows():
            stake = f" • Stake≈€{args.bankroll*r['kelly']:.2f}" if args.bankroll>0 and r["kelly"]>0 else ""
            lines.append(
                f"{badge(r['bet'])} — {r['player']} vs {r['opponent']} — {r['odds']:.2f}  \n"
                f"p={r['prob']:.2f} • Kelly={r['kelly']:.3f} • TE8={r['te8']:.2f}{stake}  \n"
                f"🗓 {ts_fmt(r['start'])} • ETA: {eta_fmt(r['eta_min'])}"
            )
        lines.append("")

    if args.diagnostics:
        lines += [
            "---",
            "### Diagnostics",
            f"- Considered rows: {len(df)}",
            f"- Picks after filters: {len(X)}",
            f"- Dog band: {ODDS_DOG_MIN}-{ODDS_DOG_MAX} | Fav band: {ODDS_FAV_MIN}-{ODDS_FAV_MAX}",
            f"- TE8 thresholds (fav/dog): {TE8_THRESHOLD_FAV}/{TE8_THRESHOLD_DOG}",
            f"- Underdog cap: {UNDERDOG_KELLY_CAP}×Kelly"
        ]

    write_summary("\n".join(lines))

if __name__ == "__main__":
    main()
