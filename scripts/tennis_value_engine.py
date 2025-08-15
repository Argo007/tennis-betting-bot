#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TrueEdge8 + Kelly Pro Engine
- Adjustable lookahead (hours) via CLI: --lookahead-h 24
- Uses your Elo CSVs: data/atp_elo.csv, data/wta_elo.csv
- Reads matches from: value_picks_pro.csv
- Excludes live/in-progress; shows only upcoming within window
- Kelly stake sizing with micro-cap for underdogs (<= 0.25 × Kelly)
- TrueEdge8 scoring (7 practical factors; no CLV fetch for speed)
- Clean, color-coded summary to GitHub Step Summary (or summary.md locally)
- Optional diagnostics: --diagnostics
"""

import os
import json
import math
import argparse
from datetime import datetime, timedelta, timezone
import pandas as pd

# --------------------------- CLI ---------------------------
def parse_args():
    p = argparse.ArgumentParser(description="TrueEdge8 + Kelly Pro Engine")
    p.add_argument("--lookahead-h", type=int, default=24, help="Hours ahead to include (default 24)")
    p.add_argument("--min-conf", type=int, default=50, help="Minimum confidence filter (default 50)")
    p.add_argument("--bankroll", type=float, default=0.0, help="Optional bankroll € to display stake size (0 = hide)")
    p.add_argument("--diagnostics", action="store_true", help="Show diagnostics section")
    return p.parse_args()

# ----------------------- Config Defaults -------------------
ODDS_DOG_MIN, ODDS_DOG_MAX = 1.90, 6.00
ODDS_FAV_MIN, ODDS_FAV_MAX = 1.15, 2.00
TE8_THRESHOLD_DOG, TE8_THRESHOLD_FAV = 0.60, 0.50
UNDERDOG_KELLY_CAP = 0.25  # stake = min(Kelly, 0.25*Kelly) -> effectively 0.25×K
BUF_MINUTES = 5            # ignore <5m (treat as live-ish)

# ---------------------- Data Loading -----------------------
def load_csv_safely(path, required_cols=None):
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    if required_cols:
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise RuntimeError(f"{path} missing required columns: {missing}")
    return df

def get_injuries():
    try:
        with open("injuries.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

# ---------------------- Elo Probability --------------------
def get_elo_rating(elo_df: pd.DataFrame, player: str) -> float:
    if elo_df.empty:
        return 1500.0
    row = elo_df.loc[elo_df["player"] == str(player)]
    return float(row["elo"].iloc[0]) if not row.empty else 1500.0

def elo_win_prob(elo_df: pd.DataFrame, p1: str, p2: str) -> float:
    e1 = get_elo_rating(elo_df, p1)
    e2 = get_elo_rating(elo_df, p2)
    # p = 1 / (1 + 10^((e2-e1)/400))
    return 1.0 / (1.0 + 10.0 ** ((e2 - e1) / 400.0))

# ----------------------- Kelly Fraction --------------------
def kelly_fraction(prob: float, odds: float) -> float:
    # Kelly (decimal odds): K = ((o-1)*p - (1-p)) / (o-1) = (o*p - 1) / (o-1)
    if not (0.0 <= prob <= 1.0) or odds <= 1.0:
        return 0.0
    b = odds - 1.0
    k = (odds * prob - 1.0) / b
    return float(max(0.0, min(1.0, k)))

# -------------------- TrueEdge8 (7 factors here) -----------
def trueedge8(row: pd.Series, injuries_map: dict) -> float:
    """
    7 pragmatic factors (0..1), averaged:
      1) Form      -> proxy from confidence column (scaled)
      2) Surface   -> placeholder (use 0.6/0.7 if surface aligns; we use mild 0.6 default)
      3) H2H       -> placeholder 0.55 (neutral/slight edge)
      4) Rest      -> assume decent rest unless very short ETA
      5) Injury    -> multiplier from injuries.json (1.0 = healthy; <1 reduces)
      6) Stage     -> mild bump (0.55) — can be tuned by round if provided
      7) Mental    -> mild bump for home_adv flag
    We keep conservative ranges so TE8 isn't overly optimistic.
    """
    conf = float(row.get("confidence", 0.0))
    eta_min = float(row.get("eta_min", 180.0))

    # 1) Form via confidence (MIN_CONF..100 -> 0.50..0.80 roughly)
    form = 0.50 + 0.30 * max(0.0, min(1.0, conf / 100.0))  # 0.50..0.80

    # 2) Surface (placeholder)
    surface = 0.60

    # 3) H2H (placeholder)
    h2h = 0.55

    # 4) Rest (if starting very soon, slightly reduce)
    if eta_min < 90:
        rest = 0.55
    elif eta_min < 240:
        rest = 0.60
    else:
        rest = 0.65

    # 5) Injury/news multiplier
    inj_mult = float(injuries_map.get(str(row.get("player", "")), 1.0))
    # convert multiplier (0..1) to a factor ~ [0.40..0.75..0.90]
    injury = 0.40 + 0.50 * inj_mult  # if 1.0 -> 0.90; if 0.0 -> 0.40

    # 6) Tournament stage (unknown -> neutral)
    stage = 0.55

    # 7) Mental (home advantage flag)
    mental = 0.60 + (0.10 if bool(row.get("home_adv", False)) else 0.0)

    factors = [form, surface, h2h, rest, injury, stage, mental]
    return round(sum(factors) / len(factors), 2)

# ---------------------- Formatting Helpers -----------------
def eta_fmt(minutes: float) -> str:
    try:
        m = int(round(minutes))
        h, mm = divmod(m, 60)
        return f"{h}h {mm:02d}m" if h else f"{mm}m"
    except Exception:
        return "—"

def ts_fmt(ts: pd.Timestamp) -> str:
    try:
        return ts.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(ts)

# -------------------------- Main ---------------------------
def main():
    args = parse_args()
    injuries_map = get_injuries()

    # Load Elo
    atp_elo = load_csv_safely("data/atp_elo.csv", ["player", "elo"])
    wta_elo = load_csv_safely("data/wta_elo.csv", ["player", "elo"])

    # Load matches from your model output
    df = load_csv_safely("value_picks_pro.csv")
    if df.empty:
        out("_No matches file (value_picks_pro.csv) found._")
        return

    # Normalize columns
    for c in ["blended_prob", "best_odds", "confidence"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "confidence" not in df.columns:
        df["confidence"] = 0

    # Parse commence time (prefer UTC column)
    if "commence_time_utc" in df.columns:
        df["commence_dt"] = pd.to_datetime(df["commence_time_utc"], utc=True, errors="coerce")
    elif "commence_time" in df.columns:
        df["commence_dt"] = pd.to_datetime(df["commence_time"], utc=True, errors="coerce")
    else:
        df["commence_dt"] = pd.NaT

    now = datetime.now(timezone.utc)
    cut = now + timedelta(minutes=BUF_MINUTES)
    horizon = now + timedelta(hours=args.lookahead_h)

    # Exclude live/in-progress if flags/strings exist
    if "is_live" in df.columns:
        df = df[~df["is_live"].fillna(False).astype(bool)]
    if "status" in df.columns:
        df = df[~df["status"].astype(str).str.contains("live|in ?play|started|progress", case=False, na=False)]

    # 24h/12h/48h filter (adjustable)
    df = df[df["commence_dt"].notna() & (df["commence_dt"] >= cut) & (df["commence_dt"] <= horizon)]

    # Confidence filter
    df = df[df["confidence"].fillna(0) >= args.min_conf]

    if df.empty:
        out(f"_No eligible matches in ≤{args.lookahead_h}h window (min_conf={args.min_conf})._")
        return

    # Precompute ETA and pick tour ELO df
    df["eta_min"] = (df["commence_dt"] - now).dt.total_seconds() / 60.0

    # Compute probabilities (Elo-based), Kelly, TE8
    rows = []
    for _, r in df.iterrows():
        tour = str(r.get("tour", "")).upper()
        elo_df = atp_elo if tour == "ATP" else wta_elo

        p_player = str(r.get("player", ""))
        p_oppo   = str(r.get("opponent", ""))

        odds = float(r.get("best_odds", float("nan")))
        if not (odds > 1.0):
            continue

        prob = elo_win_prob(elo_df, p_player, p_oppo)  # Elo probability
        kelly = kelly_fraction(prob, odds)

        # Enforce micro-stake cap for dogs
        is_dog = (ODDS_DOG_MIN <= odds <= ODDS_DOG_MAX)
        if is_dog and kelly > 0:
            kelly = 0.25 * kelly  # enforce 0.25×K

        te8 = trueedge8(r, injuries_map)

        # Decision
        te_thresh = TE8_THRESHOLD_DOG if is_dog else TE8_THRESHOLD_FAV if (ODDS_FAV_MIN <= odds <= ODDS_FAV_MAX) else TE8_THRESHOLD_FAV
        bet_ok = (kelly > 0) and (te8 >= te_thresh)

        rows.append({
            "tour": tour,
            "player": p_player,
            "opponent": p_oppo,
            "odds": odds,
            "prob": round(prob, 3),
            "kelly": round(kelly, 3),
            "te8": round(te8, 2),
            "bet": bet_ok,
            "start": r["commence_dt"],
            "eta_min": float(r["eta_min"])
        })

    if not rows:
        out(f"_No qualified picks after TE8/Kelly filters in ≤{args.lookahead_h}h window._")
        return

    X = pd.DataFrame(rows)

    # Sort by: sooner first, then higher Kelly
    X = X.sort_values(["start", "kelly"], ascending=[True, False])

    # Build summary
    header = f"_Filtered at {now.strftime('%Y-%m-%d %H:%M UTC')} · upcoming-only (≤{args.lookahead_h}h, buffer {BUF_MINUTES}m) · min_conf={args.min_conf}._"
    lines = [header, ""]

    def badge(b):  # color-coded via emoji
        return "🟢 **BET**" if b else "🔴 **PASS**"

    def stake_note(k):
        if args.bankroll > 0 and k > 0:
            return f" • Stake≈€{args.bankroll * k:.2f}"
        return ""

    for tour in ["ATP", "WTA"]:
        sub = X[X["tour"] == tour]
        if sub.empty:
            continue
        lines.append(f"## {tour} Picks")
        for _, r in sub.iterrows():
            lines.append(
                f"{badge(r['bet'])} — {r['player']} vs {r['opponent']} — {r['odds']:.2f}  \n"
                f"p={r['prob']:.2f} • Kelly={r['kelly']:.3f} • TE8={r['te8']:.2f}{stake_note(r['kelly'])}  \n"
                f"🗓 {ts_fmt(r['start'])} • ETA: {eta_fmt(r['eta_min'])}"
            )
        lines.append("")

    # Diagnostics (optional)
    if args.diagnostics:
        lines += [
            "---",
            "### Diagnostics",
            f"- Matches considered: {len(df)}",
            f"- Picks after filters: {len(X)}",
            f"- Dog odds band: [{ODDS_DOG_MIN}, {ODDS_DOG_MAX}] | Fav band: [{ODDS_FAV_MIN}, {ODDS_FAV_MAX}]",
            f"- TE8 thresholds: fav {TE8_THRESHOLD_FAV}, dog {TE8_THRESHOLD_DOG}",
            f"- Underdog micro-cap: {UNDERDOG_KELLY_CAP}×Kelly",
        ]

    # Write to GitHub Step Summary (or local file if not in Actions)
    out("\n".join(lines))

def out(text: str):
    path = os.environ.get("GITHUB_STEP_SUMMARY", "summary.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print("Summary written to:", path)

if __name__ == "__main__":
    main()
