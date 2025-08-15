#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backtest TrueEdge8 + Kelly (live-parity core, with tunable boosts)
- Reads historical matches with odds & results
- Uses Elo CSVs: data/atp_elo.csv, data/wta_elo.csv (simple, static Elo)
- Applies TrueEdge8 (same 7-factor skeleton as live); adds:
    * recent_form_weight (replaces the 0.30 amplitude in form mapping)
    * surface_boost (+TE8 if strong surface record last 365d)
    * injury_penalty (extra global reduction when in injury window)
- Kelly staking with micro-cap for dogs (0.25 × Kelly)
- Outputs accuracy %, ROI %, bankroll change, per-surface metrics, and a detailed CSV

INPUT CSV (default: data/historical_matches.csv) must have columns:
  date (YYYY-MM-DD), tour (ATP/WTA), player, opponent, best_odds, result (1/0)
Optional: surface (Hard/Clay/Grass/Indoor), home_adv (True/False)

NOTE: Uses static Elo files; for purist backtests, switch to rolling pre-match Elo.
"""

import os, json, argparse, math
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# ---------------- CLI ----------------
def parse_args():
    p = argparse.ArgumentParser("Backtest TrueEdge8 + Kelly (tunable)")
    p.add_argument("--input", type=str, default="data/historical_matches.csv")
    p.add_argument("--elo-atp", type=str, default="data/atp_elo.csv")
    p.add_argument("--elo-wta", type=str, default="data/wta_elo.csv")
    p.add_argument("--injuries", type=str, default="injuries.json")
    p.add_argument("--start", type=str, default="2023-01-01")
    p.add_argument("--end", type=str, default="2025-12-31")

    # Gates / bands
    p.add_argument("--te8-dog", type=float, default=0.60)
    p.add_argument("--te8-fav", type=float, default=0.50)
    p.add_argument("--dog-band", type=str, default="1.90,6.00")
    p.add_argument("--fav-band", type=str, default="1.15,2.00")
    p.add_argument("--dog-cap", type=float, default=0.25)

    # Tuners
    p.add_argument("--recent-form-weight", type=float, default=0.30, help="Amplitude for form factor (0.30 = live parity)")
    p.add_argument("--surface-boost", type=float, default=0.05, help="Add to TE8 if strong surface (win%>=0.60, >=10 matches last 365d)")
    p.add_argument("--injury-penalty", type=float, default=0.15, help="Multiply TE8 by (1 - penalty) if player in injury window")

    # Bankroll / outputs
    p.add_argument("--stake-unit", type=float, default=100.0)
    p.add_argument("--bankroll", type=float, default=1000.0)
    p.add_argument("--out-csv", type=str, default="backtest_results.csv")
    p.add_argument("--summary", type=str, default="backtest_summary.md")
    return p.parse_args()

# ------------- IO Helpers -------------
def load_csv(path, req=None):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    df = pd.read_csv(path)
    if req:
        miss = [c for c in req if c not in df.columns]
        if miss:
            raise RuntimeError(f"{path} missing required columns: {miss}")
    return df

def load_injuries(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ------------- Elo --------------------
def elo_rating(df, player):
    if df.empty: return 1500.0
    row = df[df["player"] == str(player)]
    return float(row["elo"].iloc[0]) if not row.empty else 1500.0

def elo_prob(df, p1, p2):
    e1 = elo_rating(df, p1); e2 = elo_rating(df, p2)
    return 1.0 / (1.0 + 10.0 ** ((e2 - e1) / 400.0))

# --------- Kelly Fraction -------------
def kelly_fraction(p, odds):
    if not (0 <= p <= 1) or odds <= 1: return 0.0
    b = odds - 1.0
    return max((odds * p - 1.0) / b, 0.0)

# -------- Injuries Lookup -------------
def injury_factor_for(player, cur_date, injuries):
    """
    injuries: list of dicts with keys: player, start_date, end_date, impact (0..1)
    Returns a TrueEdge factor mapping: 0.40 + 0.50 * impact
    """
    try:
        p = str(player).lower()
        for rec in injuries:
            if str(rec.get("player","")).lower() == p:
                start = pd.to_datetime(rec.get("start_date"))
                end   = pd.to_datetime(rec.get("end_date"))
                if pd.isna(start) or pd.isna(end): 
                    continue
                if start <= cur_date <= end:
                    impact = float(rec.get("impact", 0.90))
                    impact = min(max(impact, 0.0), 1.0)
                    return round(0.40 + 0.50 * impact, 2), True
    except Exception:
        pass
    return 0.90, False  # neutral+ if no injury

# -------- Helpers on history ----------
def last_matches(hist, player, cur_date, n=10):
    d = hist[(hist["player"]==player) & (hist["date"]<cur_date)].sort_values("date", ascending=False).head(n)
    return d

def surface_stats(hist, player, cur_date, surface, window_days=365, min_matches=10):
    if not isinstance(surface, str) or not surface: 
        return None
    start = cur_date - pd.Timedelta(days=window_days)
    d = hist[(hist["player"]==player) & (hist["date"]<cur_date) &
             (hist["date"]>=start) & (hist["surface"].astype(str).str.lower()==surface.lower())]
    if d.empty: return {"n":0,"wr":None}
    return {"n": len(d), "wr": float(d["result"].mean())}

# -------- TrueEdge8 (live skeleton + tuners) -----
def trueedge8_for_match(row, hist_all, cur_date, injuries_list, recent_form_weight=0.30,
                        surface_boost=0.05, injury_penalty=0.15):
    """
    Base (live parity):
      form = 0.50 + 0.30*winrate_last10
      surface = 0.60
      h2h = 0.55
      rest = 0.55 (≤1d) / 0.60 (≤4d) / 0.65 (>4d)
      injury = 0.90 (no injury) or mapped factor if injured
      stage = 0.55
      mental = 0.70 if home else 0.60

    Tuners:
      - recent_form_weight replaces the 0.30 amplitude above
      - if surface wr ≥ 0.60 and n ≥ 10 in last 365d → add surface_boost to final TE8 (cap at 1.0)
      - if injured → multiply final TE8 by (1 - injury_penalty)
    """

    player = row["player"]
    surface = row.get("surface", None)
    home_adv = str(row.get("home_adv","")).lower() in ("1","true","yes")

    # 1) Form via last 10 matches
    d = last_matches(hist_all, player, cur_date, n=10)
    if d.empty:
        form = 0.55
        last_date = pd.NaT
    else:
        wr = float(d["result"].mean())
        form = 0.50 + recent_form_weight * max(0.0, min(1.0, wr))  # amplitude tunable
        last_date = d["date"].max()

    # 2) Surface base factor (stays conservative)
    surface_factor = 0.60

    # 3) H2H (no lookup here) — neutral+
    h2h = 0.55

    # 4) Rest via days since last match
    if pd.isna(last_date):
        rest = 0.60
    else:
        days = (cur_date - last_date).days
        rest = 0.55 if days<=1 else (0.60 if days<=4 else 0.65)

    # 5) Injury factor + flag
    injury_factor, injured_now = injury_factor_for(player, cur_date, injuries_list)

    # 6) Stage (unknown in most CSVs)
    stage = 0.55

    # 7) Mental/home
    mental = 0.70 if home_adv else 0.60

    base_te8 = (form + surface_factor + h2h + rest + injury_factor + stage + mental) / 7.0

    # Apply tuners:
    # Surface boost if strong on this surface in last 365d
    if isinstance(surface, str) and surface:
        ss = surface_stats(hist_all, player, cur_date, surface, window_days=365, min_matches=10)
        if ss and ss["n"] >= 10 and ss["wr"] is not None and ss["wr"] >= 0.60 and surface_boost > 0:
            base_te8 = min(1.0, base_te8 + surface_boost)

    # Extra injury penalty (global) if injured
    if injured_now and injury_penalty > 0:
        base_te8 = max(0.0, base_te8 * (1.0 - injury_penalty))

    # Factor details for analysis
    detail = {
        "te_form": round(form,2),
        "te_surface": round(surface_factor,2),
        "te_h2h": round(h2h,2),
        "te_rest": round(rest,2),
        "te_injury": round(injury_factor,2),
        "te_stage": round(stage,2),
        "te_mental": round(mental,2),
        "te_surface_boost_applied": 1 if (isinstance(surface,str) and surface and ss and ss["n"]>=10 and ss["wr"]>=0.60 and surface_boost>0) else 0,
        "te_injury_penalty_applied": 1 if injured_now and injury_penalty>0 else 0
    }
    return round(base_te8,2), detail

# --------------- Main -----------------
def main():
    args = parse_args()

    # Parse bands & thresholds
    dog_min, dog_max = map(float, args.dog_band.split(","))
    fav_min, fav_max = map(float, args.fav_band.split(","))

    # Load data
    hist = load_csv(args.input, ["date","tour","player","opponent","best_odds","result"])
    atp = load_csv(args.elo_atp, ["player","elo"])
    wta = load_csv(args.elo_wta, ["player","elo"])
    injuries_list = load_injuries(args.injuries)

    # Types / parse
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce")
    hist = hist.dropna(subset=["date"])
    hist = hist[(hist["date"] >= pd.to_datetime(args.start)) & (hist["date"] <= pd.to_datetime(args.end))]
    for c in ["best_odds","result"]:
        hist[c] = pd.to_numeric(hist[c], errors="coerce")
    hist["surface"] = hist.get("surface", "")
    hist = hist.dropna(subset=["best_odds","result"])

    # Sort by date
    hist = hist.sort_values("date").reset_index(drop=True)

    logs = []
    bankroll = float(args.bankroll)

    for _, row in hist.iterrows():
        tour = str(row["tour"]).upper()
        elo_df = atp if tour == "ATP" else wta
        date = row["date"]
        player = str(row["player"]); opponent = str(row["opponent"])
        odds = float(row["best_odds"])
        y = int(row["result"])

        if not (odds > 1.0):
            continue

        # Elo probability
        p = elo_prob(elo_df, player, opponent)

        # TE8 score (with tuners)
        te8, te_detail = trueedge8_for_match(
            {"player": player, "surface": row.get("surface",""), "home_adv": row.get("home_adv", None)},
            hist, date, injuries_list,
            recent_form_weight=args.recent_form_weight,
            surface_boost=args.surface_boost,
            injury_penalty=args.injury_penalty
        )

        # Kelly
        k = kelly_fraction(p, odds)

        # Dog micro-cap
        is_dog = (dog_min <= odds <= dog_max)
        is_fav = (fav_min <= odds <= fav_max)
        if is_dog and k > 0:
            k = args.dog_cap * k

        # TE8 gate
        te_thresh = args.te8_dog if is_dog else args.te8_fav
        bet = (k > 0) and (te8 >= te_thresh)

        # Flat 1u P/L if bet
        profit_flat = (odds - 1.0) if (bet and y == 1) else (-1.0 if bet else 0.0)

        # Kelly bankroll sim
        stake = 0.0
        if bet and bankroll > 0:
            stake = bankroll * k
            bankroll += stake * (odds - 1.0) if y == 1 else -stake

        logs.append({
            "date": date.date(),
            "tour": tour,
            "surface": row.get("surface",""),
            "player": player,
            "opponent": opponent,
            "odds": round(odds,2),
            "prob": round(p,3),
            "kelly": round(k,4),
            "te8": round(te8,2),
            "bet": bool(bet),
            "result": int(y),
            "profit_flat_u": profit_flat,   # units
            "stake_kelly_eur": round(stake,2),
            # factor breakdown
            **te_detail
        })

    if not logs:
        print("No rows processed or no bets generated. Check inputs and thresholds.")
        return

    R = pd.DataFrame(logs)
    B = R[R["bet"] == True].copy()
    total_bets = len(B)
    if total_bets == 0:
        print("No bets passed filters (TE8/Kelly). Relax thresholds or check data.")
        return

    # Core metrics
    hit_rate = B["result"].mean()
    roi_flat = B["profit_flat_u"].mean()
    profit_flat_total = B["profit_flat_u"].sum()
    final_bankroll = bankroll
    roi_kelly = (final_bankroll - float(args.bankroll)) / float(args.bankroll) if args.bankroll > 0 else np.nan
    avg_kelly = B["kelly"].mean()

    # Brier / Log-loss
    def brier(p, y): return (p - y) ** 2
    def logloss(p, y, eps=1e-12):
        p = min(max(p, eps), 1 - eps)
        return -(y * math.log(p) + (1 - y) * math.log(1 - p))
    B["brier"] = [brier(p, y) for p, y in zip(B["prob"], B["result"])]
    B["logloss"] = [logloss(p, y) for p, y in zip(B["prob"], B["result"])]
    brier_mean = B["brier"].mean()
    logloss_mean = B["logloss"].mean()

    # Odds buckets
    bins = [1.0, 1.5, 2.0, 3.0, 6.0, 20.0]
    B["odds_bin"] = pd.cut(B["odds"], bins=bins, right=True)
    bucket = B.groupby("odds_bin").agg(
        bets=("odds","count"),
        hit=("result","mean"),
        avg_odds=("odds","mean"),
        avg_prob=("prob","mean"),
        roi_flat=("profit_flat_u","mean")
    ).reset_index()

    # Surface metrics
    surf = B.copy()
    if "surface" in surf.columns:
        surf["surface"] = surf["surface"].astype(str).str.title()
        surf_summary = surf.groupby("surface").agg(
            bets=("odds","count"),
            hit=("result","mean"),
            avg_odds=("odds","mean"),
            roi_flat=("profit_flat_u","mean")
        ).reset_index()
    else:
        surf_summary = pd.DataFrame(columns=["surface","bets","hit","avg_odds","roi_flat"])

    # Summary text
    lines = []
    lines.append(f"### Backtest — TrueEdge8 + Kelly (tunable)")
    lines.append(f"- Period: {R['date'].min()} to {R['date'].max()}")
    lines.append(f"- Total bets: **{total_bets}**")
    lines.append(f"- **Hit rate:** {hit_rate*100:.1f}%")
    lines.append(f"- **ROI (flat 1u):** {roi_flat*100:.2f}% per bet (Total: {profit_flat_total:.2f}u)")
    if not np.isnan(roi_kelly):
        lines.append(f"- **ROI (Kelly sim):** {roi_kelly*100:.2f}% (Final bankroll: €{final_bankroll:.2f})")
    lines.append(f"- **Avg Kelly fraction (post dog-cap):** {avg_kelly:.3f}")
    lines.append(f"- **Brier:** {brier_mean:.3f}  •  **Logloss:** {logloss_mean:.3f}")
    lines.append("")
    lines.append("#### By odds band (bets | hit% | avg_odds | avg_p | ROI flat per bet)")
    for _, b in bucket.iterrows():
        lines.append(f"- {b['odds_bin']}: {int(b['bets'])} | {b['hit']*100:.1f}% | {b['avg_odds']:.2f} | {b['avg_prob']:.2f} | {b['roi_flat']*100:.2f}%")

    if not surf_summary.empty:
        lines.append("")
        lines.append("#### By surface (bets | hit% | avg_odds | ROI flat per bet)")
        for _, s in surf_summary.iterrows():
            lines.append(f"- {s['surface']}: {int(s['bets'])} | {s['hit']*100:.1f}% | {s['avg_odds']:.2f} | {s['roi_flat']*100:.2f}%")

    summary_text = "\n".join(lines)
    print(summary_text)

    # Write outputs
    B.sort_values(["date","tour"], inplace=True)
    B.to_csv(args.out_csv, index=False)
    with open(args.summary, "w", encoding="utf-8") as f:
        f.write(summary_text)

if __name__ == "__main__":
    main()
