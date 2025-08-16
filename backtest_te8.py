#!/usr/bin/env python3
"""
TrueEdge8 backtest (Sackmann + local odds)
- Deterministic, CI-safe, no external I/O beyond provided files
- Chronological processing (no look-ahead)
"""

import argparse, json, math, sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="data/historical_matches.csv")
    p.add_argument("--elo-atp", default=None, help="optional Elo snapshot for ATP")
    p.add_argument("--elo-wta", default=None, help="optional Elo snapshot for WTA")
    p.add_argument("--injuries", default=None, help="injuries.json (optional)")

    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)

    p.add_argument("--te8-dog", type=float, required=True)
    p.add_argument("--te8-fav", type=float, required=True)

    p.add_argument("--dog-band", default="2.20,4.50")
    p.add_argument("--fav-band", default="1.15,2.00")

    p.add_argument("--dog-cap", type=float, default=0.25, help="Multiplies Kelly for dogs")
    p.add_argument("--stake-unit", type=float, default=100.0)
    p.add_argument("--bankroll", type=float, default=1000.0)

    p.add_argument("--surface-boost", type=float, default=0.05)
    p.add_argument("--recent-form-weight", type=float, default=0.30)
    p.add_argument("--injury-penalty", type=float, default=0.15)

    p.add_argument("--out-csv", default="backtest_results.csv")
    p.add_argument("--summary", default="backtest_summary.md")
    return p.parse_args()


def odds_to_prob(o):
    if pd.isna(o) or o <= 1.0:
        return np.nan
    return 1.0 / o


def kelly_fraction(p, b):
    """
    Kelly = (bp - q)/b  where b = odds-1, q = 1-p
    """
    if not (0 < p < 1) or b <= 0:
        return 0.0
    k = (b * p - (1 - p)) / b
    return max(0.0, float(k))


def parse_band(s):
    try:
        lo, hi = s.split(",")
        return float(lo), float(hi)
    except Exception:
        return (0.0, 99.0)


def load_injuries(path):
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        return []
    try:
        with p.open() as f:
            data = json.load(f)
        # Normalize
        out = []
        for r in data:
            try:
                out.append({
                    "player": str(r["player"]).strip().lower(),
                    "start": pd.to_datetime(r["start_date"]).normalize(),
                    "end": pd.to_datetime(r["end_date"]).normalize(),
                    "impact": float(r.get("impact", 0.15))
                })
            except Exception:
                continue
        return out
    except Exception:
        return []


def player_injured(inj_list, player_name, date):
    name = str(player_name).strip().lower()
    for r in inj_list:
        if r["player"] == name and r["start"] <= date <= r["end"]:
            return True, r["impact"]
    return False, 0.0


def surface_hint(surf: str):
    s = (surf or "").strip().lower()
    if s in ("clay", "cl"):
        return "clay"
    if s in ("grass", "gr"):
        return "grass"
    if s in ("hard", "hardcourt", "hc", "carpet"):
        return "hard"
    return "unknown"


def recent_form_boost(form_winrate, weight):
    # Map [0,1] -> [-weight, +weight], centered at 0.5
    if np.isnan(form_winrate):
        return 0.0
    return weight * (form_winrate - 0.5) * 2.0


def surface_boost_factor(surf, weight):
    # Light, symmetric nudge for surfaces we "like" (placeholder logic)
    s = surface_hint(surf)
    if s == "clay":
        return +weight * 0.5
    if s == "grass":
        return +weight * 0.3
    if s == "hard":
        return +weight * 0.4
    return 0.0


def te8_score(base_price_edge, elo_edge_bp, surf_boost, form_boost, injury_pen):
    """
    TE8 is a bounded confidence score in [0,1] combining:
    - price edge (fair vs market)
    - Elo edge (transformed to ~win prob delta)
    - surface boost
    - recent form
    - injury penalty (applied as negative)
    All components are squashed and combined then re-bounded to [0,1].
    """
    # Each term in [-1, +1]-ish
    x = (
        0.45 * base_price_edge +
        0.35 * elo_edge_bp +
        0.10 * surf_boost +
        0.10 * form_boost -
        injury_pen
    )
    # squash to (0,1) sigmoid-ish; keep monotonic
    return float(1.0 / (1.0 + math.exp(-3.5 * x)))


def main():
    args = parse_args()

    # Load dataset
    df = pd.read_csv(args.input)
    # Expected input columns from the build workflow:
    # date,tour,tournament,round,player,opponent,odds,opp_odds,result,elo_player,elo_opponent,source
    for c in ["date","tour","player","opponent","odds","result"]:
        if c not in df.columns:
            print(f"::error ::Missing column '{c}' in {args.input}", file=sys.stderr)
            sys.exit(1)

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    start = pd.to_datetime(args.start)
    end = pd.to_datetime(args.end)
    df = df[(df["date"] >= start) & (df["date"] <= end)].copy()

    # Odds bands
    dog_lo, dog_hi = parse_band(args.dog_band)
    fav_lo, fav_hi = parse_band(args.fav_band)

    # Implied probabilities
    df["implied_prob"] = df["odds"].apply(odds_to_prob)

    # Optional Elo snapshots (not required, build already gives per-match pre ELOs)
    # If snapshot files exist, we won't override per-match pre ratings; we’ll just ensure columns exist.
    if "elo_player" not in df.columns or "elo_opponent" not in df.columns:
        df["elo_player"] = 1500.0
        df["elo_opponent"] = 1500.0

    # Prepare injury list
    injuries = load_injuries(args.injuries)

    # Recent form tracking (rolling last N=10 outcomes for each player BEFORE current match)
    N_FORM = 10
    df = df.sort_values(["date", "tour", "player"]).reset_index(drop=True)
    form_map = {}  # key=(tour, player) -> list of last N results (1/0)

    form_rates = []
    for i, r in df.iterrows():
        key = (r["tour"], r["player"])
        hist = form_map.get(key, [])
        # winrate BEFORE this match
        winrate = np.mean(hist) if hist else np.nan
        form_rates.append(winrate)
        # update history with current result (no look-ahead)
        res = r.get("result", np.nan)
        if pd.notna(res):
            hist = (hist + [int(res)])[-N_FORM:]
            form_map[key] = hist

    df["recent_form_wr"] = form_rates

    # Price edge vs "fair" prob from Elo (soft transform): convert Elo diff -> expected prob
    # Logistic transform: p = 1 / (1 + 10^(-elo_diff/400))
    elo_diff = df["elo_player"].astype(float) - df["elo_opponent"].astype(float)
    df["p_elo"] = 1.0 / (1.0 + 10 ** (-(elo_diff / 400.0)))

    # Base price edge in [-1,+1]: (p_elo - p_market) / max(p_market, 1e-6)
    df["price_edge"] = (df["p_elo"] - df["implied_prob"]) / df["implied_prob"].clip(lower=1e-6)

    # Elo edge in "bp" units: center to ~[-1,+1]
    df["elo_edge_bp"] = (elo_diff / 200.0).clip(-2.0, 2.0) / 2.0  # scale 200 Elo ~ 0.5

    # Tuners
    df["surf_boost"] = df.get("surface", "").apply(lambda s: surface_boost_factor(s, args.surface_boost))
    df["form_boost"] = df["recent_form_wr"].apply(lambda wr: recent_form_boost(wr, args.recent_form_weight))

    # Injury penalty per match (only for our selected player)
    inj_pen = []
    for _, r in df.iterrows():
        inj, imp = player_injured(injuries, r["player"], r["date"])
        inj_pen.append(args.injury_penalty * imp if inj else 0.0)
    df["inj_pen"] = inj_pen

    # TE8 score
    df["te8"] = df.apply(
        lambda r: te8_score(
            base_price_edge=float(r["price_edge"]) if pd.notna(r["price_edge"]) else 0.0,
            elo_edge_bp=float(r["elo_edge_bp"]) if pd.notna(r["elo_edge_bp"]) else 0.0,
            surf_boost=float(r["surf_boost"]) if pd.notna(r["surf_boost"]) else 0.0,
            form_boost=float(r["form_boost"]) if pd.notna(r["form_boost"]) else 0.0,
            injury_pen=float(r["inj_pen"]) if pd.notna(r["inj_pen"]) else 0.0,
        ),
        axis=1
    )

    # Classify dog/fav by odds
    df["is_dog"] = df["odds"] >= 2.00

    # Band filters
    dog_mask = (df["odds"] >= dog_lo) & (df["odds"] <= dog_hi)
    fav_mask = (df["odds"] >= fav_lo) & (df["odds"] <= fav_hi)

    # Entry rules by TE8 threshold
    df["entry"] = False
    df.loc[df["is_dog"] & dog_mask & (df["te8"] >= args.te8_dog), "entry"] = True
    df.loc[(~df["is_dog"]) & fav_mask & (df["te8"] >= args.te8_fav), "entry"] = True

    # Kelly staking per entry
    bankroll = float(args.bankroll)
    stakes = []
    kfracs = []
    profits = []
    bankrolls = []

    for _, r in df.iterrows():
        if not bool(r["entry"]):
            stakes.append(0.0)
            kfracs.append(0.0)
            profits.append(0.0)
            bankrolls.append(bankroll)
            continue

        p_est = float(r["te8"])  # using TE8 as confidence proxy in [0,1]
        b = float(r["odds"]) - 1.0
        k = kelly_fraction(p_est, b)

        if r["is_dog"]:
            k *= args.dog_cap  # micro-cap on dogs

        stake = bankroll * k
        stake = min(stake, args.stake_unit)  # optional safety: cap absolute unit (keeps runs realistic)
        # P/L
        res = int(r["result"])
        profit = (r["odds"] - 1.0) * stake if res == 1 else -stake

        bankroll += profit
        stakes.append(float(stake))
        kfracs.append(float(k))
        profits.append(float(profit))
        bankrolls.append(float(bankroll))

    df["kelly_fraction"] = kfracs
    df["stake"] = stakes
    df["pnl"] = profits
    df["bankroll"] = bankrolls

    # Keep only useful output cols (concise)
    out_cols = [
        "date","tour","tournament","round","player","opponent",
        "odds","implied_prob","p_elo","price_edge","elo_player","elo_opponent",
        "recent_form_wr","surf_boost","inj_pen","te8","is_dog","entry",
        "kelly_fraction","stake","result","pnl","bankroll","source"
    ]
    existing = [c for c in out_cols if c in df.columns]
    out = df[existing].copy()
    out = out.sort_values(["date","tour","player"]).reset_index(drop=True)

    Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False)

    # Metrics
    picks = out[out["entry"]]
    n_picks = len(picks)
    wins = int(picks["result"].sum()) if n_picks else 0
    hitrate = wins / n_picks if n_picks else 0.0
    roi = picks["pnl"].sum() / picks["stake"].sum() if n_picks and picks["stake"].sum() > 0 else 0.0
    end_bankroll = float(out["bankroll"].iloc[-1]) if len(out) else args.bankroll
    max_dd = 0.0
    if n_picks:
        equity = picks["bankroll"].tolist()
        peak = equity[0] if equity else args.bankroll
        for v in equity:
            peak = max(peak, v)
            dd = (peak - v) / peak if peak > 0 else 0.0
            max_dd = max(max_dd, dd)

    # Summary MD
    md = []
    md.append(f"# TE8 Backtest Summary")
    md.append("")
    md.append(f"- Period: **{args.start} → {args.end}**")
    md.append(f"- TE8 thresholds: **dogs={args.te8_dog:.2f}**, **favs={args.te8_fav:.2f}**")
    md.append(f"- Bands: **dogs={args.dog_band}**, **favs={args.fav_band}**")
    md.append(f"- Bankroll start: **€{args.bankroll:,.2f}**")
    md.append(f"- Stake unit cap: **€{args.stake_unit:,.2f}**")
    md.append(f"- Dog micro-cap × Kelly: **{args.dog_cap:.2f}x**")
    md.append("")
    md.append(f"## Results")
    md.append(f"- Picks: **{n_picks}** | Wins: **{wins}** | Hit rate: **{hitrate:.1%}**")
    md.append(f"- ROI on staked: **{roi:.2%}**")
    md.append(f"- Ending bankroll: **€{end_bankroll:,.2f}**")
    md.append(f"- Max drawdown: **{max_dd:.1%}**")
    md.append("")
    md.append(f"## Tuners")
    md.append(f"- Surface boost: **{args.surface_boost:.2f}**")
    md.append(f"- Recent form weight: **{args.recent_form_weight:.2f}**")
    md.append(f"- Injury penalty: **{args.injury_penalty:.2f}**")
    md.append("")
    md.append(f"_Data: {Path(args.input).as_posix()} | Output: {Path(args.out_csv).as_posix()}_")

    Path(args.summary).write_text("\n".join(md), encoding="utf-8")

    print(f"Wrote {args.out_csv} and {args.summary}")


if __name__ == "__main__":
    main()
