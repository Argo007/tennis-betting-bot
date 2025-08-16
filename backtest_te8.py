#!/usr/bin/env python3
"""
TrueEdge8 backtest (Sackmann + local odds)
- Deterministic, CI-safe
- Chronological processing (no look-ahead)
- Robust to missing columns (e.g., surface)
"""

import argparse, json, math, sys
from pathlib import Path
import pandas as pd
import numpy as np


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="data/historical_matches.csv")
    p.add_argument("--elo-atp", default=None)
    p.add_argument("--elo-wta", default=None)
    p.add_argument("--injuries", default=None)

    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)

    p.add_argument("--te8-dog", type=float, required=True)
    p.add_argument("--te8-fav", type=float, required=True)

    p.add_argument("--dog-band", default="2.20,4.50")
    p.add_argument("--fav-band", default="1.15,2.00")

    p.add_argument("--dog-cap", type=float, default=0.25)
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
    if pd.isna(form_winrate):
        return 0.0
    return weight * (form_winrate - 0.5) * 2.0


def surface_boost_factor(surf, weight):
    s = surface_hint(surf)
    if s == "clay":
        return +weight * 0.5
    if s == "grass":
        return +weight * 0.3
    if s == "hard":
        return +weight * 0.4
    return 0.0


def te8_score(base_price_edge, elo_edge_bp, surf_boost, form_boost, injury_pen):
    x = (
        0.45 * base_price_edge +
        0.35 * elo_edge_bp +
        0.10 * surf_boost +
        0.10 * form_boost -
        injury_pen
    )
    return float(1.0 / (1.0 + math.exp(-3.5 * x)))


def main():
    args = parse_args()

    # Load
    try:
        df = pd.read_csv(args.input)
    except FileNotFoundError:
        print(f"::error ::input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    # Required minimal columns
    for c in ["date", "tour", "player", "opponent", "odds", "result"]:
        if c not in df.columns:
            print(f"::error ::Missing column '{c}' in {args.input}", file=sys.stderr)
            sys.exit(1)

    # Types and window
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["date"]).copy()
    start = pd.to_datetime(args.start)
    end = pd.to_datetime(args.end)
    df = df[(df["date"] >= start) & (df["date"] <= end)].copy()

    if df.empty:
        # write empty outputs and bail gracefully
        Path(args.out_csv).write_text("", encoding="utf-8")
        Path(args.summary).write_text(
            f"# TE8 Backtest Summary\n\n_No rows in date window {args.start} → {args.end}._\n",
            encoding="utf-8",
        )
        print(f"No rows to process for {args.start} → {args.end}.")
        return

    # Odds bands
    dog_lo, dog_hi = parse_band(args.dog_band)
    fav_lo, fav_hi = parse_band(args.fav_band)

    # Implied probs
    df["odds"] = pd.to_numeric(df["odds"], errors="coerce")
    df = df.dropna(subset=["odds"]).copy()
    df["implied_prob"] = df["odds"].apply(odds_to_prob)

    # Ensure Elo cols exist
    if "elo_player" not in df.columns:
        df["elo_player"] = 1500.0
    if "elo_opponent" not in df.columns:
        df["elo_opponent"] = 1500.0

    # Injuries
    injuries = load_injuries(args.injuries)

    # Recent form (rolling last N results for each player BEFORE the match)
    N_FORM = 10
    df = df.sort_values(["date", "tour", "player"]).reset_index(drop=True)
    form_map = {}
    form_rates = []
    for _, r in df.iterrows():
        key = (r["tour"], r["player"])
        hist = form_map.get(key, [])
        winrate = np.mean(hist) if hist else np.nan
        form_rates.append(winrate)
        # update with current result
        res_val = pd.to_numeric(pd.Series([r.get("result")]), errors="coerce").iloc[0]
        if pd.notna(res_val):
            hist = (hist + [int(res_val)])[-N_FORM:]
            form_map[key] = hist
    df["recent_form_wr"] = form_rates

    # Prob from Elo
    elo_diff = df["elo_player"].astype(float) - df["elo_opponent"].astype(float)
    df["p_elo"] = 1.0 / (1.0 + 10 ** (-(elo_diff / 400.0)))

    # Price edge
    df["price_edge"] = (df["p_elo"] - df["implied_prob"]) / df["implied_prob"].clip(lower=1e-6)

    # Elo edge scaled
    df["elo_edge_bp"] = (elo_diff / 200.0).clip(-2.0, 2.0) / 2.0

    # --- robust surface handling (this fixes your error) ---
    if "surface" in df.columns:
        surface_series = df["surface"].astype(str)
    else:
        surface_series = pd.Series([""] * len(df), index=df.index)
    df["surf_boost"] = surface_series.map(lambda s: surface_boost_factor(s, args.surface_boost))

    # Recent form boost
    df["form_boost"] = df["recent_form_wr"].apply(lambda wr: recent_form_boost(wr, args.recent_form_weight))

    # Injury penalty
    inj_pen = []
    for _, r in df.iterrows():
        inj, imp = player_injured(injuries, r["player"], r["date"])
        inj_pen.append(args.injury_penalty * imp if inj else 0.0)
    df["inj_pen"] = inj_pen

    # TE8
    df["te8"] = df.apply(
        lambda r: te8_score(
            base_price_edge=float(r.get("price_edge", 0.0) or 0.0),
            elo_edge_bp=float(r.get("elo_edge_bp", 0.0) or 0.0),
            surf_boost=float(r.get("surf_boost", 0.0) or 0.0),
            form_boost=float(r.get("form_boost", 0.0) or 0.0),
            injury_pen=float(r.get("inj_pen", 0.0) or 0.0),
        ),
        axis=1
    )

    # Dog/Fav flags + band filters
    df["is_dog"] = df["odds"] >= 2.00
    dog_mask = (df["odds"] >= dog_lo) & (df["odds"] <= dog_hi)
    fav_mask = (df["odds"] >= fav_lo) & (df["odds"] <= fav_hi)

    # Entries by threshold
    df["entry"] = False
    df.loc[df["is_dog"] & dog_mask & (df["te8"] >= args.te8_dog), "entry"] = True
    df.loc[(~df["is_dog"]) & fav_mask & (df["te8"] >= args.te8_fav), "entry"] = True

    # Kelly staking
    bankroll = float(args.bankroll)
    stakes = []
    kfracs = []
    profits = []
    bankrolls = []

    # ensure numeric result
    df["result"] = pd.to_numeric(df["result"], errors="coerce").fillna(0).astype(int)

    for _, r in df.iterrows():
        if not bool(r["entry"]):
            stakes.append(0.0); kfracs.append(0.0); profits.append(0.0); bankrolls.append(bankroll); continue
        p_est = float(r["te8"])
        b = float(r["odds"]) - 1.0
        k = kelly_fraction(p_est, b)
        if r["is_dog"]:
            k *= args.dog_cap
        stake = bankroll * k
        stake = min(stake, args.stake_unit)  # soft cap
        profit = (r["odds"] - 1.0) * stake if r["result"] == 1 else -stake
        bankroll += profit
        stakes.append(float(stake)); kfracs.append(float(k)); profits.append(float(profit)); bankrolls.append(float(bankroll))

    df["kelly_fraction"] = kfracs
    df["stake"] = stakes
    df["pnl"] = profits
    df["bankroll"] = bankrolls

    out_cols = [
        "date","tour","tournament","round","player","opponent",
        "odds","implied_prob","p_elo","price_edge","elo_player","elo_opponent",
        "recent_form_wr","surf_boost","inj_pen","te8","is_dog","entry",
        "kelly_fraction","stake","result","pnl","bankroll","source"
    ]
    existing = [c for c in out_cols if c in df.columns]
    out = df[existing].copy().sort_values(["date","tour","player"]).reset_index(drop=True)

    Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False)

    # Metrics
    picks = out[out["entry"]] if "entry" in out.columns else pd.DataFrame()
    n_picks = len(picks)
    wins = int(picks["result"].sum()) if n_picks and "result" in picks.columns else 0
    hitrate = wins / n_picks if n_picks else 0.0
    roi = picks["pnl"].sum() / picks["stake"].sum() if n_picks and picks["stake"].sum() > 0 else 0.0
    end_bankroll = float(out["bankroll"].iloc[-1]) if len(out) and "bankroll" in out.columns else float(args.bankroll)

    # Max DD on picks stream
    max_dd = 0.0
    if n_picks and "bankroll" in picks.columns:
        equity = picks["bankroll"].tolist()
        peak = equity[0] if equity else end_bankroll
        for v in equity:
            peak = max(peak, v)
            dd = (peak - v) / peak if peak > 0 else 0.0
            max_dd = max(max_dd, dd)

    # Summary
    md = []
    md.append(f"# TE8 Backtest Summary")
    md.append("")
    md.append(f"- Period: **{args.start} → {args.end}**")
    md.append(f"- TE8 thresholds: **dogs={args.te8_dog:.2f}**, **favs={args.te8_fav:.2f}**")
    md.append(f"- Bands: **dogs={args.dog_band}**, **favs={args.fav_band}**")
    md.append(f"- Bankroll start: **€{float(args.bankroll):,.2f}**")
    md.append(f"- Stake unit cap: **€{float(args.stake_unit):,.2f}**")
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
