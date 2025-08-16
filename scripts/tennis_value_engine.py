#!/usr/bin/env python3
"""
Daily decision engine: read candidate picks (value_picks_pro.csv),
blend with Elo, filter by edge, size with Kelly cap, output final list.

Input CSV (flexible): columns like date,player,opponent,odds,model_conf(optional)
Outputs:
  - outputs/picks_final.csv
  - outputs/engine_summary.md
"""
import argparse
from pathlib import Path
import pandas as pd

def elo_prob_map(elo_csv):
    if not Path(elo_csv).exists(): return {}
    df = pd.read_csv(elo_csv)
    return {str(r["player"]).lower(): float(r["elo"]) for _,r in df.iterrows()}

def prob_from_elo(pa, pb, elo_map):
    ea = elo_map.get(str(pa).lower(), 1500.0)
    eb = elo_map.get(str(pb).lower(), 1500.0)
    return 1.0/(1.0+10**(-(ea-eb)/400.0))

def kelly(p, o): 
    return max(0.0, (p*o-1)/(o-1)) if o>1 else 0.0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="value_picks_pro.csv")
    ap.add_argument("--elo-atp", default="data/atp_elo.csv")
    ap.add_argument("--elo-wta", default="data/wta_elo.csv")
    ap.add_argument("--min-edge", type=float, default=0.03)
    ap.add_argument("--kelly-cap", type=float, default=0.20)
    ap.add_argument("--max-risk", type=float, default=0.05)
    ap.add_argument("--bankroll", type=float, default=1000.0)
    ap.add_argument("--max-picks", type=int, default=20)
    args = ap.parse_args()

    df = pd.read_csv(args.input) if Path(args.input).exists() else pd.DataFrame(columns=["date","player","opponent","odds"])
    if df.empty:
        Path("outputs").mkdir(parents=True, exist_ok=True)
        Path("outputs/engine_summary.md").write_text("# Engine summary\n_No picks._")
        df.to_csv("outputs/picks_final.csv", index=False)
        return

    elo_map = elo_prob_map(args.elo_atp) | elo_prob_map(args.elo_wta)

    out=[]
    bankroll = float(args.bankroll)
    for _,r in df.iterrows():
        p = float(r.get("model_conf", "nan"))
        if not (0.0 < p < 1.0):
            p = prob_from_elo(r["player"], r["opponent"], elo_map)  # fallback

        o = float(r["odds"])
        implied = 1.0/o if o>0 else 1.0
        edge = p - implied
        if edge < args.min_edge: 
            continue

        k_full = kelly(p, o)
        k_used = min(k_full, args.kelly_cap, args.max_risk)
        stake = bankroll * k_used
        if stake < 1.0: 
            continue

        out.append({
            "date": r.get("date",""),
            "player": r["player"], "opponent": r["opponent"],
            "odds": round(o,2), "model_prob": round(p,4), "edge": round(edge,4),
            "kelly_used": round(k_used,4), "stake": round(stake,2)
        })

    picks = pd.DataFrame(out)
    picks.sort_values(["date","edge","odds"], ascending=[True, False, True], inplace=True)
    picks = picks.head(args.max_picks)

    Path("outputs").mkdir(parents=True, exist_ok=True)
    picks.to_csv("outputs/picks_final.csv", index=False)

    md=[]
    md.append("# Engine summary")
    md.append(f"- Min edge: **{args.min_edge:.2%}**")
    md.append(f"- Kelly cap: **{args.kelly_cap:.2%}**, Max risk: **{args.max_risk:.2%}**")
    md.append(f"- Max picks shown: **{args.max_picks}**")
    md.append(f"- Output rows: **{len(picks)}**")
    try:
        md.append("\n## Picks\n\n"+picks.head(25).to_markdown(index=False))
    except Exception:
        md.append("\n## Picks (CSV)\n\n"+picks.head(25).to_csv(index=False))
    Path("outputs/engine_summary.md").write_text("\n".join(md))

if __name__ == "__main__":
    main()
