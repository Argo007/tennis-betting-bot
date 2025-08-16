#!/usr/bin/env python3
"""
Daily decision engine: read candidate picks (value_picks_pro.csv),
fallback to Elo if model_conf missing, filter by edge, cap Kelly,
output final list.

Outputs:
  - outputs/picks_final.csv
  - outputs/engine_summary.md
"""
import argparse
from pathlib import Path
import pandas as pd

def elo_map(path):
    if not Path(path).exists(): return {}
    df = pd.read_csv(path)
    return {str(r["player"]).lower(): float(r["elo"]) for _,r in df.iterrows()}

def elo_prob(player, opponent, elo_atp, elo_wta):
    M = {**elo_atp, **elo_wta}
    a = M.get(str(player).lower(), 1500.0)
    b = M.get(str(opponent).lower(), 1500.0)
    return 1.0/(1.0+10**(-(a-b)/400.0))

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

    Path("outputs").mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(args.input) if Path(args.input).exists() else pd.DataFrame(columns=["date","player","opponent","odds"])

    elo_atp = elo_map(args.elo_atp)
    elo_wta = elo_map(args.elo_wta)

    out=[]
    bankroll = float(args.bankroll)
    for _,r in df.iterrows():
        o = float(r.get("odds", 0))
        if o <= 1.0: continue

        p = r.get("model_conf", None)
        try:
            p = float(p)
        except Exception:
            p = None
        if not (p and 0.0 < p < 1.0):
            p = elo_prob(r.get("player",""), r.get("opponent",""), elo_atp, elo_wta)

        implied = 1.0/o
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
            "player": r.get("player",""),
            "opponent": r.get("opponent",""),
            "odds": round(o,2),
            "model_prob": round(p,4),
            "edge": round(edge,4),
            "kelly_used": round(k_used,4),
            "stake": round(stake,2),
        })

    picks = pd.DataFrame(out)
    if not picks.empty:
        picks.sort_values(["date","edge","odds"], ascending=[True, False, True], inplace=True)
        picks = picks.head(args.max_picks)
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
