#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Grid-search MIN_EDGE × KELLY_SCALE using synthetic historical data.
Outputs: results/sweep_results.csv
"""
import argparse, os, numpy as np, pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--data", default="results/tennis_data.csv")
ap.add_argument("--outdir", default="results")
ap.add_argument("--edges", default="0.04,0.06,0.08,0.10")
ap.add_argument("--kellys", default="0.25,0.5,0.75,1.0")
ap.add_argument("--bankroll", type=float, default=1000.0)
args = ap.parse_args()

os.makedirs(args.outdir, exist_ok=True)
df = pd.read_csv(args.data)
if "odds" not in df.columns and "price" in df.columns:
    df["odds"] = df["price"]
if "p" not in df.columns and "p_model" in df.columns:
    df["p"] = df["p_model"]

edges = [float(x) for x in args.edges.split(",")]
kellys = [float(x) for x in args.kellys.split(",")]

def kelly_fraction(odds, p):
    b = odds - 1.0
    q = 1 - p
    if b <= 0: return 0.0
    return max(0.0, min(1.0, (b*p - q) / b))

results = []
for e in edges:
    # pick set
    implied = 1/df["odds"]
    picks = df[df["p"] - implied >= e].copy()
    if picks.empty:
        results.append({"edge":e,"kelly":None,"n_bets":0,"roi":0,"final":args.bankroll,"max_dd":0,"score":-1})
        continue
    # simulate once per Kelly
    for k in kellys:
        bank = args.bankroll
        peak = bank
        pnl = []
        for _, r in picks.iterrows():
            f = kelly_fraction(r["odds"], r["p"]) * k
            f = min(max(f, 0.0), 1.0)
            stake = bank * f
            res = int(r["result"]) if "result" in r and str(r["result"]).strip() != "" else int(np.random.random() < r["p"])
            if res == 1:
                bank += stake * (r["odds"] - 1.0)
                pnl.append(stake * (r["odds"] - 1.0))
            else:
                bank -= stake
                pnl.append(-stake)
            peak = max(peak, bank)
        roi = (bank - args.bankroll)/max(args.bankroll,1)
        max_dd = (peak - bank)/max(peak,1)
        vol = np.std(pnl) if pnl else 0.0
        score = roi / (vol+1e-9)  # crude risk-adjusted score
        results.append({"edge":e,"kelly":k,"n_bets":len(picks),"roi":roi,"final":bank,"max_dd":max_dd,"score":score})

res = pd.DataFrame(results).sort_values(["score","roi"], ascending=[False,False])
res["roi_pct"] = (res["roi"]*100).round(2)
res["max_dd_pct"] = (res["max_dd"]*100).round(2)
out = os.path.join(args.outdir, "sweep_results.csv")
res.to_csv(out, index=False)
print(f"Wrote sweep grid -> {out}")
if not res.empty:
    print(res.head(10).to_string(index=False))
