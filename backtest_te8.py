#!/usr/bin/env python3
"""
Backtest TE8 + Kelly with optional grid (matrix) search.

Input: data/historical_matches.csv with columns (min):
  date,tour,tournament,round,player,opponent,odds,opp_odds,result,elo_player,elo_opponent,surface

Outputs:
  - backtest_results.csv    (per-pick log, for one run or the best grid cell)
  - backtest_summary.md     (human summary + optional matrix table)
  - grid_results.csv        (ROI/hit/picks for every grid combo, if --grid supplied)

Usage (single run):
  python backtest_te8.py --input data/historical_matches.csv \
    --elo-atp data/atp_elo.csv --elo-wta data/wta_elo.csv \
    --start 2021-01-01 --end 2024-12-31 \
    --bands "dog=2.20,4.50;fav=1.15,2.00" \
    --te8-dog 0.60 --te8-fav 0.50 \
    --min-edge 0.03 --kelly-cap 0.25 --max-risk 0.05 \
    --bankroll 1000 --out-csv backtest_results.csv --summary backtest_summary.md

Usage (grid search / matrix):
  python backtest_te8.py ... \
    --grid "min_edge=0.02,0.03,0.04;kelly_cap=0.10,0.20,0.25;te8_dog=0.58,0.60;te8_fav=0.48,0.50"
"""
import argparse, re, itertools, math
from pathlib import Path
import pandas as pd
pd.options.mode.copy_on_write = True

def parse_bands(s):
    # "dog=2.20,4.50;fav=1.15,2.00"
    def pair(txt, key):
        m = re.search(rf"{key}\s*=\s*([^;]+)", s or "", flags=re.I)
        if not m: return None
        a,b = m.group(1).split(",")
        return (float(a), float(b))
    dog = pair(s, "dog") or (2.2, 4.5)
    fav = pair(s, "fav") or (1.15, 2.0)
    return dog, fav

def elo_to_prob(elo_a, elo_b):
    # Standard Elo logistic
    return 1.0 / (1.0 + 10 ** (-(elo_a - elo_b) / 400.0))

def kelly_fraction(p, o):
    # Full Kelly for decimal odds
    if o <= 1.0: return 0.0
    val = (p * o - 1.0) / (o - 1.0)
    return max(0.0, val)

def clean_df(df):
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for c in ("odds","opp_odds","elo_player","elo_opponent"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["date","odds","elo_player","elo_opponent","result"])
    return df

def run_once(df, bands, te8_dog, te8_fav, min_edge, kelly_cap, max_risk,
             start, end, bankroll_start):
    dog_band, fav_band = bands
    w = df[(df["date"]>=start) & (df["date"]<=end)].copy()
    if w.empty:
        return {"picks":0,"wins":0,"roi":0.0,"hit":0.0,"bankroll":bankroll_start,"log":pd.DataFrame()}

    rows=[]
    bankroll = float(bankroll_start)

    for _,r in w.iterrows():
        p_model = elo_to_prob(r["elo_player"], r["elo_opponent"])  # proxy for TE8
        odds = float(r["odds"])

        # Dog/Fav gating by TE8-like thresholds
        is_dog = odds >= 2.0
        ok_te8 = (is_dog and (1.0 - p_model) >= te8_dog) or (not is_dog and p_model >= te8_fav)
        if not ok_te8: 
            continue

        # Price bands
        if is_dog and not (dog_band[0] <= odds <= dog_band[1]): 
            continue
        if (not is_dog) and not (fav_band[0] <= odds <= fav_band[1]): 
            continue

        implied = 1.0/odds
        edge = p_model - implied
        if edge < min_edge: 
            continue

        k_full = kelly_fraction(p_model, odds)
        k_used = min(k_full, kelly_cap, max_risk)
        stake = bankroll * k_used
        if stake < 1.0: 
            continue

        win = int(r["result"]==1)
        pnl = stake*(odds-1.0) if win else -stake
        bankroll += pnl

        rows.append({
            "date": r["date"].date(),
            "tour": r.get("tour",""),
            "tournament": r.get("tournament",""),
            "round": r.get("round",""),
            "player": r["player"],
            "opponent": r["opponent"],
            "odds": round(odds,2),
            "is_dog": is_dog,
            "model_prob": round(p_model,4),
            "implied_prob": round(implied,4),
            "edge": round(edge,4),
            "kelly_full": round(k_full,4),
            "kelly_used": round(k_used,4),
            "stake": round(stake,2),
            "result": win,
            "pnl": round(pnl,2),
            "bankroll": round(bankroll,2),
        })

    log = pd.DataFrame(rows)
    picks = len(log)
    wins = int(log["result"].sum()) if picks else 0
    roi = (log["pnl"].sum()/log["stake"].sum()) if picks and log["stake"].sum()>0 else 0.0
    hit = (wins/picks) if picks else 0.0
    return {"picks":picks,"wins":wins,"roi":roi,"hit":hit,"bankroll":bankroll,"log":log}

def parse_grid(s):
    # "min_edge=0.02,0.03;kelly_cap=0.1,0.2;te8_dog=0.58,0.60;te8_fav=0.48,0.50"
    if not s: return {}
    out={}
    for part in s.split(";"):
        if "=" not in part: continue
        k,vals = part.split("=",1)
        vs=[float(x.strip()) for x in vals.split(",") if x.strip()]
        if vs: out[k.strip()] = vs
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--elo-atp", default="data/atp_elo.csv")
    ap.add_argument("--elo-wta", default="data/wta_elo.csv")
    ap.add_argument("--start", default="2021-01-01")
    ap.add_argument("--end",   default="2024-12-31")
    ap.add_argument("--bands", default="dog=2.20,4.50;fav=1.15,2.00")
    ap.add_argument("--te8-dog", type=float, default=0.60)
    ap.add_argument("--te8-fav", type=float, default=0.50)
    ap.add_argument("--min-edge", type=float, default=0.03)
    ap.add_argument("--kelly-cap", type=float, default=0.25)
    ap.add_argument("--max-risk", type=float, default=0.05)
    ap.add_argument("--bankroll", type=float, default=1000.0)
    ap.add_argument("--out-csv", default="backtest_results.csv")
    ap.add_argument("--summary", default="backtest_summary.md")
    ap.add_argument("--grid", default="", help="semicolon lists, e.g. min_edge=0.02,0.03;kelly_cap=0.1,0.2")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    df = clean_df(df)

    start=pd.to_datetime(args.start); end=pd.to_datetime(args.end)
    bands = parse_bands(args.bands)

    grid = parse_grid(args.grid)
    if grid:
        keys = sorted(grid.keys())
        combos = list(itertools.product(*[grid[k] for k in keys]))
        rows=[]
        best=None; best_res=None
        for combo in combos:
            kv = dict(zip(keys, combo))
            res = run_once(
                df=df,
                bands=bands,
                te8_dog=kv.get("te8_dog", args.te8_dog),
                te8_fav=kv.get("te8_fav", args.te8_fav),
                min_edge=kv.get("min_edge", args.min_edge),
                kelly_cap=kv.get("kelly_cap", args.kelly_cap),
                max_risk=kv.get("max_risk", args.max_risk),
                start=start, end=end, bankroll_start=args.bankroll
            )
            rows.append({
                **{k:kv[k] for k in keys},
                "picks":res["picks"], "wins":res["wins"],
                "hit":round(res["hit"],4), "roi":round(res["roi"],4),
                "bankroll_end": round(res["bankroll"],2)
            })
            if best_res is None or res["roi"]>best_res["roi"]:
                best_res, best = res, kv

        grid_df = pd.DataFrame(rows)
        grid_df.sort_values(["roi","hit","picks"], ascending=[False,False,False], inplace=True)
        grid_df.to_csv("grid_results.csv", index=False)

        # Save best run log as main outputs
        (best_res["log"] if best_res["log"] is not None else pd.DataFrame()).to_csv(args.out_csv, index=False)

        # Markdown summary with top-10 grid
        md = []
        md.append("# TE8 Backtest Summary (Grid)\n")
        md.append(f"- Window: **{args.start} → {args.end}**")
        md.append(f"- Price bands: **dogs {bands[0][0]}–{bands[0][1]}**, **favs {bands[1][0]}–{bands[1][1]}**")
        md.append(f"- Tested combos: **{len(grid_df)}**")
        md.append("\n## Top 10 parameter combos\n")
        try:
            md.append(grid_df.head(10).to_markdown(index=False))
        except Exception:
            md.append(grid_df.head(10).to_csv(index=False))
        md.append("\n## Best combo details\n")
        md.append(f"- Params: **{best}**")
        md.append(f"- Picks: **{best_res['picks']}**, Wins: **{best_res['wins']}**, Hit: **{best_res['hit']*100:.2f}%**")
        md.append(f"- ROI: **{best_res['roi']*100:.2f}%**, Ending bankroll: **€{best_res['bankroll']:,.2f}** (start €{args.bankroll:,.2f})")
        Path(args.summary).write_text("\n".join(md))
        print("Wrote grid_results.csv and best-run outputs.")
        return

    # Single run path
    res = run_once(df, bands, args.te8_dog, args.te8_fav, args.min_edge,
                   args.kelly_cap, args.max_risk, start, end, args.bankroll)
    res["log"].to_csv(args.out_csv, index=False)
    md=[]
    md.append("# TE8 Backtest Summary\n")
    md.append(f"- Window: **{args.start} → {args.end}**")
    md.append(f"- Picks: **{res['picks']}**, Wins: **{res['wins']}**, Hit: **{res['hit']*100:.2f}%**")
    md.append(f"- ROI: **{res['roi']*100:.2f}%**")
    md.append(f"- Ending bankroll: **€{res['bankroll']:,.2f}** (start €{args.bankroll:,.2f})")
    Path(args.summary).write_text("\n".join(md))
    print("Backtest complete.")

if __name__ == "__main__":
    main()
