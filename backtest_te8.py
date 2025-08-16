#!/usr/bin/env python3
import argparse, itertools, re
from pathlib import Path
import pandas as pd

pd.options.mode.copy_on_write = True

def parse_bands(s):
    def pair(key):
        m = re.search(rf"{key}\s*=\s*([^;]+)", s or "", flags=re.I)
        if not m: return None
        a,b = m.group(1).split(",")
        return float(a), float(b)
    return pair("dog") or (2.2,4.5), pair("fav") or (1.15,2.0)

def elo_to_prob(a,b): return 1/(1+10**(-(a-b)/400))
def kelly(p,o): 
    if o<=1: return 0.0
    v=(p*o-1)/(o-1)
    return max(0.0, v)

def clean_df(df):
    need = ["date","player","opponent","odds","elo_player","elo_opponent","result"]
    miss = [c for c in need if c not in df.columns]
    if miss: raise ValueError(f"Input CSV missing columns: {miss}")
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for c in ("odds","opp_odds","elo_player","elo_opponent","result"):
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["date","odds","elo_player","elo_opponent","result"])

def run_once(df, bands, te8_dog, te8_fav, min_edge, k_cap, max_risk, start, end, bankroll0):
    dog_band, fav_band = bands
    w = df[(df["date"]>=start) & (df["date"]<=end)].copy()
    rows=[]; bankroll=float(bankroll0)
    for _,r in w.iterrows():
        p = elo_to_prob(r["elo_player"], r["elo_opponent"])
        o = float(r["odds"])
        if o<=1: continue
        is_dog = o>=2.0
        ok = (is_dog and (1-p)>=te8_dog) or ((not is_dog) and p>=te8_fav)
        if not ok: continue
        if is_dog and not (dog_band[0]<=o<=dog_band[1]): continue
        if (not is_dog) and not (fav_band[0]<=o<=fav_band[1]): continue
        edge = p - 1.0/o
        if edge < min_edge: continue
        k_full = kelly(p,o)
        k_used = min(k_full, k_cap, max_risk)
        stake = bankroll * k_used
        if stake < 1.0: continue
        win = int(r["result"])==1
        pnl = stake*(o-1) if win else -stake
        bankroll += pnl
        rows.append({
            "date": r["date"].date(), "player": r["player"], "opponent": r["opponent"],
            "odds": round(o,2), "model_prob": round(p,4), "edge": round(edge,4),
            "stake": round(stake,2), "result": int(win), "pnl": round(pnl,2), "bankroll": round(bankroll,2)
        })
    log = pd.DataFrame(rows)
    picks = len(log)
    wins = int(log["result"].sum()) if picks else 0
    roi = (log["pnl"].sum()/log["stake"].sum()) if picks and log["stake"].sum()>0 else 0.0
    hit = (wins/picks) if picks else 0.0
    return {"picks":picks,"wins":wins,"roi":roi,"hit":hit,"bankroll":bankroll,"log":log}

def parse_grid(s):
    if not s: return {}
    out={}
    for part in s.split(";"):
        if "=" not in part: continue
        k,vals = part.split("=",1)
        vs=[float(x.strip()) for x in vals.split(",") if x.strip()]
        if vs: out[k.strip()] = vs
    return out

def safe_write_summary(path, text):
    Path(path).write_text(text)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--start", default="2021-01-01")
    ap.add_argument("--end", default="2025-12-31")
    ap.add_argument("--bands", default="dog=2.20,4.50;fav=1.15,2.00")
    ap.add_argument("--te8-dog", type=float, default=0.60)
    ap.add_argument("--te8-fav", type=float, default=0.50)
    ap.add_argument("--min-edge", type=float, default=0.03)
    ap.add_argument("--kelly-cap", type=float, default=0.25)
    ap.add_argument("--max-risk", type=float, default=0.05)
    ap.add_argument("--bankroll", type=float, default=1000.0)
    ap.add_argument("--out-csv", default="backtest_results.csv")
    ap.add_argument("--summary", default="backtest_summary.md")
    ap.add_argument("--grid", default="")
    args = ap.parse_args()

    p = Path(args.input)
    if not p.exists() or p.stat().st_size==0:
        safe_write_summary(args.summary, f"# TE8 Backtest Summary\n\nNo dataset at `{args.input}` (file missing or empty).\n")
        Path(args.out_csv).write_text("")
        print("No dataset — exiting cleanly.")
        return

    try:
        raw = pd.read_csv(p)
    except Exception as e:
        safe_write_summary(args.summary, f"# TE8 Backtest Summary\n\nFailed to read `{args.input}`: {e}\n")
        Path(args.out_csv).write_text("")
        print("Read error — exiting cleanly.")
        return

    if raw.empty:
        safe_write_summary(args.summary, "# TE8 Backtest Summary\n\nInput has headers but **0 rows**.\n")
        Path(args.out_csv).write_text("")
        print("0 rows — exiting cleanly.")
        return

    try:
        df = clean_df(raw)
    except Exception as e:
        safe_write_summary(args.summary, f"# TE8 Backtest Summary\n\nInvalid dataset: {e}\n")
        Path(args.out_csv).write_text("")
        print("Invalid dataset — exiting cleanly.")
        return

    start = pd.to_datetime(args.start); end = pd.to_datetime(args.end)
    bands = parse_bands(args.bands)

    grid = parse_grid(args.grid)
    if grid:
        keys = sorted(grid.keys())
        combos = list(itertools.product(*[grid[k] for k in keys]))
        rows=[]; best=None; best_res=None
        for combo in combos:
            kv=dict(zip(keys,combo))
            res=run_once(df,bands, kv.get("te8_dog",args.te8_dog), kv.get("te8_fav",args.te8_fav),
                         kv.get("min_edge",args.min_edge), kv.get("kelly_cap",args.kelly_cap),
                         kv.get("max_risk",args.max_risk), start,end,args.bankroll)
            rows.append({**kv,"picks":res["picks"],"wins":res["wins"],
                         "hit":round(res["hit"],4),"roi":round(res["roi"],4),
                         "bankroll_end":round(res["bankroll"],2)})
            if best_res is None or res["roi"]>best_res["roi"]:
                best, best_res = kv, res
        pd.DataFrame(rows).sort_values(["roi","hit","picks"], ascending=[False,False,False]).to_csv("grid_results.csv", index=False)
        (best_res["log"] if best_res["log"] is not None else pd.DataFrame()).to_csv(args.out_csv, index=False)
        md=[ "# TE8 Backtest Summary (Grid)",
             f"- Window: **{args.start} → {args.end}**",
             f"- Tested combos: **{len(rows)}**",
             f"- Best: **{best}**",
             f"- Picks: **{best_res['picks']}**, Wins: **{best_res['wins']}**, Hit: **{best_res['hit']*100:.2f}%**",
             f"- ROI: **{best_res['roi']*100:.2f}%**, Ending bankroll: **€{best_res['bankroll']:,.2f}** (start €{args.bankroll:,.2f})" ]
        safe_write_summary(args.summary, "\n".join(md))
        print("Grid backtest complete.")
        return

    # single run
    res=run_once(df,bands,args.te8_dog,args.te8_fav,args.min_edge,args.kelly_cap,args.max_risk,start,end,args.bankroll)
    res["log"].to_csv(args.out_csv, index=False)
    md=[ "# TE8 Backtest Summary",
         f"- Window: **{args.start} → {args.end}**",
         f"- Picks: **{res['picks']}**, Wins: **{res['wins']}**, Hit: **{res['hit']*100:.2f}%**",
         f"- ROI: **{res['roi']*100:.2f}%**",
         f"- Ending bankroll: **€{res['bankroll']:,.2f}** (start €{args.bankroll:,.2f})" ]
    safe_write_summary(args.summary, "\n".join(md))
    print("Backtest complete.")

if __name__ == "__main__":
    main()
