#!/usr/bin/env python3
import argparse, json, math, sys, os
from pathlib import Path
import pandas as pd
import numpy as np

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
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

def write_zero_summary(a, reason):
    Path(a.out_csv).write_text("", encoding="utf-8")
    lines = [
        "# TE8 Backtest Summary",
        "",
        f"- Input file: `{a.input}`",
        f"- Window: **{a.start} → {a.end}**",
        "",
        f"**No data to backtest:** {reason}.",
        "",
        "Tips:",
        "- Check the build summary (were any rows linked?).",
        "- If 0 linked rows: verify odds headers, names, and ±7-day date window.",
        "- Otherwise, lower thresholds or widen bands.",
    ]
    Path(a.summary).write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))

def odds_to_prob(o): return np.nan if (pd.isna(o) or o <= 1.0) else 1.0/o
def kelly_fraction(p,b): 
    if not (0<p<1) or b<=0: return 0.0
    k=(b*p-(1-p))/b
    return max(0.0,float(k))
def parse_band(s):
    try: lo,hi=s.split(","); return float(lo),float(hi)
    except: return (0.0,99.0)

def surface_hint(s):
    s=(s or "").strip().lower()
    if s in ("clay","cl"): return "clay"
    if s in ("grass","gr"): return "grass"
    if s in ("hard","hardcourt","hc","carpet"): return "hard"
    return "unknown"

def surface_boost_factor(surf, w):
    s=surface_hint(surf)
    return {"clay":0.5*w,"grass":0.3*w,"hard":0.4*w}.get(s,0.0)

def recent_form_boost(wr, w): 
    return 0.0 if pd.isna(wr) else w * (wr-0.5)*2.0

def te8_score(base_price_edge, elo_edge_bp, surf_boost, form_boost, injury_pen):
    x=0.45*base_price_edge + 0.35*elo_edge_bp + 0.10*surf_boost + 0.10*form_boost - injury_pen
    return float(1.0/(1.0+math.exp(-3.5*x)))

def load_injuries(path):
    if not path: return []
    p=Path(path)
    if not p.exists(): return []
    try:
        data=json.loads(p.read_text())
        out=[]
        for r in data:
            try:
                out.append({
                    "player": str(r["player"]).strip().lower(),
                    "start": pd.to_datetime(r["start_date"]).normalize(),
                    "end":   pd.to_datetime(r["end_date"]).normalize(),
                    "impact": float(r.get("impact",0.15))
                })
            except: pass
        return out
    except: return []

def player_injured(inj, name, date):
    nm=str(name).strip().lower()
    for r in inj:
        if r["player"]==nm and r["start"]<=date<=r["end"]:
            return True, r["impact"]
    return False,0.0

def main():
    a=parse_args()

    # Guard: input must exist and be non-empty
    if not Path(a.input).exists() or os.path.getsize(a.input) == 0:
        write_zero_summary(a, "input file missing or empty (0 bytes)")
        return

    # Read safely (handle empty-without-headers)
    try:
        df=pd.read_csv(a.input)
    except pd.errors.EmptyDataError:
        write_zero_summary(a, "input CSV had no header/columns")
        return

    if df.empty:
        write_zero_summary(a, "input CSV had headers but zero rows")
        return

    needed=["date","tour","player","opponent","odds","result"]
    missing=[c for c in needed if c not in df.columns]
    if missing:
        write_zero_summary(a, f"dataset missing columns: {missing}")
        return

    # Parse window
    df["date"]=pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df=df.dropna(subset=["date"]).copy()
    start=pd.to_datetime(a.start); end=pd.to_datetime(a.end)
    df=df[(df["date"]>=start)&(df["date"]<=end)].copy()
    if df.empty:
        write_zero_summary(a, "no rows in the selected date window")
        return

    dog_lo,dog_hi=parse_band(a.dog_band)
    fav_lo,fav_hi=parse_band(a.fav_band)

    df["odds"]=pd.to_numeric(df["odds"], errors="coerce")
    df=df.dropna(subset=["odds"]).copy()
    df["implied_prob"]=df["odds"].apply(odds_to_prob)

    for col in ("elo_player","elo_opponent"):
        if col not in df.columns: df[col]=1500.0

    # rolling form
    df=df.sort_values(["date","tour","player"]).reset_index(drop=True)
    N=10
    form_map={}; rates=[]
    for _,r in df.iterrows():
        key=(r["tour"], r["player"])
        hist=form_map.get(key,[])
        rates.append(np.mean(hist) if hist else np.nan)
        try:
            val=int(pd.to_numeric(r.get("result"), errors="coerce"))
        except: val=0
        hist=(hist+[val])[-N:]; form_map[key]=hist
    df["recent_form_wr"]=rates

    elo_diff=df["elo_player"].astype(float)-df["elo_opponent"].astype(float)
    df["p_elo"]=1.0/(1.0+10**(-(elo_diff/400.0)))
    df["price_edge"]=(df["p_elo"]-df["implied_prob"])/df["implied_prob"].clip(lower=1e-6)
    df["elo_edge_bp"]=(elo_diff/200.0).clip(-2,2)/2.0

    surface_series=df["surface"].astype(str) if "surface" in df.columns else pd.Series([""]*len(df), index=df.index)
    df["surf_boost"]=surface_series.map(lambda s: surface_boost_factor(s, a.surface_boost))
    df["form_boost"]=df["recent_form_wr"].apply(lambda wr: recent_form_boost(wr, a.recent_form_weight))

    injuries=load_injuries(a.injuries)
    pen=[]
    for _,r in df.iterrows():
        inj,imp=player_injured(injuries, r["player"], r["date"])
        pen.append(a.injury_penalty*imp if inj else 0.0)
    df["inj_pen"]=pen

    def te8_row(r):
        return te8_score(
            r.get("price_edge",0.0) or 0.0,
            r.get("elo_edge_bp",0.0) or 0.0,
            r.get("surf_boost",0.0) or 0.0,
            r.get("form_boost",0.0) or 0.0,
            r.get("inj_pen",0.0) or 0.0,
        )
    df["te8"]=df.apply(te8_row, axis=1)

    df["is_dog"]=df["odds"]>=2.00
    dog_mask=(df["odds"]>=dog_lo)&(df["odds"]<=dog_hi)
    fav_mask=(df["odds"]>=fav_lo)&(df["odds"]<=fav_hi)

    df["entry"]=False
    df.loc[df["is_dog"]&dog_mask&(df["te8"]>=a.te8_dog),"entry"]=True
    df.loc[(~df["is_dog"])&fav_mask&(df["te8"]>=a.te8_fav),"entry"]=True

    bankroll=float(a.bankroll)
    df["result"]=pd.to_numeric(df["result"], errors="coerce").fillna(0).astype(int)
    stakes=[]; kfracs=[]; pnls=[]; eq=[]
    for _,r in df.iterrows():
        if not bool(r["entry"]):
            stakes.append(0.0); kfracs.append(0.0); pnls.append(0.0); eq.append(bankroll); continue
        p=float(r["te8"]); b=float(r["odds"])-1.0
        k=kelly_fraction(p,b)
        if r["is_dog"]: k*=a.dog_cap
        stake=min(bankroll*k, a.stake_unit)
        pnl=(r["odds"]-1.0)*stake if r["result"]==1 else -stake
        bankroll+=pnl
        stakes.append(stake); kfracs.append(k); pnls.append(pnl); eq.append(bankroll)
    df["kelly_fraction"]=kfracs; df["stake"]=stakes; df["pnl"]=pnls; df["bankroll"]=eq

    out_cols=["date","tour","tournament","round","player","opponent","odds","implied_prob","p_elo",
              "price_edge","elo_player","elo_opponent","recent_form_wr","surf_boost","inj_pen",
              "te8","is_dog","entry","kelly_fraction","stake","result","pnl","bankroll","source"]
    out=df[[c for c in out_cols if c in df.columns]].copy().sort_values(["date","tour","player"])
    Path(a.out_csv).write_text(out.to_csv(index=False), encoding="utf-8")

    picks=out[out["entry"]] if "entry" in out.columns else pd.DataFrame()
    n=len(picks); wins=int(picks["result"].sum()) if n else 0
    hit=wins/n if n else 0.0
    roi=(picks["pnl"].sum()/picks["stake"].sum()) if n and picks["stake"].sum()>0 else 0.0
    end_eq=float(out["bankroll"].iloc[-1]) if len(out) and "bankroll" in out.columns else float(a.bankroll)

    md=[]
    md.append("# TE8 Backtest Summary")
    md.append(f"- Rows in window **{a.start} → {a.end}**: **{len(df)}**")
    md.append(f"- TE8 thresholds: dogs={a.te8_dog:.2f}, favs={a.te8_fav:.2f} | bands: dogs={a.dog_band}, favs={a.fav_band}")
    if n==0:
        md.append("> 0 picks. Reasons: (a) thresholds/bands too strict, (b) dataset has no qualifying odds.")
    else:
        md.append(f"- Picks: **{n}** | Wins: **{wins}** | Hit rate: **{hit:.1%}** | ROI: **{roi:.2%}**")
        md.append(f"- Ending bankroll: **€{end_eq:,.2f}** (start €{float(a.bankroll):,.2f})")
    Path(a.summary).write_text("\n".join(md), encoding="utf-8")
    print(f"Wrote {a.out_csv} and {a.summary}")

if __name__ == "__main__":
    main()
