#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, io, unicodedata, requests, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# ---------- ENV ----------
API_KEY = os.getenv("ODDS_API_KEY", "").strip()
if not API_KEY:
    raise SystemExit("ODDS_API_KEY not set")

LOOKAHEAD_HOURS = int(os.getenv("LOOKAHEAD_HOURS", "24"))
REGIONS         = os.getenv("REGIONS", "eu,uk,us,au")
MARKETS         = os.getenv("MARKETS", "h2h,spreads,totals")

KELLY_MIN       = float(os.getenv("KELLY_MIN", "0.05"))
MODEL_EV_MIN    = float(os.getenv("MODEL_EV_MIN", "0.00"))
MARKET_EV_MIN   = float(os.getenv("MARKET_EV_MIN", "0.00"))

TOP_DOGS        = int(os.getenv("TOP_DOGS", "3"))
TOP_FAVS        = int(os.getenv("TOP_FAVS", "2"))

OUT_DIR         = Path(os.getenv("OUT_DIR", "outputs"))
SHORTLIST_FILE  = os.getenv("SHORTLIST_FILE", "value_engine_shortlist.md")
LOCAL_TZ        = ZoneInfo(os.getenv("TZ", "Europe/Amsterdam"))

OUT_DIR.mkdir(parents=True, exist_ok=True)

SPORT_KEYS = ["tennis_atp", "tennis_wta"]  # keep it simple & reliable

# ---------- Elo ----------
START_ELO, K = 1500, 32
def _exp(a,b): return 1/(1+10**((b-a)/400))
def _upd(a,b,s): return a + K*(s-_exp(a,b))

SACK_ATP = [
 "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_2023.csv",
 "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_2024.csv",
 "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_2025.csv",
]
SACK_WTA = [
 "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_matches_2023.csv",
 "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_matches_2024.csv",
 "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_matches_2025.csv",
]

def dl_csv(url):
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200 and r.text.strip():
            return pd.read_csv(io.StringIO(r.text))
    except requests.RequestException:
        pass
    return None

def build_elo_from_urls(urls):
    frames=[]
    for u in urls:
        df = dl_csv(u)
        if df is not None: frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["player","elo"])
    df = pd.concat(frames, ignore_index=True)
    E={}
    for _,r in df.iterrows():
        w,l = r.get("winner_name"), r.get("loser_name")
        if pd.isna(w) or pd.isna(l): continue
        ew, el = E.get(w,START_ELO), E.get(l,START_ELO)
        E[w] = _upd(ew,el,1); E[l] = _upd(el,ew,0)
    return pd.DataFrame([{"player":k,"elo":v} for k,v in E.items()])

def ensure_elo():
    Path("data").mkdir(exist_ok=True)
    need=[]
    if not Path("data/atp_elo.csv").exists(): need.append("ATP")
    if not Path("data/wta_elo.csv").exists(): need.append("WTA")
    if not need: return
    if "ATP" in need:
        atp = build_elo_from_urls(SACK_ATP)
        if not atp.empty: atp.sort_values("elo", ascending=False).to_csv("data/atp_elo.csv", index=False)
    if "WTA" in need:
        wta = build_elo_from_urls(SACK_WTA)
        if not wta.empty: wta.sort_values("elo", ascending=False).to_csv("data/wta_elo.csv", index=False)

def norm_name(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    keep = set("abcdefghijklmnopqrstuvwxyz -.'")
    s = "".join(ch for ch in s.lower() if ch in keep)
    return " ".join(s.split())

def load_elo_index(path):
    df = pd.read_csv(path) if Path(path).exists() else pd.DataFrame(columns=["player","elo"])
    return {norm_name(r["player"]): float(r["elo"]) for _,r in df.iterrows()}

def p_model(p1, p2, idx):
    e1 = idx.get(norm_name(p1))
    e2 = idx.get(norm_name(p2))
    if e1 is None or e2 is None: return None
    return _exp(e1, e2)

# ---------- Odds API ----------
def fetch_odds(sport_key: str):
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = dict(apiKey=API_KEY, regions=REGIONS, markets=MARKETS,
                  oddsFormat="decimal", dateFormat="iso")
    try:
        r = requests.get(url, params=params, timeout=25)
        if r.status_code == 200:
            j = r.json()
            return j if isinstance(j, list) else []
    except requests.RequestException:
        pass
    return []

def within_window(commence_iso: str) -> bool:
    if not commence_iso: return False
    start = datetime.fromisoformat(commence_iso.replace("Z","+00:00"))
    now = datetime.now(timezone.utc)
    return timedelta(0) <= (start-now) <= timedelta(hours=LOOKAHEAD_HOURS)

def fair_two_way(a: float, b: float):
    ia, ib = 1/a, 1/b
    s = ia+ib
    if s <= 0: return 0.5,0.5,0.5,0.5
    return ia, ib, ia/s, ib/s

def kelly(p: float, o: float):
    b = o-1.0
    return max(0.0, (b*p-(1-p))/b) if b>0 else 0.0

# ---------- Engine ----------
def shortlist_md(title: str, rows):
    if not rows: return f"## {title}\n_None_\n"
    lines=[f"## {title}"]
    for r in rows:
        lines.append(
            f"- {r['player']} vs {r['opponent']} — {r['display_odds']} "
            f"(p={r['p']:.2f}, Kelly={r['kelly']:.3f}, EV={r['ev']:+.2f}) — Source: {r['source']}\n"
            f"  🗓 {r['start_utc']}"
        )
    return "\n".join(lines) + "\n"

def run():
    ensure_elo()
    elo_atp = load_elo_index("data/atp_elo.csv")
    elo_wta = load_elo_index("data/wta_elo.csv")

    # fetch ATP+WTA
    events=[]
    for key in SPORT_KEYS:
        events += fetch_odds(key)

    model_cands, market_cands, diag = [], [], []

    for ev in events:
        ct = ev.get("commence_time")
        if not ct or not within_window(ct): 
            continue
        start_utc = datetime.fromisoformat(ct.replace("Z","+00:00"))
        start_utc_str = start_utc.strftime("%Y-%m-%d %H:%M UTC")

        sport_title = (ev.get("sport_title") or "").upper()
        if "ATP" in sport_title:
            tour="ATP"; elo_idx=elo_atp
        elif "WTA" in sport_title:
            tour="WTA"; elo_idx=elo_wta
        else:
            continue

        # --- H2H best prices across books
        best={}
        for bm in ev.get("bookmakers", []):
            bname = bm.get("title","?")
            for mk in bm.get("markets", []):
                if mk.get("key")!="h2h": continue
                for out in mk.get("outcomes", []):
                    name, price = out.get("name"), out.get("price")
                    if not name or price is None: continue
                    k = norm_name(name)
                    if k not in best or price > best[k]["price"]:
                        best[k] = {"name":name, "price":float(price), "books":{bname}}
                    elif abs(price-best[k]["price"])<1e-9:
                        best[k]["books"].add(bname)

        if len(best)==2:
            a,b = list(best.values())
            # Model vs market
            pA = p_model(a["name"], b["name"], elo_idx)
            pB = p_model(b["name"], a["name"], elo_idx)
            # Market no-vig (for diagnostics + optional Kelly H2H)
            _,_,pfA,pfB = fair_two_way(a["price"], b["price"])

            # record diagnostics even if model miss
            diag.append({
                "match": f"{a['name']} vs {b['name']}",
                "sport": tour,
                "start_utc": start_utc_str,
                "odds": f"{a['price']:.2f}/{b['price']:.2f}",
                "p_model": f"{(pA if pA is not None else float('nan')):.3f} / {(pB if pB is not None else float('nan')):.3f}",
                "p_fair": f"{pfA:.3f} / {pfB:.3f}"
            })

            if pA is not None and pB is not None:
                for name, opp, p, price in [
                    (a["name"], b["name"], pA, a["price"]),
                    (b["name"], a["name"], pB, b["price"]),
                ]:
                    evu = p*price - 1.0
                    kf  = kelly(p, price)
                    if evu >= MODEL_EV_MIN:
                        model_cands.append({
                            "tour": tour, "player": name, "opponent": opp,
                            "p": p, "odds": price, "display_odds": f"{price:.2f}",
                            "kelly": kf, "ev": evu, "source":"Elo",
                            "start_utc": start_utc_str
                        })

        # --- Totals (market edges)
        lines={}
        for bm in ev.get("bookmakers", []):
            bname=bm.get("title","?")
            for mk in bm.get("markets", []):
                if mk.get("key")!="totals": continue
                for out in mk.get("outcomes", []):
                    side, pts, price = out.get("name"), out.get("point"), out.get("price")
                    if side not in ("Over","Under") or pts is None or price is None: continue
                    lines.setdefault(float(pts), {})
                    rec = lines[float(pts)].get(side)
                    if rec is None or price>rec["price"]:
                        lines[float(pts)][side]={"price":float(price), "books":{bname}}
                    elif abs(price-rec["price"])<1e-9:
                        lines[float(pts)][side]["books"].add(bname)
        for pts,sides in lines.items():
            if "Over" in sides and "Under" in sides:
                po,pu = sides["Over"]["price"], sides["Under"]["price"]
                _,_,pfO,pfU = fair_two_way(po,pu)
                for side,price,p,cell in [("Over",po,pfO,sides["Over"]),("Under",pu,pfU,sides["Under"])]:
                    evu = p*price - 1.0; kf = kelly(p, price)
                    if evu >= MARKET_EV_MIN and kf >= KELLY_MIN:
                        market_cands.append({
                            "tour": tour, "player": f"{side} {pts}", "opponent": "Totals",
                            "p": p, "odds": price, "display_odds": f"{side} {pts} @{price:.2f}",
                            "kelly": kf, "ev": evu, "source":"Kelly Totals",
                            "start_utc": start_utc_str
                        })

        # --- Spreads (market edges)
        spread={}
        for bm in ev.get("bookmakers", []):
            bname=bm.get("title","?")
            for mk in bm.get("markets", []):
                if mk.get("key")!="spreads": continue
                for out in mk.get("outcomes", []):
                    name, pts, price = out.get("name"), out.get("point"), out.get("price")
                    if name is None or pts is None or price is None: continue
                    arr = spread.setdefault(float(pts), [])
                    # keep best per side name
                    found=False
                    for rec in arr:
                        if rec["name"]==name:
                            if price>rec["price"]: rec["price"]=float(price); rec["books"]={bname}
                            elif abs(price-rec["price"])<1e-9: rec["books"].add(bname)
                            found=True; break
                    if not found:
                        arr.append({"name":name,"price":float(price),"books":{bname}})
        for pts, arr in spread.items():
            if len(arr)==2:
                a,b=arr
                _,_,pfA,pfB = fair_two_way(a["price"], b["price"])
                for tag,rec,p in [("A",a,pfA),("B",b,pfB)]:
                    evu = p*rec["price"] - 1.0; kf = kelly(p, rec["price"])
                    if evu >= MARKET_EV_MIN and kf >= KELLY_MIN:
                        market_cands.append({
                            "tour": tour, "player": f"Spread {pts} ({tag})", "opponent": "Spread",
                            "p": p, "odds": rec["price"], "display_odds": f"{pts:+.1f} @{rec['price']:.2f}",
                            "kelly": kf, "ev": evu, "source":"Kelly Spread",
                            "start_utc": start_utc_str
                        })

    # ---- Picklists ----
    def pick(rows, dogs=True, n=3):
        if dogs:
            filt=[r for r in rows if r["odds"]>=2.20]
        else:
            filt=[r for r in rows if 1.30<=r["odds"]<=1.80]
        return sorted(filt, key=lambda r: (-r["ev"], -r["kelly"]))[:n]

    # Model picks by tour
    sections=[]
    for tour in ("ATP","WTA"):
        sub=[r for r in model_cands if r["tour"]==tour]
        sections.append(shortlist_md(f"🏆 {tour} — Model Underdogs (Top {TOP_DOGS})", pick(sub, True, TOP_DOGS)))
        sections.append(shortlist_md(f"🛡 {tour} — Model Favorites (Top {TOP_FAVS})", pick(sub, False, TOP_FAVS)))

    # Market picks (both tours mixed)
    sections.append(shortlist_md(f"📈 Market Edges — Underdogs (Top {TOP_DOGS})", pick(market_cands, True, TOP_DOGS)))
    sections.append(shortlist_md(f"📈 Market Edges — Favorites (Top {TOP_FAVS})", pick(market_cands, False, TOP_FAVS)))

    # ---- Diagnostics if empty ----
    if not any("—" not in s and "None" not in s for s in sections):
        # top 10 disagreements by |p_model - p_fair|
        diag_rows=[]
        for d in diag:
            # parse p_model/p_fair quickly
            try:
                pmA, pmB = [float(x) for x in d["p_model"].split("/")]
                pfA, pfB = [float(x) for x in d["p_fair"].split("/")]
                gap = max(abs(pmA-pfA), abs(pmB-pfB))
            except Exception:
                gap = float("nan")
            diag_rows.append((gap, d))
        diag_rows = [d for d in diag_rows if not (d[0] != d[0])]  # drop NaNs
        diag_rows.sort(key=lambda x: x[0], reverse=True)
        lines = ["## 🔎 Diagnostics (why nothing qualified)",
                 "_Showing top model–market disagreements to help tune thresholds/name mapping._",
                 "",
                 "| Match | Tour | Start (UTC) | Odds A/B | p_model A/B | p_fair A/B | |",
                 "|---|---|---|---|---|---|"]
        for _,d in diag_rows[:10]:
            lines.append(f"| {d['match']} | {d['sport']} | {d['start_utc']} | {d['odds']} | {d['p_model']} | {d['p_fair']} |")
        sections.append("\n".join(lines) + "\n")

    # ---- Write output ----
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    now_loc = datetime.now(timezone.utc).astimezone(LOCAL_TZ).strftime(f"%Y-%m-%d %H:%M {LOCAL_TZ.key.split('/')[-1]}")
    md = [f"# Tennis Value Engine\n\nUpdated: {now_loc} ({now_utc})\n"] + sections
    out_path = OUT_DIR / SHORTLIST_FILE
    out_path.write_text("\n".join(md), encoding="utf-8")
    print(f"Wrote {out_path}")
    print(f"Model candidates: {len(model_cands)} • Market candidates: {len(market_cands)}")

if __name__ == "__main__":
    run()
