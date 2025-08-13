#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tennis Value Engine — Table Edition
-----------------------------------
- Builds Elo if missing (ATP/WTA, last 2–3 seasons from Sackmann).
- Pulls ATP & WTA odds (H2H + spreads + totals) from The Odds API.
- Computes:
    * Model vs Market (Elo vs best H2H)
    * Market vs Market (no-vig Kelly on H2H/Spreads/Totals)
- ALWAYS writes a Markdown table with rows (top candidates),
  even if nothing clears thresholds (they’ll show Bet:NO).
"""

import os, io, unicodedata, requests, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# ===================== ENV =====================
API_KEY = os.getenv("ODDS_API_KEY", "").strip()
if not API_KEY:
    raise SystemExit("ODDS_API_KEY not set")

LOOKAHEAD_HOURS = int(os.getenv("LOOKAHEAD_HOURS", "24"))
REGIONS         = os.getenv("REGIONS", "eu,uk,us,au")
MARKETS         = os.getenv("MARKETS", "h2h,spreads,totals")

# Recommendation gates (Bet = YES iff both gates pass)
KELLY_MIN       = float(os.getenv("KELLY_MIN", "0.05"))
EV_MIN          = float(os.getenv("EV_MIN", "0.00"))          # unified EV gate

TOP_ROWS        = int(os.getenv("TOP_ROWS", "25"))            # rows to show even if NO
OUT_DIR         = Path(os.getenv("OUT_DIR", "outputs"))
SHORTLIST_FILE  = os.getenv("SHORTLIST_FILE", "value_engine_shortlist.md")
LOCAL_TZ        = ZoneInfo(os.getenv("TZ", "Europe/Amsterdam"))

OUT_DIR.mkdir(parents=True, exist_ok=True)

SPORT_KEYS = ["tennis_atp", "tennis_wta"]

# ===================== Elo =====================
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
        if r.status_code==200 and r.text.strip():
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
    if not Path("data/atp_elo.csv").exists():
        atp = build_elo_from_urls(SACK_ATP)
        if not atp.empty: atp.sort_values("elo", ascending=False).to_csv("data/atp_elo.csv", index=False)
    if not Path("data/wta_elo.csv").exists():
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

def p_model(player_a: str, player_b: str, idx: dict):
    e1 = idx.get(norm_name(player_a))
    e2 = idx.get(norm_name(player_b))
    if e1 is None or e2 is None: return None
    return _exp(e1, e2)

# ===================== Odds API =====================
def fetch_odds(sport_key: str):
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = dict(apiKey=API_KEY, regions=REGIONS, markets=MARKETS,
                  oddsFormat="decimal", dateFormat="iso")
    try:
        r = requests.get(url, params=params, timeout=25)
        if r.status_code==200:
            j = r.json()
            return j if isinstance(j, list) else []
    except requests.RequestException:
        pass
    return []

def within_window(commence_iso: str) -> bool:
    if not commence_iso: return False
    start = datetime.fromisoformat(commence_iso.replace("Z","+00:00"))
    now = datetime.now(timezone.utc)
    return (start-now) >= timedelta(0) and (start-now) <= timedelta(hours=LOOKAHEAD_HOURS)

def fair_two_way(odds_a: float, odds_b: float):
    ia, ib = 1/odds_a, 1/odds_b
    s = ia+ib
    if s <= 0: return 0.5,0.5,0.5,0.5
    return ia, ib, ia/s, ib/s

def kelly(p: float, o: float):
    b = o-1.0
    return max(0.0, (b*p-(1-p))/b) if b>0 else 0.0

# ===================== Engine =====================
def row_recommendation(kf: float, evu: float) -> str:
    return "YES" if (kf >= KELLY_MIN and evu >= EV_MIN) else "NO"

def write_table(rows):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    now_loc = datetime.now(timezone.utc).astimezone(LOCAL_TZ).strftime(f"%Y-%m-%d %H:%M {LOCAL_TZ.key.split('/')[-1]}")
    header = [
        f"# Tennis Value Engine",
        "",
        f"Updated: {now_loc} ({now_utc})",
        "",
        f"| Tour | Market | Selection | Opponent | Odds | p_model | p_fair | EV/u | Kelly | Bet | Start (UTC) | Books | Source |",
        f"|---|---|---|---|---:|---:|---:|---:|---:|:---:|---|---|---|",
    ]
    lines = header + rows
    out_path = OUT_DIR / SHORTLIST_FILE
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    # Also push to Actions summary if available
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    print(f"Wrote {out_path}")

def run():
    ensure_elo()
    elo_atp = load_elo_index("data/atp_elo.csv")
    elo_wta = load_elo_index("data/wta_elo.csv")

    # pull ATP+WTA events
    events=[]
    for key in SPORT_KEYS:
        events += fetch_odds(key)

    candidates = []  # collect EVERYTHING, then sort and take top N

    for ev in events:
        ct = ev.get("commence_time")
        if not ct or not within_window(ct):
            continue
        start_utc = datetime.fromisoformat(ct.replace("Z","+00:00"))
        start_utc_str = start_utc.strftime("%Y-%m-%d %H:%M UTC")

        stitle = (ev.get("sport_title") or "").upper()
        if "ATP" in stitle:
            tour="ATP"; elo_idx=elo_atp
        elif "WTA" in stitle:
            tour="WTA"; elo_idx=elo_wta
        else:
            continue

        # H2H best prices
        h2h_best={}
        for bm in ev.get("bookmakers", []):
            bname=bm.get("title","?")
            for mk in bm.get("markets", []):
                if mk.get("key")!="h2h": continue
                for out in mk.get("outcomes", []):
                    name, price = out.get("name"), out.get("price")
                    if not name or price is None: continue
                    k = norm_name(name)
                    if k not in h2h_best or price>h2h_best[k]["price"]:
                        h2h_best[k]={"name":name,"price":float(price),"books":{bname}}
                    elif abs(price-h2h_best[k]["price"])<1e-9:
                        h2h_best[k]["books"].add(bname)

        if len(h2h_best)==2:
            a,b = list(h2h_best.values())
            pmA = p_model(a["name"], b["name"], elo_idx)
            pmB = p_model(b["name"], a["name"], elo_idx)
            _,_,pfA,pfB = fair_two_way(a["price"], b["price"])
            # Model rows (even if pm is None, we still show p_model = '')
            for name, opp, price, pmod, pfair, books in [
                (a["name"], b["name"], a["price"], pmA, pfA, a["books"]),
                (b["name"], a["name"], b["price"], pmB, pfB, b["books"]),
            ]:
                evu = (pmod*price - 1.0) if (pmod is not None) else float("nan")
                kf  = kelly(pmod, price) if (pmod is not None) else 0.0
                candidates.append({
                    "tour": tour, "market": "H2H", "selection": name, "opponent": opp,
                    "odds": price,
                    "p_model": ("" if pmod is None else round(pmod,3)),
                    "p_fair": round(pfair,3),
                    "evu": ("" if pmod is None else round(evu,3)),
                    "kelly": ("" if pmod is None else round(kf,3)),
                    "bet": ("" if pmod is None else row_recommendation(kf, evu)),
                    "start_utc": start_utc_str,
                    "books": ", ".join(sorted(books)),
                    "source": ("Elo" if pmod is not None else "—"),
                    "score": (evu if (pmod is not None) else -1e9)  # for sorting
                })

            # Also include pure market (no-vig) H2H Kelly rows
            for name, opp, price, pfair, books in [
                (a["name"], b["name"], a["price"], pfA, a["books"]),
                (b["name"], a["name"], b["price"], pfB, b["books"]),
            ]:
                evu = pfair*price - 1.0
                kf  = kelly(pfair, price)
                candidates.append({
                    "tour": tour, "market": "H2H", "selection": name, "opponent": opp,
                    "odds": price, "p_model": "", "p_fair": round(pfair,3),
                    "evu": round(evu,3), "kelly": round(kf,3),
                    "bet": row_recommendation(kf, evu),
                    "start_utc": start_utc_str,
                    "books": ", ".join(sorted(books)),
                    "source": "Kelly H2H",
                    "score": evu
                })

        # Totals best prices
        totals={}
        for bm in ev.get("bookmakers", []):
            bname=bm.get("title","?")
            for mk in bm.get("markets", []):
                if mk.get("key")!="totals": continue
                for out in mk.get("outcomes", []):
                    side, pts, price = out.get("name"), out.get("point"), out.get("price")
                    if side not in ("Over","Under") or pts is None or price is None: continue
                    totals.setdefault(float(pts), {})
                    prev = totals[float(pts)].get(side)
                    if prev is None or price>prev["price"]:
                        totals[float(pts)][side]={"price":float(price), "books":{bname}}
                    elif abs(price-prev["price"])<1e-9:
                        totals[float(pts)][side]["books"].add(bname)
        for pts, sides in totals.items():
            if "Over" in sides and "Under" in sides:
                po, pu = sides["Over"]["price"], sides["Under"]["price"]
                _,_,pfO,pfU = fair_two_way(po, pu)
                for side, price, pfair, cell in [("Over", po, pfO, sides["Over"]), ("Under", pu, pfU, sides["Under"])]:
                    evu = pfair*price - 1.0; kf = kelly(pfair, price)
                    candidates.append({
                        "tour": tour, "market": "Totals", "selection": f"{side} {pts}", "opponent": "",
                        "odds": price, "p_model": "", "p_fair": round(pfair,3),
                        "evu": round(evu,3), "kelly": round(kf,3),
                        "bet": row_recommendation(kf, evu),
                        "start_utc": start_utc_str,
                        "books": ", ".join(sorted(cell["books"])),
                        "source": "Kelly Totals",
                        "score": evu
                    })

        # Spreads best prices
        spreads={}
        for bm in ev.get("bookmakers", []):
            bname=bm.get("title","?")
            for mk in bm.get("markets", []):
                if mk.get("key")!="spreads": continue
                for out in mk.get("outcomes", []):
                    name, pts, price = out.get("name"), out.get("point"), out.get("price")
                    if name is None or pts is None or price is None: continue
                    arr = spreads.setdefault(float(pts), [])
                    found=False
                    for rec in arr:
                        if rec["name"]==name:
                            if price>rec["price"]: rec["price"]=float(price); rec["books"]={bname}
                            elif abs(price-rec["price"])<1e-9: rec["books"].add(bname)
                            found=True; break
                    if not found:
                        arr.append({"name":name,"price":float(price),"books":{bname}})
        for pts, arr in spreads.items():
            if len(arr)==2:
                a,b = arr
                _,_,pfA,pfB = fair_two_way(a["price"], b["price"])
                for tag, rec, pfair in [("A",a,pfA),("B",b,pfB)]:
                    evu = pfair*rec["price"] - 1.0; kf = kelly(pfair, rec["price"])
                    candidates.append({
                        "tour": tour, "market": "Spread", "selection": f"{pts:+.1f} ({tag})", "opponent": "",
                        "odds": rec["price"], "p_model": "", "p_fair": round(pfair,3),
                        "evu": round(evu,3), "kelly": round(kf,3),
                        "bet": row_recommendation(kf, evu),
                        "start_utc": start_utc_str,
                        "books": ", ".join(sorted(rec["books"])),
                        "source": "Kelly Spread",
                        "score": evu
                    })

    # Sort by EV desc (score), take top N for display
    candidates.sort(key=lambda r: (r["score"] if isinstance(r["score"], (int,float)) else -1e9), reverse=True)
    top = candidates[:TOP_ROWS]

    # Materialize rows (strings)
    rows=[]
    for r in top:
        rows.append("| {tour} | {market} | {sel} | {opp} | {odds:.2f} | {pm} | {pf:.3f} | {ev} | {kel} | {bet} | {start} | {books} | {src} |".format(
            tour=r["tour"],
            market=r["market"],
            sel=r["selection"],
            opp=(r["opponent"] or "—"),
            odds=r["odds"],
            pm=(f"{r['p_model']:.3f}" if isinstance(r["p_model"], float) else (r["p_model"] or ""))
               if r["p_model"] != "" else "",
            pf=r["p_fair"],
            ev=(f"{r['evu']:.3f}" if r["evu"] != "" else ""),
            kel=(f"{r['kelly']:.3f}" if r["kelly"] != "" else ""),
            bet=(r["bet"] if r["bet"] != "" else "NO"),
            start=r["start_utc"],
            books=r.get("books",""),
            src=r["source"],
        ))

    # If absolutely nothing at all was found, still write header + empty note.
    if not rows:
        rows = ["| – | – | – | – | – | – | – | – | – | – | – | – | – |",
                "",
                "_No markets found in the window/regions. Consider widening LOOKAHEAD_HOURS or adding regions (eu,uk,us,au)._"]

    write_table(rows)

if __name__ == "__main__":
    run()
