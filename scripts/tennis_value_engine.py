#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tennis Value Engine — with API Diagnostics
- Builds Elo if missing (ATP/WTA from Sackmann 2023–2025).
- Fetches ATP & WTA odds (h2h, spreads, totals) from The Odds API.
- Computes model vs market (Elo vs H2H) and market no-vig edges.
- ALWAYS writes a Markdown table.
- If no rows, appends a clear API diagnostics section so you see WHY.
"""

import os, io, unicodedata, requests, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# ---------------- ENV ----------------
API_KEY = os.getenv("ODDS_API_KEY", "").strip()
if not API_KEY:
    raise SystemExit("ODDS_API_KEY not set")

LOOKAHEAD_HOURS = int(os.getenv("LOOKAHEAD_HOURS", "24"))
REGIONS         = os.getenv("REGIONS", "eu,uk,us,au")            # NO SPACES
MARKETS         = os.getenv("MARKETS", "h2h,spreads,totals")     # NO SPACES

KELLY_MIN = float(os.getenv("KELLY_MIN", "0.05"))
EV_MIN    = float(os.getenv("EV_MIN", "0.00"))
TOP_ROWS  = int(os.getenv("TOP_ROWS", "25"))

OUT_DIR        = Path(os.getenv("OUT_DIR", "outputs"))
SHORTLIST_FILE = os.getenv("SHORTLIST_FILE", "value_engine_shortlist.md")
LOCAL_TZ       = ZoneInfo(os.getenv("TZ", "Europe/Amsterdam"))

OUT_DIR.mkdir(parents=True, exist_ok=True)

SPORT_KEYS = ["tennis_atp", "tennis_wta"]

# ---------------- Elo ----------------
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

# ------------- Odds API (with diagnostics) -------------
def fetch_odds_with_diag(sport_key: str):
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = dict(
        apiKey=API_KEY,
        regions=REGIONS.replace(" ", ""),   # sanitize
        markets=MARKETS.replace(" ", ""),
        oddsFormat="decimal",
        dateFormat="iso",
    )
    status = None
    headers = {}
    body_snippet = ""
    data = []
    try:
        r = requests.get(url, params=params, timeout=25)
        status = r.status_code
        headers = {k.lower(): v for k,v in r.headers.items()}
        if status == 200:
            j = r.json()
            data = j if isinstance(j, list) else []
        else:
            txt = r.text or ""
            body_snippet = (txt[:400] + "...") if len(txt) > 400 else txt
    except requests.RequestException as e:
        status = 0
        body_snippet = str(e)
    return data, status, headers, body_snippet

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

# ---------------- Engine ----------------
def rowline(r):
    return "| {tour} | {market} | {sel} | {opp} | {odds:.2f} | {pm} | {pf:.3f} | {ev} | {kel} | {bet} | {start} | {books} | {src} |".format(
        tour=r["tour"],
        market=r["market"],
        sel=r["selection"],
        opp=(r["opponent"] or "—"),
        odds=r["odds"],
        pm=(f"{r['p_model']:.3f}" if isinstance(r["p_model"], float) else (r["p_model"] or "")),
        pf=r["p_fair"],
        ev=(f"{r['evu']:.3f}" if r["evu"] != "" else ""),
        kel=(f"{r['kelly']:.3f}" if r["kelly"] != "" else ""),
        bet=(r["bet"] if r["bet"] != "" else "NO"),
        start=r["start_utc"],
        books=r.get("books",""),
        src=r["source"],
    )

def write_output(table_rows, diag_lines):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    now_loc = datetime.now(timezone.utc).astimezone(LOCAL_TZ).strftime(f"%Y-%m-%d %H:%M {LOCAL_TZ.key.split('/')[-1]}")
    head = [
        "# Tennis Value Engine",
        "",
        f"Updated: {now_loc} ({now_utc})",
        "",
        "| Tour | Market | Selection | Opponent | Odds | p_model | p_fair | EV/u | Kelly | Bet | Start (UTC) | Books | Source |",
        "|---|---|---|---|---:|---:|---:|---:|---:|:---:|---|---|---|",
    ]
    rows = head + (table_rows if table_rows else [
        "| – | – | – | – | – | – | – | – | – | – | – | – | – |",
        "",
        "_No markets returned by the API in your window/regions. See diagnostics below._",
    ])
    if diag_lines:
        rows += ["", "## API Diagnostics", *diag_lines]
    out_path = OUT_DIR / SHORTLIST_FILE
    out_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write("\n".join(rows) + "\n")
    print(f"Wrote {out_path}")

def run():
    # Build/load Elo
    ensure_elo()
    elo_atp = load_elo_index("data/atp_elo.csv")
    elo_wta = load_elo_index("data/wta_elo.csv")

    # Fetch with diagnostics
    diag = []
    events_all = []
    for key in SPORT_KEYS:
        data, status, hdrs, body = fetch_odds_with_diag(key)
        events_all += data
        diag.append(f"- `{key}` status: **{status}**, events: **{len(data)}**")
        rem = hdrs.get("x-requests-remaining")
        used = hdrs.get("x-requests-used")
        if rem or used:
            diag.append(f"  quota: remaining={rem or '?'} used={used or '?'}")
        if status != 200:
            diag.append("  error body: " + (body or "(empty)"))

    # Build candidates
    cands = []
    now = datetime.now(timezone.utc)

    for ev in events_all:
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

        # H2H best prices across books
        best={}
        for bm in ev.get("bookmakers", []):
            bname=bm.get("title","?")
            for mk in bm.get("markets", []):
                if mk.get("key")!="h2h": continue
                for out in mk.get("outcomes", []):
                    name, price = out.get("name"), out.get("price")
                    if not name or price is None: continue
                    k = norm_name(name)
                    if k not in best or price>best[k]["price"]:
                        best[k]={"name":name,"price":float(price),"books":{bname}}
                    elif abs(price-best[k]["price"])<1e-9:
                        best[k]["books"].add(bname)

        if len(best)==2:
            a,b = list(best.values())
            pmA = p_model(a["name"], b["name"], elo_idx)
            pmB = p_model(b["name"], a["name"], elo_idx)
            _,_,pfA,pfB = fair_two_way(a["price"], b["price"])

            # Model rows (may be blank p_model if name not found)
            for name, opp, price, pmod, pfair, books in [
                (a["name"], b["name"], a["price"], pmA, pfA, a["books"]),
                (b["name"], a["name"], b["price"], pmB, pfB, b["books"]),
            ]:
                evu = (pmod*price - 1.0) if (pmod is not None) else ""
                kf  = (max(0.0, ((price-1)*pmod - (1-pmod))/(price-1)) if (pmod is not None) else "")
                bet = ("YES" if (pmod is not None and float(kf)>=KELLY_MIN and float(evu)>=EV_MIN) else ("NO" if pmod is not None else "NO"))
                cands.append({
                    "tour": tour, "market": "H2H", "selection": name, "opponent": opp,
                    "odds": price, "p_model": ("" if pmod is None else pmod),
                    "p_fair": pfair, "evu": ("" if evu=="" else float(evu)), "kelly": ("" if kf=="" else float(kf)),
                    "bet": bet, "start_utc": start_utc_str, "books": ", ".join(sorted(books)), "source": ("Elo" if pmod is not None else "—"),
                    "score": (float(evu) if evu!="" else -1e9)
                })

        # Totals & Spreads market edges
        def add_market_rows(mkey, market_name):
            lines={}
            for bm in ev.get("bookmakers", []):
                bname=bm.get("title","?")
                for mk in bm.get("markets", []):
                    if mk.get("key")!=mkey: continue
                    for out in mk.get("outcomes", []):
                        if mkey=="totals":
                            side, pts, price = out.get("name"), out.get("point"), out.get("price")
                            if side not in ("Over","Under") or pts is None or price is None: continue
                            lines.setdefault(float(pts), {})
                            prev = lines[float(pts)].get(side)
                            if prev is None or price>prev["price"]:
                                lines[float(pts)][side]={"price":float(price),"books":{bname}}
                            elif abs(price-prev["price"])<1e-9:
                                lines[float(pts)][side]["books"].add(bname)
                        else:  # spreads
                            name, pts, price = out.get("name"), out.get("point"), out.get("price")
                            if name is None or pts is None or price is None: continue
                            arr = lines.setdefault(float(pts), [])
                            found=False
                            for rec in arr:
                                if rec["name"]==name:
                                    if price>rec["price"]: rec["price"]=float(price); rec["books"]={bname}
                                    elif abs(price-rec["price"])<1e-9: rec["books"].add(bname)
                                    found=True; break
                            if not found:
                                arr.append({"name":name,"price":float(price),"books":{bname}})
            if mkey=="totals":
                for pts, sides in lines.items():
                    if "Over" in sides and "Under" in sides:
                        po,pu = sides["Over"]["price"], sides["Under"]["price"]
                        _,_,pfO,pfU = fair_two_way(po,pu)
                        for side, price, pfair, cell in [("Over",po,pfO,sides["Over"]),("Under",pu,pfU,sides["Under"])]:
                            evu = pfair*price - 1.0; kf = kelly(pfair, price)
                            cands.append({
                                "tour": tour, "market": market_name, "selection": f"{side} {pts}", "opponent": "",
                                "odds": price, "p_model": "", "p_fair": pfair, "evu": evu, "kelly": kf, "bet": "YES" if (kf>=KELLY_MIN and evu>=EV_MIN) else "NO",
                                "start_utc": start_utc_str, "books": ", ".join(sorted(cell["books"])), "source": "Kelly Totals", "score": evu
                            })
            else:
                for pts, arr in lines.items():
                    if len(arr)==2:
                        a,b=arr
                        _,_,pfA,pfB = fair_two_way(a["price"], b["price"])
                        for tag, rec, pfair in [("A",a,pfA),("B",b,pfB)]:
                            evu = pfair*rec["price"] - 1.0; kf = kelly(pfair, rec["price"])
                            cands.append({
                                "tour": tour, "market": market_name, "selection": f"{pts:+.1f} ({tag})", "opponent": "",
                                "odds": rec["price"], "p_model": "", "p_fair": pfair, "evu": evu, "kelly": kf, "bet": "YES" if (kf>=KELLY_MIN and evu>=EV_MIN) else "NO",
                                "start_utc": start_utc_str, "books": ", ".join(sorted(rec["books"])), "source": "Kelly Spread", "score": evu
                            })
        add_market_rows("totals", "Totals")
        add_market_rows("spreads", "Spread")

    # Sort & limit
    cands.sort(key=lambda r: (r["score"] if isinstance(r["score"], (int,float)) else -1e9), reverse=True)
    top = cands[:TOP_ROWS]
    rows = [rowline(r) for r in top]

    write_output(rows, diag)

if __name__ == "__main__":
    run()
