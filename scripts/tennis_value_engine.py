#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, io, unicodedata, requests, pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# ----------------- ENV -----------------
API_KEY = os.getenv("ODDS_API_KEY", "").strip()
if not API_KEY:
    raise SystemExit("ODDS_API_KEY not set")

LOOKAHEAD_HOURS = int(os.getenv("LOOKAHEAD_HOURS", "24"))
KELLY_MIN       = float(os.getenv("KELLY_MIN", "0.00"))      # loosen to see output
MODEL_EV_MIN    = float(os.getenv("MODEL_EV_MIN", "-0.02"))  # loosen to see output
MARKET_EV_MIN   = float(os.getenv("MARKET_EV_MIN", "-0.02"))
REGIONS         = os.getenv("REGIONS", "eu,uk,us,au")
MARKETS         = os.getenv("MARKETS", "h2h,spreads,totals")
TZ_LOCAL        = ZoneInfo(os.getenv("TZ", "Europe/Amsterdam"))
OUT_DIR         = os.getenv("OUT_DIR", "outputs")
SHORTLIST_FILE  = os.getenv("SHORTLIST_FILE", "value_engine_shortlist.md")
TOP_DOGS        = int(os.getenv("TOP_DOGS", "3"))
TOP_FAVS        = int(os.getenv("TOP_FAVS", "2"))

SPORT_KEYS = ["tennis_atp","tennis_wta"]   # focus ATP/WTA first (most reliable)

os.makedirs(OUT_DIR, exist_ok=True)
DEBUG = True
def dbg(*a): 
    if DEBUG: print("DEBUG:", *a)

# --------------- ELO -------------------
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

def ensure_elo_files():
    os.makedirs("data", exist_ok=True)
    need = []
    for tour in ("atp","wta"):
        fp = f"data/{tour}_elo.csv"
        if not os.path.exists(fp): need.append(tour)
    if not need: 
        dbg("Elo files present.")
        return
    dbg("Building Elo for:", need)
    if "atp" in need:
        atp = build_elo_from_urls(SACK_ATP)
        if not atp.empty: atp.sort_values("elo", ascending=False).to_csv("data/atp_elo.csv", index=False)
    if "wta" in need:
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
    df = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame(columns=["player","elo"])
    idx = {norm_name(r["player"]): float(r["elo"]) for _,r in df.iterrows()}
    dbg(f"Loaded {len(idx)} Elo rows from {path}")
    return idx

# ------------- ODDS API ----------------
def fetch_sport_odds(sport_key: str):
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = dict(apiKey=API_KEY, regions=REGIONS, markets=MARKETS, oddsFormat="decimal", dateFormat="iso")
    try:
        r = requests.get(url, params=params, timeout=25)
        if r.status_code==200:
            return r.json() if isinstance(r.json(), list) else []
    except requests.RequestException:
        pass
    return []

def within_window(commence_iso: str) -> bool:
    if not commence_iso: return False
    start = datetime.fromisoformat(commence_iso.replace("Z","+00:00"))
    now = datetime.now(timezone.utc)
    return timedelta(0) <= (start-now) <= timedelta(hours=LOOKAHEAD_HOURS)

def fair_two_way(odds_a: float, odds_b: float):
    ia, ib = 1/odds_a, 1/odds_b
    s = ia+ib
    if s<=0: return 0.5,0.5,0.5,0.5
    return ia, ib, ia/s, ib/s

def kelly_fraction(p: float, o: float):
    b = o-1.0
    return max(0.0, (b*p - (1-p))/b) if b>0 else 0.0

# ------------- ENGINE ------------------
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
    # Make sure Elo exists (build if missing)
    ensure_elo_files()
    elo_atp = load_elo_index("data/atp_elo.csv")
    elo_wta = load_elo_index("data/wta_elo.csv")

    events=[]
    for key in SPORT_KEYS:
        part = fetch_sport_odds(key)
        dbg(f"{key}: fetched {len(part)} events")
        events += part

    now = datetime.now(timezone.utc)
    in_window = now + timedelta(hours=LOOKAHEAD_HOURS)

    model_candidates=[]
    market_candidates=[]

    for ev in events:
        ct = ev.get("commence_time")
        if not ct or not within_window(ct): 
            continue

        start_utc = datetime.fromisoformat(ct.replace("Z","+00:00"))
        start_local = start_utc.astimezone(TZ_LOCAL)
        start_utc_str = start_utc.strftime("%Y-%m-%d %H:%M UTC")

        home = ev.get("home_team","") or ""
        away = ev.get("away_team","") or ""
        sport = (ev.get("sport_title") or "").upper()

        # pick tour
        if "ATP" in sport:
            tour = "ATP"; elo_idx = elo_atp
        elif "WTA" in sport:
            tour = "WTA"; elo_idx = elo_wta
        else:
            continue

        bms = ev.get("bookmakers", [])
        if not bms: 
            continue

        # H2H first (model & market)
        best = {}
        for bm in bms:
            for mk in bm.get("markets", []):
                if mk.get("key")!="h2h": continue
                for out in mk.get("outcomes", []):
                    name = out.get("name"); price = out.get("price")
                    if not name or price is None: continue
                    k = norm_name(name)
                    if k not in best or price>best[k]["price"]:
                        best[k]={"name":name,"price":float(price),"books":{bm.get("title","?")}}
                    elif abs(price-best[k]["price"])<1e-9:
                        best[k]["books"].add(bm.get("title","?"))

        if len(best)==2:
            items=list(best.values())
            a,b = items[0], items[1]
            # model vs market (Elo)
            na, nb = norm_name(a["name"]), norm_name(b["name"])
            pa = elo_idx.get(na); pb = elo_idx.get(nb)
            if pa is not None and pb is not None:
                pA = _exp(pa,pb); pB = _exp(pb,pa)
                for name, price, p in [(a["name"], a["price"], pA),(b["name"], b["price"], pB)]:
                    evu = p*price - 1.0
                    kf  = kelly_fraction(p, price)
                    if evu >= MODEL_EV_MIN:
                        model_candidates.append({
                            "tour": tour, "player": name, "opponent": (b["name"] if name==a["name"] else a["name"]),
                            "p": p, "odds": price, "display_odds": f"{price:.2f}",
                            "kelly": kf, "ev": evu, "source": "Elo",
                            "start_utc": start_utc_str
                        })

            # market vs market (no-vig)
            _,_,pfA,pfB = fair_two_way(a["price"], b["price"])
            for name, price, p, cell in [(a["name"], a["price"], pfA, a), (b["name"], b["price"], pfB, b)]:
                evu = p*price - 1.0
                kf  = kelly_fraction(p, price)
                if evu >= MARKET_EV_MIN and kf >= KELLY_MIN:
                    market_candidates.append({
                        "tour": tour, "player": name, "opponent": (b["name"] if name==a["name"] else a["name"]),
                        "p": p, "odds": price, "display_odds": f"{price:.2f}",
                        "kelly": kf, "ev": evu, "source": "Kelly H2H",
                        "start_utc": start_utc_str,
                        "books": ", ".join(sorted(cell["books"]))
                    })

        # Totals
        lines={}
        for bm in bms:
            for mk in bm.get("markets", []):
                if mk.get("key")!="totals": continue
                for out in mk.get("outcomes", []):
                    side = out.get("name"); pts = out.get("point"); price = out.get("price")
                    if side not in ("Over","Under") or pts is None or price is None: continue
                    key = float(pts)
                    lines.setdefault(key, {})
                    prev = lines[key].get(side)
                    if prev is None or price>prev["price"]:
                        lines[key][side]={"price":float(price),"books":{bm.get("title","?")}}
                    elif abs(price-prev["price"])<1e-9:
                        lines[key][side]["books"].add(bm.get("title","?"))
        for pts, sides in lines.items():
            if "Over" in sides and "Under" in sides:
                po, pu = sides["Over"]["price"], sides["Under"]["price"]
                _,_,pfO,pfU = fair_two_way(po, pu)
                for side, price, p, cell in [("Over", po, pfO, sides["Over"]), ("Under", pu, pfU, sides["Under"])]:
                    evu = p*price - 1.0
                    kf  = kelly_fraction(p, price)
                    if evu >= MARKET_EV_MIN and kf >= KELLY_MIN:
                        market_candidates.append({
                            "tour": tour, "player": f"{side} {pts}", "opponent": f"{away} vs {home}",
                            "p": p, "odds": price, "display_odds": f"{side} {pts} @{price:.2f}",
                            "kelly": kf, "ev": evu, "source": "Kelly Totals",
                            "start_utc": start_utc_str,
                            "books": ", ".join(sorted(cell["books"]))
                        })

        # Spreads (pair any two at same line)
        sp={}
        for bm in bms:
            for mk in bm.get("markets", []):
                if mk.get("key")!="spreads": continue
                for out in mk.get("outcomes", []):
                    name = out.get("name"); pts = out.get("point"); price = out.get("price")
                    if name is None or pts is None or price is None: continue
                    key = float(pts)
                    sp.setdefault(key, [])
                    # store best by name
                    found = False
                    for rec in sp[key]:
                        if rec["name"]==name:
                            if price>rec["price"]: 
                                rec["price"]=float(price); rec["books"]={bm.get("title","?")}
                            elif abs(price-rec["price"])<1e-9:
                                rec["books"].add(bm.get("title","?"))
                            found=True; break
                    if not found:
                        sp[key].append({"name":name,"price":float(price),"books":{bm.get("title","?")}})
        for pts, arr in sp.items():
            if len(arr)==2:
                a,b=arr
                _,_,pfA,pfB = fair_two_way(a["price"], b["price"])
                for which, rec, p in [("A",a,pfA),("B",b,pfB)]:
                    evu = p*rec["price"] - 1.0
                    kf  = kelly_fraction(p, rec["price"])
                    if evu >= MARKET_EV_MIN and kf >= KELLY_MIN:
                        market_candidates.append({
                            "tour": tour, "player": f"Spread {pts} ({which})", "opponent": f"{away} vs {home}",
                            "p": p, "odds": rec["price"], "display_odds": f"{pts:+.1f} @{rec['price']:.2f}",
                            "kelly": kf, "ev": evu, "source": "Kelly Spread",
                            "start_utc": start_utc_str,
                            "books": ", ".join(sorted(rec["books"]))
                        })

    # Dedup and pick
    def dedup(rows):
        seen={}
        out=[]
        for r in sorted(rows, key=lambda x: (-x["ev"], -x["kelly"], x["start_utc"]))):
            key = (r["tour"], r["player"], r["opponent"], r["start_utc"], r["source"])
            if key not in seen:
                seen[key]=1; out.append(r)
        return out

    model_d = dedup(model_candidates)
    market_d = dedup(market_candidates)

    def pick(rows, dogs=True, n=3):
        if dogs:
            filt=[r for r in rows if r["odds"]>=2.20]
        else:
            filt=[r for r in rows if 1.30<=r["odds"]<=1.80]
        return sorted(filt, key=lambda r: (-r["ev"], -r["kelly"]))[:n]

    # build sections
    sections=[]
    for tour in ("ATP","WTA"):
        sub=[r for r in model_d if r["tour"]==tour]
        sections.append(shortlist_md(f"🏆 {tour} — Model Underdogs (Top {TOP_DOGS})", pick(sub, True, TOP_DOGS)))
        sections.append(shortlist_md(f"🛡 {tour} — Model Favorites (Top {TOP_FAVS})", pick(sub, False, TOP_FAVS)))

    sections.append(shortlist_md(f"📈 Market Edges — Underdogs (Top {TOP_DOGS})", pick(market_d, True, TOP_DOGS)))
    sections.append(shortlist_md(f"📈 Market Edges — Favorites (Top {TOP_FAVS})", pick(market_d, False, TOP_FAVS)))

    # write
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    now_loc = datetime.now(timezone.utc).astimezone(TZ_LOCAL).strftime(f"%Y-%m-%d %H:%M {TZ_LOCAL.key.split('/')[-1]}")
    md = [f"# Tennis Value Engine\n\nUpdated: {now_loc} ({now_utc})\n"] + sections
    out_path = os.path.join(OUT_DIR, SHORTLIST_FILE)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    run()
