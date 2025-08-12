#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tennis Value Engine
-------------------
Unifies two sources of edge:
1) Model vs Market: Elo-derived p(win) vs best H2H odds
2) Market vs Market: Kelly scan on spreads/totals (no-vig) + optional H2H

Outputs one shortlist (markdown + Actions summary).

ENV knobs:
  ODDS_API_KEY          (required)
  LOOKAHEAD_HOURS=24
  KELLY_MIN=0.05        # min full Kelly to recommend "YES"
  MODEL_EV_MIN=0.00     # model EV must be > this
  MARKET_EV_MIN=0.00    # market EV must be > this
  REGIONS=eu,uk,us,au
  MARKETS=h2h,spreads,totals
  TOURS=ATP,WTA          # Which Elo ladders to build/use
  TZ=Europe/Amsterdam
  OUT_DIR=outputs
  SHORTLIST_FILE=value_engine_shortlist.md
  TOP_DOGS=3 TOP_FAVS=2
"""

import os, math, csv
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional
import pandas as pd
import requests
from zoneinfo import ZoneInfo

# ---------- Config ----------
API_KEY = os.getenv("ODDS_API_KEY", "").strip()
LOOKAHEAD_HOURS = int(os.getenv("LOOKAHEAD_HOURS", "24"))
KELLY_MIN = float(os.getenv("KELLY_MIN", "0.05"))
MODEL_EV_MIN = float(os.getenv("MODEL_EV_MIN", "0.00"))
MARKET_EV_MIN = float(os.getenv("MARKET_EV_MIN", "0.00"))
REGIONS = os.getenv("REGIONS", "eu,uk,us,au")
MARKETS = os.getenv("MARKETS", "h2h,spreads,totals")
TOURS = [t.strip().upper() for t in os.getenv("TOURS", "ATP,WTA").split(",") if t.strip()]
LOCAL_TZ = ZoneInfo(os.getenv("TZ", "Europe/Amsterdam"))
OUT_DIR = Path(os.getenv("OUT_DIR", "outputs"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
SHORTLIST_FILE = os.getenv("SHORTLIST_FILE", "value_engine_shortlist.md")
TOP_DOGS = int(os.getenv("TOP_DOGS", "3"))
TOP_FAVS = int(os.getenv("TOP_FAVS", "2"))

SPORT_KEYS = {
    "ATP": "tennis_atp",
    "WTA": "tennis_wta",
    # Challenger/ITF are market-only in this engine to keep Elo clean
    "CHALLENGER": "tennis_challenger",
    "ITF_M": "tennis_itf_men",
    "ITF_W": "tennis_itf_women",
}

ODDS_FORMAT = "decimal"
DATE_FORMAT = "iso"

# ---------- Elo ----------
START_ELO, K = 1500, 32

def _exp(a,b): return 1/(1+10**((b-a)/400))
def _upd(a,b,s): return a + K*(s-_exp(a,b))

def build_elo_from_csvs(csv_files: List[Path]) -> pd.DataFrame:
    if not csv_files:
        return pd.DataFrame(columns=["player","elo"])
    frames=[]
    for f in sorted(csv_files):
        try:
            frames.append(pd.read_csv(f))
        except Exception:
            continue
    if not frames:
        return pd.DataFrame(columns=["player","elo"])
    df = pd.concat(frames, ignore_index=True)
    E: Dict[str,float] = {}
    for _,r in df.iterrows():
        w, l = r.get('winner_name'), r.get('loser_name')
        if pd.isna(w) or pd.isna(l): continue
        ew, el = E.get(w, START_ELO), E.get(l, START_ELO)
        E[w] = _upd(ew, el, 1); E[l] = _upd(el, ew, 0)
    return pd.DataFrame([{"player":k, "elo":v} for k,v in E.items()])

def get_elo_map(tour: str) -> Dict[str,float]:
    """Load from data/*.csv if present; else from matches/*.csv (built inline)."""
    data_file = Path("data") / f"{tour.lower()}_elo.csv"
    if data_file.exists():
        try:
            df = pd.read_csv(data_file)
            return {r["player"]: float(r["elo"]) for _,r in df.iterrows()}
        except Exception:
            pass
    # Build from matches stash if available
    m = Path("matches")
    patterns = {
        "ATP": ["atp_matches_2023.csv","atp_matches_2024.csv","atp_matches_2025.csv"],
        "WTA": ["wta_matches_2023.csv","wta_matches_2024.csv","wta_matches_2025.csv"],
    }
    files = [m/p for p in patterns.get(tour, []) if (m/p).exists()]
    df = build_elo_from_csvs(files)
    if not df.empty:
        df.sort_values("elo", ascending=False).to_csv(data_file, index=False)
        return {r["player"]: float(r["elo"]) for _,r in df.iterrows()}
    return {}

def elo_prob(p1: str, p2: str, elo_map: Dict[str,float]) -> Optional[float]:
    e1 = elo_map.get(p1); e2 = elo_map.get(p2)
    if e1 is None or e2 is None: return None
    return _exp(e1, e2)

# ---------- Odds ----------
def within_window(commence_iso: str) -> bool:
    if not commence_iso: return False
    start = datetime.fromisoformat(commence_iso.replace("Z","+00:00"))
    now = datetime.now(timezone.utc)
    return timedelta(0) <= (start-now) <= timedelta(hours=LOOKAHEAD_HOURS)

def fetch_odds(sport_key: str) -> List[Dict]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = dict(apiKey=API_KEY, regions=REGIONS, markets=MARKETS,
                  oddsFormat=ODDS_FORMAT, dateFormat=DATE_FORMAT)
    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code==200:
            data = r.json()
            return data if isinstance(data, list) else []
    except requests.RequestException:
        pass
    return []

def best_price_shop(bookmakers: List[Dict], market_key: str) -> Dict[Tuple[str,float], Dict]:
    """
    For market_key ('h2h','spreads','totals'):
      - H2H key: (player_name, 0)
      - Totals key: ('Over'/'Under', points)
      - Spreads key: (outcome_name, points) (book naming varies)
    Value: {"price": float, "books": set(str)}
    """
    grid: Dict[Tuple[str,float], Dict] = {}
    for bm in bookmakers:
        bname = bm.get("title","?")
        for mk in bm.get("markets", []):
            if mk.get("key") != market_key: continue
            for out in mk.get("outcomes", []):
                name = str(out.get("name"))
                pts = float(out.get("point") or 0.0)
                price = out.get("price")
                if price is None: continue
                k = (name, pts)
                cell = grid.get(k)
                if cell is None or price > cell["price"]:
                    grid[k] = {"price": float(price), "books": {bname}}
                elif abs(price - cell["price"]) < 1e-9:
                    cell["books"].add(bname)
    return grid

def pair_lines(price_map: Dict[Tuple[str,float], Dict], expected_pair: Tuple[str,str], exact=True):
    """Yield (points, A_dict, B_dict). For H2H points=0."""
    by_pts: Dict[float, Dict[str,Dict]] = {}
    for (name, pts), cell in price_map.items():
        by_pts.setdefault(pts, {})[name] = cell
    for pts, d in by_pts.items():
        if exact and all(x in d for x in expected_pair):
            yield (pts, d[expected_pair[0]], d[expected_pair[1]])
        else:
            if len(d)==2:
                items=list(d.values())
                yield (pts, items[0], items[1])

def fair_two_way(price_a: float, price_b: float):
    ia, ib = 1/price_a, 1/price_b
    s = ia+ib
    if s<=0: return 0.5,0.5,0.5,0.5
    return ia, ib, ia/s, ib/s

def kelly(p_true: float, odds: float) -> float:
    b = odds-1
    return max(0.0, (b*p_true-(1-p_true))/b) if b>0 else 0.0

def local_utc_strs(commence_iso: str):
    start_utc = datetime.fromisoformat(commence_iso.replace("Z","+00:00"))
    start_loc = start_utc.astimezone(LOCAL_TZ)
    return (start_loc.strftime(f"%Y-%m-%d %H:%M {start_loc.tzname()}"),
            start_utc.strftime("%Y-%m-%d %H:%M UTC"))

# ---------- Engine ----------
def shortlist_markdown(section_title: str, rows: List[Dict]) -> str:
    if not rows: return f"## {section_title}\n_None_\n"
    lines=[f"## {section_title}"]
    for r in rows:
        lines.append(
            f"- {r['player']} vs {r['opponent']} — {r['display_odds']} "
            f"(p={r['p']:.2f}, Kelly={r['kelly']:.3f}, EV={r['ev']:+.2f}) — Source: {r['source']}\n"
            f"  🗓 {r['start_utc']}"
        )
    return "\n".join(lines) + "\n"

def write_summary(shortlist_sections: List[str]):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    now_loc = datetime.now(timezone.utc).astimezone(LOCAL_TZ).strftime(f"%Y-%m-%d %H:%M {LOCAL_TZ.key.split('/')[-1]}")
    md = [f"# Tennis Value Engine\n\nUpdated: {now_loc} ({now_utc})\n"]
    md += shortlist_sections
    out_path = OUT_DIR / SHORTLIST_FILE
    out_path.write_text("\n".join(md), encoding="utf-8")
    print(f"Wrote {out_path}")

    # Also print to Actions summary if available
    gh_summary = os.getenv("GITHUB_STEP_SUMMARY")
    if gh_summary:
        with open(gh_summary, "a", encoding="utf-8") as f:
            f.write("\n".join(md))

def run():
    if not API_KEY:
        print("ERROR: ODDS_API_KEY not set.")
        return

    # Build Elo maps for requested tours
    elos = {tour: get_elo_map(tour) for tour in TOURS}

    # Fetch odds across ladders
    events: List[Dict] = []
    keys_to_pull = [SPORT_KEYS[t] for t in ["ATP","WTA","CHALLENGER","ITF_M","ITF_W"] if t in SPORT_KEYS]
    for key in keys_to_pull:
        try:
            events += fetch_odds(key)
        except Exception:
            continue

    model_candidates = []  # Elo vs H2H (ATP/WTA only)
    market_candidates = [] # Spreads/Totals/H2H (all ladders)

    for ev in events:
        commence = ev.get("commence_time")
        if not commence or not within_window(commence): continue
        home, away = ev.get("home_team") or "", ev.get("away_team") or ""
        match_name = f"{away} vs {home}" if (home and away) else " vs ".join(ev.get("teams", []))
        tour_title = (ev.get("sport_title") or "").upper()
        bms = ev.get("bookmakers", [])
        if not bms: continue

        start_local, start_utc = local_utc_strs(commence)

        # ---------- H2H market ----------
        h2h_map = best_price_shop(bms, "h2h")
        # Pair any two names @ H2H
        for _, a_dict, b_dict in pair_lines(h2h_map, ("A","B"), exact=False):
            price_a, price_b = a_dict["price"], b_dict["price"]
            # Figure out player names (grab the keys that match those dicts)
            names = [(k[0], v) for k,v in h2h_map.items() if v is a_dict or v is b_dict]
            if len(names)==2:
                name_a, name_b = names[0][0], names[1][0]
            else:
                name_a, name_b = "Player A", "Player B"

            # 1) Model vs Market (ATP/WTA only)
            tour_for_elo = "ATP" if "ATP" in tour_title else ("WTA" if "WTA" in tour_title else None)
            if tour_for_elo and elos.get(tour_for_elo):
                p_a = elo_prob(name_a, name_b, elos[tour_for_elo])
                p_b = elo_prob(name_b, name_a, elos[tour_for_elo])
                if p_a is not None and p_b is not None:
                    # model ev/kelly vs best odds
                    for name, price, p in [(name_a, price_a, p_a), (name_b, price_b, p_b)]:
                        ev_u = p*price - 1.0
                        k = kelly(p, price)
                        if ev_u >= MODEL_EV_MIN:
                            model_candidates.append({
                                "tour": tour_for_elo, "player": name, "opponent": (name_b if name==name_a else name_a),
                                "p": p, "odds": price, "display_odds": f"{price:.2f}", "kelly": k, "ev": ev_u,
                                "source": "Elo", "start_local": start_local, "start_utc": start_utc
                            })

            # 2) Market vs Market on H2H too (no-vig)
            ia, ib, pa, pb = fair_two_way(price_a, price_b)
            for name, price, p, cell in [(name_a, price_a, pa, a_dict), (name_b, price_b, pb, b_dict)]:
                ev_u = p*price - 1.0
                k = kelly(p, price)
                if ev_u >= MARKET_EV_MIN and k >= KELLY_MIN:
                    market_candidates.append({
                        "tour": tour_title.split()[0] if tour_title else "",
                        "player": name, "opponent": (name_b if name==name_a else name_a),
                        "p": p, "odds": price, "display_odds": f"{price:.2f}",
                        "kelly": k, "ev": ev_u, "source": "Kelly H2H",
                        "start_local": start_local, "start_utc": start_utc,
                        "books": ", ".join(sorted(cell["books"]))
                    })

        # ---------- Totals ----------
        totals_map = best_price_shop(bms, "totals")
        for pts, over_dict, under_dict in pair_lines({(n,p):d for (n,p),d in totals_map.items()}, ("Over","Under"), exact=True):
            po, pu = over_dict["price"], under_dict["price"]
            io, iu, fo, fu = fair_two_way(po, pu)
            for side, price, p, cell in [("Over", po, fo, over_dict), ("Under", pu, fu, under_dict)]:
                ev_u = p*price - 1.0; k = kelly(p, price)
                if ev_u >= MARKET_EV_MIN and k >= KELLY_MIN:
                    market_candidates.append({
                        "tour": tour_title.split()[0] if tour_title else "",
                        "player": f"{side} {pts}", "opponent": match_name,
                        "p": p, "odds": price, "display_odds": f"{side} {pts} @{price:.2f}",
                        "kelly": k, "ev": ev_u, "source": "Kelly Totals",
                        "start_local": start_local, "start_utc": start_utc,
                        "books": ", ".join(sorted(cell["books"]))
                    })

        # ---------- Spreads ----------
        spreads_map = best_price_shop(bms, "spreads")
        # Books vary names; any 2 outcomes at same line will be paired
        for pts, a_dict, b_dict in pair_lines({(n,p):d for (n,p),d in spreads_map.items()}, ("+","-"), exact=False):
            pa, pb = a_dict["price"], b_dict["price"]
            ia, ib, fa, fb = fair_two_way(pa, pb)
            for which, price, p, cell in [("A", pa, fa, a_dict), ("B", pb, fb, b_dict)]:
                ev_u = p*price - 1.0; k = kelly(p, price)
                if ev_u >= MARKET_EV_MIN and k >= KELLY_MIN:
                    market_candidates.append({
                        "tour": tour_title.split()[0] if tour_title else "",
                        "player": f"Spread {pts} ({which})", "opponent": match_name,
                        "p": p, "odds": price, "display_odds": f"{pts:+.1f} @{price:.2f}",
                        "kelly": k, "ev": ev_u, "source": "Kelly Spread",
                        "start_local": start_local, "start_utc": start_utc,
                        "books": ", ".join(sorted(cell["books"]))
                    })

    # ---------- Merge & shortlist ----------
    # Deduplicate by player/opponent/time; keep best EV first
    def dedup(rows: List[Dict]) -> List[Dict]:
        keyd={}
        for r in sorted(rows, key=lambda x: (-x["ev"], -x["kelly"], x["start_utc"])):
            k = (tuple(sorted([r["player"], r["opponent"]])), r["start_utc"], r["source"])
            if k not in keyd: keyd[k] = r
        return list(keyd.values())

    model_dedup = dedup(model_candidates)
    market_dedup = dedup(market_candidates)

    def pick(rows: List[Dict], dogs=True, n=3) -> List[Dict]:
        if dogs:
            filt = [r for r in rows if r["odds"] >= 2.20]
        else:
            filt = [r for r in rows if 1.30 <= r["odds"] <= 1.80]
        return sorted(filt, key=lambda r: (-r["ev"], -r["kelly"]))[:n]

    # Build sections
    sections=[]
    for tour in ["ATP","WTA"]:
        m_sub = [r for r in model_dedup if r.get("tour","").upper()==tour]
        dogs = pick(m_sub, dogs=True, n=TOP_DOGS)
        favs = pick(m_sub, dogs=False, n=TOP_FAVS)
        sections.append(shortlist_markdown(f"🏆 {tour} — Model Underdogs (Top {TOP_DOGS})", dogs))
        sections.append(shortlist_markdown(f"🛡 {tour} — Model Favorites (Top {TOP_FAVS})", favs))

    # Market picks across all ladders (ATP/WTA/Challenger/ITF)
    mkt_dogs = pick(market_dedup, dogs=True, n=TOP_DOGS)
    mkt_favs = pick(market_dedup, dogs=False, n=TOP_FAVS)
    sections.append(shortlist_markdown(f"📈 Market Edges — Underdogs (Top {TOP_DOGS})", mkt_dogs))
    sections.append(shortlist_markdown(f"📈 Market Edges — Favorites (Top {TOP_FAVS})", mkt_favs))

    write_summary(sections)

if __name__ == "__main__":
    run()
