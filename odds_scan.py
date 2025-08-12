#!/usr/bin/env python3
import os
import math
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
# (Optional) push is kept but disabled by default
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_APP_TOKEN = os.getenv("PUSHOVER_APP_TOKEN")

# ===== CONFIG =====
SPORT_KEYS = [
    "tennis_atp",
    "tennis_wta",
    "tennis_atp_challenger",
    "tennis_itf_men",
    "tennis_itf_women",
]
MARKETS = ["h2h", "spreads", "totals"]  # moneyline, handicaps, totals
REGIONS = "eu,us,uk"
ODDS_FORMAT = "decimal"
LOOKAHEAD_HOURS = 12
KELLY_THRESHOLD = 0.25  # report only if >= this
# Dynamic edge (absolute percentage points)
EDGE_FAVE_PP = 0.05   # <2.00
EDGE_MID_PP  = 0.04   # 2.00–3.00
EDGE_DOG_PP  = 0.025  # >3.00

OUTPUT_FILE = "kelly_sweet_spots.md"

# ===== HELPERS =====
def implied_prob_decimal(odds):
    return 1.0/odds if odds and odds > 0 else None

def normalize_probs(probs):
    s = sum(p for p in probs if p is not None)
    if s and s > 0:
        return [p/s for p in probs]
    return probs

def kelly_fraction(odds, p):
    b = odds - 1.0
    q = 1.0 - p
    return (b*p - q) / b

def edge_pp_for_price(odds):
    if odds < 2.0:
        return EDGE_FAVE_PP
    elif odds <= 3.0:
        return EDGE_MID_PP
    else:
        return EDGE_DOG_PP

def fetch_odds(sport_key, markets):
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    r = requests.get(url, params={
        "apiKey": ODDS_API_KEY,
        "regions": REGIONS,
        "markets": ",".join(markets),
        "oddsFormat": ODDS_FORMAT,
        "dateFormat": "iso",
    }, timeout=30)
    r.raise_for_status()
    return r.json()

def within_lookahead(start_iso):
    try:
        dt = datetime.fromisoformat(start_iso.replace("Z","+00:00"))
        now = datetime.now(timezone.utc)
        return now <= dt <= now + timedelta(hours=LOOKAHEAD_HOURS)
    except Exception:
        return False

def dual_times(start_iso):
    try:
        dt_utc = datetime.fromisoformat(start_iso.replace("Z","+00:00"))
        utc_str = dt_utc.strftime("%Y-%m-%d %H:%M UTC")
        # naive Europe/Amsterdam summer assumption (+02:00)
        dt_local = dt_utc + timedelta(hours=2)
        local_str = dt_local.strftime("%Y-%m-%d %H:%M CEST")
        return local_str, utc_str
    except Exception:
        return start_iso, start_iso


def analyze_event(ev):
    rows = []
    commence = ev.get("commence_time")
    if not within_lookahead(commence):
        return rows

    home = ev.get("home_team") or ""
    away = ev.get("away_team") or ""
    sport_title = ev.get("sport_title") or ""

    for b in ev.get("bookmakers", []):
        book = b.get("title")
        for mk in b.get("markets", []):
            mkey = mk.get("key")
            outcomes = mk.get("outcomes", [])
            if not outcomes:
                continue
            prices = [o.get("price") for o in outcomes]
            probs = normalize_probs([implied_prob_decimal(pr) for pr in prices])

            for o, pr, p_mkt in zip(outcomes, prices, probs):
                if pr is None or p_mkt is None:
                    continue
                p_adj = min(p_mkt + edge_pp_for_price(pr), 0.99)
                f = kelly_fraction(pr, p_adj)
                evu = p_adj * pr - 1.0
                local_str, utc_str = dual_times(commence)

                market = {"h2h":"moneyline","spreads":"spread","totals":"total_games"}.get(mkey, mkey)
                line = o.get("point") if mkey in ("spreads","totals") else ""

                # Heuristics:
                # - Totals: if point <= 7.5 we treat it as total_sets (e.g., 2.5, 3.5, 4.5).
                if mkey == "totals" and isinstance(line, (int,float)) and line <= 7.5:
                    market = "total_sets"

                # - Spreads: if abs(point) in {1.5, 2.5, 3.5}, label as set_spread (common in Bo3/Bo5).
                if mkey == "spreads" and isinstance(line, (int,float)) and abs(line) in (1.5, 2.5, 3.5):
                    market = "set_spread"

                rows.append({
                    "Tournament": sport_title,
                    "Match": f"{away} vs {home}",
                    "Market": market,
                    "Line": "" if line is None else line,
                    "Odds": round(pr,2),
                    "p_mkt": round(p_mkt,3),
                    "p_adj": round(p_adj,3),
                    "EV/u": round(evu,3),
                    "Kelly": round(f,3),
                    "Start (CEST)": local_str,
                    "Start (UTC)": utc_str,
                    "Book": book,
                })
    return rows

    home = ev.get("home_team") or ""
    away = ev.get("away_team") or ""
    sport_title = ev.get("sport_title") or ""

    for b in ev.get("bookmakers", []):
        book = b.get("title")
        for mk in b.get("markets", []):
            mkey = mk.get("key")
            outcomes = mk.get("outcomes", [])
            if not outcomes:
                continue
            prices = [o.get("price") for o in outcomes]
            probs = normalize_probs([implied_prob_decimal(pr) for pr in prices])

            for o, pr, p_mkt in zip(outcomes, prices, probs):
                if pr is None or p_mkt is None:
                    continue
                p_adj = min(p_mkt + edge_pp_for_price(pr), 0.99)
                f = kelly_fraction(pr, p_adj)
                evu = p_adj * pr - 1.0
                local_str, utc_str = dual_times(commence)

                market = {"h2h":"moneyline","spreads":"spread","totals":"total_games"}.get(mkey, mkey)
                line = o.get("point") if mkey in ("spreads","totals") else ""

                rows.append({
                    "Tournament": sport_title,
                    "Match": f"{away} vs {home}",
                    "Market": market,
                    "Line": "" if line is None else line,
                    "Odds": round(pr,2),
                    "p_mkt": round(p_mkt,3),
                    "p_adj": round(p_adj,3),
                    "EV/u": round(evu,3),
                    "Kelly": round(f,3),
                    "Start (CEST)": local_str,
                    "Start (UTC)": utc_str,
                    "Book": book,
                })
    return rows

def write_markdown(hits):
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc + timedelta(hours=2)  # CEST naive
    header = (
        f"# Tennis Kelly Sweet Spots (≥ {KELLY_THRESHOLD:.2f})\n\n"
        f"Last updated: {now_local.strftime('%Y-%m-%d %H:%M CEST')}  \n"
        f"({now_utc.strftime('%Y-%m-%d %H:%M UTC')})\n\n"
    )
    if not hits:
        body = "_No qualifying pre-match opportunities in the next {} hours._\n".format(LOOKAHEAD_HOURS)
        content = header + body
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Wrote {OUTPUT_FILE} (no qualifiers).")
        return

    cols = ["Tournament","Match","Market","Line","Odds","p_mkt","p_adj","EV/u","Kelly","Start (CEST)","Start (UTC)","Book"]
    lines = ["| " + " | ".join(cols) + " |",
             "| " + " | ".join(["---"]*len(cols)) + " |"]
    for r in hits:
        row = [str(r.get(c,"")) for c in cols]
        lines.append("| " + " | ".join(row) + " |")

    content = header + "\n".join(lines) + "\n"
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Wrote {OUTPUT_FILE} with {len(hits)} qualifiers.")

def main():
    if not ODDS_API_KEY:
        print("ERROR: Missing ODDS_API_KEY in environment.")
        return 2

    all_rows = []
    for key in SPORT_KEYS:
        try:
            data = fetch_odds(key, MARKETS)
        except Exception as e:
            print(f"[WARN] {key}: {e}")
            continue
        for ev in data or []:
            all_rows.extend(analyze_event(ev))

    hits = [r for r in all_rows if r.get("Kelly") is not None and r["Kelly"] >= KELLY_THRESHOLD]
    hits.sort(key=lambda x: x["EV/u"], reverse=True)

    write_markdown(hits)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
