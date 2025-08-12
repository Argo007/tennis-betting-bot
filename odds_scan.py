#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tennis Kelly Sweet Spots Scanner
--------------------------------
Scans ATP/WTA/Challenger/ITF spreads & totals (pre-match only).
- Best-price shopping across books
- No-vig fair probabilities
- Kelly filter (full Kelly)
- CET/CEST-correct timestamps via zoneinfo
- Clean Markdown output in your existing column order

Author: Optimized by ChatGPT
"""

import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo
import requests

# =========================
# Config (env variables)
# =========================
API_KEY = os.getenv("ODDS_API_KEY", "").strip()

# Only include outcomes whose Full Kelly fraction >= this value
KELLY_THRESHOLD = float(os.getenv("KELLY_THRESHOLD", "0.10"))

# Pre-match lookahead window (hours)
LOOKAHEAD_HOURS = int(os.getenv("LOOKAHEAD_HOURS", "24"))

# Output file name
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "kelly_sweet_spots.md")

# Regions/books to search
REGIONS = os.getenv("REGIONS", "eu,uk,us")

# Markets to analyze
MARKETS = os.getenv("MARKETS", "spreads,totals")

# Tennis ladders
SPORTS = [
    "tennis_atp",
    "tennis_wta",
    "tennis_challenger",
    "tennis_itf_men",
    "tennis_itf_women",
]

ODDS_FORMAT = "decimal"
DATE_FORMAT = "iso"

# Local timezone for display
LOCAL_TZ = ZoneInfo(os.getenv("TZ", "Europe/Amsterdam"))


# =========================
# Helpers
# =========================
def within_window(commence_iso: str) -> bool:
    """True if match starts within [now, now+LOOKAHEAD_HOURS] UTC."""
    if not commence_iso:
        return False
    start = datetime.fromisoformat(commence_iso.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    return timedelta(0) <= (start - now) <= timedelta(hours=LOOKAHEAD_HOURS)

def fetch_odds(sport_key: str) -> List[Dict]:
    """Fetch upcoming odds for given sport key."""
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey": API_KEY,
        "regions": REGIONS,
        "markets": MARKETS,
        "oddsFormat": ODDS_FORMAT,
        "dateFormat": DATE_FORMAT,
    }
    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code == 200:
            data = r.json()
            return data if isinstance(data, list) else []
    except requests.RequestException:
        pass
    return []

def best_price_shop(bookmakers: List[Dict], market_key: str) -> Dict[Tuple[float, str], Dict]:
    """
    Track the best (max) odds across all books for each (points, outcome_name).
    Return {(points, outcome): {"price": float, "books": set(str)}}
    """
    grid: Dict[Tuple[float, str], Dict] = {}
    for bm in bookmakers:
        bm_name = bm.get("title", "?")
        for mk in bm.get("markets", []):
            if mk.get("key") != market_key:
                continue
            for out in mk.get("outcomes", []):
                price, points, name = out.get("price"), out.get("point"), out.get("name")
                if price is None or points is None or name is None:
                    continue
                key = (float(points), str(name))
                if key not in grid or price > grid[key]["price"]:
                    grid[key] = {"price": float(price), "books": {bm_name}}
                elif abs(price - grid[key]["price"]) < 1e-9:
                    grid[key]["books"].add(bm_name)
    return grid

def pair_two_way_by_line(price_map: Dict[Tuple[float, str], Dict],
                         outcomes_pair: Tuple[str, str]) -> List[Tuple[float, Dict, Dict]]:
    """
    Pair Over/Under or two spread sides for the same line.
    For totals: ('Over','Under') pairs cleanly.
    For spreads (book naming varies), fallback: if exactly 2 outcomes on a line, pair them.
    Returns: list of (points, sideA_dict, sideB_dict)
    """
    pairs = []
    by_pts: Dict[float, List[Tuple[str, Dict]]] = {}
    for (pts, name), cell in price_map.items():
        by_pts.setdefault(pts, []).append((name, cell))

    for pts, items in by_pts.items():
        a_name, b_name = outcomes_pair
        lookup = {name: cell for name, cell in items}
        if a_name in lookup and b_name in lookup:
            pairs.append((pts, lookup[a_name], lookup[b_name]))
        elif len(items) == 2:
            pairs.append((pts, items[0][1], items[1][1]))
    return pairs

def fair_probs_two_way(price_a: float, price_b: float):
    """Return raw implied & no-vig fair probabilities for a 2-way market."""
    ia, ib = 1.0 / price_a, 1.0 / price_b
    total = ia + ib
    if total <= 0:
        return 0.5, 0.5, 0.5, 0.5
    return ia, ib, ia / total, ib / total

def kelly_fraction(p_true: float, odds: float) -> float:
    """Full Kelly fraction."""
    b = odds - 1.0
    return max(0.0, (b * p_true - (1 - p_true)) / b) if b > 0 else 0.0

def local_time_strings(commence_iso: str) -> Tuple[str, str]:
    """Return local & UTC time strings with correct tz abbreviation (CET/CEST)."""
    start_utc = datetime.fromisoformat(commence_iso.replace("Z", "+00:00"))
    start_local = start_utc.astimezone(LOCAL_TZ)
    return (
        start_local.strftime(f"%Y-%m-%d %H:%M {start_local.tzname()}"),
        start_utc.strftime("%Y-%m-%d %H:%M UTC"),
    )

# =========================
# Markdown writer (your format preserved)
# =========================
def write_markdown(hits: List[Dict]):
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(LOCAL_TZ)
    header = (
        "# Tennis Kelly Sweet Spots (>= {thr:.2f})\n\n"
        "Last updated: {local}  \n"
        "({utc})\n\n"
    ).format(
        thr=KELLY_THRESHOLD,
        local=now_local.strftime(f'%Y-%m-%d %H:%M {now_local.tzname()}'),
        utc=now_utc.strftime('%Y-%m-%d %H:%M UTC'),
    )
    cols = ["Tournament","Match","Market","Line","Odds","p_mkt","p_adj","EV/u","Kelly","Start (CEST)","Start (UTC)","Book"]
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"]*len(cols)) + " |"]

    if not hits:
        empty_row = ["–"]*len(cols)
        empty_row[0] = f"No qualifying pre-match opportunities in the next {LOOKAHEAD_HOURS} hours"
        lines.append("| " + " | ".join(empty_row) + " |")
    else:
        for r in hits:
            row = [str(r.get(c, "")) for c in cols]
            lines.append("| " + " | ".join(row) + " |")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(header + "\n".join(lines) + "\n")
    print(f"Wrote {OUTPUT_FILE} with {len(hits)} qualifiers." if hits else f"Wrote {OUTPUT_FILE} (no qualifiers).")

# =========================
# Main scan
# =========================
def main():
    if not API_KEY:
        print("ERROR: ODDS_API_KEY not set.")
        return

    all_hits: List[Dict] = []

    for sport in SPORTS:
        for ev in fetch_odds(sport):
            commence = ev.get("commence_time")
            if not commence or not within_window(commence):
                continue

            tournament = ev.get("sport_title") or sport
            home, away = ev.get("home_team") or "", ev.get("away_team") or ""
            match_name = f"{away} vs {home}" if (home and away) else " vs ".join(ev.get("teams", []))
            books = ev.get("bookmakers", [])
            if not books:
                continue

            # --- Totals ---
            for pts, over_dict, under_dict in pair_two_way_by_line(best_price_shop(books, "totals"), ("Over", "Under")):
                over_price, under_price = over_dict["price"], under_dict["price"]
                p_mkt_o, p_mkt_u, p_fair_o, p_fair_u = fair_probs_two_way(over_price, under_price)

                for side, price, p_mkt, p_fair, bookset in [
                    ("Over", over_price, p_mkt_o, p_fair_o, over_dict["books"]),
                    ("Under", under_price, p_mkt_u, p_fair_u, under_dict["books"]),
                ]:
                    ev_u = p_fair * price - 1.0
                    kelly = kelly_fraction(p_fair, price)
                    if kelly >= KELLY_THRESHOLD:
                        start_local, start_utc = local_time_strings(commence)
                        all_hits.append({
                            "Tournament": tournament,
                            "Match": match_name,
                            "Market": "Totals",
                            "Line": pts,
                            "Odds": f"{price:.3f}",
                            "p_mkt": f"{p_mkt:.3f}",
                            "p_adj": f"{p_fair:.3f}",
                            "EV/u": f"{ev_u:.2f}",
                            "Kelly": f"{kelly:.3f}",
                            "Start (CEST)": start_local,
                            "Start (UTC)": start_utc,
                            "Book": ", ".join(sorted(bookset)),
                        })

            # --- Spreads ---
            for pts, a_dict, b_dict in pair_two_way_by_line(best_price_shop(books, "spreads"), ("+SideA", "-SideB")):
                price_a, price_b = a_dict["price"], b_dict["price"]
                p_mkt_a, p_mkt_b, p_fair_a, p_fair_b = fair_probs_two_way(price_a, price_b)

                for price, p_mkt, p_fair, bookset in [
                    (price_a, p_mkt_a, p_fair_a, a_dict["books"]),
                    (price_b, p_mkt_b, p_fair_b, b_dict["books"]),
                ]:
                    ev_u = p_fair * price - 1.0
                    kelly = kelly_fraction(p_fair, price)
                    if kelly >= KELLY_THRESHOLD:
                        start_local, start_utc = local_time_strings(commence)
                        all_hits.append({
                            "Tournament": tournament,
                            "Match": match_name,
                            "Market": "Spread",
                            "Line": pts,
                            "Odds": f"{price:.3f}",
                            "p_mkt": f"{p_mkt:.3f}",
                            "p_adj": f"{p_fair:.3f}",
                            "EV/u": f"{ev_u:.2f}",
                            "Kelly": f"{kelly:.3f}",
                            "Start (CEST)": start_local,
                            "Start (UTC)": start_utc,
                            "Book": ", ".join(sorted(bookset)),
                        })

    # Sort: highest Kelly first, then earliest UTC start
    all_hits.sort(key=lambda r: (-float(r["Kelly"]), r["Start (UTC)"]))
    write_markdown(all_hits)

if __name__ == "__main__":
    main()
