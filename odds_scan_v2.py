#!/usr/bin/env python3
import os, sys, json, math, time, datetime
import requests
from typing import Dict, List, Tuple, Optional

# ----- Config -----
API_KEY = os.getenv("ODDS_API_KEY", "").strip()
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "0.20"))   # +EV threshold (e.g., 0.20 = 20%)
WINDOW_HOURS = int(os.getenv("WINDOW_HOURS", "24"))           # lookahead window
REGIONS = "eu,uk,us"                                          # shop widely
ODDS_FORMAT = "decimal"
DATE_FORMAT = "iso"
# Sports keys supported by The Odds API (some books omit ITF; script will skip unknowns gracefully)
SPORTS = [
    "tennis_atp",
    "tennis_wta",
    "tennis_challenger",
    "tennis_itf_men",
    "tennis_itf_women",
]

# Markets: pre-match spreads & totals only (NO live)
MARKETS = "spreads,totals"

OUTPUT_FILE = "kelly_sweet_spots.md"

PUSHOVER_USER = os.getenv("PUSHOVER_USER_KEY", "").strip()
PUSHOVER_TOKEN = os.getenv("PUSHOVER_APP_TOKEN", "").strip()

# ----- Helpers -----
def iso_now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

def to_local_str(ts_iso: str, tz_name: str = os.getenv("TZ", "Europe/Amsterdam")) -> str:
    from datetime import timezone
    import pytz
    utc = datetime.datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
    local = utc.astimezone(pytz.timezone(tz_name))
    return local.strftime("%Y-%m-%d %H:%M")

def kelly_fraction(p: float, odds: float) -> float:
    """Full Kelly for decimal odds assuming true probability p."""
    b = odds - 1.0
    if b <= 0: 
        return 0.0
    f = (b * p - (1 - p)) / b
    return max(0.0, f)

def fair_probs_two_way(price_a: float, price_b: float) -> Tuple[float, float]:
    """Remove vig using inverse-odds normalization for a two-way market."""
    ia = 1.0 / price_a
    ib = 1.0 / price_b
    total = ia + ib
    if total == 0:
        return 0.5, 0.5
    return ia / total, ib / total

def http_get(url: str, params: Dict) -> Optional[requests.Response]:
    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code == 200:
            return r
        # Unknown sports keys return 404; just skip
        return None
    except requests.RequestException:
        return None

def fetch_odds_for_sport(sport_key: str) -> List[Dict]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = dict(
        apiKey=API_KEY,
        regions=REGIONS,
        markets=MARKETS,
        oddsFormat=ODDS_FORMAT,
        dateFormat=DATE_FORMAT,
        # No "live" flag: v4 returns upcoming pre-match by default
    )
    resp = http_get(url, params)
    return resp.json() if resp else []

def within_window(commence_iso: str) -> bool:
    now = iso_now_utc()
    start = datetime.datetime.fromisoformat(commence_iso.replace("Z", "+00:00"))
    delta = start - now
    return  datetime.timedelta(0) <= delta <= datetime.timedelta(hours=WINDOW_HOURS)

def collect_edges(events: List[Dict], sport_label: str) -> List[Dict]:
    rows = []
    for ev in events:
        commence = ev.get("commence_time")  # ISO
        if not commence or not within_window(commence):
            continue

        home = ev.get("home_team") or ""
        away = ev.get("away_team") or ""
        teams = f"{away} @ {home}" if home and away else ev.get("teams", ["?","?"])
        if isinstance(teams, list):
            teams = f"{teams[0]} vs {teams[1]}"

        # markets: spreads & totals; group by (market, line)
        markets = ev.get("bookmakers", [])
        # Build structure: {(market_key, points): {outcome_name: (best_price, {bookmaker names})}}
        grid: Dict[Tuple[str, float], Dict[str, Tuple[float, List[str]]]] = {}

        for bm in markets:
            bm_name = bm.get("title", "?")
            for mk in bm.get("markets", []):
                key = mk.get("key")  # 'spreads' or 'totals'
                if key not in ("spreads", "totals"):
                    continue
                for out in mk.get("outcomes", []):
                    price = out.get("price")
                    points = out.get("point")
                    name = out.get("name")  # 'Over'/'Under' or team name (+/-)
                    if price is None or points is None or name is None:
                        continue
                    k = (key, float(points))
                    d = grid.setdefault(k, {})
                    # track best price and who offers it
                    if name not in d or price > d[name][0]:
                        d[name] = (price, [bm_name])
                    elif abs(price - d[name][0]) < 1e-9:
                        d[name][1].append(bm_name)

        # Now compute fair probs & edges per (market,line,outcome)
        for (key, pts), outcomes in grid.items():
            # Need two outcomes on the same line to compute fair probs
            if len(outcomes) != 2:
                continue
            names = list(outcomes.keys())
            price_a, books_a = outcomes[names[0]]
            price_b, books_b = outcomes[names[1]]
            pa, pb = fair_probs_two_way(price_a, price_b)

            # Build two rows, compute edge & Kelly
            for name, price, p_true, books in [
                (names[0], price_a, pa, books_a),
                (names[1], price_b, pb, books_b),
            ]:
                edge = price * p_true - 1.0
                if edge >= EDGE_THRESHOLD:
                    rows.append({
                        "time_local": to_local_str(commence),
                        "sport": sport_label,
                        "match": teams,
                        "market": key,
                        "line": pts,
                        "outcome": name,
                        "best_odds": round(price, 3),
                        "fair_p": round(p_true, 3),
                        "edge_pct": round(edge * 100, 1),
                        "kelly_full": round(kelly_fraction(p_true, price), 3),
                        "books": ", ".join(sorted(set(books))),
                    })
    return rows

def pushover_push(title: str, msg: str):
    if not (PUSHOVER_USER and PUSHOVER_TOKEN):
        return
    try:
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token": PUSHOVER_TOKEN,
            "user": PUSHOVER_USER,
            "title": title,
            "message": msg[:1024],
            "priority": 0,
        }, timeout=10)
    except requests.RequestException:
        pass

def main():
    if not API_KEY:
        print("ERROR: ODDS_API_KEY not set.", file=sys.stderr)
        sys.exit(1)

    all_rows: List[Dict] = []
    for sport in SPORTS:
        events = fetch_odds_for_sport(sport)
        if not isinstance(events, list):
            continue
        rows = collect_edges(events, sport_label=sport)
        all_rows.extend(rows)

    # Sort by edge then time
    all_rows.sort(key=lambda r: (-r["edge_pct"], r["time_local"]))

    # Write Markdown
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    if all_rows:
        header = (
            f"# Kelly Sweet Spots (Spreads & Totals)\n\n"
            f"Updated: **{ts}**  •  Window: **{WINDOW_HOURS}h**  •  Threshold: **{int(EDGE_THRESHOLD*100)}%+**\n\n"
            "| Time (Local) | Sport | Match | Market | Line | Outcome | Best Odds | Fair p | Edge % | Kelly | Books |\n"
            "|---|---|---|---:|---:|---|---:|---:|---:|---:|---|\n"
        )
        lines = [
            f"| {r['time_local']} | {r['sport']} | {r['match']} | {r['market']} | {r['line']} | {r['outcome']} | "
            f"{r['best_odds']} | {r['fair_p']} | {r['edge_pct']} | {r['kelly_full']} | {r['books']} |"
            for r in all_rows
        ]
        content = header + "\n".join(lines) + "\n"
    else:
        content = (
            f"# Kelly Sweet Spots (Spreads & Totals)\n\n"
            f"Updated: **{ts}**\n\n"
            f"No +EV opportunities ≥ {int(EDGE_THRESHOLD*100)}% in the next {WINDOW_HOURS} hours.\n"
        )

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(content)

    # Push top edges (optional)
    if all_rows:
        top = all_rows[:5]
        msg = "\n".join(
            f"{r['time_local']} • {r['sport']} • {r['match']} • {r['market']} {r['line']} {r['outcome']} "
            f"@{r['best_odds']} • edge {r['edge_pct']}% • Kelly {r['kelly_full']}"
            for r in top
        )
        pushover_push("Tennis Kelly Scan", msg)

if __name__ == "__main__":
    main()
