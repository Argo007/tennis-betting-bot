import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import unicodedata

# === Debug flag ===
DEBUG = True
def dbg(*args):
    if DEBUG:
        print("DEBUG:", *args)

# === Env vars ===
LOOKAHEAD_HOURS = int(os.getenv("LOOKAHEAD_HOURS", "24"))
KELLY_MIN = float(os.getenv("KELLY_MIN", "0.05"))
MODEL_EV_MIN = float(os.getenv("MODEL_EV_MIN", "0.00"))
MARKET_EV_MIN = float(os.getenv("MARKET_EV_MIN", "0.00"))
REGIONS = os.getenv("REGIONS", "eu").split(",")
MARKETS = os.getenv("MARKETS", "h2h,spreads,totals").split(",")
TOURS = os.getenv("TOURS", "ATP,WTA").split(",")
OUT_DIR = os.getenv("OUT_DIR", "outputs")
SHORTLIST_FILE = os.getenv("SHORTLIST_FILE", "value_engine_shortlist.md")
TOP_DOGS = int(os.getenv("TOP_DOGS", "3"))
TOP_FAVS = int(os.getenv("TOP_FAVS", "2"))

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
if not ODDS_API_KEY:
    raise SystemExit("ODDS_API_KEY not set")

os.makedirs(OUT_DIR, exist_ok=True)

# === Elo helpers ===
def _exp(a, b): return 1 / (1 + 10 ** ((b - a) / 400))

def norm_name(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    keep = set("abcdefghijklmnopqrstuvwxyz -.'")
    s = "".join(ch for ch in s.lower() if ch in keep)
    return " ".join(s.split())

def build_elo_index(path):
    df = pd.read_csv(path)
    idx = {}
    for _, r in df.iterrows():
        idx[norm_name(r["player"])] = r["elo"]
    return idx

elo_idx = {}
for tour in TOURS:
    fn = f"data/{tour.lower()}_elo.csv"
    if os.path.exists(fn):
        elo_idx[tour] = build_elo_index(fn)
    else:
        dbg(f"Elo file missing: {fn}")

def lookup_prob(p1, p2, tour):
    e1 = elo_idx.get(tour, {}).get(norm_name(p1))
    e2 = elo_idx.get(tour, {}).get(norm_name(p2))
    if e1 is None or e2 is None:
        return None
    return _exp(e1, e2)

# === Odds API fetch ===
def fetch_events():
    url = "https://api.the-odds-api.com/v4/sports/upcoming/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ",".join(REGIONS),
        "markets": ",".join(MARKETS),
        "oddsFormat": "decimal"
    }
    r = requests.get(url, params=params)
    if r.status_code != 200:
        raise SystemExit(f"Odds API error: {r.status_code} {r.text}")
    return r.json()

# === Main run ===
def run():
    now = datetime.now(timezone.utc)
    in_window = now + timedelta(hours=LOOKAHEAD_HOURS)

    events = fetch_events()
    dbg(f"Fetched {len(events)} events from API")

    picks = []

    for ev in events:
        sport = ev.get("sport_title", "")
        home = ev.get("home_team", "")
        away = ev.get("away_team", "")
        start = ev.get("commence_time")

        dbg("Event:", sport, away, "vs", home, "@", start)

        # window filter
        if not start:
            continue
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        if not (now <= start_dt <= in_window):
            continue

        # detect tour
        if "ATP" in sport:
            tour = "ATP"
        elif "WTA" in sport:
            tour = "WTA"
        else:
            continue

        # get odds
        for book in ev.get("bookmakers", []):
            for mk in book.get("markets", []):
                if mk.get("key") != "h2h":
                    continue
                outcomes = mk.get("outcomes", [])
                if len(outcomes) != 2:
                    continue
                p1, p2 = outcomes[0], outcomes[1]

                prob1 = lookup_prob(p1["name"], p2["name"], tour)
                prob2 = lookup_prob(p2["name"], p1["name"], tour)

                if prob1 is None or prob2 is None:
                    dbg("No Elo match for", p1["name"], p2["name"])
                    continue

                odds1, odds2 = float(p1["price"]), float(p2["price"])
                ev1 = prob1 * odds1 - 1
                ev2 = prob2 * odds2 - 1

                k1 = max(0, (prob1 * odds1 - (1 - prob1)) / odds1)
                k2 = max(0, (prob2 * odds2 - (1 - prob2)) / odds2)

                if ev1 >= MODEL_EV_MIN and k1 >= KELLY_MIN:
                    picks.append((tour, p1["name"], p2["name"], odds1, prob1, ev1, k1, start_dt))
                if ev2 >= MODEL_EV_MIN and k2 >= KELLY_MIN:
                    picks.append((tour, p2["name"], p1["name"], odds2, prob2, ev2, k2, start_dt))

    if not picks:
        dbg("No qualifying picks found.")
    else:
        dbg(f"Found {len(picks)} picks.")
        picks.sort(key=lambda x: x[5], reverse=True)
        lines = ["| Tour | Player | Opponent | Odds | Prob | EV/u | Kelly | Start (UTC) |",
                 "|------|--------|----------|------|------|------|-------|-------------|"]
        for t, p, o, od, pr, evu, kf, st in picks:
            lines.append(f"| {t} | {p} | {o} | {od:.2f} | {pr:.2f} | {evu:.3f} | {kf:.3f} | {st:%Y-%m-%d %H:%M} |")
        out_path = os.path.join(OUT_DIR, SHORTLIST_FILE)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        dbg(f"Wrote shortlist to {out_path}")

if __name__ == "__main__":
    run()
