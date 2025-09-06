#!/usr/bin/env python3
"""
Fetch live odds snapshot and write a normalized CSV.

CLI:
  python scripts/fetch_live_odds.py --outdir data/raw/odds --odds oddsportal

Output:
  data/raw/odds/live_odds_YYYYMMDD_HHMM.csv

Schema (CSV):
  match_id,event_date,tournament,player_a,player_b,odds_a,odds_b,
  implied_prob_a,implied_prob_b,source,ts_utc
"""

import argparse, csv, hashlib
from datetime import datetime, timezone, date
from pathlib import Path

def log(msg): 
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"); print(f"[{ts}] {msg}", flush=True)

def now_stamp():
    return datetime.now().strftime("%Y%m%d_%H%M")

def implied_prob(x): 
    try:
        x = float(x); 
        return (1.0 / x) if x > 0 else None
    except Exception:
        return None

def make_match_id(tournament, a, b, d):
    raw = f"{tournament}|{a}|{b}|{d}".lower()
    return hashlib.sha1(raw.encode()).hexdigest()[:16]

# --- providers (offline-safe) ---
def _fetch_from_oddsportal():
    """
    Stub provider for CI/offline. Produces deterministic tiny set.
    Replace with real scraping/API when ready.
    """
    d = date.today().isoformat()
    return [
        {"tournament":"Stub Live", "player_a":"Alpha", "player_b":"Beta",  "odds_a":1.83, "odds_b":2.02, "event_date":d, "source":"oddsportal"},
        {"tournament":"Stub Live", "player_a":"Gamma", "player_b":"Delta", "odds_a":2.65, "odds_b":1.48, "event_date":d, "source":"oddsportal"},
    ]

PROVIDERS = {"oddsportal": _fetch_from_oddsportal}

def normalize(raw_rows):
    out = []
    ts = datetime.now(timezone.utc).isoformat()
    for r in raw_rows:
        t = (r.get("tournament") or "Unknown").strip()
        a = (r.get("player_a") or "Player A").strip()
        b = (r.get("player_b") or "Player B").strip()
        oa = float(r.get("odds_a", 0) or 0)
        ob = float(r.get("odds_b", 0) or 0)
        if oa <= 1.0 or ob <= 1.0: 
            continue
        d = (r.get("event_date") or date.today().isoformat()).strip()
        mid = make_match_id(t, a, b, d)
        out.append({
            "match_id": mid,
            "event_date": d,
            "tournament": t,
            "player_a": a,
            "player_b": b,
            "odds_a": round(oa,3),
            "odds_b": round(ob,3),
            "implied_prob_a": round(implied_prob(oa),6),
            "implied_prob_b": round(implied_prob(ob),6),
            "source": (r.get("source") or "unknown"),
            "ts_utc": ts,
        })
    return out

def write_csv(rows, outpath: Path):
    outpath.parent.mkdir(parents=True, exist_ok=True)
    fields = ["match_id","event_date","tournament","player_a","player_b",
              "odds_a","odds_b","implied_prob_a","implied_prob_b","source","ts_utc"]
    with outpath.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k:r.get(k,"") for k in fields})
    log(f"wrote {len(rows)} rows → {outpath}")

def main():
    ap = argparse.ArgumentParser(description="Fetch live odds snapshot")
    ap.add_argument("--outdir", required=True, help="directory to write output CSV")
    ap.add_argument("--odds", required=True, help="odds source (e.g., oddsportal)")
    args = ap.parse_args()

    provider = args.odds.strip().lower()
    if provider not in PROVIDERS:
        raise SystemExit(f"Unsupported odds provider '{provider}'. Supported: {', '.join(PROVIDERS.keys())}")

    raw = PROVIDERS[provider]()
    rows = normalize(raw)
    if not rows:
        raise RuntimeError("No live odds produced.")
    out = Path(args.outdir) / f"live_odds_{now_stamp()}.csv"
    write_csv(rows, out)

if __name__ == "__main__":
    main()
