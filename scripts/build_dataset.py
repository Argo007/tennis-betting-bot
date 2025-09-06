#!/usr/bin/env python3
"""
Build a unified dataset for the tennis-betting-bot.

Inputs (auto-discovered; no args required):
  - data/raw/tennis_data.csv                      (from fetch_tennis_data.py)
  - data/raw/odds/close_odds_*.csv               (from fetch_close_odds.py)
  - data/raw/odds/live_odds_*.csv                (from fetch_live_odds.py, optional)
  - data/raw/odds/*.csv with 'synthetic' in name (from fill_with_synthetic_live.py, optional)

Output:
  - data/raw/historical_matches.csv

Notes:
- We join odds to matches using (tournament, player_a, player_b, event_date).
- If tennis_data.csv is missing, we produce a small stub so pipeline stays alive.
- Odds priority is controlled by env: ODDS_PRIORITY="close,live,synthetic"
"""

import csv
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from glob import glob
from pathlib import Path
from typing import List, Dict, Tuple

# ---------- paths ----------
REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR   = REPO_ROOT / "data" / "raw"
ODDS_DIR  = RAW_DIR / "odds"
OUT_FILE  = RAW_DIR / "historical_matches.csv"

ODDS_PRIORITY = os.getenv("ODDS_PRIORITY", "close,live,synthetic").split(",")

# ---------- utils ----------
def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[build_dataset] {ts} {msg}", flush=True)

def ensure_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ODDS_DIR.mkdir(parents=True, exist_ok=True)

def today_iso() -> str:
    return date.today().isoformat()

def implied_prob(odds):
    try:
        odds = float(odds)
        return 1.0 / odds if odds > 0 else None
    except Exception:
        return None

def norm_name(x: str) -> str:
    return (x or "").strip()

def make_key(tournament, pa, pb, d) -> Tuple[str, str, str, str]:
    return (norm_name(tournament).lower(), norm_name(pa).lower(), norm_name(pb).lower(), (d or "").strip())

# ---------- IO helpers ----------
def read_csv(path: Path) -> List[Dict]:
    with path.open("r", encoding="utf-8") as f:
        r = list(csv.DictReader(f))
    return r

def write_csv(rows: List[Dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise RuntimeError("No rows to write.")
    fields = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})
    log(f"wrote {len(rows)} rows → {path}")

# ---------- data sources ----------
def load_tennis_data() -> List[Dict]:
    td = RAW_DIR / "tennis_data.csv"
    if td.exists():
        rows = read_csv(td)
        log(f"loaded matches: {len(rows)} rows from {td}")
        return rows

    # Stub fallback (if upstream is offline)
    log("tennis_data.csv missing; generating stub matches")
    d = today_iso()
    return [
        {"event_date": d, "tournament": "Stub Open", "player_a": "Alpha", "player_b": "Beta"},
        {"event_date": d, "tournament": "Stub Open", "player_a": "Gamma", "player_b": "Delta"},
    ]

def load_odds() -> Dict[Tuple[str, str, str, str], Dict]:
    """
    Returns best-available odds per match key according to ODDS_PRIORITY.
    Key: (tournament, player_a, player_b, event_date) all lowercased.
    """
    files_by_kind = {
        "close":     sorted(glob(str(ODDS_DIR / "close_odds_*.csv"))),
        "live":      sorted(glob(str(ODDS_DIR / "live_odds_*.csv"))),
        "synthetic": sorted([p for p in glob(str(ODDS_DIR / "*.csv")) if "synthetic" in Path(p).name.lower()]),
    }

    def rows_from(paths: List[str]) -> List[Dict]:
        rows: List[Dict] = []
        for p in paths:
            try:
                rows.extend(read_csv(Path(p)))
            except Exception as e:
                log(f"warn: failed to read {p}: {e}")
        return rows

    book: Dict[Tuple[str, str, str, str], Dict] = {}
    total = 0
    for kind in [k.strip().lower() for k in ODDS_PRIORITY]:
        paths = files_by_kind.get(kind, [])
        if not paths:
            continue
        rows = rows_from(paths)
        total += len(rows)
        for r in rows:
            t = r.get("tournament") or r.get("event") or "Unknown"
            a = r.get("player_a") or r.get("home") or "Player A"
            b = r.get("player_b") or r.get("away") or "Player B"
            d = r.get("event_date") or today_iso()
            key = make_key(t, a, b, d)
            # Only set if not already present by a higher-priority type
            if key not in book:
                # normalize odds
                try:
                    oa = float(r.get("odds_a", 0))
                    ob = float(r.get("odds_b", 0))
                except Exception:
                    oa, ob = 0.0, 0.0
                if oa > 1.0 and ob > 1.0:
                    book[key] = {
                        "odds_a": round(oa, 6),
                        "odds_b": round(ob, 6),
                        "implied_prob_a": round(implied_prob(oa) or 0.0, 6),
                        "implied_prob_b": round(implied_prob(ob) or 0.0, 6),
                        "odds_source": r.get("source", kind),
                        "odds_kind": kind,
                    }
    log(f"aggregated {total} odds rows across kinds; unique keys = {len(book)}")
    return book

# ---------- build ----------
def build_dataset():
    ensure_dirs()
    matches = load_tennis_data()
    odds_map = load_odds()

    out_rows: List[Dict] = []
    missing_odds = 0
    for m in matches:
        t = m.get("tournament") or "Unknown"
        a = m.get("player_a") or m.get("home") or "Player A"
        b = m.get("player_b") or m.get("away") or "Player B"
        d = (m.get("event_date") or today_iso()).strip()
        key = make_key(t, a, b, d)

        base = {
            "event_date": d,
            "tournament": norm_name(t),
            "player_a": norm_name(a),
            "player_b": norm_name(b),
        }

        if key in odds_map:
            base.update(odds_map[key])
        else:
            # keep row but mark missing odds; downstream may drop these
            missing_odds += 1
            base.update({
                "odds_a": "",
                "odds_b": "",
                "implied_prob_a": "",
                "implied_prob_b": "",
                "odds_source": "",
                "odds_kind": "",
            })

        out_rows.append(base)

    log(f"assembled dataset: {len(out_rows)} rows (missing_odds={missing_odds})")
    write_csv(out_rows, OUT_FILE)

# ---------- main ----------
if __name__ == "__main__":
    try:
        build_dataset()
        log("done.")
    except Exception as e:
        log(f"FATAL: {e}")
        raise

