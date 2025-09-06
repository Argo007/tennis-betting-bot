#!/usr/bin/env python3
"""
Build a unified dataset for the tennis-betting-bot.

Inputs (auto-discovered; no args required):
  - data/raw/tennis_data.csv
  - data/raw/odds/close_odds_*.csv
  - data/raw/odds/live_odds_*.csv
  - data/raw/odds/*synthetic*.csv

Output:
  - data/raw/historical_matches.csv

Logic:
- Try to join odds onto matches via (tournament, player_a, player_b, event_date),
  using normalized case/whitespace.
- If 0 rows end up with odds, FALL BACK to building the dataset directly from
  the available odds files (so downstream steps aren't empty).
"""

import csv
import os
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

def iprob(odds):
    try:
        odds = float(odds)
        return 1.0 / odds if odds > 0 else None
    except Exception:
        return None

def norm(x: str) -> str:
    return (x or "").strip().lower()

def key4(t, a, b, d) -> Tuple[str, str, str, str]:
    return (norm(t), norm(a), norm(b), (d or "").strip())

# ---------- IO helpers ----------
def read_csv(path: Path) -> List[Dict]:
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(rows: List[Dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        # still write a header-only file to keep pipeline moving
        with path.open("w", newline="", encoding="utf-8") as f:
            f.write("event_date,tournament,player_a,player_b,odds_a,odds_b,implied_prob_a,implied_prob_b,odds_source,odds_kind\n")
        log(f"wrote 0 rows (header only) → {path}")
        return
    fields = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})
    log(f"wrote {len(rows)} rows → {path}")

# ---------- sources ----------
def load_matches() -> List[Dict]:
    td = RAW_DIR / "tennis_data.csv"
    if td.exists():
        rows = read_csv(td)
        log(f"loaded matches: {len(rows)} from {td}")
        return rows
    # Stub if fetch step produced nothing
    log("tennis_data.csv missing; generating stub matches")
    d = today_iso()
    return [
        {"event_date": d, "tournament": "Stub Open", "player_a": "Alpha", "player_b": "Beta"},
        {"event_date": d, "tournament": "Stub Open", "player_a": "Gamma", "player_b": "Delta"},
    ]

def load_odds_priority_map() -> Dict[Tuple[str, str, str, str], Dict]:
    files_by_kind = {
        "close":     sorted(glob(str(ODDS_DIR / "close_odds_*.csv"))),
        "live":      sorted(glob(str(ODDS_DIR / "live_odds_*.csv"))),
        "synthetic": sorted([p for p in glob(str(ODDS_DIR / "*.csv")) if "synthetic" in Path(p).name.lower()]),
    }

    def rows_from(paths: List[str]) -> List[Dict]:
        out: List[Dict] = []
        for p in paths:
            try:
                out.extend(read_csv(Path(p)))
            except Exception as e:
                log(f"warn: failed to read {p}: {e}")
        return out

    book: Dict[Tuple[str, str, str, str], Dict] = {}
    seen = 0
    for kind in [k.strip().lower() for k in ODDS_PRIORITY]:
        paths = files_by_kind.get(kind, [])
        if not paths:
            continue
        rows = rows_from(paths)
        seen += len(rows)
        for r in rows:
            t = r.get("tournament") or r.get("event") or "Unknown"
            a = r.get("player_a") or r.get("home") or "Player A"
            b = r.get("player_b") or r.get("away") or "Player B"
            d = r.get("event_date") or today_iso()
            oa = r.get("odds_a"); ob = r.get("odds_b")
            try:
                oa = float(oa); ob = float(ob)
            except Exception:
                continue
            if oa <= 1.0 or ob <= 1.0:
                continue
            k = key4(t, a, b, d)
            if k not in book:  # keep first by priority
                book[k] = {
                    "tournament": t, "player_a": a, "player_b": b, "event_date": d,
                    "odds_a": round(oa, 6), "odds_b": round(ob, 6),
                    "implied_prob_a": round(iprob(oa) or 0.0, 6),
                    "implied_prob_b": round(iprob(ob) or 0.0, 6),
                    "odds_source": r.get("source", kind), "odds_kind": kind,
                }
    log(f"aggregated odds rows seen={seen}, unique keys={len(book)}")
    return book

# ---------- build ----------
def build_dataset():
    ensure_dirs()
    matches = load_matches()
    odds_map = load_odds_priority_map()

    out_rows: List[Dict] = []
    got_odds = 0
    for m in matches:
        t = m.get("tournament") or "Unknown"
        a = m.get("player_a") or m.get("home") or "Player A"
        b = m.get("player_b") or m.get("away") or "Player B"
        d = (m.get("event_date") or today_iso()).strip()

        base = {
            "event_date": d,
            "tournament": (t or "").strip(),
            "player_a": (a or "").strip(),
            "player_b": (b or "").strip(),
            "odds_a": "",
            "odds_b": "",
            "implied_prob_a": "",
            "implied_prob_b": "",
            "odds_source": "",
            "odds_kind": "",
        }

        k = key4(t, a, b, d)
        if k in odds_map:
            o = odds_map[k]
            base.update({
                "odds_a": o["odds_a"], "odds_b": o["odds_b"],
                "implied_prob_a": o["implied_prob_a"], "implied_prob_b": o["implied_prob_b"],
                "odds_source": o["odds_source"], "odds_kind": o["odds_kind"],
            })
            got_odds += 1

        out_rows.append(base)

    log(f"assembled dataset from matches: rows={len(out_rows)}, with_odds={got_odds}")

    # Fallback: if none of the matches had odds, emit dataset built from odds directly
    if got_odds == 0 and odds_map:
        log("no matched odds; falling back to dataset-from-odds")
        out_rows = []
        for k, o in odds_map.items():
            out_rows.append({
                "event_date": o["event_date"],
                "tournament": o["tournament"],
                "player_a": o["player_a"],
                "player_b": o["player_b"],
                "odds_a": o["odds_a"],
                "odds_b": o["odds_b"],
                "implied_prob_a": o["implied_prob_a"],
                "implied_prob_b": o["implied_prob_b"],
                "odds_source": o["odds_source"],
                "odds_kind": o["odds_kind"],
            })
        log(f"dataset-from-odds rows={len(out_rows)}")

    write_csv(out_rows, OUT_FILE)

# ---------- main ----------
if __name__ == "__main__":
    try:
        build_dataset()
        log("done.")
    except Exception as e:
        log(f"FATAL: {e}")
        raise
