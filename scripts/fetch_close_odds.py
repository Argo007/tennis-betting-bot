#!/usr/bin/env python3
"""
Fetch closing odds for today's matches and write a normalized CSV.

CLI:
  python scripts/fetch_close_odds.py --outdir data/raw/odds --odds oddsportal

Design goals:
- Robust in CI with no network or API keys.
- If a real provider is not configured, fall back to local sample odds or
  generate a small stub so downstream steps can proceed deterministically.
- Produce a clean, normalized schema used by later steps.

Output:
  <outdir>/close_odds_YYYYMMDD.csv

Schema (CSV):
  match_id,event_date,tournament,player_a,player_b,odds_a,odds_b,
  implied_prob_a,implied_prob_b,source,ts_utc

Notes:
- If you later wire a real provider, implement `_fetch_from_<provider>()`
  and return a list[dict] in the schema specified in `_normalize_rows`.
"""

import argparse
import csv
import hashlib
import os
from pathlib import Path
from datetime import datetime, timezone, date

# ---------- helpers ----------

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)

def today_str() -> str:
    return date.today().strftime("%Y%m%d")

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def implied_prob(odds_decimal: float):
    return 1.0 / odds_decimal if odds_decimal and odds_decimal > 0 else None

def make_match_id(tournament: str, player_a: str, player_b: str, event_date: str) -> str:
    # Deterministic ID for downstream joins
    raw = f"{tournament}|{player_a}|{player_b}|{event_date}".lower()
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

def write_csv(rows, outpath: Path) -> None:
    if not rows:
        raise RuntimeError("No odds rows to write.")
    fieldnames = [
        "match_id",
        "event_date",
        "tournament",
        "player_a",
        "player_b",
        "odds_a",
        "odds_b",
        "implied_prob_a",
        "implied_prob_b",
        "source",
        "ts_utc",
    ]
    with outpath.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    log(f"wrote {len(rows)} rows → {outpath}")

# ---------- providers (stubs / offline-safe) ----------

def _fetch_from_oddsportal() -> list[dict]:
    """
    Placeholder for a real integration. In CI/offline it tries to load
    data/raw/odds/sample_odds.csv (if available). If not found, generate
    a tiny deterministic stub for today's date.
    """
    repo_root = Path(__file__).resolve().parents[1]
    sample = repo_root / "data" / "raw" / "odds" / "sample_odds.csv"

    if sample.exists():
        log(f"loading sample odds from {sample}")
        rows = []
        with sample.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append({
                    "tournament": r.get("tournament", "Sample Cup"),
                    "player_a": r.get("player_a") or r.get("home") or "Player A",
                    "player_b": r.get("player_b") or r.get("away") or "Player B",
                    "odds_a": safe_float(r.get("odds_a") or r.get("home_odds") or r.get("odds1")),
                    "odds_b": safe_float(r.get("odds_b") or r.get("away_odds") or r.get("odds2")),
                    "event_date": r.get("event_date") or date.today().isoformat(),
                    "source": "oddsportal",
                })
        if rows:
            return rows

    # Fallback stub (two matches) – harmless but keeps pipeline alive
    log("sample_odds.csv not found or empty; generating stub close odds")
    d = date.today().isoformat()
    return [
        {
            "tournament": "Stub Open",
            "player_a": "Alpha",
            "player_b": "Beta",
            "odds_a": 1.90,
            "odds_b": 1.95,
            "event_date": d,
            "source": "oddsportal",
        },
        {
            "tournament": "Stub Open",
            "player_a": "Gamma",
            "player_b": "Delta",
            "odds_a": 2.40,
            "odds_b": 1.55,
            "event_date": d,
            "source": "oddsportal",
        },
    ]

# Add more providers as needed, following the same pattern:
# def _fetch_from_pinnacle(): ...
# def _fetch_from_bet365(): ...

PROVIDERS = {
    "oddsportal": _fetch_from_oddsportal,
    # "pinnacle": _fetch_from_pinnacle,
    # "bet365": _fetch_from_bet365,
}

# ---------- normalization ----------

def _normalize_rows(raw_rows: list[dict]) -> list[dict]:
    norm = []
    now = datetime.now(timezone.utc).isoformat()
    for r in raw_rows:
        tournament = (r.get("tournament") or "").strip() or "Unknown"
        a = (r.get("player_a") or r.get("home") or "Player A").strip()
        b = (r.get("player_b") or r.get("away") or "Player B").strip()
        odds_a = safe_float(r.get("odds_a") or r.get("home_odds"))
        odds_b = safe_float(r.get("odds_b") or r.get("away_odds"))
        event_date = (r.get("event_date") or date.today().isoformat()).strip()
        source = (r.get("source") or "unknown").strip()

        # Skip junk rows
        if not (odds_a and odds_b and odds_a > 1.0 and odds_b > 1.0):
            continue

        mpid = make_match_id(tournament, a, b, event_date)
        norm.append({
            "match_id": mpid,
            "event_date": event_date,
            "tournament": tournament,
            "player_a": a,
            "player_b": b,
            "odds_a": round(odds_a, 3),
            "odds_b": round(odds_b, 3),
            "implied_prob_a": round(implied_prob(odds_a), 6),
            "implied_prob_b": round(implied_prob(odds_b), 6),
            "source": source,
            "ts_utc": now,
        })
    return norm

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser(description="Fetch closing odds and write normalized CSV")
    ap.add_argument("--outdir", required=True, help="directory to write output CSV")
    ap.add_argument("--odds", required=True, help="odds source (e.g., oddsportal)")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    ensure_dir(outdir)

    provider = args.odds.strip().lower()
    if provider not in PROVIDERS:
        raise SystemExit(
            f"Unsupported odds provider '{provider}'. Supported: {', '.join(PROVIDERS.keys())}"
        )

    log(f"provider = {provider}")
    raw_rows = PROVIDERS[provider]()
    rows = _normalize_rows(raw_rows)

    if not rows:
        raise RuntimeError("No valid odds rows after normalization.")

    outpath = outdir / f"close_odds_{today_str()}.csv"
    write_csv(rows, outpath)

if __name__ == "__main__":
    main()
