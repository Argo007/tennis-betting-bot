#!/usr/bin/env python3
"""
Compute EdgeSmith/TrueEdge8 features and raw edges.

Default I/O:
  IN  = outputs/prob_enriched.csv
  OUT = outputs/edge_enriched.csv

Uses env metrics if present:
  WEIGHT_SURFACE_BOOST, WEIGHT_RECENT_FORM, WEIGHT_ELO_CORE, ...
"""

import csv, os
from pathlib import Path
import argparse
from math import isfinite

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR   = REPO_ROOT / "outputs"
INFILE    = OUT_DIR / "prob_enriched.csv"
OUTFILE   = OUT_DIR / "edge_enriched.csv"

def log(m): print(f"[edge_enrich] {m}", flush=True)

def f(x):
    try:
        v = float(x)
        return v if isfinite(v) else None
    except: return None

def w(name, default): 
    try: return float(os.getenv(name, default))
    except: return default

WEIGHTS = {
    "SURFACE":      w("WEIGHT_SURFACE_BOOST", 0.18),
    "RECENT":       w("WEIGHT_RECENT_FORM", 0.22),
    "ELO":          w("WEIGHT_ELO_CORE", 0.28),
    "SERVE_RETURN": w("WEIGHT_SERVE_RETURN_SPLIT", 0.10),
    "H2H":          w("WEIGHT_HEAD2HEAD", 0.06),
    "TRAVEL":       w("WEIGHT_TRAVEL_FATIGUE", -0.05),
    "INJURY":       w("WEIGHT_INJURY_PENALTY", -0.07),
    "DRIFT":        w("WEIGHT_MARKET_DRIFT", 0.08),
}

def pseudo_trueedge8(row):
    # Proxy scores (until you have real predictors wired)
    # Use available probs/odds to craft stable surrogates
    pa = f(row.get("prob_a_vigfree")) or 0.5
    pb = f(row.get("prob_b_vigfree")) or 0.5
    oa = f(row.get("odds_a")) or 2.0
    ob = f(row.get("odds_b")) or 2.0

    features = {
        "SURFACE":      0.0,      # placeholder hook
        "RECENT":       0.0,
        "ELO":          (pa - pb),             # as a crude proxy
        "SERVE_RETURN": (1/oa - 1/ob),
        "H2H":          0.0,
        "TRAVEL":       0.0,
        "INJURY":       0.0,
        "DRIFT":        (pb - pa),
    }
    score = sum(features[k]*WEIGHTS[k] for k in WEIGHTS)
    return score, features

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(INFILE))
    ap.add_argument("--output", default=str(OUTFILE))
    args = ap.parse_args()

    inp = Path(args.input)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    if not inp.exists():
        with out.open("w", newline="", encoding="utf-8") as f:
            f.write("event_date,tournament,player_a,player_b,odds_a,odds_b,prob_a_vigfree,prob_b_vigfree,trueedge8,edge_a,edge_b\n")
        log(f"missing input; wrote header-only → {out}")
        return

    rows = list(csv.DictReader(inp.open("r", encoding="utf-8")))
    enriched = []
    for r in rows:
        pa = f(r.get("prob_a_vigfree")); pb = f(r.get("prob_b_vigfree"))
        oa = f(r.get("odds_a")); ob = f(r.get("odds_b"))
        if None in (pa, pb, oa, ob): 
            continue

        te8, _ = pseudo_trueedge8(r)
        # expected value edges vs decimal odds
        ev_a = pa*oa - 1.0
        ev_b = pb*ob - 1.0
        r["trueedge8"] = round(te8, 6)
        r["edge_a"] = round(ev_a, 6)
        r["edge_b"] = round(ev_b, 6)
        enriched.append(r)

    if not enriched:
        with out.open("w", newline="", encoding="utf-8") as f:
            f.write("event_date,tournament,player_a,player_b,odds_a,odds_b,prob_a_vigfree,prob_b_vigfree,trueedge8,edge_a,edge_b\n")
        log(f"no rows; wrote header-only → {out}")
        return

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=enriched[0].keys())
        w.writeheader()
        w.writerows(enriched)
    log(f"wrote {len(enriched)} rows → {out}")

if __name__ == "__main__":
    main()
