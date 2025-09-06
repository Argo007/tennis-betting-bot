#!/usr/bin/env python3
"""
Validate vig-free probabilities and emit a clean, enriched file.

Default I/O:
  IN  = data/raw/vigfree_matches.csv
  OUT = outputs/prob_enriched.csv

Behavior:
- Drops rows without valid vig-free probs.
- Ensures probs in [0,1] and pA+pB≈1 (tolerant).
"""

import csv
from pathlib import Path
import argparse, math, os

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR   = REPO_ROOT / "data" / "raw"
OUT_DIR   = REPO_ROOT / "outputs"
INFILE    = RAW_DIR / "vigfree_matches.csv"
OUTFILE   = OUT_DIR / "prob_enriched.csv"

def log(m): print(f"[check_probs] {m}", flush=True)

def clamp01(x):
    try:
        x = float(x)
        if x < 0: return 0.0
        if x > 1: return 1.0
        return x
    except:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(INFILE))
    ap.add_argument("--output", default=str(OUTFILE))
    args = ap.parse_args()

    inp = Path(args.input)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    if not inp.exists():
        # Write header-only to keep pipeline going
        with out.open("w", newline="", encoding="utf-8") as f:
            f.write("event_date,tournament,player_a,player_b,odds_a,odds_b,implied_prob_a,implied_prob_b,odds_source,odds_kind,prob_a_vigfree,prob_b_vigfree\n")
        log(f"input missing; wrote header-only → {out}")
        return

    rows = list(csv.DictReader(inp.open("r", encoding="utf-8")))
    cleaned = []
    for r in rows:
        pa = clamp01(r.get("prob_a_vigfree"))
        pb = clamp01(r.get("prob_b_vigfree"))
        if pa is None or pb is None: 
            continue
        # normalize lightly
        s = pa + pb
        if s > 0:
            pa, pb = pa/s, pb/s
        r["prob_a_vigfree"] = round(pa, 6)
        r["prob_b_vigfree"] = round(pb, 6)
        cleaned.append(r)

    if not cleaned:
        with out.open("w", newline="", encoding="utf-8") as f:
            f.write("event_date,tournament,player_a,player_b,odds_a,odds_b,implied_prob_a,implied_prob_b,odds_source,odds_kind,prob_a_vigfree,prob_b_vigfree\n")
        log(f"no valid rows; wrote header-only → {out}")
        return

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cleaned[0].keys())
        w.writeheader()
        w.writerows(cleaned)
    log(f"wrote {len(cleaned)} rows → {out}")

if __name__ == "__main__":
    main()
