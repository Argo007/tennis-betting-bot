#!/usr/bin/env python3
"""
Remove bookmaker vig from match odds and recompute true probabilities.

Usage (pipeline calls this without args, with defaults below):
    python scripts/compute_prob_vigfree.py \
        --input data/raw/historical_matches.csv \
        --output data/raw/vigfree_matches.csv

You can override via CLI flags if you like.
The method defaults to env VIG_METHOD or 'shin'.
"""

import argparse
import csv
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR   = REPO_ROOT / "data" / "raw"
DEFAULT_INPUT  = RAW_DIR / "historical_matches.csv"
DEFAULT_OUTPUT = RAW_DIR / "vigfree_matches.csv"

def log(msg): 
    print(f"[vigfree] {msg}", flush=True)

def safe_float(x, default=None):
    try:
        return float(x)
    except:
        return default

def implied_prob(odds):
    return 1.0 / odds if odds and odds > 0 else None

def vigfree_probs(o1, o2, method="shin"):
    """
    Removes vig from two-way odds markets.
    Methods: 'shin', 'proportional', 'none'
    """
    p1 = implied_prob(o1)
    p2 = implied_prob(o2)
    if p1 is None or p2 is None:
        return None, None
    overround = p1 + p2
    if overround <= 0:
        return None, None

    if method == "none":
        return p1, p2
    elif method == "proportional":
        return p1 / overround, p2 / overround
    elif method == "shin":
        # Shin: proportional plus mild correction; simple variant for robustness
        adj = max(0.0, overround - 1.0) / 2.0
        denom = max(1e-9, 1.0 - adj)
        return (max(0.0, p1 - adj)) / denom, (max(0.0, p2 - adj)) / denom
    else:
        # fallback to proportional
        return p1 / overround, p2 / overround

def process(input_csv: Path, output_csv: Path, method="shin"):
    if not input_csv.exists():
        raise FileNotFoundError(f"Input file not found: {input_csv}")

    with input_csv.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        raise RuntimeError("Input dataset is empty.")

    out_rows = []
    dropped = 0
    for r in rows:
        oa = safe_float(r.get("odds_a"))
        ob = safe_float(r.get("odds_b"))
        if oa is None or ob is None or oa <= 1.0 or ob <= 1.0:
            dropped += 1
            continue

        pa, pb = vigfree_probs(oa, ob, method=method)
        if pa is None or pb is None:
            dropped += 1
            continue

        r["prob_a_vigfree"] = round(pa, 6)
        r["prob_b_vigfree"] = round(pb, 6)
        out_rows.append(r)

    if not out_rows:
        raise RuntimeError("No rows with valid vigfree probabilities.")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_rows[0].keys())
        writer.writeheader()
        writer.writerows(out_rows)

    log(f"wrote {len(out_rows)} rows → {output_csv} (dropped={dropped})")

def main():
    ap = argparse.ArgumentParser(description="Compute vig-free probabilities")
    ap.add_argument("--input", default=str(DEFAULT_INPUT), help="Path to input CSV")
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Path to output CSV")
    ap.add_argument("--method", default=os.getenv("VIG_METHOD", "shin"),
                    help="Method: shin | proportional | none (default from env VIG_METHOD)")
    args = ap.parse_args()

    process(Path(args.input), Path(args.output), method=args.method)

if __name__ == "__main__":
    main()

