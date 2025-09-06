#!/usr/bin/env python3
"""
Remove bookmaker vig from match odds and recompute true probabilities.

Usage:
    python scripts/compute_prob_vigfree.py \
        --input data/raw/historical_matches.csv \
        --output data/raw/vigfree_matches.csv

If no valid rows exist, writes a header-only file and exits 0 (warn).
"""

import argparse, csv, os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR   = REPO_ROOT / "data" / "raw"
DEFAULT_INPUT  = RAW_DIR / "historical_matches.csv"
DEFAULT_OUTPUT = RAW_DIR / "vigfree_matches.csv"

def log(msg): 
    print(f"[vigfree] {msg}", flush=True)

def ffloat(x):
    try: return float(x)
    except: return None

def iprob(odds):
    return 1.0 / odds if (odds is not None and odds > 0) else None

def vigfree_probs(o1, o2, method="shin"):
    p1, p2 = iprob(o1), iprob(o2)
    if p1 is None or p2 is None: return None, None
    over = p1 + p2
    if over <= 0: return None, None

    method = (method or "shin").lower()
    if method == "none":
        return p1, p2
    if method == "proportional":
        return p1/over, p2/over
    # shin-ish correction
    adj = max(0.0, over - 1.0) / 2.0
    denom = max(1e-9, 1.0 - adj)
    return (max(0.0, p1 - adj))/denom, (max(0.0, p2 - adj))/denom

def process(inp: Path, outp: Path, method="shin"):
    if not inp.exists():
        # create empty output and return 0
        outp.parent.mkdir(parents=True, exist_ok=True)
        with outp.open("w", newline="", encoding="utf-8") as f:
            f.write("event_date,tournament,player_a,player_b,odds_a,odds_b,implied_prob_a,implied_prob_b,odds_source,odds_kind,prob_a_vigfree,prob_b_vigfree\n")
        log(f"input missing; wrote header-only → {outp}")
        return

    rows = list(csv.DictReader(inp.open("r", encoding="utf-8")))
    out_rows = []
    for r in rows:
        oa, ob = ffloat(r.get("odds_a")), ffloat(r.get("odds_b"))
        if oa is None or ob is None or oa <= 1.0 or ob <= 1.0: 
            continue
        pa, pb = vigfree_probs(oa, ob, method)
        if pa is None or pb is None: 
            continue
        r["prob_a_vigfree"] = round(pa, 6)
        r["prob_b_vigfree"] = round(pb, 6)
        out_rows.append(r)

    outp.parent.mkdir(parents=True, exist_ok=True)
    if not out_rows:
        # header-only but success (lets pipeline continue)
        with outp.open("w", newline="", encoding="utf-8") as f:
            f.write("event_date,tournament,player_a,player_b,odds_a,odds_b,implied_prob_a,implied_prob_b,odds_source,odds_kind,prob_a_vigfree,prob_b_vigfree\n")
        log(f"no valid rows; wrote header-only → {outp}")
        return

    with outp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_rows[0].keys())
        writer.writeheader()
        writer.writerows(out_rows)
    log(f"wrote {len(out_rows)} rows → {outp}")

def main():
    ap = argparse.ArgumentParser(description="Compute vig-free probabilities")
    ap.add_argument("--input",  default=str(DEFAULT_INPUT),  help="Path to input CSV")
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Path to output CSV")
    ap.add_argument("--method", default=os.getenv("VIG_METHOD", "shin"),
                    help="Method: shin | proportional | none")
    a = ap.parse_args()
    process(Path(a.input), Path(a.output), a.method)

if __name__ == "__main__":
    main()

