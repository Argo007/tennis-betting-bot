#!/usr/bin/env python3
"""
Produce quick run metrics/summary.

Default I/O:
  IN  = outputs/edge_enriched.csv
  OUT = results/quick_metrics.csv  (and prints summary lines)
"""

import csv, os
from pathlib import Path
import argparse
from statistics import mean

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR   = REPO_ROOT / "outputs"
RES_DIR   = REPO_ROOT / "results"
INFILE    = OUT_DIR / "edge_enriched.csv"
OUTFILE   = RES_DIR / "quick_metrics.csv"

MIN_EDGE_EV     = float(os.getenv("MIN_EDGE_EV", "0.02"))
MIN_PROBABILITY = float(os.getenv("MIN_PROBABILITY", "0.05"))

def log(m): print(f"[append_metrics] {m}", flush=True)

def f(x):
    try: return float(x)
    except: return None

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
            f.write("count,avg_edge_a,avg_edge_b\n")
        log("no input; metrics header written")
        return

    rows = list(csv.DictReader(inp.open("r", encoding="utf-8")))
    edges_a = [f(r.get("edge_a")) for r in rows if f(r.get("edge_a")) is not None]
    edges_b = [f(r.get("edge_b")) for r in rows if f(r.get("edge_b")) is not None]

    with out.open("w", newline="", encoding="utf-8") as f:
        f.write("count,avg_edge_a,avg_edge_b\n")
        if edges_a or edges_b:
            f.write(f"{len(rows)},{round(mean(edges_a) if edges_a else 0,6)},{round(mean(edges_b) if edges_b else 0,6)}\n")

    log(f"rows={len(rows)}, avg_edge_a={round(mean(edges_a) if edges_a else 0,6)}, avg_edge_b={round(mean(edges_b) if edges_b else 0,6)}")

if __name__ == "__main__":
    main()
