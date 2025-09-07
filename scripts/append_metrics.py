#!/usr/bin/env python3
"""
Produce quick run metrics/summary.

Default I/O:
  IN  = outputs/edge_enriched.csv
  OUT = results/quick_metrics.csv

Behavior:
- Handles header-only / empty files gracefully.
- Computes simple aggregates and prints a concise log line.
"""

import csv
import os
from pathlib import Path
import argparse
from statistics import mean

# ---------- paths ----------
REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR   = REPO_ROOT / "outputs"
RES_DIR   = REPO_ROOT / "results"
INFILE    = OUT_DIR / "edge_enriched.csv"
OUTFILE   = RES_DIR / "quick_metrics.csv"

# ---------- env knobs (already exported by pipeline) ----------
MIN_EDGE_EV     = float(os.getenv("MIN_EDGE_EV", "0.02"))
MIN_PROBABILITY = float(os.getenv("MIN_PROBABILITY", "0.05"))

# ---------- utils ----------
def log(m: str) -> None:
    print(f"[append_metrics] {m}", flush=True)

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def csv_has_rows(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with path.open("r", encoding="utf-8") as f:
            r = csv.reader(f)
            _header = next(r, None)
            _first  = next(r, None)
            return bool(_first)
    except Exception:
        return False

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Summarize enriched edges")
    ap.add_argument("--input", default=str(INFILE))
    ap.add_argument("--output", default=str(OUTFILE))
    args = ap.parse_args()

    inp = Path(args.input)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    # Always create the output with a header
    with out.open("w", newline="", encoding="utf-8") as f:
        f.write("rows,avg_edge_a,avg_edge_b,positive_ev_a,positive_ev_b,threshold_ev,min_prob\n")

    if not csv_has_rows(inp):
        log(f"no input rows at {inp}; wrote header-only metrics")
        return

    rows = list(csv.DictReader(inp.open("r", encoding="utf-8")))
    # Extract edges safely
    edges_a = [v for v in (safe_float(r.get("edge_a")) for r in rows) if v is not None]
    edges_b = [v for v in (safe_float(r.get("edge_b")) for r in rows) if v is not None]

    # Simple counts above EV threshold (using MIN_EDGE_EV)
    pos_a = sum(1 for v in edges_a if v >= MIN_EDGE_EV)
    pos_b = sum(1 for v in edges_b if v >= MIN_EDGE_EV)

    avg_a = round(mean(edges_a), 6) if edges_a else 0.0
    avg_b = round(mean(edges_b), 6) if edges_b else 0.0

    with out.open("a", newline="", encoding="utf-8") as f:
        f.write(f"{len(rows)},{avg_a},{avg_b},{pos_a},{pos_b},{MIN_EDGE_EV},{MIN_PROBABILITY}\n")

    log(f"rows={len(rows)} avg_edge_a={avg_a} avg_edge_b={avg_b} "
        f"positive_ev_a={pos_a} positive_ev_b={pos_b} "
        f"(threshold_ev={MIN_EDGE_EV}, min_prob={MIN_PROBABILITY})")

if __name__ == "__main__":
    main()

