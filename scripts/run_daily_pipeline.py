#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
End-to-end daily pipeline runner:
1) Ensure an enriched probability file (outputs/prob_enriched.csv).
2) Run the value engine to create picks + summaries.
3) Run matrix backtest on the enriched file (for ROI by band).
4) Leave everything in --outdir (default: outputs/).

This script is intentionally defensive:
- If the enriched CSV doesn't exist, it will try to build it from data/raw/odds/sample_odds.csv
  using scripts/compute_prob_vigfree.py.
- It never crashes the workflow on missing pieces; it logs and continues.

Inputs
------
--input       : path to enriched CSV (default outputs/prob_enriched.csv)
--outdir      : outputs directory (default outputs)
--stake-mode  : kelly | flat (default kelly)
--edge        : min edge threshold for engine/backtest (default 0.02)
--kelly-scale : Kelly scale (default 0.5)
--bankroll    : starting bankroll for backtest (default 1000)

Artifacts written (if successful)
---------------------------------
- outputs/prob_enriched.csv
- outputs/picks_final.csv
- outputs/engine_summary.md
- outputs/matrix_rankings.csv (if backtest produced rankings)
- outputs/backtest_metrics.json (if backtest produced metrics)
- outputs/pipeline_summary.md (built later by merge_report.py)
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent


def sh(cmd: list[str], check: bool = True) -> int:
    """Run a shell command, streaming output."""
    print("  $", " ".join(cmd), flush=True)
    return subprocess.run(cmd, check=check).returncode


def ensure_file(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="outputs/prob_enriched.csv", help="Enriched prob CSV")
    ap.add_argument("--outdir", default="outputs", help="Output directory")
    ap.add_argument("--stake-mode", default="kelly", choices=["kelly", "flat"])
    ap.add_argument("--edge", type=float, default=0.02)
    ap.add_argument("--kelly-scale", type=float, default=0.5)
    ap.add_argument("--bankroll", type=float, default=1000.0)
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    enriched = Path(args.input)
    sample_raw = ROOT / "data" / "raw" / "odds" / "sample_odds.csv"

    # ------------------------------------------------------------------
    # 1) Ensure enriched probability file
    # ------------------------------------------------------------------
    if not ensure_file(enriched):
        print(f"Enriched file {enriched} is missing. Attempting to build from {sample_raw}…")
        if not ensure_file(sample_raw):
            print(f"ERROR: No raw sample odds at {sample_raw}. Aborting enrichment.")
        else:
            # Compute vig-free probabilities from the raw odds.
            # compute_prob_vigfree.py accepts flexible schema but expects a price/odds column.
            sh([
                sys.executable,
                str(HERE / "compute_prob_vigfree.py"),
                "--input", str(sample_raw),
                "--output", str(enriched),
            ], check=False)

    if not ensure_file(enriched):
        print(f"WARNING: Still no {enriched}. The engine will likely yield 0 picks.")

    # ------------------------------------------------------------------
    # 2) Run value engine
    # ------------------------------------------------------------------
    print("\n== Run value engine ==")
    sh([
        sys.executable,
        str(HERE / "tennis_value_engine.py"),
        "--input", str(enriched),
        "--outdir", str(outdir),
        "--stake-mode", args.stake_mode,
        "--edge", f"{args.edge}",
        "--kelly-scale", f"{args.kelly_scale}",
        "--bankroll", f"{args.bankroll}",
    ], check=False)

    picks = outdir / "picks_final.csv"
    if ensure_file(picks):
        n_picks = sum(1 for _ in open(picks, "r", encoding="utf-8", newline=""))
        print(f"picks_final.csv exists with ~{max(0, n_picks-1)} rows (header included).")
    else:
        print("No picks_final.csv produced (likely 0 picks).")

    # ------------------------------------------------------------------
    # 3) Run matrix backtest on enriched file (ROI by band)
    # ------------------------------------------------------------------
    print("\n== Run matrix backtest (ROI by bands) ==")
    # Note: run_matrix_backtest.py reads an input CSV with at least odds/prob fields.
    # We pass the same enriched CSV; if it has the right fields, it will rank bands.
    sh([
        sys.executable,
        str(HERE / "run_matrix_backtest.py"),
        "--input", str(enriched),
        "--outdir", str(outdir),
        "--stake-mode", args.stake_mode,
        "--edge", f"{args.edge}",
        "--kelly-scale", f"{args.kelly_scale}",
        "--bankroll", f"{args.bankroll}",
    ], check=False)

    # ------------------------------------------------------------------
    # Done; merge_report.py will craft the final pipeline_summary.md
    # ------------------------------------------------------------------
    print("\nDaily pipeline finished. Check outputs/ for artifacts.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
