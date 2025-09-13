#!/usr/bin/env python3
"""
Prepare a backtest-ready dataset in outputs/prob_enriched.csv.

If outputs/prob_enriched.csv is missing/empty, build it from scratch:
- fetch_tennis_data.py
- fetch_close_odds.py
- fill_with_synthetic_live.py
- build_from_raw.py
- build_dataset.py
- ensure_dataset.py
- compute_prob_vigfree.py  (→ data/raw/vigfree_matches.csv)
- check_probabilities.py   (→ outputs/prob_enriched.csv)

Exit non-zero if we still don't have usable rows at the end.
"""

from __future__ import annotations
import csv, sys
from pathlib import Path
from subprocess import run, CalledProcessError

ROOT = Path(__file__).resolve().parents[1]
RAW  = ROOT / "data" / "raw"
ODDS = RAW / "odds"
OUT  = ROOT / "outputs"

def csv_has_rows(p: Path) -> bool:
    if not p.exists() or p.stat().st_size == 0:
        return False
    with p.open("r", encoding="utf-8") as f:
        rdr = csv.reader(f)
        try:
            header = next(rdr)
        except StopIteration:
            return False
        for _ in rdr:
            return True
    return False

def sh(cmd: list[str]):
    print("→", " ".join(cmd), flush=True)
    r = run(cmd, check=True)
    return r

def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    RAW.mkdir(parents=True, exist_ok=True)
    ODDS.mkdir(parents=True, exist_ok=True)

    target = OUT / "prob_enriched.csv"
    if csv_has_rows(target):
        print(f"[prep] found dataset with rows: {target}")
        return 0

    print("[prep] dataset missing/empty; building…")

    # Build the raw + dataset
    sh(["python", "scripts/fetch_tennis_data.py", "--outdir", str(RAW)])
    sh(["python", "scripts/fetch_close_odds.py", "--odds", "oddsportal", "--outdir", str(ODDS)])
    sh(["python", "scripts/fill_with_synthetic_live.py", "--outdir", str(ODDS)])
    sh(["python", "scripts/build_from_raw.py"])
    sh(["python", "scripts/build_dataset.py"])
    sh(["python", "scripts/ensure_dataset.py"])

    # Convert to vig-free probabilities and sanity-check
    sh([
        "python", "scripts/compute_prob_vigfree.py",
        "--input", str(RAW / "historical_matches.csv"),
        "--output", str(RAW / "vigfree_matches.csv"),
        "--method", "shin"
    ])
    sh([
        "python", "scripts/check_probabilities.py",
        "--input", str(RAW / "vigfree_matches.csv"),
        "--output", str(OUT / "prob_enriched.csv"),
    ])

    if not csv_has_rows(target):
        print("[prep] ERROR: outputs/prob_enriched.csv still empty after build.", file=sys.stderr)
        return 2

    print(f"[prep] ready: {target}")
    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CalledProcessError as e:
        print(f"[prep] subprocess failed with code {e.returncode}", file=sys.stderr)
        raise
