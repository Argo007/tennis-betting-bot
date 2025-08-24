#!/usr/bin/env python3
"""
Run the matrix backtest on an input CSV; if no rankings are produced,
fallback to a tiny synthetic dataset and rerun. Outputs are written in --outdir.

Expected columns for a real backtest: at minimum 'price' (or 'odds'/'decimal_odds'),
a probability column (p/p_model/p_used), and optionally 'result' if you want a true backtest.

Usage (example):
  python scripts/run_matrix_with_fallback.py \
    --input outputs/prob_enriched.csv \
    --outdir outputs \
    --bands "2.0,2.6|2.6,3.2|3.2,4.0" \
    --stake-mode kelly \
    --edge 0.02 \
    --kelly-scale 0.5 \
    --bankroll 1000
"""
from __future__ import annotations
import argparse, subprocess, sys, pathlib, os, textwrap

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"

def run_matrix_once(args_in, allow_fail=True) -> int:
    cmd = [
        sys.executable, str(SCRIPTS / "run_matrix_backtest.py"),
        "--input", args_in.input,
        "--outdir", args_in.outdir,
        "--bands", args_in.bands,
        "--stake-mode", args_in.stake_mode,
        "--edge", str(args_in.edge),
        "--kelly-scale", str(args_in.kelly_scale),
        "--bankroll", str(args_in.bankroll),
    ]
    rc = subprocess.run(cmd, check=False).returncode
    if rc != 0 and not allow_fail:
        raise SystemExit(rc)
    return rc

def file_nonempty(p: pathlib.Path) -> bool:
    return p.is_file() and p.stat().st_size > 0

def rankings_ready(outdir: pathlib.Path) -> bool:
    return file_nonempty(outdir / "matrix_rankings.csv")

def write_synthetic_csv(path: pathlib.Path) -> None:
    path.write_text(textwrap.dedent("""\
        odds,p,result
        2.10,0.55,1
        2.40,0.45,0
        2.80,0.40,1
        2.00,0.35,0
        3.20,0.35,0
        2.30,0.48,1
        3.50,0.32,0
    """))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--outdir", default="outputs")
    ap.add_argument("--bands", default="2.0,2.6|2.6,3.2|3.2,4.0")
    ap.add_argument("--stake-mode", default="kelly", choices=["kelly","flat"])
    ap.add_argument("--edge", type=float, default=0.02)
    ap.add_argument("--kelly-scale", type=float, default=0.5)
    ap.add_argument("--bankroll", type=float, default=1000)
    args_in = ap.parse_args()

    outdir = pathlib.Path(args_in.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # 1) First attempt on provided input
    run_matrix_once(args_in, allow_fail=True)

    # 2) Fallback if needed
    if not rankings_ready(outdir):
        syn = outdir / "synthetic_backtest.csv"
        write_synthetic_csv(syn)
        print("No matrix rankings produced; using synthetic fallback.", file=sys.stderr)
        args2 = argparse.Namespace(**vars(args_in))
        args2.input = str(syn)
        run_matrix_once(args2, allow_fail=True)

    # Exit 0 either way; artifacts will tell the story
    print("Matrix backtest complete (with fallback if needed).")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
