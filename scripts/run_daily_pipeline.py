#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_daily_pipeline.py

End-to-end driver:
  1) compute_prob_vigfree.py  → outputs/prob_enriched.csv (adds p_model)
  2) tennis_value_engine.py   → picks & engine_summary.md
  3) run_matrix_backtest.py   → rankings/metrics (synth fallback if needed)
  4) pipeline_summary.md      → job-friendly summary

Run:
  python scripts/run_daily_pipeline.py \
    --input data/raw/odds/sample_odds.csv \
    --gamma 1.08 --min-edge 0.00 --edge 0.08 --kelly-scale 0.5 \
    --bankroll 1000 --max-picks 80 --bands "2.0,2.6|2.6,3.2|3.2,4.0"
"""

from __future__ import annotations
import argparse, csv, json, os, pathlib, subprocess, sys
from typing import Optional

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent

def sh(args: list[str], ok: bool = False) -> int:
    print(f"[sh] {' '.join(args)}")
    rc = subprocess.run(args, cwd=str(REPO)).returncode
    if rc != 0 and not ok:
        raise SystemExit(rc)
    return rc

def ensure_dir(p: pathlib.Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def file_lines(p: pathlib.Path) -> int:
    try:
        with open(p, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0

def write_synth_backtest(path: pathlib.Path) -> None:
    ensure_dir(path.parent)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["odds","p","result"])
        w.writerow([2.10,0.55,1])
        w.writerow([2.40,0.45,0])
        w.writerow([2.80,0.40,1])
        w.writerow([3.20,0.35,0])
        w.writerow([2.30,0.48,1])
        w.writerow([3.50,0.32,0])

def load_json(p: pathlib.Path) -> dict:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def main():
    ap = argparse.ArgumentParser(description="Run daily tennis pipeline")
    ap.add_argument("--input", "-i", default="data/raw/odds/sample_odds.csv")
    ap.add_argument("--outdir", default="outputs")
    ap.add_argument("--gamma", type=float, default=1.05)
    ap.add_argument("--min-edge", type=float, default=0.02)
    ap.add_argument("--edge", type=float, default=0.08)
    ap.add_argument("--kelly-scale", type=float, default=0.5)
    ap.add_argument("--kelly-cap", type=float, default=0.20)
    ap.add_argument("--bankroll", type=float, default=1000.0)
    ap.add_argument("--max-picks", type=int, default=80)
    ap.add_argument("--elo-atp", default="")
    ap.add_argument("--elo-wta", default="")
    ap.add_argument("--bands", default="2.0,2.6|2.6,3.2|3.2,4.0")
    args = ap.parse_args()

    outdir = REPO / args.outdir
    ensure_dir(outdir)

    compute_prob = REPO / "scripts" / "compute_prob_vigfree.py"
    engine_py   = REPO / "scripts" / "tennis_value_engine.py"
    matrix_py   = REPO / "scripts" / "run_matrix_backtest.py"

    enriched = outdir / "prob_enriched.csv"

    # 1) Build vig-free probabilities
    sh([sys.executable, str(compute_prob),
        "--input", args.input,
        "--out", str(enriched),
        "--gamma", str(args.gamma)])

    # 2) Run engine
    picks_root = REPO / "value_picks_pro.csv"
    picks_copy = outdir / "picks_final.csv"
    eng_md     = outdir / "engine_summary.md"

    cmd = [sys.executable, str(engine_py),
           "--input", str(enriched),
           "--out-picks", str(picks_root),
           "--out-final", str(picks_copy),
           "--summary", str(eng_md),
           "--stake-mode", "kelly",
           "--edge", str(args.edge),
           "--kelly-scale", str(args.kelly_scale),
           "--kelly-cap", str(args.kelly_cap),
           "--bankroll", str(args.bankroll),
           "--min-edge", str(args.min_edge),
           "--max-picks", str(args.max_picks)]
    if args.elo_atp: cmd += ["--elo-atp", args.elo_atp]
    if args.elo_wta: cmd += ["--elo-wta", args.elo_wta]
    sh(cmd)

    # 3) Backtest (real if possible, otherwise synth)
    sh([sys.executable, str(matrix_py),
        "--input", str(enriched),
        "--outdir", str(outdir),
        "--bands", args.bands,
        "--stake-mode", "kelly",
        "--edge", "0.08",
        "--kelly-scale", "0.5",
        "--bankroll", "100"], ok=True)

    rankings = outdir / "matrix_rankings.csv"
    if file_lines(rankings) <= 1:
        synth = outdir / "synthetic_backtest.csv"
        write_synth_backtest(synth)
        sh([sys.executable, str(matrix_py),
            "--input", str(synth),
            "--outdir", str(outdir),
            "--bands", args.bands,
            "--stake-mode", "kelly",
            "--edge", "0.08",
            "--kelly-scale", "0.5",
            "--bankroll", "100"], ok=True)

    # 4) Summary for job page
    metrics = outdir / "backtest_metrics.json"
    j = load_json(metrics)
    best = (j or {}).get("best_by_roi") or {}
    def fmt(x):
        return "-" if x is None else (f"{x:.4f}" if isinstance(x, float) else str(x))

    lines = []
    lines.append("## Matrix Backtest — Best by ROI")
    if best:
        lines.append(f"- **Config:** {fmt(best.get('config_id'))}")
        lines.append(f"- **Band:** {fmt(best.get('label'))}")
        lines.append(f"- **Bets:** {fmt(best.get('bets'))} | **ROI:** {fmt(best.get('roi'))} | **PnL:** {fmt(best.get('pnl'))} | **End BR:** {fmt(best.get('end_bankroll'))}")
    else:
        lines.append("- No metrics available — no bets met the criteria or outputs are empty.")

    (outdir / "pipeline_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))

    print("\n[done] artifacts written to:", outdir)

if __name__ == "__main__":
    main()
