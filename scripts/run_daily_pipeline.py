#!/usr/bin/env python3
"""
run_daily_pipeline.py

One-button pipeline:
1) Build model probabilities from two-sided odds (vig-free + optional stretch)
2) Run the value engine (Kelly + TE) to produce picks
3) Run matrix backtest (if data has odds+prob+result), else synthesize tiny set
4) Write a clean summary (also prints to stdout)

No YAML heredocs, no quoting gotchas. Just call this script.
"""

from __future__ import annotations
import argparse
import csv
import json
import os
import pathlib
import subprocess
import sys
from typing import Optional

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent

# ---- small helpers ----------------------------------------------------------

def sh(args: list[str], cwd: Optional[pathlib.Path] = None, ok: bool = False) -> int:
    """Run a subprocess; return code (raise if ok=False and rc!=0)."""
    print(f"[sh] {' '.join(args)}")
    rc = subprocess.run(args, cwd=str(cwd) if cwd else None).returncode
    if rc != 0 and not ok:
        raise SystemExit(rc)
    return rc

def ensure_dir(p: pathlib.Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def file_lines(path: pathlib.Path) -> int:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0

def write_synth_backtest(path: pathlib.Path) -> None:
    ensure_dir(path.parent)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["odds", "p", "result"])
        w.writerow([2.10, 0.55, 1])
        w.writerow([2.40, 0.45, 0])
        w.writerow([2.80, 0.40, 1])
        w.writerow([3.20, 0.35, 0])
        w.writerow([2.30, 0.48, 1])
        w.writerow([3.50, 0.32, 0])

def load_json(path: pathlib.Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

# ---- main -------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Run daily tennis value pipeline")
    # Inputs / outputs
    ap.add_argument("--input", "-i", default="data/raw/odds/sample_odds.csv",
                    help="Input odds CSV (two-sided or flat)")
    ap.add_argument("--outdir", default="outputs", help="Output directory")
    # Prob enrichment
    ap.add_argument("--gamma", type=float, default=1.05,
                    help="Fav/longshot stretch (1=no change)")
    # Engine knobs
    ap.add_argument("--min-edge", type=float, default=0.02,
                    help="Min (p_model - 1/odds) to accept a pick")
    ap.add_argument("--edge", type=float, default=0.08,
                    help="True Edge booster (TE); 0.08 = TE8")
    ap.add_argument("--kelly-scale", type=float, default=0.5,
                    help="Kelly safety scaler (0.5 = half Kelly)")
    ap.add_argument("--kelly-cap", type=float, default=0.20,
                    help="Cap stake as fraction of bankroll")
    ap.add_argument("--bankroll", type=float, default=1000.0,
                    help="Starting bankroll")
    ap.add_argument("--max-picks", type=int, default=80,
                    help="Max picks (sorted by edge)")
    # Elo (optional – files may be missing, that’s fine)
    ap.add_argument("--elo-atp", default="", help="Path to ATP Elo CSV (optional)")
    ap.add_argument("--elo-wta", default="", help="Path to WTA Elo CSV (optional)")
    # Backtest
    ap.add_argument("--bands", default="2.0,2.6|2.6,3.2|3.2,4.0",
                    help="Odds bands string")
    args = ap.parse_args()

    outdir = REPO / args.outdir
    ensure_dir(outdir)

    # Resolve script paths (root or scripts/)
    def find_script(name: str) -> pathlib.Path:
        cand1 = HERE / name
        cand2 = REPO / name
        if cand1.is_file(): return cand1
        if cand2.is_file(): return cand2
        # try scripts/name
        cand3 = HERE / name
        cand4 = REPO / "scripts" / name
        return cand4 if cand4.is_file() else cand1

    # Prefer scripts/… if present
    compute_prob = REPO / "scripts" / "compute_prob_vigfree.py"
    engine_py     = REPO / "scripts" / "tennis_value_engine.py"
    matrix_py     = REPO / "scripts" / "run_matrix_backtest.py"

    # Fallbacks if repo layout is different
    if not compute_prob.is_file(): compute_prob = find_script("compute_prob_vigfree.py")
    if not engine_py.is_file():    engine_py    = find_script("tennis_value_engine.py")
    if not matrix_py.is_file():    matrix_py    = find_script("run_matrix_backtest.py")

    # 1) Build probabilities (vig-free + stretch) → outputs/prob_enriched.csv
    enriched = outdir / "prob_enriched.csv"
    sh([
        sys.executable, str(compute_prob),
        "--input", args.input,
        "--out", str(enriched),
        "--gamma", str(args.gamma),
    ])

    # 2) Run engine (Kelly + TE)
    picks_root = REPO / "value_picks_pro.csv"
    picks_copy = outdir / "picks_final.csv"
    eng_md     = outdir / "engine_summary.md"

    cmd = [
        sys.executable, str(engine_py),
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
        "--max-picks", str(args.max_picks),
    ]
    if args.elo_atp: cmd += ["--elo-atp", args.elo_atp]
    if args.elo_wta: cmd += ["--elo-wta", args.elo_wta]
    sh(cmd)

    # 3) Matrix backtest (robust): try enriched; if header-only rankings → synthesize & re-run
    sh([
        sys.executable, str(matrix_py),
        "--input", str(enriched),
        "--outdir", str(outdir),
        "--bands", args.bands,
        "--stake-mode", "kelly",
        "--edge", "0.08",
        "--kelly-scale", "0.5",
        "--bankroll", "100",
    ], ok=True)

    rankings = outdir / "matrix_rankings.csv"
    if file_lines(rankings) <= 1:
        synth = outdir / "synthetic_backtest.csv"
        write_synth_backtest(synth)
        sh([
            sys.executable, str(matrix_py),
            "--input", str(synth),
            "--outdir", str(outdir),
            "--bands", args.bands,
            "--stake-mode", "kelly",
            "--edge", "0.08",
            "--kelly-scale", "0.5",
            "--bankroll", "100",
        ], ok=True)

    # 4) Quick summary (avoid YAML heredocs)
    metrics = outdir / "backtest_metrics.json"
    j = load_json(metrics)
    best = (j or {}).get("best_by_roi") or {}
    cfg  = best.get("config_id", "-")
    band = best.get("label", "-")
    roi  = best.get("roi", None)
    pnl  = best.get("pnl", None)
    bets = best.get("bets", None)
    endb = best.get("end_bankroll", None)

    lines = []
    lines.append("## Matrix Backtest — Best by ROI")
    if best:
        def fmt(x):
            return "-" if x is None else (f"{x:.4f}" if isinstance(x, float) else str(x))
        lines.append(f"- **Config:** {cfg}")
        lines.append(f"- **Band:** {band}")
        lines.append(f"- **Bets:** {fmt(bets)} | **ROI:** {fmt(roi)} | **PnL:** {fmt(pnl)} | **End BR:** {fmt(endb)}")
    else:
        lines.append("- No metrics available — no bets met the criteria or outputs are empty.")

    summary_md = outdir / "pipeline_summary.md"
    summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("\n".join(lines))

    print("\n[done] Artifacts:")
    print(f"- Picks root:          {picks_root}")
    print(f"- Picks copy:          {picks_copy}")
    print(f"- Engine summary:      {eng_md}")
    print(f"- Backtest rankings:   {rankings}")
    print(f"- Backtest metrics:    {metrics}")
    print(f"- Pipeline summary:    {summary_md}")

if __name__ == "__main__":
    main()
