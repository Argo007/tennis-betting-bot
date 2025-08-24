#!/usr/bin/env python3
"""
End-to-end DAILY pipeline:
  1) compute_prob_vigfree.py -> outputs/prob_enriched.csv
  2) tennis_value_engine.py   -> value_picks_pro.csv + outputs/picks_final.csv + outputs/engine_summary.md
  3) run_matrix_with_fallback.py -> outputs/{matrix_rankings.csv,backtest_metrics.json,results.csv}
  4) Write outputs/pipeline_summary.md (concise, human-readable)

Run example:
  python scripts/run_daily_pipeline.py \
    --input data/raw/odds/sample_odds.csv \
    --gamma 1.06 \
    --min-edge 0.00 \
    --min-edge-te 0.02 \
    --stake-mode kelly \
    --kelly-scale 0.5 \
    --kelly-cap 0.20 \
    --flat-stake 1.0 \
    --bankroll 1000 \
    --bands "2.0,2.6|2.6,3.2|3.2,4.0"
"""
from __future__ import annotations
import argparse, pathlib, subprocess, sys, json, re

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
OUTDIR = ROOT / "outputs"

def run(cmd: list[str]) -> None:
    rc = subprocess.run(cmd, check=False)
    if rc.returncode != 0:
        print(f"[warn] Command failed (ignored): {' '.join(cmd)}", file=sys.stderr)

def compute_probs(inp: str, gamma: float) -> pathlib.Path:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    out = OUTDIR / "prob_enriched.csv"
    cmd = [sys.executable, str(SCRIPTS / "compute_prob_vigfree.py"),
           "--input", inp, "--output", str(out), "--gamma", str(gamma)]
    run(cmd)
    return out

def run_engine(prob_csv: pathlib.Path, args) -> None:
    py = SCRIPTS / "tennis_value_engine.py"
    if not py.exists():
        py = ROOT / "tennis_value_engine.py"  # fallback if at repo root
    cmd = [
        sys.executable, str(py),
        "--input", str(prob_csv),
        "--out-picks", "value_picks_pro.csv",
        "--out-final", str(OUTDIR / "picks_final.csv"),
        "--summary",   str(OUTDIR / "engine_summary.md"),
        "--min-edge",  str(args.min_edge),
        "--stake-mode", args.stake_mode,
        "--kelly-scale", str(args.kelly_scale),
        "--kelly-cap",   str(args.kelly_cap),
        "--flat-stake",  str(args.flat_stake),
        "--bankroll",    str(args.bankroll),
    ]
    run(cmd)

def run_matrix(args) -> None:
    cmd = [
        sys.executable, str(SCRIPTS / "run_matrix_with_fallback.py"),
        "--input", str(OUTDIR / "prob_enriched.csv"),
        "--outdir", str(OUTDIR),
        "--bands", args.bands,
        "--stake-mode", "kelly" if args.stake_mode == "kelly" else "flat",
        "--edge", str(args.min_edge_te),
        "--kelly-scale", str(args.kelly_scale),
        "--bankroll", str(args.bankroll),
    ]
    run(cmd)

def _grep(pattern: str, text: str, default: str = "-") -> str:
    m = re.search(pattern, text)
    return m.group(1) if m else default

def write_pipeline_summary() -> pathlib.Path:
    eng = OUTDIR / "engine_summary.md"
    best_json = OUTDIR / "backtest_metrics.json"
    pipeline = OUTDIR / "pipeline_summary.md"

    picks = total_stake = avg_odds = avg_edge_raw = avg_edge_te = "-"
    if eng.exists():
        t = eng.read_text()
        picks        = _grep(r"Picks:\s*(\d+)", t)
        total_stake  = _grep(r"Total stake:\s*([\d.]+)", t)
        avg_odds     = _grep(r"Avg odds:\s*([\d.]+)", t)
        avg_edge_raw = _grep(r"Avg edge \(raw\):\s*([\d.\-]+)", t)
        avg_edge_te  = _grep(r"Avg edge \(TE\):\s*([\d.\-]+)", t)

    best = {}
    if best_json.exists() and best_json.stat().st_size > 0:
        try:
            j = json.loads(best_json.read_text())
            best = (j or {}).get("best_by_roi") or {}
        except Exception:
            best = {}

    def fnum(x):
        return f"{x:.4f}" if isinstance(x, (int, float)) else (x or "-")

    lines = []
    lines.append("# Pipeline Summary\n")
    lines.append("Daily Picks\n")
    lines.append(f"* Picks: {picks}")
    lines.append(f"* Total stake: {total_stake}")
    lines.append(f"* Avg odds: {avg_odds} | Avg edge (raw): {avg_edge_raw} | Avg edge (TE): {avg_edge_te}\n")
    lines.append("Matrix Backtest — Best by ROI")
    if best:
        lines.append(f"* Config: {best.get('config_id','-')}")
        lines.append(f"* Band: {best.get('label','-')}")
        lines.append(f"* Bets: {best.get('bets','-')} | ROI: {fnum(best.get('roi'))} | "
                     f"PnL: {fnum(best.get('pnl'))} | End BR: {fnum(best.get('end_bankroll'))}")
    else:
        lines.append("* No metrics available — no bets met the criteria or outputs are empty.")
    pipeline.write_text("\n".join(lines) + "\n")
    print(pipeline.read_text())
    return pipeline

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/raw/odds/sample_odds.csv")
    ap.add_argument("--gamma", type=float, default=1.06)
    ap.add_argument("--min-edge", type=float, default=0.00, dest="min_edge")
    ap.add_argument("--min-edge-te", type=float, default=0.02, dest="min_edge_te")
    ap.add_argument("--stake-mode", choices=["kelly", "flat"], default="kelly")
    ap.add_argument("--kelly-scale", type=float, default=0.5)
    ap.add_argument("--kelly-cap", type=float, default=0.20)
    ap.add_argument("--flat-stake", type=float, default=1.0)
    ap.add_argument("--bankroll", type=float, default=1000)
    ap.add_argument("--bands", default="2.0,2.6|2.6,3.2|3.2,4.0")
    return ap.parse_args()

def main():
    args = parse_args()
    prob_csv = compute_probs(args.input, args.gamma)
    run_engine(prob_csv, args)
    run_matrix(args)
    write_pipeline_summary()
    print("\n[OK] Daily pipeline finished.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
