#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_daily_pipeline.py

ONE BUTTON daily pipeline:
  1) Load raw odds CSV
  2) Build vig-free / stretched probabilities (prob_enriched.csv)
  3) Run value engine (Kelly + TE) with **TE-based filtering**
  4) (Optional) Backtest simple odds bands
  5) Write clean job summaries

Outputs (under ./outputs):
  - prob_enriched.csv
  - picks_final.csv            (copy of root value_picks_pro.csv)
  - value_engine_shortlist.md  (compact table)
  - engine_summary.md
  - matrix_rankings.csv, backtest_metrics.json, results.csv (if bands provided)
  - pipeline_summary.md
"""

from __future__ import annotations
import argparse, csv, os, sys, math, pathlib, subprocess, textwrap, statistics as stats
from typing import List, Dict

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
OUT = ROOT / "outputs"

def ensure_dirs():
    OUT.mkdir(parents=True, exist_ok=True)

def read_csv(path: pathlib.Path) -> List[Dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(path: pathlib.Path, rows: List[Dict]):
    os.makedirs(path.parent, exist_ok=True)
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            pass
        return
    keys = []
    seen = set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); keys.append(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)

def pick_col(header, candidates):
    hset = {c.lower(): c for c in header}
    for c in candidates:
        if c.lower() in hset:
            return hset[c.lower()]
    return None

# ---------- step 1: probability enrichment (vig-free + gamma stretch) ----------
def enrich_probabilities(input_csv: pathlib.Path, gamma: float) -> pathlib.Path:
    rows = read_csv(input_csv)
    if not rows:
        raise SystemExit(f"No rows in {input_csv}")

    hdr = list(rows[0].keys())
    col_price = pick_col(hdr, ["price","odds","decimal_odds"])
    if not col_price:
        raise SystemExit("Input must have 'price' (or odds/decimal_odds) column")
    # Optional existing probs:
    col_prob = pick_col(hdr, ["p_model","p","prob","model_prob","probability"])

    enriched = []
    for r in rows:
        try:
            price = float(r.get(col_price, "nan"))
        except Exception:
            continue
        if not (price and price > 1.0 and math.isfinite(price)):
            continue

        implied = 1.0 / price
        # Start with implied if no model prob; if model exists, start from that
        if col_prob and r.get(col_prob) not in ("", None, "NA"):
            try:
                p0 = float(r[col_prob])
            except Exception:
                p0 = implied
        else:
            p0 = implied

        # Gamma stretch (push fav/longshot a bit away from 0.5):
        # Simple monotonic transform: p' = 0.5 + (p - 0.5) * gamma
        p_model = 0.5 + (p0 - 0.5) * gamma
        p_model = max(0.0, min(1.0, p_model))

        out = dict(r)
        out["price"] = price
        out["p_model"] = p_model
        enriched.append(out)

    path = OUT / "prob_enriched.csv"
    write_csv(path, enriched)
    return path

# ---------- step 2: run engine (with TE-based filtering) ----------
def run_engine(enriched: pathlib.Path, min_edge: float, edge: float,
               kelly_scale: float, kelly_cap: float, bankroll: float,
               max_picks: int) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path]:
    engine_py = SCRIPTS / "tennis_value_engine.py"
    picks_root = ROOT / "value_picks_pro.csv"
    picks_copy = OUT / "picks_final.csv"
    eng_md = OUT / "engine_summary.md"

    cmd = [sys.executable, str(engine_py),
           "--input", str(enriched),
           "--out-picks", str(picks_root),
           "--out-final", str(picks_copy),
           "--summary", str(eng_md),
           "--stake-mode", "kelly",
           "--edge", str(edge),
           "--kelly-scale", str(kelly_scale),
           "--kelly-cap", str(kelly_cap),
           "--bankroll", str(bankroll),
           "--min-edge", str(min_edge),
           "--max-picks", str(max_picks),
           "--filter-on-te"]  # <<< use TE-boosted edge for selection
    print("RUN:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    return picks_root, picks_copy, eng_md

# ---------- step 3: optional matrix backtest ----------
def run_matrix_backtest(input_csv: pathlib.Path, bands: str, edge: float,
                        kelly_scale: float, bankroll: float):
    mb = SCRIPTS / "run_matrix_backtest.py"
    if not bands:
        return
    cmd = [sys.executable, str(mb),
           "--input", str(input_csv),
           "--outdir", str(OUT),
           "--bands", bands,
           "--stake-mode", "kelly",
           "--edge", str(edge),
           "--kelly-scale", str(kelly_scale),
           "--bankroll", str(bankroll)]
    print("RUN:", " ".join(cmd))
    # Don't fail the pipeline if backtest can't find result column, etc.
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print("Backtest step soft-failed:", e)

# ---------- small helpers for pretty shortlist ----------
def build_shortlist_md(picks_csv: pathlib.Path) -> pathlib.Path:
    rows = read_csv(picks_csv)
    lines = ["# Tennis Value Engine (shortlist)",""]
    if not rows:
        lines.append("_No picks._")
    else:
        # Simple markdown table
        cols = ["Tour","Market","Selection","Opponent","Odds","p_model","p_used","EVu","Kelly","Conf","Bet"]
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("|" + "|".join(["---"]*len(cols)) + "|")
        for r in rows:
            tour = r.get("tour","ATP/WTA")
            mkt  = r.get("market","H2H")
            sel  = r.get("player","?")
            opp  = r.get("opponent","?")
            price= float(r.get("price",0))
            pm   = float(r.get("p_model",0))
            pu   = float(r.get("p_used", pm))  # engine writes p_used
            evu  = pu*price - 1.0
            kf   = float(r.get("kelly_f_raw",0))
            conf = r.get("model_conf", r.get("model_confidence","-"))
            bet  = float(r.get("stake_units",0))
            lines.append(f"| {tour} | {mkt} | {sel} | {opp} | {price:.2f} | {pm:.3f} | {pu:.3f} | {evu:.3f} | {kf:.3f} | {conf} | {bet:.2f} |")
    path = OUT / "value_engine_shortlist.md"
    path.write_text("\n".join(lines)+"\n", encoding="utf-8")
    return path

def write_pipeline_summary(engine_md: pathlib.Path, shortlist_md: pathlib.Path,
                           backtest_metrics: pathlib.Path | None):
    lines = ["# Pipeline Summary",""]
    # Inline key engine stats
    if engine_md.is_file():
        lines.append("## Daily Picks")
        lines.append(engine_md.read_text(encoding="utf-8"))
    if shortlist_md.is_file():
        lines.append("\n## Shortlist")
        lines.append(shortlist_md.read_text(encoding="utf-8"))

    # Inline backtest highlight
    if backtest_metrics and backtest_metrics.is_file():
        import json
        j = json.loads(backtest_metrics.read_text(encoding="utf-8"))
        best = (j or {}).get("best_by_roi") or {}
        lines.append("\n## Matrix Backtest — Best by ROI")
        if best:
            lines.append(f"- **Config**: `{best.get('config_id','')}`")
            lines.append(f"- **Band**: {best.get('label','')}")
            lines.append(f"- **Bets**: {best.get('bets','')}"
                         f" | **ROI**: {best.get('roi','')}"
                         f" | **PnL**: {best.get('pnl','')}"
                         f" | **End BR**: {best.get('end_bankroll','')}")
        else:
            lines.append("- No metrics available.")
    (OUT / "pipeline_summary.md").write_text("\n".join(lines)+"\n", encoding="utf-8")

# ---------- CLI ----------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", "-i", default="data/raw/odds/sample_odds.csv",
                    help="Input odds CSV")
    ap.add_argument("--gamma", type=float, default=1.06,
                    help="Fav/longshot stretch (1=no change)")
    ap.add_argument("--min-edge", type=float, default=0.02,
                    help="Edge filter threshold")
    ap.add_argument("--edge", type=float, default=0.08,
                    help="True Edge booster (TE8=0.08)")
    ap.add_argument("--kelly-scale", type=float, default=0.5)
    ap.add_argument("--kelly-cap", type=float, default=0.20)
    ap.add_argument("--bankroll", type=float, default=1000.0)
    ap.add_argument("--max-picks", type=int, default=80)
    ap.add_argument("--bands", default="",
                    help='Matrix bands, e.g. "2.0,2.6|2.6,3.2|3.2,4.0" (optional)')
    return ap.parse_args()

def main():
    args = parse_args()
    ensure_dirs()

    # 1) Enrich
    inp = ROOT / args.input
    enriched = enrich_probabilities(inp, args.gamma)

    # 2) Engine (TE-based filtering)
    picks_root, picks_copy, eng_md = run_engine(
        enriched=enriched,
        min_edge=args.min_edge,
        edge=args.edge,
        kelly_scale=args.kelly_scale,
        kelly_cap=args.kelly_cap,
        bankroll=args.bankroll,
        max_picks=args.max_picks,
    )

    # 3) Backtest (optional; if bands present)
    if args.bands:
        run_matrix_backtest(input_csv=picks_copy, bands=args.bands,
                            edge=args.edge, kelly_scale=args.kelly_scale,
                            bankroll=args.bankroll)

    # 4) Shortlist + summary
    shortlist = build_shortlist_md(picks_root)
    metrics = OUT / "backtest_metrics.json"
    write_pipeline_summary(eng_md, shortlist, metrics if metrics.is_file() else None)

    print("DONE. Artifacts in ./outputs")

if __name__ == "__main__":
    main()
