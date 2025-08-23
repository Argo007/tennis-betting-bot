#!/usr/bin/env python3
"""
quick_summary.py

Robust summary for the matrix backtest job:
- Tries outputs/backtest_metrics.json first
- Falls back to outputs/matrix_rankings.csv if JSON is empty/missing
- Prints a compact markdown block safe for $GITHUB_STEP_SUMMARY
"""

from __future__ import annotations
import json, csv, pathlib, sys
from typing import Dict, Optional

OUTDIR = pathlib.Path("outputs")
METRICS = OUTDIR / "backtest_metrics.json"
RANKINGS = OUTDIR / "matrix_rankings.csv"

def fmt(x):
    if x is None:
        return "-"
    if isinstance(x, (int, float)):
        try:
            return f"{float(x):.4f}"
        except Exception:
            return str(x)
    return str(x)

def read_best_from_json() -> Optional[Dict]:
    if not METRICS.exists() or METRICS.stat().st_size == 0:
        return None
    try:
        j = json.loads(METRICS.read_text())
    except Exception:
        return None
    best = (j or {}).get("best_by_roi") or None
    return best

def read_top_from_rankings() -> Optional[Dict]:
    if not RANKINGS.exists() or RANKINGS.stat().st_size == 0:
        return None
    try:
        with RANKINGS.open(newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            rows = list(rdr)
            if not rows:
                return None
            # assume already sorted by ROI desc, otherwise sort here:
            # rows.sort(key=lambda r: float(r.get("roi", 0) or 0), reverse=True)
            return rows[0]
    except Exception:
        return None

def main():
    print("## Matrix Backtest — Best by ROI")

    best = read_best_from_json()
    from_csv = False

    if best is None:
        # graceful fallback to rankings.csv
        r = read_top_from_rankings()
        if r is None:
            print("No metrics available — no bets met the criteria or outputs are empty.")
            sys.exit(0)
        from_csv = True
        # normalize keys to match json shape
        best = {
            "config_id": r.get("config_id"),
            "label": r.get("label"),
            "bets": r.get("bets"),
            "wins": r.get("wins"),
            "hit_rate": r.get("hit_rate"),
            "avg_odds": r.get("avg_odds"),
            "turnover": r.get("turnover"),
            "pnl": r.get("pnl"),
            "roi": r.get("roi"),
            "end_bankroll": r.get("end_bankroll"),
            "max_drawdown": r.get("max_drawdown"),
        }

    # headline
    print(f"- **Config**: `{best.get('config_id', '-')}`")
    print(f"- **Band**: {best.get('label', '-')}")

    # metrics block (only print if present)
    bets = best.get("bets")
    roi = best.get("roi")
    pnl = best.get("pnl")
    end_br = best.get("end_bankroll")
    extra = []

    if bets is not None:
        extra.append(f"**Bets**: {fmt(bets)}")
    if roi is not None:
        extra.append(f"**ROI**: {fmt(roi)}")
    if pnl is not None:
        extra.append(f"**PnL**: {fmt(pnl)}")
    if end_br is not None:
        extra.append(f"**End BR**: {fmt(end_br)}")

    if extra:
        print("- " + " | ".join(extra))

    if from_csv:
        print("_Summary derived from matrix_rankings.csv (JSON was empty)._")

if __name__ == "__main__":
    main()
