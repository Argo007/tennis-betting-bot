#!/usr/bin/env python3
"""
Select value picks using EV + probability thresholds and export picks.

Default:
  IN      = outputs/edge_enriched.csv
  OUTDIR  = repo root (picks_live.csv at top-level + results/picks_YYYYMMDD.csv)

Env knobs (printed by pipeline and inherited here):
  MIN_EDGE_EV, MIN_PROBABILITY, KELLY_FRACTION, KELLY_SCALE, STAKE_CAP_PCT, DAILY_RISK_BUDGET_PCT
"""

import csv, os, argparse
from pathlib import Path
from datetime import date

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTS_DIR  = REPO_ROOT / "outputs"
RES_DIR   = REPO_ROOT / "results"
DEFAULT_IN = OUTS_DIR / "edge_enriched.csv"

MIN_EDGE_EV      = float(os.getenv("MIN_EDGE_EV", "0.02"))
MIN_PROBABILITY  = float(os.getenv("MIN_PROBABILITY", "0.05"))
KELLY_FRACTION   = float(os.getenv("KELLY_FRACTION", "0.5"))
KELLY_SCALE      = float(os.getenv("KELLY_SCALE", "1.0"))
STAKE_CAP_PCT    = float(os.getenv("STAKE_CAP_PCT", "0.04"))
DAILY_RISK_BUDGET_PCT = float(os.getenv("DAILY_RISK_BUDGET_PCT", "0.12"))
BANKROLL_FILE    = Path(os.getenv("BANKROLL_FILE", REPO_ROOT / "state" / "bankroll.json"))

def log(m): print(f"[picks_pro] {m}", flush=True)

def kelly(prob, odds):
    b = odds - 1.0
    edge = b*prob - (1-prob)
    denom = b
    if denom <= 0: return 0.0
    f = edge/denom
    return max(0.0, f)

def bankroll():
    try:
        import json
        if BANKROLL_FILE.exists():
            return float(json.loads(BANKROLL_FILE.read_text()).get("bankroll", 1000.0))
    except Exception:
        pass
    return 1000.0

def main():
    ap = argparse.ArgumentParser(description="Generate value picks (pro)")
    ap.add_argument("--input", default=str(DEFAULT_IN))
    ap.add_argument("--outdir", default=str(REPO_ROOT))
    args = ap.parse_args()

    inp = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    RES_DIR.mkdir(parents=True, exist_ok=True)

    picks_live = outdir / "picks_live.csv"
    dated = RES_DIR / f"picks_{date.today().isoformat()}.csv"

    if not inp.exists():
        # still write headers so pipeline moves on
        for p in [picks_live, dated]:
            p.write_text("event_date,tournament,player,side,odds,prob,edge,stake\n")
        log("no input; wrote header-only picks")
        return

    rows = list(csv.DictReader(inp.open("r", encoding="utf-8")))
    bkr = bankroll()
    daily_budget = bkr * DAILY_RISK_BUDGET_PCT

    picks = []
    budget_used = 0.0
    for r in rows:
        # candidate A
        pa = float(r.get("prob_a_vigfree") or 0)
        oa = float(r.get("odds_a") or 0)
        eva = float(r.get("edge_a") or 0)
        if pa >= MIN_PROBABILITY and eva >= MIN_EDGE_EV and oa > 1.0:
            f = kelly(pa, oa) * KELLY_FRACTION * KELLY_SCALE
            stake = min(bkr * f, bkr * STAKE_CAP_PCT, daily_budget - budget_used)
            if stake > 0:
                picks.append({
                    "event_date": r.get("event_date"),
                    "tournament": r.get("tournament"),
                    "player": r.get("player_a"),
                    "side": "A",
                    "odds": round(oa, 3),
                    "prob": round(pa, 6),
                    "edge": round(eva, 6),
                    "stake": round(stake, 2),
                })
                budget_used += stake

        # candidate B
        pb = float(r.get("prob_b_vigfree") or 0)
        ob = float(r.get("odds_b") or 0)
        evb = float(r.get("edge_b") or 0)
        if pb >= MIN_PROBABILITY and evb >= MIN_EDGE_EV and ob > 1.0 and budget_used < daily_budget:
            f = kelly(pb, ob) * KELLY_FRACTION * KELLY_SCALE
            stake = min(bkr * f, bkr * STAKE_CAP_PCT, daily_budget - budget_used)
            if stake > 0:
                picks.append({
                    "event_date": r.get("event_date"),
                    "tournament": r.get("tournament"),
                    "player": r.get("player_b"),
                    "side": "B",
                    "odds": round(ob, 3),
                    "prob": round(pb, 6),
                    "edge": round(evb, 6),
                    "stake": round(stake, 2),
                })
                budget_used += stake

    # Write outputs (even if empty)
    header = ["event_date","tournament","player","side","odds","prob","edge","stake"]
    for p in [picks_live, dated]:
        with p.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            for row in picks:
                w.writerow(row)

    log(f"picks={len(picks)}, budget_used={round(budget_used,2)} / {round(daily_budget,2)} → {picks_live} & {dated}")

if __name__ == "__main__":
    main()
