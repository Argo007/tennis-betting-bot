#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, csv, json, math
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# ---------- Utils

ALIASES = {
    "date": ["date", "event_date", "match_date"],
    "player_a": ["player_a", "a", "home", "team_a", "side_a"],
    "player_b": ["player_b", "b", "away", "team_b", "side_b"],
    "oa": ["oa", "odds_a", "oddsA", "odds_a_close", "odds_a_open"],
    "ob": ["ob", "odds_b", "oddsB", "odds_b_close", "odds_b_open"],
    "pa": ["pa", "prob_a", "probA", "prob_a_vigfree", "implied_prob_a", "p_a"],
    "pb": ["pb", "prob_b", "probB", "prob_b_vigfree", "implied_prob_b", "p_b"],
    "result": ["result", "winner", "outcome", "y", "label"],
}

def pick_col(row: Dict[str,str], keys: List[str]) -> Optional[str]:
    for k in keys:
        if k in row and row[k] not in (None, "", "nan", "NaN"):
            return row[k]
    return None

def to_float(x: Optional[str]) -> Optional[float]:
    if x is None: return None
    try:
        return float(x)
    except:
        return None

def norm_result(x: Optional[str]) -> Optional[str]:
    if x is None: return None
    x = str(x).strip().upper()
    if x in ("A","B"): return x
    if x in ("1","0"): return "A" if x=="1" else "B"
    return None

def kelly_fraction(p: float, o: float) -> float:
    # Decimal odds Kelly: f* = (p*o - 1) / (o - 1)
    denom = (o - 1.0)
    if denom <= 0: return 0.0
    return max(0.0, (p*o - 1.0)/denom)

# ---------- Core Backtest

def parse_args():
    ap = argparse.ArgumentParser(description="Simple tennis backtest (real PnL).")
    ap.add_argument("--dataset", default="outputs/prob_enriched.csv",
                    help="Path to CSV with pa/pb (and ideally oa/ob).")
    ap.add_argument("--staking", choices=["kelly","flat"], default="kelly")
    ap.add_argument("--kelly-scale", type=float, default=0.5,
                    help="Multiply Kelly by this (0.5 = half Kelly).")
    ap.add_argument("--flat-stake", type=float, default=10.0,
                    help="Units per bet when staking=flat.")
    ap.add_argument("--min-edge", type=float, default=0.00,
                    help="Minimum true edge to place a bet (e.g., 0.02 = +2%).")
    ap.add_argument("--bankroll", type=float, default=1000.0)
    ap.add_argument("--max-risk-pct", type=float, default=0.05,
                    help="Cap stake per bet as fraction of bankroll (e.g., 0.05=5%).")
    ap.add_argument("--outdir", default="results/backtests",
                    help="Directory to write results into.")
    return ap.parse_args()

def read_and_normalize(path: Path) -> Tuple[List[Dict], Dict]:
    rows, issues = [], {"total_rows":0, "usable_rows":0, "skipped_missing":0, "notes":[]}
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    with path.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for raw in rdr:
            issues["total_rows"] += 1
            row = {}
            row["date"] = pick_col(raw, ALIASES["date"]) or ""
            row["player_a"] = pick_col(raw, ALIASES["player_a"]) or ""
            row["player_b"] = pick_col(raw, ALIASES["player_b"]) or ""
            row["oa"] = to_float(pick_col(raw, ALIASES["oa"]))
            row["ob"] = to_float(pick_col(raw, ALIASES["ob"]))
            row["pa"] = to_float(pick_col(raw, ALIASES["pa"]))
            row["pb"] = to_float(pick_col(raw, ALIASES["pb"]))
            row["result"] = norm_result(pick_col(raw, ALIASES["result"]))

            # Basic sanity
            if row["pa"] is None or row["pb"] is None:
                issues["skipped_missing"] += 1
                continue
            # If both provided, softly renormalize small drift
            s = row["pa"] + row["pb"]
            if s > 0:
                row["pa"], row["pb"] = row["pa"]/s, row["pb"]/s

            rows.append(row)

    issues["usable_rows"] = len(rows)
    return rows, issues

def decide_bet(pa: float, pb: float, oa: Optional[float], ob: Optional[float], min_edge: float):
    """
    Returns (side, edge, price, p) or (None, 0, None, None) if no bet.
    Edge definition: EV = p*o - 1 ; we take the side with higher EV if >= min_edge.
    """
    ev_a = ev_b = -1.0
    if oa and oa > 1.0: ev_a = pa*oa - 1.0
    if ob and ob > 1.0: ev_b = pb*ob - 1.0

    best = ("A", ev_a, oa, pa) if ev_a >= ev_b else ("B", ev_b, ob, pb)
    side, ev, price, p = best
    if price is None or ev < min_edge:
        return None, 0.0, None, None
    return side, ev, price, p

def stake_amount(staking: str, bankroll: float, p: float, o: float,
                 kelly_scale: float, flat_stake: float, max_risk_pct: float) -> float:
    if staking == "flat":
        stake = flat_stake
    else:
        f = kelly_fraction(p, o) * kelly_scale
        stake = bankroll * max(0.0, min(f, max_risk_pct))
    return max(0.0, min(stake, bankroll))

def run():
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    picks_dir = outdir / "logs"; picks_dir.mkdir(parents=True, exist_ok=True)

    rows, diag = read_and_normalize(Path(args.dataset))

    bankroll = args.bankroll
    pnl = 0.0
    n_bets = 0
    wins = 0

    picks_path = picks_dir / "picks_cfg1.csv"
    with picks_path.open("w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        wr.writerow(["date","player_a","player_b","side","price","p","edge","stake","result","pnl_after"])
        for r in rows:
            side, edge, price, p = decide_bet(r["pa"], r["pb"], r["oa"], r["ob"], args.min_edge)
            if side is None:
                continue

            stake = stake_amount(args.staking, bankroll, p, price,
                                 args.kelly_scale, args.flat_stake, args.max_risk_pct)
            if stake <= 0: 
                continue

            n_bets += 1
            # Settle if we have a result
            this_pnl = 0.0
            if r["result"] is not None:
                won = (r["result"] == side)
                if won:
                    this_pnl = stake * (price - 1.0)
                    wins += 1
                else:
                    this_pnl = -stake
                bankroll += this_pnl
                pnl += this_pnl

            wr.writerow([r["date"], r["player_a"], r["player_b"], side, price, p, edge, stake, r["result"] or "", bankroll])

    roi = (pnl / max(1.0, args.bankroll)) if args.bankroll > 0 else 0.0
    hitrate = (wins / n_bets) if n_bets > 0 else 0.0

    # Write summary + params
    summary_path = outdir / "summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        wr.writerow(["cfg_id","n_bets","total_staked","pnl","roi","hitrate","sharpe","end_bankroll"])
        # we don't keep total_staked/ sharpe in this minimal step; fill 0
        wr.writerow([1, n_bets, 0.0, round(pnl,6), round(roi,6), round(hitrate,6), 0.0, round(bankroll,6)])

    params = {
        "cfg_id": 1,
        "staking": args.staking,
        "kelly_scale": args.kelly_scale,
        "flat_stake": args.flat_stake,
        "min_edge": args.min_edge,
        "bankroll_start": args.bankroll,
        "max_risk_pct": args.max_risk_pct,
        "dataset": args.dataset,
    }
    params_path = outdir / "params_cfg1.json"
    params_path.write_text(json.dumps(params, indent=2))

    diag_path = outdir / "_diagnostics.json"
    diag_path.write_text(json.dumps(diag, indent=2))

    # Minimal HTML so you can open just one file
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Tennis Bot — Backtest Report</title>
<style>
body{{font-family: -apple-system, system-ui, Segoe UI, Roboto, Arial; margin:24px}}
table{{border-collapse:collapse}} td,th{{border:1px solid #ddd;padding:6px 8px}}
code{{background:#f6f8fa;padding:2px 4px;border-radius:3px}}
</style></head><body>
<h1>Tennis Bot — Backtest Report</h1>
<p><em>Generated from <code>{args.dataset}</code></em></p>

<h3>Recommended Config (cfg 1)</h3>
<pre>{json.dumps({
    "cfg_id":1,"n_bets":n_bets,"pnl":round(pnl,6),"roi":round(roi,6),
    "hitrate":round(hitrate,6),"end_bankroll":round(bankroll,6)
}, indent=2)}</pre>

<p>Params: <code>{params_path.as_posix()}</code><br/>
Picks: <code>{picks_path.as_posix()}</code></p>

<h3>Top Backtest Results</h3>
<table>
<tr><th>cfg_id</th><th>n_bets</th><th>pnl</th><th>roi</th><th>hitrate</th><th>end_bankroll</th></tr>
<tr><td>1</td><td>{n_bets}</td><td>{round(pnl,6)}</td><td>{round(roi,6)}</td>
<td>{round(hitrate,6)}</td><td>{round(bankroll,6)}</td></tr>
</table>

<h3>Diagnostics</h3>
<pre>{json.dumps(diag, indent=2)}</pre>

</body></html>"""
    (outdir / "index.html").write_text(html, encoding="utf-8")

    print(f"[done] picks -> {picks_path}")
    print(f"[done] summary -> {summary_path}")
    print(f"[done] html -> {(outdir/'index.html')}")
    return 0

if __name__ == "__main__":
    raise SystemExit(run())
