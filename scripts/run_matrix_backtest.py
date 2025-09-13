
#!/usr/bin/env python3
"""
Matrix backtest over odds bands with Kelly/flat staking.

- Input must have canonical columns: oa, ob, pa, pb (the report-normalizer
  already guarantees these exist by duplicating from aliases).
- Optional outcome columns (any one of):
  * result: "A" or "B" (case-insensitive)
  * winner: "A"/"B" or player_a/player_b names
  * result_a/result_b: 1/0 flags
  If no outcome is present, we compute EXPECTED PnL (EV mode) and mark it.

Outputs:
  results/backtests/summary.csv         (aggregated per cfg)
  results/backtests/params_cfg{n}.json  (config used)
  results/backtests/logs/picks_cfg{n}.csv (per-bet log)

CLI example:
  python scripts/run_matrix_backtest.py \
    --input outputs/prob_enriched.csv \
    --bands "1.2,2.0|2.0,2.6|2.6,3.2|3.2,4.0" \
    --staking kelly --kelly-scale 0.5 \
    --min-edge 0.01 --bankroll 1000 --outdir results/backtests
"""

from __future__ import annotations
import argparse, csv, json, math
from pathlib import Path
from typing import Dict, List, Optional, Tuple

def read_rows(path: Path) -> List[Dict[str,str]]:
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_rows(path: Path, rows: List[Dict[str,str]], header: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})

def wjson(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2))

def fnum(x: str) -> float:
    try: return float(x)
    except: return 0.0

def has_outcomes(row: Dict[str,str]) -> Tuple[bool, Optional[int]]:
    """
    Returns (has_result, winner_idx) where winner_idx is 0 for A, 1 for B when resolvable.
    """
    # result as A/B
    r = (row.get("result") or row.get("Result") or "").strip().upper()
    if r in ("A","B"):
        return True, 0 if r=="A" else 1

    # binary flags
    ra = row.get("result_a")
    rb = row.get("result_b")
    if ra is not None and rb is not None:
        try:
            ia = int(float(ra)); ib = int(float(rb))
            if ia==1 and ib==0: return True, 0
            if ia==0 and ib==1: return True, 1
        except: pass

    # winner as name
    w = (row.get("winner") or "").strip()
    if w:
        a = (row.get("player_a") or "").strip()
        b = (row.get("player_b") or "").strip()
        if a and w == a: return True, 0
        if b and w == b: return True, 1

    return False, None

def kelly_fraction(p: float, o: float) -> float:
    # decimal odds o => net odds b = o-1
    b = o - 1.0
    if b <= 0: return 0.0
    # Kelly f* = (p*b - (1-p)) / b  = (p*o - 1)/(o-1)
    return (p*o - 1.0) / b

def parse_bands(s: str) -> List[Tuple[float,float]]:
    bands = []
    for chunk in s.split("|"):
        chunk = chunk.strip()
        if not chunk: continue
        a,b = chunk.split(",")
        bands.append((float(a), float(b)))
    return bands

def run_cfg(cfg_id: int, band: Tuple[float,float], rows: List[Dict[str,str]],
            staking: str, kelly_scale: float, min_edge: float, bankroll0: float,
            outdir: Path) -> Dict[str, float]:
    lo, hi = band
    picks_log: List[Dict[str,str]] = []
    bankroll = bankroll0
    total_staked = 0.0
    pnl = 0.0
    n_bets = 0
    wins = 0
    realized_mode = False

    for r in rows:
        oa = fnum(r.get("oa","0")); ob = fnum(r.get("ob","0"))
        pa = fnum(r.get("pa","0")); pb = fnum(r.get("pb","0"))
        if oa<=1.0 or ob<=1.0 or pa<=0 or pb<=0:  # guardrails
            continue

        # band filter: take the side whose odds in [lo, hi)
        cand = []
        if lo <= oa < hi:
            edge_a = pa*oa - 1.0
            cand.append(("A", 0, oa, pa, edge_a))
        if lo <= ob < hi:
            edge_b = pb*ob - 1.0
            cand.append(("B", 1, ob, pb, edge_b))
        if not cand:
            continue

        # choose side with larger edge
        side, idx, o, p, edge = max(cand, key=lambda x: x[4])

        if edge < min_edge:
            continue

        # stake
        if staking == "kelly":
            frac = max(0.0, min(1.0, kelly_scale * kelly_fraction(p, o)))
            stake = bankroll * frac
        else:
            stake = 1.0

        if stake <= 0:
            continue

        n_bets += 1
        total_staked += stake

        # settle: realized if we have outcomes; else EV
        has_res, winner_idx = has_outcomes(r)
        if has_res and winner_idx is not None:
            realized_mode = True
            won = (winner_idx == idx)
            gain = stake * (o - 1.0) if won else -stake
            if won: wins += 1
            pnl += gain
            bankroll += gain
        else:
            # EV contribution
            ev = stake * (p*(o - 1.0) - (1.0 - p))
            pnl += ev
            bankroll += ev

        picks_log.append({
            "cfg_id": cfg_id,
            "event_date": r.get("event_date",""),
            "player_a": r.get("player_a",""),
            "player_b": r.get("player_b",""),
            "side": side,
            "odds": f"{o:.3f}",
            "p": f"{p:.4f}",
            "edge": f"{edge:.4f}",
            "stake": f"{stake:.2f}",
        })

    # metrics
    roi = (pnl / total_staked) if total_staked > 0 else 0.0
    hitrate = (wins / n_bets) if (n_bets > 0 and realized_mode) else 0.0
    # simple sharpe proxy on EV/realized stream (not tracking variance here)
    sharpe = roi  # placeholder; conservative

    # write artifacts
    outdir.mkdir(parents=True, exist_ok=True)
    wjson(outdir / f"params_cfg{cfg_id}.json", {
        "cfg_id": cfg_id,
        "band": band,
        "staking": staking,
        "kelly_scale": kelly_scale,
        "min_edge": min_edge,
        "bankroll_start": bankroll0,
        "mode": "realized" if realized_mode else "expected",
    })
    log_path = outdir / "logs"
    log_path.mkdir(parents=True, exist_ok=True)
    write_rows(
        log_path / f"picks_cfg{cfg_id}.csv",
        picks_log,
        ["cfg_id","event_date","player_a","player_b","side","odds","p","edge","stake"]
    )

    return {
        "cfg_id": cfg_id,
        "n_bets": n_bets,
        "total_staked": total_staked,
        "pnl": pnl,
        "roi": roi,
        "hitrate": hitrate,
        "sharpe": sharpe,
        "end_bankroll": bankroll,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--bands", default="1.2,2.0|2.0,2.6|2.6,3.2|3.2,4.0")
    ap.add_argument("--staking", choices=["kelly","flat"], default="kelly")
    ap.add_argument("--kelly-scale", type=float, default=0.5)
    ap.add_argument("--min-edge", type=float, default=0.01)   # 1% default so we actually take bets
    ap.add_argument("--bankroll", type=float, default=1000.0)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    rows = read_rows(Path(args.input))
    if not rows:
        raise SystemExit("input has no rows")

    summary_rows: List[Dict[str,str]] = []
    cfg_id = 1
    for band in parse_bands(args.bands):
        m = run_cfg(cfg_id, band, rows, args.staking, args.kelly_scale,
                    args.min_edge, args.bankroll, Path(args.outdir))
        summary_rows.append({k: f"{v:.4f}" if isinstance(v,float) else str(v) for k,v in m.items()})
        cfg_id += 1

    # write summary
    summary_path = Path(args.outdir) / "summary.csv"
    write_rows(summary_path, summary_rows,
               ["cfg_id","n_bets","total_staked","pnl","roi","hitrate","sharpe","end_bankroll"])

if __name__ == "__main__":
    main()
