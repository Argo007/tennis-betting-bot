#!/usr/bin/env python3
# scripts/matrix_backtest.py
import argparse, csv, json, os
from dataclasses import dataclass
from typing import List, Tuple

@dataclass
class Config:
    cfg_id: int
    bands: List[Tuple[float,float]]
    staking: str           # "kelly" | "flat"
    kelly_scale: float
    bankroll_start: float
    flat_units: float

def parse_bands(bands_str: str) -> List[Tuple[float,float]]:
    # "2.0,2.6|2.6,3.2|3.2,4.0"
    out = []
    for grp in bands_str.split("|"):
        lo, hi = grp.split(",")
        out.append((float(lo), float(hi)))
    return out

def kelly_fraction(p: float, odds: float) -> float:
    # fraction of bankroll to bet; clamp to [0,1]
    b = odds - 1.0
    f = (p*b - (1.0 - p)) / b if b > 0 else 0.0
    return max(0.0, min(1.0, f))

def read_rows(path: str) -> List[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return [dict(x) for x in r]

def to_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def derive_candidates(r: dict) -> List[dict]:
    """
    Return candidate bets for a row.
    Prefer explicit columns: selection/odds/p/edge.
    Fallback: compute from oa,ob and pa,pb.
    """
    cands = []
    # explicit single-selection schema
    if "selection" in r and "odds" in r and ("p" in r or "prob" in r or "pa" in r or "pb" in r):
        p = to_float(r.get("p") or r.get("prob") or r.get("pa"))
        if p is None and r.get("selection") and r["selection"].lower() in ("b","player_b","away"):
            p = to_float(r.get("pb"))
        odds = to_float(r.get("odds"))
        edge = to_float(r.get("edge"))
        if p is not None and odds is not None:
            if edge is None:
                edge = p*odds - 1.0
            cands.append({"sel": r.get("selection","A"), "odds": odds, "p": p, "edge": edge})
        return cands

    # fallback two-sided schema
    oa, ob = to_float(r.get("oa")), to_float(r.get("ob"))
    pa, pb = to_float(r.get("pa")), to_float(r.get("pb"))
    if oa and pa:
        cands.append({"sel": "A", "odds": oa, "p": pa, "edge": pa*oa - 1.0})
    if ob and pb:
        cands.append({"sel": "B", "odds": ob, "p": pb, "edge": pb*ob - 1.0})
    return cands

def in_any_band(odds: float, bands: List[Tuple[float,float]]) -> bool:
    for lo, hi in bands:
        if lo <= odds <= hi:
            return True
    return False

def ensure_dirs(paths: List[str]):
    for p in paths:
        os.makedirs(p, exist_ok=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="CSV with either selection/odds/p[/edge] or oa,ob,pa,pb")
    ap.add_argument("--bands", required=True, help="e.g. '2.0,2.6|2.6,3.2|3.2,4.0'")
    ap.add_argument("--staking", default="kelly", choices=["kelly","flat"])
    ap.add_argument("--kelly-scale", type=float, default=0.5)
    ap.add_argument("--bankroll", type=float, default=1000.0)
    ap.add_argument("--flat-units", type=float, default=1.0)
    ap.add_argument("--logs-dir", required=True)
    ap.add_argument("--summary-csv", required=True)
    args = ap.parse_args()

    cfg = Config(
        cfg_id=1,
        bands=parse_bands(args.bands),
        staking=args.staking,
        kelly_scale=args.kelly_scale,
        bankroll_start=args.bankroll,
        flat_units=args.flat_units,
    )

    ensure_dirs([os.path.dirname(args.summary_csv), args.logs_dir])

    rows = read_rows(args.input)
    # build pick list
    picks = []
    for r in rows:
        cands = derive_candidates(r)
        if not cands:
            continue
        # choose best by edge
        c = max(cands, key=lambda x: x["edge"])
        if not in_any_band(c["odds"], cfg.bands):
            continue
        stake = 0.0
        if cfg.staking == "kelly":
            f = kelly_fraction(c["p"], c["odds"]) * cfg.kelly_scale
            stake = max(0.0, f * cfg.bankroll_start)  # backtest treats all at t0 bankroll
        else:
            stake = cfg.flat_units
        picks.append({
            "selection": c["sel"],
            "odds": f"{c['odds']:.3f}",
            "p": f"{c['p']:.3f}",
            "edge": f"{c['edge']:.4f}",
            "stake": f"{stake:.2f}",
        })

    # outcome simulation only if result column exists
    bankroll = cfg.bankroll_start
    total_staked = 0.0
    pnl = 0.0
    n_bets = 0
    # Try to pair picks back to source rows to read result columns
    for i, r in enumerate(rows):
        if i >= len(picks):
            break
        pick = picks[i]
        stake = to_float(pick["stake"], 0.0)
        total_staked += stake
        n_bets += 1
        # detect a simple result field: result or win_a/win_b or winner ('A'/'B')
        win = None
        if "result" in r:
            win = str(r["result"]).strip().lower() in ("1","true","win","won","a","home")
        elif "winner" in r:
            w = str(r["winner"]).strip().upper()
            win = (w == "A" and pick["selection"]=="A") or (w == "B" and pick["selection"]=="B")
        elif "win_a" in r or "win_b" in r:
            wa = str(r.get("win_a","")).strip().lower() in ("1","true")
            wb = str(r.get("win_b","")).strip().lower() in ("1","true")
            win = (wa and pick["selection"]=="A") or (wb and pick["selection"]=="B")

        if win is None:
            # no result data; skip PnL change (counts as no-play but keeps summary valid)
            n_bets -= 1
            total_staked -= stake
            continue

        odds = float(pick["odds"])
        if win:
            bankroll += stake * (odds - 1.0)
            pnl += stake * (odds - 1.0)
        else:
            bankroll -= stake
            pnl -= stake

    roi = (pnl / total_staked) if total_staked > 0 else 0.0
    sharpe = 0.0  # keep simple for now; can be expanded later
    hitrate = 0.0 # needs actual results to compute properly

    # write summary
    with open(args.summary_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cfg_id","n_bets","total_staked","pnl","roi","hitrate","sharpe","end_bankroll"])
        w.writerow([cfg.cfg_id, n_bets, f"{total_staked:.4f}", f"{pnl:.4f}", f"{roi:.4f}", f"{hitrate:.4f}", f"{sharpe:.4f}", f"{bankroll:.4f}"])

    # write picks log
    picks_path = os.path.join(args.logs_dir, f"picks_cfg{cfg.cfg_id}.csv")
    with open(picks_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["selection","odds","p","edge","stake"])
        for p in picks:
            w.writerow([p["selection"], p["odds"], p["p"], p["edge"], p["stake"]])

    # write params JSON (used by your HTML)
    params_path = os.path.join(os.path.dirname(args.summary_csv), f"params_cfg{cfg.cfg_id}.json")
    with open(params_path, "w", encoding="utf-8") as f:
        json.dump({
            "cfg_id": cfg.cfg_id,
            "bands": cfg.bands,
            "staking": cfg.staking,
            "kelly_scale": cfg.kelly_scale,
            "bankroll_start": cfg.bankroll_start,
        }, f, indent=2)

if __name__ == "__main__":
    main()
