#!/usr/bin/env python3
"""
Matrix backtest for tennis-betting-bot.

- Loads base market/prob data from, in order of availability:
    1) outputs/prob_enriched.csv
    2) data/raw/vigfree_matches.csv
- Uses vig-free probabilities + odds to compute edges.
- If a realized outcome column is present, uses realized PnL.
  Otherwise, uses expected value (EV) as proxy.

Outputs:
  results/backtests/summary.csv            (grid results)
  results/backtests/logs/picks_cfg<N>.csv  (per-config pick logs)
  results/backtests/params_cfg<N>.json     (params for each config)
"""

from __future__ import annotations
import csv, json, math, os
from pathlib import Path
from statistics import mean, pstdev
from datetime import datetime

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR   = REPO_ROOT / "outputs"
RAW_DIR   = REPO_ROOT / "data" / "raw"
RES_DIR   = REPO_ROOT / "results"
BT_DIR    = RES_DIR / "backtests"
LOG_DIR   = BT_DIR / "logs"

# ---- env / defaults (inherit from pipeline but safe alone) --------------------
BANKROLL_START        = float(os.getenv("BANKROLL_START", "1000.0"))
KELLY_FRACTION_DFT    = float(os.getenv("KELLY_FRACTION", "0.5"))
KELLY_SCALE_DFT       = float(os.getenv("KELLY_SCALE", "1.0"))
STAKE_CAP_PCT_DFT     = float(os.getenv("STAKE_CAP_PCT", "0.04"))
DAILY_RISK_BUDGET_PCT = float(os.getenv("DAILY_RISK_BUDGET_PCT", "0.12"))
MAX_MATCHES_PER_EVENT = int(float(os.getenv("MAX_MATCHES_PER_EVENT", "3")))

MIN_EDGE_EV_DFT       = float(os.getenv("MIN_EDGE_EV", "0.02"))
MIN_PROBABILITY_DFT   = float(os.getenv("MIN_PROBABILITY", "0.05"))

def log(msg: str):
    print(f"[backtest] {msg}", flush=True)

# ---- IO helpers ---------------------------------------------------------------
def find_input() -> Path | None:
    # prefer prob_enriched (already normalized), fall back to vigfree
    cand = [OUT_DIR / "prob_enriched.csv", RAW_DIR / "vigfree_matches.csv"]
    for p in cand:
        if p.exists() and p.stat().st_size > 0:
            return p
    return None

def read_csv(path: Path) -> list[dict]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []

def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("config_id,event_date,tournament,player,side,odds,prob,edge,stake,delta,bankroll\n")
        return
    fields = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})

def write_json(obj, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2))

def csv_has_rows(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size == 0: return False
        with path.open("r", encoding="utf-8") as f:
            r = csv.reader(f); _ = next(r, None); return next(r, None) is not None
    except Exception:
        return False

# ---- math --------------------------------------------------------------------
def ffloat(x):
    try: return float(x)
    except: return None

def kelly(prob: float, odds: float) -> float:
    b = odds - 1.0
    if b <= 0: return 0.0
    edge = b*prob - (1.0 - prob)
    return max(0.0, edge / b)

def ev_edge(prob: float, odds: float) -> float:
    return prob * odds - 1.0

def sharpe_from_deltas(deltas: list[float]) -> float:
    if not deltas: return 0.0
    mu = mean(deltas)
    sd = math.sqrt(pstdev(deltas)**2 + 1e-12)
    return mu / sd if sd > 0 else 0.0

# ---- outcome detection --------------------------------------------------------
def detect_outcome(row: dict) -> int|None:
    """
    Returns +1 if A wins, -1 if B wins, None if unknown.
    Accepted columns (case-insensitive):
      - 'winner' in {'A','B', 'player_a','player_b', '<name>' match}
      - 'result' or 'outcome' in {'A','B','W','L'} with side context is messy;
        we only support clear 'winner'.
    """
    keys = {k.lower(): k for k in row.keys()}
    # explicit winner side
    for k in ("winner","winner_side","win_side"):
        if k in keys:
            v = str(row[keys[k]]).strip().lower()
            if v in ("a","player_a","home","1"): return +1
            if v in ("b","player_b","away","2"): return -1
    # explicit winner by name
    a = (row.get("player_a") or row.get("home") or "").strip().lower()
    b = (row.get("player_b") or row.get("away") or "").strip().lower()
    for k in ("winner_name","winner_player","winner"):
        if k in keys:
            v = str(row[keys[k]]).strip().lower()
            if v and a and v == a: return +1
            if v and b and v == b: return -1
    return None  # unknown

# ---- pick simulation ----------------------------------------------------------
def simulate_config(cfg_id: int, params: dict, rows: list[dict]) -> dict:
    """
    params:
      MIN_EDGE_EV, MIN_PROBABILITY, KELLY_FRACTION, KELLY_SCALE,
      STAKE_CAP_PCT, DAILY_RISK_BUDGET_PCT
    """
    bankroll = float(BANKROLL_START)
    daily_budget_pct = float(params["DAILY_RISK_BUDGET_PCT"])
    daily_date = None
    daily_spent = 0.0

    picks_log: list[dict] = []
    bet_deltas: list[float] = []
    total_staked = 0.0
    wins = 0; losses = 0

    # event limiter (by tournament/date "event" key)
    event_count: dict[str,int] = {}

    for r in rows:
        # date normalization
        d = (r.get("event_date") or r.get("date") or "").strip() or "0000-00-00"
        if daily_date != d:
            daily_date = d
            daily_spent = 0.0

        # basics
        pa = ffloat(r.get("prob_a_vigfree") or r.get("prob_a") or r.get("implied_prob_a"))
        pb = ffloat(r.get("prob_b_vigfree") or r.get("prob_b") or r.get("implied_prob_b"))
        oa = ffloat(r.get("odds_a")); ob = ffloat(r.get("odds_b"))
        if None in (pa,pb,oa,ob) or oa<=1.0 or ob<=1.0:
            continue

        # edges
        edge_a = ev_edge(pa, oa)
        edge_b = ev_edge(pb, ob)

        # eligibility
        min_ev  = float(params["MIN_EDGE_EV"])
        min_pr  = float(params["MIN_PROBABILITY"])

        cand = []
        if pa >= min_pr and edge_a >= min_ev:
            cand.append(("A", pa, oa, edge_a, r.get("player_a","A")))
        if pb >= min_pr and edge_b >= min_ev:
            cand.append(("B", pb, ob, edge_b, r.get("player_b","B")))
        if not cand:
            continue

        # per-event cap
        ev_key = f"{d}|{r.get('tournament','?')}"
        if event_count.get(ev_key,0) >= MAX_MATCHES_PER_EVENT:
            continue
        event_count[ev_key] = event_count.get(ev_key,0)+1

        # choose best edge if both sides eligible
        side, p, o, e, name = max(cand, key=lambda x: x[3])

        # stake sizing
        f = kelly(p, o) * float(params["KELLY_FRACTION"]) * float(params["KELLY_SCALE"])
        stake_cap = bankroll * float(params["STAKE_CAP_PCT"])
        daily_cap = bankroll * daily_budget_pct
        stake = min(bankroll * f, stake_cap, max(0.0, daily_cap - daily_spent))
        if stake <= 0:  # daily budget exhausted or zero Kelly
            continue

        # realized vs expected outcome
        outcome = detect_outcome(r)  # +1 A wins, -1 B wins, None
        if outcome is None:
            # expected value delta (not changing bankroll for EV mode to avoid drift)
            delta = stake * e
            new_br = bankroll  # keep bankroll stable in EV mode
        else:
            win = (outcome == +1 and side=="A") or (outcome == -1 and side=="B")
            payout = stake * o if win else 0.0
            delta  = payout - stake
            new_br = bankroll + delta
            wins  += int(win)
            losses+= int(not win)

        # log
        picks_log.append({
            "config_id": cfg_id,
            "event_date": d,
            "tournament": r.get("tournament",""),
            "player": name,
            "side": side,
            "odds": round(o,3),
            "prob": round(p,6),
            "edge": round(e,6),
            "stake": round(stake,2),
            "delta": round(delta,2),
            "bankroll": round(new_br,2),
        })
        bet_deltas.append(delta)
        total_staked += stake
        daily_spent  += stake
        bankroll = new_br

    n_bets = len(picks_log)
    pnl = sum(bet_deltas)
    roi = (pnl / total_staked) if total_staked > 0 else 0.0
    hitrate = (wins / (wins+losses)) if (wins+losses)>0 else 0.0
    sharpe = sharpe_from_deltas(bet_deltas)

    return {
        "cfg_id": cfg_id,
        "n_bets": n_bets,
        "total_staked": round(total_staked,2),
        "pnl": round(pnl,2),
        "roi": round(roi,4),
        "hitrate": round(hitrate,4),
        "sharpe": round(sharpe,4),
        "end_bankroll": round(bankroll,2),
        "picks_log": picks_log,
    }

# ---- grid --------------------------------------------------------------------
def build_grid() -> list[dict]:
    """
    Conservative grid sizes — fast and useful. Expand later.
    """
    min_edge_opts = [0.01, 0.015, 0.02, 0.03]
    min_prob_opts = [0.03, 0.04, 0.05]
    kelly_frac    = [0.25, 0.5, 0.75]
    stake_cap     = [0.02, 0.04]
    daily_budget  = [0.08, 0.12, 0.18]

    grid = []
    cfg_id = 0
    for me in min_edge_opts:
        for mp in min_prob_opts:
            for kf in kelly_frac:
                for sc in stake_cap:
                    for db in daily_budget:
                        cfg_id += 1
                        grid.append({
                            "CFG_ID": cfg_id,
                            "MIN_EDGE_EV": me,
                            "MIN_PROBABILITY": mp,
                            "KELLY_FRACTION": kf,
                            "KELLY_SCALE": 1.0,
                            "STAKE_CAP_PCT": sc,
                            "DAILY_RISK_BUDGET_PCT": db,
                        })
    return grid

# ---- main --------------------------------------------------------------------
def main():
    BT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    inp = find_input()
    if not inp or not csv_has_rows(inp):
        log("No usable input (prob_enriched.csv or vigfree_matches.csv). Writing empty summary.")
        (BT_DIR / "summary.csv").write_text("cfg_id,n_bets,total_staked,pnl,roi,hitrate,sharpe,end_bankroll\n")
        return

    rows = read_csv(inp)
    # sort by date if present
    rows.sort(key=lambda r: (r.get("event_date") or r.get("date") or ""))

    grid = build_grid()
    summary_rows = []

    for cfg in grid:
        cfg_id = cfg["CFG_ID"]
        res = simulate_config(cfg_id, cfg, rows)

        # write per-config artifacts
        write_csv(res["picks_log"], LOG_DIR / f"picks_cfg{cfg_id}.csv")
        write_json(cfg,               BT_DIR / f"params_cfg{cfg_id}.json")

        summary_rows.append({
            "cfg_id": cfg_id,
            "n_bets": res["n_bets"],
            "total_staked": res["total_staked"],
            "pnl": res["pnl"],
            "roi": res["roi"],
            "hitrate": res["hitrate"],
            "sharpe": res["sharpe"],
            "end_bankroll": res["end_bankroll"],
        })

    # write summary
    write_csv(summary_rows, BT_DIR / "summary.csv")
    log(f"Backtest done over {len(grid)} configs → {BT_DIR/'summary.csv'}")

if __name__ == "__main__":
    main()
