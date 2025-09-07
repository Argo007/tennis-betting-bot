#!/usr/bin/env python3
"""
Matrix backtest for tennis-betting-bot (robust loader + diagnostics).

Finds input in this order:
  1) outputs/prob_enriched.csv
  2) outputs/edge_enriched.csv
  3) data/raw/vigfree_matches.csv

Understands many column aliases:
  - date: event_date | date
  - players: player_a/player_b | home/away
  - odds: odds_a/odds_b | price_a/price_b | oa/ob
  - probs: prob_a_vigfree/prob_b_vigfree | prob_a/prob_b | pa/pb | implied_prob_a/_b
  - outcome: winner | winner_side | winner_name

If outcomes are missing, uses EV deltas (does NOT drift bankroll on EV to avoid bias).

Writes:
  results/backtests/summary.csv
  results/backtests/logs/picks_cfg<N>.csv
  results/backtests/params_cfg<N>.json
  results/backtests/_diagnostics.json  (why we got 0 rows, etc.)
"""

from __future__ import annotations
import csv, json, math, os
from pathlib import Path
from statistics import mean, pstdev

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR   = REPO_ROOT / "outputs"
RAW_DIR   = REPO_ROOT / "data" / "raw"
RES_DIR   = REPO_ROOT / "results"
BT_DIR    = RES_DIR / "backtests"
LOG_DIR   = BT_DIR / "logs"

BANKROLL_START        = float(os.getenv("BANKROLL_START", "1000.0"))
MAX_MATCHES_PER_EVENT = int(float(os.getenv("MAX_MATCHES_PER_EVENT", "3")))

def log(m): print(f"[backtest] {m}", flush=True)

# ------- IO helpers ------------------------------------------------------------
def find_input() -> Path | None:
    for p in [OUT_DIR/"prob_enriched.csv", OUT_DIR/"edge_enriched.csv", RAW_DIR/"vigfree_matches.csv"]:
        if p.exists() and p.stat().st_size > 0:
            return p
    return None

def read_csv_rows(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("cfg_id,n_bets,total_staked,pnl,roi,hitrate,sharpe,end_bankroll\n" if path.name=="summary.csv"
                        else "config_id,event_date,tournament,player,side,odds,prob,edge,stake,delta,bankroll\n")
        return
    fields = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        w.writerows(rows)

def write_json(obj, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2))

def csv_has_data(path: Path) -> bool:
    try:
        with path.open("r", encoding="utf-8") as f:
            r = csv.reader(f); next(r, None); return next(r, None) is not None
    except Exception:
        return False

# ------- column resolver -------------------------------------------------------
ALIASES = {
    "date": ["event_date","date","match_date"],
    "tour": ["tournament","comp","event","league"],
    "a":    ["player_a","home","a_player","a_name"],
    "b":    ["player_b","away","b_player","b_name"],
    "oa":   ["odds_a","price_a","oa","odd_a","decimal_odds_a"],
    "ob":   ["odds_b","price_b","ob","odd_b","decimal_odds_b"],
    "pa":   ["prob_a_vigfree","prob_a","implied_prob_a","pa","p_a"],
    "pb":   ["prob_b_vigfree","prob_b","implied_prob_b","pb","p_b"],
    "win":  ["winner","winner_side","win_side","winner_name","winner_player"],
}

def pick(row: dict, keys: list[str]) -> str:
    lk = {k.lower(): k for k in row.keys()}
    for k in keys:
        if k in lk: return row[lk[k]]
    return ""

def to_float(x):
    try: return float(x)
    except: return None

def normalize(rows: list[dict]) -> tuple[list[dict], dict]:
    """Return normalized rows + diagnostics (counts & missing)."""
    out = []
    diag = {"total_rows": len(rows), "usable": 0, "reasons": []}
    for r in rows:
        d  = (pick(r, ALIASES["date"]) or "").strip()
        tr = (pick(r, ALIASES["tour"]) or "").strip()
        a  = (pick(r, ALIASES["a"]) or "").strip()
        b  = (pick(r, ALIASES["b"]) or "").strip()
        oa = to_float(pick(r, ALIASES["oa"]))
        ob = to_float(pick(r, ALIASES["ob"]))
        pa = to_float(pick(r, ALIASES["pa"]))
        pb = to_float(pick(r, ALIASES["pb"]))

        if None in (oa,ob,pa,pb) or oa<=1 or ob<=1 or pa<0 or pb<0:
            continue

        out.append({
            "date": d or "",
            "tournament": tr,
            "player_a": a or "A",
            "player_b": b or "B",
            "odds_a": oa, "odds_b": ob,
            "prob_a": pa, "prob_b": pb,
            "raw": r,
        })
    diag["usable"] = len(out)
    if diag["usable"] == 0:
        # sample a reason from first row
        diag["reasons"].append("No rows with both odds and probs present (oa,ob,pa,pb).")
    return out, diag

# ------- math ------------------------------------------------------------------
def kelly(p: float, o: float) -> float:
    b = o - 1.0
    if b <= 0: return 0.0
    edge = b*p - (1.0 - p)
    return max(0.0, edge / b)

def edge_ev(p: float, o: float) -> float:
    return p*o - 1.0

def sharpe(deltas):
    if not deltas: return 0.0
    mu = mean(deltas); var = pstdev(deltas)**2
    sd = math.sqrt(var + 1e-12)
    return mu/sd if sd>0 else 0.0

# ------- simulation ------------------------------------------------------------
def simulate(cfg_id: int, params: dict, rows: list[dict]) -> dict:
    bankroll = float(params["BANKROLL_START"])
    daily_budget_pct = float(params["DAILY_RISK_BUDGET_PCT"])
    stake_cap_pct    = float(params["STAKE_CAP_PCT"])
    kf = float(params["KELLY_FRACTION"]) * float(params["KELLY_SCALE"])

    daily_key = None
    daily_spent = 0.0
    event_count = {}

    picks = []
    deltas = []
    wins=losses=0
    total_staked=0.0

    for r in rows:
        d = r["date"]
        if daily_key != d:
            daily_key = d; daily_spent = 0.0

        pa, pb = r["prob_a"], r["prob_b"]
        oa, ob = r["odds_a"], r["odds_b"]
        ea, eb = edge_ev(pa,oa), edge_ev(pb,ob)

        choose = []
        if pa >= params["MIN_PROBABILITY"] and ea >= params["MIN_EDGE_EV"]:
            choose.append(("A", pa, oa, ea, r["player_a"]))
        if pb >= params["MIN_PROBABILITY"] and eb >= params["MIN_EDGE_EV"]:
            choose.append(("B", pb, ob, eb, r["player_b"]))
        if not choose: continue
        side,p,o,e,name = max(choose, key=lambda x:x[3])

        event_id = f"{d}|{r['tournament']}"
        if event_count.get(event_id,0) >= MAX_MATCHES_PER_EVENT:
            continue
        event_count[event_id] = event_count.get(event_id,0)+1

        frac  = kelly(p,o) * kf
        stake = min(bankroll*frac, bankroll*stake_cap_pct, bankroll*daily_budget_pct - daily_spent)
        if stake <= 0: continue

        # outcome (optional)
        winflag = None
        w = (pick(r["raw"], ALIASES["win"]) or "").strip().lower()
        if w in ("a","player_a","home","1", r["player_a"].strip().lower()):
            winflag = (side=="A")
        elif w in ("b","player_b","away","2", r["player_b"].strip().lower()):
            winflag = (side=="B")

        if winflag is None:
            delta = stake*e   # EV mode (don’t update bankroll to avoid drift)
            new_br = bankroll
        else:
            payout = stake*o if winflag else 0.0
            delta  = payout - stake
            new_br = bankroll + delta
            wins += int(winflag); losses += int(not winflag)

        picks.append({
            "config_id": cfg_id,
            "event_date": d, "tournament": r["tournament"],
            "player": name, "side": side,
            "odds": round(o,3), "prob": round(p,6), "edge": round(e,6),
            "stake": round(stake,2), "delta": round(delta,2), "bankroll": round(new_br,2),
        })
        total_staked += stake; deltas.append(delta); bankroll = new_br; daily_spent += stake

    pnl = sum(deltas)
    roi = pnl/total_staked if total_staked>0 else 0.0
    hit = wins/(wins+losses) if (wins+losses)>0 else 0.0
    shp = sharpe(deltas)

    return {
        "cfg_id": cfg_id, "n_bets": len(picks),
        "total_staked": round(total_staked,2), "pnl": round(pnl,2),
        "roi": round(roi,4), "hitrate": round(hit,4), "sharpe": round(shp,4),
        "end_bankroll": round(bankroll,2), "picks": picks,
    }

# ------- grid ------------------------------------------------------------------
def grid() -> list[dict]:
    def cfg(me, mp, kf, sc, db):
        return {
            "MIN_EDGE_EV": me, "MIN_PROBABILITY": mp,
            "KELLY_FRACTION": kf, "KELLY_SCALE": 1.0,
            "STAKE_CAP_PCT": sc, "DAILY_RISK_BUDGET_PCT": db,
            "BANKROLL_START": float(os.getenv("BANKROLL_START","1000")),
        }
    g=[]
    for me in [0.01,0.015,0.02,0.03]:
        for mp in [0.03,0.04,0.05]:
            for kf in [0.25,0.5,0.75]:
                for sc in [0.02,0.04]:
                    for db in [0.08,0.12,0.18]:
                        g.append(cfg(me,mp,kf,sc,db))
    return g

# ------- main ------------------------------------------------------------------
def main():
    BT_DIR.mkdir(parents=True, exist_ok=True); LOG_DIR.mkdir(parents=True, exist_ok=True)
    inp = find_input()
    diag = {"input": str(inp) if inp else None}
    if not inp:
        write_csv([], BT_DIR/"summary.csv")
        write_json({"reason": "no_input_file", **diag}, BT_DIR/"_diagnostics.json")
        log("No input file found."); return

    raw = read_csv_rows(inp)
    norm, nd = normalize(raw)
    diag.update(nd)
    if nd["usable"] == 0:
        write_csv([], BT_DIR/"summary.csv")
        write_json({"reason": "no_usable_rows", **diag}, BT_DIR/"_diagnostics.json")
        log("Input has no usable rows with odds & probs."); return

    # sort by date string (already ISO-like)
    norm.sort(key=lambda r: r["date"])
    configs = grid()

    summary=[]
    for i, p in enumerate(configs, start=1):
        res = simulate(i, p, norm)
        write_csv(res["picks"], LOG_DIR/f"picks_cfg{i}.csv")
        write_json(p, BT_DIR/f"params_cfg{i}.json")
        summary.append({k: res[k] for k in ("cfg_id","n_bets","total_staked","pnl","roi","hitrate","sharpe","end_bankroll")})

    write_csv(summary, BT_DIR/"summary.csv")
    write_json(diag, BT_DIR/"_diagnostics.json")
    log(f"Backtest done over {len(configs)} configs → {BT_DIR/'summary.csv'}")

if __name__ == "__main__":
    main()
