#!/usr/bin/env python3
# backtest_core.py
from __future__ import annotations
import csv, json, os, math
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
from bet_math import KellyConfig, infer_prob, infer_odds, infer_result, stake_amount, settle_bet, max_drawdown

@dataclass
class BetRow:
    idx: int
    row: Dict
    price: float
    p_model: float
    result: int

def read_rows(input_csv: str) -> List[BetRow]:
    rows: List[BetRow] = []
    with open(input_csv, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        for i, r in enumerate(rdr):
            try:
                price = infer_odds(r)
                p = infer_prob(r)
                if p is None:
                    # no model prob—fallback to market implied (already done in infer_prob).
                    # If still None, skip safely.
                    continue
                res = infer_result(r)
                rows.append(BetRow(i, r, price, p, res))
            except Exception:
                # Skip malformed rows quietly; backtest should be robust.
                continue
    return rows

def filter_by_band(rows: List[BetRow], lo: float, hi: float) -> List[BetRow]:
    return [r for r in rows if (r.price >= lo and r.price < hi)]

def simulate(rows: List[BetRow], cfg: KellyConfig, config_id: str, out_rows: List[Dict]) -> Dict:
    bankroll = cfg.bankroll_init
    equity = [bankroll]
    n = 0
    wins = 0
    total_staked = 0.0
    total_pnl = 0.0

    for br in rows:
        stake, p_used, f_star_raw = stake_amount(cfg, bankroll, br.p_model, br.price)
        # Skip negative/zero-stake bets (unfavorable Kelly)
        if stake <= 0.0:
            continue

        before = bankroll
        bankroll, pnl = settle_bet(bankroll, stake, br.price, br.result)
        after = bankroll

        n += 1
        if pnl > 0: wins += 1
        total_staked += stake
        total_pnl += pnl
        equity.append(after)

        out = {
            "config_id": config_id,
            "config": {
                "stake_mode": cfg.stake_mode,
                "edge": cfg.edge,
                "kelly_scale": cfg.kelly_scale,
                "flat_stake": cfg.flat_stake,
                "bankroll_init": cfg.bankroll_init
            },
            "row_idx": br.idx,
            "price": br.price,
            "p_model": br.p_model,
            "p_used": p_used,
            "kelly_f_raw": f_star_raw,
            "stake": round(stake, 6),
            "result": br.result,
            "pnl": round(pnl, 6),
            "bankroll_before": round(before, 6),
            "bankroll_after": round(after, 6),
        }
        out_rows.append(out)

    if n == 0:
        return {
            "config_id": config_id,
            "bets": 0,
            "wins": 0,
            "hit_rate": 0.0,
            "avg_odds": float("nan"),
            "turnover": 0.0,
            "pnl": 0.0,
            "roi": 0.0,
            "end_bankroll": bankroll,
            "max_drawdown": 0.0
        }

    avg_odds = sum(r.price for r in rows) / len(rows) if len(rows) > 0 else float("nan")
    roi = (total_pnl / total_staked) if total_staked > 0 else 0.0
    mdd = max_drawdown(equity)

    return {
        "config_id": config_id,
        "bets": n,
        "wins": wins,
        "hit_rate": wins / n if n > 0 else 0.0,
        "avg_odds": avg_odds,
        "turnover": total_staked,
        "pnl": total_pnl,
        "roi": roi,
        "end_bankroll": bankroll,
        "max_drawdown": mdd
    }

def parse_bands(bands: str) -> List[Tuple[float, float]]:
    """
    Format: "2.0,2.6|2.6,3.2|3.2,4.0"
    """
    out = []
    for chunk in bands.split("|"):
        chunk = chunk.strip()
        if not chunk: continue
        lohi = chunk.split(",")
        if len(lohi) != 2:
            raise ValueError(f"Bad band: {chunk}")
        lo, hi = float(lohi[0]), float(lohi[1])
        if hi <= lo: raise ValueError(f"Band must have hi>lo: {chunk}")
        out.append((lo, hi))
    return out

def write_results(all_bets: List[Dict], rank_rows: List[Dict], outdir: str) -> Dict:
    os.makedirs(outdir, exist_ok=True)
    # results.csv
    res_path = os.path.join(outdir, "results.csv")
    if all_bets:
        # Flatten config dict
        flat = []
        for r in all_bets:
            flat_r = {**{k:v for k,v in r.items() if k!="config"}}
            cfg = r["config"]
            for ck, cv in cfg.items():
                flat_r[f"cfg_{ck}"] = cv
            flat.append(flat_r)
        keys = sorted({k for rr in flat for k in rr.keys()})
        with open(res_path, "w", newline='', encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=keys)
            wr.writeheader()
            wr.writerows(flat)

    # matrix_rankings.csv
    rank_path = os.path.join(outdir, "matrix_rankings.csv")
    if rank_rows:
        keys = ["config_id","label","bets","wins","hit_rate","avg_odds",
                "turnover","pnl","roi","end_bankroll","max_drawdown"]
        with open(rank_path, "w", newline='', encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=keys)
            wr.writeheader()
            wr.writerows(rank_rows)

    # backtest_metrics.json (best by ROI)
    best = None
    for rr in rank_rows:
        if best is None or rr["roi"] > best["roi"]:
            best = rr
    metrics = {"best_by_roi": best, "n_configs": len(rank_rows)}
    with open(os.path.join(outdir, "backtest_metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    return {"results_csv": res_path, "rankings_csv": rank_path, "metrics_json": os.path.join(outdir, "backtest_metrics.json")}
