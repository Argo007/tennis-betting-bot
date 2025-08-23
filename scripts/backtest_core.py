#!/usr/bin/env python3
# backtest_core.py
from __future__ import annotations
import csv, json, os
from dataclasses import dataclass
from typing import List, Dict, Tuple
from bet_math import KellyConfig, infer_prob, infer_odds, infer_result, stake_amount, settle_bet, max_drawdown

@dataclass
class BetRow:
    idx: int
    src: Dict
    price: float
    p_model: float
    result: int

def read_rows(input_csv: str) -> List[BetRow]:
    rows: List[BetRow] = []
    with open(input_csv, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        for i, r in enumerate(rdr):
            try:
                price = infer_odds(r) or 0.0
                p = infer_prob(r)
                if p is None: 
                    continue
                res = infer_result(r)
                rows.append(BetRow(i, r, price, p, res))
            except Exception:
                continue
    return rows

def filter_by_band(rows: List[BetRow], lo: float, hi: float) -> List[BetRow]:
    return [r for r in rows if lo <= r.price < hi]

def simulate(rows: List[BetRow], cfg: KellyConfig, config_id: str, all_bets_out: List[Dict]) -> Dict:
    bankroll = cfg.bankroll_init
    equity = [bankroll]
    bets, wins, turnover, pnl = 0, 0, 0.0, 0.0

    for br in rows:
        stake, p_used, f_raw = stake_amount(cfg, bankroll, br.p_model, br.price)
        if stake <= 0: 
            continue
        before = bankroll
        bankroll, bet_pnl = settle_bet(bankroll, stake, br.price, br.result)
        after = bankroll

        bets += 1
        wins += int(bet_pnl > 0)
        turnover += stake
        pnl += bet_pnl
        equity.append(after)

        all_bets_out.append({
            "config_id": config_id,
            "config": {"stake_mode": cfg.stake_mode, "edge": cfg.edge, "kelly_scale": cfg.kelly_scale,
                       "flat_stake": cfg.flat_stake, "bankroll_init": cfg.bankroll_init},
            "row_idx": br.idx,
            "price": br.price,
            "p_model": br.p_model,
            "p_used": p_used,
            "kelly_f_raw": f_raw,
            "stake": round(stake, 6),
            "result": br.result,
            "pnl": round(bet_pnl, 6),
            "bankroll_before": round(before, 6),
            "bankroll_after": round(after, 6),
        })

    if bets == 0:
        return {"bets":0,"wins":0,"hit_rate":0.0,"avg_odds":float("nan"),
                "turnover":0.0,"pnl":0.0,"roi":0.0,"end_bankroll":bankroll,"max_drawdown":0.0}

    avg_odds = sum(r.price for r in rows)/len(rows) if rows else float("nan")
    roi = (pnl/turnover) if turnover>0 else 0.0
    mdd = max_drawdown(equity)
    return {"bets":bets,"wins":wins,"hit_rate":wins/bets,"avg_odds":avg_odds,
            "turnover":turnover,"pnl":pnl,"roi":roi,"end_bankroll":bankroll,"max_drawdown":mdd}

def parse_bands(bands: str) -> List[Tuple[float,float]]:
    out = []
    for chunk in bands.split("|"):
        if not chunk.strip(): continue
        a,b = map(float, chunk.split(","))
        if b <= a: raise ValueError(f"Bad band: {chunk}")
        out.append((a,b))
    return out

def write_results(all_bets: List[Dict], ranks: List[Dict], outdir: str) -> Dict:
    os.makedirs(outdir, exist_ok=True)
    # results.csv
    res_path = os.path.join(outdir, "results.csv")
    if all_bets:
        flat = []
        for r in all_bets:
            fr = {k:v for k,v in r.items() if k!="config"}
            for ck,cv in r["config"].items():
                fr[f"cfg_{ck}"] = cv
            flat.append(fr)
        keys = sorted({k for rr in flat for k in rr.keys()})
        import csv
        with open(res_path, "w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=keys)
            wr.writeheader(); wr.writerows(flat)

    # matrix_rankings.csv
    rank_path = os.path.join(outdir, "matrix_rankings.csv")
    if ranks:
        keys = ["config_id","label","bets","wins","hit_rate","avg_odds","turnover","pnl","roi","end_bankroll","max_drawdown"]
        with open(rank_path, "w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=keys)
            wr.writeheader(); wr.writerows(ranks)

    # backtest_metrics.json (best by ROI)
    best = None
    for rr in ranks:
        if best is None or rr["roi"] > best["roi"]:
            best = rr
    metrics = {"best_by_roi": best, "n_configs": len(ranks)}
    with open(os.path.join(outdir, "backtest_metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    return {"results_csv":res_path, "rankings_csv":rank_path,
            "metrics_json":os.path.join(outdir,"backtest_metrics.json")}
