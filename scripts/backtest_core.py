#!/usr/bin/env python3
"""
Core backtest utilities: transform, select, stake, simulate.
Importable by backtest_all.py and matrix_backtest.py.
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import pandas as pd

@dataclass
class Config:
    cfg_id: int = 1
    dataset: str = "data/raw/odds/sample_odds_enriched.csv"
    bands: tuple[float,float] = (1.2, 2.0)   # odds range inclusive
    min_edge: float = 0.0
    staking: str = "kelly"
    kelly_scale: float = 0.5
    bankroll: float = 1000.0

def _long_format(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        for side in ("A","B"):
            odds = r["odds_a"] if side == "A" else r["odds_b"]
            row = {
                "date": r["date"],
                "player": r["player_a"] if side == "A" else r["player_b"],
                "opp":    r["player_b"] if side == "A" else r["player_a"],
                "side": side,
                "odds": float(odds),
                "model_prob": float(r["model_prob_a"] if side == "A" else r["model_prob_b"]),
                "implied_prob": float(r["implied_prob_a"] if side == "A" else r["implied_prob_b"]),
                "edge": float(r["edge_a"] if side == "A" else r["edge_b"]),
                "winner": bool(r.get("winner", "X") == side),
            }
            rows.append(row)
    return pd.DataFrame(rows)

def _match_key(row: pd.Series) -> str:
    a, b = sorted([row["player"], row["opp"]])
    return f"{row['date']}|{a} vs {b}"

def select_signals(df_long: pd.DataFrame, bands: tuple[float,float], min_edge: float) -> pd.DataFrame:
    lo, hi = bands
    cand = df_long[(df_long["odds"] >= lo) & (df_long["odds"] <= hi) & (df_long["edge"] > min_edge)].copy()
    cand["match"] = cand.apply(_match_key, axis=1)
    best = cand.sort_values(["match","edge"], ascending=[True, False]).groupby("match").head(1).reset_index(drop=True)
    return best

def kelly_stake(bankroll: float, odds: float, p: float, scale: float) -> float:
    b = odds - 1.0
    q = 1.0 - p
    f_star = (b*p - q) / b if b != 0 else 0.0
    return max(0.0, scale * f_star * bankroll)

def simulate(cfg: Config) -> tuple[pd.DataFrame, pd.DataFrame]:
    src = Path(cfg.dataset)
    if not src.exists():
        raise FileNotFoundError(f"Dataset not found: {src}")
    raw = pd.read_csv(src)
    long = _long_format(raw)
    sigs = select_signals(long, cfg.bands, cfg.min_edge)

    bankroll = float(cfg.bankroll)
    bets = []
    for _, s in sigs.iterrows():
        stake = 0.0
        if cfg.staking == "kelly":
            stake = kelly_stake(bankroll, s["odds"], s["model_prob"], cfg.kelly_scale)
        elif cfg.staking == "flat":
            stake = min(0.02 * bankroll, bankroll)  # example flat 2%

        result = "win" if s["winner"] else "loss"
        pnl = stake * (s["odds"] - 1.0) if result == "win" else -stake
        bankroll += pnl
        bets.append({
            "date": s["date"], "match": s["match"], "pick": s["player"],
            "odds": round(s["odds"],2), "edge": round(s["edge"],4),
            "p_model": round(s["model_prob"],4), "stake": round(stake,2),
            "result": result, "pnl": round(pnl,2), "bankroll_after": round(bankroll,2),
        })

    bets_df = pd.DataFrame(bets)
    total_staked = float(round(bets_df["stake"].sum(), 2)) if len(bets_df) else 0.0
    pnl = float(round(bets_df["pnl"].sum(), 2)) if len(bets_df) else 0.0
    roi = float(round((pnl / total_staked), 4)) if total_staked > 0 else 0.0
    summary = pd.DataFrame([{
        "cfg_id": cfg.cfg_id, "n_bets": int(len(bets_df)),
        "total_staked": total_staked, "pnl": pnl, "roi": roi,
        "end_bankroll": float(round(bankroll,2)),
    }])
    return bets_df, summary

