#!/usr/bin/env python3
# bet_math.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Tuple, List, Dict
import math

@dataclass(frozen=True)
class KellyConfig:
    stake_mode: str = "kelly"          # "kelly" or "flat"
    edge: float = 0.08                 # e.g., 0.08 => “TE8”
    kelly_scale: float = 0.5           # 0.5 = half-Kelly
    flat_stake: float = 1.0            # units per bet when stake_mode="flat"
    bankroll_init: float = 100.0

def clamp01(x: float) -> float:
    if x < 0.0: return 0.0
    if x > 1.0: return 1.0
    return x

def infer_prob(row: Dict) -> float:
    """
    Try to find model probability in row.
    Accepted keys (first match wins): 'p', 'prob', 'model_prob', 'p_model'
    Fallback: 1 / odds (market-implied) if odds present.
    """
    for k in ("p", "prob", "model_prob", "p_model"):
        if k in row and row[k] is not None:
            try:
                v = float(row[k])
                if math.isnan(v): continue
                return clamp01(v)
            except Exception:
                pass
    # fallback to market implied
    for ok in ("odds","price","decimal_odds"):
        if ok in row and row[ok] not in (None, ""):
            try:
                o = float(row[ok])
                if o > 1.0:
                    return clamp01(1.0/o)
            except Exception:
                pass
    # If all else fails, no edge—return None and caller decides.
    return None

def infer_odds(row: Dict) -> float:
    for ok in ("odds","price","decimal_odds"):
        if ok in row and row[ok] not in (None, ""):
            try:
                o = float(row[ok])
                if o > 1.0:
                    return o
            except Exception:
                pass
    raise ValueError("No odds/price found in row; expected one of ['odds','price','decimal_odds'].")

def infer_result(row: Dict) -> int:
    """
    Accepts 1/0 or True/False under keys:
    'result','won','outcome','is_win','y','label'
    """
    for rk in ("result","won","outcome","is_win","y","label"):
        if rk in row:
            v = row[rk]
            if isinstance(v, bool):
                return 1 if v else 0
            try:
                f = float(v)
                return 1 if f >= 1.0 else 0
            except Exception:
                s = str(v).strip().lower()
                if s in ("win","won","true","yes"): return 1
                if s in ("loss","lost","false","no"): return 0
    raise ValueError("No result column found; expected one of ['result','won','outcome','is_win','y','label'].")

def kelly_fraction(p_model: float, price: float, edge: float) -> float:
    """
    f* = (b*p - (1-p))/b, with b = price - 1
    p = clamp01(p_model*(1+edge))
    """
    b = price - 1.0
    if b <= 0:
        return 0.0
    p = clamp01(p_model * (1.0 + edge))
    f_star = (b * p - (1.0 - p)) / b
    return max(0.0, f_star)

def stake_amount(cfg: KellyConfig, bankroll: float, p_model: float, price: float) -> Tuple[float, float, float]:
    """
    Returns (stake, p_used, f_star_raw).
    p_used is the clamped/edged p that fed Kelly; f_star_raw is unscaled Kelly before kelly_scale.
    """
    if cfg.stake_mode == "flat":
        stake = min(bankroll, max(0.0, cfg.flat_stake))
        return stake, p_model, 0.0
    # Kelly
    f_star = kelly_fraction(p_model, price, cfg.edge)  # >= 0
    if f_star <= 0.0:
        return 0.0, clamp01(p_model * (1.0 + cfg.edge)), f_star
    scaled = f_star * max(0.0, cfg.kelly_scale)
    stake = bankroll * scaled
    stake = min(stake, bankroll)  # can't bet more than bankroll
    return stake, clamp01(p_model*(1.0+cfg.edge)), f_star

def settle_bet(bankroll: float, stake: float, price: float, is_win: int) -> Tuple[float, float]:
    """
    Returns (new_bankroll, pnl)
    """
    if stake <= 0.0:
        return bankroll, 0.0
    if is_win == 1:
        profit = stake * (price - 1.0)
        return bankroll + profit, profit
    else:
        return bankroll - stake, -stake

def max_drawdown(equity_curve: Iterable[float]) -> float:
    peak = -1e18
    mdd = 0.0
    for x in equity_curve:
        if x > peak: peak = x
        dd = (peak - x)
        if dd > mdd: mdd = dd
    return mdd
