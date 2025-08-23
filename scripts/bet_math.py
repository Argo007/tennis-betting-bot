#!/usr/bin/env python3
# bet_math.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Iterable, Tuple, Optional
import math

@dataclass(frozen=True)
class KellyConfig:
    stake_mode: str = "kelly"          # "kelly" or "flat"
    edge: float = 0.08                 # TE8 by default
    kelly_scale: float = 0.5           # 0.5 = half-Kelly
    flat_stake: float = 1.0            # when stake_mode="flat"
    bankroll_init: float = 100.0

# ------- helpers -------
def clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x

def _get_float(row: Dict, keys) -> Optional[float]:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            try:
                v = float(row[k])
                if math.isnan(v):  # type: ignore
                    continue
                return v
            except Exception:
                continue
    return None

def infer_prob(row: Dict) -> Optional[float]:
    p = _get_float(row, ("p","prob","model_prob","p_model","probability","pred_prob","win_prob","p_hat"))
    if p is not None:
        return clamp01(p)
    # fallback to market implied
    odds = infer_odds(row, strict=False)
    return clamp01(1.0/odds) if odds and odds > 1 else None

def infer_odds(row: Dict, strict: bool=True) -> Optional[float]:
    o = _get_float(row, ("odds","price","decimal_odds"))
    if strict and (o is None or o <= 1.0):
        raise ValueError("Missing decimal odds; expected one of ['odds','price','decimal_odds'].")
    return o

def infer_result(row: Dict) -> int:
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
                if s in ("win","won","true","yes","w"): return 1
                if s in ("loss","lost","false","no","l"): return 0
    raise ValueError("Missing result; expected one of ['result','won','outcome','is_win','y','label'].")

# ------- Kelly -------
def kelly_fraction(p_model: float, price: float, edge: float) -> float:
    b = price - 1.0
    if b <= 0: return 0.0
    p = clamp01(p_model * (1.0 + edge))
    f_star = (b*p - (1-p)) / b
    return max(0.0, f_star)

def stake_amount(cfg: KellyConfig, bankroll: float, p_model: float, price: float) -> Tuple[float,float,float]:
    if bankroll <= 0: return 0.0, p_model, 0.0
    if cfg.stake_mode == "flat":
        stake = min(bankroll, max(0.0, cfg.flat_stake))
        return stake, p_model, 0.0
    f_star = kelly_fraction(p_model, price, cfg.edge)
    if f_star <= 0.0:
        return 0.0, clamp01(p_model*(1.0+cfg.edge)), f_star
    scaled = f_star * max(0.0, cfg.kelly_scale)
    stake = min(bankroll * scaled, bankroll)
    return stake, clamp01(p_model*(1.0+cfg.edge)), f_star

def settle_bet(bankroll: float, stake: float, price: float, is_win: int) -> Tuple[float,float]:
    if stake <= 0: return bankroll, 0.0
    if is_win: 
        profit = stake * (price - 1.0)
        return bankroll + profit, profit
    return bankroll - stake, -stake

def max_drawdown(equity: Iterable[float]) -> float:
    peak, mdd = -1e18, 0.0
    for x in equity:
        if x > peak: peak = x
        dd = peak - x
        if dd > mdd: mdd = dd
    return mdd
