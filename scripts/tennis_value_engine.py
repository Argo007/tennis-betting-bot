#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tennis_value_engine.py

Reads a flat odds CSV, ensures we have `price` and `p_model` columns,
computes edge, sizes bets (Kelly or flat), writes:
  - value_picks_pro.csv     (repo root for convenience)
  - outputs/picks_final.csv (inside outputs/)
  - outputs/engine_summary.md

Kelly with True-Edge (TE) booster:
  p_used = clip(p_model * (1 + edge), 0, 1)
  b = price - 1
  f* = (b*p_used - (1-p_used)) / b
  stake_frac = clip(f* * kelly_scale, 0, kelly_cap)

CLI:
  --input INPUT.csv
  --out-picks value_picks_pro.csv
  --out-final outputs/picks_final.csv
  --summary outputs/engine_summary.md
  --stake-mode {kelly,flat}
  --edge 0.08
  --kelly-scale 0.5
  --kelly-cap 0.2
  --flat-stake 1
  --bankroll 1000
  --min-edge 0.02
  --max-picks 80
  --filter-on-te            # NEW: filter on (p_used - 1/price) instead of (p_model - 1/price)
  [--elo-atp FILE] [--elo-wta FILE]  # accepted but optional
"""

from __future__ import annotations
import argparse, csv, math, os, pathlib, statistics as stats
from typing import Dict, List, Optional

def _f(x, d=6):
    try:
        return round(float(x), d)
    except Exception:
        return 0.0

def _clip(x, lo, hi):
    return max(lo, min(hi, x))

def read_csv(path: str) -> List[Dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(path: str, rows: List[Dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["player","opponent","price","p_model","p_used","edge_model","edge_te","stake_frac_br","stake_units"])
        return
    keys, seen = [], set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                keys.append(k); seen.add(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)

def pick_col(header: List[str], candidates: List[str]) -> Optional[str]:
    hset = {c.lower(): c for c in header}
    for c in candidates:
        if c.lower() in hset:
            return hset[c.lower()]
    return None

def ensure_price_prob(rows: List[Dict]) -> tuple[str, Optional[str]]:
    if not rows:
        raise SystemExit("No input rows.")
    hdr = list(rows[0].keys())
    col_price = pick_col(hdr, ["price","odds","decimal_odds"])
    col_prob  = pick_col(hdr, ["p_model","p","prob","model_prob","probability"])
    if not col_price:
        raise SystemExit("No price/odds column found (expected one of price/odds/decimal_odds).")
    for r in rows:
        if "price" not in r and col_price != "price":
            r["price"] = r[col_price]
        if col_prob and "p_model" not in r:
            r["p_model"] = r[col_prob]
    return "price", col_prob and "p_model" or None

def kelly_fraction(price: float, p_used: float) -> float:
    b = max(price - 1.0, 1e-12)
    return (b * p_used - (1.0 - p_used)) / b

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", "-i", required=True)
    ap.add_argument("--out-picks", default="value_picks_pro.csv")
    ap.add_argument("--out-final", default="outputs/picks_final.csv")
    ap.add_argument("--summary", default="outputs/engine_summary.md")

    ap.add_argument("--stake-mode", choices=["kelly","flat"], default="kelly")
    ap.add_argument("--edge", type=float, default=0.08)           # TE8 default
    ap.add_argument("--kelly-scale", type=float, default=0.5)     # half-Kelly
    ap.add_argument("--kelly-cap", type=float, default=0.20)
    ap.add_argument("--flat-stake", type=float, default=1.0)
    ap.add_argument("--bankroll", type=float, default=1000.0)

    ap.add_argument("--min-edge", type=float, default=0.02)
    ap.add_argument("--max-picks", type=int, default=80)
    ap.add_argument("--filter-on-te", action="store_true", help="Filter using TE edge (p_used - 1/price) instead of raw model edge.")

    ap.add_argument("--elo-atp", default="")
    ap.add_argument("--elo-wta", default="")

    args = ap.parse_args()

    rows = read_csv(args.input)
    price_key, prob_key = ensure_price_prob(rows)

    enriched: List[Dict] = []
    for r in rows:
        price = _f(r.get("price", 0))
        if price <= 1.0:
            continue
        breakeven = 1.0 / price

        if "p_model" in r and r["p_model"] not in (None, "", "NA"):
            p_model = _f(r["p_model"])
            p_model = _clip(p_model, 0.0, 1.0)
        else:
            p_model = breakeven  # fallback

        p_used = _clip(p_model * (1.0 + args.edge), 0.0, 1.0)

        edge_model = p_model - breakeven
        edge_te    = p_used  - breakeven

        stake_frac_br = 0.0
        stake_units   = 0.0

        if args.stake_mode == "kelly":
            f_raw = kelly_fraction(price, p_used)
            f_adj = max(0.0, f_raw) * args.kelly_scale
            stake_frac_br = _clip(f_adj, 0.0, args.kelly_cap)
            stake_units   = args.bankroll * stake_frac_br
        else:
            stake_units   = args.flat_stake
            stake_frac_br = stake_units / max(args.bankroll, 1e-9)

        out = dict(r)
        out.setdefault("player", r.get("player", r.get("player_a","")))
        out.setdefault("opponent", r.get("opponent", r.get("player_b","")))
        out["price"]         = price
        out["breakeven"]     = _f(breakeven, 6)
        out["p_model"]       = _f(p_model, 6)
        out["p_used"]        = _f(p_used, 6)
        out["edge_model"]    = _f(edge_model, 6)
        out["edge_te"]       = _f(edge_te, 6)
        out["kelly_f_raw"]   = _f(kelly_fraction(price, p_used), 6)
        out["stake_frac_br"] = _f(stake_frac_br, 6)
        out["stake_units"]   = _f(stake_units, 4)
        enriched.append(out)

    # === filtering ===
    if args.filter_on_te:
        # use TE-boosted edge for selection
        picks = [r for r in enriched if r["edge_te"] >= args.min_edge]
    else:
        # legacy: use raw model edge
        picks = [r for r in enriched if r["edge_model"] >= args.min_edge]

    # Sort: higher edge first, then better (lower) price
    key_field = "edge_te" if args.filter_on_te else "edge_model"
    picks.sort(key=lambda r: (r[key_field], -r["price"]), reverse=True)
    if args.max_picks and len(picks) > args.max_picks:
        picks = picks[: args.max_picks]

    write_csv(args.out_picks, picks)
    write_csv(args.out_final, picks)

    os.makedirs(os.path.dirname(args.summary) or ".", exist_ok=True)
    n = len(picks)
    avg_odds = _f(stats.mean([r["price"] for r in picks]), 3) if n else 0.0
    avg_edge_raw = _f(stats.mean([r["edge_model"] for r in picks]), 3) if n else 0.0
    avg_edge_te  = _f(stats.mean([r["edge_te"] for r in picks]), 3) if n else 0.0
    total_stake = _f(sum(r["stake_units"] for r in picks), 4)

    lines = []
    lines.append("# Tennis Value — Daily Picks")
    lines.append("")
    lines.append(f"- Picks: **{n}**")
    lines.append(f"- Min edge: **{args.min_edge:.3f}**  |  Filter on TE: **{'yes' if args.filter_on_te else 'no'}**")
    lines.append(f"- Kelly: mode=**{args.stake_mode}**, TE=**{args.edge:.2f}**, scale=**{args.kelly_scale}**, cap=**{args.kelly_cap}**")
    lines.append(f"- Bankroll: **{args.bankroll:.2f}**")
    lines.append(f"- Total stake: **{total_stake:.4f}**")
    lines.append(f"- Avg odds: **{avg_odds:.3f}** | Avg edge (raw): **{avg_edge_raw:.3f}** | Avg edge (TE): **{avg_edge_te:.3f}**")
    lines.append(f"- Elo loaded: **{'yes' if (args.elo_atp or args.elo_wta) else 'no'}**")
    lines.append("")
    pathlib.Path(args.summary).write_text("\n".join(lines) + "\n", encoding="utf-8")

if __name__ == "__main__":
    main()
