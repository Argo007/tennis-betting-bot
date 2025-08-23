#!/usr/bin/env python3
"""
compute_prob_vigfree.py

Convert two-sided odds (date,player_a,player_b,odds_a,odds_b)
into flat rows with a model probability p_model per side.

Steps:
- implied probs: p_imp = 1/odds
- remove bookmaker overround: p_fair_i = p_imp_i / (p_imp_A + p_imp_B)
- optional favorite/longshot stretch in logit space: gamma (>1 boosts favs, <1 compresses to 0.5)
- output flat rows with columns: date, player, opponent, price, p_model

If input is already flat (has 'player' + 'opponent' + 'price'), rows are passed through;
if a 'p_model' column exists, it’s preserved (we don’t overwrite).

This is a pragmatic baseline so the engine has non-zero edges
even before you plug in real Elo/model probabilities.
"""

from __future__ import annotations
import argparse, csv, math, os, sys
from typing import Dict, List

def _flt(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def logit(p: float) -> float:
    p = min(max(p, 1e-9), 1 - 1e-9)
    return math.log(p / (1 - p))

def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))

def read_csv(path: str) -> List[Dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(path: str, rows: List[Dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("note\nempty\n")
        return
    keys = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=keys)
        wr.writeheader()
        wr.writerows(rows)

def from_two_sided(row: Dict, gamma: float) -> List[Dict]:
    lc = {k.lower(): k for k in row.keys()}
    need = all(k in lc for k in ("player_a", "player_b", "odds_a", "odds_b"))
    if not need:
        return []

    date = row.get(lc.get("date", "date"), row.get("date", ""))
    pa, pb = row[lc["player_a"]], row[lc["player_b"]]
    oa, ob = _flt(row[lc["odds_a"]], 0.0), _flt(row[lc["odds_b"]], 0.0)
    if oa <= 1.0 or ob <= 1.0:
        return []

    # implied + vig removal
    p_imp_a, p_imp_b = 1.0 / oa, 1.0 / ob
    s = p_imp_a + p_imp_b
    if s <= 0:
        return []

    p_fair_a = p_imp_a / s
    p_fair_b = p_imp_b / s

    # favorite/longshot stretch (logit space)
    if gamma != 1.0:
        p_fair_a = sigmoid(gamma * logit(p_fair_a))
        p_fair_b = 1.0 - p_fair_a  # keep symmetry

    ra = {
        "date": date,
        "player": pa,
        "opponent": pb,
        "price": oa,
        "p_model": round(p_fair_a, 6),
        "source_p": "vigfree_gamma",
    }
    rb = {
        "date": date,
        "player": pb,
        "opponent": pa,
        "price": ob,
        "p_model": round(p_fair_b, 6),
        "source_p": "vigfree_gamma",
    }
    return [ra, rb]

def pass_through(row: Dict) -> Dict:
    # If already flat with player/opponent/price, keep as-is.
    # If p_model missing, leave it blank; the engine may fill via Elo.
    return dict(row)

def main():
    ap = argparse.ArgumentParser(description="Compute p_model from two-sided odds (vig-free + stretch).")
    ap.add_argument("--input", "-i", required=True, help="CSV with two-sided odds OR flat rows.")
    ap.add_argument("--out", "-o", required=True, help="Output flat CSV with p_model.")
    ap.add_argument("--gamma", type=float, default=1.05, help="Favorite/longshot stretch (1=no change).")
    args = ap.parse_args()

    rows = read_csv(args.input)
    if not rows:
        write_csv(args.out, [])
        print("[prob] no input rows; wrote empty output")
        return

    out: List[Dict] = []
    for r in rows:
        lc = {k.lower(): k for k in r.keys()}
        is_two_sided = all(k in lc for k in ("player_a", "player_b", "odds_a", "odds_b"))
        is_flat = all(k in lc for k in ("player", "opponent")) and ("price" in lc or "odds" in lc or "decimal_odds" in lc)

        if is_two_sided:
            out.extend(from_two_sided(r, gamma=args.gamma))
        elif is_flat:
            out.append(pass_through(r))
        else:
            # Unknown shape: skip
            continue

    write_csv(args.out, out)
    print(f"[prob] wrote {len(out)} rows -> {args.out}")

if __name__ == "__main__":
    main()
