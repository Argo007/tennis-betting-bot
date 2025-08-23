#!/usr/bin/env python3
# tennis_value_engine.py
# Drop-in engine that:
# 1) reads a candidates CSV,
# 2) filters by min edge,
# 3) sizes picks with Kelly (TE edge booster, safety scaler, caps),
# 4) writes value_picks_pro.csv (root) + outputs/picks_with_stakes.csv,
# 5) emits outputs/engine_summary.md

from __future__ import annotations
import argparse, csv, os, math, sys
from typing import List, Dict, Optional
from bet_math import KellyConfig, infer_prob, infer_odds, stake_amount

def clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x

def f2(x: float) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def read_rows(path: str) -> List[Dict]:
    with open(path, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        return list(rdr)

def write_csv(path: str, rows: List[Dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not rows:
        # write empty with a minimal header
        with open(path, "w", newline='', encoding="utf-8") as f:
            f.write("note\nempty\n")
        return
    keys = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline='', encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=keys)
        wr.writeheader()
        wr.writerows(rows)

def main():
    ap = argparse.ArgumentParser(description="Value engine with Kelly sizing (TE booster + caps).")
    # I/O
    ap.add_argument("--input", "-i",
                    default="data/raw/odds/sample_odds.csv",
                    help="Candidates CSV with at least odds/price and (preferably) model prob + result for offline QA.")
    ap.add_argument("--out-picks",
                    default="value_picks_pro.csv",
                    help="Main picks CSV to write at repo root.")
    ap.add_argument("--out-final",
                    default="outputs/picks_final.csv",
                    help="Secondary copy with the same rows (for workflows/artifacts).")
    ap.add_argument("--summary",
                    default="outputs/engine_summary.md",
                    help="Markdown summary path.")

    # Legacy/compat placeholders (accepted so existing workflows don’t break)
    ap.add_argument("--elo-atp", default="", help="(optional, accepted for compatibility)")
    ap.add_argument("--elo-wta", default="", help="(optional, accepted for compatibility)")

    # Selection knobs
    ap.add_argument("--min-edge", type=float, default=0.05,
                    help="Minimum (model_p - 1/odds) to accept a pick.")
    ap.add_argument("--max-picks", type=int, default=20,
                    help="Max number of picks to output (highest edge first).")

    # Kelly & risk knobs
    ap.add_argument("--stake-mode", choices=["kelly", "flat"], default="kelly")
    ap.add_argument("--edge", type=float, default=0.08, help="True-edge booster multiplier for p: p'=clamp(p*(1+edge)).")
    ap.add_argument("--kelly-scale", type=float, default=0.5, help="Kelly safety scaler (0.5 = half-Kelly).")
    ap.add_argument("--flat-stake", type=float, default=1.0, help="Units when stake-mode=flat.")
    ap.add_argument("--bankroll", type=float, default=100.0, help="Bankroll used to size stakes.")
    ap.add_argument("--kelly-cap", type=float, default=0.20,
                    help="Cap on scaled Kelly fraction per bet (e.g., 0.20 = max 20% BR).")
    ap.add_argument("--max-risk", type=float, default=0.25,
                    help="Hard cap on stake as a fraction of bankroll (safety valve).")

    args = ap.parse_args()

    # Load candidates
    if not os.path.isfile(args.input):
        print(f"[engine] input not found: {args.input}", file=sys.stderr)
        write_csv(args.out_picks, [])
        write_csv(args.out_final, [])
        os.makedirs(os.path.dirname(args.summary) or ".", exist_ok=True)
        with open(args.summary, "w", encoding="utf-8") as f:
            f.write(f"# Engine Summary\n\nInput missing: `{args.input}`.\n")
        return

    cand = read_rows(args.input)
    if not cand:
        write_csv(args.out_picks, [])
        write_csv(args.out_final, [])
        os.makedirs(os.path.dirname(args.summary) or ".", exist_ok=True)
        with open(args.summary, "w", encoding="utf-8") as f:
            f.write(f"# Engine Summary\n\nNo rows in `{args.input}`.\n")
        return

    # Build Kelly config
    kcfg = KellyConfig(
        stake_mode=args.stake_mode,
        edge=args.edge,
        kelly_scale=args.kelly_scale,
        flat_stake=args.flat_stake,
        bankroll_init=args.bankroll
    )

    # Compute edge + stakes
    rows: List[Dict] = []
    bankroll = kcfg.bankroll_init

    for r in cand:
        row = dict(r)

        # odds
        try:
            price = infer_odds(r) or 0.0
        except Exception:
            price = f2(r.get("price") or r.get("odds") or r.get("decimal_odds") or 0.0)
        if price <= 1.0:
            continue  # invalid odds

        # model prob (preferred) → fallback to market implied
        p_model = infer_prob(r)
        if p_model is None:
            p_model = clamp01(1.0 / price)

        # intrinsic edge vs market
        breakeven = 1.0 / price
        model_edge = p_model - breakeven
        row["price"] = price
        row["p_model"] = round(p_model, 6)
        row["breakeven"] = round(breakeven, 6)
        row["edge_model"] = round(model_edge, 6)

        if model_edge < args.min_edge:
            continue

        # Kelly sizing (with TE booster inside stake_amount)
        stake, p_used, f_raw = stake_amount(kcfg, bankroll, p_model, price)

        # Apply caps on scaled Kelly fraction (stake / bankroll)
        frac = (stake / bankroll) if bankroll > 0 else 0.0
        cap_frac = min(max(args.kelly_cap, 0.0), max(args.max_risk, 0.0))
        if frac > cap_frac:
            stake = bankroll * cap_frac
            frac = cap_frac

        row["stake_units"] = round(stake, 6)
        row["stake_frac_br"] = round(frac, 6)
        row["kelly_f_raw"] = round(f_raw, 6)
        row["p_used"] = round(p_used, 6)

        rows.append(row)

    # Sort by edge desc and clip to max picks
    rows.sort(key=lambda x: x.get("edge_model", 0.0), reverse=True)
    if args.max_picks and args.max_picks > 0:
        rows = rows[: args.max_picks]

    # Write outputs
    write_csv(args.out_picks, rows)
    write_csv(args.out_final, rows)

    # Summary
    os.makedirs(os.path.dirname(args.summary) or ".", exist_ok=True)
    total_stake = sum(f2(r.get("stake_units")) for r in rows)
    avg_price = (sum(f2(r.get("price")) for r in rows) / len(rows)) if rows else 0.0
    avg_edge = (sum(f2(r.get("edge_model")) for r in rows) / len(rows)) if rows else 0.0

    with open(args.summary, "w", encoding="utf-8") as f:
        f.write(
            "# Tennis Value — Daily Picks\n\n"
            f"- Picks: **{len(rows)}**  \n"
            f"- Min edge: **{args.min_edge:.3f}**  \n"
            f"- Kelly: mode=**{args.stake_mode}**, TE=**{args.edge}**, scale=**{args.kelly_scale}**, cap=**{args.kelly_cap}**  \n"
            f"- Bankroll: **{args.bankroll:.2f}**  \n"
            f"- Total stake: **{total_stake:.4f}**  \n"
            f"- Avg odds: **{avg_price:.3f}** | Avg edge: **{avg_edge:.3f}**  \n"
        )

    print(f"[engine] wrote {args.out_picks} and {args.out_final}; summary -> {args.summary}")

if __name__ == "__main__":
    main()
