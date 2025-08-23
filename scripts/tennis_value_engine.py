#!/usr/bin/env python3
# tennis_value_engine.py
# Reads candidate tennis matches and outputs sized picks with Kelly.
# - Supports two-sided odds CSVs like: date,player_a,player_b,odds_a,odds_b
# - Also supports flat rows with 'odds'/'price' and optional model probability.
# - Falls back to p = 1/odds when model probs are missing.
# - Default min-edge = 0 so the above fallback still emits picks.

from __future__ import annotations
import argparse, csv, os, sys
from typing import List, Dict, Optional
from bet_math import KellyConfig, infer_prob, infer_odds, stake_amount

# ---------- utils ----------
def _read_csv(path: str) -> List[Dict]:
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def _write_csv(path: str, rows: List[Dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not rows:
        # Write an empty but valid CSV with a placeholder header
        with open(path, "w", newline='', encoding="utf-8") as f:
            f.write("note\nempty\n")
        return
    keys = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline='', encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=keys)
        wr.writeheader(); wr.writerows(rows)

def _flt(x, default: float = 0.0) -> float:
    try:
        return float(x) if x not in (None, "") else default
    except Exception:
        return default

def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x

def expand_two_sided_rows(rows: List[Dict]) -> List[Dict]:
    """
    Expand rows with columns [date, player_a, player_b, odds_a, odds_b]
    into two flat candidate rows, one per player, with a unified 'price' column.
    """
    out: List[Dict] = []
    for r in rows:
        # Lowercase map of columns -> original case
        lc = {k.lower(): k for k in r.keys()}
        has = all(k in lc for k in ("player_a","player_b","odds_a","odds_b"))
        if not has:
            # Pass through; later logic can still size if it has 'odds'/'price'
            out.append(r)
            continue

        dt = r.get(lc.get("date","date"), r.get("date",""))
        pa = r[lc["player_a"]]
        pb = r[lc["player_b"]]
        oa = _flt(r[lc["odds_a"]], 0.0)
        ob = _flt(r[lc["odds_b"]], 0.0)

        if oa > 1.0:
            out.append({
                "date": dt,
                "player": pa,
                "opponent": pb,
                "side": "A",
                "price": oa,   # normalized odds column used downstream
            })
        if ob > 1.0:
            out.append({
                "date": dt,
                "player": pb,
                "opponent": pa,
                "side": "B",
                "price": ob,
            })
    return out

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Tennis value engine with Kelly sizing.")
    # I/O
    ap.add_argument("--input", "-i",
        default="data/raw/odds/sample_odds.csv",
        help="Input CSV: two-sided odds (date,player_a,player_b,odds_a,odds_b) or flat rows with 'odds'/'price'.")
    ap.add_argument("--out-picks", default="value_picks_pro.csv", help="Primary picks output (repo root).")
    ap.add_argument("--out-final", default="outputs/picks_final.csv", help="Secondary copy for artifacts.")
    ap.add_argument("--summary", default="outputs/engine_summary.md", help="Markdown run summary.")

    # Legacy/compatibility args (ignored but accepted so workflows don’t break)
    ap.add_argument("--elo-atp", dest="elo_atp", default="", help="(ignored) kept for workflow compatibility")
    ap.add_argument("--elo-wta", dest="elo_wta", default="", help="(ignored) kept for workflow compatibility")

    # Selection
    ap.add_argument("--min-edge", type=float, default=0.00,
        help="Minimum (p_model - 1/odds) to accept a pick. With p=1/odds fallback, set to 0.00.")
    ap.add_argument("--max-picks", type=int, default=40, help="Cap number of picks (highest edge first).")

    # Kelly knobs
    ap.add_argument("--stake-mode", choices=["kelly","flat"], default="kelly")
    ap.add_argument("--edge", type=float, default=0.08, help="True-edge booster (TE). p_used = clamp(p_model*(1+edge)).")
    ap.add_argument("--kelly-scale", type=float, default=0.5, help="Safety scaler (0.5=half Kelly).")
    ap.add_argument("--flat-stake", type=float, default=1.0, help="Units per bet when stake-mode=flat.")
    ap.add_argument("--bankroll", type=float, default=1000.0, help="Bankroll used to size stakes.")
    ap.add_argument("--kelly-cap", type=float, default=0.20, help="Cap stake as fraction of bankroll (post-scale).")
    ap.add_argument("--max-risk", type=float, default=0.25, help="Hard cap on stake as fraction of bankroll.")

    args = ap.parse_args()

    # Load input
    if not os.path.isfile(args.input):
        os.makedirs(os.path.dirname(args.summary) or ".", exist_ok=True)
        _write_csv(args.out_picks, [])
        _write_csv(args.out_final, [])
        with open(args.summary, "w", encoding="utf-8") as f:
            f.write(f"# Tennis Value — Daily Picks\n\nInput missing: `{args.input}`.\n")
        print(f"[engine] input missing: {args.input}", file=sys.stderr)
        return

    raw_rows = _read_csv(args.input)
    if not raw_rows:
        _write_csv(args.out_picks, [])
        _write_csv(args.out_final, [])
        with open(args.summary, "w", encoding="utf-8") as f:
            f.write(f"# Tennis Value — Daily Picks\n\nNo rows in `{args.input}`.\n")
        print("[engine] no rows", file=sys.stderr)
        return

    # Normalize: expand two-sided into flat candidates with a 'price' column.
    rows = expand_two_sided_rows(raw_rows)
    if not rows:
        rows = raw_rows[:]  # fallback to original if nothing expanded

    # Kelly config
    kcfg = KellyConfig(
        stake_mode=args.stake_mode,
        edge=args.edge,
        kelly_scale=args.kelly_scale,
        flat_stake=args.flat_stake,
        bankroll_init=args.bankroll
    )

    picks: List[Dict] = []
    bankroll = kcfg.bankroll_init

    for r in rows:
        row = dict(r)

        # Normalize price
        price = 0.0
        if "price" in r and r["price"] not in ("", None):
            price = _flt(r["price"])
        else:
            try:
                price = infer_odds(r) or 0.0
            except Exception:
                price = _flt(r.get("odds") or r.get("decimal_odds"), 0.0)
        if price <= 1.0:
            continue

        # Model probability preferred → fallback to 1/odds (so we emit picks)
        p_model: Optional[float] = None
        try:
            p_model = infer_prob(r)
        except Exception:
            p_model = None
        if p_model is None:
            p_model = _clamp01(1.0 / price)

        breakeven = 1.0 / price
        edge_model = p_model - breakeven
        row["price"] = price
        row["p_model"] = round(p_model, 6)
        row["breakeven"] = round(breakeven, 6)
        row["edge_model"] = round(edge_model, 6)

        if edge_model < args.min_edge:
            continue

        # Kelly sizing
        stake, p_used, f_raw = stake_amount(kcfg, bankroll, p_model, price)
        # Cap stake as fraction of bankroll
        frac = (stake / bankroll) if bankroll > 0 else 0.0
        cap_frac = min(max(args.kelly_cap, 0.0), max(args.max_risk, 0.0))
        if frac > cap_frac:
            stake = bankroll * cap_frac
            frac = cap_frac

        row["stake_units"] = round(stake, 6)
        row["stake_frac_br"] = round(frac, 6)
        row["kelly_f_raw"] = round(f_raw, 6)
        row["p_used"] = round(p_used, 6)

        picks.append(row)

    # Sort by edge desc; cap count
    picks.sort(key=lambda x: x.get("edge_model", 0.0), reverse=True)
    if args.max_picks and args.max_picks > 0:
        picks = picks[: args.max_picks]

    # Write outputs
    _write_csv(args.out_picks, picks)
    _write_csv(args.out_final, picks)

    # Summary
    os.makedirs(os.path.dirname(args.summary) or ".", exist_ok=True)
    total_stake = sum(_flt(r.get("stake_units")) for r in picks)
    avg_odds = (sum(_flt(r.get("price")) for r in picks) / len(picks)) if picks else 0.0
    avg_edge = (sum(_flt(r.get("edge_model")) for r in picks) / len(picks)) if picks else 0.0
    with open(args.summary, "w", encoding="utf-8") as f:
        f.write(
            "# Tennis Value — Daily Picks\n\n"
            f"- Picks: **{len(picks)}**  \n"
            f"- Min edge: **{args.min_edge:.3f}**  \n"
            f"- Kelly: mode=**{args.stake_mode}**, TE=**{args.edge}**, scale=**{args.kelly_scale}**, cap=**{args.kelly_cap}**  \n"
            f"- Bankroll: **{args.bankroll:.2f}**  \n"
            f"- Total stake: **{total_stake:.4f}**  \n"
            f"- Avg odds: **{avg_odds:.3f}** | Avg edge: **{avg_edge:.3f}**  \n"
        )

    print(f"[engine] wrote {args.out_picks} and {args.out_final}; picks={len(picks)}")

if __name__ == "__main__":
    main()
