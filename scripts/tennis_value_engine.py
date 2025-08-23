#!/usr/bin/env python3
"""
tennis_value_engine.py
- Kelly staking with True Edge (TE) and scaler
- Uses model probability columns if present
- Else tries Elo (if --elo-atp / --elo-wta provided)
- Else falls back to p = 1/odds so the pipeline still runs
- Auto-overrides min-edge to 0.00 when no prob cols exist (so you still get picks)
- Optional --aggressive preset for more volume (TE=0.12, scale=1.0, cap=0.25)
"""

from __future__ import annotations
import argparse, csv, math, os, sys
from typing import Dict, List, Optional, Tuple
from bet_math import KellyConfig, infer_prob, infer_odds, stake_amount

# ----------------- utilities -----------------
def _read_csv(path: str) -> List[Dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def _write_csv(path: str, rows: List[Dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("note\nempty\n")
        return
    keys = sorted({k for r in rows for k in r.keys()})
    # (fixed quotes) ⬇
    with open(path, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=keys)
        wr.writeheader()
        wr.writerows(rows)

def _flt(x, default: float = 0.0) -> float:
    try:
        return float(x) if x not in (None, "") else default
    except Exception:
        return default

def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else (1.0 if x > 1 else x)

def _norm_name(s: str) -> str:
    return (s or "").strip().lower()

# ----------------- Elo support -----------------
class EloBook:
    def __init__(self) -> None:
        self.map: Dict[str, float] = {}

    @staticmethod
    def _pick(row: Dict) -> Optional[Tuple[str, float]]:
        lc = {k.lower(): k for k in row.keys()}
        pkey = lc.get("player") or lc.get("name")
        ekey = lc.get("elo") or lc.get("rating")
        if not pkey or not ekey:
            return None
        name = _norm_name(row[pkey])
        try:
            elo = float(row[ekey])
        except Exception:
            return None
        return (name, elo)

    def load(self, path: str) -> None:
        if not path or not os.path.isfile(path):
            return
        try:
            with open(path, newline="", encoding="utf-8") as f:
                rdr = csv.DictReader(f)
                for r in rdr:
                    picked = self._pick(r)
                    if picked:
                        nm, elo = picked
                        self.map[nm] = elo  # last seen wins
        except Exception:
            # fail-soft; engine must not crash on Elo read
            pass

    def get(self, name: str) -> Optional[float]:
        return self.map.get(_norm_name(name))

def elo_win_prob(elo_a: float, elo_b: float) -> float:
    # 1 / (1 + 10^(-Δ/400))
    return 1.0 / (1.0 + math.pow(10.0, -(elo_a - elo_b) / 400.0))

# ----------------- input normalization -----------------
def expand_two_sided_rows(rows: List[Dict]) -> List[Dict]:
    """
    Expand [date,player_a,player_b,odds_a,odds_b] into two flat rows with 'price'.
    """
    out: List[Dict] = []
    for r in rows:
        lc = {k.lower(): k for k in r.keys()}
        has = all(k in lc for k in ("player_a", "player_b", "odds_a", "odds_b"))
        if not has:
            out.append(r)
            continue

        dt = r.get(lc.get("date", "date"), r.get("date", ""))
        pa, pb = r[lc["player_a"]], r[lc["player_b"]]
        oa, ob = _flt(r[lc["odds_a"]], 0.0), _flt(r[lc["odds_b"]], 0.0)

        if oa > 1.0:
            out.append({"date": dt, "player": pa, "opponent": pb, "side": "A", "price": oa})
        if ob > 1.0:
            out.append({"date": dt, "player": pb, "opponent": pa, "side": "B", "price": ob})
    return out

# ----------------- main -----------------
def main():
    ap = argparse.ArgumentParser(description="Tennis Value Engine (Kelly + TE + optional Elo).")
    # I/O
    ap.add_argument("--input", "-i", default="data/raw/odds/sample_odds.csv",
                    help="Two-sided odds CSV or flat rows with 'odds'/'price'.")
    ap.add_argument("--out-picks", default="value_picks_pro.csv", help="Primary CSV (repo root).")
    ap.add_argument("--out-final", default="outputs/picks_final.csv", help="Copy for artifacts.")
    ap.add_argument("--summary", default="outputs/engine_summary.md", help="Markdown run summary.")

    # Elo (optional)
    ap.add_argument("--elo-atp", default="", help="ATP Elo CSV with columns: player, elo")
    ap.add_argument("--elo-wta", default="", help="WTA Elo CSV with columns: player, elo")

    # Selection
    ap.add_argument("--min-edge", type=float, default=0.00,
                    help="Minimum (p_model - 1/odds). Auto 0.00 if no prob columns exist.")
    ap.add_argument("--max-picks", type=int, default=60, help="Cap number of picks (sorted by edge).")

    # Aggression preset
    ap.add_argument("--aggressive", action="store_true",
                    help="Bumps defaults to TE=0.12, kelly-scale=1.0, cap=0.25, max-picks=80.")

    # Kelly / staking
    ap.add_argument("--stake-mode", choices=["kelly", "flat"], default="kelly")
    ap.add_argument("--edge", type=float, default=0.08, help="True-edge booster (TE).")
    ap.add_argument("--kelly-scale", type=float, default=0.5, help="Kelly safety scaler.")
    ap.add_argument("--flat-stake", type=float, default=1.0, help="Units when stake-mode=flat.")
    ap.add_argument("--bankroll", type=float, default=1000.0, help="Bankroll to size stakes.")
    ap.add_argument("--kelly-cap", type=float, default=0.20, help="Cap stake as fraction of bankroll.")
    ap.add_argument("--max-risk", type=float, default=0.25, help="Hard max stake fraction of bankroll.")

    args = ap.parse_args()

    # Aggressive defaults (only if user didn't override)
    if args.aggressive:
        if args.edge == ap.get_default("edge"): args.edge = 0.12
        if args.kelly_scale == ap.get_default("kelly_scale"): args.kelly_scale = 1.0
        if args.kelly_cap == ap.get_default("kelly_cap"): args.kelly_cap = 0.25
        if args.max_picks == ap.get_default("max_picks"): args.max_picks = 80

    # Load input
    if not os.path.isfile(args.input):
        os.makedirs(os.path.dirname(args.summary) or ".", exist_ok=True)
        _write_csv(args.out_picks, [])
        _write_csv(args.out_final, [])
        with open(args.summary, "w", encoding="utf-8") as f:
            f.write(f"# Tennis Value — Daily Picks\n\nInput missing: `{args.input}`.\n")
        print(f"[engine] input missing: {args.input}", file=sys.stderr)
        return

    raw = _read_csv(args.input)
    if not raw:
        _write_csv(args.out_picks, [])
        _write_csv(args.out_final, [])
        with open(args.summary, "w", encoding="utf-8") as f:
            f.write(f"# Tennis Value — Daily Picks\n\nNo rows in `{args.input}`.\n")
        print("[engine] no rows", file=sys.stderr)
        return

    # Check header for probability columns
    hdr = {c.lower() for c in raw[0].keys()}
    prob_keys = {"p", "prob", "p_model", "model_prob", "probability", "pred_prob", "win_prob", "p_hat"}
    header_has_probs = any(k in hdr for k in prob_keys)
    effective_min_edge = 0.00 if not header_has_probs else args.min_edge
    auto_note = "(auto 0.00: no prob fields)" if not header_has_probs else ""

    # Expand to flat candidates if needed
    rows = expand_two_sided_rows(raw) or raw

    # Load Elo
    elos = EloBook()
    elos.load(args.elo_atp)
    elos.load(args.elo_wta)
    have_elo = len(elos.map) > 0

    # Kelly config
    kcfg = KellyConfig(
        stake_mode=args.stake_mode,
        edge=args.edge,
        kelly_scale=args.kelly_scale,
        flat_stake=args.flat_stake,
        bankroll_init=args.bankroll,
    )

    picks: List[Dict] = []
    bankroll = kcfg.bankroll_init

    for r in rows:
        row = dict(r)

        # Normalize odds/price
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
        row["price"] = price
        row["odds"] = price  # keep legacy column too

        # Names for Elo
        player = row.get("player") or row.get("selection") or ""
        opp    = row.get("opponent") or row.get("against") or ""

        # 1) explicit model prob
        p_model: Optional[float] = None
        try:
            p_model = infer_prob(r)
        except Exception:
            p_model = None

        # 2) Elo if available and both players found
        if p_model is None and have_elo and player and opp:
            e_p, e_o = elos.get(player), elos.get(opp)
            if e_p is not None and e_o is not None:
                p_model = _clamp01(elo_win_prob(e_p, e_o))

        # 3) fallback to market implied
        if p_model is None:
            p_model = _clamp01(1.0 / price)

        breakeven = 1.0 / price
        edge_model = p_model - breakeven
        if edge_model < effective_min_edge:
            continue

        # Kelly stake
        stake, p_used, f_raw = stake_amount(kcfg, bankroll, p_model, price)

        # Cap by bankroll fraction
        frac = stake / bankroll if bankroll > 0 else 0.0
        cap_frac = min(max(args.kelly_cap, 0.0), max(args.max_risk, 0.0))
        if frac > cap_frac:
            stake = bankroll * cap_frac
            frac = cap_frac

        row["p_model"] = round(p_model, 6)
        row["breakeven"] = round(breakeven, 6)
        row["edge_model"] = round(edge_model, 6)
        row["p_used"] = round(p_used, 6)
        row["kelly_f_raw"] = round(f_raw, 6)
        row["stake_units"] = round(stake, 6)
        row["stake_frac_br"] = round(frac, 6)

        picks.append(row)

    # Sort & cap
    picks.sort(key=lambda x: x.get("edge_model", 0.0), reverse=True)
    if args.max_picks and args.max_picks > 0:
        picks = picks[: args.max_picks]

    # Outputs
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
            f"- Min edge: **{effective_min_edge:.3f}** {auto_note}  \n"
            f"- Kelly: mode=**{args.stake_mode}**, TE=**{args.edge}**, scale=**{args.kelly_scale}**, cap=**{args.kelly_cap}**  \n"
            f"- Bankroll: **{args.bankroll:.2f}**  \n"
            f"- Total stake: **{total_stake:.4f}**  \n"
            f"- Avg odds: **{avg_odds:.3f}** | Avg edge: **{avg_edge:.3f}**  \n"
            f"- Elo loaded: {'yes' if have_elo else 'no'}  \n"
        )

    print(f"[engine] picks={len(picks)}; min_edge={effective_min_edge}; TE={args.edge}; "
          f"scale={args.kelly_scale}; elo={'yes' if have_elo else 'no'}")

if __name__ == "__main__":
    main()
