#!/usr/bin/env python3
# tennis_value_engine.py
# Value engine with Kelly sizing, TE, and optional Elo-derived probabilities.
# - Accepts two-sided odds (date,player_a,player_b,odds_a,odds_b) or flat rows with 'price'/'odds'.
# - p_model sources (in priority order):
#     1) explicit prob fields (p, prob, p_model, model_prob, probability)
#     2) Elo (if --elo-atp/--elo-wta files provided and players found)
#     3) fallback p = 1/odds  (market-implied)
# - If no explicit prob fields exist in the header, min-edge is auto-overridden to 0.00,
#   so you still get sized picks via TE even with p=1/odds.
#
# Elo file format (flexible):
#   CSV with columns including: player, [elo|rating], optional date
#   We'll take the last seen rating per player. Names matched case-insensitively, stripped.

from __future__ import annotations
import argparse, csv, os, sys, math
from typing import List, Dict, Optional, Tuple
from bet_math import KellyConfig, infer_prob, infer_odds, stake_amount

# ---------- tiny utils ----------
def _read_csv(path: str) -> List[Dict]:
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def _write_csv(path: str, rows: List[Dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not rows:
        with open(path, "w", newline='', encoding='utf-8') as f:
            f.write("note\nempty\n")
        return
    keys = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline='', encoding='utf-8") as f:  # noqa
        wr = csv.DictWriter(f, fieldnames=keys)
        wr.writeheader(); wr.writerows(rows)

def _flt(x, default: float = 0.0) -> float:
    try:
        return float(x) if x not in (None, "") else default
    except Exception:
        return default

def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x

def _norm_name(s: str) -> str:
    return (s or "").strip().lower()

# ---------- Elo loader ----------
class EloBook:
    def __init__(self):
        self.r: Dict[str, float] = {}

    @staticmethod
    def _pick_elo_row(row: Dict) -> Optional[Tuple[str, float]]:
        # Flexible columns: player / name ; elo / rating
        keys = {k.lower(): k for k in row.keys()}
        pkey = keys.get("player") or keys.get("name")
        if not pkey: return None
        ekey = keys.get("elo") or keys.get("rating")
        if not ekey: return None
        nm = _norm_name(row[pkey])
        try:
            elo = float(row[ekey])
        except Exception:
            return None
        return (nm, elo)

    def load(self, path: str) -> None:
        if not path or not os.path.isfile(path): return
        try:
            with open(path, newline='', encoding="utf-8") as f:
                rdr = csv.DictReader(f)
                for row in rdr:
                    picked = self._pick_elo_row(row)
                    if picked:
                        nm, elo = picked
                        self.r[nm] = elo  # last seen wins
        except Exception:
            # don't kill the run — fail soft
            pass

    def get(self, name: str) -> Optional[float]:
        return self.r.get(_norm_name(name))

def elo_logistic_p(elo_fav: float, elo_dog: float) -> float:
    # Standard Elo win prob: 1 / (1 + 10^(-Δ/400))
    diff = (elo_fav - elo_dog)
    return 1.0 / (1.0 + math.pow(10.0, -diff / 400.0))

# ---------- input normalization ----------
def expand_two_sided_rows(rows: List[Dict]) -> List[Dict]:
    """
    Expand rows with columns [date, player_a, player_b, odds_a, odds_b]
    into two flat candidate rows, one per player, with a unified 'price' column.
    """
    out: List[Dict] = []
    for r in rows:
        lc = {k.lower(): k for k in r.keys()}
        has = all(k in lc for k in ("player_a","player_b","odds_a","odds_b"))
        if not has:
            out.append(r)
            continue

        dt = r.get(lc.get("date","date"), r.get("date",""))
        pa = r[lc["player_a"]]
        pb = r[lc["player_b"]]
        oa = _flt(r[lc["odds_a"]], 0.0)
        ob = _flt(r[lc["odds_b"]], 0.0)

        if oa > 1.0:
            out.append({"date": dt, "player": pa, "opponent": pb, "side": "A", "price": oa})
        if ob > 1.0:
            out.append({"date": dt, "player": pb, "opponent": pa, "side": "B", "price": ob})
    return out

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Tennis value engine with Kelly sizing + Elo")
    # I/O
    ap.add_argument("--input", "-i", default="data/raw/odds/sample_odds.csv",
                    help="Two-sided odds (date,player_a,player_b,odds_a,odds_b) or flat rows with 'odds'/'price'.")
    ap.add_argument("--out-picks", default="value_picks_pro.csv", help="Primary picks output (repo root).")
    ap.add_argument("--out-final", default="outputs/picks_final.csv", help="Secondary copy for artifacts.")
    ap.add_argument("--summary", default="outputs/engine_summary.md", help="Markdown run summary.")

    # Elo inputs (now used if provided)
    ap.add_argument("--elo-atp", dest="elo_atp", default="", help="ATP Elo CSV (player, elo[, date])")
    ap.add_argument("--elo-wta", dest="elo_wta", default="", help="WTA Elo CSV (player, elo[, date])")

    # Selection
    ap.add_argument("--min-edge", type=float, default=0.00,
                    help="Minimum (p_model - 1/odds) to accept a pick. Auto-set to 0 if no prob fields in header.")
    ap.add_argument("--max-picks", type=int, default=60, help="Cap number of picks (highest edge first).")

    # Aggression toggle (optional)
    ap.add_argument("--aggressive", action="store_true",
                    help="If set, defaults shift to TE=0.12, Kelly scale=1.0, cap=0.25 unless overridden explicitly.")

    # Kelly knobs
    ap.add_argument("--stake-mode", choices=["kelly","flat"], default="kelly")
    ap.add_argument("--edge", type=float, default=0.08, help="True-edge booster (TE). p_used = clamp(p*(1+edge)).")
    ap.add_argument("--kelly-scale", type=float, default=0.5, help="Safety scaler (0.5 = half Kelly).")
    ap.add_argument("--flat-stake", type=float, default=1.0, help="Units per bet when stake-mode=flat.")
    ap.add_argument("--bankroll", type=float, default=1000.0, help="Bankroll used to size stakes.")
    ap.add_argument("--kelly-cap", type=float, default=0.20, help="Cap stake as fraction of bankroll (post-scale).")
    ap.add_argument("--max-risk", type=float, default=0.25, help="Hard cap on stake as fraction of bankroll.")

    args = ap.parse_args()

    # Aggressive defaults if requested (but don't override if user explicitly set different)
    if args.aggressive:
        if ap.get_default("edge") == args.edge:          args.edge = 0.12
        if ap.get_default("kelly_scale") == args.kelly_scale: args.kelly_scale = 1.0
        if ap.get_default("kelly_cap") == args.kelly_cap: args.kelly_cap = 0.25
        if ap.get_default("max_picks") == args.max_picks: args.max_picks = 80

    # Load input
    if not os.path.isfile(args.input):
        os.makedirs(os.path.dirname(args.summary) or ".", exist_ok=True)
        _write_csv(args.out_picks, []); _write_csv(args.out_final, [])
        with open(args.summary, "w", encoding="utf-8") as f:
            f.write(f"# Tennis Value — Daily Picks\n\nInput missing: `{args.input}`.\n")
        print(f"[engine] input missing: {args.input}", file=sys.stderr)
        return
    raw_rows = _read_csv(args.input)
    if not raw_rows:
        _write_csv(args.out_picks, []); _write_csv(args.out_final, [])
        with open(args.summary, "w", encoding="utf-8") as f:
            f.write(f"# Tennis Value — Daily Picks\n\nNo rows in `{args.input}`.\n")
        print("[engine] no rows", file=sys.stderr); return

    # Header check for explicit prob fields
    fields_lower = {c.lower() for c in (raw_rows[0].keys() if raw_rows else [])}
    prob_keys = {"p","prob","model_prob","p_model","probability","pred_prob","win_prob","p_hat"}
    header_has_probs = any(k in fields_lower for k in prob_keys)

    # Expand two-sided rows
    rows = expand_two_sided_rows(raw_rows) or raw_rows[:]

    # Load Elo (optional)
    elo = EloBook()
    elo.load(args.elo_atp)
    elo.load(args.elo_wta)
    have_elo = len(elo.r) > 0

    # Effective min-edge
    effective_min_edge = 0.00 if not header_has_probs else args.min_edge
    auto_note = "(auto 0.00: no prob fields)" if not header_has_probs else ""

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

        # Resolve player/opponent names if present
        player = row.get("player") or row.get("selection") or row.get("runner") or ""
        opp    = row.get("opponent") or row.get("oppo") or row.get("against") or ""

        # Preferred p_model from data
        p_model: Optional[float] = None
        try:
            p_model = infer_prob(r)  # checks many common keys
        except Exception:
            p_model = None

        # If absent, try Elo (both players must be found)
        if p_model is None and have_elo and player and opp:
            e_p = elo.get(player)
            e_o = elo.get(opp)
            if e_p is not None and e_o is not None:
                p_model = _clamp01(elo_logistic_p(e_p, e_o))

        # Final fallback: market implied
        if p_model is None:
            p_model = _clamp01(1.0 / price)

        breakeven = 1.0 / price
        edge_model = p_model - breakeven

        row["price"] = price
        row["p_model"] = round(p_model, 6)
        row["breakeven"] = round(breakeven, 6)
        row["edge_model"] = round(edge_model, 6)

        if edge_model < effective_min_edge:
            continue

        # Kelly sizing with TE
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
            f"- Min edge: **{effective_min_edge:.3f}** {auto_note}  \n"
            f"- Kelly: mode=**{args.stake_mode}**, TE=**{args.edge}**, scale=**{args.kelly_scale}**, cap=**{args.kelly_cap}**  \n"
            f"- Bankroll: **{args.bankroll:.2f}**  \n"
            f"- Total stake: **{total_stake:.4f}**  \n"
            f"- Avg odds: **{avg_odds:.3f}** | Avg edge: **{avg_edge:.3f}**  \n"
            f"- Elo loaded: {'yes' if have_elo else 'no'}  \n"
        )

    print(f"[engine] picks={len(picks)}; effective_min_edge={effective_min_edge}; elo={'yes' if have_elo else 'no'}")

if __name__ == "__main__":
    main()
