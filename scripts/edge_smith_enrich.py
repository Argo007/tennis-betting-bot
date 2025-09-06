# scripts/edge_smith_enrich.py
# EdgeSmith — enrich picks with adjusted probabilities, edge & Kelly stake.
# Works even if only {player, opponent, odds, model_conf} exist.
# Optional columns it will use if present (0–1 unless stated otherwise):
# - surface: "hard|clay|grass"  (string, optional)
# - player_surface_wr, opponent_surface_wr  (winrates 0–1)
# - form_elo_diff  (player minus opp, elo points; or player_form - opp_form as 0–1)
# - h2h_player_wins, h2h_opp_wins (ints)
# - days_rest_player, days_rest_opp (ints)
# - round / tournament / match text (for Finals handling)

from __future__ import annotations
import csv, os, math, json
from pathlib import Path
from typing import Dict, Any

def fnum(x, pct_allowed=True):
    try:
        s = str(x).strip()
        if pct_allowed and s.endswith('%'): return float(s[:-1])/100.0
        return float(s)
    except Exception:
        return None

def clamp(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, v))

def read_csv(path: str) -> list[Dict[str,str]]:
    p = Path(path)
    if not p.is_file() or p.stat().st_size == 0: return []
    with open(path, newline='') as f:
        return list(csv.DictReader(f))

def write_csv(path: str, rows: list[Dict[str,Any]], header: list[str]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows: w.writerow(r)

def choose(*vals):
    for v in vals:
        if v is not None and str(v).strip() != "": return str(v).strip()
    return ""

def infer_match(row: Dict[str,str]) -> str:
    # Try direct match/event first
    m = choose(row.get('match'), row.get('event'), row.get('event_name'),
               row.get('fixture'), row.get('game'))
    if m: return m
    # Build from sides
    home = choose(row.get('home'), row.get('home_team'),
                  row.get('player_a'), row.get('player1'), row.get('p1'),
                  row.get('player'))
    away = choose(row.get('away'), row.get('away_team'),
                  row.get('player_b'), row.get('player2'), row.get('p2'),
                  row.get('opponent'), row.get('opp'), row.get('vs'), row.get('against'))
    if home or away: return f"{home} vs {away}".strip()
    return "—"

def infer_selection(row: Dict[str,str]) -> str:
    return choose(row.get('selection'), row.get('selection_name'),
                  row.get('runner'), row.get('runner_name'),
                  row.get('player'), row.get('team'), row.get('name'),
                  row.get('side'), row.get('bet_selection')) or "—"

def finals_tag(row: Dict[str,str]) -> bool:
    r = (row.get('round') or row.get('tournament_round') or "").lower()
    if "final" in r: return True
    t = (row.get('tournament') or row.get('event') or row.get('match') or "").lower()
    return "final" in t

def tanh(x):  # safe tanh
    try:
        return math.tanh(float(x))
    except Exception:
        return 0.0

def enrich(
    picks_path: str,
    bankroll: float,
    max_stake: float,
    kelly_scale: float,
    min_edge: float,
    force_strategy: str = "none",
    uplift_pct: float = 0.0,
    weights: Dict[str,float] | None = None,
) -> Dict[str,Any]:
    rows = read_csv(picks_path)
    if not rows: return {"rows": 0, "with_edge": 0}

    # Default weights; override via env
    W = {
        "surface": 0.06,    # up to ±6% absolute probability swing if surface WRs provided
        "form":    0.08,    # recency/form (elo diff scaled)
        "h2h":     0.03,    # head-to-head soft bias
        "rest":    0.02,    # days rest differential
        "finals_scale": 0.85  # shrink Kelly in finals (markets tighter)
    }
    if weights:
        W.update({k: float(v) for k,v in weights.items() if v is not None})

    header = list(rows[0].keys())
    for k in ("match","selection","implied_p","adj_prob","edge","kelly_stake"):
        if k not in header: header.append(k)

    with_edge = 0
    for r in rows:
        # Names
        r["match"] = infer_match(r)
        r["selection"] = infer_selection(r)

        # Core numbers
        odds = fnum(r.get("odds"))
        base_p = fnum(r.get("model_conf"))
        implied = 1.0/odds if (odds and odds>0) else None
        r["implied_p"] = f"{implied:.6f}" if implied is not None else ""

        # Start with base model prob if valid
        p = base_p if (base_p is not None and 0.0 < base_p < 1.0) else None

        # If no base prob, try to rebuild from any columns we know
        if p is None and implied is not None:
            # fair odds or prob columns
            fo = fnum(r.get("fair")) or fnum(r.get("fair_odds")) or fnum(r.get("true_odds")) or fnum(r.get("model_odds"))
            if fo and fo>0: p = 1.0/fo
            if p is None:
                p = fnum(r.get('prob')) or fnum(r.get('win_prob')) or fnum(r.get('p'))

        # Optionally force an edge if still nothing
        ed = None
        if implied is not None:
            if p is not None:
                ed = p - implied
            else:
                if force_strategy == "uplift_pct" and uplift_pct>0:
                    p = clamp(implied * (1.0 + uplift_pct/100.0))
                    ed = p - implied
                elif force_strategy == "min_edge":
                    ed = min_edge
                    p = clamp(implied * (1.0 + ed))

        # Boosts (only if we have a base prob)
        if p is not None:
            adj = 0.0

            # SURFACE: uses player_surface_wr vs opponent_surface_wr if present (0–1)
            psw = fnum(r.get("player_surface_wr"))
            osw = fnum(r.get("opponent_surface_wr"))
            if psw is not None and osw is not None:
                adj += W["surface"] * (psw - osw)

            # FORM: if form_elo_diff is present (elo pts) or a 0–1 diff
            fed = fnum(r.get("form_elo_diff"), pct_allowed=False)
            if fed is not None:
                # If looks like elo points, scale ~400 -> 0.1 swing
                if abs(fed) > 1.0:
                    adj += W["form"] * (fed / 400.0)
                else:
                    adj += W["form"] * fed  # already normalized

            # H2H: player vs opponent wins
            pw = fnum(r.get("h2h_player_wins"), pct_allowed=False)
            ow = fnum(r.get("h2h_opp_wins"), pct_allowed=False)
            if pw is not None and ow is not None:
                diff = (pw - ow)
                adj += W["h2h"] * tanh(diff / 5.0)

            # REST: days rest diff (more rest is small plus)
            pr = fnum(r.get("days_rest_player"), pct_allowed=False)
            or_ = fnum(r.get("days_rest_opp"), pct_allowed=False)
            if pr is not None and or_ is not None:
                adj += W["rest"] * tanh((pr - or_) / 4.0)

            p_adj = clamp(p + adj, 0.02, 0.98)  # sane caps
        else:
            p_adj = None

        # Final edge after boosts
        if p_adj is not None and implied is not None:
            ed = p_adj - implied
        # Persist values
        r["adj_prob"] = f"{p_adj:.6f}" if p_adj is not None else ""
        r["edge"] = f"{ed:.6f}" if ed is not None else ""

        # Kelly stake
        stake = 0.0
        if ed is not None and p_adj is not None and odds and odds>1.0:
            b = odds - 1.0
            k_star = ((p_adj*b) - (1.0 - p_adj)) / b
            k_star = max(0.0, k_star)
            # Finals shrinkage (tighter markets)
            finals = finals_tag(r)
            scale = kelly_scale * (float(os.environ.get("FINALS_KELLY_SCALE", 1.0)) if finals else 1.0)
            if finals and os.environ.get("FINALS_KELLY_SCALE") is None:
                scale = kelly_scale * W["finals_scale"]
            stake = min(max_stake, bankroll * k_star * scale)

        r["kelly_stake"] = f"{stake:.2f}"

        if ed is not None: with_edge += 1

    # Re-write picks with new columns
    write_csv(picks_path, rows, header)
    return {
        "rows": len(rows),
        "with_edge": with_edge,
    }

def main():
    # Read envs from workflow
    picks = os.environ.get("PICKS_FILE","picks_live.csv")
    bankroll = float(os.environ.get("BANKROLL","1000"))
    max_stake = float(os.environ.get("MAX_STAKE_EUR","20"))
    kelly_scale = float(os.environ.get("KELLY_SCALE","0.5"))
    min_edge = float(os.environ.get("MIN_EDGE","0.05"))
    force_strategy = (os.environ.get("FORCE_EDGE_STRATEGY") or "none").lower().strip()
    uplift = float(os.environ.get("FORCE_EDGE_UPLIFT_PCT","0") or 0.0)

    weights = {}
    for k in ("WEIGHT_SURFACE","WEIGHT_FORM","WEIGHT_H2H","WEIGHT_REST","FINALS_KELLY_SCALE"):
        v = os.environ.get(k)
        if v is not None and v != "":
            weights[k.lower().replace("weight_","").replace("finals_","finals_")] = float(v)

    out = enrich(
        picks, bankroll, max_stake, kelly_scale, min_edge,
        force_strategy=force_strategy, uplift_pct=uplift, weights=weights
    )
    print(f"Enriched {out['rows']} rows; edges computed for {out['with_edge']}.")

if __name__ == "__main__":
    main()
