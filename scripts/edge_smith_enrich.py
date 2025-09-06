#!/usr/bin/env python3
import os, csv, math
from pathlib import Path

def fnum(x, pct_allowed=True):
    try:
        s = str(x).strip()
        if pct_allowed and s.endswith('%'): return float(s[:-1])/100.0
        return float(s)
    except: return None

def clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))

def read_csv(path):
    p = Path(path)
    if not p.is_file() or p.stat().st_size == 0: return []
    with open(path, newline='') as f:
        return list(csv.DictReader(f))

def write_csv(path, rows, headers):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader(); w.writerows(rows)

def choose(*vals):
    for v in vals:
        if v is not None and str(v).strip() != "": return str(v).strip()
    return ""

def infer_match(r):
    m = choose(r.get('match'), r.get('event'), r.get('event_name'),
               r.get('fixture'), r.get('game'))
    if m: return m
    home = choose(r.get('home'), r.get('home_team'), r.get('player_a'),
                  r.get('player1'), r.get('p1'), r.get('player'))
    away = choose(r.get('away'), r.get('away_team'), r.get('player_b'),
                  r.get('player2'), r.get('p2'), r.get('opponent'),
                  r.get('opp'), r.get('vs'), r.get('against'))
    if home or away: return f"{home} vs {away}".strip()
    return "—"

def infer_selection(r):
    return choose(r.get('selection'), r.get('selection_name'), r.get('runner'),
                  r.get('runner_name'), r.get('player'), r.get('team'),
                  r.get('name'), r.get('side'), r.get('bet_selection')) or "—"

def enrich(picks_path, bankroll, max_stake, kelly_scale, min_edge,
           force_strategy="none", uplift_pct=0.0):
    rows = read_csv(picks_path)
    if not rows: return
    headers = list(rows[0].keys())
    for k in ("match","selection","implied_p","adj_prob","edge","kelly_stake"):
        if k not in headers: headers.append(k)

    for r in rows:
        r["match"] = infer_match(r)
        r["selection"] = infer_selection(r)

        odds = fnum(r.get("odds"))
        implied = 1.0/odds if (odds and odds>0) else None
        r["implied_p"] = f"{implied:.6f}" if implied is not None else ""

        base_p = fnum(r.get("model_conf"))
        p = base_p if (base_p is not None and 0.0 < base_p < 1.0) else None

        if p is None and implied is not None:
            p = clamp(implied * (1.0 + float(os.environ.get("FORCE_EDGE_UPLIFT_PCT","0") or 0)/100.0))

        ed = (p - implied) if (p is not None and implied is not None) else None
        r["adj_prob"] = f"{p:.6f}" if p is not None else ""
        r["edge"] = f"{ed:.6f}" if ed is not None else ""

        stake=0.0
        if ed is not None and p is not None and odds and odds>1.0:
            b = odds - 1.0
            k_star = ((p*b) - (1.0 - p)) / b
            k_star = max(0.0, k_star)
            finals_scale = float(os.environ.get("FINALS_KELLY_SCALE", 0.85)) if "final" in r.get("match","").lower() else 1.0
            scale = float(kelly_scale) * finals_scale
            stake = min(float(max_stake), float(bankroll) * k_star * scale)
        r["kelly_stake"] = f"{stake:.2f}"

    write_csv(picks_path, rows, headers)

if __name__ == "__main__":
    picks = os.environ.get("PICKS_FILE","picks_live.csv")
    bankroll = float(os.environ.get("BANKROLL","1000"))
    max_stake = float(os.environ.get("MAX_STAKE_EUR","20"))
    kelly_scale = float(os.environ.get("KELLY_SCALE","0.5"))
    min_edge = float(os.environ.get("MIN_EDGE","0.05"))
    enrich(picks, bankroll, max_stake, kelly_scale, min_edge,
           os.environ.get("FORCE_EDGE_STRATEGY","none"),
           float(os.environ.get("FORCE_EDGE_UPLIFT_PCT","0") or 0.0))
