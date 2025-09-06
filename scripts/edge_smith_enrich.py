- name: Write EdgeSmith enrich script
  shell: bash
  run: |
    mkdir -p scripts
    cat > scripts/edge_smith_enrich.py <<'PY'
#!/usr/bin/env python3
# EdgeSmith — enrich picks with adjusted probabilities, edge & Kelly stake.
import os, csv, math
from pathlib import Path

def fnum(x, pct_allowed=True):
    try:
        s = str(x).strip()
        if pct_allowed and s.endswith('%'): return float(s[:-1])/100.0
        return float(s)
    except: return None

def clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))
def tanh(x):
    try: return math.tanh(float(x))
    except: return 0.0

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

def finals_tag(r):
    r1 = (r.get('round') or r.get('tournament_round') or "").lower()
    if "final" in r1: return True
    t = (r.get('tournament') or r.get('event') or r.get('match') or "").lower()
    return "final" in t

def enrich(picks_path, bankroll, max_stake, kelly_scale, min_edge,
           force_strategy="none", uplift_pct=0.0):
    rows = read_csv(picks_path)
    if not rows: return {"rows":0,"with_edge":0}

    headers = list(rows[0].keys())
    for k in ("match","selection","implied_p","adj_prob","edge","kelly_stake"):
        if k not in headers: headers.append(k)

    with_edge=0
    for r in rows:
        r["match"] = infer_match(r)
        r["selection"] = infer_selection(r)

        odds = fnum(r.get("odds"))
        implied = 1.0/odds if (odds and odds>0) else None
        r["implied_p"] = f"{implied:.6f}" if implied is not None else ""

        base_p = fnum(r.get("model_conf"))
        p = base_p if (base_p is not None and 0.0 < base_p < 1.0) else None

        # infer prob if missing
        if p is None:
            fo = fnum(r.get("fair")) or fnum(r.get("fair_odds")) or fnum(r.get("true_odds")) or fnum(r.get("model_odds"))
            if fo and fo>0: p = 1.0/fo
            if p is None:
                p = fnum(r.get('prob')) or fnum(r.get('win_prob')) or fnum(r.get('p'))

        ed = None
        if implied is not None:
            if p is not None:
                ed = p - implied
            else:
                if force_strategy == "uplift_pct" and float(uplift_pct)>0:
                    p = clamp(implied * (1.0 + float(uplift_pct)/100.0))
                    ed = p - implied
                elif force_strategy == "min_edge":
                    ed = min_edge
                    p  = clamp(implied * (1.0 + ed))

        r["adj_prob"] = f"{p:.6f}" if p is not None else ""
        r["edge"] = f"{ed:.6f}" if ed is not None else ""

        # Kelly
        stake=0.0
        if ed is not None and p is not None and odds and odds>1.0:
            b = odds - 1.0
            k_star = ((p*b) - (1.0 - p)) / b
            k_star = max(0.0, k_star)
            finals = finals_tag(r)
            finals_scale = float(os.environ.get("FINALS_KELLY_SCALE", 0.85)) if finals else 1.0
            scale = float(kelly_scale) * finals_scale
            stake = min(float(max_stake), float(bankroll) * k_star * scale)
        r["kelly_stake"] = f"{stake:.2f}"

        if ed is not None: with_edge += 1

    write_csv(picks_path, rows, headers)
    print(f"Enriched {len(rows)} rows; edges for {with_edge}.")
    return {"rows":len(rows),"with_edge":with_edge}

if __name__ == "__main__":
    picks = os.environ.get("PICKS_FILE","picks_live.csv")
    bankroll = float(os.environ.get("BANKROLL","1000"))
    max_stake = float(os.environ.get("MAX_STAKE_EUR","20"))
    kelly_scale = float(os.environ.get("KELLY_SCALE","0.5"))
    min_edge = float(os.environ.get("MIN_EDGE","0.05"))
    force_strategy = (os.environ.get("FORCE_EDGE_STRATEGY") or "none").lower().strip()
    uplift = float(os.environ.get("FORCE_EDGE_UPLIFT_PCT","0") or 0.0)
    enrich(picks, bankroll, max_stake, kelly_scale, min_edge, force_strategy, uplift)
PY
    chmod +x scripts/edge_smith_enrich.py
