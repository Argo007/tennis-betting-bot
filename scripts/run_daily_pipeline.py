#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_daily_pipeline.py  (guardrails edition)

Adds:
  --odds-min / --odds-max: keep bets within an odds window
  --day-risk: cap total stake as fraction of bankroll (e.g., 0.25)
  --bet-cap: cap each bet as fraction of bankroll (e.g., 0.05)

Pipeline:
  normalize(two-sided -> one-sided) -> prob_enrich (gamma)
  -> engine (TE + Kelly) -> postprocess caps/filters
  -> optional matrix backtest -> summaries
"""

from __future__ import annotations
import argparse, csv, os, sys, math, pathlib, subprocess
from typing import List, Dict, Optional

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
OUT = ROOT / "outputs"

# ----------------- utils -----------------
def ensure_dirs():
    OUT.mkdir(parents=True, exist_ok=True)

def read_csv(path: pathlib.Path) -> List[Dict]:
    if not path.is_file():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(path: pathlib.Path, rows: List[Dict]):
    os.makedirs(path.parent, exist_ok=True)
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            pass
        return
    keys, seen = [], set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); keys.append(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)

def pick_col(header, candidates) -> Optional[str]:
    hset = {c.lower(): c for c in header}
    for c in candidates:
        if c.lower() in hset:
            return hset[c.lower()]
    return None

# ---------- step 0: normalize raw odds ----------
def normalize_odds(input_csv: pathlib.Path) -> List[Dict]:
    raw = read_csv(input_csv)
    if not raw:
        raise SystemExit(f"No rows in {input_csv}")
    hdr = list(raw[0].keys())

    col_pa = pick_col(hdr, ["player_a","home_player","fighter_a","team_a"])
    col_pb = pick_col(hdr, ["player_b","away_player","fighter_b","team_b","opponent"])
    col_oa = pick_col(hdr, ["odds_a","price_a","decimal_odds_a"])
    col_ob = pick_col(hdr, ["odds_b","price_b","decimal_odds_b"])

    col_p1 = pick_col(hdr, ["player","selection"])
    col_op = pick_col(hdr, ["opponent"])
    col_od = pick_col(hdr, ["price","odds","decimal_odds"])

    norm: List[Dict] = []

    if col_pa and col_pb and col_oa and col_ob:
        for r in raw:
            pa, pb = r.get(col_pa,""), r.get(col_pb,"")
            try:
                oa = float(r.get(col_oa,"nan")); ob = float(r.get(col_ob,"nan"))
            except Exception:
                continue
            if oa and oa > 1.0 and math.isfinite(oa):
                norm.append({
                    "player": pa, "opponent": pb, "price": oa,
                    "tour": r.get("tour",""), "market": r.get("market","H2H"),
                    "date": r.get("date",""),
                })
            if ob and ob > 1.0 and math.isfinite(ob):
                norm.append({
                    "player": pb, "opponent": pa, "price": ob,
                    "tour": r.get("tour",""), "market": r.get("market","H2H"),
                    "date": r.get("date",""),
                })
    elif col_p1 and col_op and col_od:
        for r in raw:
            try:
                pr = float(r.get(col_od,"nan"))
            except Exception:
                continue
            if pr and pr > 1.0 and math.isfinite(pr):
                norm.append({
                    "player": r.get(col_p1,""), "opponent": r.get(col_op,""),
                    "price": pr, "tour": r.get("tour",""), "market": r.get("market","H2H"),
                    "date": r.get("date",""),
                })
    else:
        raise SystemExit(
            "Input must have either:"
            " two-sided (player_a/player_b + odds_a/odds_b) or"
            " one-sided (player/opponent + price/odds/decimal_odds)"
        )
    if not norm:
        raise SystemExit("No valid odds rows after normalization.")
    return norm

# ---------- step 1: probability enrichment ----------
def enrich_probabilities(input_csv: pathlib.Path, gamma: float) -> pathlib.Path:
    base = normalize_odds(input_csv)
    enriched = []
    for r in base:
        price = float(r["price"])
        implied = 1.0 / price
        p0 = implied
        if r.get("p_model") not in ("", None, "NA"):
            try: p0 = float(r["p_model"])
            except Exception: p0 = implied
        # gamma stretch around 0.5
        p_model = 0.5 + (p0 - 0.5) * gamma
        p_model = max(0.0, min(1.0, p_model))
        out = dict(r); out["p_model"] = p_model
        enriched.append(out)
    path = OUT / "prob_enriched.csv"
    write_csv(path, enriched)
    return path

# ---------- step 2: engine ----------
def run_engine(enriched: pathlib.Path, min_edge: float, edge: float,
               kelly_scale: float, kelly_cap: float, bankroll: float,
               max_picks: int) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path]:
    engine_py = SCRIPTS / "tennis_value_engine.py"
    picks_root = ROOT / "value_picks_pro.csv"
    picks_copy = OUT / "picks_final.csv"
    eng_md = OUT / "engine_summary.md"
    cmd = [sys.executable, str(engine_py),
           "--input", str(enriched),
           "--out-picks", str(picks_root),
           "--out-final", str(picks_copy),
           "--summary", str(eng_md),
           "--stake-mode", "kelly",
           "--edge", str(edge),
           "--kelly-scale", str(kelly_scale),
           "--kelly-cap", str(kelly_cap),
           "--bankroll", str(bankroll),
           "--min-edge", str(min_edge),
           "--max-picks", str(max_picks),
           "--filter-on-te"]
    print("RUN:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    return picks_root, picks_copy, eng_md

# ---------- NEW: step 2.5 postprocess – odds window + caps ----------
def postprocess_picks(picks_csv: pathlib.Path, bankroll: float,
                      odds_min: float, odds_max: float,
                      day_risk: float, bet_cap: float) -> pathlib.Path:
    rows = read_csv(picks_csv)
    if not rows:
        return picks_csv

    # 1) odds window filter
    filtered = []
    for r in rows:
        try:
            price = float(r.get("price") or r.get("odds") or r.get("decimal_odds"))
        except Exception:
            continue
        if (odds_min and price < odds_min) or (odds_max and price > odds_max):
            continue
        filtered.append(r)

    # 2) enforce per-bet cap (units)
    per_cap_units = bankroll * bet_cap
    for r in filtered:
        try:
            units = float(r.get("stake_units", 0.0))
        except Exception:
            units = 0.0
        if per_cap_units > 0 and units > per_cap_units:
            r["stake_units"] = per_cap_units
            # adjust stake_frac_br if present
            try:
                r["stake_frac_br"] = per_cap_units / bankroll
            except Exception:
                pass

    # 3) enforce daily cap – scale down proportionally if needed
    total_units = sum(float(r.get("stake_units", 0.0)) for r in filtered)
    max_units = bankroll * day_risk
    scale = 1.0
    if max_units > 0 and total_units > max_units:
        scale = max_units / total_units
        for r in filtered:
            u = float(r.get("stake_units", 0.0)) * scale
            r["stake_units"] = u
            try:
                r["stake_frac_br"] = u / bankroll
            except Exception:
                pass

    out = OUT / "picks_final.csv"
    write_csv(out, filtered)
    # Write a small note for the summary step to pick up
    (OUT / "postprocess_note.txt").write_text(
        f"odds_window=[{odds_min},{odds_max}], per_bet_cap={bet_cap:.2%}, "
        f"day_risk_cap={day_risk:.2%}, scale={scale:.3f}\n", encoding="utf-8"
    )
    return out

# ---------- step 3: optional matrix backtest ----------
def run_matrix_backtest(input_csv: pathlib.Path, bands: str, edge: float,
                        kelly_scale: float, bankroll: float):
    if not bands:
        return
    mb = SCRIPTS / "run_matrix_backtest.py"
    cmd = [sys.executable, str(mb),
           "--input", str(input_csv),
           "--outdir", str(OUT),
           "--bands", bands,
           "--stake-mode", "kelly",
           "--edge", str(edge),
           "--kelly-scale", str(kelly_scale),
           "--bankroll", str(bankroll)]
    print("RUN:", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print("Backtest step soft-failed:", e)

# ---------- step 4: shortlist + summary ----------
def build_shortlist_md(picks_csv: pathlib.Path) -> pathlib.Path:
    rows = read_csv(picks_csv)
    lines = ["# Tennis Value Engine (shortlist)",""]
    if not rows:
        lines.append("_No picks._")
    else:
        cols = ["Tour","Market","Selection","Opponent","Odds","p_model","p_used","EVu","Kelly","Conf","Bet"]
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("|" + "|".join(["---"]*len(cols)) + "|")
        for r in rows:
            tour = r.get("tour","ATP/WTA")
            mkt  = r.get("market","H2H")
            sel  = r.get("player","?")
            opp  = r.get("opponent","?")
            try: price= float(r.get("price",0))
            except Exception: price = 0.0
            try: pm   = float(r.get("p_model",0))
            except Exception: pm = 0.0
            try: pu   = float(r.get("p_used", pm))
            except Exception: pu = pm
            evu  = pu*price - 1.0
            try: kf   = float(r.get("kelly_f_raw",0))
            except Exception: kf = 0.0
            conf = r.get("model_conf", r.get("model_confidence","-"))
            try: bet  = float(r.get("stake_units",0))
            except Exception: bet = 0.0
            lines.append(f"| {tour} | {mkt} | {sel} | {opp} | {price:.2f} | {pm:.3f} | {pu:.3f} | {evu:.3f} | {kf:.3f} | {conf} | {bet:.2f} |")
    path = OUT / "value_engine_shortlist.md"
    path.write_text("\n".join(lines)+"\n", encoding="utf-8")
    return path

def write_pipeline_summary(engine_md: pathlib.Path, shortlist_md: pathlib.Path,
                           backtest_metrics: pathlib.Path | None):
    lines = ["# Pipeline Summary",""]
    note = OUT / "postprocess_note.txt"
    if note.is_file():
        lines.append(f"_Postprocess: {note.read_text(encoding='utf-8').strip()}_")
        lines.append("")
    if engine_md.is_file():
        lines.append("## Daily Picks")
        lines.append(engine_md.read_text(encoding="utf-8"))
    if shortlist_md.is_file():
        lines.append("\n## Shortlist")
        lines.append(shortlist_md.read_text(encoding="utf-8"))
    if backtest_metrics and backtest_metrics.is_file():
        import json
        j = json.loads(backtest_metrics.read_text(encoding="utf-8"))
        best = (j or {}).get("best_by_roi") or {}
        lines.append("\n## Matrix Backtest — Best by ROI")
        if best:
            lines.append(f"- **Config**: `{best.get('config_id','')}`")
            lines.append(f"- **Band**: {best.get('label','')}")
            lines.append(f"- **Bets**: {best.get('bets','')}"
                         f" | **ROI**: {best.get('roi','')}"
                         f" | **PnL**: {best.get('pnl','')}"
                         f" | **End BR**: {best.get('end_bankroll','')}")
        else:
            lines.append("- No metrics available.")
    (OUT / "pipeline_summary.md").write_text("\n".join(lines)+"n", encoding="utf-8")

# ----------------- CLI -----------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", "-i", default="data/raw/odds/sample_odds.csv",
                    help="Input odds CSV (two-sided or one-sided)")
    ap.add_argument("--gamma", type=float, default=1.06,
                    help="Fav/longshot stretch (1=no change)")
    ap.add_argument("--min-edge", type=float, default=0.02,
                    help="Edge filter threshold (raw engine)")
    ap.add_argument("--edge", type=float, default=0.08,
                    help="True Edge booster (TE8=0.08)")
    ap.add_argument("--kelly-scale", type=float, default=0.5)
    ap.add_argument("--kelly-cap", type=float, default=0.20)
    ap.add_argument("--bankroll", type=float, default=1000.0)
    ap.add_argument("--max-picks", type=int, default=80)
    ap.add_argument("--bands", default="", help='Matrix bands, e.g. "2.0,2.6|2.6,3.2|3.2,4.0" (optional)')
    # NEW guardrails
    ap.add_argument("--odds-min", type=float, default=1.35)
    ap.add_argument("--odds-max", type=float, default=2.40)
    ap.add_argument("--day-risk", type=float, default=0.25,
                    help="Max total daily stake as fraction of bankroll")
    ap.add_argument("--bet-cap", type=float, default=0.05,
                    help="Per-bet stake cap as fraction of bankroll")
    return ap.parse_args()

def main():
    args = parse_args()
    ensure_dirs()

    # 1) Enrich
    enriched = enrich_probabilities(ROOT / args.input, args.gamma)

    # 2) Engine
    picks_root, picks_copy, eng_md = run_engine(
        enriched=enriched,
        min_edge=args.min_edge,
        edge=args.edge,
        kelly_scale=args.kelly_scale,
        kelly_cap=args.kelly_cap,
        bankroll=args.bankroll,
        max_picks=args.max_picks,
    )

    # 2.5) Guardrails
    picks_post = postprocess_picks(
        picks_csv=picks_copy,
        bankroll=args.bankroll,
        odds_min=args.odds_min,
        odds_max=args.odds_max,
        day_risk=args.day_risk,
        bet_cap=args.bet_cap,
    )

    # 3) Optional backtest (use postprocessed file)
    if args.bands:
        run_matrix_backtest(
            input_csv=picks_post, bands=args.bands,
            edge=args.edge, kelly_scale=args.kelly_scale,
            bankroll=args.bankroll
        )

    # 4) Shortlist + pipeline summary
    shortlist = build_shortlist_md(picks_root)
    metrics = OUT / "backtest_metrics.json"
    write_pipeline_summary(eng_md, shortlist, metrics if metrics.is_file() else None)

    print("DONE. Artifacts in ./outputs")

if __name__ == "__main__":
    main()
